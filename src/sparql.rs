use crate::graph_iri::{insert_graph_mapping, resolve_hdt_graph_path, resolve_named_graph_path};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::term::{BlankNode, NamedNode, Term};
use spargebra::{Query, SparqlParser};
use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock},
    time::SystemTime,
};

fn lock_read_file_paths<'a>(
    file_paths: &'a Arc<RwLock<HashMap<String, PathBuf>>>,
    context: &str,
) -> anyhow::Result<std::sync::RwLockReadGuard<'a, HashMap<String, PathBuf>>> {
    file_paths
        .read()
        .map_err(|e| anyhow::anyhow!("{context}: poisoned lock: {e}"))
}

#[cfg(feature = "server")]
fn lock_write_file_paths<'a>(
    file_paths: &'a Arc<RwLock<HashMap<String, PathBuf>>>,
    context: &str,
) -> anyhow::Result<std::sync::RwLockWriteGuard<'a, HashMap<String, PathBuf>>> {
    file_paths
        .write()
        .map_err(|e| anyhow::anyhow!("{context}: poisoned lock: {e}"))
}

#[cfg(all(test, feature = "server"))]
fn graph_uri_for_path(path: &Path) -> anyhow::Result<String> {
    resolve_hdt_graph_path(path).map(|resolved| resolved.into_parts().0)
}

/// Boundary over a Header-Dictionary-Triples (HDT) storage layer.
///
/// Maps graph IRIs to files on disk and keeps the packages those files decode
/// to open between queries. Opening is not cheap: the hybrid backend decodes a
/// whole wavelet tree out of the index sidecar, which for a 5 GB package is
/// about a second of CPU and 660 MB of allocation. Doing that per request also
/// meant concurrent requests each decoded their own copy of identical data.
///
/// Handles are shared as `Arc<HdtAny>`, which is sound because `HdtAny` is
/// `Sync` and holds no interior mutability beyond its own caches, so queries
/// take no lock at all. Only the cache map is locked, and only to look up.
#[derive(Clone)]
pub struct AggregateHdt {
    // Map graph names (URIs) to file paths on disk
    file_paths: Arc<RwLock<HashMap<String, PathBuf>>>,
    // Packages kept open between queries, keyed by path. Shared across clones
    // of this store, which is what makes it useful to `serve`: the store is
    // cloned per request.
    handles: Arc<Mutex<HandleCache>>,
    // Optional explicit default graph membership. If None, all loaded graphs are used as default.
    default_graphs: Option<HashSet<String>>,
    // Optional explicit named graph membership. If None, all loaded graphs are named.
    named_graphs: Option<HashSet<String>>,
}

fn new_handle_cache() -> Arc<Mutex<HandleCache>> {
    Arc::new(Mutex::new(HandleCache::new(default_handle_cache_budget())))
}

/// Share of system memory the handle cache may hold by default.
const HANDLE_CACHE_MEMORY_SHARE: u64 = 4;

/// Budget used when system memory cannot be determined.
const HANDLE_CACHE_FALLBACK_BYTES: usize = 1 << 30;

/// Default ceiling on what the handle cache may hold.
fn default_handle_cache_budget() -> usize {
    system_memory_bytes().map_or(HANDLE_CACHE_FALLBACK_BYTES, |total| {
        usize::try_from(total / HANDLE_CACHE_MEMORY_SHARE).unwrap_or(usize::MAX)
    })
}

#[cfg(target_os = "linux")]
fn system_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    line.split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn system_memory_bytes() -> Option<u64> {
    None
}

/// What makes a cached handle still the right answer for a path.
///
/// Rebuilding an HDT in place changes its modification time, and almost always
/// its length too. `serve` already stats every package on every request to
/// decide whether to re-`sync`, so checking this costs nothing new.
///
/// A file whose modification time cannot be read has no key and is therefore
/// never cached, rather than being cached under a key that cannot detect a
/// change.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct FileKey {
    modified: SystemTime,
    len: u64,
}

impl FileKey {
    fn for_path(path: &Path) -> Option<Self> {
        let metadata = std::fs::metadata(path).ok()?;
        Some(Self {
            modified: metadata.modified().ok()?,
            len: metadata.len(),
        })
    }
}

/// One HDT package held open between queries.
struct CachedHandle {
    key: FileKey,
    hdt: Arc<hdt::HdtAny>,
    /// Heap footprint, from `HdtAny::size_in_bytes`, which counts the decoded
    /// structures but not memory-mapped file pages -- those live in the OS page
    /// cache and are the kernel's to reclaim, not ours to budget.
    bytes: usize,
    /// Tick of the last snapshot to use this handle, for LRU eviction.
    last_used: u64,
}

/// Open HDT packages, bounded by heap footprint.
struct HandleCache {
    entries: HashMap<PathBuf, CachedHandle>,
    budget: usize,
    tick: u64,
}

impl HandleCache {
    fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            budget,
            tick: 0,
        }
    }

    /// Total held. Summed rather than tracked so there is no counter to drift
    /// out of step with the map; a store has tens of packages, not millions.
    fn bytes(&self) -> usize {
        self.entries.values().map(|entry| entry.bytes).sum()
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    /// The handle for `path`, if it is open and still matches the file on disk.
    fn get(&mut self, path: &Path, key: FileKey) -> Option<Arc<hdt::HdtAny>> {
        let tick = self.next_tick();
        let entry = self.entries.get_mut(path)?;
        if entry.key != key {
            // The file was rebuilt under us; the caller reopens it.
            return None;
        }
        entry.last_used = tick;
        Some(Arc::clone(&entry.hdt))
    }

    fn insert(&mut self, path: PathBuf, key: FileKey, hdt: Arc<hdt::HdtAny>) {
        let tick = self.next_tick();
        let bytes = hdt.size_in_bytes();
        self.entries.insert(
            path,
            CachedHandle {
                key,
                hdt,
                bytes,
                last_used: tick,
            },
        );
    }

    /// Drop least-recently-used handles until the budget is met.
    ///
    /// Packages the calling snapshot is about to use are never evicted. They
    /// are alive through that snapshot's `Arc` regardless, so dropping them
    /// here would not free anything -- it would only force the next request to
    /// decode them again. The budget is therefore a bound on what is retained
    /// *between* queries; the peak stays what it always was, the working set of
    /// a single request.
    fn evict(&mut self, in_use: &HashSet<PathBuf>) {
        let mut total = self.bytes();
        if total <= self.budget {
            return;
        }
        let mut candidates: Vec<(u64, PathBuf)> = self
            .entries
            .iter()
            .filter(|(path, _)| !in_use.contains(*path))
            .map(|(path, entry)| (entry.last_used, path.clone()))
            .collect();
        candidates.sort_unstable();
        for (_, path) in candidates {
            if total <= self.budget {
                break;
            }
            if let Some(entry) = self.entries.remove(&path) {
                total = total.saturating_sub(entry.bytes);
            }
        }
    }

    /// Forget handles for files that are no longer part of the dataset.
    fn retain_paths(&mut self, keep: &HashSet<PathBuf>) {
        self.entries.retain(|path, _| keep.contains(path));
    }
}

/// The loaded graphs a query runs against, with the per-query lookups already
/// resolved.
///
/// `spareval` calls `internal_quads_for_pattern` once per binding of the outer
/// pattern in a nested-loop join, so anything this type recomputes per call is
/// multiplied by the size of the intermediate result. Graph selection and the
/// `Arc<str>` form of each graph IRI are therefore resolved once here, at
/// snapshot time, rather than on every probe.
pub struct AggregateHdtSnapshot {
    // The loaded graphs, in a stable order. Each entry can be either the
    // hybrid (mmap+cache) or fully in-memory backend; HdtAny picks the right
    // one per file (see hdt::HdtAny::open).
    graphs: Vec<LoadedGraph>,
    // Indices into `graphs` that make up the default graph: the explicitly
    // configured membership when there is one, else every loaded graph.
    default_graph: Box<[usize]>,
    // Indices into `graphs` that are visible as named graphs, under the same
    // rule.
    named_graph: Box<[usize]>,
    // Graph IRI to index, for resolving a GRAPH <iri> pattern.
    by_iri: HashMap<Arc<str>, usize>,
}

/// One loaded graph, with its IRI pre-wrapped so that yielding it with a quad
/// is a refcount increment rather than an allocation.
struct LoadedGraph {
    iri: Arc<str>,
    hdt: Arc<hdt::HdtAny>,
}

/// Which graphs one `internal_quads_for_pattern` call has to visit.
///
/// Borrowed from the snapshot rather than collected, so that the common cases
/// -- the default graph, or one named graph -- allocate nothing per call.
#[derive(Clone, Copy)]
enum GraphSelection<'a> {
    /// Every graph at these indices, in order.
    Many(&'a [usize]),
    /// Exactly one graph.
    One(usize),
    /// No graph matches; the pattern yields nothing.
    Empty,
}

impl GraphSelection<'_> {
    fn get(self, position: usize) -> Option<usize> {
        match self {
            Self::Many(indices) => indices.get(position).copied(),
            Self::One(index) if position == 0 => Some(index),
            Self::One(_) | Self::Empty => None,
        }
    }
}

impl AggregateHdtSnapshot {
    /// Build a snapshot from loaded graphs and the configured memberships.
    ///
    /// `default_graphs` and `named_graphs` are `None` when the dataset was
    /// discovered from the filesystem, which means "every loaded graph".
    fn new(
        loaded: Vec<(String, Arc<hdt::HdtAny>)>,
        default_graphs: Option<&HashSet<String>>,
        named_graphs: Option<&HashSet<String>>,
    ) -> Self {
        let graphs: Vec<LoadedGraph> = loaded
            .into_iter()
            .map(|(iri, hdt)| LoadedGraph {
                iri: Arc::from(iri),
                hdt,
            })
            .collect();
        let select = |members: Option<&HashSet<String>>| -> Box<[usize]> {
            graphs
                .iter()
                .enumerate()
                .filter(|(_, graph)| members.is_none_or(|m| m.contains(graph.iri.as_ref())))
                .map(|(index, _)| index)
                .collect()
        };
        let default_graph = select(default_graphs);
        let named_graph = select(named_graphs);
        let by_iri = graphs
            .iter()
            .enumerate()
            .map(|(index, graph)| (Arc::clone(&graph.iri), index))
            .collect();
        Self {
            graphs,
            default_graph,
            named_graph,
            by_iri,
        }
    }

    /// The graphs a pattern with this `graph_name` has to be answered from.
    ///
    /// Mirrors `spareval`'s encoding: `Some(None)` is the default graph,
    /// `Some(Some(iri))` one named graph, and `None` every named graph. The
    /// nested `Option` is that trait's parameter shape, not a choice made here.
    #[allow(clippy::option_option)]
    fn select(&self, graph_name: Option<Option<&Arc<str>>>) -> GraphSelection<'_> {
        match graph_name {
            Some(None) => GraphSelection::Many(&self.default_graph),
            Some(Some(target)) => match self.by_iri.get(target) {
                Some(&index) if self.named_graph.contains(&index) => GraphSelection::One(index),
                _ => GraphSelection::Empty,
            },
            None => GraphSelection::Many(&self.named_graph),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternQuad {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub graph_name: Option<String>,
}

impl AggregateHdt {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            file_paths: Arc::new(RwLock::new(HashMap::new())),
            handles: new_handle_cache(),
            default_graphs: None,
            named_graphs: None,
        }
    }

    pub fn new(paths: &[String]) -> anyhow::Result<Self> {
        let mut file_paths: HashMap<String, std::path::PathBuf> = HashMap::new();
        for p in paths {
            let resolved = resolve_hdt_graph_path(Path::new(p))?;
            insert_graph_mapping(&mut file_paths, resolved, "duplicate graph IRI")?;
        }

        Ok(Self {
            file_paths: Arc::new(RwLock::new(file_paths)),
            handles: new_handle_cache(),
            default_graphs: None,
            named_graphs: None,
        })
    }

    pub fn new_with_mappings(
        default_paths: &[String],
        named_graphs: &[(String, String)],
    ) -> anyhow::Result<Self> {
        let mut file_paths: HashMap<String, std::path::PathBuf> = HashMap::new();
        let mut default_graphs: HashSet<String> = HashSet::new();
        let mut named_graph_set: HashSet<String> = HashSet::new();

        for p in default_paths {
            let resolved = resolve_hdt_graph_path(Path::new(p))?;
            default_graphs.insert(resolved.graph_iri().to_string());
            insert_graph_mapping(&mut file_paths, resolved, "duplicate graph IRI")?;
        }

        for (graph_iri, file_path) in named_graphs {
            let resolved = resolve_named_graph_path(graph_iri, Path::new(file_path))?;
            named_graph_set.insert(resolved.graph_iri().to_string());
            insert_graph_mapping(&mut file_paths, resolved, "named graph IRI")?;
        }

        Ok(Self {
            file_paths: Arc::new(RwLock::new(file_paths)),
            handles: new_handle_cache(),
            default_graphs: Some(default_graphs),
            named_graphs: Some(named_graph_set),
        })
    }

    /// Cap what the handle cache retains between queries, in bytes of decoded
    /// HDT structure.
    ///
    /// Defaults to a quarter of system memory. Lower it when the process shares
    /// a machine, raise it when the dataset is larger than that and every graph
    /// is queried often. A budget smaller than a single package does not break
    /// anything -- that package is simply reopened per request, as it was
    /// before this cache existed.
    #[must_use]
    pub fn with_handle_cache_budget(self, bytes: usize) -> Self {
        self.lock_handles().budget = bytes;
        self
    }

    /// How many HDT packages are currently held open.
    #[must_use]
    pub fn cached_handle_count(&self) -> usize {
        self.lock_handles().entries.len()
    }

    /// Bytes of decoded HDT structure currently held open. Memory-mapped file
    /// pages are not counted; see [`CachedHandle::bytes`].
    #[must_use]
    pub fn cached_handle_bytes(&self) -> usize {
        self.lock_handles().bytes()
    }

    /// Take the handle-cache lock, recovering from poisoning.
    ///
    /// A panic elsewhere must not make the store permanently unqueryable, and
    /// there is no invariant here for a panicking thread to leave half-applied:
    /// entries are inserted and removed one at a time, the byte total is
    /// derived rather than tracked, and a handle whose file has changed is
    /// rejected by its own key.
    fn lock_handles(&self) -> MutexGuard<'_, HandleCache> {
        self.handles.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Create a snapshot of HDT instances for querying.
    ///
    /// # Arguments
    /// * `named_graphs` - Optional filter to only load specific named graphs.
    ///   If None, all available graphs are loaded.
    ///   If Some(vec), only graphs in the vec are loaded.
    ///
    /// # Performance
    /// Packages already open are handed straight back, so a snapshot over an
    /// unchanged dataset costs a stat per graph and a refcount increment. Only
    /// the first request after a package appears or is rebuilt pays to decode
    /// it. Filtering still matters for the first request and for the cache
    /// budget: a graph never asked for is never opened.
    ///
    /// # Example
    /// ```ignore
    /// // Load only specific graphs
    /// let snapshot = store.get_snapshot(Some(vec![
    ///     "file:///graph1.hdt".to_string(),
    ///     "file:///graph2.hdt".to_string(),
    /// ]))?;
    ///
    /// // Load all graphs
    /// let snapshot = store.get_snapshot(None)?;
    /// ```
    pub fn get_snapshot(
        &self,
        named_graphs: Option<Vec<String>>,
    ) -> anyhow::Result<AggregateHdtSnapshot> {
        use rayon::prelude::*;

        let file_paths_guard = lock_read_file_paths(&self.file_paths, "reading HDT path map")?;
        let named_graph_filter: Option<HashSet<String>> =
            named_graphs.map(|g| g.into_iter().collect());

        // Optimization: Filter graphs BEFORE loading into memory
        let paths_vec: Vec<(String, std::path::PathBuf)> = file_paths_guard
            .iter()
            .filter(|(graph_name, _path)| {
                // If named_graphs filter is specified, only include graphs in the filter
                if let Some(ref filter) = named_graph_filter {
                    filter.contains(graph_name.as_str())
                } else {
                    true // No filter - include all graphs
                }
            })
            .map(|(g, path)| (g.clone(), path.clone()))
            .collect();
        drop(file_paths_guard);

        // Identify each package before consulting the cache, so a file that has
        // been rebuilt since it was opened is reopened rather than answered
        // from a stale handle.
        let keyed: Vec<(String, PathBuf, Option<FileKey>)> = paths_vec
            .into_iter()
            .map(|(graph_name, path)| {
                let key = FileKey::for_path(&path);
                (graph_name, path, key)
            })
            .collect();

        // Cache hits in one pass, so the lock is never held across an open.
        let mut handles: Vec<Option<Arc<hdt::HdtAny>>> = {
            let mut cache = self.lock_handles();
            keyed
                .iter()
                .map(|(_, path, key)| key.and_then(|key| cache.get(path, key)))
                .collect()
        };

        // Open what is left in parallel. HdtAny::open prefers the hybrid
        // (mmap+cache) backend but falls back to in-memory for files where
        // the hybrid cache cannot be built (e.g. zero-triple HDTs that the
        // wavelet-tree library refuses to index).
        //
        // Two requests racing on the same missing package both open it and the
        // second insert wins. That wastes one decode and is not worth a
        // per-path lock to avoid: the alternative is making every request wait
        // on whichever one got there first.
        let opened: Vec<(usize, Arc<hdt::HdtAny>)> = handles
            .iter()
            .enumerate()
            .filter(|(_, handle)| handle.is_none())
            .map(|(index, _)| index)
            .collect::<Vec<_>>()
            .par_iter()
            .map(|&index| -> anyhow::Result<(usize, Arc<hdt::HdtAny>)> {
                let path = &keyed[index].1;
                let hdt = hdt::HdtAny::open_with_threshold(path, None).map_err(|e| {
                    anyhow::anyhow!("Failed to load HDT from {}: {e}", path.display())
                })?;
                Ok((index, Arc::new(hdt)))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        {
            let mut cache = self.lock_handles();
            for (index, hdt) in &opened {
                let (_, path, key) = &keyed[*index];
                if let Some(key) = *key {
                    cache.insert(path.clone(), key, Arc::clone(hdt));
                }
            }
            let in_use: HashSet<PathBuf> = keyed.iter().map(|(_, path, _)| path.clone()).collect();
            cache.evict(&in_use);
        }
        for (index, hdt) in opened {
            handles[index] = Some(hdt);
        }

        let hdts: Vec<(String, Arc<hdt::HdtAny>)> = keyed
            .into_iter()
            .zip(handles)
            .map(|((graph_name, _, _), handle)| {
                let hdt = handle.expect("every selected graph was either cached or opened above");
                (graph_name, hdt)
            })
            .collect();

        Ok(AggregateHdtSnapshot::new(
            hdts,
            self.default_graphs.as_ref(),
            self.named_graphs.as_ref(),
        ))
    }

    /// Returns `true` if the given graph IRI is registered in the path map.
    ///
    /// This is a cheap path-map lookup; it does not load any HDT data.
    /// Useful for store-protocol HEAD/PUT/DELETE preconditions.
    #[cfg(feature = "server")]
    pub fn contains_graph_name(&self, graph_name: &str) -> anyhow::Result<bool> {
        let guard = lock_read_file_paths(&self.file_paths, "checking graph existence")?;
        Ok(guard.contains_key(graph_name))
    }

    /// Returns all graph IRIs currently registered in the path map.
    ///
    /// This is a cheap path-map lookup; it does not load any HDT data.
    #[cfg(feature = "server")]
    pub fn graph_names(&self) -> anyhow::Result<Vec<String>> {
        let guard = lock_read_file_paths(&self.file_paths, "listing graph names")?;
        Ok(guard.keys().cloned().collect())
    }

    /// Returns the on-disk canonical path for the given graph IRI, or
    /// `None` if it is not registered.
    ///
    /// This is a cheap path-map lookup; it does not load any HDT data.
    #[cfg(feature = "server")]
    pub fn graph_path(&self, graph_name: &str) -> anyhow::Result<Option<PathBuf>> {
        let guard = lock_read_file_paths(&self.file_paths, "reading graph path")?;
        Ok(guard.get(graph_name).cloned())
    }

    /// Sync the `AggregateHdt` with the current HDT files in the specified location.
    /// This method refreshes mappings by re-scanning HDT files in the location.
    ///
    /// Returns a tuple of (`added_count`, `removed_count`).
    #[cfg(feature = "server")]
    pub fn sync(&self, location: &std::path::Path) -> Result<(usize, usize), anyhow::Error> {
        use std::collections::HashSet;

        if self.default_graphs.is_some() || self.named_graphs.is_some() {
            return Err(anyhow::anyhow!(
                "sync is only supported for filesystem-discovered datasets"
            ));
        }

        let canonical_location = location.canonicalize().map_err(|e| {
            anyhow::anyhow!(
                "Failed to canonicalize sync location {}: {e}",
                location.display()
            )
        })?;
        if !canonical_location.is_dir() {
            return Err(anyhow::anyhow!(
                "Sync location is not a directory: {}",
                location.display()
            ));
        }

        // Scan the location for .hdt files
        let mut current_files: HashSet<std::path::PathBuf> = HashSet::new();
        let mut discovered_mappings: HashMap<String, std::path::PathBuf> = HashMap::new();
        for entry in std::fs::read_dir(&canonical_location)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file()
                && let Some(ext) = path.extension()
                && ext.eq_ignore_ascii_case("hdt")
            {
                let resolved = resolve_hdt_graph_path(&path)?;
                current_files.insert(resolved.canonical_path().to_path_buf());
                insert_graph_mapping(&mut discovered_mappings, resolved, "duplicate graph IRI")?;
            }
        }

        let (added, removed) = {
            let mut file_paths = lock_write_file_paths(&self.file_paths, "modifying HDT paths")?;
            // Build set of existing paths for comparison
            let existing_paths: HashSet<PathBuf> = file_paths.values().cloned().collect();
            let added = current_files.difference(&existing_paths).count();
            let removed = existing_paths.difference(&current_files).count();
            *file_paths = discovered_mappings;
            (added, removed)
        };

        // Let go of packages that are no longer part of the dataset. A file
        // that is still here but was rebuilt needs no action: its cached handle
        // carries the old modification time and will be rejected on next use.
        // Taken after the path-map guard is dropped, so the two locks are never
        // held at once.
        self.lock_handles().retain_paths(&current_files);

        Ok((added, removed))
    }
}

/// Create the correct term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
// Based on https://github.com/KonradHoeffner/hdt/blob/871db777db3220dc4874af022287975b31d72d3a/src/hdt_graph.rs#L64
pub fn hdt_bgp_str_to_term(s: &str) -> Result<Term, Error> {
    match s.chars().next() {
        None => Err(Error::new(ErrorKind::InvalidData, "empty input")),
        // Double-quote delimiters are used around the string.
        Some('"') => match Term::from_str(s) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::new(
                ErrorKind::InvalidData,
                format!("literal parse error {e} for {s}"),
            )),
        },
        // Underscore prefix indicating a Blank Node.
        Some('_') => match BlankNode::from_str(s) {
            Ok(n) => Ok(n.into()),
            Err(e) => Err(Error::new(
                ErrorKind::InvalidData,
                format!("blanknode parse error {e} for {s}"),
            )),
        },
        // Double-quote delimiters not present. Underscore prefix
        // not present. Assuming a URI.
        _ => {
            // Note that Term::from_str() will not work for URIs (NamedNode) when the string is not within "<" and ">" delimiters.
            match NamedNode::new(s) {
                Ok(n) => Ok(n.into()),
                Err(e) => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("iri parse error {e} for {s}"),
                )),
            }
        }
    }
}

/// Convert triple string formats from `OxRDF` to HDT.
#[must_use]
pub fn term_to_hdt_bgp_str(term: Term) -> String {
    match term {
        Term::NamedNode(named_node) => named_node.into_string(),
        Term::Literal(literal) => literal.to_string(),
        Term::BlankNode(s) => s.to_string(),
    }
}

struct StreamingInternalQuadIter<'a> {
    graphs: &'a [LoadedGraph],
    selection: GraphSelection<'a>,
    subject: Option<Arc<str>>,
    predicate: Option<Arc<str>>,
    object: Option<Arc<str>>,
    emit_default_graph: bool,
    position: usize,
    current_iter: Option<StreamingInternalQuadIterator<'a>>,
}

type StreamingInternalQuadIterator<'a> =
    Box<dyn Iterator<Item = Result<InternalQuad<Arc<str>>, Error>> + 'a>;

fn scoped_blank_node(term: Arc<str>, graph_name: &Arc<str>) -> Arc<str> {
    if !term.starts_with("_:") {
        return term;
    }
    let graph_hex = graph_name_hex(graph_name);
    let original = &term[2..];
    Arc::from(format!("_:g{graph_hex}_{original}"))
}

fn graph_name_hex(graph_name: &Arc<str>) -> String {
    let mut graph_hex = String::with_capacity(graph_name.len() * 2);
    for b in graph_name.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut graph_hex, "{b:02x}");
    }
    graph_hex
}

fn unscoped_pattern_term(term: &Arc<str>, graph_name: &Arc<str>) -> Option<Arc<str>> {
    if !term.starts_with("_:g") {
        return Some(term.clone());
    }
    let prefix = format!("_:g{}_", graph_name_hex(graph_name));
    let original = term.strip_prefix(&prefix)?;
    Some(Arc::from(format!("_:{original}")))
}

/// Rewrite the three pattern positions into one graph's own term space.
///
/// `None` means the pattern cannot match in this graph at all, because a bound
/// term is a blank node scoped to a different one.
fn unscoped_pattern(
    pattern: [Option<&Arc<str>>; 3],
    graph_name: &Arc<str>,
) -> Option<[Option<Arc<str>>; 3]> {
    let mut unscoped: [Option<Arc<str>>; 3] = [None, None, None];
    for (slot, term) in unscoped.iter_mut().zip(pattern) {
        if let Some(term) = term {
            *slot = Some(unscoped_pattern_term(term, graph_name)?);
        }
    }
    Some(unscoped)
}

impl StreamingInternalQuadIter<'_> {
    fn ensure_current_iter(&mut self) -> bool {
        while self.current_iter.is_none() {
            let Some(index) = self.selection.get(self.position) else {
                break;
            };
            self.position += 1;
            let LoadedGraph { iri, hdt } = &self.graphs[index];
            let graph_name = Arc::clone(iri);
            let Some([subject_filter, predicate_filter, object_filter]) = unscoped_pattern(
                [
                    self.subject.as_ref(),
                    self.predicate.as_ref(),
                    self.object.as_ref(),
                ],
                &graph_name,
            ) else {
                continue;
            };
            let emit_default_graph = self.emit_default_graph;

            // Upstream owns the pattern dispatch, and with it the
            // `TripleCache` that materialises each dictionary id in a single
            // `Arc` allocation and reuses the previous one when consecutive
            // triples repeat a term -- which, in SPO order, most of them do.
            self.current_iter = Some(Box::new(
                hdt.triples_with_arc_pattern(subject_filter, predicate_filter, object_filter)
                    .map(move |[subject, predicate, object]| {
                        let output_graph_name = if emit_default_graph {
                            None
                        } else {
                            Some(graph_name.clone())
                        };
                        Ok(InternalQuad {
                            subject: scoped_blank_node(subject, &graph_name),
                            predicate,
                            object: scoped_blank_node(object, &graph_name),
                            graph_name: output_graph_name,
                        })
                    }),
            ));
        }
        self.current_iter.is_some()
    }
}

impl Iterator for StreamingInternalQuadIter<'_> {
    type Item = Result<InternalQuad<Arc<str>>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.ensure_current_iter() {
                return None;
            }
            if let Some(current_iter) = self.current_iter.as_mut() {
                if let Some(item) = current_iter.next() {
                    return Some(item);
                }
                self.current_iter = None;
            }
        }
    }
}

impl<'a> QueryableDataset<'a> for &'a AggregateHdtSnapshot {
    type InternalTerm = Arc<str>;
    type Error = Error;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&Arc<str>>,
        predicate: Option<&Arc<str>>,
        object: Option<&Arc<str>>,
        graph_name: Option<Option<&Arc<str>>>,
    ) -> impl Iterator<Item = Result<InternalQuad<Self::InternalTerm>, Error>> + use<'a> {
        let emit_default_graph = matches!(graph_name, Some(None));
        // Graph membership was resolved when the snapshot was built, so this is
        // a slice borrow (or, for GRAPH <iri>, one hash lookup) rather than a
        // filtered scan of every loaded graph on every probe.
        let selection = self.select(graph_name);

        let subject = subject.cloned();
        let predicate = predicate.cloned();
        let object = object.cloned();

        StreamingInternalQuadIter::<'a> {
            graphs: &self.graphs,
            selection,
            subject,
            predicate,
            object,
            emit_default_graph,
            position: 0,
            current_iter: None,
        }
    }

    fn internalize_term(&self, term: Term) -> Result<Arc<str>, Error> {
        Ok(Arc::from(term_to_hdt_bgp_str(term)))
    }

    fn externalize_term(&self, term: Arc<str>) -> Result<Term, Error> {
        hdt_bgp_str_to_term(&term)
    }

    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<Self::InternalTerm, Self::Error>> + use<'a> {
        let keys: Vec<Arc<str>> = self
            .named_graph
            .iter()
            .map(|&index| Arc::clone(&self.graphs[index].iri))
            .collect();
        keys.into_iter().map(Ok)
    }

    /// Count the triples matching a pattern, summed over the graphs it reads.
    ///
    /// Comunica's HDT source does the same thing through `countTriples`, and for
    /// the same reason: without it the optimiser scores every `?s <p> ?o`
    /// pattern alike and, when two tie, leaves them in the order the query was
    /// written -- which for a nested-loop join decides whether the selective
    /// pattern drives the scan or is driven by it.
    ///
    /// Most shapes cost O(1) here because the HDT index already knows the extent
    /// of the range a pattern walks, so this stays far cheaper than the plan it
    /// improves. `?P?` and `S?O` come back as approximations, which is fine:
    /// the number is only ever compared against other patterns' numbers.
    fn estimated_cardinality(
        &self,
        subject: Option<&Arc<str>>,
        predicate: Option<&Arc<str>>,
        object: Option<&Arc<str>>,
        graph_name: Option<Option<&Arc<str>>>,
    ) -> Option<usize> {
        let selection = self.select(graph_name);
        let mut total: usize = 0;
        let mut position = 0;
        while let Some(index) = selection.get(position) {
            position += 1;
            let LoadedGraph { iri, hdt } = &self.graphs[index];
            let Some([subject, predicate, object]) =
                unscoped_pattern([subject, predicate, object], iri)
            else {
                continue;
            };
            total = total.saturating_add(
                hdt.cardinality(subject.as_deref(), predicate.as_deref(), object.as_deref())
                    .value,
            );
        }
        Some(total)
    }

    fn contains_internal_graph_name(&self, graph_name: &Arc<str>) -> Result<bool, Self::Error> {
        Ok(matches!(
            self.select(Some(Some(graph_name))),
            GraphSelection::One(_)
        ))
    }
}

pub fn query<'a, D>(
    q: &str,
    dataset: D,
    base_iri: Option<String>,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError>
where
    D: QueryableDataset<'a>,
{
    query_with_debug_plan(q, dataset, base_iri, false)
}

pub fn parse_query(q: &str, base_iri: &str) -> Result<Query, QueryEvaluationError> {
    Ok(SparqlParser::new()
        .with_base_iri(base_iri.to_string())
        .map_err(|e| QueryEvaluationError::Unexpected(Box::new(e)))?
        .parse_query(q)?)
}

pub fn query_parsed_with_debug_plan<'a, D>(
    parsed: &Query,
    dataset: D,
    debug_plan: bool,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError>
where
    D: QueryableDataset<'a>,
{
    evaluate_query_with_debug_plan(parsed, dataset, debug_plan)
}

pub fn query_with_debug_plan<'a, D>(
    q: &str,
    dataset: D,
    base_iri: Option<String>,
    debug_plan: bool,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError>
where
    D: QueryableDataset<'a>,
{
    let base = base_iri.unwrap_or_else(|| "http://example.com/".to_string());
    let parsed = parse_query(q, &base)?;
    evaluate_query_with_debug_plan(&parsed, dataset, debug_plan)
}

pub fn query_select_tsv_with_debug_plan<'a, D>(
    q: &str,
    dataset: D,
    base_iri: Option<String>,
    debug_plan: bool,
) -> anyhow::Result<Vec<u8>>
where
    D: QueryableDataset<'a>,
{
    let results = query_with_debug_plan(q, dataset, base_iri, debug_plan)
        .map_err(|e| anyhow::anyhow!("problem executing the hdt query: {e}"))?;
    let spareval::QueryResults::Solutions(query_solution_iter) = results else {
        return Err(anyhow::anyhow!(
            "expected SELECT query results while serializing TSV"
        ));
    };
    let mut output = Vec::new();
    let results_writer = QueryResultsSerializer::from_format(QueryResultsFormat::Tsv);
    let mut serializer = results_writer
        .serialize_solutions_to_writer(&mut output, query_solution_iter.variables().into())?;
    for solution in query_solution_iter {
        serializer.serialize(&solution?)?;
    }
    serializer.finish()?;
    Ok(output)
}

pub fn quads_for_pattern_strings(
    hdt: &AggregateHdtSnapshot,
    subject: Option<&str>,
    predicate: Option<&str>,
    object: Option<&str>,
    graph_name: Option<Option<&str>>,
) -> anyhow::Result<Vec<PatternQuad>> {
    use spareval::QueryableDataset;

    let subject = subject.map(Arc::<str>::from);
    let predicate = predicate.map(Arc::<str>::from);
    let object = object.map(Arc::<str>::from);
    let graph_name = graph_name.map(|value| value.map(Arc::<str>::from));
    let graph_name_ref = graph_name.as_ref().map(|value| value.as_ref());

    let mut quads = Vec::new();
    for quad in hdt.internal_quads_for_pattern(
        subject.as_ref(),
        predicate.as_ref(),
        object.as_ref(),
        graph_name_ref,
    ) {
        let quad = quad?;
        quads.push(PatternQuad {
            subject: quad.subject.to_string(),
            predicate: quad.predicate.to_string(),
            object: quad.object.to_string(),
            graph_name: quad.graph_name.map(|value| value.to_string()),
        });
    }
    Ok(quads)
}

fn evaluate_query_with_debug_plan<'a, D>(
    parsed: &Query,
    dataset: D,
    debug_plan: bool,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError>
where
    D: QueryableDataset<'a>,
{
    // NOTE: previously this ran .without_optimizations() in order to pass W3C suites, but is no
    // longer needed with current spareval revision. With optimizations, query responses are
    // significantly reduced.
    // TODO: monitor for optimizer-specific regressions, perhaps introducing toggle
    let evaluator = QueryEvaluator::new();
    if debug_plan {
        let (results, explanation) = evaluator.prepare(parsed).explain(dataset);
        let mut json = Vec::new();
        explanation
            .write_in_json(&mut json)
            .map_err(|e| QueryEvaluationError::Unexpected(Box::new(e)))?;
        eprintln!("{}", String::from_utf8_lossy(&json));
        results
    } else {
        evaluator.prepare(parsed).execute(dataset)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(feature = "server")]
    use super::*;
    #[cfg(not(feature = "server"))]
    use super::{
        AggregateHdtSnapshot, QueryEvaluationError, query, scoped_blank_node, unscoped_pattern_term,
    };
    #[cfg(feature = "server")]
    use spareval::QueryableDataset;

    /// Helper function to get the path to a test HDT file
    #[cfg(feature = "server")]
    fn get_test_hdt_path(filename: &str) -> String {
        use std::path::PathBuf;

        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests");
        path.push("resources");
        path.push(filename);
        path.to_string_lossy().into_owned()
    }

    #[cfg(feature = "server")]
    fn utf8_tempdir() -> anyhow::Result<tempfile::TempDir> {
        tempfile::Builder::new()
            .prefix("de-tests-")
            .tempdir_in(std::env::current_dir()?)
            .map_err(anyhow::Error::from)
    }

    /// A package opened once is handed back, not decoded again.
    #[test]
    #[cfg(feature = "server")]
    fn test_handle_is_reused_across_snapshots() -> anyhow::Result<()> {
        let hdt_path = get_test_hdt_path("apple.hdt");
        let store = AggregateHdt::new(std::slice::from_ref(&hdt_path))?;

        let first = store
            .get_snapshot(None)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(store.cached_handle_count(), 1);
        assert!(store.cached_handle_bytes() > 0);

        let second = store
            .get_snapshot(None)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert!(
            Arc::ptr_eq(&first.graphs[0].hdt, &second.graphs[0].hdt),
            "the second snapshot should share the first snapshot's handle"
        );
        assert_eq!(store.cached_handle_count(), 1, "no second copy was opened");
        Ok(())
    }

    /// Rebuilding a package in place invalidates its handle.
    ///
    /// The cache key is the file's modification time and length, so a rebuild
    /// is noticed without the store being told about it.
    #[test]
    #[cfg(feature = "server")]
    fn test_rebuilt_package_is_reopened() -> anyhow::Result<()> {
        let work_dir = utf8_tempdir()?;
        let hdt_path = work_dir.path().join("fruit.hdt");
        std::fs::copy(get_test_hdt_path("apple.hdt"), &hdt_path)?;
        let hdt_path_str = hdt_path.to_string_lossy().into_owned();
        let store = AggregateHdt::new(std::slice::from_ref(&hdt_path_str))?;

        let first = store
            .get_snapshot(None)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let first_handle = Arc::clone(&first.graphs[0].hdt);
        drop(first);

        // Rewrite the file with different content so both halves of the key
        // change, then make sure the modification time really moved: a copy
        // finishing inside the filesystem's timestamp granularity would
        // otherwise leave the key looking unchanged.
        std::fs::copy(get_test_hdt_path("apple.hdt"), &hdt_path)?;
        let bumped = SystemTime::now() + std::time::Duration::from_secs(1);
        fs_set_times(&hdt_path, bumped)?;

        let second = store
            .get_snapshot(None)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert!(
            !Arc::ptr_eq(&first_handle, &second.graphs[0].hdt),
            "a rebuilt package must not be answered from the old handle"
        );
        assert_eq!(
            store.cached_handle_count(),
            1,
            "the stale entry was replaced"
        );
        Ok(())
    }

    /// The cache stays inside its budget by dropping what was used least
    /// recently, and never drops a package the caller is still using.
    #[test]
    #[cfg(feature = "server")]
    fn test_handle_cache_evicts_to_budget() -> anyhow::Result<()> {
        let work_dir = utf8_tempdir()?;
        let first_path = work_dir.path().join("first.hdt");
        let second_path = work_dir.path().join("second.hdt");
        std::fs::copy(get_test_hdt_path("apple.hdt"), &first_path)?;
        std::fs::copy(get_test_hdt_path("apple.hdt"), &second_path)?;
        let first_str = first_path.to_string_lossy().into_owned();
        let second_str = second_path.to_string_lossy().into_owned();

        // Room for one package but not two.
        let store = AggregateHdt::new(&[first_str.clone(), second_str.clone()])?;
        let one_package_bytes = {
            let probe = AggregateHdt::new(std::slice::from_ref(&first_str))?;
            drop(
                probe
                    .get_snapshot(None)
                    .map_err(|e| anyhow::anyhow!("{e}"))?,
            );
            probe.cached_handle_bytes()
        };
        let store = store.with_handle_cache_budget(one_package_bytes);

        let first_graph = store
            .graph_names()?
            .into_iter()
            .find(|name| name.contains("first.hdt"));
        let first_graph = first_graph.expect("first.hdt should be registered");
        let second_graph = store
            .graph_names()?
            .into_iter()
            .find(|name| name.contains("second.hdt"));
        let second_graph = second_graph.expect("second.hdt should be registered");

        drop(
            store
                .get_snapshot(Some(vec![first_graph]))
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        assert_eq!(store.cached_handle_count(), 1);

        drop(
            store
                .get_snapshot(Some(vec![second_graph]))
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        assert_eq!(
            store.cached_handle_count(),
            1,
            "the older package should have been evicted to stay inside the budget"
        );
        assert!(store.cached_handle_bytes() <= one_package_bytes);

        // Both at once: the budget does not apply to the working set of a
        // single snapshot, which is alive through its own Arcs regardless.
        let both = store
            .get_snapshot(None)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(both.graphs.len(), 2);
        assert_eq!(
            store.cached_handle_count(),
            2,
            "packages in use must not be evicted out from under the snapshot"
        );
        Ok(())
    }

    /// A file that leaves the dataset takes its handle with it.
    #[test]
    #[cfg(feature = "server")]
    fn test_sync_drops_handles_for_removed_files() -> anyhow::Result<()> {
        let work_dir = utf8_tempdir()?;
        let sync_dir = work_dir.path().join("sync");
        std::fs::create_dir(&sync_dir)?;
        let kept = sync_dir.join("kept.hdt");
        let removed = sync_dir.join("removed.hdt");
        std::fs::copy(get_test_hdt_path("apple.hdt"), &kept)?;
        std::fs::copy(get_test_hdt_path("apple.hdt"), &removed)?;

        let store = AggregateHdt::new(&[
            kept.to_string_lossy().into_owned(),
            removed.to_string_lossy().into_owned(),
        ])?;
        drop(
            store
                .get_snapshot(None)
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        );
        assert_eq!(store.cached_handle_count(), 2);

        std::fs::remove_file(&removed)?;
        let (added, removed_count) = store.sync(&sync_dir)?;
        assert_eq!(added, 0);
        assert_eq!(removed_count, 1);
        assert_eq!(
            store.cached_handle_count(),
            1,
            "sync should let go of the package that is no longer in the dataset"
        );
        Ok(())
    }

    /// Set a file's modification time, so a test can make a rewrite visible to
    /// the cache key regardless of filesystem timestamp granularity.
    #[cfg(feature = "server")]
    fn fs_set_times(path: &Path, modified: SystemTime) -> anyhow::Result<()> {
        let file = std::fs::File::options().write(true).open(path)?;
        file.set_modified(modified)?;
        Ok(())
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_contains_named_graph_found() {
        // Create an AggregateHDT with test.hdt
        let test_hdt_path = get_test_hdt_path("apple.hdt");
        let store = &AggregateHdt::new(std::slice::from_ref(&test_hdt_path))
            .expect("Failed to create AggregateHDT")
            .get_snapshot(None)
            .expect("msg");

        // Test 1: Graph should be found with file:/// URI scheme matching the filename
        let graph_name = graph_uri_for_path(std::path::Path::new(&test_hdt_path))
            .expect("Failed to build graph URI");
        let graph_name: Arc<str> = Arc::from(graph_name);
        let result = store.contains_internal_graph_name(&graph_name);
        assert!(
            result.is_ok(),
            "contains_named_graph should not return error"
        );
        assert!(
            result.unwrap(),
            "Graph 'file:///test.hdt' should be found in the store"
        );
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_contains_named_graph_not_found() {
        // Create an AggregateHDT with test.hdt
        let test_hdt_path = get_test_hdt_path("apple.hdt");
        let store = &AggregateHdt::new(&[test_hdt_path])
            .expect("Failed to create AggregateHDT")
            .get_snapshot(None)
            .expect("msg");

        // Test 1: Graph with different filename should not be found
        let missing_graph: Arc<str> = Arc::from("file:///nonexistent.hdt");
        let result = store.contains_internal_graph_name(&missing_graph);
        assert!(
            result.is_ok(),
            "contains_named_graph should not return error"
        );
        assert!(
            !result.unwrap(),
            "Graph 'file:///nonexistent.hdt' should not be found"
        );

        // Test 2: Graph with non-file URI scheme should not be found
        let http_graph: Arc<str> = Arc::from("http://example.org/test.hdt");
        let result_http = store.contains_internal_graph_name(&http_graph);
        assert!(
            result_http.is_ok(),
            "contains_named_graph should not return error"
        );
        assert!(
            !result_http.unwrap(),
            "Graph with http:// scheme should not be found (only file:// supported)"
        );

        // Test 3: Graph with different stem should not be found
        let wrong_stem: Arc<str> = Arc::from("file:///different");
        let result_wrong = store.contains_internal_graph_name(&wrong_stem);
        assert!(
            result_wrong.is_ok(),
            "contains_named_graph should not return error"
        );
        assert!(
            !result_wrong.unwrap(),
            "Graph 'file:///different' should not be found"
        );
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_new_with_duplicate_filenames_keeps_graphs_distinct() -> anyhow::Result<()> {
        let fixture_hdt_path = get_test_hdt_path("apple.hdt");
        let work_dir = utf8_tempdir()?;

        let first_dir = work_dir.path().join("first");
        let second_dir = work_dir.path().join("second");
        std::fs::create_dir(&first_dir)?;
        std::fs::create_dir(&second_dir)?;

        let first_hdt = first_dir.join("duplicate.hdt");
        let second_hdt = second_dir.join("duplicate.hdt");
        std::fs::copy(&fixture_hdt_path, &first_hdt)?;
        std::fs::copy(&fixture_hdt_path, &second_hdt)?;

        let store = AggregateHdt::new(&[
            first_hdt.to_string_lossy().into_owned(),
            second_hdt.to_string_lossy().into_owned(),
        ])?;
        let snapshot = store
            .get_snapshot(None)
            .map_err(|err| anyhow::anyhow!("{err}"))?;

        let graph_names: Vec<String> = (&snapshot)
            .internal_named_graphs()
            .map(|graph| graph.map(|name| name.to_string()))
            .collect::<Result<_, _>>()?;

        assert_eq!(
            graph_names.len(),
            2,
            "duplicate basenames should create separate graphs"
        );

        let expected_first = graph_uri_for_path(&first_hdt)?;
        let expected_second = graph_uri_for_path(&second_hdt)?;
        assert_ne!(
            expected_first, expected_second,
            "graph URIs should be unique"
        );
        assert!(graph_names.contains(&expected_first));
        assert!(graph_names.contains(&expected_second));

        Ok(())
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_sync_uses_full_path_graph_uris() -> anyhow::Result<()> {
        let fixture_hdt_path = get_test_hdt_path("apple.hdt");
        let work_dir = utf8_tempdir()?;

        let sync_dir = work_dir.path().join("sync");
        std::fs::create_dir(&sync_dir)?;

        let initial_hdt = sync_dir.join("initial.hdt");
        let synced_hdt = sync_dir.join("synced.hdt");
        std::fs::copy(&fixture_hdt_path, &initial_hdt)?;
        std::fs::copy(&fixture_hdt_path, &synced_hdt)?;

        let initial_hdt_str = initial_hdt.to_string_lossy().into_owned();
        let store = AggregateHdt::new(std::slice::from_ref(&initial_hdt_str))
            .expect("Failed to create AggregateHDT");
        let (added, removed) = store.sync(&sync_dir)?;
        assert_eq!(added, 1);
        assert_eq!(removed, 0);

        let snapshot = store
            .get_snapshot(None)
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        let graph_names: Vec<String> = (&snapshot)
            .internal_named_graphs()
            .map(|graph| graph.map(|name| name.to_string()))
            .collect::<Result<_, _>>()?;

        assert_eq!(
            graph_names.len(),
            2,
            "sync should keep both synced HDT graphs"
        );
        let expected_initial = graph_uri_for_path(&initial_hdt)?;
        let expected_synced = graph_uri_for_path(&synced_hdt)?;
        assert!(graph_names.contains(&expected_initial));
        assert!(graph_names.contains(&expected_synced));

        Ok(())
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_sync_with_relative_location_is_stable_and_canonical() -> anyhow::Result<()> {
        let fixture_hdt_path = get_test_hdt_path("apple.hdt");
        let work_dir = utf8_tempdir()?;

        let sync_dir = work_dir.path().join("sync-relative");
        std::fs::create_dir(&sync_dir)?;

        let hdt_path = sync_dir.join("relative.hdt");
        std::fs::copy(&fixture_hdt_path, &hdt_path)?;
        let hdt_path_str = hdt_path.to_string_lossy().into_owned();
        let store = AggregateHdt::new(std::slice::from_ref(&hdt_path_str))?;

        let cwd = std::env::current_dir()?;
        let relative_sync_dir = sync_dir.strip_prefix(&cwd).map_err(|_| {
            anyhow::anyhow!(
                "test sync directory {sync_dir:?} should be under current directory {cwd:?}"
            )
        })?;

        let (added_first, removed_first) = store.sync(relative_sync_dir)?;
        assert_eq!(added_first, 0);
        assert_eq!(removed_first, 0);

        let (added_second, removed_second) = store.sync(relative_sync_dir)?;
        assert_eq!(added_second, 0);
        assert_eq!(removed_second, 0);

        let expected_graph = graph_uri_for_path(&hdt_path)?;
        let file_paths = lock_read_file_paths(&store.file_paths, "reading graph map")?;
        let actual_entry = file_paths.get(&expected_graph).ok_or_else(|| {
            anyhow::anyhow!("expected graph mapping for {expected_graph} to exist after sync")
        })?;
        assert_eq!(actual_entry, &hdt_path.canonicalize()?);

        Ok(())
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_sync_rejects_explicit_graph_membership_dataset() -> anyhow::Result<()> {
        let fixture_hdt_path = get_test_hdt_path("apple.hdt");
        let store = AggregateHdt::new_with_mappings(
            std::slice::from_ref(&fixture_hdt_path),
            &[(
                "http://example.org/named".to_string(),
                fixture_hdt_path.clone(),
            )],
        )?;

        let err = store
            .sync(&std::env::current_dir()?)
            .expect_err("sync should reject datasets with explicit graph memberships");
        assert!(
            err.to_string().contains("filesystem-discovered datasets"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "server")]
    async fn test_sync_rejects_duplicate_graph_iri_metadata() -> anyhow::Result<()> {
        let work_dir = utf8_tempdir()?;
        let sync_dir = work_dir.path().join("sync-duplicate-iri");
        std::fs::create_dir(&sync_dir)?;

        let nt_a = sync_dir.join("a.nt");
        let nt_b = sync_dir.join("b.nt");
        std::fs::write(
            &nt_a,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;
        std::fs::write(
            &nt_b,
            "<http://example.org/s2> <http://example.org/p> <http://example.org/o2> .\n",
        )?;

        let hdt_a = sync_dir.join("a.hdt");
        let hdt_b = sync_dir.join("b.hdt");
        let shared_graph_iri = "http://example.org/graph/shared";
        crate::create::do_create_with_options(
            Some(&hdt_a.to_string_lossy()),
            &[nt_a.to_string_lossy().into_owned()],
            false,
            Some(shared_graph_iri),
            &[],
            None,
            None,
        )
        .await?;
        crate::create::do_create_with_options(
            Some(&hdt_b.to_string_lossy()),
            &[nt_b.to_string_lossy().into_owned()],
            false,
            Some(shared_graph_iri),
            &[],
            None,
            None,
        )
        .await?;

        let store = AggregateHdt::new(&[hdt_a.to_string_lossy().into_owned()])?;
        let err = store
            .sync(&sync_dir)
            .expect_err("sync should fail on duplicate graph IRI metadata");
        assert!(
            err.to_string().contains("duplicate graph IRI"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_get_snapshot_fails_when_lock_is_poisoned() {
        let test_hdt_path = get_test_hdt_path("apple.hdt");
        let store = &AggregateHdt::new(std::slice::from_ref(&test_hdt_path))
            .expect("Failed to create AggregateHDT");

        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.file_paths.write().unwrap();
            panic!("poison lock");
        }));
        assert!(
            panic_result.is_err(),
            "expected lock poisoning setup to panic"
        );

        let snapshot_result = store.get_snapshot(None);
        assert!(
            snapshot_result.is_err(),
            "snapshot should fail on poisoned lock"
        );
        assert!(
            snapshot_result
                .err()
                .unwrap()
                .to_string()
                .contains("poisoned lock"),
            "expected poisoned lock error"
        );
    }

    #[test]
    fn test_query_rejects_invalid_base_iri_without_panic() {
        let snapshot = AggregateHdtSnapshot::new(Vec::new(), None, None);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            query(
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
                &snapshot,
                Some("://bad-base".to_string()),
            )
        }));
        assert!(result.is_ok(), "query must not panic with invalid base IRI");
        let result = result.expect("query panicked");
        let Err(err) = result else {
            panic!("query should return error for invalid base IRI");
        };
        if let QueryEvaluationError::Unexpected(err) = err {
            assert!(
                err.to_string().contains("IRI") || err.to_string().contains("base"),
                "unexpected parser error: {err}"
            );
        } else {
            panic!("expected unexpected parser error");
        }
    }

    #[test]
    fn test_scoped_blank_node_round_trips_for_same_graph() {
        let graph_name: Arc<str> = Arc::from("file:///tmp/example.hdt");
        let scoped = scoped_blank_node(Arc::from("_:b0"), &graph_name);
        let unscoped = unscoped_pattern_term(&scoped, &graph_name)
            .expect("scoped blank node should round-trip");
        assert_eq!(unscoped.as_ref(), "_:b0");
    }

    #[test]
    fn test_scoped_blank_node_does_not_match_other_graph() {
        let graph_a: Arc<str> = Arc::from("file:///tmp/a.hdt");
        let graph_b: Arc<str> = Arc::from("file:///tmp/b.hdt");
        let scoped = scoped_blank_node(Arc::from("_:b0"), &graph_a);
        assert!(
            unscoped_pattern_term(&scoped, &graph_b).is_none(),
            "scoped blank nodes should stay graph-local"
        );
    }
}
