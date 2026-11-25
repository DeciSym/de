use crate::graph_iri::{insert_graph_mapping, resolve_hdt_graph_path, resolve_named_graph_path};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{
    CancellationToken, InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset,
};
use spargebra::term::{BlankNode, NamedNode, Term};
use spargebra::{Query, SparqlParser};
use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
    iter,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, RwLock},
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
/// Stores file paths only; HDT instances are created per-request for better concurrency.
#[derive(Clone)]
pub struct AggregateHdt {
    // Map graph names (URIs) to file paths on disk
    file_paths: Arc<RwLock<HashMap<String, PathBuf>>>,
    // Optional explicit default graph membership. If None, all loaded graphs are used as default.
    default_graphs: Option<HashSet<String>>,
    // Optional explicit named graph membership. If None, all loaded graphs are named.
    named_graphs: Option<HashSet<String>>,
}

pub struct AggregateHdtSnapshot {
    // Map graph names (URIs) to HDT instances. Each value can be either the
    // hybrid (mmap+cache) or fully in-memory backend; HdtAny picks the right
    // one per file (see hdt::HdtAny::open).
    pub hdts: HashMap<String, hdt::HdtAny>,
    // Optional explicit default graph membership. If None, all loaded graphs are used as default.
    pub default_graphs: Option<HashSet<String>>,
    // Optional explicit named graph membership. If None, all loaded graphs are named.
    pub named_graphs: Option<HashSet<String>>,
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
            default_graphs: Some(default_graphs),
            named_graphs: Some(named_graph_set),
        })
    }

    /// Create a snapshot of HDT instances for querying.
    ///
    /// # Arguments
    /// * `named_graphs` - Optional filter to only load specific named graphs.
    ///   If None, all available graphs are loaded.
    ///   If Some(vec), only graphs in the vec are loaded.
    ///
    /// # Performance
    /// Filtering graphs before loading can significantly reduce memory usage and load time
    /// when you only need to query a subset of available graphs.
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

        // Load filtered HDTs in parallel. HdtAny::open prefers the hybrid
        // (mmap+cache) backend but falls back to in-memory for files where
        // the hybrid cache cannot be built (e.g. zero-triple HDTs that the
        // wavelet-tree library refuses to index).
        let hdts: HashMap<String, hdt::HdtAny> = paths_vec
            .par_iter()
            .map(
                |(graph_name, path)| -> anyhow::Result<(String, hdt::HdtAny)> {
                    let hdt = hdt::HdtAny::open_with_threshold(path, None).map_err(|e| {
                        anyhow::anyhow!("Failed to load HDT from {}: {e}", path.display())
                    })?;
                    Ok((graph_name.clone(), hdt))
                },
            )
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .collect();

        Ok(AggregateHdtSnapshot {
            hdts,
            default_graphs: self.default_graphs.clone(),
            named_graphs: self.named_graphs.clone(),
        })
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
        self.sync_files(location, false)
    }

    /// [`Self::sync`], with the `.hdt` files in `location`'s subdirectories
    /// too, at any depth.
    #[cfg(feature = "server")]
    pub fn sync_recursive(
        &self,
        location: &std::path::Path,
    ) -> Result<(usize, usize), anyhow::Error> {
        self.sync_files(location, true)
    }

    #[cfg(feature = "server")]
    fn sync_files(
        &self,
        location: &std::path::Path,
        recursive: bool,
    ) -> Result<(usize, usize), anyhow::Error> {
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
        for path in hdt_files(&canonical_location, recursive)? {
            let resolved = resolve_hdt_graph_path(&path)?;
            current_files.insert(resolved.canonical_path().to_path_buf());
            insert_graph_mapping(&mut discovered_mappings, resolved, "duplicate graph IRI")?;
        }

        let mut file_paths = lock_write_file_paths(&self.file_paths, "modifying HDT paths")?;
        // Build set of existing paths for comparison
        let existing_paths: HashSet<PathBuf> = file_paths.values().cloned().collect();
        let added = current_files.difference(&existing_paths).count();
        let removed = existing_paths.difference(&current_files).count();
        *file_paths = discovered_mappings;

        Ok((added, removed))
    }
}

/// The `.hdt` files in `location`, and in its subdirectories at any depth
/// when `recursive`, sorted. Symbolic links to directories are followed, but
/// a directory is read once, so a link back up the tree ends there.
#[cfg(feature = "server")]
pub fn hdt_files(location: &Path, recursive: bool) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    let mut pending = vec![location.to_path_buf()];
    while let Some(dir) = pending.pop() {
        if !seen.insert(dir.canonicalize()?) {
            continue;
        }
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                if recursive {
                    pending.push(path);
                }
            } else if path.is_file()
                && path
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("hdt"))
            {
                files.push(path);
            }
        }
    }
    files.sort_unstable();
    Ok(files)
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

/// Streams the quads that match a pattern, one graph after another.
///
/// The default graph is the merge of the graphs it is made of, and an RDF graph
/// is a set: a triple that two files both hold is in it once. So when it reads
/// the default graph, the iterator skips a triple that a graph it has already
/// read holds, since that graph returned it. To check, it looks the triple up
/// in each such graph, rather than remembering what it returned, so memory
/// stays flat however many triples match.
struct StreamingInternalQuadIter<'a> {
    graphs: Vec<(&'a String, &'a hdt::HdtAny)>,
    subject: Option<Arc<str>>,
    predicate: Option<Arc<str>>,
    object: Option<Arc<str>>,
    emit_default_graph: bool,
    current_graph: usize,
    current_iter: Option<GraphTriples<'a>>,
    // When reading the default graph from more than one graph: the graphs read
    // so far that hold triples matching the pattern.
    read_graphs: Vec<ReadGraph<'a>>,
}

/// The triples of one graph that match the pattern, before blank node scoping.
struct GraphTriples<'a> {
    graph_name: Arc<str>,
    triples: Box<dyn Iterator<Item = StringTriple> + 'a>,
    graph: ReadGraph<'a>,
}

/// A graph, with the constants of the pattern resolved against its dictionary.
struct ReadGraph<'a> {
    hdt: &'a hdt::HdtAny,
    // The dictionary IDs of the constants, by position: 0 where the pattern has
    // a variable.
    ids: [usize; 3],
    // The term last looked up in each position the pattern leaves open, with
    // its ID, 0 if this graph doesn't have it there. Consecutive triples often
    // share a term: a scan returns the triples of a subject together.
    last: [Option<(Arc<str>, usize)>; 3],
}

impl<'a> ReadGraph<'a> {
    fn new(hdt: &'a hdt::HdtAny, ids: [usize; 3]) -> Self {
        Self {
            hdt,
            ids,
            last: [None, None, None],
        }
    }

    /// Whether this graph holds `triple`, which matches the pattern.
    fn holds(&mut self, triple: &StringTriple) -> bool {
        let mut ids = self.ids;
        // Settle the terms that were just looked up first, since one this graph
        // doesn't have rules the triple out for free.
        for (position, last) in self.last.iter().enumerate() {
            if ids[position] == 0
                && let Some((term, id)) = last
                && *term == triple[position]
            {
                if *id == 0 {
                    return false;
                }
                ids[position] = *id;
            }
        }
        // Then look up the others, the predicate first: its dictionary is the
        // smallest, and it is the term that is missing when files are split by
        // predicate.
        for position in [1, 0, 2] {
            if ids[position] == 0 {
                let term = &triple[position];
                let id = self.hdt.string_to_id(term, hdt::IdKind::KINDS[position]);
                self.last[position] = Some((term.clone(), id));
                if id == 0 {
                    return false;
                }
                ids[position] = id;
            }
        }
        contains_triple_ids(self.hdt, ids)
    }
}

/// Whether one of `graphs` holds `triple`.
fn held_by_any(graphs: &mut [ReadGraph<'_>], triple: &StringTriple) -> bool {
    let [subject, _, object] = triple;
    // A blank node is scoped to its graph (see `scoped_blank_node`), so no
    // other graph holds a triple that has one.
    if subject.starts_with("_:") || object.starts_with("_:") {
        return false;
    }
    graphs.iter_mut().any(|graph| graph.holds(triple))
}

/// Whether `hdt` holds the triple with these dictionary IDs.
fn contains_triple_ids(hdt: &hdt::HdtAny, ids: [usize; 3]) -> bool {
    use hdt::triples::SubjectIter;
    match hdt {
        hdt::HdtAny::Hybrid(h) => SubjectIter::with_pattern(&h.triples, ids).next().is_some(),
        hdt::HdtAny::InMemory(h) => SubjectIter::with_pattern(&h.triples, ids).next().is_some(),
    }
}

type StringTriple = [Arc<str>; 3];
/// The terms of a pattern by position, each constant with its ID in one HDT's
/// dictionary, and `None` for a variable.
type ResolvedPattern = [Option<(Arc<str>, usize)>; 3];

/// Resolves the constants of a pattern against the dictionary of `hdt`.
/// Returns `None` if one of them is not in it, in its position: then no triple
/// of `hdt` matches.
fn resolve_pattern(hdt: &hdt::HdtAny, terms: [Option<Arc<str>>; 3]) -> Option<ResolvedPattern> {
    let mut pattern: ResolvedPattern = [None, None, None];
    for ((term, kind), resolved) in terms.into_iter().zip(hdt::IdKind::KINDS).zip(&mut pattern) {
        if let Some(term) = term {
            let id = hdt.string_to_id(&term, kind);
            if id == 0 {
                return None;
            }
            *resolved = Some((term, id));
        }
    }
    Some(pattern)
}

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

// Upstream's `hdt::HdtGeneric::triples_with_pattern` does what this function
// does, but from strings. It looks the constants up itself, while
// `StreamingInternalQuadIter` needs their IDs too, to look triples up in the
// graphs it has read. And its signature,
// `triples_with_pattern<'a>(&'a self, sp: Option<&'a str>, ...)`, ties the
// input `&str` lifetime to the returned iterator's, which our filter terms
// (computed locally in `StreamingInternalQuadIter::ensure_current_iter` after
// blank-node unscoping) don't outlive. TODO: switch to upstream if it gains a
// variant that takes resolved IDs, which would also pick up its `TripleCache`
// for repeated id->string lookups.
fn indexed_triples_with_pattern(
    hdt: &hdt::HdtAny,
    pattern: ResolvedPattern,
) -> Box<dyn Iterator<Item = StringTriple> + '_> {
    match hdt {
        hdt::HdtAny::Hybrid(h) => indexed_triples_with_pattern_generic(h, pattern),
        hdt::HdtAny::InMemory(h) => indexed_triples_with_pattern_generic(h, pattern),
    }
}

fn indexed_triples_with_pattern_generic<'a, D, S, B>(
    hdt: &'a hdt::HdtGeneric<D, S, B>,
    pattern: ResolvedPattern,
) -> Box<dyn Iterator<Item = StringTriple> + 'a>
where
    D: hdt::DictSectPfcAccess + 'a,
    S: hdt::containers::SequenceAccess + 'a,
    B: hdt::containers::BitmapAccess + 'a,
{
    use hdt::IdKind;
    use hdt::triples::{ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter};

    match pattern {
        [Some(s), Some(p), Some(o)] => {
            if SubjectIter::with_pattern(&hdt.triples, [s.1, p.1, o.1])
                .next()
                .is_some()
            {
                Box::new(iter::once([s.0, p.0, o.0]))
            } else {
                Box::new(iter::empty())
            }
        }
        [Some(s), Some(p), None] => Box::new(
            SubjectIter::with_pattern(&hdt.triples, [s.1, p.1, 0]).map(move |triple| {
                [
                    s.0.clone(),
                    p.0.clone(),
                    Arc::from(hdt.dict.id_to_string(triple[2], IdKind::Object).unwrap()),
                ]
            }),
        ),
        [Some(s), None, Some(o)] => Box::new(
            SubjectIter::with_pattern(&hdt.triples, [s.1, 0, o.1]).map(move |triple| {
                [
                    s.0.clone(),
                    Arc::from(hdt.dict.id_to_string(triple[1], IdKind::Predicate).unwrap()),
                    o.0.clone(),
                ]
            }),
        ),
        [Some(s), None, None] => Box::new(
            SubjectIter::with_pattern(&hdt.triples, [s.1, 0, 0]).map(move |triple| {
                [
                    s.0.clone(),
                    Arc::from(hdt.dict.id_to_string(triple[1], IdKind::Predicate).unwrap()),
                    Arc::from(hdt.dict.id_to_string(triple[2], IdKind::Object).unwrap()),
                ]
            }),
        ),
        [None, Some(p), Some(o)] => Box::new(PredicateObjectIter::new(&hdt.triples, p.1, o.1).map(
            move |subject_id| {
                [
                    Arc::from(hdt.dict.id_to_string(subject_id, IdKind::Subject).unwrap()),
                    p.0.clone(),
                    o.0.clone(),
                ]
            },
        )),
        [None, Some(p), None] => {
            Box::new(PredicateIter::new(&hdt.triples, p.1).map(move |triple| {
                [
                    Arc::from(hdt.dict.id_to_string(triple[0], IdKind::Subject).unwrap()),
                    p.0.clone(),
                    Arc::from(hdt.dict.id_to_string(triple[2], IdKind::Object).unwrap()),
                ]
            }))
        }
        [None, None, Some(o)] => Box::new(ObjectIter::new(&hdt.triples, o.1).map(move |triple| {
            [
                Arc::from(hdt.dict.id_to_string(triple[0], IdKind::Subject).unwrap()),
                Arc::from(hdt.dict.id_to_string(triple[1], IdKind::Predicate).unwrap()),
                o.0.clone(),
            ]
        })),
        [None, None, None] => Box::new(hdt.triples_all()),
    }
}

impl<'a> StreamingInternalQuadIter<'a> {
    /// Reads `graphs` in the order given. With `emit_default_graph`, they make
    /// up the default graph; otherwise each quad names the graph it is from.
    fn new(
        graphs: Vec<(&'a String, &'a hdt::HdtAny)>,
        [subject, predicate, object]: [Option<Arc<str>>; 3],
        emit_default_graph: bool,
    ) -> Self {
        Self {
            graphs,
            subject,
            predicate,
            object,
            emit_default_graph,
            current_graph: 0,
            current_iter: None,
            read_graphs: Vec::new(),
        }
    }

    fn ensure_current_iter(&mut self) -> bool {
        while self.current_iter.is_none() && self.current_graph < self.graphs.len() {
            let (graph_name, hdt) = self.graphs[self.current_graph];
            self.current_graph += 1;
            let graph_name: Arc<str> = Arc::from(graph_name.as_str());
            let subject_filter = match self.subject.as_ref() {
                Some(term) => match unscoped_pattern_term(term, &graph_name) {
                    Some(term) => Some(term),
                    None => continue,
                },
                None => None,
            };
            let predicate_filter = match self.predicate.as_ref() {
                Some(term) => match unscoped_pattern_term(term, &graph_name) {
                    Some(term) => Some(term),
                    None => continue,
                },
                None => None,
            };
            let object_filter = match self.object.as_ref() {
                Some(term) => match unscoped_pattern_term(term, &graph_name) {
                    Some(term) => Some(term),
                    None => continue,
                },
                None => None,
            };
            let Some(pattern) =
                resolve_pattern(hdt, [subject_filter, predicate_filter, object_filter])
            else {
                continue;
            };
            let ids = pattern
                .each_ref()
                .map(|term| term.as_ref().map_or(0, |(_, id)| *id));

            self.current_iter = Some(GraphTriples {
                graph_name,
                triples: indexed_triples_with_pattern(hdt, pattern),
                graph: ReadGraph::new(hdt, ids),
            });
        }
        self.current_iter.is_some()
    }

    /// Whether the triples of the graphs read so far are skipped when a later
    /// graph holds them too.
    fn dedups_default_graph(&self) -> bool {
        self.emit_default_graph && self.graphs.len() > 1
    }
}

impl Iterator for StreamingInternalQuadIter<'_> {
    type Item = Result<InternalQuad<Arc<str>>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.ensure_current_iter() {
            let current = self.current_iter.as_mut()?;
            let Some(triple) = current.triples.next() else {
                let finished = self.current_iter.take()?;
                if self.dedups_default_graph() && self.current_graph < self.graphs.len() {
                    self.read_graphs.push(finished.graph);
                }
                continue;
            };
            // Only the default graph keeps read graphs; see `dedups_default_graph`.
            if !self.read_graphs.is_empty() && held_by_any(&mut self.read_graphs, &triple) {
                continue;
            }
            let [subject, predicate, object] = triple;
            return Some(Ok(InternalQuad {
                subject: scoped_blank_node(subject, &current.graph_name),
                predicate,
                object: scoped_blank_node(object, &current.graph_name),
                graph_name: (!self.emit_default_graph).then(|| current.graph_name.clone()),
            }));
        }
        None
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
        let graph_name_owned = graph_name.map(|inner: Option<&Arc<str>>| inner.cloned());
        let emit_default_graph = matches!(graph_name_owned, Some(None));

        // Optimization: Pre-filter graphs to reduce unnecessary work
        // Note: get_snapshot() already filtered graphs at load time,
        // so self.hdts contains only the required graphs. This filter
        // handles additional runtime graph name matching from the query.
        let graphs_to_query: Vec<(&String, &hdt::HdtAny)> = self
            .hdts
            .iter()
            .filter(|(g, _h)| {
                match &graph_name_owned {
                    // Query for default graph: Some(None)
                    // Default graph is explicit when configured, else union of all loaded graphs
                    Some(None) => self
                        .default_graphs
                        .as_ref()
                        .is_none_or(|defaults| defaults.contains(g.as_str())),
                    // Query for specific named graph: Some(Some(graph))
                    Some(Some(target_graph)) => {
                        g.as_str() == target_graph.as_ref()
                            && self
                                .named_graphs
                                .as_ref()
                                .is_none_or(|named| named.contains(g.as_str()))
                    }
                    // Query across all graphs: None
                    None => self
                        .named_graphs
                        .as_ref()
                        .is_none_or(|named| named.contains(g.as_str())),
                }
            })
            .collect();

        StreamingInternalQuadIter::new(
            graphs_to_query,
            [subject.cloned(), predicate.cloned(), object.cloned()],
            emit_default_graph,
        )
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
        let keys: Vec<Arc<str>> = match &self.named_graphs {
            Some(named) => self
                .hdts
                .keys()
                .filter(|k| named.contains(k.as_str()))
                .map(|k| Arc::from(k.as_str()))
                .collect(),
            None => self.hdts.keys().map(|k| Arc::from(k.as_str())).collect(),
        };
        keys.into_iter().map(Ok)
    }

    fn contains_internal_graph_name(&self, graph_name: &Arc<str>) -> Result<bool, Self::Error> {
        Ok(self.hdts.contains_key(graph_name.as_ref())
            && self
                .named_graphs
                .as_ref()
                .is_none_or(|named| named.contains(graph_name.as_ref())))
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

/// Evaluates `parsed` over `dataset`, stopping with
/// [`QueryEvaluationError::Cancelled`] once `cancellation_token` is cancelled.
pub fn query_parsed_with_cancellation<'a, D>(
    parsed: &Query,
    dataset: D,
    cancellation_token: CancellationToken,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError>
where
    D: QueryableDataset<'a>,
{
    evaluator()
        .with_cancellation_token(cancellation_token)
        .prepare(parsed)
        .execute(dataset)
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
    let evaluator = evaluator();
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

/// The evaluator every query runs with.
fn evaluator() -> QueryEvaluator {
    // NOTE: previously this ran .without_optimizations() in order to pass W3C suites, but is no
    // longer needed with current spareval revision. With optimizations, query responses are
    // significantly reduced.
    // TODO: monitor for optimizer-specific regressions, perhaps introducing toggle
    QueryEvaluator::new()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(feature = "server")]
    use super::*;
    #[cfg(not(feature = "server"))]
    use super::{
        AggregateHdt, AggregateHdtSnapshot, QueryEvaluationError, StreamingInternalQuadIter,
        quads_for_pattern_strings, query, scoped_blank_node, unscoped_pattern_term,
    };
    #[cfg(feature = "server")]
    use spareval::QueryableDataset;
    use std::collections::HashSet;

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
        let snapshot = AggregateHdtSnapshot {
            hdts: std::collections::HashMap::new(),
            default_graphs: None,
            named_graphs: None,
        };
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

    /// Two files that share two ground triples and the blank node label `_:x`.
    /// Each also holds a triple whose terms the other file has too, in the same
    /// positions, so only looking the whole triple up shows it isn't shared,
    /// and two triples of a subject the other file doesn't have, which a scan
    /// reads one after the other.
    const FILE_A: &str = r#"<http://ex/alice> <http://ex/knows> <http://ex/bob> .
<http://ex/alice> <http://ex/name> "Alice" .
<http://ex/bob> <http://ex/knows> <http://ex/carol> .
<http://ex/erin> <http://ex/knows> <http://ex/bob> .
<http://ex/erin> <http://ex/name> "Erin" .
_:x <http://ex/label> "blank" .
"#;
    const FILE_B: &str = r#"<http://ex/alice> <http://ex/knows> <http://ex/bob> .
<http://ex/alice> <http://ex/name> "Alice" .
<http://ex/alice> <http://ex/knows> <http://ex/carol> .
<http://ex/bob> <http://ex/name> "Bob" .
<http://ex/dave> <http://ex/knows> <http://ex/alice> .
<http://ex/dave> <http://ex/name> "Dave" .
_:x <http://ex/label> "blank" .
"#;

    /// The default graph of `FILE_A` and `FILE_B`, as HDT strings: the shared
    /// triples once, and the `_:x` triple twice, since each file's `_:x` is a
    /// blank node of its own.
    const MERGED: [[&str; 3]; 11] = [
        ["http://ex/alice", "http://ex/knows", "http://ex/bob"],
        ["http://ex/alice", "http://ex/name", "\"Alice\""],
        ["http://ex/bob", "http://ex/knows", "http://ex/carol"],
        ["http://ex/erin", "http://ex/knows", "http://ex/bob"],
        ["http://ex/erin", "http://ex/name", "\"Erin\""],
        ["http://ex/alice", "http://ex/knows", "http://ex/carol"],
        ["http://ex/bob", "http://ex/name", "\"Bob\""],
        ["http://ex/dave", "http://ex/knows", "http://ex/alice"],
        ["http://ex/dave", "http://ex/name", "\"Dave\""],
        ["_:x", "http://ex/label", "\"blank\""],
        ["_:x", "http://ex/label", "\"blank\""],
    ];

    /// A pattern of each shape, then ones with a constant that only one file
    /// has, or neither.
    const PATTERNS: [[Option<&str>; 3]; 12] = [
        [None, None, None],
        [Some("http://ex/alice"), None, None],
        [None, Some("http://ex/knows"), None],
        [None, None, Some("http://ex/carol")],
        [Some("http://ex/alice"), Some("http://ex/knows"), None],
        [Some("http://ex/alice"), None, Some("http://ex/bob")],
        [None, Some("http://ex/knows"), Some("http://ex/bob")],
        [
            Some("http://ex/alice"),
            Some("http://ex/knows"),
            Some("http://ex/bob"),
        ],
        [Some("http://ex/bob"), Some("http://ex/name"), None],
        [None, None, Some("\"Bob\"")],
        [None, Some("http://ex/label"), None],
        [Some("http://ex/carol"), None, None],
    ];

    /// Converts `ntriples` to `<dir>/<name>.hdt`.
    fn hdt_from_ntriples(
        dir: &std::path::Path,
        name: &str,
        ntriples: &str,
    ) -> anyhow::Result<std::path::PathBuf> {
        let nt = dir.join(format!("{name}.nt"));
        let hdt = dir.join(format!("{name}.hdt"));
        std::fs::write(&nt, ntriples)?;
        crate::create::nt_file_to_hdt(&nt, &hdt, false)?;
        Ok(hdt)
    }

    /// The triples of `MERGED` that match `pattern`, sorted.
    fn merged_matches(pattern: [Option<&str>; 3]) -> Vec<[String; 3]> {
        let mut matches: Vec<[String; 3]> = MERGED
            .iter()
            .filter(|triple| {
                triple
                    .iter()
                    .zip(pattern)
                    .all(|(term, constant)| constant.is_none_or(|constant| constant == *term))
            })
            .map(|triple| triple.map(String::from))
            .collect();
        matches.sort();
        matches
    }

    /// The triples that match `pattern` in the default graph made of `graphs`,
    /// read in that order, sorted. Blank nodes come back as `_:x`, once it is
    /// checked that no two triples have the same one: each file's `_:x` is in
    /// one triple, so two would mean the files' blank nodes were merged.
    fn default_graph_matches(
        graphs: &[(&String, &hdt::HdtAny)],
        pattern: [Option<&str>; 3],
    ) -> anyhow::Result<Vec<[String; 3]>> {
        let mut blank_nodes = HashSet::new();
        let mut matches = Vec::new();
        let terms = pattern.map(|term| term.map(Arc::from));
        for quad in StreamingInternalQuadIter::new(graphs.to_vec(), terms, true) {
            let quad = quad?;
            assert!(quad.graph_name.is_none(), "the default graph has no name");
            let mut triple =
                [quad.subject, quad.predicate, quad.object].map(|term| term.to_string());
            if triple[0].starts_with("_:") {
                assert!(
                    blank_nodes.insert(triple[0].clone()),
                    "two triples have the blank node {}",
                    triple[0]
                );
                triple[0] = "_:x".to_string();
            }
            matches.push(triple);
        }
        matches.sort();
        Ok(matches)
    }

    /// The values `variable` takes in the solutions of `sparql`, sorted.
    fn select_values(
        snapshot: &AggregateHdtSnapshot,
        sparql: &str,
        variable: &str,
    ) -> anyhow::Result<Vec<String>> {
        let spareval::QueryResults::Solutions(solutions) = query(sparql, snapshot, None)? else {
            anyhow::bail!("expected solutions for {sparql}");
        };
        let mut values = Vec::new();
        for solution in solutions {
            values.push(
                solution?
                    .get(variable)
                    .map_or_else(String::new, ToString::to_string),
            );
        }
        values.sort();
        Ok(values)
    }

    /// The default graph is the merge of its files, and an RDF graph is a set,
    /// so a triple two files share is in it once: whichever file is read
    /// first, and whichever backend each is opened with.
    #[test]
    fn test_default_graph_holds_a_shared_triple_once() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let a = hdt_from_ntriples(dir.path(), "a", FILE_A)?;
        let b = hdt_from_ntriples(dir.path(), "b", FILE_B)?;
        let (name_a, name_b) = ("file:///a.hdt".to_string(), "file:///b.hdt".to_string());
        // A threshold of 0 opens the hybrid backend, and usize::MAX the
        // in-memory one.
        for (threshold_a, threshold_b) in [(0, 0), (usize::MAX, usize::MAX), (0, usize::MAX)] {
            let hdt_a = hdt::HdtAny::open_with_threshold(&a, Some(threshold_a))?;
            let hdt_b = hdt::HdtAny::open_with_threshold(&b, Some(threshold_b))?;
            assert_eq!(
                (hdt_a.is_hybrid(), hdt_b.is_hybrid()),
                (threshold_a == 0, threshold_b == 0)
            );
            for graphs in [
                [(&name_a, &hdt_a), (&name_b, &hdt_b)],
                [(&name_b, &hdt_b), (&name_a, &hdt_a)],
            ] {
                for pattern in PATTERNS {
                    assert_eq!(
                        default_graph_matches(&graphs, pattern)?,
                        merged_matches(pattern),
                        "pattern {pattern:?} reading {} first, hybrid {}/{}",
                        graphs[0].0,
                        hdt_a.is_hybrid(),
                        hdt_b.is_hybrid()
                    );
                }
            }
        }
        Ok(())
    }

    /// Named graphs aren't merged: `GRAPH ?g` finds a shared triple in each.
    #[test]
    fn test_named_graphs_each_hold_a_shared_triple() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = [("a", FILE_A), ("b", FILE_B)]
            .map(|(name, ntriples)| hdt_from_ntriples(dir.path(), name, ntriples));
        let paths = paths
            .into_iter()
            .map(|path| Ok(path?.to_string_lossy().into_owned()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let snapshot = AggregateHdt::new(&paths)?.get_snapshot(None)?;
        let (s, p, o) = (
            Some("http://ex/alice"),
            Some("http://ex/knows"),
            Some("http://ex/bob"),
        );

        let named = quads_for_pattern_strings(&snapshot, s, p, o, None)?;
        let graph_names: HashSet<_> = named.iter().map(|quad| &quad.graph_name).collect();
        assert_eq!((named.len(), graph_names.len()), (2, 2), "{named:?}");

        let default = quads_for_pattern_strings(&snapshot, s, p, o, Some(None))?;
        assert_eq!(default.len(), 1, "{default:?}");
        Ok(())
    }

    /// Through SPARQL, as `de query` builds the dataset: a shared triple counts
    /// once, and a join through it doesn't double what it reaches.
    #[test]
    fn test_query_counts_a_shared_triple_once() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut paths = Vec::new();
        for (name, ntriples) in [("a", FILE_A), ("b", FILE_B)] {
            let path = hdt_from_ntriples(dir.path(), name, ntriples)?;
            paths.push(path.to_string_lossy().into_owned());
        }
        let snapshot = AggregateHdt::new_with_mappings(&paths, &[])?.get_snapshot(None)?;

        assert_eq!(
            select_values(&snapshot, "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", "n")?,
            [r#""11"^^<http://www.w3.org/2001/XMLSchema#integer>"#]
        );
        assert_eq!(
            select_values(
                &snapshot,
                "SELECT ?o WHERE { <http://ex/alice> <http://ex/knows> ?x . ?x <http://ex/knows> ?o }",
                "o"
            )?,
            ["<http://ex/carol>"]
        );
        Ok(())
    }
}
