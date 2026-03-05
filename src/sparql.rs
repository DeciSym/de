use crate::graph_iri::{insert_graph_mapping, resolve_hdt_graph_path, resolve_named_graph_path};
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::term::{BlankNode, NamedNode, Term};
use spargebra::{Query, SparqlParser};
use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
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
    // Map graph names (URIs) to HDT instances
    pub hdts: HashMap<String, hdt::hdt::HdtHybrid>,
    // Optional explicit default graph membership. If None, all loaded graphs are used as default.
    pub default_graphs: Option<HashSet<String>>,
    // Optional explicit named graph membership. If None, all loaded graphs are named.
    pub named_graphs: Option<HashSet<String>>,
}

impl AggregateHdt {
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

        // Load filtered HDTs in parallel
        let hdts: HashMap<String, hdt::hdt::HdtHybrid> = paths_vec
            .par_iter()
            .map(
                |(graph_name, path)| -> anyhow::Result<(String, hdt::hdt::HdtHybrid)> {
                    let hdt = hdt::Hdt::new_hybrid_cache(path, true)
                        .map_err(|e| anyhow::anyhow!("failed to load HDT from {:?}: {e}", path))?;
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

    /// Sync the AggregateHdt with the current HDT files in the specified location.
    /// This method refreshes mappings by re-scanning HDT files in the location.
    ///
    /// Returns a tuple of (added_count, removed_count).
    #[cfg(feature = "server")]
    pub fn sync(&self, location: std::path::PathBuf) -> Result<(usize, usize), anyhow::Error> {
        use std::collections::HashSet;

        if self.default_graphs.is_some() || self.named_graphs.is_some() {
            return Err(anyhow::anyhow!(
                "sync is only supported for filesystem-discovered datasets"
            ));
        }

        let canonical_location = location.canonicalize().map_err(|e| {
            anyhow::anyhow!("Failed to canonicalize sync location {:?}: {e}", location)
        })?;
        if !canonical_location.is_dir() {
            return Err(anyhow::anyhow!(
                "Sync location is not a directory: {:?}",
                location
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

        let mut file_paths = lock_write_file_paths(&self.file_paths, "modifying HDT paths")?;
        // Build set of existing paths for comparison
        let existing_paths: HashSet<PathBuf> = file_paths.values().cloned().collect();
        let added = current_files.difference(&existing_paths).count();
        let removed = existing_paths.difference(&current_files).count();
        *file_paths = discovered_mappings;

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

/// Convert triple string formats from OxRDF to HDT.
pub fn term_to_hdt_bgp_str(term: Term) -> String {
    match term {
        Term::NamedNode(named_node) => named_node.into_string(),
        Term::Literal(literal) => literal.to_string(),
        Term::BlankNode(s) => s.to_string(),
    }
}

struct StreamingInternalQuadIter<'a> {
    graphs: Vec<(&'a String, &'a hdt::hdt::HdtHybrid)>,
    subject: Option<Arc<str>>,
    predicate: Option<Arc<str>>,
    object: Option<Arc<str>>,
    emit_default_graph: bool,
    current_graph: usize,
    current_iter: Option<StreamingInternalQuadIterator<'a>>,
}

type StreamingInternalQuadIterator<'a> =
    Box<dyn Iterator<Item = Result<InternalQuad<Arc<str>>, Error>> + 'a>;

fn scoped_blank_node(term: Arc<str>, graph_name: &Arc<str>) -> Arc<str> {
    if !term.starts_with("_:") {
        return term;
    }
    let mut graph_hex = String::with_capacity(graph_name.len() * 2);
    for b in graph_name.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut graph_hex, "{b:02x}");
    }
    let original = &term[2..];
    Arc::from(format!("_:g{graph_hex}_{original}"))
}

impl<'a> StreamingInternalQuadIter<'a> {
    fn ensure_current_iter(&mut self) -> bool {
        while self.current_iter.is_none() && self.current_graph < self.graphs.len() {
            let (graph_name, hdt) = self.graphs[self.current_graph];
            self.current_graph += 1;
            let graph_name: Arc<str> = Arc::from(graph_name.as_str());
            let subject_filter = self.subject.clone();
            let predicate_filter = self.predicate.clone();
            let object_filter = self.object.clone();
            let emit_default_graph = self.emit_default_graph;

            self.current_iter = Some(Box::new(
                hdt.triples_all()
                    .filter(move |[subject, predicate, object]| {
                        subject_filter
                            .as_ref()
                            .is_none_or(|s| s.as_ref() == subject.as_ref())
                            && predicate_filter
                                .as_ref()
                                .is_none_or(|p| p.as_ref() == predicate.as_ref())
                            && object_filter
                                .as_ref()
                                .is_none_or(|o| o.as_ref() == object.as_ref())
                    })
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

impl<'a> Iterator for StreamingInternalQuadIter<'a> {
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
        let graph_name_owned = graph_name.map(|inner| inner.cloned());
        let emit_default_graph = matches!(graph_name_owned, Some(None));

        // Optimization: Pre-filter graphs to reduce unnecessary work
        // Note: get_snapshot() already filtered graphs at load time,
        // so self.hdts contains only the required graphs. This filter
        // handles additional runtime graph name matching from the query.
        let graphs_to_query: Vec<(&String, &hdt::hdt::HdtHybrid)> = self
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

        let subject = subject.cloned();
        let predicate = predicate.cloned();
        let object = object.cloned();

        StreamingInternalQuadIter::<'a> {
            graphs: graphs_to_query,
            subject,
            predicate,
            object,
            emit_default_graph,
            current_graph: 0,
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

pub fn query<'a>(
    q: &str,
    hdt: &'a AggregateHdtSnapshot,
    base_iri: Option<String>,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    query_with_debug_plan(q, hdt, base_iri, false)
}

pub fn parse_query(q: &str, base_iri: &str) -> Result<Query, QueryEvaluationError> {
    Ok(SparqlParser::new()
        .with_base_iri(base_iri.to_string())
        .map_err(|e| QueryEvaluationError::Unexpected(Box::new(e)))?
        .parse_query(q)?)
}

pub fn query_parsed_with_debug_plan<'a>(
    parsed: Query,
    hdt: &'a AggregateHdtSnapshot,
    debug_plan: bool,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    evaluate_query_with_debug_plan(parsed, hdt, debug_plan)
}

pub fn query_with_debug_plan<'a>(
    q: &str,
    hdt: &'a AggregateHdtSnapshot,
    base_iri: Option<String>,
    debug_plan: bool,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    let base = base_iri.unwrap_or_else(|| "http://example.com/".to_string());
    let parsed = parse_query(q, &base)?;
    evaluate_query_with_debug_plan(parsed, hdt, debug_plan)
}

fn evaluate_query_with_debug_plan<'a>(
    parsed: Query,
    hdt: &'a AggregateHdtSnapshot,
    debug_plan: bool,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    // Keep optimizer disabled for all execution paths: this matches current upstream patch behavior
    // used to pass W3C suites in this repository and avoids optimizer-specific regressions.
    let evaluator = QueryEvaluator::new().without_optimizations();
    if debug_plan {
        let (results, explanation) = evaluator.prepare(&parsed).explain(hdt);
        let mut json = Vec::new();
        explanation
            .write_in_json(&mut json)
            .map_err(|e| QueryEvaluationError::Unexpected(Box::new(e)))?;
        eprintln!("{}", String::from_utf8_lossy(&json));
        results
    } else {
        evaluator.prepare(&parsed).execute(hdt)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "server")]
    use super::*;
    #[cfg(not(feature = "server"))]
    use super::{AggregateHdtSnapshot, QueryEvaluationError, query};
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
            .map_err(|err| anyhow::anyhow!("{}", err))?;

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
        let (added, removed) = store.sync(sync_dir.clone())?;
        assert_eq!(added, 1);
        assert_eq!(removed, 0);

        let snapshot = store
            .get_snapshot(None)
            .map_err(|err| anyhow::anyhow!("{}", err))?;
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
                "test sync directory {:?} should be under current directory {:?}",
                sync_dir,
                cwd
            )
        })?;

        let (added_first, removed_first) = store.sync(relative_sync_dir.to_path_buf())?;
        assert_eq!(added_first, 0);
        assert_eq!(removed_first, 0);

        let (added_second, removed_second) = store.sync(relative_sync_dir.to_path_buf())?;
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
            .sync(std::env::current_dir()?)
            .expect_err("sync should reject datasets with explicit graph memberships");
        assert!(
            err.to_string().contains("filesystem-discovered datasets"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_sync_rejects_duplicate_graph_iri_metadata() -> anyhow::Result<()> {
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
            &hdt_a.to_string_lossy(),
            &[nt_a.to_string_lossy().into_owned()],
            false,
            Some(shared_graph_iri),
        )?;
        crate::create::do_create_with_options(
            &hdt_b.to_string_lossy(),
            &[nt_b.to_string_lossy().into_owned()],
            false,
            Some(shared_graph_iri),
        )?;

        let store = AggregateHdt::new(&[hdt_a.to_string_lossy().into_owned()])?;
        let err = store
            .sync(sync_dir.clone())
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
        let err = match result {
            Ok(_) => panic!("query should return error for invalid base IRI"),
            Err(err) => err,
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
}
