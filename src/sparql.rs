use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::term::{BlankNode, NamedNode, Term};
use spargebra::SparqlParser;
use std::io::BufReader;
use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
};

/// Boundry over a Header-Dictionary-Triplies (HDT) storage layer.
/// Stores file paths only; HDT instances are created per-request for better concurrency.
pub struct AggregateHdt {
    // Map graph names (URIs) to file paths on disk
    file_paths: Arc<RwLock<HashMap<String, std::path::PathBuf>>>,
}

pub struct AggregateHdtSnapshot {
    // Map graph names (URIs) to HDT instances
    pub hdts: HashMap<String, hdt::Hdt>,
}

impl AggregateHdt {
    pub fn new(paths: &[String]) -> anyhow::Result<Self> {
        let mut file_paths: HashMap<String, std::path::PathBuf> = HashMap::new();
        if paths.is_empty() {
            return Err(anyhow::anyhow!("no hdt files detected"));
        }

        for p in paths {
            let path = Path::new(p);

            // Verify the file exists
            if !path.exists() {
                return Err(anyhow::anyhow!("HDT file does not exist: {}", p));
            }

            // Create graph name from filename
            let graph_name = format!(
                "file:///{}",
                path.file_name()
                    .ok_or_else(|| anyhow::anyhow!("Invalid file path: {}", p))?
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid filename encoding: {}", p))?
            );

            file_paths.insert(graph_name, path.to_path_buf());
        }

        Ok(Self {
            file_paths: Arc::new(RwLock::new(file_paths)),
        })
    }

    /// Create a snapshot of HDT instances for querying.
    ///
    /// # Arguments
    /// * `named_graphs` - Optional filter to only load specific named graphs.
    ///                   If None, all available graphs are loaded.
    ///                   If Some(vec), only graphs in the vec are loaded.
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
    ) -> Result<AggregateHdtSnapshot, Box<dyn std::error::Error>> {
        use rayon::prelude::*;

        let file_paths_guard = self.file_paths.read().unwrap();

        // Optimization: Filter graphs BEFORE loading into memory
        let paths_vec: Vec<(String, std::path::PathBuf)> = file_paths_guard
            .iter()
            .filter(|(graph_name, _path)| {
                // If named_graphs filter is specified, only include graphs in the filter
                if let Some(ref filter) = named_graphs {
                    filter.contains(graph_name)
                } else {
                    true // No filter - include all graphs
                }
            })
            .map(|(g, p)| (g.clone(), p.clone()))
            .collect();
        drop(file_paths_guard);

        // Load filtered HDTs in parallel
        let hdts: HashMap<String, hdt::Hdt> = paths_vec
            .par_iter()
            .map(|(graph_name, path)| -> anyhow::Result<(String, hdt::Hdt)> {
                let mut reader = BufReader::new(std::fs::File::open(path)?);
                let hdt = hdt::Hdt::read(&mut reader)
                    .map_err(|e| anyhow::anyhow!("Failed to load HDT from {:?}: {}", path, e))?;
                Ok((graph_name.clone(), hdt))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .collect();

        Ok(AggregateHdtSnapshot { hdts })
    }

    #[cfg(feature = "server")]
    pub fn contains_graph_name(&self, graph_name: &String) -> Result<bool, anyhow::Error> {
        Ok(self.file_paths.read().unwrap().contains_key(graph_name))
    }

    #[cfg(feature = "server")]
    pub fn insert_named_graph(
        &self,
        graph_name: &NamedNode,
        file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        use std::io::Write;

        let extension = file_path
            .extension()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("File has no extension: {:?}", file_path))?;

        let final_path = match extension {
            "hdt" => {
                // Use HDT file directly
                if !file_path.exists() {
                    return Err(anyhow::anyhow!("HDT file does not exist: {:?}", file_path));
                }
                file_path.to_path_buf()
            }
            "nt" => {
                // Convert NT to HDT
                // create new HDT file in same dir as original file, with same name
                let hdt_path = format!(
                    "{}/{}.hdt",
                    file_path.to_path_buf().parent().unwrap().to_str().unwrap(),
                    file_path.file_stem().unwrap().to_str().unwrap()
                );

                // Read the NT file and convert to HDT
                let h = hdt::Hdt::read_nt(file_path)?;
                let mut hdt_writer = std::io::BufWriter::new(std::fs::File::create(&hdt_path)?);

                h.write(&mut hdt_writer)?;
                hdt_writer.flush()?;
                std::path::PathBuf::from(&hdt_path)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported file extension: {}. Only .hdt and .nt are supported.",
                    extension
                ));
            }
        };

        let mut file_paths = self.file_paths.write().unwrap();
        file_paths.insert(graph_name.clone().into_string(), final_path);
        Ok(())
    }

    #[cfg(feature = "server")]
    pub fn remove_named_graph(&self, graph_name: &NamedNode) -> Result<bool, anyhow::Error> {
        let mut file_paths = self.file_paths.write().unwrap();
        if let Some(path) = file_paths.remove(graph_name.as_str()) {
            // Delete the HDT file from disk
            if path.exists() {
                std::fs::remove_file(&path)?;
                eprintln!("Deleted HDT file: {:?}", path);
            }

            // Delete associated cache files
            if let Some(parent) = path.parent() {
                if let Some(filename) = path.file_name() {
                    let filename_str = filename.to_string_lossy();

                    if let Ok(entries) = std::fs::read_dir(parent) {
                        for entry in entries.flatten() {
                            let entry_path = entry.path();
                            if let Some(entry_name) = entry_path.file_name() {
                                let entry_name_str = entry_name.to_string_lossy();

                                // Check if this is a cache file for our HDT
                                if entry_name_str.starts_with(&*filename_str)
                                    && (entry_name_str.contains(".index.")
                                        || entry_name_str.ends_with(".cache"))
                                {
                                    if let Err(e) = std::fs::remove_file(&entry_path) {
                                        eprintln!(
                                            "Warning: Failed to delete cache file {:?}: {}",
                                            entry_path, e
                                        );
                                    } else {
                                        eprintln!("Deleted cache file: {:?}", entry_path);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[cfg(feature = "server")]
    pub fn clear(&self) -> Result<(), anyhow::Error> {
        let mut file_paths = self.file_paths.write().unwrap();
        file_paths.clear();
        Ok(())
    }

    /// Get all graph names and their associated HDT header information.
    /// Returns a Vec of tuples containing (graph_name, file_path, hdt::header::Header).
    #[cfg(feature = "server")]
    pub fn get_all_graphs(
        &self,
    ) -> Result<Vec<(String, std::path::PathBuf, hdt::header::Header)>, anyhow::Error> {
        let file_paths = self.file_paths.read().unwrap();
        let mut result = Vec::new();

        for (graph_name, path) in file_paths.iter() {
            // Read HDT header to get metadata
            let mut reader =
                BufReader::new(std::fs::File::open(path).map_err(|e| anyhow::anyhow!("{e}"))?);

            // Read control info first, then header
            let header = hdt::containers::ControlInfo::read(&mut reader)
                .map_err(|e| format!("ControlInfo error: {}", e))
                .and_then(|_| {
                    hdt::header::Header::read(&mut reader)
                        .map_err(|e| format!("Header error: {}", e))
                })
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            result.push((graph_name.clone(), path.clone(), header));
        }

        // Sort by graph name
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    /// Collect all triples from all HDTs and return them as a Vec with their graph names.
    /// This is useful for scenarios where you need a consumable iterator.
    /// NOTE: This creates HDT instances for all graphs, so it may be memory-intensive.
    #[cfg(feature = "server")]
    pub fn collect_all_triples(&self) -> Vec<(String, [Arc<str>; 3])> {
        let file_paths = self.file_paths.read().unwrap();
        let mut result = Vec::new();
        for (graph_name, path) in file_paths.iter() {
            // Create HDT instance for this file
            if let Ok(hdt) = hdt::hdt::Hdt::new_hybrid_cache(path, true) {
                for triple in hdt.triples_all() {
                    result.push((graph_name.clone(), triple));
                }
            }
        }
        result
    }
}

#[cfg(feature = "server")]
pub fn graph_to_file(name: oxrdf::NamedOrBlankNodeRef) -> Option<String> {
    if let oxrdf::NamedOrBlankNodeRef::NamedNode(n) = name {
        let res = n.to_string().parse::<http::Uri>();
        let Ok(uri) = res else {
            return None;
        };
        let paths = uri.path().split("/").collect::<Vec<_>>();
        if let Some(p) = paths.last() {
            return Some(
                Path::new(p)
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }
    None
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
        let subject_pattern = subject.cloned();
        let predicate_pattern = predicate.cloned();
        let object_pattern = object.cloned();

        let graph_name_owned = graph_name.map(|inner| inner.cloned());

        // Optimization: Pre-filter graphs to reduce unnecessary work
        // Note: get_snapshot() already filtered graphs at load time,
        // so self.hdts contains only the required graphs. This filter
        // handles additional runtime graph name matching from the query.
        let graphs_to_query: Vec<(&String, &hdt::Hdt)> = self
            .hdts
            .iter()
            .filter(|(g, _h)| {
                match &graph_name_owned {
                    // Query for default graph: Some(None)
                    // Default graph is always union of all loaded graphs
                    Some(None) => true,
                    // Query for specific named graph: Some(Some(graph))
                    Some(Some(target_graph)) => {
                        let g_arc: Arc<str> = Arc::from(g.as_str());
                        &g_arc == target_graph
                    }
                    // Query across all graphs: None
                    None => true,
                }
            })
            .collect();

        // Optimization: Collect iterators into a Vec first, then flatten
        // allows lazy evaluation of triples
        let iters: Vec<_> = graphs_to_query
            .iter()
            .map(|(graph_name, hdt)| {
                let ps = subject_pattern.as_ref().map(|s| s.as_ref());
                let pp = predicate_pattern.as_ref().map(|p| p.as_ref());
                let po = object_pattern.as_ref().map(|o| o.as_ref());
                let graph_arc: Arc<str> = Arc::from(graph_name.as_str());

                // Get iterator and immediately convert to owned triples with graph name
                // Due to HDT's API design (returns Box<dyn Iterator + '_>), must collect here
                let triples: Vec<_> = hdt
                    .triples_with_pattern(ps, pp, po)
                    .map(|[subject, predicate, object]| {
                        (subject, predicate, object, graph_arc.clone())
                    })
                    .collect();
                triples
            })
            .collect();

        // Optimization: Flatten collected results without additional copying
        iters
            .into_iter()
            .flatten()
            .map(|(subject, predicate, object, graph_arc)| {
                Ok(InternalQuad {
                    subject,
                    predicate,
                    object,
                    graph_name: Some(graph_arc),
                })
            })
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
        let keys: Vec<Arc<str>> = self.hdts.keys().map(|k| Arc::from(k.as_str())).collect();
        keys.into_iter().map(Ok)
    }

    fn contains_internal_graph_name(&self, graph_name: &Arc<str>) -> Result<bool, Self::Error> {
        Ok(self.hdts.contains_key(graph_name.as_ref()))
    }
}

pub fn query<'a>(
    q: &str,
    hdt: &'a AggregateHdtSnapshot,
    base_iri: Option<String>,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    let query = SparqlParser::new()
        .with_base_iri(base_iri.unwrap_or("http://example.com/".to_string()))
        .unwrap()
        .parse_query(q)?;
    QueryEvaluator::new().prepare(&query).execute(hdt)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "server")]
    use super::*;

    /// Helper function to get the path to a test HDT file
    #[cfg(feature = "server")]
    fn get_test_hdt_path(filename: &str) -> String {
        use std::path::PathBuf;

        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(filename);
        path.to_string_lossy().into_owned()
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_contains_named_graph_found() {
        // Create an AggregateHDT with test.hdt
        let test_hdt_path = get_test_hdt_path("test.hdt");
        let store = &AggregateHdt::new(&[test_hdt_path])
            .expect("Failed to create AggregateHDT")
            .get_snapshot(None)
            .expect("msg");

        // Test 1: Graph should be found with file:/// URI scheme matching the filename
        let graph_name: Arc<str> = Arc::from("file:///test.hdt");
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
        let test_hdt_path = get_test_hdt_path("test.hdt");
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
    fn test_contains_named_graph_multiple_graphs() {
        // Create an AggregateHDT with multiple HDT files
        let test_hdt = get_test_hdt_path("test.hdt");
        let literal_hdt = get_test_hdt_path("literal.hdt");
        let store = &AggregateHdt::new(&[test_hdt, literal_hdt])
            .expect("Failed to create AggregateHDT with multiple files")
            .get_snapshot(None)
            .expect("msg");

        // Test 1: First graph should be found
        let graph1: Arc<str> = Arc::from("file:///test.hdt");
        assert!(
            store.contains_internal_graph_name(&graph1).unwrap(),
            "First graph 'test' should be found"
        );

        // Test 2: Second graph should be found
        let graph2: Arc<str> = Arc::from("file:///literal.hdt");
        assert!(
            store.contains_internal_graph_name(&graph2).unwrap(),
            "Second graph 'literal' should be found"
        );

        // Test 3: Non-existent graph should not be found
        let missing: Arc<str> = Arc::from("file:///missing.hdt");
        assert!(
            !store.contains_internal_graph_name(&missing).unwrap(),
            "Non-existent graph should not be found"
        );
    }

    #[test]
    #[cfg(feature = "server")]
    fn test_contains_named_graph_after_insert() {
        // Create an AggregateHDT with one HDT file
        let test_hdt_path = get_test_hdt_path("test.hdt");
        let store = &AggregateHdt::new(std::slice::from_ref(&test_hdt_path))
            .expect("Failed to create AggregateHDT");

        let snapshot = &store.get_snapshot(None).expect("msg");

        // Graph should exist initially
        let existing_graph: Arc<str> = Arc::from("file:///test.hdt");
        assert!(
            snapshot
                .contains_internal_graph_name(&existing_graph)
                .unwrap(),
            "Initial graph should exist"
        );

        // Insert a new graph
        let new_graph = "http://example.org/newgraph".to_string();
        let new_graph_arc: Arc<str> = Arc::from(new_graph.as_str());

        // Before insertion, should not exist
        assert!(
            !snapshot
                .contains_internal_graph_name(&new_graph_arc)
                .unwrap(),
            "New graph should not exist before insertion"
        );

        // Insert the graph
        let hdt_path = Path::new(&test_hdt_path);
        store
            .insert_named_graph(&NamedNode::new(&new_graph).unwrap(), hdt_path)
            .expect("Failed to insert named graph");

        // After insertion, should exist (need a new snapshot!)
        let snapshot2 = &store.get_snapshot(None).expect("msg");
        assert!(
            snapshot2
                .contains_internal_graph_name(&new_graph_arc)
                .unwrap(),
            "New graph should exist after insertion"
        );
    }
}
