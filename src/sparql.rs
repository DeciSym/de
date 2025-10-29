use oxrdf::NamedOrBlankNodeRef;
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::term::{BlankNode, NamedNode, Term};
use spargebra::SparqlParser;
use std::io::Write;
use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
};

/// Boundry over a Header-Dictionary-Triplies (HDT) storage layer.
pub struct AggregateHdt {
    // Map graph names (URIs) to HDT files
    hdts: Arc<RwLock<HashMap<String, hdt::hdt::HdtHybrid>>>,
}

impl AggregateHdt {
    pub fn new(paths: &[String]) -> anyhow::Result<Self> {
        use rayon::prelude::*;
        let mut hdts: HashMap<String, hdt::hdt::HdtHybrid> = HashMap::new();
        if paths.is_empty() {
            return Err(anyhow::anyhow!("no hdt files detected"));
        }

        let graphs: Vec<(String, hdt::hdt::HdtHybrid)> = paths
            .par_iter()
            .map(|p| -> anyhow::Result<(String, hdt::hdt::HdtHybrid)> {
                Ok((
                    format!(
                        "file:///{}",
                        Path::new(p).file_name().unwrap().to_str().unwrap()
                    ),
                    hdt::hdt::Hdt::new_hybrid_cache(Path::new(p), true)
                        .map_err(|e| anyhow::anyhow!("{}", e))?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (graph, h) in graphs {
            hdts.insert(graph, h);
        }

        Ok(Self {
            hdts: Arc::new(RwLock::new(hdts)),
        })
    }

    pub fn insert_named_graph(
        &self,
        graph_name: &NamedNode,
        file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let extension = file_path
            .extension()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("File has no extension: {:?}", file_path))?;

        let hdt = match extension {
            "hdt" => {
                // Read HDT file directly with hybrid cache
                hdt::hdt::Hdt::new_hybrid_cache(file_path, true)
                    .map_err(|e| anyhow::anyhow!("{}", e))?
            }
            "nt" => {
                // Convert NT to HDT
                // First, create a temporary HDT file
                let tmp_hdt = tempfile::Builder::new().suffix(".hdt").tempfile()?;
                let (hdt_file, hdt_path) = tmp_hdt.keep()?;

                // Read the NT file and convert to HDT
                let h = hdt::Hdt::read_nt(file_path)?;
                let mut hdt_writer = std::io::BufWriter::new(&hdt_file);

                h.write(&mut hdt_writer)?;
                hdt_writer.flush()?;
                hdt::hdt::Hdt::new_hybrid_cache(hdt_path.as_path(), true)
                    .map_err(|e| anyhow::anyhow!("{}", e))?
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported file extension: {}. Only .hdt is supported.",
                    extension
                ));
            }
        };

        let mut hdts = self.hdts.write().unwrap();
        hdts.insert(graph_name.clone().into_string(), hdt);
        Ok(())
    }

    pub fn remove_named_graph(&self, graph_name: &NamedNode) -> Result<bool, anyhow::Error> {
        let mut hdts = self.hdts.write().unwrap();
        Ok(hdts.remove(graph_name.as_str()).is_some())
    }

    pub fn clear(&self) -> Result<(), anyhow::Error> {
        let mut hdts = self.hdts.write().unwrap();
        hdts.clear();
        Ok(())
    }

    /// Iterate over all HDTs in the HashMap.
    /// Accepts a closure that receives the graph name and HDT reference for each entry.
    pub fn iter<F>(&self, mut f: F)
    where
        F: FnMut(&String, &hdt::hdt::HdtHybrid),
    {
        let hdts = self.hdts.read().unwrap();
        for (key, hdt) in hdts.iter() {
            f(key, hdt);
        }
    }

    /// Collect all triples from all HDTs and return them as a Vec with their graph names.
    /// This is useful for scenarios where you need a consumable iterator.
    pub fn collect_all_triples(&self) -> Vec<(String, [Arc<str>; 3])> {
        let hdts = self.hdts.read().unwrap();
        let mut result = Vec::new();
        for (graph_name, hdt) in hdts.iter() {
            for triple in hdt.triples_all() {
                result.push((graph_name.clone(), triple));
            }
        }
        result
    }
}

pub fn graph_to_file(name: NamedOrBlankNodeRef) -> Option<String> {
    if let NamedOrBlankNodeRef::NamedNode(n) = name {
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

impl<'a> QueryableDataset<'a> for &'a AggregateHdt {
    type InternalTerm = String;
    type Error = Error;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&String>,
        predicate: Option<&String>,
        object: Option<&String>,
        graph_name: Option<Option<&String>>,
    ) -> impl Iterator<Item = Result<InternalQuad<Self::InternalTerm>, Error>> + use<'a> {
        use rayon::prelude::*;
        let [ps, pp, po] = [subject, predicate, object].map(|x| x.map(String::as_str));
        // Query each HDT for BGP by string values in parallel.
        let v: Vec<_> = self
            .hdts
            .read()
            .unwrap()
            .par_iter()
            .flat_map(|(g, h)| {
                if let Some(Some(graph_name)) = graph_name {
                    if g != graph_name {
                        return vec![];
                    }
                }
                h.triples_with_pattern(ps, pp, po)
                    .map(|[subject, predicate, object]| {
                        Ok(InternalQuad {
                            subject: subject.to_string(),
                            predicate: predicate.to_string(),
                            object: object.to_string(),
                            graph_name: Some(g.to_string()),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        v.into_iter()
    }

    fn internalize_term(&self, term: Term) -> Result<String, Error> {
        Ok(term_to_hdt_bgp_str(term))
    }

    fn externalize_term(&self, term: String) -> Result<Term, Error> {
        hdt_bgp_str_to_term(&term)
    }

    /// Fetches the list of dataset named graphs
    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<Self::InternalTerm, Self::Error>> + use<'a> {
        let hdts = self.hdts.read().unwrap();
        let keys: Vec<String> = hdts.keys().cloned().collect();
        keys.into_iter().map(Ok)
    }

    /// Returns if the dataset contains a given named graph
    fn contains_internal_graph_name(&self, graph_name: &String) -> Result<bool, Self::Error> {
        let hdts = self.hdts.read().unwrap();
        Ok(hdts.contains_key(graph_name))
    }
}

pub fn query<'a>(
    q: &str,
    hdt: &'a AggregateHdt,
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
    use super::*;
    use std::path::PathBuf;

    /// Helper function to get the path to a test HDT file
    fn get_test_hdt_path(filename: &str) -> String {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(filename);
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_contains_named_graph_found() {
        // Create an AggregateHDT with test.hdt
        let test_hdt_path = get_test_hdt_path("test.hdt");
        let store = &AggregateHdt::new(&[test_hdt_path]).expect("Failed to create AggregateHDT");

        // Test 1: Graph should be found with file:/// URI scheme matching the filename
        let graph_name = "file:///test.hdt".to_string();
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
    fn test_contains_named_graph_not_found() {
        // Create an AggregateHDT with test.hdt
        let test_hdt_path = get_test_hdt_path("test.hdt");
        let store = &AggregateHdt::new(&[test_hdt_path]).expect("Failed to create AggregateHDT");

        // Test 1: Graph with different filename should not be found
        let missing_graph = "file:///nonexistent.hdt".to_string();
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
        let http_graph = "http://example.org/test.hdt".to_string();
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
        let wrong_stem = "file:///different".to_string();
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
    fn test_contains_named_graph_multiple_graphs() {
        // Create an AggregateHDT with multiple HDT files
        let test_hdt = get_test_hdt_path("test.hdt");
        let literal_hdt = get_test_hdt_path("literal.hdt");
        let store = &AggregateHdt::new(&[test_hdt, literal_hdt])
            .expect("Failed to create AggregateHDT with multiple files");

        // Test 1: First graph should be found
        let graph1 = "file:///test.hdt".to_string();
        assert!(
            store.contains_internal_graph_name(&graph1).unwrap(),
            "First graph 'test' should be found"
        );

        // Test 2: Second graph should be found
        let graph2 = "file:///literal.hdt".to_string();
        assert!(
            store.contains_internal_graph_name(&graph2).unwrap(),
            "Second graph 'literal' should be found"
        );

        // Test 3: Non-existent graph should not be found
        let missing = "file:///missing.hdt".to_string();
        assert!(
            !store.contains_internal_graph_name(&missing).unwrap(),
            "Non-existent graph should not be found"
        );
    }

    #[test]
    fn test_contains_named_graph_after_insert() {
        // Create an AggregateHDT with one HDT file
        let test_hdt_path = get_test_hdt_path("test.hdt");
        let store = &AggregateHdt::new(std::slice::from_ref(&test_hdt_path))
            .expect("Failed to create AggregateHDT");

        // Graph should exist initially
        let existing_graph = "file:///test.hdt".to_string();
        assert!(
            store.contains_internal_graph_name(&existing_graph).unwrap(),
            "Initial graph should exist"
        );

        // Insert a new graph
        let new_graph = "http://example.org/newgraph".to_string();

        // Before insertion, should not exist
        assert!(
            !store.contains_internal_graph_name(&new_graph).unwrap(),
            "New graph should not exist before insertion"
        );

        // Insert the graph
        let hdt_path = Path::new(&test_hdt_path);
        store
            .insert_named_graph(&NamedNode::new(&new_graph).unwrap(), hdt_path)
            .expect("Failed to insert named graph");

        // After insertion, should exist
        assert!(
            store.contains_internal_graph_name(&new_graph).unwrap(),
            "New graph should exist after insertion"
        );
    }
}
