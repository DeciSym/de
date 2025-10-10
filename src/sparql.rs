use std::{
    fs::File,
    io::{BufReader, Error, ErrorKind},
    str::FromStr,
};

use oxrdf::{BlankNode, NamedNode, Term};
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::SparqlParser;

/// Boundry over a Header-Dictionary-Triplies (HDT) storage layer.
pub struct AggregateHDT {
    // collection of HDT files in the dataset
    hdts: Vec<hdt::Hdt>,
}

impl AggregateHDT {
    pub fn new(paths: &[String]) -> Result<Self, anyhow::Error> {
        let mut hdts: Vec<hdt::Hdt> = Vec::new();
        if paths.is_empty() {
            return Err(anyhow::anyhow!("no hdt files detected"));
        }
        for path in paths {
            // TODO catch error and proceed to next file?
            let mut reader = BufReader::new(File::open(path).expect("failed to open HDT file"));
            let hdt = hdt::Hdt::read(&mut reader).unwrap();
            hdts.push(hdt);
            continue;
        }

        Ok(Self { hdts })
    }
}

/// Create the correct term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
// Based on https://github.com/KonradHoeffner/hdt/blob/871db777db3220dc4874af022287975b31d72d3a/src/hdt_graph.rs#L64
fn hdt_bgp_str_to_term(s: &str) -> Result<Term, Error> {
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
fn term_to_hdt_bgp_str(term: Term) -> String {
    match term {
        Term::NamedNode(named_node) => named_node.into_string(),
        Term::Literal(literal) => literal.to_string(),
        Term::BlankNode(s) => s.to_string(),
    }
}

impl<'a> QueryableDataset<'a> for &'a AggregateHDT {
    type InternalTerm = String;
    type Error = Error;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&String>,
        predicate: Option<&String>,
        object: Option<&String>,
        graph_name: Option<Option<&String>>,
    ) -> impl Iterator<Item = Result<InternalQuad<Self::InternalTerm>, Error>> + use<'a> {
        if let Some(Some(graph_name)) = graph_name {
            return vec![Err(Error::new(
                ErrorKind::InvalidData,
                format!("HDT does not support named graph: {graph_name:?}"),
            ))]
            .into_iter();
        }
        let mut v: Vec<Result<InternalQuad<_>, Error>> = Vec::new();
        // let subject_owned = subject.cloned();
        // let predicate_owned = predicate.cloned();
        // let object_owned = object.cloned();
        for data in &self.hdts {
            for triple in data.internal_quads_for_pattern(subject, predicate, object, None) {
                v.push(triple);
            }
        }
        v.into_iter()
    }

    fn internalize_term(&self, term: Term) -> Result<String, Error> {
        Ok(term_to_hdt_bgp_str(term))
    }

    fn externalize_term(&self, term: String) -> Result<Term, Error> {
        hdt_bgp_str_to_term(&term)
    }
}

pub fn query<'a>(
    q: &str,
    hdt: &'a AggregateHDT,
    base: Option<&str>,
) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    let query = SparqlParser::new()
        .with_base_iri(base.unwrap_or("http://example.com/"))
        .expect(&format!("invalid base iri provided: {base:?}"))
        .parse_query(q)?;
    QueryEvaluator::new().execute(hdt, &query)
}
