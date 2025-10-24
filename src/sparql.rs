use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::term::{BlankNode, NamedNode, Term};
use spargebra::SparqlParser;
use std::io::{BufReader, Error, ErrorKind};
use std::str::FromStr;

pub struct AggregateHdt {
    hdts: Vec<hdt::Hdt>,
}

impl AggregateHdt {
    pub fn new(paths: &[String]) -> Result<Self, Box<dyn std::error::Error>> {
        use rayon::prelude::*;

        let hdts: Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>> = paths
            .par_iter()
            .map(
                |p| -> Result<hdt::Hdt, Box<dyn std::error::Error + Send + Sync>> {
                    let reader = BufReader::new(std::fs::File::open(p)?);
                    Ok(hdt::Hdt::read(reader)?)
                },
            )
            .collect();

        Ok(Self {
            hdts: hdts.map_err(|e| anyhow::anyhow!("hdt error: {e}"))?,
        })
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
        if let Some(Some(graph_name)) = graph_name {
            return vec![Err(Error::new(
                ErrorKind::InvalidData,
                format!("HDT does not support named graph: {graph_name:?}"),
            ))]
            .into_iter();
        }
        let [ps, pp, po] = [subject, predicate, object].map(|x| x.map(String::as_str));
        // Query each HDT for BGP by string values in parallel.
        let v: Vec<_> = self
            .hdts
            .par_iter()
            .flat_map(|h| {
                h.triples_with_pattern(ps, pp, po)
                    .map(|[subject, predicate, object]| {
                        Ok(InternalQuad {
                            subject: subject.to_string(),
                            predicate: predicate.to_string(),
                            object: object.to_string(),
                            graph_name: None,
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
