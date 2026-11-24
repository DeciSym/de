//! Where the endpoint's data comes from.

use super::error::HttpError;
use spareval::{QueryEvaluationError, QueryResults, QueryableDataset};
use spargebra::Query;

/// The dataset a request asks for, from the SPARQL protocol's parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DatasetSelection {
    /// The graphs named by `default-graph-uri`.
    pub default_graph_uris: Vec<String>,
    /// The graphs named by `named-graph-uri`.
    pub named_graph_uris: Vec<String>,
    /// Whether the default graph is the union of every graph: the
    /// `union-default-graph` parameter, or the service's default when the
    /// request names no graphs.
    pub union_default_graph: bool,
}

/// Where a [`SparqlService`](super::SparqlService) gets its data.
///
/// [`HdtDirectory`](super::HdtDirectory) serves the HDT files in a directory.
/// A source can serve anything spareval can query: what it opens is a
/// [`ServeDataset`], which every type whose references implement
/// `QueryableDataset` already is.
pub trait DatasetSource: Send + Sync + 'static {
    /// Called on the request's thread before each query: the chance to
    /// notice new data.
    fn refresh(&self) -> Result<(), HttpError> {
        Ok(())
    }

    /// Opens the data a query reads. Called on the query's worker thread.
    /// The dataset lives until the results have been streamed, so it can
    /// hold whatever must stay open until then.
    fn open(&self, selection: &DatasetSelection) -> Result<Box<dyn ServeDataset + '_>, HttpError>;
}

/// A dataset opened for one query.
pub trait ServeDataset {
    fn evaluate(&self, query: &Query) -> Result<QueryResults<'_>, QueryEvaluationError>;
}

impl<D> ServeDataset for D
where
    for<'a> &'a D: QueryableDataset<'a>,
{
    fn evaluate(&self, query: &Query) -> Result<QueryResults<'_>, QueryEvaluationError> {
        crate::sparql::query_parsed_with_debug_plan(query, self, false)
    }
}
