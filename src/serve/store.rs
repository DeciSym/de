//! The Graph Store Protocol on `/store`: reading graphs with GET and HEAD.
//!
//! A request names a graph with `?graph=<iri>`, whose IRI resolves against
//! the request's URL, or the default graph with `?default`. A path under
//! `/store/` names the graph whose IRI is the request's own URL, without its
//! query (direct identification). `/store` with neither parameter is the
//! whole dataset.

use super::{
    body::{RequestBody, ResponseBody},
    dataset::{DatasetSelection, DatasetSource},
    error::{HttpError, bad_request, internal_server_error},
    negotiate::{dataset_content_negotiation, rdf_content_negotiation},
    protocol::{base_url, url_query},
    service::ServiceOptions,
    stream::{ResultsAs, evaluate_query},
};
use http::{Method, Request, Response, StatusCode, header::CONTENT_TYPE};
use oxiri::Iri;
use oxrdf::NamedNode;
use spargebra::SparqlParser;
use std::sync::Arc;
use url::form_urlencoded;

/// What a request to `/store` is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum StoreTarget {
    /// `/store` alone: every graph, as a dataset.
    Dataset,
    /// `?default`: the default graph.
    DefaultGraph,
    /// `?graph=<iri>`, or a path under `/store/`: a named graph.
    NamedGraph(NamedNode),
}

/// Whether `path` is the Graph Store Protocol's: `/store`, or under `/store/`.
pub(super) fn is_store_path(path: &str) -> bool {
    path == "/store" || path.starts_with("/store/")
}

/// What the request names, from its path and its `graph` and `default`
/// parameters.
pub(super) fn store_target(request: &Request<RequestBody>) -> Result<StoreTarget, HttpError> {
    if request.uri().path() != "/store" {
        return resolve(request, "").map(StoreTarget::NamedGraph);
    }
    let mut graph = None;
    let mut default = false;
    for (key, value) in form_urlencoded::parse(url_query(request)) {
        match key.as_ref() {
            "graph" if graph.is_some() => {
                return Err(bad_request("Multiple graph parameters provided"));
            }
            "graph" => graph = Some(value.into_owned()),
            "default" => default = true,
            _ => (),
        }
    }
    match (graph, default) {
        (Some(_), true) => Err(bad_request(
            "Both graph and default parameters should not be set at the same time",
        )),
        (Some(graph), false) => resolve(request, &graph).map(StoreTarget::NamedGraph),
        (None, true) => Ok(StoreTarget::DefaultGraph),
        (None, false) => Ok(StoreTarget::Dataset),
    }
}

/// `iri` resolved against the request's URL, without its query.
fn resolve(request: &Request<RequestBody>, iri: &str) -> Result<NamedNode, HttpError> {
    let base = Iri::parse(base_url(request))
        .map_err(|e| bad_request(format!("Invalid request URL: {e}")))?;
    let resolved = base
        .resolve(iri)
        .map_err(|e| bad_request(format!("Invalid graph IRI {iri}: {e}")))?;
    Ok(NamedNode::new_unchecked(resolved.into_inner()))
}

/// Answers GET or HEAD on `/store`: the graph the request names, or every
/// graph, in the format its Accept header asks for. A named graph that
/// doesn't exist is a 404. The default graph is the union of every graph if
/// the service's default graph is.
pub(super) fn read(
    source: &Arc<dyn DatasetSource>,
    request: &Request<RequestBody>,
    options: &ServiceOptions,
) -> Result<Response<ResponseBody>, HttpError> {
    let target = store_target(request)?;
    source.refresh()?;
    let (query, selection, format, results_as) = match &target {
        StoreTarget::NamedGraph(graph) => {
            if !source.contains_graph(graph)? {
                return Err(HttpError::new(
                    StatusCode::NOT_FOUND,
                    format!("The graph {graph} does not exist"),
                ));
            }
            let selection = DatasetSelection {
                named_graph_uris: vec![graph.as_str().to_owned()],
                ..DatasetSelection::default()
            };
            (
                format!("CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH {graph} {{ ?s ?p ?o }} }}"),
                selection,
                rdf_content_negotiation(request)?,
                ResultsAs::Negotiated,
            )
        }
        StoreTarget::DefaultGraph => (
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }".to_owned(),
            DatasetSelection {
                union_default_graph: options.union_default_graph,
                ..DatasetSelection::default()
            },
            rdf_content_negotiation(request)?,
            ResultsAs::Negotiated,
        ),
        // The default graph's own triples, then every named graph's. A
        // union default graph would repeat the named graphs, so it isn't one
        // here.
        StoreTarget::Dataset => {
            let format = dataset_content_negotiation(request)?;
            (
                "SELECT ?s ?p ?o ?g WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }"
                    .to_owned(),
                DatasetSelection::default(),
                format,
                ResultsAs::Quads(format),
            )
        }
    };
    if request.method() == Method::HEAD {
        return Response::builder()
            .header(CONTENT_TYPE, format.media_type())
            .body(ResponseBody::Empty)
            .map_err(internal_server_error);
    }
    let query = SparqlParser::new()
        .parse_query(&query)
        .map_err(internal_server_error)?;
    evaluate_query(
        source,
        query,
        selection,
        request,
        options.query_timeout,
        results_as,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(uri: &str) -> Result<StoreTarget, HttpError> {
        store_target(&Request::get(uri).body(RequestBody::empty()).unwrap())
    }

    fn named(iri: &str) -> StoreTarget {
        StoreTarget::NamedGraph(NamedNode::new(iri).unwrap())
    }

    #[test]
    fn a_graph_parameter_names_a_graph() {
        assert_eq!(
            target("http://localhost/store?graph=http%3A%2F%2Fexample.org%2Fg").unwrap(),
            named("http://example.org/g")
        );
    }

    #[test]
    fn a_relative_graph_resolves_against_the_request() {
        assert_eq!(
            target("http://localhost:7878/store?graph=/store/person/1.ttl").unwrap(),
            named("http://localhost:7878/store/person/1.ttl")
        );
    }

    #[test]
    fn a_path_under_store_is_the_graph_iri() {
        assert_eq!(
            target("http://localhost/store/person/1.ttl?ignored=1").unwrap(),
            named("http://localhost/store/person/1.ttl")
        );
    }

    #[test]
    fn default_and_dataset() {
        assert_eq!(
            target("http://localhost/store?default").unwrap(),
            StoreTarget::DefaultGraph
        );
        assert_eq!(
            target("http://localhost/store").unwrap(),
            StoreTarget::Dataset
        );
    }

    #[test]
    fn graph_and_default_together_are_a_bad_request() {
        let error =
            target("http://localhost/store?graph=http://example.org/g&default").unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn two_graphs_are_a_bad_request() {
        let error =
            target("http://localhost/store?graph=http://example.org/a&graph=http://example.org/b")
                .unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn only_store_and_its_subpaths_are_store_paths() {
        assert!(is_store_path("/store"));
        assert!(is_store_path("/store/a"));
        assert!(!is_store_path("/stored"));
        assert!(!is_store_path("/storex/a"));
    }
}
