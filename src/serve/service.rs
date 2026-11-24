//! Routing a request to the page or endpoint that answers it.

use super::{
    error::{
        HttpError, bad_request, internal_server_error, not_implemented, unsupported_media_type,
    },
    hdt_dir::maybe_sync_store,
    negotiate::rdf_content_negotiation,
    protocol::{
        configure_and_evaluate_sparql_query, content_type, limited_body, limited_string_body,
        url_query,
    },
    ui::{HTML_ROOT_PAGE, LOGO, YASGUI_CSS, YASGUI_JS},
};
use crate::{
    service_description::{EndpointKind, generate_service_description},
    sparql::AggregateHdt,
};
use http::{Request, Response, StatusCode, header::CONTENT_TYPE};
use log::debug;
use oxhttp::model::Body;
use std::path::Path;

pub fn handle_request(
    request: &mut Request<Body>,
    store: &AggregateHdt,
    // read_only: bool,
    union_default_graph: bool,
    // timeout: Option<Duration>,
    locations: &str,
) -> Result<Response<Body>, HttpError> {
    debug!("{} {}", request.method(), request.uri().path());
    match (request.uri().path(), request.method().as_ref()) {
        ("/", "HEAD") => Response::builder()
            .header(CONTENT_TYPE, "text/html")
            .body(Body::empty())
            .map_err(internal_server_error),
        ("/", "GET") => Response::builder()
            .header(CONTENT_TYPE, "text/html")
            .body(HTML_ROOT_PAGE.into())
            .map_err(internal_server_error),
        ("/yasgui.min.css", "HEAD") => Response::builder()
            .header(CONTENT_TYPE, "text/css")
            .body(Body::empty())
            .map_err(internal_server_error),
        ("/yasgui.min.css", "GET") => Response::builder()
            .header(CONTENT_TYPE, "text/css")
            .body(YASGUI_CSS.into())
            .map_err(internal_server_error),
        ("/yasgui.min.js", "HEAD") => Response::builder()
            .header(CONTENT_TYPE, "application/javascript")
            .body(Body::empty())
            .map_err(internal_server_error),
        ("/yasgui.min.js", "GET") => Response::builder()
            .header(CONTENT_TYPE, "application/javascript")
            .body(YASGUI_JS.into())
            .map_err(internal_server_error),
        ("/logo.svg", "HEAD") => Response::builder()
            .header(CONTENT_TYPE, "image/svg+xml")
            .body(Body::empty())
            .map_err(internal_server_error),
        ("/logo.svg", "GET") => Response::builder()
            .header(CONTENT_TYPE, "image/svg+xml")
            .body(LOGO.into())
            .map_err(internal_server_error),
        ("/query", "GET") => {
            let query = url_query(request);
            if query.is_empty() {
                let format = rdf_content_negotiation(request)?;
                let description =
                    generate_service_description(format, EndpointKind::Query, union_default_graph)
                        .map_err(internal_server_error)?;
                Response::builder()
                    .header(CONTENT_TYPE, format.media_type())
                    .body(description.into())
                    .map_err(internal_server_error)
            } else {
                maybe_sync_store(store, Path::new(locations))?;
                configure_and_evaluate_sparql_query(
                    store,
                    &[url_query(request)],
                    None,
                    request,
                    union_default_graph,
                    // timeout,
                )
            }
        }
        ("/query", "POST") => {
            maybe_sync_store(store, Path::new(locations))?;
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if content_type == "application/sparql-query" {
                let query = limited_string_body(request)?;
                configure_and_evaluate_sparql_query(
                    store,
                    &[url_query(request)],
                    Some(query),
                    request,
                    union_default_graph,
                    // timeout,
                )
            } else if content_type == "application/x-www-form-urlencoded" {
                let buffer = limited_body(request)?;
                configure_and_evaluate_sparql_query(
                    store,
                    &[url_query(request), &buffer],
                    None,
                    request,
                    union_default_graph,
                    // timeout,
                )
            } else {
                Err(unsupported_media_type(&content_type))
            }
        }
        ("/update", "GET" | "POST") => Err(not_implemented(
            "SPARQL Update is not supported by this server",
        )),
        (path, _method) if is_store_path(path) => Err(not_implemented(
            "Graph Store Protocol is not supported by this server",
        )),
        _ => Err((
            StatusCode::NOT_FOUND,
            format!(
                "{} {} is not supported by this server",
                request.method(),
                request.uri().path()
            ),
        )),
    }
}

fn is_store_path(path: &str) -> bool {
    path == "/store" || path.starts_with("/store/")
}
