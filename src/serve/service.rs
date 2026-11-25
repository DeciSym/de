//! The SPARQL service: answering a request with a page or an endpoint.

use super::{
    body::{RequestBody, ResponseBody},
    dataset::DatasetSource,
    error::{
        HttpError, bad_request, internal_server_error, not_implemented, unsupported_media_type,
    },
    negotiate::rdf_content_negotiation,
    protocol::{
        configure_and_evaluate_sparql_query, content_type, limited_body, limited_string_body,
        url_query,
    },
};
use crate::service_description::{EndpointKind, generate_service_description};
use http::{
    HeaderValue, Method, Request, Response, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, CONTENT_TYPE, ORIGIN,
    },
};
use log::debug;
use std::{sync::Arc, time::Duration};

/// How a [`SparqlService`] answers.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceOptions {
    /// With no dataset in the request, the default graph is the union of
    /// every graph. Default: true.
    pub union_default_graph: bool,
    /// Answer CORS preflight requests, and allow any origin. Default: false.
    pub cors: bool,
    /// The largest request body accepted, in bytes. Default: 128 MiB.
    pub max_body_bytes: u64,
    /// How long a query may run. One still running past it is cancelled: it
    /// gets a 503 if it hasn't sent results yet, or else its response is cut
    /// off. Results already computed still go out, so a slow client can take
    /// longer to read them. Default: none.
    pub query_timeout: Option<Duration>,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            union_default_graph: true,
            cors: false,
            max_body_bytes: 1024 * 1024 * 128,
            query_timeout: None,
        }
    }
}

/// Pages and endpoints of its own that a server adds, such as a query page.
/// They are tried before the SPARQL endpoints, in the order they were added.
pub trait Routes: Send + Sync + 'static {
    /// Answers `request`, or `None` to leave it to the next routes and then
    /// to the SPARQL endpoints.
    fn route(
        &self,
        request: &mut Request<RequestBody>,
    ) -> Option<Result<Response<ResponseBody>, HttpError>>;
}

/// A read-only SPARQL endpoint over a [`DatasetSource`].
///
/// It answers SPARQL 1.1 Protocol queries on `/query`, with GET or POST, and
/// the service description on `GET /query` without a query; other methods
/// get 405. `/update` and `/store` answer 501, whatever the method. [`Routes`]
/// add other paths. It doesn't depend on any HTTP server:
/// [`spawn`](super::spawn) runs one with oxhttp.
///
/// A query's response starts once its first results are ready, and then
/// streams the rest. So a query that fails before its first results gets an
/// error status; one that fails later can only have its body cut off.
pub struct SparqlService {
    source: Arc<dyn DatasetSource>,
    options: ServiceOptions,
    routes: Vec<Box<dyn Routes>>,
}

impl SparqlService {
    pub fn new(source: impl DatasetSource) -> Self {
        Self {
            source: Arc::new(source),
            options: ServiceOptions::default(),
            routes: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_options(mut self, options: ServiceOptions) -> Self {
        self.options = options;
        self
    }

    /// Adds `routes`, tried after those already added.
    #[must_use]
    pub fn with_routes(mut self, routes: impl Routes) -> Self {
        self.routes.push(Box::new(routes));
        self
    }

    #[must_use]
    pub fn options(&self) -> &ServiceOptions {
        &self.options
    }

    /// Answers `request`: with an error response if it fails, and with CORS
    /// headers if they are enabled.
    ///
    /// A spareval `CancellationToken` in the request's extensions cancels
    /// its query: an HTTP server can cancel it when the client goes away, as
    /// [`axum::router`](super::axum::router) does.
    pub fn handle(&self, request: &mut Request<RequestBody>) -> Response<ResponseBody> {
        if self.options.cors && *request.method() == Method::OPTIONS {
            return cors_preflight(request);
        }
        let mut response = self
            .try_handle(request)
            .unwrap_or_else(HttpError::into_response);
        if self.options.cors && request.headers().get(ORIGIN).is_some() {
            response
                .headers_mut()
                .append(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        }
        response
    }

    /// Answers `request`, or says why it can't. Unlike
    /// [`handle`](Self::handle), it adds no CORS headers.
    pub fn try_handle(
        &self,
        request: &mut Request<RequestBody>,
    ) -> Result<Response<ResponseBody>, HttpError> {
        debug!("{} {}", request.method(), request.uri().path());
        for routes in &self.routes {
            if let Some(answer) = routes.route(request) {
                return answer;
            }
        }
        let options = &self.options;
        let max_body_bytes = options.max_body_bytes;
        match (request.uri().path(), request.method().as_ref()) {
            ("/query", "GET") => {
                let query = url_query(request);
                if query.is_empty() {
                    let format = rdf_content_negotiation(request)?;
                    let description = generate_service_description(
                        format,
                        EndpointKind::Query,
                        options.union_default_graph,
                    )
                    .map_err(internal_server_error)?;
                    Response::builder()
                        .header(CONTENT_TYPE, format.media_type())
                        .body(description.into())
                        .map_err(internal_server_error)
                } else {
                    self.source.refresh()?;
                    configure_and_evaluate_sparql_query(
                        &self.source,
                        &[url_query(request)],
                        None,
                        request,
                        options,
                    )
                }
            }
            ("/query", "POST") => {
                self.source.refresh()?;
                let content_type =
                    content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
                if content_type == "application/sparql-query" {
                    let query = limited_string_body(request, max_body_bytes)?;
                    configure_and_evaluate_sparql_query(
                        &self.source,
                        &[url_query(request)],
                        Some(query),
                        request,
                        options,
                    )
                } else if content_type == "application/x-www-form-urlencoded" {
                    let buffer = limited_body(request, max_body_bytes)?;
                    configure_and_evaluate_sparql_query(
                        &self.source,
                        &[url_query(request), &buffer],
                        None,
                        request,
                        options,
                    )
                } else {
                    Err(unsupported_media_type(&content_type))
                }
            }
            ("/query", _) => Err(HttpError::method_not_allowed(
                request.method(),
                "/query",
                &[Method::GET, Method::POST],
            )),
            ("/update", _) => Err(not_implemented(
                "SPARQL Update is not supported by this server",
            )),
            (path, _method) if is_store_path(path) => Err(not_implemented(
                "Graph Store Protocol is not supported by this server",
            )),
            _ => Err(HttpError::new(
                StatusCode::NOT_FOUND,
                format!(
                    "{} {} is not supported by this server",
                    request.method(),
                    request.uri().path()
                ),
            )),
        }
    }
}

fn is_store_path(path: &str) -> bool {
    path == "/store" || path.starts_with("/store/")
}

/// The answer to a CORS preflight request: any origin, and the method and
/// headers it asks for.
fn cors_preflight(request: &Request<RequestBody>) -> Response<ResponseBody> {
    let mut response = Response::builder().status(StatusCode::NO_CONTENT);
    let request_headers = request.headers();
    if request_headers.get(ORIGIN).is_some() {
        response = response.header(
            ACCESS_CONTROL_ALLOW_ORIGIN.clone(),
            HeaderValue::from_static("*"),
        );
    }
    if let Some(method) = request_headers.get(ACCESS_CONTROL_REQUEST_METHOD) {
        response = response.header(ACCESS_CONTROL_ALLOW_METHODS, method.clone());
    }
    if let Some(headers) = request_headers.get(ACCESS_CONTROL_REQUEST_HEADERS) {
        response = response.header(ACCESS_CONTROL_ALLOW_HEADERS, headers.clone());
    }
    response.body(ResponseBody::Empty).unwrap_or_else(|e| {
        HttpError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to build CORS preflight response: {e}"),
        )
        .into_response()
    })
}
