use http::{
    header::{
        ACCEPT, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
        CONTENT_TYPE, LOCATION, ORIGIN,
    },
    uri::PathAndQuery,
    HeaderValue, Method, Request, Response, StatusCode,
};
use oxhttp::{model::Body, Server};
use oxiri::Iri;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, TripleRef};
use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};
use rand::random;
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{QueryEvaluator, QueryResults, QueryableDataset};
use spargebra::SparqlParser;
use std::str::FromStr;
use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::min,
    fmt,
    io::{self, BufWriter, Read, Write},
    net::ToSocketAddrs,
    path::Path,
    rc::Rc,
    thread::available_parallelism,
    time::Duration,
};
use url::form_urlencoded;

use crate::{
    service_description::{generate_service_description, EndpointKind},
    sparql::AggregateHdt,
};

type HttpError = (StatusCode, String);

const MAX_SPARQL_BODY_SIZE: u64 = 1024 * 1024 * 128; // 128MB
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const HTML_ROOT_PAGE: &str = include_str!("../templates/query.html");
#[expect(clippy::large_include_file)]
const YASGUI_JS: &str = include_str!("../templates/yasgui/yasgui.min.js");
const YASGUI_CSS: &str = include_str!("../templates/yasgui/yasgui.min.css");
const LOGO: &str = include_str!("../templates/logo.svg");

pub fn serve(
    locations: &str,
    bind: &str,
    // read_only: bool,
    // cors: bool,
    // union_default_graph: bool,
    // timeout_s: Option<u64>,
) -> anyhow::Result<()> {
    let union_default_graph = true;
    let cors = false;

    // Find all *.hdt files in the locations directory
    let hdt_paths: Vec<String> = std::fs::read_dir(locations)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "hdt" {
                Some(path.to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect();

    eprintln!("Found {} HDT files in {}", hdt_paths.len(), locations);
    for path in &hdt_paths {
        eprintln!("  - {}", path);
    }

    // Create the AggregateHdt store from the found HDT files
    let store = AggregateHdt::new(&hdt_paths)?;

    // let timeout = timeout_s.map(Duration::from_secs);
    let mut server = if cors {
        Server::new(cors_middleware(move |request| {
            handle_request(request, &store, union_default_graph)
                .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            handle_request(request, &store, union_default_graph)
                .unwrap_or_else(|(status, message)| error(status, message))
        })
    }
    .with_global_timeout(HTTP_TIMEOUT)
    .with_server_name(concat!("Oxigraph/", env!("CARGO_PKG_VERSION")))?
    .with_max_concurrent_connections(available_parallelism()?.get() * 128);
    for socket in bind.to_socket_addrs()? {
        server = server.bind(socket);
    }
    let server = server.spawn()?;
    #[cfg(target_os = "linux")]
    systemd_notify_ready()?;
    eprintln!("Listening for requests at http://{bind}");
    server.join()?;
    Ok(())
}

fn cors_middleware(
    on_request: impl Fn(&mut Request<Body>) -> Response<Body> + Send + Sync + 'static,
) -> impl Fn(&mut Request<Body>) -> Response<Body> + Send + Sync + 'static {
    move |request| {
        if *request.method() == Method::OPTIONS {
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
            response.body(Body::empty()).unwrap()
        } else {
            let mut response = on_request(request);
            if request.headers().get(ORIGIN).is_some() {
                response
                    .headers_mut()
                    .append(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            }
            response
        }
    }
}

fn handle_request(
    request: &mut Request<Body>,
    store: &AggregateHdt,
    // read_only: bool,
    union_default_graph: bool,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    println!("{}  {}", request.uri().path(), request.method().as_ref());
    match (request.uri().path(), request.method().as_ref()) {
        ("/", "HEAD") => Ok(Response::builder()
            .header(CONTENT_TYPE, "text/html")
            .body(Body::empty())
            .unwrap()),
        ("/", "GET") => Ok(Response::builder()
            .header(CONTENT_TYPE, "text/html")
            .body(HTML_ROOT_PAGE.into())
            .unwrap()),
        ("/yasgui.min.css", "HEAD") => Ok(Response::builder()
            .header(CONTENT_TYPE, "text/css")
            .body(Body::empty())
            .unwrap()),
        ("/yasgui.min.css", "GET") => Ok(Response::builder()
            .header(CONTENT_TYPE, "text/css")
            .body(YASGUI_CSS.into())
            .unwrap()),
        ("/yasgui.min.js", "HEAD") => Ok(Response::builder()
            .header(CONTENT_TYPE, "application/javascript")
            .body(Body::empty())
            .unwrap()),
        ("/yasgui.min.js", "GET") => Ok(Response::builder()
            .header(CONTENT_TYPE, "application/javascript")
            .body(YASGUI_JS.into())
            .unwrap()),
        ("/logo.svg", "HEAD") => Ok(Response::builder()
            .header(CONTENT_TYPE, "image/svg+xml")
            .body(Body::empty())
            .unwrap()),
        ("/logo.svg", "GET") => Ok(Response::builder()
            .header(CONTENT_TYPE, "image/svg+xml")
            .body(LOGO.into())
            .unwrap()),
        ("/query", "GET") => {
            let query = url_query(request);
            if query.is_empty() {
                let format = rdf_content_negotiation(request)?;
                let description =
                    generate_service_description(format, EndpointKind::Query, union_default_graph);
                Ok(Response::builder()
                    .header(CONTENT_TYPE, format.media_type())
                    .body(description.into())
                    .unwrap())
            } else {
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
        ("/update", "GET") => {
            // if read_only {
            //     return Err(the_server_is_read_only());
            // }
            let format = rdf_content_negotiation(request)?;
            let description =
                generate_service_description(format, EndpointKind::Update, union_default_graph);
            Ok(Response::builder()
                .header(CONTENT_TYPE, format.media_type())
                .body(description.into())
                .unwrap())
        }
        ("/update", "POST") => {
            // if read_only {
            //     return Err(the_server_is_read_only());
            // }
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if content_type == "application/sparql-update" {
                let update = limited_string_body(request)?;
                configure_and_evaluate_sparql_update(
                    store,
                    &[url_query(request)],
                    Some(update),
                    request,
                    union_default_graph,
                )
            } else if content_type == "application/x-www-form-urlencoded" {
                let buffer = limited_body(request)?;
                configure_and_evaluate_sparql_update(
                    store,
                    &[url_query(request), &buffer],
                    None,
                    request,
                    union_default_graph,
                )
            } else {
                Err(unsupported_media_type(&content_type))
            }
        }
        (path, "GET") if path.starts_with("/store") => {
            if let Some(target) = store_target(request)? {
                assert_that_graph_exists(store, &target)?;
                let format = rdf_content_negotiation(request)?;

                // TODO: Implement proper graph retrieval
                let triples: Vec<_> = store
                    .internal_quads_for_pattern(
                        None,
                        None,
                        None,
                        Some(Some(&GraphName::from(target).to_string())),
                    )
                    .collect();
                ReadForWrite::build_response(
                    move |w| {
                        Ok((
                            RdfSerializer::from_format(format).for_writer(w),
                            triples.into_iter(),
                        ))
                    },
                    |(mut serializer, mut triples_iter)| {
                        Ok(if let Some(triple) = triples_iter.next() {
                            let triple = triple?;
                            // Parse the triple parts into an RDF triple
                            let subject = NamedOrBlankNode::try_from(
                                oxrdf::Term::from_str(&triple.subject)
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            )
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e))
                            })?;
                            let predicate = NamedNode::try_from(
                                oxrdf::Term::from_str(&triple.predicate)
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            )
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e))
                            })?;
                            let object = oxrdf::Term::from_str(&triple.object)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                            let triple = oxrdf::Triple {
                                subject,
                                predicate,
                                object,
                            };
                            serializer.serialize_triple(&triple)?;
                            Some((serializer, triples_iter))
                        } else {
                            serializer.finish()?;
                            None
                        })
                    },
                    format.media_type(),
                )
            } else {
                let format = rdf_content_negotiation(request)?;
                if !format.supports_datasets() {
                    return Err(bad_request(format!(
                        "It is not possible to serialize the full RDF dataset using {format} that does not support named graphs"
                    )));
                }
                let triples = store.collect_all_triples();
                ReadForWrite::build_response(
                    move |w| {
                        Ok((
                            RdfSerializer::from_format(format).for_writer(w),
                            triples.into_iter(),
                        ))
                    },
                    |(mut serializer, mut triples_iter)| {
                        Ok(
                            if let Some((_graph_name, triple_parts)) = triples_iter.next() {
                                // Parse the triple parts into an RDF triple
                                let subject = NamedOrBlankNode::try_from(
                                    oxrdf::Term::from_str(&triple_parts[0]).map_err(|e| {
                                        io::Error::new(io::ErrorKind::InvalidData, e)
                                    })?,
                                )
                                .map_err(|e| {
                                    io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e))
                                })?;
                                let predicate = NamedNode::try_from(
                                    oxrdf::Term::from_str(&triple_parts[1]).map_err(|e| {
                                        io::Error::new(io::ErrorKind::InvalidData, e)
                                    })?,
                                )
                                .map_err(|e| {
                                    io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e))
                                })?;
                                let object = oxrdf::Term::from_str(&triple_parts[2])
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                                let triple = oxrdf::Triple {
                                    subject,
                                    predicate,
                                    object,
                                };
                                serializer.serialize_triple(&triple)?;
                                Some((serializer, triples_iter))
                            } else {
                                serializer.finish()?;
                                None
                            },
                        )
                    },
                    format.media_type(),
                )
            }
        }
        (path, "PUT") if path.starts_with("/store") => {
            // if read_only {
            //     return Err(the_server_is_read_only());
            // }
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if let Some(target) = store_target(request)? {
                let format = RdfFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                let p = web_load_graph(store, request, format, &GraphName::from(target.clone()))?;
                let new = !match &target {
                    NamedGraphName::NamedNode(target) => {
                        if store
                            .contains_named_graph(target)
                            .map_err(internal_server_error)?
                        {
                            store
                                .remove_named_graph(target)
                                .map_err(internal_server_error)?;
                            true
                        } else {
                            store
                                .insert_named_graph(target, Path::new(&p))
                                .map_err(internal_server_error)?;
                            false
                        }
                    }
                    NamedGraphName::DefaultGraph => return Err(internal_server_error("")),
                };

                Ok(Response::builder()
                    .status(if new {
                        StatusCode::CREATED
                    } else {
                        StatusCode::NO_CONTENT
                    })
                    .body(Body::empty())
                    .unwrap())
            } else {
                let format = RdfFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                store.clear().map_err(internal_server_error)?;
                web_load_dataset(store, request, format)?;
                Ok(Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::empty())
                    .unwrap())
            }
        }
        (path, "DELETE") if path.starts_with("/store") => {
            // if read_only {
            //     return Err(the_server_is_read_only());
            // }
            if let Some(target) = store_target(request)? {
                match target {
                    NamedGraphName::DefaultGraph => todo!(),
                    NamedGraphName::NamedNode(target) => {
                        if store
                            .contains_named_graph(&target)
                            .map_err(internal_server_error)?
                        {
                            store
                                .remove_named_graph(&target)
                                .map_err(internal_server_error)?;
                        } else {
                            return Err((
                                StatusCode::NOT_FOUND,
                                format!("The graph {target} does not exists"),
                            ));
                        }
                    }
                }
            } else {
                store.clear().map_err(internal_server_error)?;
            }
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap())
        }
        (path, "POST") if path.starts_with("/store") => {
            // if read_only {
            //     return Err(the_server_is_read_only());
            // }
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if let Some(target) = store_target(request)? {
                let format = RdfFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                let new = assert_that_graph_exists(store, &target).is_ok();
                web_load_graph(store, request, format, &GraphName::from(target))?;
                Ok(Response::builder()
                    .status(if new {
                        StatusCode::CREATED
                    } else {
                        StatusCode::NO_CONTENT
                    })
                    .body(Body::empty())
                    .unwrap())
            } else {
                let format = RdfFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                if format.supports_datasets() {
                    web_load_dataset(store, request, format)?;
                    Ok(Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .unwrap())
                } else {
                    let graph =
                        resolve_with_base(request, &format!("/store/{:x}", random::<u128>()))?;
                    web_load_graph(store, request, format, &graph.clone().into())?;
                    Ok(Response::builder()
                        .status(StatusCode::CREATED)
                        .header(LOCATION, graph.into_string())
                        .body(Body::empty())
                        .unwrap())
                }
            }
        }
        (path, "HEAD") if path.starts_with("/store") => {
            if let Some(target) = store_target(request)? {
                assert_that_graph_exists(store, &target)?;
            }
            Ok(Response::builder().body(Body::empty()).unwrap())
        }
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

fn base_url(request: &Request<Body>) -> String {
    let uri = request.uri();
    if uri.query().is_some() {
        // We remove the query
        let mut parts = uri.clone().into_parts();
        if let Some(path_and_query) = &mut parts.path_and_query {
            if path_and_query.query().is_some() {
                *path_and_query = PathAndQuery::try_from(path_and_query.path()).unwrap();
            }
        };
        http::Uri::from_parts(parts).unwrap().to_string()
    } else {
        uri.to_string()
    }
}

fn resolve_with_base(request: &Request<Body>, url: &str) -> Result<NamedNode, HttpError> {
    Ok(Iri::parse(base_url(request))
        .map_err(bad_request)?
        .resolve(url)
        .map_err(bad_request)?
        .into())
}

fn url_query(request: &Request<Body>) -> &[u8] {
    request.uri().query().unwrap_or_default().as_bytes()
}

fn url_query_parameter<'a>(request: &'a Request<Body>, param: &str) -> Option<Cow<'a, str>> {
    form_urlencoded::parse(url_query(request))
        .find(|(k, _)| k == param)
        .map(|(_, v)| v)
}

fn limited_string_body(request: &mut Request<Body>) -> Result<String, HttpError> {
    String::from_utf8(limited_body(request)?)
        .map_err(|e| bad_request(format!("Invalid UTF-8 body: {e}")))
}

fn limited_body(request: &mut Request<Body>) -> Result<Vec<u8>, HttpError> {
    let body = request.body_mut();
    if let Some(body_len) = body.len() {
        if body_len > MAX_SPARQL_BODY_SIZE {
            // it's too big
            return Err(bad_request(format!(
                "SPARQL body payloads are limited to {MAX_SPARQL_BODY_SIZE} bytes, found {body_len} bytes"
            )));
        }
        let mut payload = Vec::with_capacity(
            body_len
                .try_into()
                .map_err(|_| bad_request("Huge body size"))?,
        );
        body.read_to_end(&mut payload)
            .map_err(internal_server_error)?;
        Ok(payload)
    } else {
        let mut payload = Vec::new();
        body.take(MAX_SPARQL_BODY_SIZE + 1)
            .read_to_end(&mut payload)
            .map_err(internal_server_error)?;
        if payload.len() > MAX_SPARQL_BODY_SIZE.try_into().unwrap() {
            return Err(bad_request(format!(
                "SPARQL body payloads are limited to {MAX_SPARQL_BODY_SIZE} bytes"
            )));
        }
        Ok(payload)
    }
}

fn configure_and_evaluate_sparql_query(
    store: &AggregateHdt,
    encoded: &[&[u8]],
    mut query: Option<String>,
    request: &Request<Body>,
    default_use_default_graph_as_union: bool,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    let mut use_default_graph_as_union = false;
    for encoded in encoded {
        for (k, v) in form_urlencoded::parse(encoded) {
            match k.as_ref() {
                "query" => {
                    if query.is_some() {
                        return Err(bad_request("Multiple query parameters provided"));
                    }
                    query = Some(v.into_owned())
                }
                "default-graph-uri" => default_graph_uris.push(v.into_owned()),
                "union-default-graph" => use_default_graph_as_union = true,
                "named-graph-uri" => named_graph_uris.push(v.into_owned()),
                _ => (),
            }
        }
    }
    if default_graph_uris.is_empty() && named_graph_uris.is_empty() {
        use_default_graph_as_union |= default_use_default_graph_as_union;
    }
    let query = query.ok_or_else(|| bad_request("You should set the 'query' parameter"))?;
    evaluate_sparql_query(
        store,
        &query,
        use_default_graph_as_union,
        default_graph_uris,
        named_graph_uris,
        request,
        // timeout,
    )
}

fn evaluate_sparql_query(
    store: &AggregateHdt,
    query: &str,
    _use_default_graph_as_union: bool,
    _default_graph_uris: Vec<String>,
    _named_graph_uris: Vec<String>,
    request: &Request<Body>,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    // let mut evaluator = default_sparql_evaluator()
    //     .with_base_iri(base_url(request))
    //     .map_err(bad_request)?;

    let stuff = SparqlParser::new()
        .with_base_iri(base_url(request))
        .map_err(bad_request)?
        .parse_query(query)
        .map_err(bad_request)?;

    // if use_default_graph_as_union {
    //     if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
    //         return Err(bad_request(
    //             "default-graph-uri or named-graph-uri and union-default-graph should not be set at the same time",
    //         ));
    //     }
    //     prepared.dataset_mut().set_default_graph_as_union()
    // } else if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
    //     prepared.dataset_mut().set_default_graph(
    //         default_graph_uris
    //             .into_iter()
    //             .map(|e| Ok(NamedNode::new(e)?.into()))
    //             .collect::<Result<Vec<GraphName>, IriParseError>>()
    //             .map_err(bad_request)?,
    //     );
    //     prepared.dataset_mut().set_available_named_graphs(
    //         named_graph_uris
    //             .into_iter()
    //             .map(|e| Ok(NamedNode::new(e)?.into()))
    //             .collect::<Result<Vec<NamedOrBlankNode>, IriParseError>>()
    //             .map_err(bad_request)?,
    //     );
    // }

    let results = QueryEvaluator::new()
        .prepare(&stuff)
        .execute(store)
        .map_err(internal_server_error)?;
    match results {
        QueryResults::Solutions(solutions) => {
            let format = query_results_content_negotiation(request)?;
            // Collect variable names and solutions to avoid lifetime issues
            let variables = solutions.variables().to_vec();
            let solutions_vec: Vec<_> = solutions
                .collect::<Result<_, _>>()
                .map_err(internal_server_error)?;
            ReadForWrite::build_response(
                move |w| {
                    Ok((
                        QueryResultsSerializer::from_format(format)
                            .serialize_solutions_to_writer(w, variables)?,
                        solutions_vec.into_iter(),
                    ))
                },
                |(mut serializer, mut solutions_iter)| {
                    Ok(if let Some(solution) = solutions_iter.next() {
                        serializer.serialize(&solution)?;
                        Some((serializer, solutions_iter))
                    } else {
                        serializer.finish()?;
                        None
                    })
                },
                format.media_type(),
            )
        }
        QueryResults::Boolean(result) => {
            let format = query_results_content_negotiation(request)?;
            let mut body = Vec::new();
            QueryResultsSerializer::from_format(format)
                .serialize_boolean_to_writer(&mut body, result)
                .map_err(internal_server_error)?;
            Ok(Response::builder()
                .header(CONTENT_TYPE, format.media_type())
                .body(body.into())
                .unwrap())
        }
        QueryResults::Graph(triples) => {
            let format = rdf_content_negotiation(request)?;
            // Collect triples to avoid lifetime issues
            let triples: Vec<_> = triples
                .collect::<Result<_, _>>()
                .map_err(internal_server_error)?;
            ReadForWrite::build_response(
                move |w| {
                    Ok((
                        RdfSerializer::from_format(format).for_writer(w),
                        triples.into_iter(),
                    ))
                },
                |(mut serializer, mut triples_iter)| {
                    Ok(if let Some(t) = triples_iter.next() {
                        serializer.serialize_triple(&t)?;
                        Some((serializer, triples_iter))
                    } else {
                        serializer.finish()?;
                        None
                    })
                },
                format.media_type(),
            )
        }
    }
}

// spargebra re-exports oxrdf types, so Quad already contains oxrdf types
// No conversion functions needed!

fn configure_and_evaluate_sparql_update(
    store: &AggregateHdt,
    encoded: &[&[u8]],
    mut update: Option<String>,
    request: &Request<Body>,
    default_use_default_graph_as_union: bool,
) -> Result<Response<Body>, HttpError> {
    let mut use_default_graph_as_union = false;
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    for encoded in encoded {
        for (k, v) in form_urlencoded::parse(encoded) {
            match k.as_ref() {
                "update" => {
                    if update.is_some() {
                        return Err(bad_request("Multiple update parameters provided"));
                    }
                    update = Some(v.into_owned())
                }
                "using-graph-uri" => default_graph_uris.push(v.into_owned()),
                "using-union-graph" => use_default_graph_as_union = true,
                "using-named-graph-uri" => named_graph_uris.push(v.into_owned()),
                _ => (),
            }
        }
    }
    if default_graph_uris.is_empty() && named_graph_uris.is_empty() {
        use_default_graph_as_union |= default_use_default_graph_as_union;
    }
    let update = update.ok_or_else(|| bad_request("You should set the 'update' parameter"))?;
    evaluate_sparql_update(
        store,
        &update,
        use_default_graph_as_union,
        default_graph_uris,
        named_graph_uris,
        request,
    )
}

fn evaluate_sparql_update(
    store: &AggregateHdt,
    update: &str,
    _use_default_graph_as_union: bool,
    _default_graph_uris: Vec<String>,
    _named_graph_uris: Vec<String>,
    request: &Request<Body>,
) -> Result<Response<Body>, HttpError> {
    use spargebra::GraphUpdateOperation;

    let update_ops = spargebra::SparqlParser::new()
        .with_base_iri(base_url(request).as_str())
        .map_err(|e| bad_request(format!("Invalid base IRI: {}", e)))?
        .parse_update(update)
        .map_err(|e| bad_request(format!("Invalid SPARQL update: {}", e)))?;

    // Validate operations - only allow CREATE and INSERT DATA to new graphs
    // Reject any operations that modify existing graphs
    for op in &update_ops.operations {
        match op {
            // Allow CREATE - will be a no-op, just for SPARQL compliance
            GraphUpdateOperation::Create { graph, silent } => {
                // Check if graph already exists
                let exists = store
                    .contains_named_graph(graph)
                    .map_err(internal_server_error)?;

                if exists && !silent {
                    return Err(bad_request(format!("Graph {} already exists.", graph)));
                }
            }

            // Allow INSERT DATA - but only to new graphs
            GraphUpdateOperation::InsertData { data } => {
                use spargebra::term::GraphName as SparqlGraphName;
                use std::collections::HashSet;

                // Extract all graph names from the quads
                let mut graphs_used = HashSet::new();
                for quad in data {
                    match &quad.graph_name {
                        SparqlGraphName::NamedNode(graph) => {
                            graphs_used.insert(graph);
                        }
                        SparqlGraphName::DefaultGraph => {
                            return Err(bad_request(
                                "INSERT DATA to default graph is not allowed. Only named graphs are supported."
                            ));
                        }
                    }
                }

                // Check that all target graphs don't already exist
                for graph in graphs_used {
                    if store
                        .contains_named_graph(graph)
                        .map_err(internal_server_error)?
                    {
                        return Err(bad_request(format!(
                            "Graph {} already exists. INSERT DATA is only allowed to new graphs.",
                            graph
                        )));
                    }
                }
            }

            // Allow LOAD - but only to new graphs
            GraphUpdateOperation::Load {
                destination,
                source: _,
                silent,
            } => {
                use spargebra::term::GraphName as SparqlGraphName;

                if let SparqlGraphName::NamedNode(graph) = destination {
                    let exists = store
                        .contains_named_graph(graph)
                        .map_err(internal_server_error)?;

                    if exists && !silent {
                        return Err(bad_request(format!(
                            "Graph {} already exists. LOAD is only allowed to new graphs.",
                            graph
                        )));
                    }
                }
                // Note: LOAD to default graph is also rejected for safety
                else {
                    return Err(bad_request(
                        "LOAD to default graph is not allowed. Only named graphs can be created.",
                    ));
                }
            }

            // Reject all operations that modify existing data
            GraphUpdateOperation::DeleteData { .. } => {
                return Err(bad_request(
                    "DELETE DATA is not allowed. Only INSERT DATA to new graphs is permitted.",
                ));
            }

            GraphUpdateOperation::DeleteInsert { .. } => {
                return Err(bad_request(
                    "DELETE/INSERT operations are not allowed. Only INSERT DATA to new graphs is permitted."
                ));
            }

            GraphUpdateOperation::Clear { .. } => {
                return Err(bad_request(
                    "CLEAR is not allowed. Only INSERT DATA to new graphs is permitted.",
                ));
            }

            GraphUpdateOperation::Drop { .. } => {
                return Err(bad_request(
                    "DROP is not allowed. Only INSERT DATA to new graphs is permitted.",
                ));
            }
        }
    }

    // If validation passes, execute the allowed operations
    for op in &update_ops.operations {
        match op {
            GraphUpdateOperation::Create { graph, silent } => {
                // CREATE is a no-op - graph will be created on first INSERT DATA
                // Just verify it doesn't already exist (already checked in validation)
                let exists = store
                    .contains_named_graph(graph)
                    .map_err(internal_server_error)?;

                if !exists {
                    // Success - graph doesn't exist, ready for future INSERT
                    eprintln!("CREATE GRAPH {} - will be created on first INSERT", graph);
                } else if !silent {
                    return Err(bad_request(format!("Graph {} already exists", graph)));
                }
            }

            GraphUpdateOperation::InsertData { data } => {
                use spargebra::term::GraphName as SparqlGraphName;
                use std::collections::HashMap;

                // Group quads by graph name
                let mut quads_by_graph: HashMap<&NamedNode, Vec<&spargebra::term::Quad>> =
                    HashMap::new();
                for quad in data {
                    if let SparqlGraphName::NamedNode(graph) = &quad.graph_name {
                        quads_by_graph.entry(graph).or_default().push(quad);
                    }
                }

                // For each graph, create an NT file and convert to HDT
                for (graph, quads) in quads_by_graph {
                    let quad_count = quads.len();

                    // Create temporary NT file
                    let tmp_nt = tempfile::Builder::new()
                        .suffix(".nt")
                        .tempfile()
                        .map_err(internal_server_error)?;

                    let (nt_file, nt_path) = tmp_nt.keep().map_err(internal_server_error)?;
                    let mut nt_writer = BufWriter::new(&nt_file);

                    // Write quads as triples to NT file
                    let mut serializer = RdfSerializer::from_format(RdfFormat::NTriples)
                        .for_writer(nt_writer.by_ref());

                    for quad in quads {
                        // spargebra::term::Quad already contains oxrdf types, so we can use them directly
                        serializer
                            .serialize_triple(TripleRef::new(
                                quad.subject.as_ref(),
                                quad.predicate.as_ref(),
                                quad.object.as_ref(),
                            ))
                            .map_err(internal_server_error)?;
                    }

                    serializer.finish().map_err(internal_server_error)?;
                    drop(nt_writer);
                    drop(nt_file);

                    // Now convert NT to HDT and insert into store
                    // This will use the existing infrastructure that converts NT -> HDT
                    store
                        .insert_named_graph(graph, nt_path.as_path())
                        .map_err(|e| {
                            internal_server_error(format!(
                                "Failed to create graph {}: {}",
                                graph, e
                            ))
                        })?;

                    eprintln!("Created new graph {} with {} triples", graph, quad_count);
                }
            }

            GraphUpdateOperation::Load {
                destination,
                source: _,
                silent: _,
            } => {
                use spargebra::term::GraphName as SparqlGraphName;

                if let SparqlGraphName::NamedNode(_graph) = destination {
                    // LOAD operation is not yet implemented
                    // Would require: URL fetching, format detection, parsing, conversion to HDT
                    return Err(bad_request(
                        "LOAD operation is not yet implemented. Please use INSERT DATA or the /store endpoint with PUT to add new graphs."
                    ));
                } else {
                    return Err(bad_request("LOAD to default graph is not allowed"));
                }
            }

            _ => {
                // Should never reach here due to validation above
                return Err(internal_server_error(
                    "Unexpected operation passed validation",
                ));
            }
        }
    }

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

fn store_target(request: &Request<Body>) -> Result<Option<NamedGraphName>, HttpError> {
    if request.uri().path() == "/store" {
        if let Some(graph) = url_query_parameter(request, "graph") {
            if url_query_parameter(request, "default").is_some() {
                Err(bad_request(
                    "Both graph and default parameters should not be set at the same time",
                ))
            } else {
                Ok(Some(NamedGraphName::NamedNode(resolve_with_base(
                    request, &graph,
                )?)))
            }
        } else if url_query_parameter(request, "default").is_some() {
            Ok(Some(NamedGraphName::DefaultGraph))
        } else {
            Ok(None)
        }
    } else {
        Ok(Some(NamedGraphName::NamedNode(resolve_with_base(
            request, "",
        )?)))
    }
}

fn assert_that_graph_exists(
    store: &AggregateHdt,
    target: &NamedGraphName,
) -> Result<(), HttpError> {
    if match target {
        NamedGraphName::DefaultGraph => true,
        NamedGraphName::NamedNode(target) => store
            .contains_named_graph(target)
            .map_err(internal_server_error)?,
    } {
        Ok(())
    } else {
        Err((
            StatusCode::NOT_FOUND,
            format!(
                "The graph {} does not exists",
                GraphName::from(target.clone())
            ),
        ))
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
enum NamedGraphName {
    NamedNode(NamedNode),
    DefaultGraph,
}

impl From<NamedGraphName> for GraphName {
    fn from(graph_name: NamedGraphName) -> Self {
        match graph_name {
            NamedGraphName::NamedNode(node) => node.into(),
            NamedGraphName::DefaultGraph => Self::DefaultGraph,
        }
    }
}

fn rdf_content_negotiation(request: &Request<Body>) -> Result<RdfFormat, HttpError> {
    content_negotiation(
        request,
        RdfFormat::from_media_type,
        RdfFormat::NQuads,
        &[
            ("application", RdfFormat::NQuads),
            ("text", RdfFormat::NQuads),
        ],
        "application/n-quads or text/turtle",
    )
}

fn query_results_content_negotiation(
    request: &Request<Body>,
) -> Result<QueryResultsFormat, HttpError> {
    content_negotiation(
        request,
        QueryResultsFormat::from_media_type,
        QueryResultsFormat::Json,
        &[
            ("application", QueryResultsFormat::Json),
            ("text", QueryResultsFormat::Json),
        ],
        "application/sparql-results+json or text/tsv",
    )
}

fn content_negotiation<F: Copy>(
    request: &Request<Body>,
    parse: impl Fn(&str) -> Option<F>,
    default: F,
    default_by_base: &[(&str, F)],
    example: &str,
) -> Result<F, HttpError> {
    let header = request
        .headers()
        .get(ACCEPT)
        .map(|h| h.to_str())
        .transpose()
        .map_err(|_| bad_request("The Accept header should be a valid ASCII string"))?
        .unwrap_or_default();

    if header.is_empty() {
        println!(" default {ACCEPT}");
        return Ok(default);
    }
    println!("{ACCEPT} {header}");
    let mut result = None;
    let mut result_score = 0_f32;
    for mut possible in header.split(',') {
        let mut score = 1.;
        if let Some((possible_type, last_parameter)) = possible.rsplit_once(';') {
            if let Some((name, value)) = last_parameter.split_once('=') {
                if name.trim().eq_ignore_ascii_case("q") {
                    score = f32::from_str(value.trim()).map_err(|_| {
                        bad_request(format!("Invalid Accept media type score: {value}"))
                    })?;
                    possible = possible_type;
                }
            }
        }
        if score <= result_score {
            continue;
        }
        let (possible_base, possible_sub) = possible
            .split_once(';')
            .unwrap_or((possible, ""))
            .0
            .split_once('/')
            .ok_or_else(|| bad_request(format!("Invalid media type: '{possible}'")))?;
        let possible_base = possible_base.trim();
        let possible_sub = possible_sub.trim();

        let mut format = None;
        if possible_base == "*" && possible_sub == "*" {
            format = Some(default);
        } else if possible_sub == "*" {
            for (base, sub_format) in default_by_base {
                if *base == possible_base {
                    format = Some(*sub_format);
                }
            }
        } else {
            format = parse(possible);
        }
        if let Some(format) = format {
            result = Some(format);
            result_score = score;
        }
    }
    result.ok_or_else(|| {
        eprintln!(
            "Not Acceptable: the accept header does not provide any accepted format like {example}"
        );
        (
            StatusCode::NOT_ACCEPTABLE,
            format!("The accept header does not provide any accepted format like {example}"),
        )
    })
}

fn content_type(request: &Request<Body>) -> Option<String> {
    let value = request.headers().get(CONTENT_TYPE)?.to_str().ok()?;
    eprintln!("request content_type: {value}");
    Some(
        value
            .split_once(';')
            .map_or(value, |(b, _)| b)
            .trim()
            .to_ascii_lowercase(),
    )
}

fn web_load_graph(
    store: &AggregateHdt,
    request: &mut Request<Body>,
    format: RdfFormat,
    to_graph_name: &GraphName,
) -> Result<String, HttpError> {
    let base_iri = if let GraphName::NamedNode(graph_name) = to_graph_name {
        Some(graph_name.as_str())
    } else {
        None
    };
    let mut parser = RdfParser::from_format(format)
        .without_named_graphs()
        .with_default_graph(to_graph_name.clone());

    if let Some(base_iri) = base_iri {
        parser = parser.with_base_iri(base_iri).map_err(bad_request)?;
    }

    let quads = parser.for_reader(request.body_mut());
    let tmp_file = tempfile::Builder::new()
        .suffix(".nt")
        .tempfile()
        .map_err(|_| internal_server_error("message"))?;
    let (f, p) = tmp_file.keep().map_err(|_| internal_server_error(""))?;
    let mut dest_writer = BufWriter::new(&f);

    let mut serializer =
        RdfSerializer::from_format(RdfFormat::NTriples).for_writer(dest_writer.by_ref());

    for q in quads.flatten() {
        serializer
            .serialize_triple(TripleRef::new(
                q.subject.as_ref(),
                q.predicate.as_ref(),
                q.object.as_ref(),
            ))
            .map_err(|_| internal_server_error("message"))?
    }

    store
        .insert_named_graph(
            &NamedNode::from_str(base_iri.unwrap_or(&format!(
                "file:///{}",
                p.as_path().file_name().unwrap().to_str().unwrap()
            )))
            .map_err(|_| internal_server_error("message"))?,
            p.as_path(),
        )
        .map_err(|_| internal_server_error("message"))?;

    Ok(p.as_path().to_str().unwrap().to_string())
}

fn web_load_dataset(
    store: &AggregateHdt,
    request: &mut Request<Body>,
    format: RdfFormat,
) -> Result<String, HttpError> {
    web_load_graph(store, request, format, &GraphName::DefaultGraph)
}

// fn web_bulk_loader<'a>(store: &'a AggregateHdt, request: &Request<Body>) -> BulkLoader<'a> {
//     let start = Instant::now();
//     let mut loader = store.bulk_loader().on_progress(move |size| {
//         let elapsed = start.elapsed();
//         eprintln!(
//             "{} triples loaded in {}s ({} t/s)",
//             size,
//             elapsed.as_secs(),
//             ((size as f64) / elapsed.as_secs_f64()).round()
//         )
//     });
//     if url_query_parameter(request, "lenient").is_some() {
//         loader = loader.on_parse_error(move |e| {
//             eprintln!("Parsing error: {e}");
//             Ok(())
//         })
//     }
//     loader
// }

fn error(status: StatusCode, message: impl fmt::Display) -> Response<Body> {
    eprintln!("ERROR {status:?}: {message}");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(message.to_string().into())
        .unwrap()
}

fn bad_request(message: impl fmt::Display) -> HttpError {
    eprintln!("BAD REQUEST: {message}");
    (StatusCode::BAD_REQUEST, message.to_string())
}

#[allow(dead_code)]
fn the_server_is_read_only() -> HttpError {
    eprintln!("FORBIDDEN: readonly");
    (StatusCode::FORBIDDEN, "The server is read-only".into())
}

fn unsupported_media_type(content_type: &str) -> HttpError {
    eprintln!("Unsupported Media Type: {content_type}");
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!("No supported content Content-Type given: {content_type}"),
    )
}

fn internal_server_error(message: impl fmt::Display) -> HttpError {
    eprintln!("Internal server error: {message}");
    (StatusCode::INTERNAL_SERVER_ERROR, message.to_string())
}

// fn loader_to_http_error(e: LoaderError) -> HttpError {
//     match e {
//         LoaderError::Parsing(e) => bad_request(e),
//         LoaderError::Storage(e) => internal_server_error(e),
//         LoaderError::InvalidBaseIri { .. } => bad_request(e),
//     }
// }

/// Hacky tool to allow implementing read on top of a write loop
struct ReadForWrite<O, U: (Fn(O) -> io::Result<Option<O>>)> {
    buffer: Rc<RefCell<Vec<u8>>>,
    position: usize,
    add_more_data: U,
    state: Option<O>,
}

impl<O: 'static, U: (Fn(O) -> io::Result<Option<O>>) + 'static> ReadForWrite<O, U> {
    fn build_response(
        initial_state_builder: impl FnOnce(ReadForWriteWriter) -> io::Result<O>,
        add_more_data: U,
        content_type: &'static str,
    ) -> Result<Response<Body>, HttpError> {
        let buffer = Rc::new(RefCell::new(Vec::new()));
        let state = initial_state_builder(ReadForWriteWriter {
            buffer: Rc::clone(&buffer),
        })
        .map_err(internal_server_error)?;
        Response::builder()
            .header(CONTENT_TYPE, content_type)
            .body(Body::from_read(Self {
                buffer,
                position: 0,
                add_more_data,
                state: Some(state),
            }))
            .map_err(internal_server_error)
    }
}

impl<O, U: (Fn(O) -> io::Result<Option<O>>)> Read for ReadForWrite<O, U> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.position == self.buffer.borrow().len() {
            // We read more data
            if let Some(state) = self.state.take() {
                self.buffer.borrow_mut().clear();
                self.position = 0;
                self.state = match (self.add_more_data)(state) {
                    Ok(state) => state,
                    Err(e) => {
                        eprintln!("Internal server error while streaming results: {e}");
                        self.buffer
                            .borrow_mut()
                            .write_all(e.to_string().as_bytes())?;
                        None
                    }
                }
            } else {
                return Ok(0); // End
            }
        }
        let buffer = self.buffer.borrow();
        let len = min(buffer.len() - self.position, buf.len());
        buf[..len].copy_from_slice(&buffer[self.position..self.position + len]);
        self.position += len;
        Ok(len)
    }
}

struct ReadForWriteWriter {
    buffer: Rc<RefCell<Vec<u8>>>,
}

impl Write for ReadForWriteWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.buffer.borrow_mut().write_all(buf)
    }
}

#[cfg(target_os = "linux")]
fn systemd_notify_ready() -> io::Result<()> {
    use std::env;

    if let Some(path) = env::var_os("NOTIFY_SOCKET") {
        use std::os::unix::net::UnixDatagram;

        UnixDatagram::unbound()?.send_to(b"READY=1", path)?;
    }
    Ok(())
}
