// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! The protocol cases: requests, and what a conforming server answers.
//!
//! Each case's id says where it comes from:
//!
//! * `w3c/…` — the query cases of the W3C SPARQL 1.1 Protocol tests
//!   (`rdf-tests/sparql/sparql11/protocol/manifest.ttl`). They are written in
//!   prose, against RDF documents on kasei.us that the server should fetch.
//!   Here the documents are fixture graphs served under the same IRIs, and two
//!   cases whose expected answer is a copy-paste slip upstream check what
//!   their names describe instead (noted on each).
//! * `oxigraph/…` — the HTTP tests of `oxigraph serve` (`cli/src/main.rs`)
//!   that apply to a read-only endpoint, which is where this server code
//!   started. They read the default graph of an empty store; here queries use
//!   `VALUES` so the fixtures don't change their answers.
//! * `de/…` — behaviour the protocol requires that the W3C cases don't pin
//!   down, found by reading the server code.
//! * `readonly/…` — a server without SPARQL Update or the Graph Store
//!   Protocol answers 501 on those endpoints.

use crate::fixtures::{DATA1, DATA2, DATA3, FOAF_DOCUMENT, OTHER};
use crate::report::Outcome;
use crate::target::Target;
use crate::{Capabilities, ServiceSettings};
use http::header::{ACCEPT, ALLOW, CONTENT_TYPE};
use http::{HeaderMap, Method, Request, StatusCode};
use oxrdfio::{RdfFormat, RdfParser};
use sparesults::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput};
use std::collections::BTreeSet;
use std::io::Read;
use std::sync::Arc;

pub(crate) const RESULTS_JSON: &str = "application/sparql-results+json";
pub(crate) const RESULTS_XML: &str = "application/sparql-results+xml";
pub(crate) const RESULTS_CSV: &str = "text/csv";
pub(crate) const RESULTS_TSV: &str = "text/tab-separated-values";
const SPARQL_QUERY: &str = "application/sparql-query";
const SPARQL_UPDATE: &str = "application/sparql-update";
const FORM: &str = "application/x-www-form-urlencoded";

/// A query with three variables and no solutions, whatever the data.
const EMPTY_SELECT: &str = "SELECT ?s ?p ?o WHERE { VALUES ?s {} }";

/// What a case runs against.
pub(crate) struct Ctx<'a> {
    pub target: &'a Arc<dyn Target>,
    pub settings: &'a ServiceSettings,
    pub allocator: Option<&'static crate::PeakAlloc>,
}

pub(crate) struct Case {
    pub id: &'static str,
    pub description: &'static str,
    /// `Some(reason)` when the case doesn't apply to a server with these capabilities.
    pub skip_unless: fn(&Capabilities) -> Option<&'static str>,
    pub run: fn(&Ctx<'_>) -> Outcome,
}

fn always(_: &Capabilities) -> Option<&'static str> {
    None
}

fn union_default(c: &Capabilities) -> Option<&'static str> {
    (!c.union_default_graph).then_some("the server's default graph isn't the union of its graphs")
}

fn without_update(c: &Capabilities) -> Option<&'static str> {
    c.update.then_some("the server supports SPARQL Update")
}

fn without_graph_store(c: &Capabilities) -> Option<&'static str> {
    c.graph_store
        .then_some("the server supports the Graph Store Protocol")
}

fn with_cors(c: &Capabilities) -> Option<&'static str> {
    (!c.cors).then_some("the server doesn't enable CORS")
}

/// Turns a check into an outcome.
pub(crate) fn outcome(result: Result<(), String>) -> Outcome {
    match result {
        Ok(()) => Outcome::Pass(String::new()),
        Err(why) => Outcome::Fail(why),
    }
}

macro_rules! case {
    ($id:literal, $description:literal, $skip:expr, $run:expr) => {
        Case {
            id: $id,
            description: $description,
            skip_unless: $skip,
            run: |ctx| outcome($run(ctx)),
        }
    };
}

// A table of cases; splitting it would only scatter it.
#[allow(clippy::too_many_lines)]
pub(crate) fn protocol_cases() -> Vec<Case> {
    vec![
        // W3C SPARQL 1.1 Protocol, query operation.
        case!(
            "w3c/query_post_form",
            "query via URL-encoded POST",
            always,
            |ctx| {
                ask_true(&send(
                    ctx,
                    post("/query", Some(FORM), "query=ASK%20%7B%7D"),
                )?)
            }
        ),
        case!(
            "w3c/query_dataset_default_graphs_get",
            "GET query with protocol-specified default graphs",
            always,
            |ctx| {
                let query = format!("ASK {{ <{DATA1}> a ?type . <{DATA2}> a ?type . }}");
                let uri = query_uri(
                    &query,
                    &[("default-graph-uri", DATA1), ("default-graph-uri", DATA2)],
                );
                ask_true(&send(ctx, get(&uri, &[]))?)
            }
        ),
        case!(
            "w3c/query_dataset_default_graphs_post",
            "POST query with protocol-specified default graphs",
            always,
            |ctx| {
                let uri = params_uri(
                    "/query",
                    &[("default-graph-uri", DATA1), ("default-graph-uri", DATA2)],
                );
                let query = format!("ASK {{ <{DATA1}> ?p ?o . <{DATA2}> ?p ?o }}");
                ask_true(&send(ctx, post(&uri, Some(SPARQL_QUERY), &query))?)
            }
        ),
        case!(
            "w3c/query_dataset_named_graphs_post",
            "POST query with protocol-specified named graphs",
            always,
            |ctx| {
                let uri = params_uri(
                    "/query",
                    &[("named-graph-uri", DATA1), ("named-graph-uri", DATA2)],
                );
                ask_true(&send(
                    ctx,
                    post(&uri, Some(SPARQL_QUERY), "ASK { GRAPH ?g { ?s ?p ?o } }"),
                )?)
            }
        ),
        case!(
            "w3c/query_dataset_named_graphs_get",
            "GET query with protocol-specified named graphs",
            always,
            |ctx| {
                let uri = query_uri(
                    "ASK { GRAPH ?g { ?s ?p ?o } }",
                    &[("named-graph-uri", DATA1), ("named-graph-uri", DATA2)],
                );
                ask_true(&send(ctx, get(&uri, &[]))?)
            }
        ),
        // Upstream expects `true` from a SELECT, a copy-paste slip. This checks
        // the solutions the protocol-specified dataset gives.
        case!(
            "w3c/query_dataset_full",
            "query with protocol-specified dataset (both named and default graphs)",
            always,
            |ctx| {
                let uri = params_uri(
                    "/query",
                    &[
                        ("default-graph-uri", DATA3),
                        ("named-graph-uri", DATA1),
                        ("named-graph-uri", DATA2),
                    ],
                );
                let answer = send(
                    ctx,
                    post(
                        &uri,
                        Some(SPARQL_QUERY),
                        "SELECT ?g ?x ?s { ?x ?y ?o GRAPH ?g { ?s ?p ?o } }",
                    ),
                )?;
                answer.expect_success()?;
                let (_, rows) = solutions(&answer)?;
                let expected = vec![
                    vec![iri(DATA1), iri(DATA3), iri(DATA1)],
                    vec![iri(DATA2), iri(DATA3), iri(DATA2)],
                ];
                if rows == expected {
                    Ok(())
                } else {
                    Err(format!(
                        "solutions (?g ?x ?s) were {rows:?}, expected {expected:?}: the default graph must be \
                         data3 alone and the named graphs data1 and data2"
                    ))
                }
            }
        ),
        // Upstream's query names `<data1.rdf>`, a relative IRI that resolves
        // against the endpoint, so its expected `true` can't hold. This asks
        // what the case's name says: the protocol's dataset replaces FROM.
        case!(
            "w3c/query_multiple_dataset",
            "query specifying dataset in both query string and protocol; the protocol's dataset wins",
            always,
            |ctx| {
                let uri = params_uri("/query", &[("default-graph-uri", DATA2)]);
                let query = format!("ASK FROM <{DATA1}> {{ <{DATA2}> ?p ?o }}");
                ask_true(&send(ctx, post(&uri, Some(SPARQL_QUERY), &query))?)
            }
        ),
        case!("w3c/query_get", "query via GET", always, |ctx| {
            ask_true(&send(ctx, get("/query?query=ASK%20%7B%7D", &[]))?)
        }),
        case!(
            "w3c/query_content_type_select",
            "query appropriate content type (expect one of: XML, JSON, CSV, TSV)",
            always,
            |ctx| {
                let answer = send(
                    ctx,
                    post("/query", Some(SPARQL_QUERY), "SELECT (1 AS ?value) {}"),
                )?;
                answer.expect_success()?;
                answer.expect_media_type(&[RESULTS_XML, RESULTS_JSON, RESULTS_TSV, RESULTS_CSV])
            }
        ),
        case!(
            "w3c/query_content_type_ask",
            "query appropriate content type (expect one of: XML, JSON)",
            always,
            |ctx| {
                let answer = send(ctx, post("/query", Some(SPARQL_QUERY), "ASK {}"))?;
                answer.expect_success()?;
                answer.expect_media_type(&[RESULTS_XML, RESULTS_JSON])
            }
        ),
        case!(
            "w3c/query_content_type_describe",
            "query appropriate content type (expect one of: RDF/XML, Turtle, N-Triples, RDFa)",
            always,
            |ctx| {
                let answer = send(
                    ctx,
                    post(
                        "/query",
                        Some(SPARQL_QUERY),
                        "DESCRIBE <http://example.org/>",
                    ),
                )?;
                answer.expect_success()?;
                answer.expect_media_type(GRAPH_MEDIA_TYPES)
            }
        ),
        case!(
            "w3c/query_content_type_construct",
            "query appropriate content type (expect one of: RDF/XML, Turtle, N-Triples, RDFa)",
            always,
            |ctx| {
                let answer = send(
                    ctx,
                    post(
                        "/query",
                        Some(SPARQL_QUERY),
                        "CONSTRUCT { <s> <p> 1 } WHERE {}",
                    ),
                )?;
                answer.expect_success()?;
                answer.expect_media_type(GRAPH_MEDIA_TYPES)
            }
        ),
        case!(
            "w3c/query_post_direct",
            "query via POST directly",
            always,
            |ctx| { ask_true(&send(ctx, post("/query", Some(SPARQL_QUERY), "ASK {}"))?) }
        ),
        case!(
            "w3c/bad_query_method",
            "invoke query operation with a method other than GET or POST",
            always,
            |ctx| send(
                ctx,
                request(Method::PUT, "/query?query=ASK%20%7B%7D", &[], Vec::new())
            )?
            .expect_client_error()
        ),
        case!(
            "w3c/bad_multiple_queries",
            "invoke query operation with more than one query string",
            always,
            |ctx| send(
                ctx,
                get("/query?query=ASK%20%7B%7D&query=SELECT%20%2A%20%7B%7D", &[])
            )?
            .expect_client_error()
        ),
        case!(
            "w3c/bad_query_wrong_media_type",
            "invoke query operation with a POST with media type that's not url-encoded or application/sparql-query",
            always,
            |ctx| send(ctx, post("/query", Some("text/plain"), "ASK {}"))?.expect_client_error()
        ),
        case!(
            "w3c/bad_query_missing_form_type",
            "invoke query operation with url-encoded body, but without application/x-www-form-urlencoded media type",
            always,
            |ctx| send(ctx, post("/query", None, "query=ASK%20%7B%7D"))?.expect_client_error()
        ),
        case!(
            "w3c/bad_query_missing_direct_type",
            "invoke query operation with SPARQL body, but without application/sparql-query media type",
            always,
            |ctx| send(ctx, post("/query", None, "ASK {}"))?.expect_client_error()
        ),
        case!(
            "w3c/bad_query_non_utf8",
            "invoke query operation with direct POST, but with a non-UTF8 encoding (UTF-16)",
            always,
            |ctx| {
                let mut body = vec![0xFE, 0xFF];
                body.extend("ASK {}".encode_utf16().flat_map(u16::to_be_bytes));
                let headers = [(
                    CONTENT_TYPE.as_str(),
                    "application/sparql-query; charset=UTF-16",
                )];
                send(ctx, request(Method::POST, "/query", &headers, body))?.expect_client_error()
            }
        ),
        case!(
            "w3c/bad_query_syntax",
            "invoke query operation with invalid query syntax (4XX result)",
            always,
            |ctx| send(ctx, get("/query?query=ASK%20%7B", &[]))?.expect_client_error()
        ),
        // oxigraph serve's HTTP tests.
        case!(
            "oxigraph/get_query",
            "GET a query with Accept: text/csv",
            always,
            |ctx| {
                let query = "SELECT ?s ?p ?o WHERE { VALUES (?s ?p ?o) { (<http://example.com> <http://example.com> <http://example.com>) } }";
                let answer = send(
                    ctx,
                    get(&query_uri(query, &[]), &[(ACCEPT.as_str(), RESULTS_CSV)]),
                )?;
                answer.expect_body(
                    "s,p,o\r\nhttp://example.com,http://example.com,http://example.com\r\n",
                )
            }
        ),
        case!(
            "oxigraph/get_query_accept_star",
            "Accept: */* gets JSON results",
            always,
            |ctx| { empty_select_as(ctx, "*/*", RESULTS_JSON) }
        ),
        case!(
            "oxigraph/get_query_accept_substar",
            "Accept: text/* gets JSON results",
            always,
            |ctx| { empty_select_as(ctx, "text/*", RESULTS_JSON) }
        ),
        case!(
            "oxigraph/get_query_accept_good",
            "Accept with a charset parameter",
            always,
            |ctx| empty_select_as(
                ctx,
                "application/sparql-results+json;charset=utf-8",
                RESULTS_JSON
            )
        ),
        case!(
            "oxigraph/get_query_accept_bad",
            "an unknown Accept is 406",
            always,
            |ctx| {
                send(
                    ctx,
                    get(
                        &query_uri(EMPTY_SELECT, &[]),
                        &[(ACCEPT.as_str(), "application/foo")],
                    ),
                )?
                .expect_status(StatusCode::NOT_ACCEPTABLE)
            }
        ),
        case!(
            "oxigraph/get_query_accept_explicit_priority",
            "the Accept type with the higher q wins",
            always,
            |ctx| empty_select_as(ctx, "text/foo;q=0.5 , text/json ; q = 0.7", RESULTS_JSON)
        ),
        case!(
            "oxigraph/get_query_accept_implicit_priority",
            "a supported Accept type wins over an unknown one",
            always,
            |ctx| empty_select_as(ctx, "text/json,text/foo", RESULTS_JSON)
        ),
        case!(
            "oxigraph/get_query_accept_implicit_and_explicit_priority",
            "q defaults to 1",
            always,
            |ctx| {
                let answer = send(
                    ctx,
                    get(
                        &query_uri(EMPTY_SELECT, &[]),
                        &[(ACCEPT.as_str(), "text/foo;q=0.9,text/csv")],
                    ),
                )?;
                answer.expect_body("s,p,o\r\n")
            }
        ),
        case!(
            "oxigraph/get_bad_query",
            "GET an invalid query",
            always,
            |ctx| {
                send(ctx, get("/query?query=SELECT", &[]))?.expect_status(StatusCode::BAD_REQUEST)
            }
        ),
        case!(
            "oxigraph/get_query_union_graph",
            "union-default-graph makes the default graph the union of all graphs",
            always,
            |ctx| {
                let uri = format!("{}&union-default-graph", query_uri(&documents_query(), &[]));
                expect_all_documents(&send(ctx, get(&uri, &[(ACCEPT.as_str(), RESULTS_JSON)]))?)
            }
        ),
        case!(
            "oxigraph/get_query_union_graph_in_url_and_urlencoded",
            "union-default-graph in the URL, query in a form body",
            always,
            |ctx| {
                let body = format!("query={}", encode(&documents_query()));
                let mut req = post("/query?union-default-graph", Some(FORM), &body);
                req.headers_mut()
                    .insert(ACCEPT, RESULTS_JSON.parse().map_err(|e| format!("{e}"))?);
                expect_all_documents(&send(ctx, req)?)
            }
        ),
        case!(
            "oxigraph/get_query_union_graph_and_default_graph",
            "union-default-graph together with default-graph-uri is 400",
            always,
            |ctx| {
                let uri = format!(
                    "{}&union-default-graph",
                    query_uri(&documents_query(), &[("default-graph-uri", DATA1)])
                );
                send(ctx, get(&uri, &[]))?.expect_status(StatusCode::BAD_REQUEST)
            }
        ),
        case!(
            "oxigraph/get_query_description",
            "GET /query without a query is the service description",
            always,
            |ctx| {
                let answer = send(ctx, get("/query", &[(ACCEPT.as_str(), "text/turtle")]))?;
                answer.expect_status(StatusCode::OK)?;
                answer.expect_media_type(&["text/turtle"])?;
                triples(&answer).map(|_| ())
            }
        ),
        case!("oxigraph/post_query", "POST a query", always, |ctx| {
            send(
                ctx,
                post("/query", Some(SPARQL_QUERY), "SELECT * WHERE { ?s ?p ?o }"),
            )?
            .expect_status(StatusCode::OK)
        }),
        case!(
            "oxigraph/post_bad_query",
            "POST an invalid query",
            always,
            |ctx| {
                send(ctx, post("/query", Some(SPARQL_QUERY), "SELECT"))?
                    .expect_status(StatusCode::BAD_REQUEST)
            }
        ),
        case!(
            "oxigraph/post_unknown_query",
            "POST an unknown body type",
            always,
            |ctx| {
                send(
                    ctx,
                    post("/query", Some("application/sparql-todo"), "SELECT"),
                )?
                .expect_status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
            }
        ),
        // Found by reading the server code.
        case!(
            "de/default_dataset_is_union",
            "with no dataset in the request, the default graph is the union of every graph",
            union_default,
            |ctx| {
                let query = format!(
                    "ASK {{ <{DATA1}> a <{FOAF_DOCUMENT}> . <{DATA2}> a <{FOAF_DOCUMENT}> . \
                     <{DATA3}> a <{FOAF_DOCUMENT}> . <{OTHER}> a <{FOAF_DOCUMENT}> }}"
                );
                ask_true(&send(ctx, get(&query_uri(&query, &[]), &[]))?)
            }
        ),
        case!(
            "de/named_graph_uri_alone_leaves_default_graph_empty",
            "named-graph-uri without default-graph-uri gives an empty default graph",
            always,
            |ctx| {
                let uri = query_uri("ASK { ?s ?p ?o }", &[("named-graph-uri", DATA1)]);
                let answer = send(ctx, get(&uri, &[]))?;
                answer.expect_success()?;
                if boolean(&answer)? {
                    Err("found triples in the default graph: only data1 was named, as a named graph".to_string())
                } else {
                    Ok(())
                }
            }
        ),
        case!(
            "de/evaluation_error_has_error_status",
            "a query that fails before its first result gets an error status, not a cut-off 200",
            always,
            |ctx| {
                let query = "SELECT * WHERE { SERVICE <http://127.0.0.1:9/sparql> { ?s ?p ?o } }";
                let answer = send(ctx, get(&query_uri(query, &[]), &[]))?;
                match (&answer.body, answer.status.is_success()) {
                    (_, false) => Ok(()),
                    (Err(cut), true) => Err(format!(
                        "answered {} and then cut the body off: {cut}",
                        answer.status
                    )),
                    (Ok(body), true) => Err(format!(
                        "answered {} with a complete body: {}",
                        answer.status,
                        String::from_utf8_lossy(&body[..body.len().min(200)])
                    )),
                }
            }
        ),
        case!(
            "de/wrong_method_is_405",
            "PUT on /query is 405 Method Not Allowed, with an Allow header",
            always,
            |ctx| {
                let answer = send(
                    ctx,
                    request(Method::PUT, "/query?query=ASK%20%7B%7D", &[], Vec::new()),
                )?;
                answer.expect_status(StatusCode::METHOD_NOT_ALLOWED)?;
                let allow = answer
                    .headers
                    .get(ALLOW)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                if allow.contains("GET") && allow.contains("POST") {
                    Ok(())
                } else {
                    Err(format!("Allow is {allow:?}, expected GET and POST"))
                }
            }
        ),
        case!(
            "de/service_description",
            "the service description says it's a SPARQL 1.1 query service",
            always,
            |ctx| {
                let answer = send(
                    ctx,
                    get("/query", &[(ACCEPT.as_str(), "application/n-triples")]),
                )?;
                answer.expect_status(StatusCode::OK)?;
                let triples = triples(&answer)?;
                let sd = "http://www.w3.org/ns/sparql-service-description#";
                for needed in [
                    format!("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{sd}Service>"),
                    format!("<{sd}supportedLanguage> <{sd}SPARQL11Query>"),
                ] {
                    if !triples.iter().any(|t| t.contains(&needed)) {
                        return Err(format!("no triple with {needed} in {triples:?}"));
                    }
                }
                Ok(())
            }
        ),
        // CORS, for a server that enables it.
        case!(
            "cors/preflight",
            "an OPTIONS preflight from another origin is allowed",
            with_cors,
            |ctx| {
                let headers = [
                    ("origin", "http://example.com"),
                    ("access-control-request-method", "POST"),
                    ("access-control-request-headers", "content-type"),
                ];
                let answer = send(
                    ctx,
                    request(Method::OPTIONS, "/query", &headers, Vec::new()),
                )?;
                if !answer.status.is_success() {
                    return Err(format!("expected 2xx, got {}", answer.describe()));
                }
                expect_header(&answer, "access-control-allow-origin", |v| {
                    v == "*" || v == "http://example.com"
                })?;
                // POST is a CORS-safelisted method, so a preflight for it may
                // leave Access-Control-Allow-Methods out (and oxhttp drops it).
                if answer.headers.contains_key("access-control-allow-methods") {
                    expect_header(&answer, "access-control-allow-methods", |v| {
                        v.to_ascii_uppercase().contains("POST")
                    })?;
                }
                expect_header(&answer, "access-control-allow-headers", |v| {
                    v.to_ascii_lowercase().contains("content-type")
                })
            }
        ),
        case!(
            "cors/query",
            "a query from another origin is allowed",
            with_cors,
            |ctx| {
                let answer = send(
                    ctx,
                    get(
                        "/query?query=ASK%20%7B%7D",
                        &[("origin", "http://example.com")],
                    ),
                )?;
                answer.expect_success()?;
                expect_header(&answer, "access-control-allow-origin", |v| {
                    v == "*" || v == "http://example.com"
                })
            }
        ),
        // A server without SPARQL Update or the Graph Store Protocol.
        case!(
            "readonly/update_post",
            "POST /update is 501",
            without_update,
            |ctx| {
                let update = "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }";
                send(ctx, post("/update", Some(SPARQL_UPDATE), update))?
                    .expect_status(StatusCode::NOT_IMPLEMENTED)
            }
        ),
        case!(
            "readonly/update_get",
            "GET /update is 501",
            without_update,
            |ctx| { send(ctx, get("/update", &[]))?.expect_status(StatusCode::NOT_IMPLEMENTED) }
        ),
        case!(
            "readonly/store_get",
            "GET /store is 501",
            without_graph_store,
            |ctx| {
                let uri = params_uri("/store", &[("graph", DATA1)]);
                send(ctx, get(&uri, &[]))?.expect_status(StatusCode::NOT_IMPLEMENTED)
            }
        ),
        case!(
            "readonly/store_head",
            "HEAD /store is 501",
            without_graph_store,
            |ctx| {
                let uri = params_uri("/store", &[("graph", DATA1)]);
                send(ctx, request(Method::HEAD, &uri, &[], Vec::new()))?
                    .expect_status(StatusCode::NOT_IMPLEMENTED)
            }
        ),
        case!(
            "readonly/store_put",
            "PUT /store is 501",
            without_graph_store,
            |ctx| {
                let uri = params_uri("/store", &[("graph", "http://example.org/new")]);
                let body = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";
                send(
                    ctx,
                    request(
                        Method::PUT,
                        &uri,
                        &[(CONTENT_TYPE.as_str(), "application/n-triples")],
                        body.into(),
                    ),
                )?
                .expect_status(StatusCode::NOT_IMPLEMENTED)
            }
        ),
        case!(
            "readonly/store_post",
            "POST /store is 501",
            without_graph_store,
            |ctx| {
                let body = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .";
                send(ctx, post("/store", Some("application/n-triples"), body))?
                    .expect_status(StatusCode::NOT_IMPLEMENTED)
            }
        ),
        case!(
            "readonly/store_delete",
            "DELETE /store is 501",
            without_graph_store,
            |ctx| {
                let uri = params_uri("/store", &[("graph", DATA1)]);
                send(ctx, request(Method::DELETE, &uri, &[], Vec::new()))?
                    .expect_status(StatusCode::NOT_IMPLEMENTED)
            }
        ),
    ]
}

const GRAPH_MEDIA_TYPES: &[&str] = &[
    "application/rdf+xml",
    "application/rdf+json",
    "text/turtle",
    "application/n-triples",
];

/// `SELECT ?doc WHERE { ?doc a foaf:Document }`: one row per fixture graph.
fn documents_query() -> String {
    format!("SELECT ?doc WHERE {{ ?doc a <{FOAF_DOCUMENT}> }}")
}

fn expect_all_documents(answer: &Answer) -> Result<(), String> {
    answer.expect_success()?;
    let (_, rows) = solutions(answer)?;
    let mut expected: Vec<Vec<String>> = [DATA1, DATA2, DATA3, OTHER]
        .iter()
        .map(|g| vec![iri(g)])
        .collect();
    expected.sort();
    if rows == expected {
        Ok(())
    } else {
        Err(format!("rows were {rows:?}, expected {expected:?}"))
    }
}

fn empty_select_as(ctx: &Ctx<'_>, accept: &str, media_type: &str) -> Result<(), String> {
    let answer = send(
        ctx,
        get(&query_uri(EMPTY_SELECT, &[]), &[(ACCEPT.as_str(), accept)]),
    )?;
    answer.expect_status(StatusCode::OK)?;
    answer.expect_media_type(&[media_type])?;
    let (variables, rows) = solutions(&answer)?;
    if variables == ["s", "p", "o"] && rows.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "got variables {variables:?} and rows {rows:?}, expected s, p, o and no rows"
        ))
    }
}

fn ask_true(answer: &Answer) -> Result<(), String> {
    answer.expect_success()?;
    answer.expect_media_type(&[RESULTS_XML, RESULTS_JSON])?;
    if boolean(answer)? {
        Ok(())
    } else {
        Err("answered false, expected true".to_string())
    }
}

fn expect_header(answer: &Answer, name: &str, accept: fn(&str) -> bool) -> Result<(), String> {
    match answer.headers.get(name).map(|v| v.to_str()) {
        Some(Ok(value)) if accept(value) => Ok(()),
        Some(Ok(value)) => Err(format!("{name} was {value:?}")),
        _ => Err(format!("no {name} header")),
    }
}

fn iri(value: &str) -> String {
    format!("<{value}>")
}

/// A response with its body read to the end, or the error that cut it off.
pub(crate) struct Answer {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Result<Vec<u8>, String>,
}

impl Answer {
    pub fn media_type(&self) -> String {
        self.headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
    }

    fn body(&self) -> Result<&[u8], String> {
        self.body
            .as_deref()
            .map_err(|cut| format!("the {} response's body was cut off: {cut}", self.status))
    }

    fn describe(&self) -> String {
        match &self.body {
            Ok(body) => format!(
                "{}: {}",
                self.status,
                String::from_utf8_lossy(&body[..body.len().min(300)]).trim()
            ),
            Err(cut) => format!("{} with a body cut off: {cut}", self.status),
        }
    }

    pub fn expect_status(&self, status: StatusCode) -> Result<(), String> {
        if self.status == status {
            Ok(())
        } else {
            Err(format!("expected {status}, got {}", self.describe()))
        }
    }

    /// 2xx or 3xx, as the W3C cases put it, with a complete body.
    pub fn expect_success(&self) -> Result<(), String> {
        if !(self.status.is_success() || self.status.is_redirection()) {
            return Err(format!("expected 2xx or 3xx, got {}", self.describe()));
        }
        self.body().map(|_| ())
    }

    fn expect_client_error(&self) -> Result<(), String> {
        if self.status.is_client_error() {
            Ok(())
        } else {
            Err(format!("expected 4xx, got {}", self.describe()))
        }
    }

    fn expect_media_type(&self, allowed: &[&str]) -> Result<(), String> {
        let media_type = self.media_type();
        if allowed.contains(&media_type.as_str()) {
            Ok(())
        } else {
            Err(format!(
                "Content-Type was {media_type:?}, expected one of {allowed:?}"
            ))
        }
    }

    fn expect_body(&self, expected: &str) -> Result<(), String> {
        self.expect_status(StatusCode::OK)?;
        let body = self.body()?;
        if body == expected.as_bytes() {
            Ok(())
        } else {
            Err(format!(
                "body was {:?}, expected {expected:?}",
                String::from_utf8_lossy(body)
            ))
        }
    }
}

pub(crate) fn send(ctx: &Ctx<'_>, request: Request<Vec<u8>>) -> Result<Answer, String> {
    let description = format!("{} {}", request.method(), request.uri());
    let mut response = ctx
        .target
        .send(request)
        .map_err(|e| format!("{description} failed: {e:#}"))?;
    let mut body = Vec::new();
    let body = match response.body.read_to_end(&mut body) {
        Ok(_) => Ok(body),
        Err(e) => Err(e.to_string()),
    };
    Ok(Answer {
        status: response.status,
        headers: response.headers,
        body,
    })
}

pub(crate) fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// `path?k=v&…`, form-encoded.
pub(crate) fn params_uri(path: &str, params: &[(&str, &str)]) -> String {
    let mut uri = path.to_string();
    for (i, (key, value)) in params.iter().enumerate() {
        uri.push(if i == 0 { '?' } else { '&' });
        uri.push_str(key);
        uri.push('=');
        uri.push_str(&encode(value));
    }
    uri
}

/// `/query?query=…` followed by `params`.
pub(crate) fn query_uri(query: &str, params: &[(&str, &str)]) -> String {
    let mut all = vec![("query", query)];
    all.extend_from_slice(params);
    params_uri("/query", &all)
}

pub(crate) fn request(
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Request<Vec<u8>> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder.body(body).expect("the kit builds valid requests")
}

pub(crate) fn get(uri: &str, headers: &[(&str, &str)]) -> Request<Vec<u8>> {
    request(Method::GET, uri, headers, Vec::new())
}

pub(crate) fn post(uri: &str, content_type: Option<&str>, body: &str) -> Request<Vec<u8>> {
    let headers: Vec<(&str, &str)> = content_type
        .map(|t| (CONTENT_TYPE.as_str(), t))
        .into_iter()
        .collect();
    request(Method::POST, uri, &headers, body.as_bytes().to_vec())
}

fn results_format(answer: &Answer) -> Result<QueryResultsFormat, String> {
    let media_type = answer.media_type();
    QueryResultsFormat::from_media_type(&media_type)
        .ok_or_else(|| format!("Content-Type {media_type:?} isn't a SPARQL results format"))
}

pub(crate) fn boolean(answer: &Answer) -> Result<bool, String> {
    let parsed = QueryResultsParser::from_format(results_format(answer)?)
        .for_reader(answer.body()?)
        .map_err(|e| format!("unparsable results: {e}"))?;
    match parsed {
        ReaderQueryResultsParserOutput::Boolean(value) => Ok(value),
        ReaderQueryResultsParserOutput::Solutions(_) => {
            Err("got solutions, expected a boolean".to_string())
        }
    }
}

/// The variables, and the rows sorted, each term in N-Triples syntax.
pub(crate) fn solutions(answer: &Answer) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let parsed = QueryResultsParser::from_format(results_format(answer)?)
        .for_reader(answer.body()?)
        .map_err(|e| format!("unparsable results: {e}"))?;
    let ReaderQueryResultsParserOutput::Solutions(solutions) = parsed else {
        return Err("got a boolean, expected solutions".to_string());
    };
    let variables: Vec<String> = solutions
        .variables()
        .iter()
        .map(|v| v.as_str().to_string())
        .collect();
    let mut rows = Vec::new();
    for solution in solutions {
        let solution = solution.map_err(|e| format!("unparsable results: {e}"))?;
        rows.push(
            variables
                .iter()
                .map(|v| {
                    solution
                        .get(v.as_str())
                        .map_or_else(|| "UNBOUND".to_string(), ToString::to_string)
                })
                .collect(),
        );
    }
    rows.sort();
    Ok((variables, rows))
}

/// The triples of an RDF response, in N-Triples syntax.
///
/// Relative IRIs resolve against the endpoint, as they would for a client
/// that fetched the document from it: the service description names its
/// endpoint as `<>`.
pub(crate) fn triples(answer: &Answer) -> Result<BTreeSet<String>, String> {
    let media_type = answer.media_type();
    let format = RdfFormat::from_media_type(&media_type)
        .ok_or_else(|| format!("Content-Type {media_type:?} isn't an RDF format"))?;
    let parser = RdfParser::from_format(format)
        .with_base_iri("http://localhost/query")
        .map_err(|e| format!("{e}"))?;
    let mut triples = BTreeSet::new();
    for quad in parser.for_reader(answer.body()?) {
        let quad = quad.map_err(|e| format!("unparsable RDF: {e}"))?;
        triples.insert(format!(
            "{} {} {}",
            quad.subject, quad.predicate, quad.object
        ));
    }
    Ok(triples)
}
