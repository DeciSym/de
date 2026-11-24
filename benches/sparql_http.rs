// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! The SPARQL endpoint, timed over HTTP: `de serve`'s request handling on a
//! loopback socket, driven by a client in the same process.
//!
//! The other targets time `de query` and the evaluator. This one adds what a
//! server does per request: reading and parsing the request, checking the
//! data directory for changes, opening the HDT files, starting the worker
//! thread, negotiating the result format and streaming the results. That is
//! the code a server built on `de` reuses, so a regression here is one that
//! server inherits.
//!
//! The workload is the BSBM explore use case (Berlin SPARQL Benchmark), the
//! usual benchmark for SPARQL endpoints: twelve query templates, five
//! instances each, with `OPTIONAL`, `UNION`, regex `FILTER`s, `ORDER BY`, and
//! `DESCRIBE` and `CONSTRUCT` results. Fixtures come from
//! `scripts/gen-bsbm-data.py` (`make bench-http` runs it); without them this
//! target prints how to get them and measures nothing. Product count is
//! `DE_BSBM_PRODUCT_COUNT`, `1000` (~372K triples) by default.
//!
//! Before timing anything, every query instance is answered over HTTP and in
//! process, and the two answers must match: a faster wrong answer fails here.
//!
//! Groups:
//!
//! * `sparql_http_overhead` — `ASK {}` three ways, to split the fixed cost of
//!   a request: over HTTP (`http_ask`); through the request handler without a
//!   socket (`handler_ask`: the data-directory check, the worker thread and
//!   the response stream); and evaluated directly (`inproc_ask`: open the
//!   HDT, evaluate, serialise).
//! * `sparql_http_bsbm` — one iteration sends a template's five instances.
//! * `sparql_http_format` — one 50,000-row `SELECT` in each result format.
//! * `sparql_http_concurrent` — four clients each send the whole explore mix
//!   at once. Contention makes it noisy, so the comparison reports it without
//!   failing on it (`--report-only` in `.github/workflows/bench.yml`).
//!
//! The server is `de::serve::spawn`, which `de serve` runs too. The target
//! needs the `server` feature (`required-features` in Cargo.toml).

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use de::serve::{HdtDirectory, RequestBody, ServerConfig, SparqlService};
use de::sparql::{AggregateHdt, parse_query, query_parsed_with_debug_plan};
use http::header::ACCEPT;
use http::{Method, Request, StatusCode};
use oxhttp::model::Body;
use oxrdfio::{RdfFormat, RdfParser};
use sparesults::{
    QueryResultsFormat, QueryResultsParser, QueryResultsSerializer, ReaderQueryResultsParserOutput,
};
use spareval::QueryResults;
use std::collections::BTreeMap;
use std::io::{Read, sink};
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PRODUCT_COUNT_VAR: &str = "DE_BSBM_PRODUCT_COUNT";
const DEFAULT_PRODUCT_COUNT: &str = "1000";
const CONCURRENT_CLIENTS: usize = 4;
const FORMAT_ROWS: usize = 50_000;
/// Base IRI for in-process parsing. BSBM's queries use absolute IRIs only.
const BASE_IRI: &str = "http://localhost/query";

#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};

/// One template's instances, and the result form they share.
struct Template {
    name: String,
    graph: bool,
    queries: Vec<String>,
}

/// Reads `queries/manifest.tsv` and groups its rows by template (`q01`…`q12`).
fn load_workload(queries_dir: &Path) -> Vec<Template> {
    let manifest_path = queries_dir.join("manifest.tsv");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));
    let mut grouped: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
    for line in manifest.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let [name, file, form] = fields[..] else {
            panic!("malformed row in {}: {line}", manifest_path.display());
        };
        let graph = match form {
            "select" => false,
            "graph" => true,
            other => panic!(
                "unknown result form {other:?} in {}",
                manifest_path.display()
            ),
        };
        let template = name.split_once('_').map_or(name, |(t, _)| t).to_string();
        let path = queries_dir.join(file);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        grouped
            .entry(template)
            .or_insert((graph, Vec::new()))
            .1
            .push(text);
    }
    grouped
        .into_iter()
        .map(|(name, (graph, queries))| Template {
            name,
            graph,
            queries,
        })
        .collect()
}

/// Builds `dataset.hdt` from the generated N-Triples the first time.
fn ensure_hdt(dir: &Path) -> String {
    let hdt = dir.join("dataset.hdt");
    let hdt_path = hdt.to_string_lossy().into_owned();
    if !hdt.exists() {
        eprintln!("building {} from dataset.nt (one-off)", hdt.display());
        let sources = vec![dir.join("dataset.nt").to_string_lossy().into_owned()];
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build a tokio runtime")
            .block_on(de::create::do_create(&hdt_path, &sources))
            .expect("failed to build the BSBM HDT package");
    }
    hdt_path
}

/// Starts `service` on a free loopback port, as `de serve` runs it, and
/// returns its base URL. The server runs until the process exits.
fn start_server(service: SparqlService) -> String {
    let port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .expect("failed to find a free port")
        .port();
    let server = de::serve::spawn(
        service,
        &format!("127.0.0.1:{port}"),
        &ServerConfig::default(),
    )
    .expect("failed to start the endpoint");
    // The listener threads stay up for the rest of the process.
    std::mem::forget(server);
    format!("http://127.0.0.1:{port}")
}

/// Sends a GET for `query`, checks the status and media type, and reads the
/// whole body. Returns the body when `keep` is set, otherwise its length.
fn get(client: &oxhttp::Client, base: &str, query: &str, accept: &str, keep: bool) -> Vec<u8> {
    let uri = format!(
        "{base}/query?query={}",
        url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let request = Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .header(ACCEPT, accept)
        .body(Body::empty())
        .expect("valid request");
    let response = client
        .request(request)
        .unwrap_or_else(|e| panic!("request failed: {e}\n{query}"));
    let status = response.status();
    let mut body = response.into_body();
    if status != StatusCode::OK {
        let mut message = String::new();
        let _ = body.read_to_string(&mut message);
        panic!("{status}: {message}\n{query}");
    }
    if keep {
        let mut bytes = Vec::new();
        body.read_to_end(&mut bytes)
            .unwrap_or_else(|e| panic!("body cut off: {e}\n{query}"));
        bytes
    } else {
        std::io::copy(&mut body, &mut sink())
            .unwrap_or_else(|e| panic!("body cut off: {e}\n{query}"));
        Vec::new()
    }
}

fn accept_for(template: &Template) -> &'static str {
    if template.graph {
        "text/turtle"
    } else {
        "application/sparql-results+json"
    }
}

/// An answer reduced to sorted lines, for comparing two ways of getting it.
fn canonical_solutions(results: QueryResults<'_>) -> Vec<String> {
    let mut lines = match results {
        QueryResults::Solutions(solutions) => {
            let variables = solutions.variables().to_vec();
            solutions
                .map(|solution| {
                    let solution = solution.expect("in-process evaluation failed");
                    variables
                        .iter()
                        .map(|v| {
                            solution
                                .get(v)
                                .map_or_else(String::new, ToString::to_string)
                        })
                        .collect::<Vec<_>>()
                        .join("\t")
                })
                .collect()
        }
        QueryResults::Graph(triples) => triples
            .map(|t| t.expect("in-process evaluation failed").to_string())
            .collect(),
        QueryResults::Boolean(value) => vec![value.to_string()],
    };
    lines.sort();
    lines
}

fn canonical_http_solutions(body: &[u8]) -> Vec<String> {
    let parsed = QueryResultsParser::from_format(QueryResultsFormat::Json)
        .for_reader(body)
        .expect("unparsable JSON results");
    let mut lines: Vec<String> = match parsed {
        ReaderQueryResultsParserOutput::Solutions(solutions) => {
            let variables = solutions.variables().to_vec();
            solutions
                .map(|solution| {
                    let solution = solution.expect("unparsable JSON results");
                    variables
                        .iter()
                        .map(|v| {
                            solution
                                .get(v)
                                .map_or_else(String::new, ToString::to_string)
                        })
                        .collect::<Vec<_>>()
                        .join("\t")
                })
                .collect()
        }
        ReaderQueryResultsParserOutput::Boolean(value) => vec![value.to_string()],
    };
    lines.sort();
    lines
}

fn canonical_http_triples(body: &[u8]) -> Vec<String> {
    let mut lines: Vec<String> = RdfParser::from_format(RdfFormat::NTriples)
        .for_reader(body)
        .map(|quad| {
            let quad = quad.expect("unparsable N-Triples");
            format!("{} {} {}", quad.subject, quad.predicate, quad.object)
        })
        .collect();
    lines.sort();
    lines
}

/// Answers every instance over HTTP and in process, and fails on any difference.
fn check_answers(client: &oxhttp::Client, base: &str, store: &AggregateHdt, workload: &[Template]) {
    let snapshot = store
        .get_snapshot(None)
        .expect("failed to open the BSBM HDT");
    let mut rows = 0;
    for template in workload {
        for (i, query) in template.queries.iter().enumerate() {
            let parsed = parse_query(query, BASE_IRI)
                .unwrap_or_else(|e| panic!("{} #{i} doesn't parse: {e}", template.name));
            let expected = canonical_solutions(
                query_parsed_with_debug_plan(&parsed, &snapshot, false)
                    .unwrap_or_else(|e| panic!("{} #{i} failed in process: {e}", template.name)),
            );
            let actual = if template.graph {
                canonical_http_triples(&get(client, base, query, "application/n-triples", true))
            } else {
                canonical_http_solutions(&get(
                    client,
                    base,
                    query,
                    "application/sparql-results+json",
                    true,
                ))
            };
            assert!(
                actual == expected,
                "{} #{i}: the HTTP answer ({} lines) differs from the in-process one ({} lines)\n{query}",
                template.name,
                actual.len(),
                expected.len()
            );
            rows += expected.len();
        }
    }
    eprintln!(
        "checked {} BSBM queries over HTTP against in-process answers ({rows} rows)",
        workload.iter().map(|t| t.queries.len()).sum::<usize>()
    );
}

fn bench_overhead(
    c: &mut Criterion,
    scale: &str,
    client: &oxhttp::Client,
    base: &str,
    store: &AggregateHdt,
    handler: &SparqlService,
) {
    let ask = parse_query("ASK {}", BASE_IRI).expect("ASK {} parses");
    let mut group = c.benchmark_group(format!("sparql_http_overhead/{scale}"));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("http_ask", |b| {
        b.iter(|| {
            get(
                client,
                base,
                "ASK {}",
                "application/sparql-results+json",
                false,
            )
        });
    });
    group.bench_function("handler_ask", |b| {
        b.iter(|| {
            let mut request = Request::builder()
                .method(Method::GET)
                .uri("http://localhost/query?query=ASK%20%7B%7D")
                .header(ACCEPT, "application/sparql-results+json")
                .body(RequestBody::empty())
                .expect("valid request");
            let response = handler
                .try_handle(&mut request)
                .unwrap_or_else(|error| panic!("{error}"));
            std::io::copy(&mut response.into_body().into_reader(), &mut sink())
                .expect("body cut off");
        });
    });
    group.bench_function("inproc_ask", |b| {
        b.iter(|| {
            let snapshot = store.get_snapshot(None).expect("failed to open the HDT");
            let QueryResults::Boolean(value) =
                query_parsed_with_debug_plan(&ask, &snapshot, false).expect("ASK {} evaluates")
            else {
                panic!("ASK gave something other than a boolean");
            };
            QueryResultsSerializer::from_format(QueryResultsFormat::Json)
                .serialize_boolean_to_writer(sink(), value)
                .expect("serialising a boolean");
        });
    });
    group.finish();
}

fn bench_templates(
    c: &mut Criterion,
    scale: &str,
    client: &oxhttp::Client,
    base: &str,
    workload: &[Template],
) {
    let mut group = c.benchmark_group(format!("sparql_http_bsbm/{scale}"));
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5));
    for template in workload {
        group.bench_function(&template.name, |b| {
            b.iter(|| {
                for query in &template.queries {
                    get(client, base, query, accept_for(template), false);
                }
            });
        });
    }
    group.finish();
}

fn bench_formats(c: &mut Criterion, scale: &str, client: &oxhttp::Client, base: &str) {
    let query = format!("SELECT ?s ?p ?o WHERE {{ ?s ?p ?o }} LIMIT {FORMAT_ROWS}");
    let mut group = c.benchmark_group(format!("sparql_http_format/{scale}"));
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(FORMAT_ROWS as u64));
    for (name, media_type) in [
        ("json", "application/sparql-results+json"),
        ("xml", "application/sparql-results+xml"),
        ("csv", "text/csv"),
        ("tsv", "text/tab-separated-values"),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            media_type,
            |b, accept| {
                b.iter(|| get(client, base, &query, accept, false));
            },
        );
    }
    group.finish();
}

fn bench_concurrent(c: &mut Criterion, scale: &str, base: &str, workload: &Arc<Vec<Template>>) {
    let mut group = c.benchmark_group(format!("sparql_http_concurrent/{scale}"));
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function(format!("{CONCURRENT_CLIENTS}_clients_mix"), |b| {
        b.iter_custom(|iterations| {
            let started = Instant::now();
            for _ in 0..iterations {
                let clients: Vec<_> = (0..CONCURRENT_CLIENTS)
                    .map(|_| {
                        let workload = Arc::clone(workload);
                        let base = base.to_string();
                        std::thread::spawn(move || {
                            let client = oxhttp::Client::new();
                            for template in workload.iter() {
                                for query in &template.queries {
                                    get(&client, &base, query, accept_for(template), false);
                                }
                            }
                        })
                    })
                    .collect();
                for client in clients {
                    client.join().expect("a client thread panicked");
                }
            }
            started.elapsed()
        });
    });
    group.finish();
}

fn fixture_dir(product_count: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("bsbm")
        .join(product_count)
}

fn sparql_http(c: &mut Criterion) {
    let scale =
        std::env::var(PRODUCT_COUNT_VAR).unwrap_or_else(|_| DEFAULT_PRODUCT_COUNT.to_string());
    let dir = fixture_dir(&scale);
    let queries_dir = dir.join("queries");
    if let Some(missing) = [dir.join("dataset.nt"), queries_dir.join("manifest.tsv")]
        .into_iter()
        .find(|path| !path.exists())
    {
        eprintln!(
            "skipping SPARQL HTTP benchmarks: {} is missing.\n\
             run `make bench-http` (or `python3 scripts/gen-bsbm-data.py {scale}`) to generate \
             the BSBM fixtures; the generator needs Java.",
            missing.display(),
        );
        return;
    }
    let hdt = ensure_hdt(&dir);
    let store = AggregateHdt::new(std::slice::from_ref(&hdt)).expect("failed to open the BSBM HDT");
    // Two services over one store: one behind the HTTP server, one called
    // directly for `handler_ask`.
    let base = start_server(SparqlService::new(HdtDirectory::with_store(
        store.clone(),
        &dir,
    )));
    let handler = SparqlService::new(HdtDirectory::with_store(store.clone(), &dir));
    let client = oxhttp::Client::new();
    let workload = Arc::new(load_workload(&queries_dir));

    // Untimed: builds the HDT's index sidecar on first use, and checks answers.
    check_answers(&client, &base, &store, &workload);

    bench_overhead(c, &scale, &client, &base, &store, &handler);
    bench_templates(c, &scale, &client, &base, &workload);
    bench_formats(c, &scale, &client, &base);
    bench_concurrent(c, &scale, &base, &workload);
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(500));
    targets = sparql_http
}

#[cfg(not(target_os = "linux"))]
criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_millis(500));
    targets = sparql_http
}

criterion_main!(benches);
