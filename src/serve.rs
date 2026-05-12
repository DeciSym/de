use http::{
    HeaderValue, Method, Request, Response, StatusCode,
    header::{
        ACCEPT, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
        CONTENT_TYPE, ORIGIN,
    },
    uri::PathAndQuery,
};
use log::{debug, warn};
use oxhttp::{Server, model::Body};
use oxrdfio::{RdfFormat, RdfSerializer};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::QueryResults;
use std::{
    collections::HashMap,
    fmt,
    io::{self, Read, Write},
    net::ToSocketAddrs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    thread::available_parallelism,
    time::Duration,
    time::SystemTime,
};
use std::{str::FromStr, sync::mpsc};
use url::form_urlencoded;

use crate::{
    service_description::{EndpointKind, generate_service_description},
    sparql::AggregateHdt,
};

type HttpError = (StatusCode, String);

const MAX_SPARQL_BODY_SIZE: u64 = 1024 * 1024 * 128; // 128MB
const HTTP_TIMEOUT: Duration = Duration::from_mins(1);
const HTML_ROOT_PAGE: &str = include_str!("../templates/query.html");
#[expect(clippy::large_include_file)]
const YASGUI_JS: &str = include_str!("../templates/yasgui/yasgui.min.js");
const YASGUI_CSS: &str = include_str!("../templates/yasgui/yasgui.min.css");
const LOGO: &str = include_str!("../templates/logo.svg");

#[derive(Clone, Eq, PartialEq)]
struct HdtFileFingerprint {
    path: PathBuf,
    modified: SystemTime,
    len: u64,
}

#[derive(Clone, Eq, PartialEq)]
struct StoreFingerprint {
    files: Vec<HdtFileFingerprint>,
}

static STORE_SYNC_CACHE: OnceLock<Mutex<HashMap<PathBuf, StoreFingerprint>>> = OnceLock::new();

fn collect_hdt_paths(location: &Path) -> anyhow::Result<Vec<String>> {
    let mut hdt_paths: Vec<String> = std::fs::read_dir(location)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if !entry.file_type().ok()?.is_file() {
                return None;
            }
            let path = entry.path();
            let is_hdt = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("hdt"));
            if is_hdt {
                Some(path.to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect();
    hdt_paths.sort_unstable();
    hdt_paths.dedup();
    Ok(hdt_paths)
}

fn compute_store_fingerprint(location: &Path) -> Result<StoreFingerprint, HttpError> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(location)
        .map_err(|e| internal_server_error(format!("error reading data location: {e}")))?
    {
        let entry = entry.map_err(|e| {
            internal_server_error(format!("error while iterating data location: {e}"))
        })?;
        let path = entry.path();
        if !path
            .extension()
            .is_some_and(|extension| extension.to_string_lossy().eq_ignore_ascii_case("hdt"))
        {
            continue;
        }
        let metadata = entry.metadata().map_err(|e| {
            internal_server_error(format!(
                "error reading HDT metadata for {}: {e}",
                path.display()
            ))
        })?;
        let modified = metadata.modified().map_err(|e| {
            internal_server_error(format!(
                "error reading HDT mtime for {}: {e}",
                path.display()
            ))
        })?;
        files.push(HdtFileFingerprint {
            path,
            modified,
            len: metadata.len(),
        });
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(StoreFingerprint { files })
}

fn maybe_sync_store(store: &AggregateHdt, location: &Path) -> Result<(), HttpError> {
    let canonical_location = location
        .canonicalize()
        .map_err(|e| internal_server_error(format!("error resolving data location: {e}")))?;
    let fingerprint = compute_store_fingerprint(&canonical_location)?;

    let cache = STORE_SYNC_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = cache
            .lock()
            .map_err(|_| internal_server_error("sync cache lock poisoned"))?;
        if guard
            .get(&canonical_location)
            .is_some_and(|cached| *cached == fingerprint)
        {
            return Ok(());
        }
    }

    store
        .sync(&canonical_location)
        .map_err(|e| internal_server_error(format!("error loading data files: {e}")))?;

    let mut guard = cache
        .lock()
        .map_err(|_| internal_server_error("sync cache lock poisoned"))?;
    guard.insert(canonical_location, fingerprint);
    Ok(())
}

pub fn serve(
    locations: String,
    bind: &str,
    // read_only: bool,
    // cors: bool,
    // union_default_graph: bool,
    // timeout_s: Option<u64>,
) -> anyhow::Result<()> {
    let union_default_graph = true;
    let cors = false;

    let hdt_paths = collect_hdt_paths(Path::new(&locations))?;

    eprintln!("Found {} HDT files in {}", hdt_paths.len(), locations);
    for path in &hdt_paths {
        eprintln!("  - {path}");
    }

    // Create the AggregateHdt store from the found HDT files
    let store = if hdt_paths.is_empty() {
        warn!("Warning: No HDT files found in the specified locations: {locations}");
        AggregateHdt::empty()
    } else {
        AggregateHdt::new(&hdt_paths)?
    };

    // let timeout = timeout_s.map(Duration::from_secs);
    let mut server = if cors {
        Server::new(cors_middleware(move |request| {
            handle_request(request, &store, union_default_graph, &locations)
                .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            handle_request(request, &store, union_default_graph, &locations)
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
            response.body(Body::empty()).unwrap_or_else(|e| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to build CORS preflight response: {e}"),
                )
            })
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

fn base_url(request: &Request<Body>) -> String {
    let uri = request.uri();
    if uri.query().is_some() {
        // We remove the query
        let mut parts = uri.clone().into_parts();
        if let Some(path_and_query) = &mut parts.path_and_query
            && path_and_query.query().is_some()
        {
            if let Ok(path_only) = PathAndQuery::try_from(path_and_query.path()) {
                *path_and_query = path_only;
            } else {
                return uri.path().to_string();
            }
        }
        http::Uri::from_parts(parts)
            .map_or_else(|_| uri.path().to_string(), |built| built.to_string())
    } else {
        uri.to_string()
    }
}

fn url_query(request: &Request<Body>) -> &[u8] {
    request.uri().query().unwrap_or_default().as_bytes()
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
        let max_len =
            usize::try_from(MAX_SPARQL_BODY_SIZE).map_err(|_| bad_request("Huge body size"))?;
        if payload.len() > max_len {
            return Err(bad_request(format!(
                "SPARQL body payloads are limited to {MAX_SPARQL_BODY_SIZE} bytes"
            )));
        }
        Ok(payload)
    }
}

const QUERY_STREAM_CHUNK_SIZE: usize = 32 * 1024;

enum QueryStreamItem {
    Data(Vec<u8>),
    Error(String),
}

struct QueryStreamReader {
    receiver: mpsc::Receiver<QueryStreamItem>,
    buffer: Vec<u8>,
    position: usize,
}

impl QueryStreamReader {
    fn new(receiver: mpsc::Receiver<QueryStreamItem>) -> Self {
        Self {
            receiver,
            buffer: Vec::new(),
            position: 0,
        }
    }

    fn fill_buffer(&mut self) -> io::Result<bool> {
        loop {
            match self.receiver.recv() {
                Ok(QueryStreamItem::Data(data)) => {
                    if data.is_empty() {
                        continue;
                    }
                    self.buffer = data;
                    self.position = 0;
                    return Ok(true);
                }
                Ok(QueryStreamItem::Error(err)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, err));
                }
                Err(_) => return Ok(false),
            }
        }
    }
}

impl Read for QueryStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.buffer.len() && !self.fill_buffer()? {
            return Ok(0);
        }
        let available = std::cmp::min(buf.len(), self.buffer.len() - self.position);
        buf[..available].copy_from_slice(&self.buffer[self.position..self.position + available]);
        self.position += available;
        if self.position >= self.buffer.len() {
            self.buffer.clear();
            self.position = 0;
        }
        Ok(available)
    }
}

fn send_query_stream_buffer(
    tx: &mpsc::SyncSender<QueryStreamItem>,
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    tx.send(QueryStreamItem::Data(std::mem::take(buffer)))
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "client disconnected"))?;
    Ok(())
}

struct QueryStreamChunkWriter {
    tx: mpsc::SyncSender<QueryStreamItem>,
    buffer: Vec<u8>,
}

impl QueryStreamChunkWriter {
    fn new(tx: mpsc::SyncSender<QueryStreamItem>) -> Self {
        Self {
            tx,
            buffer: Vec::new(),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            send_query_stream_buffer(&self.tx, &mut self.buffer)?;
        }
        Ok(())
    }
}

impl Write for QueryStreamChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= QUERY_STREAM_CHUNK_SIZE {
            self.flush()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        QueryStreamChunkWriter::flush(self)
    }
}

fn stream_query_solution_results(
    format: QueryResultsFormat,
    tx: &mpsc::SyncSender<QueryStreamItem>,
    solutions: spareval::QuerySolutionIter,
    variables: Vec<oxrdf::Variable>,
) -> io::Result<()> {
    let mut writer = QueryStreamChunkWriter::new(tx.clone());
    {
        let mut serializer = QueryResultsSerializer::from_format(format)
            .serialize_solutions_to_writer(&mut writer, variables)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        for solution in solutions {
            let solution =
                solution.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            serializer.serialize(&solution).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("serializing query solution: {e}"),
                )
            })?;
        }

        serializer.finish().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("finalizing results: {e}"),
            )
        })?;
    }
    writer.flush()
}

fn stream_query_graph_results(
    format: RdfFormat,
    tx: &mpsc::SyncSender<QueryStreamItem>,
    triples: spareval::QueryTripleIter,
) -> io::Result<()> {
    let mut writer = QueryStreamChunkWriter::new(tx.clone());
    {
        let mut serializer = RdfSerializer::from_format(format).for_writer(&mut writer);
        for triple in triples {
            let triple =
                triple.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            serializer.serialize_triple(&triple).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("serializing RDF triple: {e}"),
                )
            })?;
        }
        serializer.finish().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("finalizing RDF: {e}"))
        })?;
    }
    writer.flush()
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
                    query = Some(v.into_owned());
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
        &default_graph_uris,
        &named_graph_uris,
        request,
        // timeout,
    )
}

#[allow(clippy::too_many_lines)]
fn evaluate_sparql_query(
    store: &AggregateHdt,
    query: &str,
    use_default_graph_as_union: bool,
    default_graph_uris: &[String],
    named_graph_uris: &[String],
    request: &Request<Body>,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    debug!("query: {query}");
    let base = base_url(request);
    let parsed_query = crate::sparql::parse_query(query, &base)
        .map_err(|e| bad_request(format!("parse query: {e}")))?;
    let is_rdf_result = matches!(
        &parsed_query,
        spargebra::Query::Construct { .. } | spargebra::Query::Describe { .. }
    );

    // Get snapshot with optional graph filtering
    // Optimization: Filter graphs BEFORE loading into memory by passing an explicit list.
    // This significantly reduces memory usage and load time when only a subset of graphs are
    // needed for the query.
    let graph_filter = {
        let mut selected_graphs: Vec<String> = if use_default_graph_as_union {
            let mut selected = default_graph_uris.to_vec();
            selected.extend_from_slice(named_graph_uris);
            selected
        } else if default_graph_uris.is_empty() {
            named_graph_uris.to_vec()
        } else {
            default_graph_uris.to_vec()
        };

        selected_graphs.sort_unstable();
        selected_graphs.dedup();

        if selected_graphs.is_empty() {
            None
        } else {
            Some(selected_graphs)
        }
    };
    if let Some(graph_filter_ref) = graph_filter.as_ref() {
        debug!("using graph filter for query: {graph_filter_ref:?}");
    } else {
        debug!("using default graph filter for query: all graphs");
    }

    let (tx, rx) = mpsc::sync_channel::<QueryStreamItem>(4);
    let store = store.clone();
    let output_media_type = if is_rdf_result {
        let graph_format = rdf_content_negotiation(request)?;
        let _ = std::thread::spawn(move || {
            let tx_result = (|| -> io::Result<()> {
                let snapshot = store.get_snapshot(graph_filter).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "data temporarily unavailable")
                })?;
                let results =
                    crate::sparql::query_parsed_with_debug_plan(&parsed_query, &snapshot, false)
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("query execution: {e}"),
                            )
                        })?;
                match results {
                    QueryResults::Graph(triples) => {
                        stream_query_graph_results(graph_format, &tx, triples)?;
                    }
                    QueryResults::Solutions(_) | QueryResults::Boolean(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "construct/describe expected",
                    ))?,
                }
                Ok(())
            })();
            if let Err(err) = tx_result {
                let _ = tx.send(QueryStreamItem::Error(err.to_string()));
            }
        });
        graph_format.media_type()
    } else {
        let solutions_format = query_results_content_negotiation(request)?;
        let _ = std::thread::spawn(move || {
            let tx_result = (|| -> io::Result<()> {
                let snapshot = store.get_snapshot(graph_filter).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "data temporarily unavailable")
                })?;
                let results =
                    crate::sparql::query_parsed_with_debug_plan(&parsed_query, &snapshot, false)
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("query execution: {e}"),
                            )
                        })?;
                match results {
                    QueryResults::Solutions(solutions) => {
                        let variables = solutions.variables().to_vec();
                        stream_query_solution_results(solutions_format, &tx, solutions, variables)?;
                    }
                    QueryResults::Boolean(result) => {
                        let mut body = Vec::new();
                        QueryResultsSerializer::from_format(solutions_format)
                            .serialize_boolean_to_writer(&mut body, result)
                            .map_err(|e| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("serializing boolean result: {e}"),
                                )
                            })?;
                        tx.send(QueryStreamItem::Data(body)).map_err(|_| {
                            io::Error::new(io::ErrorKind::BrokenPipe, "client disconnected")
                        })?;
                    }
                    QueryResults::Graph(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "select/ask expected",
                    ))?,
                }
                Ok(())
            })();
            if let Err(err) = tx_result {
                let _ = tx.send(QueryStreamItem::Error(err.to_string()));
            }
        });
        solutions_format.media_type()
    };

    Response::builder()
        .header(CONTENT_TYPE, output_media_type)
        .body(Body::from_read(QueryStreamReader::new(rx)))
        .map_err(internal_server_error)
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
        debug!("accept header missing, using default content type");
        return Ok(default);
    }
    debug!("{ACCEPT}: {header}");
    let mut result = None;
    let mut result_score = 0_f32;
    for mut possible in header.split(',') {
        let mut score = 1.;
        if let Some((possible_type, last_parameter)) = possible.rsplit_once(';')
            && let Some((name, value)) = last_parameter.split_once('=')
            && name.trim().eq_ignore_ascii_case("q")
        {
            score = f32::from_str(value.trim())
                .map_err(|_| bad_request(format!("Invalid Accept media type score: {value}")))?;
            possible = possible_type;
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
    debug!("request content_type: {value}");
    Some(
        value
            .split_once(';')
            .map_or(value, |(b, _)| b)
            .trim()
            .to_ascii_lowercase(),
    )
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
    let mut response = Response::new(message.to_string().into());
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn bad_request(message: impl fmt::Display) -> HttpError {
    eprintln!("BAD REQUEST: {message}");
    (StatusCode::BAD_REQUEST, message.to_string())
}

fn not_implemented(message: impl fmt::Display) -> HttpError {
    eprintln!("NOT IMPLEMENTED: {message}");
    (StatusCode::NOT_IMPLEMENTED, message.to_string())
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

#[cfg(target_os = "linux")]
fn systemd_notify_ready() -> io::Result<()> {
    use std::env;

    if let Some(path) = env::var_os("NOTIFY_SOCKET") {
        use std::os::unix::net::UnixDatagram;

        UnixDatagram::unbound()?.send_to(b"READY=1", path)?;
    }
    Ok(())
}
