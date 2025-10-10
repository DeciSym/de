use std::{
    cell::RefCell,
    cmp::min,
    fmt,
    fs::File,
    io::{self, BufWriter, Read, Write},
    net::ToSocketAddrs,
    path::Path,
    rc::Rc,
    thread::available_parallelism,
    time::Duration,
};

use http::{
    header::{ACCEPT, CONTENT_TYPE},
    Request, Response, StatusCode,
};
use log::debug;
use oxhttp::{model::Body, Server};
use oxrdfio::{RdfFormat, RdfSerializer};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{QueryEvaluator, QueryResults};
use std::str::FromStr;
use url::form_urlencoded;

use crate::sparql;

type HttpError = (StatusCode, String);

const MAX_SPARQL_BODY_SIZE: u64 = 1024 * 1024 * 128; // 128MB
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const HTML_ROOT_PAGE: &str = include_str!("../templates/query.html");
#[expect(clippy::large_include_file)]
const YASGUI_JS: &str = include_str!("../templates/yasgui/yasgui.min.js");
const YASGUI_CSS: &str = include_str!("../templates/yasgui/yasgui.min.css");
const LOGO: &str = include_str!("../templates/logo.svg");

pub fn serve(data: &str, bind: &str) -> anyhow::Result<()> {
    let p = Path::new(data);
    assert!(p.exists());
    let mut hdts = vec![];
    if p.is_file() {
        hdts.push(data.to_string());
    } else if p.is_dir() {
        for f in p.read_dir().expect("") {
            if let Ok(entry) = f {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "hdt" {
                        hdts.push(path.to_str().unwrap().to_string())
                    }
                }
            }
        }
    }
    debug!("service files {hdts:?}");
    let store = sparql::AggregateHDT::new(&hdts)?;

    // let timeout = timeout_s.map(Duration::from_secs);
    let mut server = Server::new(move |request| {
        handle_request(
            request,
            &store, // read_only,
                   // union_default_graph,
                   // timeout,
        )
        .unwrap_or_else(|(status, message)| error(status, message))
    })
    .with_global_timeout(HTTP_TIMEOUT)
    .with_server_name(concat!("DeciSym Engine / ", env!("CARGO_PKG_VERSION")))?
    .with_max_concurrent_connections(available_parallelism()?.get() * 128);
    for socket in bind.to_socket_addrs()? {
        server = server.bind(socket);
    }
    let server = server.spawn()?;
    // #[cfg(target_os = "linux")]
    // systemd_notify_ready()?;
    eprintln!("Listening for requests at http://{bind}");
    server.join()?;
    Ok(())
}

fn error(status: StatusCode, message: impl fmt::Display) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(message.to_string().into())
        .unwrap()
}

fn url_query(request: &Request<Body>) -> &[u8] {
    request.uri().query().unwrap_or_default().as_bytes()
}

fn bad_request(message: impl fmt::Display) -> HttpError {
    (StatusCode::BAD_REQUEST, message.to_string())
}

fn internal_server_error(message: impl fmt::Display) -> HttpError {
    eprintln!("Internal server error: {message}");
    (StatusCode::INTERNAL_SERVER_ERROR, message.to_string())
}

fn unsupported_media_type(content_type: &str) -> HttpError {
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!("No supported content Content-Type given: {content_type}"),
    )
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
        return Ok(default);
    }
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
        (
            StatusCode::NOT_ACCEPTABLE,
            format!("The accept header does not provide any accepted format like {example}"),
        )
    })
}

fn content_type(request: &Request<Body>) -> Option<String> {
    let value = request.headers().get(CONTENT_TYPE)?.to_str().ok()?;
    Some(
        value
            .split_once(';')
            .map_or(value, |(b, _)| b)
            .trim()
            .to_ascii_lowercase(),
    )
}

fn configure_and_evaluate_sparql_query(
    store: &sparql::AggregateHDT,
    encoded: &[&[u8]],
    mut query: Option<String>,
    request: &Request<Body>,
    // default_use_default_graph_as_union: bool,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    // let mut default_graph_uris = Vec::new();
    // let mut named_graph_uris = Vec::new();
    // let mut use_default_graph_as_union = false;
    debug!("eval query");
    for encoded in encoded {
        for (k, v) in form_urlencoded::parse(encoded) {
            match k.as_ref() {
                "query" => {
                    if query.is_some() {
                        return Err(bad_request("Multiple query parameters provided"));
                    }
                    query = Some(v.into_owned())
                }
                // "default-graph-uri" => default_graph_uris.push(v.into_owned()),
                // "union-default-graph" => use_default_graph_as_union = true,
                // "named-graph-uri" => named_graph_uris.push(v.into_owned()),
                _ => (),
            }
        }
    }
    // if default_graph_uris.is_empty() && named_graph_uris.is_empty() {
    //     use_default_graph_as_union |= default_use_default_graph_as_union;
    // }
    let query = query.ok_or_else(|| bad_request("You should set the 'query' parameter"))?;
    evaluate_sparql_query(
        store, &query,
        // use_default_graph_as_union,
        // default_graph_uris,
        // named_graph_uris,
        request,
        // timeout,
    )
}

fn evaluate_sparql_query(
    store: &sparql::AggregateHDT,
    query: &str,
    // use_default_graph_as_union: bool,
    // default_graph_uris: Vec<String>,
    // named_graph_uris: Vec<String>,
    request: &Request<Body>,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    let evaluator = QueryEvaluator::new();
    let query = spargebra::SparqlParser::new()
        .parse_query(query)
        .map_err(|_| bad_request("incorrect query format"))?;
    let results = evaluator
        .execute(store, &query)
        .map_err(internal_server_error)?;

    match results {
        QueryResults::Solutions(solutions) => {
            let format = query_results_content_negotiation(request)?;
            // Collect solutions to avoid lifetime issues
            let variables = solutions.variables().to_vec();
            let collected_solutions: Result<Vec<_>, _> = solutions.collect();
            let collected_solutions = collected_solutions.map_err(internal_server_error)?;
            let solution_iter = collected_solutions.into_iter();

            ReadForWrite::build_response(
                move |w| {
                    Ok((
                        QueryResultsSerializer::from_format(format)
                            .serialize_solutions_to_writer(w, variables)?,
                        solution_iter,
                    ))
                },
                |(mut serializer, mut solutions)| {
                    Ok(if let Some(solution) = solutions.next() {
                        serializer.serialize(&solution)?;
                        Some((serializer, solutions))
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
            let collected_triples: Result<Vec<_>, _> = triples.collect();
            let collected_triples = collected_triples.map_err(internal_server_error)?;
            let triple_iter = collected_triples.into_iter();

            ReadForWrite::build_response(
                move |w| {
                    Ok((
                        RdfSerializer::from_format(format).for_writer(w),
                        triple_iter,
                    ))
                },
                |(mut serializer, mut triples)| {
                    Ok(if let Some(t) = triples.next() {
                        serializer.serialize_triple(&t)?;
                        Some((serializer, triples))
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

fn handle_request(
    request: &mut Request<Body>,
    store: &sparql::AggregateHDT,
    // read_only: bool,
    // union_default_graph: bool,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
    debug!(
        "handling {} {}",
        request.uri().path(),
        request.method().as_ref()
    );
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
                Err(bad_request("missing 'query' parameter"))
            } else {
                configure_and_evaluate_sparql_query(
                    &store,
                    &[url_query(request)],
                    None,
                    request,
                    // union_default_graph,
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
                    &store,
                    &[url_query(request)],
                    Some(query),
                    request,
                    // union_default_graph,
                    // timeout,
                )
            } else if content_type == "application/x-www-form-urlencoded" {
                let buffer = limited_body(request)?;
                configure_and_evaluate_sparql_query(
                    &store,
                    &[url_query(request), &buffer],
                    None,
                    request,
                    // union_default_graph,
                    // timeout,
                )
            } else {
                Err(unsupported_media_type(&content_type))
            }
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

fn close_file_writer(writer: BufWriter<File>) -> io::Result<()> {
    let mut file = writer
        .into_inner()
        .map_err(io::IntoInnerError::into_error)?;
    file.flush()?;
    file.sync_all()
}
