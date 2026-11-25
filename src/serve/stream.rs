//! Evaluating a query on a worker thread, and streaming its results as the body.

use super::{
    body::{RequestBody, ResponseBody},
    dataset::{DatasetSelection, DatasetSource, EvalOptions},
    error::{HttpError, bad_request, internal_server_error},
    negotiate::{query_results_content_negotiation, rdf_content_negotiation},
    protocol::base_url,
};
use http::{Request, Response, StatusCode, header::CONTENT_TYPE};
use log::debug;
use oxrdfio::{RdfFormat, RdfSerializer};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{CancellationToken, QueryEvaluationError, QueryResults};
use std::{
    io::{self, Read, Write},
    sync::{
        Arc,
        mpsc::{self, RecvTimeoutError, TryRecvError},
    },
    time::{Duration, Instant},
};

const QUERY_STREAM_CHUNK_SIZE: usize = 32 * 1024;

enum QueryStreamItem {
    Data(Vec<u8>),
    Error(HttpError),
}

/// The messages from a query's worker thread, until the query's deadline.
/// Dropping it cancels the query, which then stops at its next step.
struct QueryStream {
    receiver: mpsc::Receiver<QueryStreamItem>,
    cancellation: CancellationToken,
    /// When the query must end by, and the timeout that set it.
    deadline: Option<(Instant, Duration)>,
}

/// Why a query's results stopped before their end.
enum Ended {
    Failed(HttpError),
    TimedOut(Duration),
}

impl QueryStream {
    /// The next chunk of results, `None` once there are no more, or why they
    /// stopped. Results that are ready go out even past the deadline: the
    /// query is cancelled only when it would have to be waited for, so its
    /// timeout limits how long it runs, not how long a client takes to read.
    fn next(&self) -> Result<Option<Vec<u8>>, Ended> {
        let item = match self.receiver.try_recv() {
            Ok(item) => Some(item),
            Err(TryRecvError::Disconnected) => None,
            Err(TryRecvError::Empty) => match self.deadline {
                None => self.receiver.recv().ok(),
                Some((deadline, timeout)) => {
                    match self
                        .receiver
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(item) => Some(item),
                        Err(RecvTimeoutError::Disconnected) => None,
                        Err(RecvTimeoutError::Timeout) => {
                            self.cancellation.cancel();
                            return Err(Ended::TimedOut(timeout));
                        }
                    }
                }
            },
        };
        match item {
            Some(QueryStreamItem::Data(data)) => Ok(Some(data)),
            Some(QueryStreamItem::Error(error)) => Err(Ended::Failed(error)),
            None => Ok(None),
        }
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn timeout_message(timeout: Duration) -> String {
    format!("the query ran past this server's {timeout:?} timeout")
}

impl From<Ended> for HttpError {
    fn from(ended: Ended) -> Self {
        match ended {
            Ended::Failed(error) => error,
            Ended::TimedOut(timeout) => {
                HttpError::new(StatusCode::SERVICE_UNAVAILABLE, timeout_message(timeout))
            }
        }
    }
}

impl From<Ended> for io::Error {
    fn from(ended: Ended) -> Self {
        match ended {
            Ended::Failed(error) => io::Error::new(io::ErrorKind::InvalidData, error.message()),
            Ended::TimedOut(timeout) => {
                io::Error::new(io::ErrorKind::TimedOut, timeout_message(timeout))
            }
        }
    }
}

/// A query's response body: its results, as the worker thread sends them.
struct QueryStreamReader {
    stream: QueryStream,
    buffer: Vec<u8>,
    position: usize,
}

impl QueryStreamReader {
    /// A reader that first returns `first`, which came from `stream`.
    fn new(stream: QueryStream, first: Vec<u8>) -> Self {
        Self {
            stream,
            buffer: first,
            position: 0,
        }
    }

    fn fill_buffer(&mut self) -> io::Result<bool> {
        while let Some(data) = self.stream.next()? {
            if !data.is_empty() {
                self.buffer = data;
                self.position = 0;
                return Ok(true);
            }
        }
        Ok(false)
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
) -> Result<(), HttpError> {
    let mut writer = QueryStreamChunkWriter::new(tx.clone());
    {
        let mut serializer = QueryResultsSerializer::from_format(format)
            .serialize_solutions_to_writer(&mut writer, variables)
            .map_err(|e| stream_error("serializing results", &e))?;

        for solution in solutions {
            let solution = solution.map_err(|e| evaluation_error(&e))?;
            serializer
                .serialize(&solution)
                .map_err(|e| stream_error("serializing query solution", &e))?;
        }

        serializer
            .finish()
            .map_err(|e| stream_error("finalizing results", &e))?;
    }
    writer
        .flush()
        .map_err(|e| stream_error("sending results", &e))
}

fn stream_query_graph_results(
    format: RdfFormat,
    tx: &mpsc::SyncSender<QueryStreamItem>,
    triples: spareval::QueryTripleIter,
) -> Result<(), HttpError> {
    let mut writer = QueryStreamChunkWriter::new(tx.clone());
    {
        let mut serializer = RdfSerializer::from_format(format).for_writer(&mut writer);
        for triple in triples {
            let triple = triple.map_err(|e| evaluation_error(&e))?;
            serializer
                .serialize_triple(&triple)
                .map_err(|e| stream_error("serializing RDF triple", &e))?;
        }
        serializer
            .finish()
            .map_err(|e| stream_error("finalizing RDF", &e))?;
    }
    writer.flush().map_err(|e| stream_error("sending RDF", &e))
}

#[allow(clippy::too_many_lines)]
pub(super) fn evaluate_sparql_query(
    source: &Arc<dyn DatasetSource>,
    query: &str,
    selection: DatasetSelection,
    request: &Request<RequestBody>,
    timeout: Option<Duration>,
) -> Result<Response<ResponseBody>, HttpError> {
    let started = Instant::now();
    debug!("query: {query}");
    let base = base_url(request);
    let mut parsed_query = crate::sparql::parse_query(query, &base)
        .map_err(|e| bad_request(format!("parse query: {e}")))?;
    if !selection.default_graph_uris.is_empty() || !selection.named_graph_uris.is_empty() {
        // The protocol's dataset replaces the query's FROM and FROM NAMED.
        match &mut parsed_query {
            spargebra::Query::Select { dataset, .. }
            | spargebra::Query::Construct { dataset, .. }
            | spargebra::Query::Describe { dataset, .. }
            | spargebra::Query::Ask { dataset, .. } => *dataset = None,
        }
    }
    let is_rdf_result = matches!(
        &parsed_query,
        spargebra::Query::Construct { .. } | spargebra::Query::Describe { .. }
    );

    let (tx, rx) = mpsc::sync_channel::<QueryStreamItem>(4);
    // The HTTP server's token, if it put one in the request, so that it can
    // cancel the query when the client goes away.
    let cancellation = request
        .extensions()
        .get::<CancellationToken>()
        .cloned()
        .unwrap_or_default();
    let eval_options = EvalOptions {
        cancellation_token: cancellation.clone(),
    };
    let source = Arc::clone(source);
    let output_media_type = if is_rdf_result {
        let graph_format = rdf_content_negotiation(request)?;
        let _ = std::thread::spawn(move || {
            let outcome = (|| -> Result<(), HttpError> {
                let dataset = source.open(&selection)?;
                let results = dataset
                    .evaluate(&parsed_query, &eval_options)
                    .map_err(|e| evaluation_error(&e))?;
                match results {
                    QueryResults::Graph(triples) => {
                        stream_query_graph_results(graph_format, &tx, triples)?;
                    }
                    QueryResults::Solutions(_) | QueryResults::Boolean(_) => {
                        return Err(HttpError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "construct/describe expected",
                        ));
                    }
                }
                Ok(())
            })();
            if let Err(error) = outcome {
                let _ = tx.send(QueryStreamItem::Error(error));
            }
        });
        graph_format.media_type()
    } else {
        let solutions_format = query_results_content_negotiation(request)?;
        let _ = std::thread::spawn(move || {
            let outcome = (|| -> Result<(), HttpError> {
                let dataset = source.open(&selection)?;
                let results = dataset
                    .evaluate(&parsed_query, &eval_options)
                    .map_err(|e| evaluation_error(&e))?;
                match results {
                    QueryResults::Solutions(solutions) => {
                        let variables = solutions.variables().to_vec();
                        stream_query_solution_results(solutions_format, &tx, solutions, variables)?;
                    }
                    QueryResults::Boolean(result) => {
                        let mut body = Vec::new();
                        QueryResultsSerializer::from_format(solutions_format)
                            .serialize_boolean_to_writer(&mut body, result)
                            .map_err(|e| stream_error("serializing boolean result", &e))?;
                        // A send fails only once the client has gone.
                        let _ = tx.send(QueryStreamItem::Data(body));
                    }
                    QueryResults::Graph(_) => {
                        return Err(HttpError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "select/ask expected",
                        ));
                    }
                }
                Ok(())
            })();
            if let Err(error) = outcome {
                let _ = tx.send(QueryStreamItem::Error(error));
            }
        });
        solutions_format.media_type()
    };

    // The status goes out with the headers, so the response waits for the
    // worker's first message: a query that fails before its first results
    // then gets an error status, instead of a 200 whose body is cut off.
    let stream = QueryStream {
        receiver: rx,
        cancellation,
        deadline: timeout.map(|timeout| (started + timeout, timeout)),
    };
    let first = match stream.next() {
        Ok(Some(first)) => first,
        // The worker finished without sending anything: no results.
        Ok(None) => Vec::new(),
        Err(ended) => return Err(ended.into()),
    };
    Response::builder()
        .header(CONTENT_TYPE, output_media_type)
        .body(ResponseBody::Stream(Box::new(QueryStreamReader::new(
            stream, first,
        ))))
        .map_err(internal_server_error)
}

/// The error for a query spareval couldn't evaluate, whether `evaluate`
/// returned it or the results did, as they do for a `SERVICE` call.
fn evaluation_error(error: &QueryEvaluationError) -> HttpError {
    let status = if matches!(error, QueryEvaluationError::UnsupportedService(_)) {
        StatusCode::NOT_IMPLEMENTED
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    HttpError::new(status, format!("query execution: {error}"))
}

/// An error while serializing results, or because the client went away.
fn stream_error(context: &str, error: &io::Error) -> HttpError {
    HttpError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("{context}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A query's stream, as the worker thread would feed it, with a deadline
    /// `timeout` from now.
    fn query_stream(
        timeout: Option<Duration>,
    ) -> (
        mpsc::SyncSender<QueryStreamItem>,
        QueryStream,
        CancellationToken,
    ) {
        let (tx, rx) = mpsc::sync_channel(4);
        let cancellation = CancellationToken::new();
        let stream = QueryStream {
            receiver: rx,
            cancellation: cancellation.clone(),
            deadline: timeout.map(|timeout| (Instant::now() + timeout, timeout)),
        };
        (tx, stream, cancellation)
    }

    #[test]
    fn waiting_past_the_deadline_cancels_the_query() {
        let (_tx, stream, cancellation) = query_stream(Some(Duration::from_millis(50)));
        let started = Instant::now();
        let Err(ended) = stream.next() else {
            panic!("results came from a worker that sent none");
        };
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(cancellation.is_cancelled());
        assert_eq!(
            HttpError::from(ended).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn a_timeout_after_the_first_results_cuts_the_body_off() {
        let (_tx, stream, cancellation) = query_stream(Some(Duration::from_millis(50)));
        let mut body = Vec::new();
        let error = QueryStreamReader::new(stream, b"first".to_vec())
            .read_to_end(&mut body)
            .unwrap_err();
        assert_eq!(body, b"first");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn results_that_are_ready_go_out_past_the_deadline() {
        let (tx, stream, _cancellation) = query_stream(Some(Duration::ZERO));
        tx.send(QueryStreamItem::Data(b" rest".to_vec())).unwrap();
        drop(tx);
        let mut body = Vec::new();
        QueryStreamReader::new(stream, b"first".to_vec())
            .read_to_end(&mut body)
            .unwrap();
        assert_eq!(body, b"first rest");
    }

    #[test]
    fn dropping_the_body_cancels_the_query() {
        let (_tx, stream, cancellation) = query_stream(None);
        let reader = QueryStreamReader::new(stream, b"first".to_vec());
        assert!(!cancellation.is_cancelled());
        drop(reader);
        assert!(cancellation.is_cancelled());
    }
}
