//! Evaluating a query on a worker thread, and streaming its results as the body.

use super::{
    error::{HttpError, bad_request, internal_server_error},
    negotiate::{query_results_content_negotiation, rdf_content_negotiation},
    protocol::base_url,
};
use crate::sparql::AggregateHdt;
use http::{Request, Response, header::CONTENT_TYPE};
use log::debug;
use oxhttp::model::Body;
use oxrdfio::{RdfFormat, RdfSerializer};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::QueryResults;
use std::{
    io::{self, Read, Write},
    sync::mpsc,
};

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

#[allow(clippy::too_many_lines)]
pub(super) fn evaluate_sparql_query(
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
