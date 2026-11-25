//! Running a [`SparqlService`] on axum and hyper: [`router`] to mount it in a
//! server's own axum app, and [`spawn`] to run it on a server of its own.
//!
//! The service is synchronous. It reads its request body as a reader, and it
//! may block while it refreshes its data or waits for a query's first
//! results. So each request runs on tokio's blocking threads, and a streamed
//! response is read there and handed to hyper a chunk at a time.

use super::{
    body::{RequestBody, ResponseBody},
    error::{HttpError, bad_request},
    server::{ServerConfig, hdt_service, max_connections},
    service::{ServiceOptions, SparqlService},
};
use ::axum::{
    Router, body::Body, extract::Request as AxumRequest, middleware,
    response::Response as AxumResponse,
};
use futures_util::StreamExt as _;
use http::{
    HeaderValue, Request, Response, StatusCode, Uri,
    header::{HOST, SERVER},
    request::Parts,
    uri::{Authority, PathAndQuery, Scheme},
};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use log::warn;
use spareval::CancellationToken;
use std::{
    future::Future,
    io::{self, Cursor, IoSlice, Read},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    thread,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::{Semaphore, mpsc, oneshot, watch},
    time::Sleep,
};

/// How many chunks of a streamed body wait between the thread that reads it
/// and hyper.
const BODY_CHANNEL_CHUNKS: usize = 4;
/// The most read from a streamed body at a time.
const BODY_CHUNK_BYTES: usize = 32 * 1024;

/// An axum router that answers every request with `service`.
///
/// The service does its own routing, so mount the router where it sees every
/// path, such as with `Router::fallback_service`. Each request's URI is made
/// absolute from its Host header, as the service expects, and a request
/// without one gets a 400. When the client goes away before the answer or
/// while it streams, the query is cancelled.
pub fn router(service: SparqlService) -> Router {
    let service = Arc::new(service);
    Router::new().fallback(move |request: AxumRequest| handle(Arc::clone(&service), request))
}

/// Answers one request with `service`.
async fn handle(service: Arc<SparqlService>, request: AxumRequest) -> AxumResponse {
    let (mut parts, body) = request.into_parts();
    if let Err(error) = make_absolute(&mut parts) {
        return into_axum(error.into_response(), CancelOnDrop::default());
    }
    let body = match read_body(body, service.options().max_body_bytes).await {
        Ok(body) => body,
        Err(error) => return into_axum(error.into_response(), CancelOnDrop::default()),
    };
    // The query's token. It's cancelled when this future is dropped, as hyper
    // drops it when the client goes away before the answer, or, for an answer
    // that streams, when hyper drops the response body.
    let cancel = CancelOnDrop::default();
    parts.extensions.insert(cancel.0.clone());
    let answer = tokio::task::spawn_blocking(move || {
        let mut request = Request::from_parts(parts, RequestBody::from_reader(Cursor::new(body)));
        service.handle(&mut request)
    })
    .await;
    match answer {
        Ok(response) => into_axum(response, cancel),
        Err(error) => into_axum(
            HttpError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("the request's thread failed: {error}"),
            )
            .into_response(),
            cancel,
        ),
    }
}

/// Makes the request's URI absolute, with `http` and its Host header, as the
/// service expects of its requests.
fn make_absolute(parts: &mut Parts) -> Result<(), HttpError> {
    if parts.uri.scheme().is_some() {
        return Ok(());
    }
    let host = parts
        .headers
        .get(HOST)
        .ok_or_else(|| bad_request("No Host header in the request"))?;
    let authority = Authority::try_from(host.as_bytes())
        .map_err(|e| bad_request(format!("Invalid Host header: {e}")))?;
    let mut uri = std::mem::take(&mut parts.uri).into_parts();
    uri.scheme = Some(Scheme::HTTP);
    uri.authority = Some(authority);
    if uri.path_and_query.is_none() {
        uri.path_and_query = Some(PathAndQuery::from_static("/"));
    }
    parts.uri =
        Uri::from_parts(uri).map_err(|e| bad_request(format!("Invalid request URI: {e}")))?;
    Ok(())
}

/// Reads a request body, stopping once it's past `max_bytes`: the service
/// then rejects it, as it does any body over its limit.
async fn read_body(body: Body, max_bytes: u64) -> Result<Vec<u8>, HttpError> {
    let limit = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    let mut stream = body.into_data_stream();
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| bad_request(format!("Reading the request body: {e}")))?;
        buffer.extend_from_slice(&chunk);
        if buffer.len() > limit {
            break;
        }
    }
    Ok(buffer)
}

/// Converts the service's response. A streamed body holds `cancel`, which
/// cancels the query once hyper drops the body.
fn into_axum(response: Response<ResponseBody>, cancel: CancelOnDrop) -> AxumResponse {
    let (parts, body) = response.into_parts();
    let body = match body {
        ResponseBody::Empty => Body::empty(),
        ResponseBody::Bytes(bytes) => Body::from(bytes),
        ResponseBody::Stream(reader) => stream_body(reader, cancel),
    };
    AxumResponse::from_parts(parts, body)
}

/// A body that a blocking thread fills from `reader`, a chunk at a time.
fn stream_body(mut reader: Box<dyn Read + Send>, cancel: CancelOnDrop) -> Body {
    let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>(BODY_CHANNEL_CHUNKS);
    tokio::task::spawn_blocking(move || {
        loop {
            let mut chunk = vec![0; BODY_CHUNK_BYTES];
            let item = match reader.read(&mut chunk) {
                Ok(0) => return,
                Ok(read) => {
                    chunk.truncate(read);
                    Ok(chunk)
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => Err(error),
            };
            let failed = item.is_err();
            // A send fails once hyper has dropped the body.
            if tx.blocking_send(item).is_err() || failed {
                return;
            }
        }
    });
    Body::from_stream(futures_util::stream::unfold(
        (rx, cancel),
        |(mut rx, cancel)| async move { rx.recv().await.map(|item| (item, (rx, cancel))) },
    ))
}

/// A query's cancellation token, cancelled when this is dropped.
#[derive(Default)]
struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A [`SparqlService`] served on hyper by [`spawn`], on a runtime of its own.
/// Dropping it shuts the server down, as [`shutdown`](Self::shutdown) does.
pub struct AxumServer {
    addresses: Vec<SocketAddr>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl AxumServer {
    /// The addresses the server listens on, with the port the system chose
    /// where the bind address asked for port 0.
    #[must_use]
    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.addresses
    }

    /// Stops accepting connections, lets open ones finish their responses
    /// for up to the server's timeout, and returns once the server stops.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for AxumServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// How [`spawn`]'s server runs its connections.
#[derive(Clone, Copy)]
struct Limits {
    max_connections: usize,
    /// How long a client may take to send a request's headers or to take the
    /// next part of a response, and how long open connections get to finish
    /// when the server shuts down.
    timeout: Duration,
}

/// Serves `service` over HTTP/1.1 on `bind`, a host and port, on every
/// address it resolves to, and returns once the server listens. The server
/// runs on threads of its own until the returned [`AxumServer`] is shut down
/// or dropped.
///
/// Like the oxhttp server, it sends `config.server_name` as the `Server`
/// header and serves at most `config.max_connections` connections at once. A
/// client gets `config.timeout` to send a request's headers, and to take each
/// next part of a response: one that stops reading is disconnected, which
/// cancels its query.
pub fn spawn(
    service: SparqlService,
    bind: &str,
    config: &ServerConfig,
) -> anyhow::Result<AxumServer> {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for address in bind.to_socket_addrs()? {
        let listener = std::net::TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        addresses.push(listener.local_addr()?);
        listeners.push(listener);
    }
    anyhow::ensure!(!listeners.is_empty(), "{bind} resolves to no address");
    let server_name = HeaderValue::from_str(&config.server_name)?;
    let app = router(service).layer(middleware::map_response(
        move |mut response: AxumResponse| {
            let server_name = server_name.clone();
            async move {
                response.headers_mut().entry(SERVER).or_insert(server_name);
                response
            }
        },
    ));
    let limits = Limits {
        max_connections: max_connections(config)?,
        timeout: config.timeout,
    };
    let (shutdown, shutdown_requested) = oneshot::channel();
    // The runtime lives and dies on the server's thread: dropped on the
    // caller's, as on an error here, it would panic in async code.
    let (listening, started) = std::sync::mpsc::channel::<io::Result<()>>();
    let thread = thread::Builder::new()
        .name("de-serve".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("de-serve")
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = listening.send(Err(error));
                    return;
                }
            };
            let listeners = {
                let _runtime = runtime.enter();
                listeners
                    .into_iter()
                    .map(TcpListener::from_std)
                    .collect::<io::Result<Vec<_>>>()
            };
            match listeners {
                Ok(listeners) => {
                    let _ = listening.send(Ok(()));
                    runtime.block_on(run(listeners, app, limits, shutdown_requested));
                }
                Err(error) => {
                    let _ = listening.send(Err(error));
                }
            }
            runtime.shutdown_timeout(Duration::from_secs(1));
        })?;
    match started.recv() {
        Ok(Ok(())) => Ok(AxumServer {
            addresses,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }),
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error.into())
        }
        Err(_) => {
            let _ = thread.join();
            anyhow::bail!("the server's thread stopped before it listened")
        }
    }
}

/// `de serve` on axum: the HDT files in `location`, with the query page, on
/// `bind`. Returns the running server once it listens, having told systemd,
/// or whatever set `NOTIFY_SOCKET`.
pub fn start(location: &str, bind: &str, options: ServiceOptions) -> anyhow::Result<AxumServer> {
    let server = spawn(
        hdt_service(location, options)?,
        bind,
        &ServerConfig::default(),
    )?;
    #[cfg(target_os = "linux")]
    super::server::systemd_notify_ready()?;
    eprintln!("Listening for requests at http://{bind}");
    Ok(server)
}

/// Accepts connections on `listeners` until shutdown is requested, then
/// waits for the open ones to finish, for up to the timeout.
async fn run(
    listeners: Vec<TcpListener>,
    app: Router,
    limits: Limits,
    shutdown_requested: oneshot::Receiver<()>,
) {
    let (stop, stopping) = watch::channel(());
    let connections = Arc::new(Semaphore::new(limits.max_connections));
    let accepting: Vec<_> = listeners
        .into_iter()
        .map(|listener| {
            tokio::spawn(accept(
                listener,
                app.clone(),
                Arc::clone(&connections),
                limits.timeout,
                stopping.clone(),
            ))
        })
        .collect();
    // A dropped sender asks for shutdown too.
    let _ = shutdown_requested.await;
    let _ = stop.send(());
    for task in accepting {
        let _ = task.await;
    }
    // Every connection holds a permit until it closes.
    let all = u32::try_from(limits.max_connections).unwrap_or(u32::MAX);
    let _ = tokio::time::timeout(limits.timeout, connections.acquire_many(all)).await;
}

/// Accepts connections on `listener` and serves each on a task of its own,
/// until `stopping` changes.
async fn accept(
    listener: TcpListener,
    app: Router,
    connections: Arc<Semaphore>,
    header_timeout: Duration,
    mut stopping: watch::Receiver<()>,
) {
    loop {
        let permit = tokio::select! {
            permit = Arc::clone(&connections).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return,
            },
            _ = stopping.changed() => return,
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => {
                    // Such as when the process runs out of file descriptors:
                    // waiting beats spinning.
                    warn!("accepting a connection failed: {error}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = stopping.changed() => return,
        };
        let _ = stream.set_nodelay(true);
        let stream = WriteTimeout::new(stream, header_timeout);
        let service = TowerToHyperService::new(app.clone());
        let mut stopping = stopping.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut builder = http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout);
            let connection = builder.serve_connection(TokioIo::new(stream), service);
            tokio::pin!(connection);
            tokio::select! {
                _ = connection.as_mut() => {}
                _ = stopping.changed() => {
                    connection.as_mut().graceful_shutdown();
                    let _ = connection.await;
                }
            }
        });
    }
}

/// A connection whose writes fail once one has waited `timeout` for the
/// client to take more. Reads have no timeout: hyper reads while it streams
/// a response, to notice a client that goes away, and a long answer is no
/// reason to drop the connection.
struct WriteTimeout<T> {
    inner: T,
    timeout: Duration,
    /// Set while a write waits for the client.
    waiting: Option<Pin<Box<Sleep>>>,
}

impl<T> WriteTimeout<T> {
    fn new(inner: T, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            waiting: None,
        }
    }

    /// What a write that `inner` left pending returns: still pending, or an
    /// error once it has waited `timeout`.
    fn wait<R>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<R>> {
        let timeout = self.timeout;
        let waiting = self
            .waiting
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        match waiting.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("the client took nothing for {timeout:?}"),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Passes on a write's outcome, waiting while it's pending.
    fn written<R>(
        &mut self,
        cx: &mut Context<'_>,
        written: Poll<io::Result<R>>,
    ) -> Poll<io::Result<R>> {
        match written {
            Poll::Ready(result) => {
                self.waiting = None;
                Poll::Ready(result)
            }
            Poll::Pending => self.wait(cx),
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for WriteTimeout<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for WriteTimeout<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.written(cx, written)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.written(cx, written)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let flushed = Pin::new(&mut self.inner).poll_flush(cx);
        self.written(cx, flushed)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        serve::{HdtDirectory, ServiceOptions},
        sparql::AggregateHdt,
    };
    use endpoint_kit::{HttpTarget, Target};
    use http::header::{ACCEPT, CONTENT_TYPE};

    fn service(dir: &tempfile::TempDir) -> SparqlService {
        SparqlService::new(HdtDirectory::with_store(AggregateHdt::empty(), dir.path()))
    }

    fn get(uri: &str) -> AxumRequest {
        Request::get(uri)
            .header(HOST, "localhost:7878")
            .body(Body::empty())
            .unwrap()
    }

    async fn body_text(response: AxumResponse) -> String {
        let bytes = ::axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn query_uri(query: &str) -> String {
        let query: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        format!("/query?query={query}")
    }

    #[tokio::test]
    async fn a_query_is_answered() {
        let dir = tempfile::tempdir().unwrap();
        let response = handle(Arc::new(service(&dir)), get(&query_uri("ASK {}"))).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("true"));
    }

    #[tokio::test]
    async fn a_request_without_host_is_400() {
        let dir = tempfile::tempdir().unwrap();
        let request = Request::get(query_uri("ASK {}"))
            .body(Body::empty())
            .unwrap();
        let response = handle(Arc::new(service(&dir)), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(body_text(response).await.contains("Host"));
    }

    #[test]
    fn the_uri_is_made_absolute_from_host() {
        let (mut parts, ()) = Request::get("/query?query=ASK")
            .header(HOST, "example.org:8080")
            .body(())
            .unwrap()
            .into_parts();
        make_absolute(&mut parts).unwrap();
        assert_eq!(parts.uri, "http://example.org:8080/query?query=ASK");

        // A URI that's already absolute is kept, Host or not.
        let (mut parts, ()) = Request::get("http://example.com/query")
            .body(())
            .unwrap()
            .into_parts();
        make_absolute(&mut parts).unwrap();
        assert_eq!(parts.uri, "http://example.com/query");
    }

    /// An answer of several chunks comes through whole and in order.
    #[tokio::test]
    async fn a_large_answer_streams_whole() {
        let dir = tempfile::tempdir().unwrap();
        let values = (0..20_000).map(|i| i.to_string()).collect::<Vec<_>>();
        let query = format!("SELECT ?x WHERE {{ VALUES ?x {{ {} }} }}", values.join(" "));
        let request = Request::post("/query")
            .header(HOST, "localhost")
            .header(CONTENT_TYPE, "application/sparql-query")
            .header(ACCEPT, "text/tab-separated-values")
            .body(Body::from(query))
            .unwrap();
        let response = handle(Arc::new(service(&dir)), request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let text = body_text(response).await;
        assert!(text.len() > 2 * BODY_CHUNK_BYTES, "{} bytes", text.len());
        let rows: Vec<&str> = text.lines().skip(1).collect();
        assert_eq!(rows.len(), values.len());
        assert!(
            rows.iter()
                .zip(&values)
                .all(|(row, value)| row.contains(value.as_str()))
        );
    }

    #[tokio::test]
    async fn a_body_over_the_limit_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let service = service(&dir).with_options(ServiceOptions {
            max_body_bytes: 64,
            ..ServiceOptions::default()
        });
        let request = Request::post("/query")
            .header(HOST, "localhost")
            .header(CONTENT_TYPE, "application/sparql-query")
            .body(Body::from(format!("ASK {{}} # {}", "x".repeat(1024))))
            .unwrap();
        let response = handle(Arc::new(service), request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(body_text(response).await.contains("limited to 64 bytes"));
    }

    /// An address that's taken is an error, even from async code, where
    /// dropping the server's unused runtime would panic.
    #[tokio::test]
    async fn spawn_on_a_taken_address_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = taken.local_addr().unwrap().to_string();
        let error = spawn(service(&dir), &bind, &ServerConfig::default())
            .err()
            .expect("bound an address that was taken");
        assert!(error.to_string().contains("in use"), "{error:#}");
    }

    /// A write the client doesn't take fails after the timeout; one it takes
    /// in time doesn't.
    #[tokio::test]
    async fn a_write_the_client_doesnt_take_times_out() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let (server, mut client) = tokio::io::duplex(16);
        let mut server = WriteTimeout::new(server, Duration::from_millis(100));
        server.write_all(b"fits").await.unwrap();
        let mut taken = [0; 4];
        client.read_exact(&mut taken).await.unwrap();

        let started = std::time::Instant::now();
        let error = server.write_all(&[0; 64]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= Duration::from_millis(100));
    }

    /// A server of its own: it names itself, and stops listening on shutdown.
    #[test]
    fn spawn_serves_until_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let server = spawn(service(&dir), "127.0.0.1:0", &ServerConfig::default()).unwrap();
        let address = server.local_addrs()[0];
        let target = HttpTarget::new(format!("http://{address}"));
        let mut body = String::new();
        let response = target
            .send(Request::get(query_uri("ASK {}")).body(Vec::new()).unwrap())
            .unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.headers.get(SERVER).unwrap(),
            concat!("de/", env!("CARGO_PKG_VERSION"))
        );
        response
            .body
            .take(1 << 20)
            .read_to_string(&mut body)
            .unwrap();
        assert!(body.contains("true"));

        server.shutdown();
        assert!(std::net::TcpStream::connect(address).is_err());
    }
}
