// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! The servers the kit can test, and how it reaches them.
//!
//! Nothing here depends on the HTTP stack of the server under test. A
//! [`Target`] takes an [`http::Request`] with a byte body and returns the
//! status, headers and a streaming body. [`InProcess`] wraps a handler the
//! consumer calls directly (no socket), and [`HttpTarget`] talks to a server
//! over the network, such as one started with [`SpawnedServer`].

use anyhow::Context as _;
use http::{HeaderMap, Request, StatusCode};
use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// A response as the kit sees it.
pub struct KitResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// The body, read as it streams in. A read error means the server cut the
    /// response off.
    pub body: Box<dyn Read>,
}

/// Where the server under test runs, for the contracts that watch its CPU time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerProcess {
    /// In this process: the handler is called directly.
    InProcess,
    /// In the process with this id, on this machine.
    Pid(u32),
    /// Somewhere the kit can't observe.
    Unknown,
}

/// A SPARQL endpoint the kit sends requests to.
pub trait Target: Send + Sync {
    /// Sends `request`. Its URI holds only a path and query, such as
    /// `/query?query=ASK%7B%7D`; the target supplies the rest.
    fn send(&self, request: Request<Vec<u8>>) -> anyhow::Result<KitResponse>;

    /// The process that evaluates the queries.
    fn server_process(&self) -> ServerProcess;

    /// Stops the server, if the target started one. The suite calls this when
    /// its cases are done: a contract can leave a request running on a thread
    /// of its own, which keeps the target alive past the end of the run.
    fn stop(&self) {}
}

/// A handler called directly, without a socket.
///
/// The handler gets requests with an absolute URI on `http://localhost`, which
/// is also what a server sees as its base IRI. Converting to and from the
/// server's own request and body types is the consumer's job, which keeps
/// this crate independent of the server's HTTP stack.
pub struct InProcess<F> {
    handler: F,
}

impl<F> InProcess<F>
where
    F: Fn(Request<Vec<u8>>) -> anyhow::Result<KitResponse> + Send + Sync,
{
    pub fn new(handler: F) -> Self {
        Self { handler }
    }
}

impl<F> Target for InProcess<F>
where
    F: Fn(Request<Vec<u8>>) -> anyhow::Result<KitResponse> + Send + Sync,
{
    fn send(&self, request: Request<Vec<u8>>) -> anyhow::Result<KitResponse> {
        let (mut parts, body) = request.into_parts();
        parts.uri = format!("http://localhost{}", parts.uri)
            .parse()
            .context("building the request URI")?;
        (self.handler)(Request::from_parts(parts, body))
    }

    fn server_process(&self) -> ServerProcess {
        ServerProcess::InProcess
    }
}

/// A server reached over HTTP.
pub struct HttpTarget {
    base_url: String,
    client: oxhttp::Client,
    process: ServerProcess,
}

impl HttpTarget {
    /// A server at `base_url`, such as `http://127.0.0.1:7878`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: oxhttp::Client::new().with_global_timeout(Duration::from_secs(120)),
            process: ServerProcess::Unknown,
        }
    }

    /// Names the process serving `base_url`, so the CPU-time contracts can run.
    #[must_use]
    pub fn with_process(mut self, process: ServerProcess) -> Self {
        self.process = process;
        self
    }
}

impl Target for HttpTarget {
    fn send(&self, request: Request<Vec<u8>>) -> anyhow::Result<KitResponse> {
        let (parts, body) = request.into_parts();
        let mut builder = Request::builder()
            .method(parts.method)
            .uri(format!("{}{}", self.base_url, parts.uri));
        for (name, value) in &parts.headers {
            builder = builder.header(name, value);
        }
        let response = self
            .client
            .request(builder.body(oxhttp::model::Body::from(body))?)
            .with_context(|| format!("sending a request to {}", self.base_url))?;
        let (parts, body) = response.into_parts();
        Ok(KitResponse {
            status: parts.status,
            headers: parts.headers,
            body: Box::new(body),
        })
    }

    fn server_process(&self) -> ServerProcess {
        self.process
    }
}

/// A port on 127.0.0.1 that was free a moment ago.
///
/// There is a window between this call and the server binding the port, in
/// which something else can take it; that's rare enough for tests.
pub fn free_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("finding a free port")?;
    Ok(listener.local_addr()?.port())
}

/// A server process the kit started, stopped when this is dropped.
///
/// Its stderr goes to a log file, which is quoted if the server never answers.
pub struct SpawnedServer {
    child: Mutex<Child>,
    pid: u32,
    target: HttpTarget,
}

impl SpawnedServer {
    /// Starts `command` and waits until `GET {base_url}{ready_path}` gets any
    /// HTTP response, or `timeout` passes.
    pub fn start(
        mut command: Command,
        base_url: &str,
        ready_path: &str,
        log: &Path,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let log_file = File::create(log).with_context(|| format!("creating {}", log.display()))?;
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log_file)
            .spawn()
            .with_context(|| format!("starting {command:?}"))?;
        let pid = child.id();
        let server = Self {
            child: Mutex::new(child),
            pid,
            target: HttpTarget::new(base_url).with_process(ServerProcess::Pid(pid)),
        };
        let started = Instant::now();
        loop {
            let exited = server.child().try_wait()?;
            if let Some(status) = exited {
                anyhow::bail!(
                    "{command:?} exited with {status} before answering:\n{}",
                    log_tail(log)
                );
            }
            let probe = Request::get(ready_path).body(Vec::new())?;
            if server.target.send(probe).is_ok() {
                return Ok(server);
            }
            if started.elapsed() > timeout {
                server.stop();
                anyhow::bail!(
                    "{command:?} didn't answer at {base_url}{ready_path} within {timeout:?}:\n{}",
                    log_tail(log)
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The server's process id.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    fn child(&self) -> MutexGuard<'_, Child> {
        // A panic while holding the lock leaves the child as it was.
        self.child.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Target for SpawnedServer {
    fn send(&self, request: Request<Vec<u8>>) -> anyhow::Result<KitResponse> {
        self.target.send(request)
    }

    fn server_process(&self) -> ServerProcess {
        self.target.server_process()
    }

    fn stop(&self) {
        let mut child = self.child();
        let _ = child.kill();
        let _ = child.wait();
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        Target::stop(self);
    }
}

fn log_tail(log: &Path) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let start = text.len().saturating_sub(4000);
    let start = (start..text.len())
        .find(|i| text.is_char_boundary(*i))
        .unwrap_or(text.len());
    text[start..].to_string()
}
