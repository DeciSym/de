//! Running a [`SparqlService`] over HTTP with oxhttp, and `de serve`.
//!
//! This module and [`axum`](super::axum) are the only ones that know an HTTP
//! server: the service takes and returns its own body types, which they
//! convert.

use super::{
    body::{RequestBody, ResponseBody},
    hdt_dir::{HdtDirectory, collect_hdt_paths},
    service::{ServiceOptions, SparqlService},
    ui::Yasgui,
};
use crate::sparql::AggregateHdt;
use http::{Request, Response};
use log::warn;
use oxhttp::{ListeningServer, Server, model::Body};
use std::{
    io, net::ToSocketAddrs, path::Path, sync::Arc, thread::available_parallelism, time::Duration,
};

/// How the HTTP server runs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServerConfig {
    /// The `Server` header. Default: `de/` and this crate's version.
    pub server_name: String,
    /// How long a connection may wait on a read or a write. Default: one minute.
    pub timeout: Duration,
    /// The most connections served at once. Default: 128 per CPU.
    pub max_connections: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server_name: concat!("de/", env!("CARGO_PKG_VERSION")).to_string(),
            timeout: Duration::from_mins(1),
            max_connections: None,
        }
    }
}

/// Starts serving `service` on `bind` (a host and port), and returns once it
/// listens. The server runs on threads of its own until the process exits.
pub fn spawn(
    service: SparqlService,
    bind: &str,
    config: &ServerConfig,
) -> anyhow::Result<ListeningServer> {
    let service = Arc::new(service);
    let max_connections = max_connections(config)?;
    let mut server = Server::new(move |request| handle(&service, request))
        .with_global_timeout(config.timeout)
        .with_server_name(config.server_name.clone())?
        .with_max_concurrent_connections(max_connections);
    for socket in bind.to_socket_addrs()? {
        server = server.bind(socket);
    }
    Ok(server.spawn()?)
}

/// The most connections `config` allows at once.
pub(super) fn max_connections(config: &ServerConfig) -> io::Result<usize> {
    match config.max_connections {
        Some(max) => Ok(max),
        None => Ok(available_parallelism()?.get() * 128),
    }
}

/// Answers one oxhttp request with `service`, converting the bodies.
fn handle(service: &SparqlService, request: &mut Request<Body>) -> Response<Body> {
    let (parts, body) = std::mem::take(request).into_parts();
    let mut request = Request::from_parts(parts, RequestBody::from_reader(body));
    service.handle(&mut request).map(|body| match body {
        ResponseBody::Empty => Body::empty(),
        ResponseBody::Bytes(bytes) => Body::from(bytes),
        ResponseBody::Stream(reader) => Body::from_read(reader),
    })
}

/// `de serve`: the HDT files in `location`, with the query page, on `bind`,
/// until the process exits.
pub fn serve(location: &str, bind: &str, options: ServiceOptions) -> anyhow::Result<()> {
    let hdt_paths = collect_hdt_paths(Path::new(location))?;

    eprintln!("Found {} HDT files in {}", hdt_paths.len(), location);
    for path in &hdt_paths {
        eprintln!("  - {path}");
    }

    // Create the AggregateHdt store from the found HDT files
    let store = if hdt_paths.is_empty() {
        warn!("Warning: No HDT files found in the specified locations: {location}");
        AggregateHdt::empty()
    } else {
        AggregateHdt::new(&hdt_paths)?
    };

    let service = SparqlService::new(HdtDirectory::with_store(store, location))
        .with_options(options)
        .with_routes(Yasgui);
    let server = spawn(service, bind, &ServerConfig::default())?;
    #[cfg(target_os = "linux")]
    systemd_notify_ready()?;
    eprintln!("Listening for requests at http://{bind}");
    server.join()?;
    Ok(())
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
