//! What a server around a [`SparqlService`] shares, whatever runs it: its
//! settings, and the service `de serve` runs. [`axum`](super::axum) runs it.

use super::{
    hdt_dir::{HdtDirectory, collect_hdt_paths},
    service::{ServiceOptions, SparqlService},
    ui::Yasgui,
};
use crate::sparql::AggregateHdt;
use log::warn;
use std::{io, path::Path, thread::available_parallelism, time::Duration};

/// How the HTTP server runs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServerConfig {
    /// The `Server` header. Default: `de/` and this crate's version.
    pub server_name: String,
    /// How long a connection may wait on the client: to send a request's
    /// headers, and to take each next part of a response. Also how long open
    /// connections get to finish when the server shuts down. Default: one
    /// minute.
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

/// The most connections `config` allows at once.
pub(super) fn max_connections(config: &ServerConfig) -> io::Result<usize> {
    match config.max_connections {
        Some(max) => Ok(max),
        None => Ok(available_parallelism()?.get() * 128),
    }
}

/// `de serve`'s service: the HDT files in `location`, with the query page.
pub(super) fn hdt_service(
    location: &str,
    options: ServiceOptions,
) -> anyhow::Result<SparqlService> {
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

    Ok(
        SparqlService::new(HdtDirectory::with_store(store, location))
            .with_options(options)
            .with_routes(Yasgui),
    )
}

/// Tells systemd, or whatever set `NOTIFY_SOCKET`, that the server listens.
#[cfg(target_os = "linux")]
pub(super) fn systemd_notify_ready() -> io::Result<()> {
    use std::env;

    if let Some(path) = env::var_os("NOTIFY_SOCKET") {
        use std::os::unix::net::UnixDatagram;

        UnixDatagram::unbound()?.send_to(b"READY=1", path)?;
    }
    Ok(())
}
