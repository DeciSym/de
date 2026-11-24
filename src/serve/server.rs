//! Running the endpoint: the HTTP server, CORS, and telling systemd it's ready.

use super::{error::error, hdt_dir::collect_hdt_paths, service::handle_request};
use crate::sparql::AggregateHdt;
use http::{
    HeaderValue, Method, Request, Response, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN,
    },
};
use log::warn;
use oxhttp::{Server, model::Body};
use std::{io, net::ToSocketAddrs, path::Path, thread::available_parallelism, time::Duration};

const HTTP_TIMEOUT: Duration = Duration::from_mins(1);

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

#[cfg(target_os = "linux")]
fn systemd_notify_ready() -> io::Result<()> {
    use std::env;

    if let Some(path) = env::var_os("NOTIFY_SOCKET") {
        use std::os::unix::net::UnixDatagram;

        UnixDatagram::unbound()?.send_to(b"READY=1", path)?;
    }
    Ok(())
}
