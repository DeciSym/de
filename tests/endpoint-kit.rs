// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Runs the `endpoint-kit` cases against `de serve` three ways: by calling
//! `de::serve::SparqlService` directly, against the `de` binary over a
//! socket, and against `de::serve::axum`'s server, in this process, over a
//! socket. The services built in this process also answer the Graph Store
//! Protocol's reads, as a server built on `de` can; the binary's run checks
//! that `de serve` keeps answering 501 on `/store`.
//!
//! The cases live in the `endpoint-kit` workspace crate so that servers built
//! on `de` can run them too. This file loads the kit's fixture graphs into
//! HDT files, one per graph with its IRI in the header, and lists the cases
//! `de` fails today. Each entry names the fix that should remove it; the
//! report is written to `target/endpoint-kit-*.txt`.
//!
//! The suite is a single test so that nothing else runs while the memory and
//! CPU-time contracts measure this process.

#![cfg(feature = "server")]

use de::serve::{
    HdtDirectory, RequestBody, ServerConfig, ServiceOptions, SparqlService, Yasgui,
    axum::AxumServer,
};
use endpoint_kit::{
    Capabilities, FixtureSet, HttpTarget, InProcess, KitResponse, PeakAlloc, Report, ServerProcess,
    ServiceSettings, SpawnedServer, Suite, Target,
};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

#[global_allocator]
static ALLOCATOR: PeakAlloc = PeakAlloc::new();

/// Every run turns CORS on, so that the CORS cases check `--cors`.
fn suite(
    label: &str,
    graph_store_read: bool,
    expected_failures: &[(&'static str, &'static str)],
) -> Suite {
    let capabilities = Capabilities {
        cors: true,
        graph_store_read,
        ..Capabilities::default()
    };
    expected_failures.iter().fold(
        Suite::new(label).capabilities(capabilities),
        |suite, (id, reason)| suite.expect_failure(id, reason),
    )
}

/// Builds one HDT per fixture graph in `<fixtures>/store`, with the graph's
/// IRI in its header so that `de` serves it under that IRI.
fn build_store(fixtures: &FixtureSet) -> anyhow::Result<PathBuf> {
    let store = fixtures.dir.join("store");
    std::fs::create_dir_all(&store)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for graph in &fixtures.graphs {
        let hdt = store.join(format!("{}.hdt", graph.name));
        runtime.block_on(de::create::do_create_with_options(
            Some(&hdt.to_string_lossy()),
            &[graph.ntriples.to_string_lossy().into_owned()],
            false,
            Some(&graph.iri),
            &[],
            None,
            None,
        ))?;
    }
    Ok(store)
}

/// `de serve`'s service, with its query page, over the fixtures, and with
/// the Graph Store Protocol's reads on.
fn service(fixtures: &FixtureSet, settings: &ServiceSettings) -> anyhow::Result<SparqlService> {
    let mut options = ServiceOptions::default();
    options.cors = true;
    options.query_timeout = settings.query_timeout;
    options.graph_store_read = true;
    Ok(
        SparqlService::new(HdtDirectory::new(build_store(fixtures)?)?)
            .with_options(options)
            .with_routes(Yasgui),
    )
}

fn in_process(
    fixtures: &FixtureSet,
    settings: &ServiceSettings,
) -> anyhow::Result<Box<dyn Target>> {
    let service = service(fixtures, settings)?;
    Ok(Box::new(InProcess::new(
        move |request: http::Request<Vec<u8>>| {
            let mut request = request.map(RequestBody::from);
            let (parts, body) = service.handle(&mut request).into_parts();
            Ok(KitResponse {
                status: parts.status,
                headers: parts.headers,
                body: body.into_reader(),
            })
        },
    )))
}

fn spawned(fixtures: &FixtureSet, settings: &ServiceSettings) -> anyhow::Result<Box<dyn Target>> {
    let store_dir = build_store(fixtures)?;
    let port = endpoint_kit::free_port()?;
    let bind = format!("127.0.0.1:{port}");
    let mut command = Command::new(env!("CARGO_BIN_EXE_de"));
    command
        .arg("serve")
        .arg("--location")
        .arg(&store_dir)
        .arg("--bind")
        .arg(&bind)
        .arg("--cors");
    if let Some(timeout) = settings.query_timeout {
        command
            .arg("--timeout-s")
            .arg(timeout.as_secs_f64().to_string());
    }
    let server = SpawnedServer::start(
        command,
        &format!("http://{bind}"),
        "/query",
        &fixtures.dir.join("server.log"),
        Duration::from_secs(30),
    )?;
    Ok(Box::new(server))
}

/// `de::serve::axum`'s server, in this process, which the kit reaches over a
/// socket; stopping the target shuts the server down.
struct AxumTarget {
    http: HttpTarget,
    server: Mutex<Option<AxumServer>>,
}

impl Target for AxumTarget {
    fn send(&self, request: http::Request<Vec<u8>>) -> anyhow::Result<KitResponse> {
        self.http.send(request)
    }

    fn server_process(&self) -> ServerProcess {
        self.http.server_process()
    }

    fn send_without_reading(
        &self,
        request: http::Request<Vec<u8>>,
    ) -> anyhow::Result<Option<Box<dyn Send>>> {
        self.http.send_without_reading(request)
    }

    fn stop(&self) {
        let server = self.server.lock().map(|mut server| server.take());
        if let Ok(Some(server)) = server {
            server.shutdown();
        }
    }
}

fn on_axum(fixtures: &FixtureSet, settings: &ServiceSettings) -> anyhow::Result<Box<dyn Target>> {
    let server = de::serve::axum::spawn(
        service(fixtures, settings)?,
        "127.0.0.1:0",
        &ServerConfig::default(),
    )?;
    let address = server.local_addrs()[0];
    Ok(Box::new(AxumTarget {
        http: HttpTarget::new(format!("http://{address}")).with_process(ServerProcess::InProcess),
        server: Mutex::new(Some(server)),
    }))
}

fn finish(report: &Report, file: &str) -> anyhow::Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(file);
    report.write(&path)?;
    eprintln!("{}", report.render());
    report.check()
}

#[test]
fn endpoint_kit() -> anyhow::Result<()> {
    let in_process = suite("de::serve::SparqlService, in process", true, &[])
        .allocator(&ALLOCATOR)
        .run(in_process)?;
    let over_http = suite("de serve, over HTTP", false, &[]).run(spawned)?;
    let axum = suite("de::serve::axum, over HTTP", true, &[])
        .allocator(&ALLOCATOR)
        .run(on_axum)?;
    // Every report is written before any is checked, so a failure in one
    // doesn't hide the others'.
    let checks = [
        finish(&in_process, "endpoint-kit-in-process.txt"),
        finish(&over_http, "endpoint-kit-http.txt"),
        finish(&axum, "endpoint-kit-axum.txt"),
    ];
    checks.into_iter().collect()
}
