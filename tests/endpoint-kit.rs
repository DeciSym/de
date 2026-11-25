// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Runs the `endpoint-kit` cases against `de serve`: once by calling
//! `de::serve::SparqlService` directly, once against the `de` binary over a
//! socket.
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

use de::serve::{HdtDirectory, RequestBody, ServiceOptions, SparqlService, Yasgui};
use endpoint_kit::{
    Capabilities, FixtureSet, InProcess, KitResponse, PeakAlloc, Report, ServiceSettings,
    SpawnedServer, Suite, Target,
};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[global_allocator]
static ALLOCATOR: PeakAlloc = PeakAlloc::new();

/// The cases `de serve` fails today, and why.
const EXPECTED_FAILURES: &[(&str, &str)] = &[
    (
        "w3c/query_content_type_describe",
        "graph results default to N-Quads",
    ),
    (
        "w3c/query_content_type_construct",
        "graph results default to N-Quads",
    ),
    (
        "de/wrong_method_is_405",
        "an unsupported method is 404, without an Allow header",
    ),
    (
        "contract/disconnect_stops_evaluation",
        "oxhttp notices a client left only when it next writes, and the response starts with the first result",
    ),
];

/// Both runs turn CORS on, so that the CORS cases check `--cors`.
fn suite(label: &str) -> Suite {
    let capabilities = Capabilities {
        cors: true,
        ..Capabilities::default()
    };
    EXPECTED_FAILURES.iter().fold(
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

fn in_process(
    fixtures: &FixtureSet,
    settings: &ServiceSettings,
) -> anyhow::Result<Box<dyn Target>> {
    let mut options = ServiceOptions::default();
    options.cors = true;
    options.query_timeout = settings.query_timeout;
    let service = SparqlService::new(HdtDirectory::new(build_store(fixtures)?)?)
        .with_options(options)
        .with_routes(Yasgui);
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
    let in_process = suite("de::serve::SparqlService, in process")
        .allocator(&ALLOCATOR)
        .run(in_process)?;
    let over_http = suite("de serve, over HTTP").run(spawned)?;
    // Both reports are written before either is checked, so a failure in
    // one doesn't hide the other's.
    let first = finish(&in_process, "endpoint-kit-in-process.txt");
    let second = finish(&over_http, "endpoint-kit-http.txt");
    first.and(second)
}
