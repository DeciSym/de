// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Integration test entrypoint for the W3C RDF/SPARQL conformance harness.
//!
//! The harness implementation lives in the sibling `w3c-validation` workspace
//! crate so it can be reused by `decisym-engine-rs`. This file only wires up
//! `de`'s own query function as the runner callback and points the harness at
//! a report path under this crate's `target/`.

use futures::future::BoxFuture;
use std::io::BufWriter;
use std::path::Path;
use w3c_validation::{W3cRunInputs, run_w3c_rdf_tests_and_emit_report};

/// Runner callback wired into the shared W3C harness. A free `fn` (rather than
/// a closure) is used here so the higher-ranked `for<'a> Fn(...)` bound the
/// harness requires falls out naturally — closures get coerced to a single
/// concrete lifetime and would fail to satisfy the HRTB.
fn run_query<'a>(args: W3cRunInputs<'a>) -> BoxFuture<'a, anyhow::Result<()>> {
    Box::pin(async move {
        let mut writer = BufWriter::new(args.writer);
        de::query::do_query_with_dataset(
            args.data_files,
            args.named_graph_bindings,
            args.query_files,
            args.entailment,
            args.out,
            &mut writer,
        )
        .await
    })
}

#[tokio::test]
async fn run_w3c_rdf_tests_and_emit_report_test() -> anyhow::Result<()> {
    let report_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/w3c-rdf-tests-report.txt");
    run_w3c_rdf_tests_and_emit_report(&report_path, run_query).await
}
