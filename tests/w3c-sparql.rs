// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Integration test entrypoint for the W3C RDF/SPARQL conformance harness.
//!
//! Implementation is split across `tests/w3c_sparql/` modules to keep each
//! concern reviewable: manifest loading, case execution, result comparison,
//! and report emission.

mod w3c_sparql;

#[tokio::test]
async fn run_w3c_rdf_tests_and_emit_report() -> anyhow::Result<()> {
    w3c_sparql::run_w3c_rdf_tests_and_emit_report().await
}
