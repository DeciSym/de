// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! W3C RDF/SPARQL conformance runner for `de`.
//!
//! This harness executes a vendored snapshot of upstream W3C tests from
//! `tests/resources/rdf-tests` and emits a status report at
//! `target/w3c-rdf-tests-report.txt`.
//!
//! ## Scope
//! - Runs SPARQL query evaluation and syntax cases plus RDF syntax/eval cases.
//! - Runs entailment cases supported by this crate's reasoner integration.
//! - Tracks unsupported features explicitly (for example SPARQL Update
//!   evaluation), instead of silently skipping them.
//!
//! Result policy:
//! - `CaseStatus::Fail` fails this integration test.
//! - `CaseStatus::Unsupported` is reported (with reason) and does not fail the run.
//!   Unsupported cases should correspond only to explicitly out-of-scope features.
//!
//! ## Comparison semantics
//! - Solution rows are compared by binding semantics, not column order.
//! - `ORDER BY` and `REDUCED` are detected from the parsed query algebra.
//! - Blank-node result sets use isomorphism checks rather than raw label equality.
//! - Literal lexical forms are normalized only within the same datatype to avoid
//!   false negatives while still preserving W3C datatype semantics.
//!
//! This file is the W3C upstream runner. It is distinct from other integration
//! tests (for example CLI command smoke tests and cache-race stress tests).

use de::query::DeOutput;
use oxrdfio::RdfFormat;
use sparesults::QueryResultsFormat;
use std::path::PathBuf;

mod compare;
mod manifest;
mod report;
mod run;

const MF_INCLUDE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include";
const MF_ENTRIES: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries";
const MF_ACTION: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action";
const MF_NAME: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#name";
const MF_RESULT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result";
const MF_MANIFEST: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#Manifest";
const MF_QUERY_EVALUATION_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest";
const MF_CSV_RESULT_FORMAT_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#CSVResultFormatTest";
const MF_POSITIVE_SYNTAX_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest";
const MF_NEGATIVE_SYNTAX_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest";
const MF_POSITIVE_SYNTAX_TEST11: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11";
const MF_NEGATIVE_SYNTAX_TEST11: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest11";
const MF_POSITIVE_UPDATE_SYNTAX_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest";
const MF_NEGATIVE_UPDATE_SYNTAX_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeUpdateSyntaxTest";
const MF_POSITIVE_UPDATE_SYNTAX_TEST11: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11";
const MF_NEGATIVE_UPDATE_SYNTAX_TEST11: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeUpdateSyntaxTest11";
const MF_UPDATE_EVALUATION_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#UpdateEvaluationTest";
const MF_POSITIVE_ENTAILMENT_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveEntailmentTest";
const MF_NEGATIVE_ENTAILMENT_TEST: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeEntailmentTest";
const MF_ENTAILMENT_REGIME: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entailmentRegime";
const MF_RECOGNIZED_DATATYPES: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#recognizedDatatypes";

const QT_QUERY: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#query";
const QT_DATA: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#data";
const QT_GRAPH_DATA: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#graphData";
const QT_GRAPH: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#graph";
const UT_REQUEST: &str = "http://www.w3.org/2009/sparql/tests/test-update#request";
const SD_ENTAILMENT_REGIME: &str =
    "http://www.w3.org/ns/sparql-service-description#entailmentRegime";

const RDFT_TEST_NTRIPLES_POSITIVE_SYNTAX: &str =
    "http://www.w3.org/ns/rdftest#TestNTriplesPositiveSyntax";
const RDFT_TEST_NTRIPLES_NEGATIVE_SYNTAX: &str =
    "http://www.w3.org/ns/rdftest#TestNTriplesNegativeSyntax";
const RDFT_TEST_NQUADS_POSITIVE_SYNTAX: &str =
    "http://www.w3.org/ns/rdftest#TestNQuadsPositiveSyntax";
const RDFT_TEST_NQUADS_NEGATIVE_SYNTAX: &str =
    "http://www.w3.org/ns/rdftest#TestNQuadsNegativeSyntax";
const RDFT_TEST_TURTLE_POSITIVE_SYNTAX: &str =
    "http://www.w3.org/ns/rdftest#TestTurtlePositiveSyntax";
const RDFT_TEST_TURTLE_NEGATIVE_SYNTAX: &str =
    "http://www.w3.org/ns/rdftest#TestTurtleNegativeSyntax";
const RDFT_TEST_TRIG_POSITIVE_SYNTAX: &str = "http://www.w3.org/ns/rdftest#TestTrigPositiveSyntax";
const RDFT_TEST_TRIG_NEGATIVE_SYNTAX: &str = "http://www.w3.org/ns/rdftest#TestTrigNegativeSyntax";
const RDFT_TEST_XML_NEGATIVE_SYNTAX: &str = "http://www.w3.org/ns/rdftest#TestXMLNegativeSyntax";
const RDFT_TEST_TURTLE_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTurtleEval";
const RDFT_TEST_TRIG_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTrigEval";
const RDFT_TEST_XML_EVAL: &str = "http://www.w3.org/ns/rdftest#TestXMLEval";
const RDFT_TEST_TURTLE_NEGATIVE_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTurtleNegativeEval";
const RDFT_TEST_TRIG_NEGATIVE_EVAL: &str = "http://www.w3.org/ns/rdftest#TestTrigNegativeEval";

const W3C_MANIFEST_PATHS: [&str; 3] = [
    "sparql/sparql10/manifest.ttl",
    "sparql/sparql11/manifest.ttl",
    "rdf/rdf11/manifest.ttl",
];

/// A single test case discovered from a W3C manifest graph.
#[derive(Debug, Clone)]
struct W3cManifestCase {
    /// Human-readable case identifier (`mf:name` or subject fallback).
    id: String,
    /// Manifest test type IRI (for example `mf:QueryEvaluationTest`).
    test_type: String,
    /// Canonical path to the manifest that defined this case.
    manifest: PathBuf,
    /// Parsed and validated case payload.
    kind: ManifestCaseKind,
}

/// Internal normalized representation of supported W3C manifest case kinds.
#[derive(Debug, Clone)]
enum ManifestCaseKind {
    /// Query evaluation test (`mf:QueryEvaluationTest` or CSV variant).
    QueryEvaluation(W3cQueryCase),
    /// Positive or negative SPARQL query syntax test.
    SparqlQuerySyntax { path: PathBuf, positive: bool },
    /// Positive or negative SPARQL update syntax test.
    SparqlUpdateSyntax { path: PathBuf, positive: bool },
    /// Positive or negative RDF syntax test for a specific RDF format.
    RdfSyntax {
        action: PathBuf,
        format: RdfFormat,
        positive: bool,
    },
    /// RDF evaluation test: parse action and expected graph/dataset and compare.
    RdfEval {
        action: PathBuf,
        expected: PathBuf,
        action_format: RdfFormat,
        expected_format: RdfFormat,
    },
    /// Entailment test with optional expected graph (`mf:result=false` means inconsistency case).
    Entailment {
        action: PathBuf,
        expected: Option<PathBuf>,
        positive: bool,
        regime: Option<String>,
        recognized_datatypes: Vec<String>,
    },
    /// Explicitly unsupported case that should be reported, not hidden.
    UnsupportedFeature { reason: String },
    /// Manifest case that is malformed for this runner and should fail fast.
    InvalidCase { reason: String },
}

/// Parsed payload required to execute one query evaluation test case.
#[derive(Debug, Clone)]
struct W3cQueryCase {
    /// Query file path (`qt:query`).
    query: PathBuf,
    /// Default graph data files (`qt:data`).
    default_data: Vec<PathBuf>,
    /// Named graph bindings from graph IRI to local data file.
    named_graph_data: Vec<(String, PathBuf)>,
    /// Expected results file (`mf:result`).
    result: PathBuf,
    /// Whether entailment mode should be enabled for this case.
    entailment: bool,
    /// Output format requested from `de` when running the query.
    de_output: DeOutput,
    /// Comparison strategy derived from expected result file type.
    compare_kind: CompareKind,
}

/// How expected query output should be interpreted and compared.
#[derive(Debug, Clone)]
enum CompareKind {
    /// SPARQL results document (JSON/XML/TSV/CSV).
    Query(QueryResultsFormat),
    /// RDF graph/dataset serialization.
    Rdf(RdfFormat),
    /// RDF result-set graph that should be interpreted as query solutions.
    QueryRdf(RdfFormat),
}

/// High-level SPARQL query form, used to validate expected result type compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryForm {
    Solutions,
    Boolean,
    Graph,
}

/// Final per-case outcome written to the report.
#[derive(Debug, Clone)]
enum CaseStatus {
    Pass,
    Fail(String),
    Unsupported(String),
}

/// Canonical in-memory representation of parsed SPARQL query results.
#[derive(Debug, PartialEq, Eq)]
enum ParsedQueryResults {
    Boolean(bool),
    Solutions {
        variables: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

/// Executes all discovered W3C cases and writes a machine-readable status report.
///
/// The test fails if any case has [`CaseStatus::Fail`].
pub async fn run_w3c_rdf_tests_and_emit_report() -> anyhow::Result<()> {
    let mut cases = manifest::discover_manifest_cases()?;
    assert!(!cases.is_empty(), "No W3C tests discovered");

    cases.sort_by(|a, b| {
        a.id.cmp(&b.id)
            .then_with(|| a.test_type.cmp(&b.test_type))
            .then_with(|| a.manifest.cmp(&b.manifest))
    });

    let mut report_rows = Vec::<(String, String, String, CaseStatus)>::new();
    for case in &cases {
        let status = run::run_case(case).await;
        report_rows.push((
            case.id.clone(),
            case.test_type.clone(),
            report::path_for_report(&case.manifest),
            status,
        ));
    }

    report::write_report(&report_rows)?;
    let fail_count = report_rows
        .iter()
        .filter(|(_, _, _, status)| matches!(status, CaseStatus::Fail(_)))
        .count();
    if fail_count > 0 {
        anyhow::bail!(
            "W3C rdf-tests report contains {fail_count} failing case(s); see {}",
            report::report_output_path().display()
        );
    }
    Ok(())
}
