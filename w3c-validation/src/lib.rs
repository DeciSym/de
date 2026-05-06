// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Shared W3C RDF/SPARQL conformance harness for `de` and downstream crates.
//!
//! This crate vends manifest discovery, case execution, result comparison, and
//! report emission for the W3C `rdf-tests` upstream snapshot.
//!
//! ## Pluggable query runner
//!
//! The non-trivial difference between consumers is *which* query function each
//! one wants to exercise. Everything else is shared. So the entry point takes a
//! runner callback:
//!
//! ```ignore
//! use w3c_validation::{run_w3c_rdf_tests_and_emit_report, W3cRunInputs};
//! use futures::future::BoxFuture;
//!
//! let runner = |args: W3cRunInputs<'_>| -> BoxFuture<'_, anyhow::Result<()>> {
//!     Box::pin(async move {
//!         // call your engine's query function with `args.*`,
//!         // write CSV/Turtle/whatever bytes into `args.writer`.
//!         Ok(())
//!     })
//! };
//! run_w3c_rdf_tests_and_emit_report(&"target/w3c-report.txt".into(), runner).await
//! ```
//!
//! ## Result policy
//! - `CaseStatus::Fail` fails the run (the `anyhow::Result<()>` returned from
//!   `run_w3c_rdf_tests_and_emit_report` is `Err` when any case failed).
//! - `CaseStatus::Unsupported` is reported with a reason and does not fail.
//!   Reserved for explicitly out-of-scope features (e.g. SPARQL Update
//!   evaluation against the read-only HDT backend).
//!
//! ## Comparison semantics
//! - Solution rows are compared by binding semantics, not column order.
//! - `ORDER BY` and `REDUCED` are detected from the parsed query algebra.
//! - Blank-node result sets use isomorphism rather than raw label equality.
//! - Literal lexical forms are normalized only within the same datatype to
//!   avoid false negatives while preserving W3C datatype semantics.

use de::query::DeOutput;
use futures::future::BoxFuture;
use oxrdfio::RdfFormat;
use sparesults::QueryResultsFormat;
use std::path::{Path, PathBuf};

mod compare;
mod manifest;
mod report;
mod run;

// Public re-exports so consumers don't need a parallel `use de::query::...`.
pub use de::query::DeOutput as W3cQueryOutput;
pub use de::query::{EntailmentMode, NamedGraphBinding};

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

/// Returns the W3C `rdf-tests` vendored snapshot root.
///
/// The snapshot is the upstream `https://github.com/w3c/rdf-tests` git
/// submodule, vendored *inside this crate* at `<w3c-validation>/rdf-tests`.
/// Override with the `DE_W3C_RESOURCES_DIR` env var if your checkout layout
/// differs.
///
/// `CARGO_MANIFEST_DIR` resolves to `<de>/w3c-validation` at build time of
/// this crate, so the default join already lands on a canonical path with no
/// `..` segments — no canonicalize-then-fallback needed.
pub fn w3c_resources_root() -> PathBuf {
    if let Ok(custom) = std::env::var("DE_W3C_RESOURCES_DIR") {
        return PathBuf::from(custom);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("rdf-tests")
}

/// Inputs handed to a consumer's query runner for a single query-evaluation
/// case.
///
/// The runner calls into its engine's `do_query`-equivalent and writes the
/// raw response bytes (CSV / TSV / RDF / etc., per `out`) into `writer`.
/// The harness handles parsing those bytes back and comparing them to the
/// W3C expected result.
pub struct W3cRunInputs<'a> {
    pub data_files: &'a [String],
    pub named_graph_bindings: &'a [NamedGraphBinding],
    pub query_files: &'a [String],
    pub entailment: EntailmentMode,
    pub out: &'a DeOutput,
    pub writer: &'a mut Vec<u8>,
}

/// Pluggable query runner: takes [`W3cRunInputs`] and returns a future
/// resolving to `Ok(())` once results are written into the inputs' writer.
///
/// Consumers wrap their engine's `do_query` function in a closure of this
/// shape — see the crate-level docs for an example.
pub type BoxedQueryRunner =
    dyn for<'a> Fn(W3cRunInputs<'a>) -> BoxFuture<'a, anyhow::Result<()>> + Send + Sync;

/// A single test case discovered from a W3C manifest graph.
#[derive(Debug, Clone)]
pub(crate) struct W3cManifestCase {
    pub(crate) id: String,
    pub(crate) test_type: String,
    pub(crate) manifest: PathBuf,
    pub(crate) kind: ManifestCaseKind,
}

/// Internal normalized representation of supported W3C manifest case kinds.
#[derive(Debug, Clone)]
pub(crate) enum ManifestCaseKind {
    QueryEvaluation(W3cQueryCase),
    SparqlQuerySyntax {
        path: PathBuf,
        positive: bool,
    },
    SparqlUpdateSyntax {
        path: PathBuf,
        positive: bool,
    },
    RdfSyntax {
        action: PathBuf,
        format: RdfFormat,
        positive: bool,
    },
    RdfEval {
        action: PathBuf,
        expected: PathBuf,
        action_format: RdfFormat,
        expected_format: RdfFormat,
    },
    Entailment {
        action: PathBuf,
        expected: Option<PathBuf>,
        positive: bool,
        regime: Option<String>,
        recognized_datatypes: Vec<String>,
    },
    UnsupportedFeature {
        reason: String,
    },
    InvalidCase {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct W3cQueryCase {
    pub(crate) query: PathBuf,
    pub(crate) default_data: Vec<PathBuf>,
    pub(crate) named_graph_data: Vec<(String, PathBuf)>,
    pub(crate) result: PathBuf,
    pub(crate) entailment: bool,
    pub(crate) de_output: DeOutput,
    pub(crate) compare_kind: CompareKind,
}

#[derive(Debug, Clone)]
pub(crate) enum CompareKind {
    Query(QueryResultsFormat),
    Rdf(RdfFormat),
    QueryRdf(RdfFormat),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryForm {
    Solutions,
    Boolean,
    Graph,
}

/// Final per-case outcome written to the report.
#[derive(Debug, Clone)]
pub(crate) enum CaseStatus {
    Pass,
    Fail(String),
    Unsupported(String),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ParsedQueryResults {
    Boolean(bool),
    Solutions {
        variables: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

/// Discovers all W3C cases under [`w3c_resources_root`], runs each through
/// the supplied `runner` (for query-evaluation cases) plus the harness's
/// built-in syntax/RDF/entailment runners, and writes a status report to
/// `report_path`.
///
/// Returns `Err` when at least one case ended in [`CaseStatus::Fail`];
/// unsupported cases are reported but do not fail the run.
pub async fn run_w3c_rdf_tests_and_emit_report<F>(
    report_path: &Path,
    runner: F,
) -> anyhow::Result<()>
where
    F: for<'a> Fn(W3cRunInputs<'a>) -> BoxFuture<'a, anyhow::Result<()>>,
{
    let mut cases = manifest::discover_manifest_cases()?;
    assert!(!cases.is_empty(), "No W3C tests discovered");

    cases.sort_by(|a, b| {
        a.id.cmp(&b.id)
            .then_with(|| a.test_type.cmp(&b.test_type))
            .then_with(|| a.manifest.cmp(&b.manifest))
    });

    let mut report_rows = Vec::<(String, String, String, CaseStatus)>::new();
    for case in &cases {
        let status = run::run_case(case, &runner).await;
        report_rows.push((
            case.id.clone(),
            case.test_type.clone(),
            report::path_for_report(&case.manifest),
            status,
        ));
    }

    report::write_report(report_path, &report_rows)?;
    let fail_count = report_rows
        .iter()
        .filter(|(_, _, _, status)| matches!(status, CaseStatus::Fail(_)))
        .count();
    if fail_count > 0 {
        anyhow::bail!(
            "W3C rdf-tests report contains {fail_count} failing case(s); see {}",
            report_path.display()
        );
    }
    Ok(())
}
