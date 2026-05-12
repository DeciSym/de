// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Case execution for the W3C RDF/SPARQL harness.
//!
//! This module runs per-case logic (syntax, query evaluation, entailment) and
//! invokes the same dataset query path used by the CLI.

#![allow(clippy::wildcard_imports)]

use super::{compare, manifest, report, *};
use futures::future::BoxFuture;
use oxrdf::{Dataset, Term, graph::CanonicalizationAlgorithm};
use oxrdfio::{RdfFormat, RdfParser};
use reasonable::reasoner::Reasoner;
use sparesults::QueryResultsFormat;
use spargebra::SparqlParser;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

pub(super) async fn run_case<F>(case: &W3cManifestCase, runner: &F) -> CaseStatus
where
    F: for<'a> Fn(W3cRunInputs<'a>) -> BoxFuture<'a, anyhow::Result<()>>,
{
    match &case.kind {
        ManifestCaseKind::InvalidCase { reason } => CaseStatus::Fail(reason.clone()),
        ManifestCaseKind::UnsupportedFeature { reason } => CaseStatus::Unsupported(reason.clone()),
        ManifestCaseKind::SparqlQuerySyntax { path, positive } => {
            run_sparql_query_syntax_case(path, *positive)
        }
        ManifestCaseKind::SparqlUpdateSyntax { path, positive } => {
            run_sparql_update_syntax_case(path, *positive)
        }
        ManifestCaseKind::RdfSyntax {
            action,
            format,
            positive,
        } => run_rdf_syntax_case(action, *format, *positive),
        ManifestCaseKind::RdfEval {
            action,
            expected,
            action_format,
            expected_format,
        } => run_rdf_eval_case(action, expected, *action_format, *expected_format),
        ManifestCaseKind::Entailment {
            action,
            expected,
            positive,
            regime,
            recognized_datatypes,
        } => run_entailment_case(
            action,
            expected.as_deref(),
            *positive,
            regime.as_deref(),
            recognized_datatypes,
        ),
        ManifestCaseKind::QueryEvaluation(test) => run_query_evaluation_case(test, runner).await,
    }
}

/// Executes one query evaluation case and compares actual output with the expected artifact.
///
/// Comparison strategy is selected from [`W3cQueryCase::compare_kind`].
#[allow(clippy::too_many_lines)]
async fn run_query_evaluation_case<F>(test: &W3cQueryCase, runner: &F) -> CaseStatus
where
    F: for<'a> Fn(W3cRunInputs<'a>) -> BoxFuture<'a, anyhow::Result<()>>,
{
    let expected = match std::fs::read(&test.result) {
        Ok(bytes) => bytes,
        Err(error) => {
            return CaseStatus::Fail(format!(
                "failed to read expected result {}: {error}",
                test.result.display()
            ));
        }
    };

    let actual = match run_query_case(test, runner).await {
        Ok(bytes) => bytes,
        Err(error) => return CaseStatus::Fail(format!("query execution error: {error}")),
    };
    let (reduced_query, ordered_query) = compare::query_result_modifiers(&test.query);

    match (&test.compare_kind, &test.de_output) {
        (CompareKind::Query(format), _) if *format == QueryResultsFormat::Csv => {
            let (expected_headers, expected_rows) = match compare::parse_csv_table(&expected) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return CaseStatus::Fail(format!("expected CSV parse error: {error}"));
                }
            };
            let (actual_headers, actual_rows) = match compare::parse_csv_table(&actual) {
                Ok(parsed) => parsed,
                Err(error) => return CaseStatus::Fail(format!("actual CSV parse error: {error}")),
            };
            let expected_results = ParsedQueryResults::Solutions {
                variables: expected_headers,
                rows: expected_rows,
            };
            let actual_results = ParsedQueryResults::Solutions {
                variables: actual_headers,
                rows: actual_rows,
            };
            if compare::query_results_equivalent(
                &expected_results,
                &actual_results,
                reduced_query,
                ordered_query,
            ) {
                CaseStatus::Pass
            } else {
                CaseStatus::Fail(format!(
                    "query result mismatch: expected={} actual={}",
                    compare::summarize_query_results(&expected_results),
                    compare::summarize_query_results(&actual_results)
                ))
            }
        }
        (CompareKind::Query(format), _) => {
            let expected_results = match compare::parse_query_results(&expected, *format) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return CaseStatus::Fail(format!("expected result parse error: {error}"));
                }
            };
            let actual_results = match compare::parse_query_results(&actual, *format) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return CaseStatus::Fail(format!("actual result parse error: {error}"));
                }
            };
            if compare::query_results_equivalent(
                &expected_results,
                &actual_results,
                reduced_query,
                ordered_query,
            ) {
                CaseStatus::Pass
            } else {
                CaseStatus::Fail(format!(
                    "query result mismatch: expected={} actual={}",
                    compare::summarize_query_results(&expected_results),
                    compare::summarize_query_results(&actual_results)
                ))
            }
        }
        (CompareKind::QueryRdf(format), _) => {
            let expected_results = match compare::parse_query_results_rdf(
                &expected,
                *format,
                Some(&manifest::manifest_file_uri(&test.result)),
            ) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return CaseStatus::Fail(format!("expected RDF-results parse error: {error}"));
                }
            };
            let actual_results =
                match compare::parse_query_results(&actual, QueryResultsFormat::Json) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return CaseStatus::Fail(format!("actual result parse error: {error}"));
                    }
                };
            if compare::query_results_equivalent(
                &expected_results,
                &actual_results,
                reduced_query,
                ordered_query,
            ) {
                CaseStatus::Pass
            } else {
                CaseStatus::Fail(format!(
                    "query result mismatch: expected={} actual={}",
                    compare::summarize_query_results(&expected_results),
                    compare::summarize_query_results(&actual_results)
                ))
            }
        }
        (CompareKind::Rdf(format), _) => {
            let expected_quads = match parse_rdf_dataset_bytes(&expected, *format) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return CaseStatus::Fail(format!("expected RDF parse error: {error}"));
                }
            };
            let actual_quads = match parse_rdf_dataset_bytes(&actual, *format) {
                Ok(parsed) => parsed,
                Err(error) => return CaseStatus::Fail(format!("actual RDF parse error: {error}")),
            };
            if expected_quads == actual_quads {
                CaseStatus::Pass
            } else {
                CaseStatus::Fail("RDF result mismatch".to_string())
            }
        }
    }
}

/// Runs a positive or negative SPARQL query syntax test.
fn run_sparql_query_syntax_case(path: &Path, positive: bool) -> CaseStatus {
    let mut text = String::new();
    if let Err(error) = File::open(path).and_then(|mut f| f.read_to_string(&mut text)) {
        return CaseStatus::Fail(format!(
            "failed to read syntax file {}: {error}",
            path.display()
        ));
    }

    let parsed_ok = match SparqlParser::new().with_base_iri("http://example.com/") {
        Ok(parser) => parser.parse_query(&text).is_ok(),
        Err(_) => false,
    };

    match (positive, parsed_ok) {
        (true, true) | (false, false) => CaseStatus::Pass,
        (true, false) => CaseStatus::Fail("expected query syntax success".to_string()),
        (false, true) => CaseStatus::Fail("expected query syntax failure".to_string()),
    }
}

/// Runs a positive or negative SPARQL update syntax test.
fn run_sparql_update_syntax_case(path: &Path, positive: bool) -> CaseStatus {
    let mut text = String::new();
    if let Err(error) = File::open(path).and_then(|mut f| f.read_to_string(&mut text)) {
        return CaseStatus::Fail(format!(
            "failed to read update syntax file {}: {error}",
            path.display()
        ));
    }

    let parsed_ok = match SparqlParser::new().with_base_iri("http://example.com/") {
        Ok(parser) => parser.parse_update(&text).is_ok(),
        Err(_) => false,
    };

    match (positive, parsed_ok) {
        (true, true) | (false, false) => CaseStatus::Pass,
        (true, false) => CaseStatus::Fail("expected update syntax success".to_string()),
        (false, true) => CaseStatus::Fail("expected update syntax failure".to_string()),
    }
}

/// Runs an RDF syntax test by checking parse success/failure for the given format.
fn run_rdf_syntax_case(action: &Path, format: RdfFormat, positive: bool) -> CaseStatus {
    let parsed = parse_rdf_dataset_file(action, format);
    match (positive, parsed.is_ok()) {
        (true, true) | (false, false) => CaseStatus::Pass,
        (true, false) => CaseStatus::Fail("expected RDF syntax success".to_string()),
        (false, true) => CaseStatus::Fail("expected RDF syntax failure".to_string()),
    }
}

/// Runs an RDF evaluation test by canonicalizing and comparing parsed datasets.
fn run_rdf_eval_case(
    action: &Path,
    expected: &Path,
    action_format: RdfFormat,
    expected_format: RdfFormat,
) -> CaseStatus {
    let actual = match parse_rdf_dataset_file(action, action_format) {
        Ok(v) => v,
        Err(error) => return CaseStatus::Fail(format!("action parse failed: {error}")),
    };
    let expected = match parse_rdf_dataset_file(expected, expected_format) {
        Ok(v) => v,
        Err(error) => return CaseStatus::Fail(format!("expected parse failed: {error}")),
    };

    if actual == expected {
        CaseStatus::Pass
    } else {
        CaseStatus::Fail("RDF evaluation mismatch".to_string())
    }
}

/// Runs an entailment case using the `reasonable` reasoner closure output.
///
/// When `expected` is `None`, the case models inconsistency expectations via
/// `mf:result=false`.
fn run_entailment_case(
    action: &Path,
    expected: Option<&Path>,
    positive: bool,
    regime: Option<&str>,
    recognized_datatypes: &[String],
) -> CaseStatus {
    let action_format = match manifest::rdf_format_from_path(action) {
        Ok(f) => f,
        Err(error) => return CaseStatus::Fail(format!("action format error: {error}")),
    };
    let action_dataset = match parse_rdf_dataset_file(action, action_format) {
        Ok(v) => v,
        Err(error) => return CaseStatus::Fail(format!("action parse failed: {error}")),
    };

    let mut reasoner = Reasoner::new();
    reasoner.load_triples(dataset_to_triples(&action_dataset));
    reasoner.reason();

    if expected.is_none() {
        let datatypes_recognized = !recognized_datatypes.is_empty();
        let inconsistent = reasoner.errors().iter().any(|error| {
            if !datatypes_recognized && error.rule().starts_with("rdfs-datatype") {
                return false;
            }
            true
        });
        return match (positive, inconsistent) {
            (true, true) | (false, false) => CaseStatus::Pass,
            (true, false) => CaseStatus::Fail(format!(
                "expected inconsistency entailment (result=false){}",
                regime_suffix(regime)
            )),
            (false, true) => CaseStatus::Fail(format!(
                "unexpected inconsistency entailment (result=false){}",
                regime_suffix(regime)
            )),
        };
    }

    let closure = reasoner.view_output().to_vec();
    let expected_path = expected.expect("checked above");
    let expected_format = match manifest::rdf_format_from_path(expected_path) {
        Ok(f) => f,
        Err(error) => return CaseStatus::Fail(format!("expected format error: {error}")),
    };
    let expected_dataset = match parse_rdf_dataset_file(expected_path, expected_format) {
        Ok(v) => v,
        Err(error) => return CaseStatus::Fail(format!("expected parse failed: {error}")),
    };
    let expected_triples = dataset_to_triples(&expected_dataset);

    let entailed = triples_are_entailed(&expected_triples, &closure);
    match (positive, entailed) {
        (true, true) | (false, false) => CaseStatus::Pass,
        (true, false) => CaseStatus::Fail(format!(
            "expected entailment did not hold{}",
            regime_suffix(regime)
        )),
        (false, true) => CaseStatus::Fail(format!(
            "unexpected entailment held{}",
            regime_suffix(regime)
        )),
    }
}

/// Formats the optional entailment regime for failure diagnostics.
fn regime_suffix(regime: Option<&str>) -> String {
    regime.map(|r| format!(" [regime={r}]")).unwrap_or_default()
}

/// Drops graph-name information and converts a dataset into triples for reasoner input.
fn dataset_to_triples(dataset: &Dataset) -> Vec<oxrdf::Triple> {
    dataset
        .iter()
        .map(|q| oxrdf::Triple::new(q.subject, q.predicate, q.object))
        .collect()
}

/// Checks whether all `expected` triples are entailed by `actual`.
///
/// This comparison allows blank-node isomorphism and semantic equality for
/// selected literal classes used by entailment tests.
#[allow(clippy::too_many_lines)]
fn triples_are_entailed(expected: &[oxrdf::Triple], actual: &[oxrdf::Triple]) -> bool {
    fn semantically_equal_literals(a: &oxrdf::Literal, b: &oxrdf::Literal) -> bool {
        let xsd_string = "http://www.w3.org/2001/XMLSchema#string";
        let xsd_boolean = "http://www.w3.org/2001/XMLSchema#boolean";
        let xsd_integer = "http://www.w3.org/2001/XMLSchema#integer";
        let xsd_decimal = "http://www.w3.org/2001/XMLSchema#decimal";
        let xsd_double = "http://www.w3.org/2001/XMLSchema#double";
        let xsd_float = "http://www.w3.org/2001/XMLSchema#float";

        if a.language().is_some() || b.language().is_some() {
            return a == b;
        }
        let adt = a.datatype().as_str();
        let bdt = b.datatype().as_str();

        if adt == xsd_string && bdt == xsd_string {
            return a.value() == b.value();
        }

        if adt == xsd_boolean && bdt == xsd_boolean {
            let av = match a.value() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            };
            let bv = match b.value() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            };
            return av.is_some() && av == bv;
        }

        let is_numeric_dt = |dt: &str| {
            dt == xsd_integer || dt == xsd_decimal || dt == xsd_double || dt == xsd_float
        };
        let a_num = if is_numeric_dt(adt) {
            a.value().parse::<f64>().ok()
        } else {
            None
        };
        let b_num = if is_numeric_dt(bdt) {
            b.value().parse::<f64>().ok()
        } else {
            None
        };
        if let (Some(an), Some(bn)) = (a_num, b_num) {
            return (an - bn).abs() < f64::EPSILON;
        }

        a == b
    }

    fn terms_equal(a: &Term, b: &Term) -> bool {
        match (a, b) {
            (Term::NamedNode(an), Term::NamedNode(bn)) => an == bn,
            (Term::BlankNode(an), Term::BlankNode(bn)) => an == bn,
            (Term::Literal(al), Term::Literal(bl)) => semantically_equal_literals(al, bl),
            _ => false,
        }
    }

    fn subject_key(subject: &oxrdf::NamedOrBlankNode) -> String {
        subject.to_string()
    }
    fn subject_as_term(subject: &oxrdf::NamedOrBlankNode) -> Term {
        match subject {
            oxrdf::NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n.clone()),
            oxrdf::NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b.clone()),
        }
    }

    fn candidates_for<'a>(
        e: &oxrdf::Triple,
        actual: &'a [oxrdf::Triple],
    ) -> Vec<&'a oxrdf::Triple> {
        actual
            .iter()
            .filter(|a| {
                if e.predicate != a.predicate {
                    return false;
                }
                let subject_ok = match &e.subject {
                    oxrdf::NamedOrBlankNode::NamedNode(s) => {
                        matches!(&a.subject, oxrdf::NamedOrBlankNode::NamedNode(asub) if asub == s)
                    }
                    oxrdf::NamedOrBlankNode::BlankNode(_) => true,
                };
                let object_ok = match &e.object {
                    Term::NamedNode(o) => terms_equal(&Term::NamedNode(o.clone()), &a.object),
                    Term::BlankNode(_) => true,
                    Term::Literal(o) => terms_equal(&Term::Literal(o.clone()), &a.object),
                };
                subject_ok && object_ok
            })
            .collect()
    }

    fn compatible_and_extend(
        e: &oxrdf::Triple,
        a: &oxrdf::Triple,
        mapping: &HashMap<String, Term>,
    ) -> Option<HashMap<String, Term>> {
        let mut next = mapping.clone();

        if let oxrdf::NamedOrBlankNode::BlankNode(b) = &e.subject {
            let key = b.to_string();
            let value = subject_as_term(&a.subject);
            if let Some(existing) = next.get(&key) {
                if !terms_equal(existing, &value) {
                    return None;
                }
            } else {
                next.insert(key, value);
            }
        } else if subject_key(&e.subject) != subject_key(&a.subject) {
            return None;
        }

        if let Term::BlankNode(b) = &e.object {
            let key = b.to_string();
            let value = a.object.clone();
            if let Some(existing) = next.get(&key) {
                if !terms_equal(existing, &value) {
                    return None;
                }
            } else {
                next.insert(key, value);
            }
        } else if !terms_equal(&e.object, &a.object) {
            return None;
        }

        Some(next)
    }

    fn dfs(
        index: usize,
        ordered: &[oxrdf::Triple],
        actual: &[oxrdf::Triple],
        mapping: &HashMap<String, Term>,
    ) -> bool {
        if index == ordered.len() {
            return true;
        }
        let e = &ordered[index];
        for candidate in candidates_for(e, actual) {
            if let Some(next) = compatible_and_extend(e, candidate, mapping)
                && dfs(index + 1, ordered, actual, &next)
            {
                return true;
            }
        }
        false
    }

    let mut ordered = expected.to_vec();
    ordered.sort_by_key(|e| {
        let mut n = 0usize;
        if matches!(e.subject, oxrdf::NamedOrBlankNode::BlankNode(_)) {
            n += 1;
        }
        if matches!(e.object, Term::BlankNode(_)) {
            n += 1;
        }
        n
    });

    dfs(0, &ordered, actual, &HashMap::new())
}

/// Parses an RDF file into a canonicalized dataset.
fn parse_rdf_dataset_file(path: &Path, format: RdfFormat) -> anyhow::Result<Dataset> {
    let file = File::open(path)?;
    let base_iri = w3c_test_file_base_iri(path);
    let parser = RdfParser::from_format(format)
        .with_base_iri(&base_iri)?
        .for_reader(BufReader::new(file));
    let mut dataset = Dataset::new();
    for quad in parser {
        dataset.insert(quad?.as_ref());
    }
    dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    Ok(dataset)
}

/// Returns the preferred base IRI for files under the vendored `rdf-tests` tree.
///
/// Using the upstream GitHub URL keeps relative IRI resolution aligned with
/// upstream test intent.
fn w3c_test_file_base_iri(path: &Path) -> String {
    let root = super::w3c_resources_root();
    if let Ok(relative) = path.strip_prefix(&root) {
        return format!(
            "https://w3c.github.io/rdf-tests/{}",
            relative.to_string_lossy().replace('\\', "/")
        );
    }
    manifest::manifest_file_uri(path)
}

/// Parses RDF bytes into a canonicalized dataset.
fn parse_rdf_dataset_bytes(bytes: &[u8], format: RdfFormat) -> anyhow::Result<Dataset> {
    let parser = RdfParser::from_format(format).for_reader(std::io::Cursor::new(bytes));
    let mut dataset = Dataset::new();
    for quad in parser {
        dataset.insert(quad?.as_ref());
    }
    dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    Ok(dataset)
}

/// Materializes a query case's inputs and dispatches to the consumer-supplied
/// runner. The runner is responsible for actually executing the SPARQL query
/// against its engine and writing the response bytes into `args.writer`.
async fn run_query_case<F>(test: &W3cQueryCase, runner: &F) -> anyhow::Result<Vec<u8>>
where
    F: for<'a> Fn(W3cRunInputs<'a>) -> BoxFuture<'a, anyhow::Result<()>>,
{
    let query_files = vec![report::path_for_cli(&test.query)];
    let data_files = test
        .default_data
        .iter()
        .map(|data| report::path_for_cli(data))
        .collect::<Vec<_>>();
    let named_graph_bindings = test
        .named_graph_data
        .iter()
        .map(|(iri, path)| NamedGraphBinding {
            graph_iri: iri.clone(),
            data_file: report::path_for_cli(path),
        })
        .collect::<Vec<_>>();
    let entailment_mode = if test.entailment {
        EntailmentMode::OwlRl
    } else {
        EntailmentMode::Off
    };
    let mut output = Vec::new();
    runner(W3cRunInputs {
        data_files: &data_files,
        named_graph_bindings: &named_graph_bindings,
        query_files: &query_files,
        entailment: entailment_mode,
        out: &test.de_output,
        writer: &mut output,
    })
    .await?;
    Ok(output)
}
