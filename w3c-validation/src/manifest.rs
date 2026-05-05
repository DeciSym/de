// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Manifest discovery and case construction for the W3C RDF/SPARQL harness.

use super::*;
use de::query::DeOutput;
use oxrdf::{NamedNode, Term, vocab::rdf};
use oxrdfio::{RdfFormat, RdfParser};
use sparesults::QueryResultsFormat;
use spargebra::{Query as SparqlQuery, SparqlParser};
use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

pub(super) fn discover_manifest_cases() -> anyhow::Result<Vec<W3cManifestCase>> {
    let root = super::w3c_resources_root();
    let mut visited = HashSet::new();
    let mut cases = Vec::new();
    let mut seen = HashSet::<String>::new();

    for rel in W3C_MANIFEST_PATHS {
        let manifest_path = root.join(rel);
        collect_cases_from_manifest(&manifest_path, &mut visited, &mut cases, &mut seen)?;
    }

    Ok(cases)
}

/// Recursively loads one manifest and appends normalized cases into `cases`.
fn collect_cases_from_manifest(
    manifest_path: &Path,
    visited: &mut HashSet<PathBuf>,
    cases: &mut Vec<W3cManifestCase>,
    seen: &mut HashSet<String>,
) -> anyhow::Result<()> {
    let manifest_path = manifest_path.canonicalize()?;
    if !visited.insert(manifest_path.clone()) {
        return Ok(());
    }
    let manifest_base = manifest_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_path_buf();

    let graph = match load_manifest_graph(&manifest_path) {
        Ok(graph) => graph,
        Err(error) => {
            cases.push(W3cManifestCase {
                id: "manifest-parse-error".to_string(),
                test_type: "manifest-parse-error".to_string(),
                manifest: manifest_path,
                kind: ManifestCaseKind::InvalidCase {
                    reason: format!("manifest parse error: {error}"),
                },
            });
            return Ok(());
        }
    };
    let manifest_entries = manifest_entry_subjects(&graph);
    let enforce_manifest_entries = !manifest_entries.is_empty();

    for triple in graph.iter() {
        let subject = Term::from(triple.subject);
        if triple.predicate.as_str() == MF_INCLUDE {
            add_included_manifests(
                &manifest_path,
                &manifest_base,
                &subject,
                &graph,
                visited,
                cases,
                seen,
            )?;
        }

        if triple.predicate.as_str() == rdf::TYPE.as_str() {
            let type_term: Term = triple.object.into();
            let Term::NamedNode(type_node) = type_term else {
                continue;
            };
            let test_type = type_node.as_str().to_string();
            if test_type == MF_MANIFEST {
                continue;
            }
            if enforce_manifest_entries && !manifest_entries.contains(&subject.to_string()) {
                continue;
            }

            let key = format!("{}::{}::{}", manifest_path.display(), subject, test_type);
            if !seen.insert(key) {
                continue;
            }

            let id = case_id(&graph, &subject);
            let kind =
                build_case_kind(&manifest_path, &manifest_base, &graph, &subject, &test_type);
            cases.push(W3cManifestCase {
                id,
                test_type,
                manifest: manifest_path.clone(),
                kind,
            });
        }
    }

    Ok(())
}

/// Extracts the set of subjects explicitly listed in `mf:entries`.
///
/// If non-empty, only listed subjects are considered executable tests.
fn manifest_entry_subjects(graph: &oxrdf::Graph) -> HashSet<String> {
    let mut entries = HashSet::new();
    for triple in graph.iter() {
        if triple.predicate.as_str() != rdf::TYPE.as_str() {
            continue;
        }
        let subject = Term::from(triple.subject);
        let object: Term = triple.object.into();
        let Term::NamedNode(type_node) = object else {
            continue;
        };
        if type_node.as_str() != MF_MANIFEST {
            continue;
        }
        for entries_term in objects_of(graph, &subject, MF_ENTRIES) {
            let manifest_entries = if matches!(entries_term, Term::BlankNode(_)) {
                collect_rdf_list(graph, entries_term)
            } else {
                vec![entries_term]
            };
            for entry in manifest_entries {
                entries.insert(entry.to_string());
            }
        }
    }
    entries
}

/// Maps a manifest test type IRI plus case subject into an executable case payload.
fn build_case_kind(
    manifest_path: &Path,
    manifest_base: &Path,
    manifest_graph: &oxrdf::Graph,
    subject: &Term,
    test_type: &str,
) -> ManifestCaseKind {
    match test_type {
        MF_QUERY_EVALUATION_TEST | MF_CSV_RESULT_FORMAT_TEST => {
            match parse_query_test(manifest_path, manifest_base, manifest_graph, subject) {
                Ok(c) => ManifestCaseKind::QueryEvaluation(c),
                Err(e) => ManifestCaseKind::InvalidCase {
                    reason: format!("QueryEvaluationTest invalid: {e}"),
                },
            }
        }
        MF_POSITIVE_SYNTAX_TEST | MF_POSITIVE_SYNTAX_TEST11 => {
            match parse_action_path(
                manifest_path,
                manifest_base,
                manifest_graph,
                subject,
                &[QT_QUERY],
            ) {
                Ok(path) => ManifestCaseKind::SparqlQuerySyntax {
                    path,
                    positive: true,
                },
                Err(e) => ManifestCaseKind::InvalidCase {
                    reason: format!("Positive SPARQL syntax test invalid: {e}"),
                },
            }
        }
        MF_NEGATIVE_SYNTAX_TEST | MF_NEGATIVE_SYNTAX_TEST11 => {
            match parse_action_path(
                manifest_path,
                manifest_base,
                manifest_graph,
                subject,
                &[QT_QUERY],
            ) {
                Ok(path) => ManifestCaseKind::SparqlQuerySyntax {
                    path,
                    positive: false,
                },
                Err(e) => ManifestCaseKind::InvalidCase {
                    reason: format!("Negative SPARQL syntax test invalid: {e}"),
                },
            }
        }
        MF_POSITIVE_UPDATE_SYNTAX_TEST | MF_POSITIVE_UPDATE_SYNTAX_TEST11 => {
            match parse_action_path(
                manifest_path,
                manifest_base,
                manifest_graph,
                subject,
                &[UT_REQUEST],
            ) {
                Ok(path) => ManifestCaseKind::SparqlUpdateSyntax {
                    path,
                    positive: true,
                },
                Err(e) => ManifestCaseKind::InvalidCase {
                    reason: format!("Positive SPARQL update syntax test invalid: {e}"),
                },
            }
        }
        MF_NEGATIVE_UPDATE_SYNTAX_TEST | MF_NEGATIVE_UPDATE_SYNTAX_TEST11 => {
            match parse_action_path(
                manifest_path,
                manifest_base,
                manifest_graph,
                subject,
                &[UT_REQUEST],
            ) {
                Ok(path) => ManifestCaseKind::SparqlUpdateSyntax {
                    path,
                    positive: false,
                },
                Err(e) => ManifestCaseKind::InvalidCase {
                    reason: format!("Negative SPARQL update syntax test invalid: {e}"),
                },
            }
        }
        MF_UPDATE_EVALUATION_TEST => ManifestCaseKind::UnsupportedFeature {
            reason:
                "SPARQL Update evaluation is not implemented in de CLI (query/create/view only)"
                    .to_string(),
        },
        MF_POSITIVE_ENTAILMENT_TEST => {
            entailment_case(manifest_path, manifest_base, manifest_graph, subject, true)
        }
        MF_NEGATIVE_ENTAILMENT_TEST => {
            entailment_case(manifest_path, manifest_base, manifest_graph, subject, false)
        }
        RDFT_TEST_NTRIPLES_POSITIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::NTriples,
            true,
        ),
        RDFT_TEST_NTRIPLES_NEGATIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::NTriples,
            false,
        ),
        RDFT_TEST_NQUADS_POSITIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::NQuads,
            true,
        ),
        RDFT_TEST_NQUADS_NEGATIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::NQuads,
            false,
        ),
        RDFT_TEST_TURTLE_POSITIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::Turtle,
            true,
        ),
        RDFT_TEST_TURTLE_NEGATIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::Turtle,
            false,
        ),
        RDFT_TEST_TRIG_POSITIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::TriG,
            true,
        ),
        RDFT_TEST_TRIG_NEGATIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::TriG,
            false,
        ),
        RDFT_TEST_XML_NEGATIVE_SYNTAX => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::RdfXml,
            false,
        ),
        RDFT_TEST_TURTLE_EVAL => rdf_eval_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::Turtle,
        ),
        RDFT_TEST_TRIG_EVAL => rdf_eval_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::TriG,
        ),
        RDFT_TEST_XML_EVAL => rdf_eval_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::RdfXml,
        ),
        RDFT_TEST_TURTLE_NEGATIVE_EVAL => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::Turtle,
            false,
        ),
        RDFT_TEST_TRIG_NEGATIVE_EVAL => rdf_syntax_case(
            manifest_path,
            manifest_base,
            manifest_graph,
            subject,
            RdfFormat::TriG,
            false,
        ),
        _ => ManifestCaseKind::InvalidCase {
            reason: format!("unknown test type: {test_type}"),
        },
    }
}

/// Builds an entailment case payload from manifest terms.
fn entailment_case(
    manifest_path: &Path,
    manifest_base: &Path,
    graph: &oxrdf::Graph,
    subject: &Term,
    positive: bool,
) -> ManifestCaseKind {
    let action = parse_action_path(manifest_path, manifest_base, graph, subject, &[]);
    let result_term = single_object(graph, subject, MF_RESULT);
    let regime = single_object(graph, subject, MF_ENTAILMENT_REGIME).map(|t| t.to_string());
    let recognized_datatypes = parse_manifest_iri_list(graph, subject, MF_RECOGNIZED_DATATYPES);

    let expected = match result_term {
        Some(Term::Literal(lit))
            if lit.value().eq_ignore_ascii_case("false")
                || lit.value() == "0"
                || lit.value().eq_ignore_ascii_case("no") =>
        {
            Ok(None)
        }
        Some(term) => term_to_path(manifest_path, manifest_base, &term)
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!("entailment mf:result is neither false nor a resolvable path")
            }),
        None => Err(anyhow::anyhow!("entailment test missing mf:result")),
    };

    match (action, expected) {
        (Ok(action), Ok(expected)) => ManifestCaseKind::Entailment {
            action,
            expected,
            positive,
            regime,
            recognized_datatypes,
        },
        (Err(e), _) => ManifestCaseKind::InvalidCase {
            reason: format!("entailment action invalid: {e}"),
        },
        (_, Err(e)) => ManifestCaseKind::InvalidCase {
            reason: format!("entailment result invalid: {e}"),
        },
    }
}

/// Parses a manifest predicate whose object can be either an IRI or RDF list of IRIs.
fn parse_manifest_iri_list(
    graph: &oxrdf::Graph,
    subject: &Term,
    predicate_iri: &str,
) -> Vec<String> {
    objects_of(graph, subject, predicate_iri)
        .into_iter()
        .flat_map(|term| {
            if matches!(term, Term::BlankNode(_)) {
                collect_rdf_list(graph, term)
            } else if matches!(&term, Term::NamedNode(node) if node.as_str() == rdf::NIL.as_str()) {
                Vec::new()
            } else {
                vec![term]
            }
        })
        .filter_map(|term| {
            if let Term::NamedNode(node) = term {
                Some(node.as_str().to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Builds an RDF syntax case payload.
fn rdf_syntax_case(
    manifest_path: &Path,
    manifest_base: &Path,
    graph: &oxrdf::Graph,
    subject: &Term,
    format: RdfFormat,
    positive: bool,
) -> ManifestCaseKind {
    match parse_action_path(manifest_path, manifest_base, graph, subject, &[]) {
        Ok(action) => ManifestCaseKind::RdfSyntax {
            action,
            format,
            positive,
        },
        Err(e) => ManifestCaseKind::InvalidCase {
            reason: format!("RDF syntax case invalid: {e}"),
        },
    }
}

/// Builds an RDF evaluation case payload.
fn rdf_eval_case(
    manifest_path: &Path,
    manifest_base: &Path,
    graph: &oxrdf::Graph,
    subject: &Term,
    action_format: RdfFormat,
) -> ManifestCaseKind {
    let action = parse_action_path(manifest_path, manifest_base, graph, subject, &[]);
    let expected = single_object(graph, subject, MF_RESULT)
        .and_then(|t| term_to_path(manifest_path, manifest_base, &t));

    match (action, expected) {
        (Ok(action), Some(expected)) => {
            let expected_format = match rdf_format_from_path(&expected) {
                Ok(f) => f,
                Err(e) => {
                    return ManifestCaseKind::InvalidCase {
                        reason: format!("RDF eval expected format error: {e}"),
                    };
                }
            };
            ManifestCaseKind::RdfEval {
                action,
                expected,
                action_format,
                expected_format,
            }
        }
        (Err(e), _) => ManifestCaseKind::InvalidCase {
            reason: format!("RDF eval action invalid: {e}"),
        },
        (_, None) => ManifestCaseKind::InvalidCase {
            reason: "RDF eval missing mf:result".to_string(),
        },
    }
}

/// Resolves `mf:action` to an existing local file path.
///
/// Some manifests encode the action as a blank node with nested predicates
/// (`qt:query` / `ut:request`), handled by `nested_predicates`.
fn parse_action_path(
    manifest_path: &Path,
    manifest_base: &Path,
    graph: &oxrdf::Graph,
    subject: &Term,
    nested_predicates: &[&str],
) -> anyhow::Result<PathBuf> {
    let action_term = single_object(graph, subject, MF_ACTION)
        .ok_or_else(|| anyhow::anyhow!("missing mf:action"))?;

    if let Some(path) = term_to_path(manifest_path, manifest_base, &action_term)
        && path.exists()
    {
        return Ok(path);
    }

    if let Term::BlankNode(_) = action_term {
        for pred in nested_predicates {
            for term in objects_of(graph, &action_term, pred) {
                if let Some(path) = term_to_path(manifest_path, manifest_base, &term)
                    && path.exists()
                {
                    return Ok(path);
                }
            }
        }
    }

    Err(anyhow::anyhow!("unable to resolve mf:action file path"))
}

/// Returns case display name from `mf:name`, falling back to the subject term.
fn case_id(graph: &oxrdf::Graph, subject: &Term) -> String {
    objects_of(graph, subject, MF_NAME)
        .first()
        .and_then(|term| {
            if let Term::Literal(lit) = term {
                Some(lit.value().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| subject.to_string())
}

/// Returns `true` when a comparison strategy expects query-results artifacts.
fn compare_kind_is_query(compare_kind: &CompareKind) -> bool {
    matches!(compare_kind, CompareKind::Query(_))
}

/// Returns `true` when a comparison strategy expects RDF artifacts.
fn compare_kind_is_rdf(compare_kind: &CompareKind) -> bool {
    matches!(compare_kind, CompareKind::Rdf(_))
}

/// Parses a query file and classifies its top-level query form.
fn detect_query_form(path: &Path) -> anyhow::Result<QueryForm> {
    let mut text = String::new();
    File::open(path)?.read_to_string(&mut text)?;
    let query = SparqlParser::new()
        .with_base_iri("http://example.com/")?
        .parse_query(&text)?;
    let form = match query {
        SparqlQuery::Select { .. } => QueryForm::Solutions,
        SparqlQuery::Ask { .. } => QueryForm::Boolean,
        SparqlQuery::Construct { .. } | SparqlQuery::Describe { .. } => QueryForm::Graph,
    };
    Ok(form)
}

/// Returns `(uses_reduced, uses_order_by)` for a query.
///
/// The parser path is authoritative; a simple textual fallback is used only
/// when parsing fails so diagnostics can still proceed.
fn parse_query_test(
    manifest_path: &Path,
    manifest_base: &Path,
    manifest_graph: &oxrdf::Graph,
    subject: &Term,
) -> anyhow::Result<W3cQueryCase> {
    let action_term = single_object(manifest_graph, subject, MF_ACTION)
        .ok_or_else(|| anyhow::anyhow!("missing mf:action"))?;

    let query_term = single_object(manifest_graph, &action_term, QT_QUERY)
        .or_else(|| {
            if let Term::NamedNode(_) = action_term {
                Some(action_term.clone())
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow::anyhow!("missing qt:query on mf:action"))?;

    let query_path = term_to_path(manifest_path, manifest_base, &query_term)
        .ok_or_else(|| anyhow::anyhow!("cannot resolve qt:query path"))?;

    let result_term = single_object(manifest_graph, subject, MF_RESULT)
        .ok_or_else(|| anyhow::anyhow!("missing mf:result"))?;
    let result_path = term_to_path(manifest_path, manifest_base, &result_term)
        .ok_or_else(|| anyhow::anyhow!("cannot resolve mf:result path"))?;

    let (mut de_output, mut compare_kind) = map_expected_result(&result_path)?;
    let query_form = detect_query_form(&query_path)?;
    match query_form {
        QueryForm::Solutions | QueryForm::Boolean if compare_kind_is_rdf(&compare_kind) => {
            let CompareKind::Rdf(format) = compare_kind else {
                unreachable!();
            };
            compare_kind = CompareKind::QueryRdf(format);
            de_output = DeOutput::JSON;
        }
        QueryForm::Graph if compare_kind_is_query(&compare_kind) => {
            return Err(anyhow::anyhow!(
                "graph query expects RDF result format, got {}",
                result_path.display()
            ));
        }
        _ => {}
    }

    let default_data_paths =
        parse_default_data_paths(manifest_path, manifest_base, manifest_graph, &action_term)?;
    let named_graph_data =
        parse_named_graph_data(manifest_path, manifest_base, manifest_graph, &action_term)?;

    if !query_path.exists() {
        return Err(anyhow::anyhow!(
            "query file missing: {}",
            query_path.display()
        ));
    }
    if !result_path.exists() {
        return Err(anyhow::anyhow!(
            "result file missing: {}",
            result_path.display()
        ));
    }

    let entailment = !objects_of(manifest_graph, &action_term, SD_ENTAILMENT_REGIME).is_empty();

    Ok(W3cQueryCase {
        query: query_path,
        default_data: default_data_paths,
        named_graph_data,
        result: result_path,
        entailment,
        de_output,
        compare_kind,
    })
}

/// Parses `qt:graphData` into `(graph_iri, data_path)` bindings.
///
/// Invalid graph-data forms are treated as hard errors to avoid silently
/// weakening test intent.
fn parse_named_graph_data(
    manifest_path: &Path,
    manifest_base: &Path,
    manifest_graph: &oxrdf::Graph,
    action_term: &Term,
) -> anyhow::Result<Vec<(String, PathBuf)>> {
    fn ensure_existing_path(path: PathBuf, kind: &str) -> anyhow::Result<PathBuf> {
        if path.exists() {
            Ok(path)
        } else {
            Err(anyhow::anyhow!("{kind} file missing: {}", path.display()))
        }
    }

    fn resolved_binding(
        manifest_path: &Path,
        manifest_base: &Path,
        graph_term: &Term,
        data_term: &Term,
    ) -> anyhow::Result<(String, PathBuf)> {
        let path = term_to_path(manifest_path, manifest_base, data_term)
            .ok_or_else(|| anyhow::anyhow!("cannot resolve qt:data path from term {data_term}"))?;
        let path = ensure_existing_path(path, "qt:graphData")?;
        let iri = graph_iri_for_term(manifest_base, graph_term)
            .unwrap_or_else(|| file_uri_for_path(&path));
        Ok((iri, path))
    }

    let mut output = Vec::new();
    let mut seen = HashSet::new();

    for graph_data in objects_of(manifest_graph, action_term, QT_GRAPH_DATA) {
        let (iri, path) = if term_to_path(manifest_path, manifest_base, &graph_data).is_some() {
            resolved_binding(manifest_path, manifest_base, &graph_data, &graph_data)?
        } else if let Term::BlankNode(_) = graph_data {
            let graph_term =
                single_object(manifest_graph, &graph_data, QT_GRAPH).ok_or_else(|| {
                    anyhow::anyhow!("qt:graphData blank node missing qt:graph: {graph_data}")
                })?;
            let data_term =
                single_object(manifest_graph, &graph_data, QT_DATA).ok_or_else(|| {
                    anyhow::anyhow!("qt:graphData blank node missing qt:data: {graph_data}")
                })?;
            resolved_binding(manifest_path, manifest_base, &graph_term, &data_term)?
        } else {
            return Err(anyhow::anyhow!(
                "cannot resolve qt:graphData term to local file path: {graph_data}"
            ));
        };

        if seen.insert((iri.clone(), path.clone())) {
            output.push((iri, path));
        }
    }

    Ok(output)
}

/// Parses and validates `qt:data` entries for the default graph.
fn parse_default_data_paths(
    manifest_path: &Path,
    manifest_base: &Path,
    manifest_graph: &oxrdf::Graph,
    action_term: &Term,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for data_term in objects_of(manifest_graph, action_term, QT_DATA) {
        let path = term_to_path(manifest_path, manifest_base, &data_term)
            .ok_or_else(|| anyhow::anyhow!("cannot resolve qt:data path from term {data_term}"))?;
        if !path.exists() {
            return Err(anyhow::anyhow!("qt:data file missing: {}", path.display()));
        }
        out.push(path);
    }
    Ok(out)
}

/// Resolves `mf:include` references (including RDF lists) and loads child manifests.
fn add_included_manifests(
    manifest_path: &Path,
    manifest_base: &Path,
    subject: &Term,
    graph: &oxrdf::Graph,
    visited: &mut HashSet<PathBuf>,
    cases: &mut Vec<W3cManifestCase>,
    seen: &mut HashSet<String>,
) -> anyhow::Result<()> {
    let included = objects_of(graph, subject, MF_INCLUDE)
        .into_iter()
        .flat_map(|term| {
            if let Term::BlankNode(_) = term {
                collect_rdf_list(graph, term)
            } else {
                vec![term]
            }
        })
        .collect::<Vec<_>>();

    for include in included {
        if let Some(include_path) = term_to_path(manifest_path, manifest_base, &include) {
            collect_cases_from_manifest(&include_path, visited, cases, seen)?;
        }
    }

    Ok(())
}

/// Collects members of an RDF list starting from `head`.
///
/// Cycles and malformed lists are handled defensively by stopping traversal.
fn collect_rdf_list(graph: &oxrdf::Graph, head: Term) -> Vec<Term> {
    let mut items = Vec::new();
    let mut current = head;
    let mut seen = HashSet::new();
    while let Term::BlankNode(_) = current.clone() {
        let item = current.to_string();
        if !seen.insert(item) {
            break;
        }

        let Some(first) = single_object(graph, &current, rdf::FIRST.as_str()) else {
            break;
        };
        let Some(rest) = single_object(graph, &current, rdf::REST.as_str()) else {
            break;
        };
        items.push(first);
        current = rest;
        if current == Term::NamedNode(NamedNode::new(rdf::NIL.as_str()).expect("RDF NIL")) {
            break;
        }
    }

    items
}

/// Returns all objects for `(subject, predicate_iri)`.
pub(super) fn objects_of(graph: &oxrdf::Graph, subject: &Term, predicate_iri: &str) -> Vec<Term> {
    let subject_id = subject.to_string();
    graph
        .iter()
        .filter(|triple| {
            triple.predicate.as_str() == predicate_iri && triple.subject.to_string() == subject_id
        })
        .map(|triple| triple.object.into())
        .collect()
}

/// Returns the first object for `(subject, predicate_iri)`, if any.
pub(super) fn single_object(
    graph: &oxrdf::Graph,
    subject: &Term,
    predicate_iri: &str,
) -> Option<Term> {
    objects_of(graph, subject, predicate_iri).into_iter().next()
}

/// Chooses the query output mode and comparison strategy from expected result extension.
fn map_expected_result(path: &Path) -> anyhow::Result<(DeOutput, CompareKind)> {
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "srj" => Ok((DeOutput::JSON, CompareKind::Query(QueryResultsFormat::Json))),
        "srx" => Ok((DeOutput::XML, CompareKind::Query(QueryResultsFormat::Xml))),
        "tsv" => Ok((DeOutput::TSV, CompareKind::Query(QueryResultsFormat::Tsv))),
        "csv" => Ok((DeOutput::CSV, CompareKind::Query(QueryResultsFormat::Csv))),
        "nt" => Ok((DeOutput::NTRIPLE, CompareKind::Rdf(RdfFormat::NTriples))),
        "ttl" => Ok((DeOutput::TURTLE, CompareKind::Rdf(RdfFormat::Turtle))),
        "n3" => Ok((DeOutput::N3, CompareKind::Rdf(RdfFormat::N3))),
        "nq" => Ok((DeOutput::NQUADS, CompareKind::Rdf(RdfFormat::NQuads))),
        "rdf" | "xml" => Ok((DeOutput::RDFXML, CompareKind::Rdf(RdfFormat::RdfXml))),
        "trig" => Ok((DeOutput::TRIG, CompareKind::Rdf(RdfFormat::TriG))),
        _ => Err(anyhow::anyhow!(
            "unsupported expected result extension {:?}",
            ext
        )),
    }
}

/// Infers RDF syntax from file extension.
pub(super) fn rdf_format_from_path(path: &Path) -> anyhow::Result<RdfFormat> {
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "nt" => Ok(RdfFormat::NTriples),
        "nq" => Ok(RdfFormat::NQuads),
        "ttl" => Ok(RdfFormat::Turtle),
        "trig" => Ok(RdfFormat::TriG),
        "rdf" | "xml" => Ok(RdfFormat::RdfXml),
        "n3" => Ok(RdfFormat::N3),
        _ => Err(anyhow::anyhow!("unsupported RDF extension {:?}", ext)),
    }
}

/// Loads a manifest graph, trying Turtle first then N3 for compatibility.
fn load_manifest_graph(path: &Path) -> anyhow::Result<oxrdf::Graph> {
    parse_manifest_graph(path, RdfFormat::Turtle).or_else(|turtle_error| {
        parse_manifest_graph(path, RdfFormat::N3).map_err(|n3_error| {
            anyhow::anyhow!(
                "failed to parse manifest {} as Turtle ({turtle_error}) or N3 ({n3_error})",
                path.display()
            )
        })
    })
}

/// Parses one manifest graph using the provided RDF syntax.
fn parse_manifest_graph(path: &Path, format: RdfFormat) -> anyhow::Result<oxrdf::Graph> {
    let file = File::open(path)?;
    let base_iri = manifest_file_uri(path);
    let parser = RdfParser::from_format(format)
        .with_base_iri(&base_iri)?
        .for_reader(BufReader::new(file));

    let mut graph = oxrdf::Graph::new();
    for triple in parser {
        let quad = triple?;
        graph.insert(quad.as_ref());
    }
    Ok(graph)
}

/// Converts a filesystem path into a `file://` IRI.
pub(super) fn manifest_file_uri(path: &Path) -> String {
    de::file_graph_uri_for_path(path).unwrap_or_else(|_| {
        let path = path.to_string_lossy();
        format!("file://{}", path)
    })
}

/// Resolves a manifest term into a local filesystem path when applicable.
///
/// Remote HTTP(S) IRIs are intentionally not materialized as local files.
fn term_to_path(manifest_path: &Path, manifest_base: &Path, term: &Term) -> Option<PathBuf> {
    match term {
        Term::NamedNode(node) => {
            let iri = node.as_str();
            let iri = iri.split('#').next().unwrap_or(iri);
            if iri == "*" {
                return None;
            }
            if iri.starts_with("file://") {
                let mut path = iri.trim_start_matches("file://").to_owned();
                if let Some(rest) = path.strip_prefix("localhost/") {
                    path = format!("/{rest}");
                }
                return Some(Path::new(&path).to_path_buf());
            }
            if iri.starts_with("http://") || iri.starts_with("https://") {
                return None;
            }
            let candidate = Path::new(iri);
            if candidate.is_absolute() {
                Some(candidate.to_path_buf())
            } else {
                Some(manifest_base.join(candidate))
            }
        }
        Term::BlankNode(_) => {
            if manifest_path.exists() {
                Some(manifest_path.to_path_buf())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Resolves a manifest graph term into a graph IRI used for named graph binding.
fn graph_iri_for_term(manifest_base: &Path, term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(node) => {
            let iri = node.as_str();
            if iri == "*" {
                return None;
            }
            if iri.starts_with("http://")
                || iri.starts_with("https://")
                || iri.starts_with("file://")
            {
                return Some(iri.to_string());
            }
            let candidate = Path::new(iri);
            if candidate.is_absolute() {
                Some(file_uri_for_path(candidate))
            } else {
                Some(file_uri_for_path(&manifest_base.join(candidate)))
            }
        }
        _ => None,
    }
}

/// Returns a stable absolute `file://` IRI for `path`.
fn file_uri_for_path(path: &Path) -> String {
    de::file_graph_uri_for_path(path).unwrap_or_else(|_| {
        let absolute = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        format!("file://{}", absolute.to_string_lossy())
    })
}
