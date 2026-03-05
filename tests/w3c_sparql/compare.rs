// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Query-result parsing and equivalence logic for W3C harness cases.

use super::manifest::{objects_of, single_object};
use super::*;
use oxrdf::Term;
use oxrdfio::{RdfFormat, RdfParser};
use sparesults::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput};
use spargebra::{Query as SparqlQuery, SparqlParser};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    path::Path,
    str::FromStr,
};

pub(super) fn query_result_modifiers(path: &Path) -> (bool, bool) {
    let mut text = String::new();
    if File::open(path)
        .and_then(|mut f| f.read_to_string(&mut text))
        .is_err()
    {
        return (false, false);
    }

    let parsed = SparqlParser::new()
        .with_base_iri("http://example.com/")
        .ok()
        .and_then(|parser| parser.parse_query(&text).ok());
    if let Some(query) = parsed {
        let pattern = match &query {
            SparqlQuery::Select { pattern, .. }
            | SparqlQuery::Construct { pattern, .. }
            | SparqlQuery::Describe { pattern, .. }
            | SparqlQuery::Ask { pattern, .. } => pattern,
        };
        return (
            graph_pattern_uses_reduced(pattern),
            graph_pattern_uses_order_by(pattern),
        );
    }

    let lower = text.to_ascii_lowercase();
    (lower.contains("select reduced"), lower.contains("order by"))
}

/// Recursively checks whether a parsed algebra tree contains `REDUCED`.
fn graph_pattern_uses_reduced(pattern: &spargebra::algebra::GraphPattern) -> bool {
    use spargebra::algebra::GraphPattern;

    match pattern {
        GraphPattern::Reduced { .. } => true,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            graph_pattern_uses_reduced(left) || graph_pattern_uses_reduced(right)
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            graph_pattern_uses_reduced(left) || graph_pattern_uses_reduced(right)
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => graph_pattern_uses_reduced(inner),
        other => other.to_string().to_ascii_lowercase().contains("reduced"),
    }
}

/// Recursively checks whether a parsed algebra tree contains `ORDER BY`.
fn graph_pattern_uses_order_by(pattern: &spargebra::algebra::GraphPattern) -> bool {
    use spargebra::algebra::GraphPattern;

    match pattern {
        GraphPattern::OrderBy { .. } => true,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            graph_pattern_uses_order_by(left) || graph_pattern_uses_order_by(right)
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            graph_pattern_uses_order_by(left) || graph_pattern_uses_order_by(right)
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => graph_pattern_uses_order_by(inner),
        other => other.to_string().to_ascii_lowercase().contains("order by"),
    }
}

/// Parses SPARQL results bytes into a canonical row-based representation.
pub(super) fn parse_query_results(
    data: &[u8],
    format: QueryResultsFormat,
) -> anyhow::Result<ParsedQueryResults> {
    let parser = QueryResultsParser::from_format(format);
    let parsed = parser.for_reader(std::io::Cursor::new(data))?;
    match parsed {
        ReaderQueryResultsParserOutput::Boolean(value) => Ok(ParsedQueryResults::Boolean(value)),
        ReaderQueryResultsParserOutput::Solutions(solutions) => {
            let variables = solutions
                .variables()
                .iter()
                .map(|variable| variable.as_str().to_string())
                .collect::<Vec<_>>();
            let mut rows = Vec::new();
            for solution in solutions {
                let solution = solution?;
                let row = variables
                    .iter()
                    .map(|name| match solution.get(name.as_str()) {
                        Some(term) => term.to_string(),
                        None => String::from("<UNBOUND>"),
                    })
                    .collect::<Vec<_>>();
                rows.push(row);
            }
            Ok(ParsedQueryResults::Solutions { variables, rows })
        }
    }
}

/// Minimal CSV parser for W3C result-table fixtures.
///
/// This is intentionally local to avoid additional runtime dependencies in the
/// test harness path.
pub(super) fn parse_csv_table(data: &[u8]) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)> {
    let text = String::from_utf8(data.to_vec())?;
    let mut rows = Vec::<Vec<String>>::new();
    let mut row = Vec::<String>::new();
    let mut cell = String::new();
    let mut chars = text.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cell.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                cell.push(ch);
            }
            continue;
        }

        match ch {
            '"' => in_quotes = true,
            ',' => {
                row.push(std::mem::take(&mut cell));
            }
            '\n' => {
                row.push(std::mem::take(&mut cell));
                rows.push(std::mem::take(&mut row));
            }
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    continue;
                }
                row.push(std::mem::take(&mut cell));
                rows.push(std::mem::take(&mut row));
            }
            _ => cell.push(ch),
        }
    }

    if in_quotes {
        return Err(anyhow::anyhow!("unterminated CSV quoted field"));
    }

    if !cell.is_empty() || !row.is_empty() {
        row.push(cell);
        rows.push(row);
    }

    if rows.is_empty() {
        return Err(anyhow::anyhow!("empty CSV"));
    }
    let headers = rows.remove(0);
    Ok((headers, rows))
}

/// Produces compact result previews for mismatch diagnostics.
pub(super) fn summarize_query_results(results: &ParsedQueryResults) -> String {
    match results {
        ParsedQueryResults::Boolean(value) => format!("boolean({value})"),
        ParsedQueryResults::Solutions { variables, rows } => {
            let mut preview = rows.iter().take(3).cloned().collect::<Vec<_>>();
            if preview.len() < rows.len() {
                preview.push(vec!["...".to_string()]);
            }
            format!(
                "solutions(vars={variables:?}, rows={preview:?}, total_rows={})",
                rows.len()
            )
        }
    }
}

/// Normalized cell representation used by solution comparison logic.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CellValue {
    Unbound,
    Concrete(String),
    BNode(String),
}

/// Compares two parsed query results according to query modifiers.
pub(super) fn query_results_equivalent(
    expected: &ParsedQueryResults,
    actual: &ParsedQueryResults,
    reduced_query: bool,
    ordered_query: bool,
) -> bool {
    match (expected, actual) {
        (ParsedQueryResults::Boolean(e), ParsedQueryResults::Boolean(a)) => e == a,
        (
            ParsedQueryResults::Solutions {
                variables: ev,
                rows: er,
            },
            ParsedQueryResults::Solutions {
                variables: av,
                rows: ar,
            },
        ) => solutions_equivalent(ev, er, av, ar, reduced_query, ordered_query),
        _ => false,
    }
}

/// Compares two solution sets with support for `ORDER BY`, `REDUCED`, and bnode isomorphism.
fn solutions_equivalent(
    expected_vars: &[String],
    expected_rows: &[Vec<String>],
    actual_vars: &[String],
    actual_rows: &[Vec<String>],
    reduced_query: bool,
    ordered_query: bool,
) -> bool {
    if !reduced_query && expected_rows.len() != actual_rows.len() {
        return false;
    }

    let mut all_vars = expected_vars.iter().cloned().collect::<HashSet<_>>();
    let actual_set = actual_vars.iter().cloned().collect::<HashSet<_>>();
    if all_vars != actual_set {
        return false;
    }
    let mut vars = all_vars.drain().collect::<Vec<_>>();
    vars.sort_unstable();

    let eindex = expected_vars
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), i))
        .collect::<HashMap<_, _>>();
    let aindex = actual_vars
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), i))
        .collect::<HashMap<_, _>>();

    let mut e_norm = expected_rows
        .iter()
        .map(|r| normalize_row(r, &vars, &eindex))
        .collect::<Vec<_>>();
    let mut a_norm = actual_rows
        .iter()
        .map(|r| normalize_row(r, &vars, &aindex))
        .collect::<Vec<_>>();

    let e_has_bnode = e_norm
        .iter()
        .any(|row| row.iter().any(|v| matches!(v, CellValue::BNode(_))));
    let a_has_bnode = a_norm
        .iter()
        .any(|row| row.iter().any(|v| matches!(v, CellValue::BNode(_))));

    if reduced_query && !e_has_bnode && !a_has_bnode {
        e_norm.sort_unstable_by_key(|row| row_sort_key(row.as_slice()));
        a_norm.sort_unstable_by_key(|row| row_sort_key(row.as_slice()));
        e_norm.dedup();
        a_norm.dedup();
        return e_norm == a_norm;
    }

    if ordered_query && !reduced_query {
        if e_has_bnode || a_has_bnode {
            return ordered_solutions_bnode_isomorphic(&e_norm, &a_norm);
        }
        return e_norm == a_norm;
    }

    if !e_has_bnode && !a_has_bnode {
        e_norm.sort_unstable_by_key(|row| row_sort_key(row.as_slice()));
        a_norm.sort_unstable_by_key(|row| row_sort_key(row.as_slice()));
        return e_norm == a_norm;
    }

    solutions_bnode_isomorphic(&e_norm, &a_norm)
}

/// Ordered solution comparison that allows blank-node relabeling under a consistent bijection.
fn ordered_solutions_bnode_isomorphic(
    expected_rows: &[Vec<CellValue>],
    actual_rows: &[Vec<CellValue>],
) -> bool {
    if expected_rows.len() != actual_rows.len() {
        return false;
    }

    let mut e2a = HashMap::<String, String>::new();
    let mut a2e = HashMap::<String, String>::new();

    for (expected_row, actual_row) in expected_rows.iter().zip(actual_rows) {
        if expected_row.len() != actual_row.len() {
            return false;
        }
        for (expected_cell, actual_cell) in expected_row.iter().zip(actual_row) {
            match (expected_cell, actual_cell) {
                (CellValue::Unbound, CellValue::Unbound) => {}
                (CellValue::Concrete(e), CellValue::Concrete(a)) if e == a => {}
                (CellValue::BNode(e), CellValue::BNode(a)) => {
                    if let Some(mapped) = e2a.get(e) {
                        if mapped != a {
                            return false;
                        }
                    } else {
                        e2a.insert(e.clone(), a.clone());
                    }
                    if let Some(mapped) = a2e.get(a) {
                        if mapped != e {
                            return false;
                        }
                    } else {
                        a2e.insert(a.clone(), e.clone());
                    }
                }
                _ => return false,
            }
        }
    }

    true
}

/// Reorders one solution row according to canonical variable order and normalizes cells.
fn normalize_row(
    row: &[String],
    vars: &[String],
    index: &HashMap<String, usize>,
) -> Vec<CellValue> {
    vars.iter()
        .map(|name| {
            let Some(i) = index.get(name) else {
                return CellValue::Unbound;
            };
            let Some(value) = row.get(*i) else {
                return CellValue::Unbound;
            };
            if value == "<UNBOUND>" {
                CellValue::Unbound
            } else if let Some(label) = value.strip_prefix("_:") {
                CellValue::BNode(label.to_string())
            } else {
                CellValue::Concrete(canonicalize_query_cell(value))
            }
        })
        .collect::<Vec<_>>()
}

/// Canonicalizes literal lexical forms while preserving datatype identity.
///
/// This prevents false negatives due to equivalent lexical variants (for
/// example `2` vs `2.0` for `xsd:decimal`) without collapsing different
/// datatypes into one bucket.
fn canonicalize_query_cell(value: &str) -> String {
    let Ok(term) = Term::from_str(value) else {
        return value.to_string();
    };
    let Term::Literal(lit) = term else {
        return value.to_string();
    };

    let dt = lit.datatype().as_str();
    if let Some(lang) = lit.language() {
        return format!("lit|lang:{}|{}", lang.to_ascii_lowercase(), lit.value());
    }

    if dt == "http://www.w3.org/2001/XMLSchema#boolean" {
        return match lit.value() {
            "true" | "1" => format!("lit|dt:{dt}|true"),
            "false" | "0" => format!("lit|dt:{dt}|false"),
            _ => format!("lit|dt:{dt}|{}", lit.value()),
        };
    }

    if is_integer_xsd_datatype(dt) {
        if let Some(normalized) = normalize_integer_lexical(lit.value()) {
            return format!("lit|dt:{dt}|{normalized}");
        }
        return format!("lit|dt:{dt}|{}", lit.value());
    }

    if dt == "http://www.w3.org/2001/XMLSchema#decimal" {
        if let Some(normalized) = normalize_decimal_lexical(lit.value()) {
            return format!("lit|dt:{dt}|{normalized}");
        }
        return format!("lit|dt:{dt}|{}", lit.value());
    }

    if dt == "http://www.w3.org/2001/XMLSchema#double"
        || dt == "http://www.w3.org/2001/XMLSchema#float"
    {
        if let Some(normalized) = normalize_float_lexical(lit.value()) {
            return format!("lit|dt:{dt}|{normalized}");
        }
        return format!("lit|dt:{dt}|{}", lit.value());
    }

    format!("lit|dt:{dt}|{}", lit.value())
}

/// Returns whether `dt` is one of the integer-family XSD datatypes.
fn is_integer_xsd_datatype(dt: &str) -> bool {
    matches!(
        dt,
        "http://www.w3.org/2001/XMLSchema#integer"
            | "http://www.w3.org/2001/XMLSchema#long"
            | "http://www.w3.org/2001/XMLSchema#int"
            | "http://www.w3.org/2001/XMLSchema#short"
            | "http://www.w3.org/2001/XMLSchema#byte"
            | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
            | "http://www.w3.org/2001/XMLSchema#positiveInteger"
            | "http://www.w3.org/2001/XMLSchema#unsignedLong"
            | "http://www.w3.org/2001/XMLSchema#unsignedInt"
            | "http://www.w3.org/2001/XMLSchema#unsignedShort"
            | "http://www.w3.org/2001/XMLSchema#unsignedByte"
            | "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
            | "http://www.w3.org/2001/XMLSchema#negativeInteger"
    )
}

/// Canonicalizes integer lexical forms (sign and leading zeros).
fn normalize_integer_lexical(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (negative, digits) = if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else {
        (false, trimmed)
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let without_zeros = digits.trim_start_matches('0');
    if without_zeros.is_empty() {
        return Some("0".to_string());
    }
    if negative {
        Some(format!("-{without_zeros}"))
    } else {
        Some(without_zeros.to_string())
    }
}

/// Canonicalizes decimal lexical forms (optional sign and trailing zeros).
fn normalize_decimal_lexical(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (negative, body) = if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else {
        (false, trimmed)
    };
    if body.is_empty() || body.contains('e') || body.contains('E') {
        return None;
    }
    let (int_part, frac_part) = if let Some((i, f)) = body.split_once('.') {
        (i, f)
    } else {
        (body, "")
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let int_norm = {
        let t = int_part.trim_start_matches('0');
        if t.is_empty() { "0" } else { t }
    };
    let frac_norm = frac_part.trim_end_matches('0');
    let mut normalized = if frac_norm.is_empty() {
        int_norm.to_string()
    } else {
        format!("{int_norm}.{frac_norm}")
    };
    if normalized == "0" {
        return Some(normalized);
    }
    if negative {
        normalized.insert(0, '-');
    }
    Some(normalized)
}

/// Canonicalizes float/double lexical forms, including NaN/INF handling.
fn normalize_float_lexical(value: &str) -> Option<String> {
    let parsed = value.parse::<f64>().ok()?;
    if parsed.is_nan() {
        return Some("NaN".to_string());
    }
    if parsed.is_infinite() {
        if parsed.is_sign_negative() {
            return Some("-INF".to_string());
        }
        return Some("INF".to_string());
    }
    Some(format!("{parsed:e}"))
}

/// Stable string key used for deterministic row sorting.
fn row_sort_key(row: &[CellValue]) -> String {
    row.iter()
        .map(|v| match v {
            CellValue::Unbound => "U".to_string(),
            CellValue::Concrete(c) => format!("C:{c}"),
            CellValue::BNode(b) => format!("B:{b}"),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Row signature that masks blank-node labels for candidate bucketing.
fn row_signature_without_bnodes(row: &[CellValue]) -> String {
    row.iter()
        .map(|v| match v {
            CellValue::Unbound => "U".to_string(),
            CellValue::Concrete(c) => format!("C:{c}"),
            CellValue::BNode(_) => "B:*".to_string(),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Unordered solution comparison with blank-node isomorphism via backtracking search.
fn solutions_bnode_isomorphic(
    expected_rows: &[Vec<CellValue>],
    actual_rows: &[Vec<CellValue>],
) -> bool {
    if expected_rows.len() != actual_rows.len() {
        return false;
    }

    let mut by_sig = HashMap::<String, Vec<usize>>::new();
    for (i, row) in actual_rows.iter().enumerate() {
        by_sig
            .entry(row_signature_without_bnodes(row))
            .or_default()
            .push(i);
    }

    let mut used = vec![false; actual_rows.len()];
    let mut e2a = HashMap::<String, String>::new();
    let mut a2e = HashMap::<String, String>::new();

    fn backtrack(
        idx: usize,
        expected_rows: &[Vec<CellValue>],
        actual_rows: &[Vec<CellValue>],
        by_sig: &HashMap<String, Vec<usize>>,
        used: &mut [bool],
        e2a: &mut HashMap<String, String>,
        a2e: &mut HashMap<String, String>,
    ) -> bool {
        if idx == expected_rows.len() {
            return true;
        }
        let erow = &expected_rows[idx];
        let sig = row_signature_without_bnodes(erow);
        let Some(candidates) = by_sig.get(&sig) else {
            return false;
        };

        for &j in candidates {
            if used[j] {
                continue;
            }
            let arow = &actual_rows[j];
            let mut new_pairs = Vec::<(String, String)>::new();
            let mut ok = true;
            for (e, a) in erow.iter().zip(arow.iter()) {
                match (e, a) {
                    (CellValue::Unbound, CellValue::Unbound) => {}
                    (CellValue::Concrete(ec), CellValue::Concrete(ac)) if ec == ac => {}
                    (CellValue::BNode(eb), CellValue::BNode(ab)) => {
                        if let Some(mapped) = e2a.get(eb) {
                            if mapped != ab {
                                ok = false;
                                break;
                            }
                        } else if let Some(mapped_back) = a2e.get(ab) {
                            if mapped_back != eb {
                                ok = false;
                                break;
                            }
                        } else {
                            e2a.insert(eb.clone(), ab.clone());
                            a2e.insert(ab.clone(), eb.clone());
                            new_pairs.push((eb.clone(), ab.clone()));
                        }
                    }
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                used[j] = true;
                if backtrack(idx + 1, expected_rows, actual_rows, by_sig, used, e2a, a2e) {
                    return true;
                }
                used[j] = false;
            }
            for (eb, ab) in new_pairs {
                e2a.remove(&eb);
                a2e.remove(&ab);
            }
        }

        false
    }

    backtrack(
        0,
        expected_rows,
        actual_rows,
        &by_sig,
        &mut used,
        &mut e2a,
        &mut a2e,
    )
}

/// SPARQL Results Vocabulary predicates used by RDF result-set fixtures.
const RS_BOOLEAN: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#boolean";
const RS_RESULT_VARIABLE: &str =
    "http://www.w3.org/2001/sw/DataAccess/tests/result-set#resultVariable";
const RS_SOLUTION: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#solution";
const RS_BINDING: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#binding";
const RS_VARIABLE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#variable";
const RS_VALUE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#value";
const RS_INDEX: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#index";

/// Parses RDF-encoded SPARQL result sets (`.ttl`, `.rdf`, etc.) into row form.
///
/// If `rs:index` is present, solution rows are ordered by index to preserve
/// explicit ordering semantics from W3C fixtures.
pub(super) fn parse_query_results_rdf(
    data: &[u8],
    format: RdfFormat,
    base_iri: Option<&str>,
) -> anyhow::Result<ParsedQueryResults> {
    let mut graph = oxrdf::Graph::new();
    let parser = match base_iri {
        Some(base) => RdfParser::from_format(format)
            .with_base_iri(base)?
            .for_reader(std::io::Cursor::new(data)),
        None => RdfParser::from_format(format).for_reader(std::io::Cursor::new(data)),
    };
    for triple in parser {
        let quad = triple?;
        graph.insert(quad.as_ref());
    }

    for t in graph.iter() {
        if t.predicate.as_str() == RS_BOOLEAN
            && let Term::Literal(lit) = Term::from(t.object)
        {
            return Ok(ParsedQueryResults::Boolean(lit.value() == "true"));
        }
    }

    let mut variables = Vec::<String>::new();
    for t in graph.iter() {
        if t.predicate.as_str() == RS_RESULT_VARIABLE
            && let Term::Literal(lit) = Term::from(t.object)
        {
            variables.push(lit.value().to_string());
        }
    }
    if variables.is_empty() {
        variables.push("".to_string());
        variables.clear();
    }

    let mut rows = Vec::<(Option<u64>, Vec<String>)>::new();
    for t in graph.iter() {
        if t.predicate.as_str() != RS_SOLUTION {
            continue;
        }
        let solution_subject = Term::from(t.object);
        let mut by_var = std::collections::HashMap::<String, String>::new();
        for binding in objects_of(&graph, &solution_subject, RS_BINDING) {
            let var = single_object(&graph, &binding, RS_VARIABLE).and_then(|v| {
                if let Term::Literal(lit) = v {
                    Some(lit.value().to_string())
                } else {
                    None
                }
            });
            let value = single_object(&graph, &binding, RS_VALUE).map(|v| v.to_string());
            if let (Some(var), Some(value)) = (var, value) {
                by_var.insert(var, value);
            }
        }
        if variables.is_empty() {
            let mut keys = by_var.keys().cloned().collect::<Vec<_>>();
            keys.sort_unstable();
            variables = keys;
        }
        let row = variables
            .iter()
            .map(|name| {
                by_var
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| "<UNBOUND>".to_string())
            })
            .collect::<Vec<_>>();
        let index = single_object(&graph, &solution_subject, RS_INDEX).and_then(|term| {
            let Term::Literal(lit) = term else {
                return None;
            };
            lit.value().parse::<u64>().ok()
        });
        rows.push((index, row));
    }

    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    let rows = rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    Ok(ParsedQueryResults::Solutions { variables, rows })
}
