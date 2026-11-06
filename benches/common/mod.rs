// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Shared plumbing for the `WatDiv` and BSBM query benchmarks.
//!
//! Both targets measure the same thing in the same way: open one HDT package,
//! parse a fixed workload of SPARQL queries, and then time evaluation alone.
//! Opening and parsing stay outside the measured region on purpose.
//!
//! * Opening is per-package, not per-query, and is measured separately by the
//!   `*_open` group, because it is the cost the HDT-handle cache is meant to
//!   remove and mixing it into every query would hide the query numbers behind
//!   a constant.
//! * Parsing is `spargebra` work with no HDT in it at all.
//!
//! What is left inside the timed region is planning, evaluation and
//! serialisation — the three things the improvements in
//! `SPARQL-QUERY-PERFORMANCE.md` actually move.

#![allow(dead_code)]

use de::query::{DeOutput, QueryExecutionOptions, run_query_against_dataset};
use de::sparql::{AggregateHdtSnapshot, parse_query};
use spargebra::Query;
use std::io::{BufWriter, Sink, sink};
use std::path::{Path, PathBuf};

/// Base IRI for queries that do not set one. Neither workload uses relative
/// IRIs, so the value only has to parse.
pub const BASE_IRI: &str = "http://example.org/";

/// Results go to a sink rather than to a file or `/dev/null`.
///
/// Serialisation still runs — every solution is formatted through this writer —
/// but the write syscall that would follow is not part of what these benchmarks
/// track, and dropping it also avoids the `/dev/null` vs `NUL` split. This
/// matches what `benches/trainmarks.rs` does.
pub fn null_writer() -> BufWriter<Sink> {
    BufWriter::new(sink())
}

/// `<repo>/benches`, where both workloads keep their fixtures.
pub fn benches_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("benches")
}

/// Split a `WatDiv` template file into its individual query instances.
///
/// `WatDiv` ships one file per template holding several parameterised instances
/// separated by blank lines. Comunica's `benchmark-watdiv-hdt` runs five of
/// them per template; the published fixtures contain exactly five.
pub fn split_query_instances(text: &str) -> Vec<String> {
    let mut instances = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.trim().is_empty() {
                instances.push(std::mem::take(&mut current));
            }
            current.clear();
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.trim().is_empty() {
        instances.push(current);
    }
    instances
}

/// Parse every instance up front so that parsing stays out of the timed region.
pub fn parse_instances(label: &str, texts: &[String]) -> Vec<Query> {
    texts
        .iter()
        .map(|text| {
            parse_query(text, BASE_IRI)
                .unwrap_or_else(|e| panic!("failed to parse {label} instance: {e}\n{text}"))
        })
        .collect()
}

/// Evaluate every instance of one template once, serialising each result set.
///
/// One criterion iteration is a whole template rather than a single instance:
/// that is the unit Comunica reports, and averaging the five instances of a
/// template smooths out the fact that `WatDiv` parameterisation makes some
/// instances match nothing.
pub fn run_instances(
    label: &str,
    queries: &[Query],
    snapshot: &AggregateHdtSnapshot,
    out: &DeOutput,
    writer: &mut BufWriter<Sink>,
) {
    for query in queries {
        run_query_against_dataset(
            query,
            snapshot,
            out,
            writer,
            QueryExecutionOptions::default(),
        )
        .unwrap_or_else(|e| panic!("failed to evaluate {label}: {e}"));
    }
}
