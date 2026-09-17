// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! `WatDiv` query benchmarks over an HDT package.
//!
//! This is the Rust equivalent of Comunica's `performance/benchmark-watdiv-hdt`.
//! Comunica drives `WatDiv` through jbr, which stands up a SPARQL endpoint and
//! measures it over HTTP; `de` has no such indirection, so the workload — the
//! same published `dataset.hdt` and the same twenty templates with five
//! instances each — is measured in process instead.
//!
//! It is measured two ways:
//!
//! * `watdiv_do_query/<scale>` answers each template's five instances with one
//!   `de::query::do_query` call, as `de query` would: open the package, parse
//!   the queries, evaluate them and serialise the results. This is the group CI
//!   compares across commits, because every commit since the target was added
//!   can run it.
//! * `watdiv_query/<scale>` times planning, evaluation and serialisation alone,
//!   over a package opened and queries parsed beforehand, and
//!   `watdiv_open/<scale>` times opening alone. These use APIs this branch adds.
//!
//! The twenty templates come in four families, and they are worth keeping
//! separate because they stress different things:
//!
//! * `C1`–`C3` — complex: long star-and-path mixtures, the ones where join
//!   order decides whether the query takes a millisecond or a second.
//! * `F1`–`F5` — snowflake: a central entity joined out to several branches.
//! * `L1`–`L5` — linear: chains, where each pattern feeds the next.
//! * `S1`–`S7` — star: many patterns sharing one subject.
//!
//! Fixtures come from `make bench-watdiv` (or
//! `scripts/fetch-watdiv-data.sh <scale>`). Without them this target prints how
//! to get them and measures nothing, so `cargo bench` still works on a fresh
//! clone.
//!
//! Scale is `DE_WATDIV_SCALE`: `10` (~1M triples, the default) or `100`
//! (~10M triples). Comunica runs both, as `benchmark-watdiv-hdt` and
//! `benchmark-watdiv-hdt-100`.

mod common;

use common::{benches_dir, null_writer, parse_instances, run_instances, split_query_instances};
use criterion::{Criterion, criterion_group, criterion_main};
use de::query::{self, DeOutput};
use de::sparql::AggregateHdt;
#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};
use spargebra::Query;
use std::io::{BufWriter, Sink};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::tempdir;

/// The twenty `WatDiv` templates, in family order.
const TEMPLATES: [&str; 20] = [
    "C1", "C2", "C3", "F1", "F2", "F3", "F4", "F5", "L1", "L2", "L3", "L4", "L5", "S1", "S2", "S3",
    "S4", "S5", "S6", "S7",
];

const SCALE_VAR: &str = "DE_WATDIV_SCALE";
const DEFAULT_SCALE: &str = "10";

fn fixture_dir(scale: &str) -> PathBuf {
    benches_dir().join("watdiv").join(scale)
}

/// The scale to run, and its package and query directory, or `None` (after
/// saying how to get them) when the fixtures are missing.
fn fixtures() -> Option<(String, PathBuf, PathBuf)> {
    let scale = std::env::var(SCALE_VAR).unwrap_or_else(|_| DEFAULT_SCALE.to_string());
    let dir = fixture_dir(&scale);
    let hdt = dir.join("dataset.hdt");
    let queries_dir = dir.join("queries");
    if let Some(missing) = [&hdt, &queries_dir]
        .into_iter()
        .find(|path| !path.exists())
    {
        eprintln!(
            "skipping WatDiv benchmarks: {} is missing.\n\
             run `make bench-watdiv` (or `WATDIV_SCALE={scale} make bench-watdiv`) to download \
             the fixtures.",
            missing.display(),
        );
        return None;
    }
    Some((scale, hdt, queries_dir))
}

/// Read one template file's instances.
fn read_instances(queries_dir: &Path, template: &str) -> Vec<String> {
    let path = queries_dir.join(format!("{template}.txt"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let instances = split_query_instances(&text);
    assert!(
        !instances.is_empty(),
        "{} contains no queries",
        path.display()
    );
    instances
}

/// Load every template's instances, parsed and ready to evaluate.
fn load_workload(queries_dir: &Path) -> Vec<(&'static str, Vec<Query>)> {
    TEMPLATES
        .iter()
        .map(|template| {
            let instances = read_instances(queries_dir, template);
            (*template, parse_instances(template, &instances))
        })
        .collect()
}

/// Write every template's instances to `.rq` files under `dir`, since
/// `do_query` reads queries from files, and return each template's files.
fn write_instances(queries_dir: &Path, dir: &Path) -> Vec<(&'static str, Vec<String>)> {
    TEMPLATES
        .iter()
        .map(|template| {
            let files = read_instances(queries_dir, template)
                .iter()
                .enumerate()
                .map(|(i, instance)| {
                    let file = dir.join(format!("{template}-{i}.rq"));
                    std::fs::write(&file, instance)
                        .unwrap_or_else(|e| panic!("failed to write {}: {e}", file.display()));
                    file.to_str()
                        .expect("query instance path must be valid UTF-8")
                        .to_string()
                })
                .collect();
            (*template, files)
        })
        .collect()
}

/// Answer `query_files` over `data_files` with one `do_query` call.
fn run_do_query(
    runtime: &tokio::runtime::Runtime,
    data_files: &[String],
    query_files: &[String],
    writer: &mut BufWriter<Sink>,
) {
    runtime.block_on(async {
        query::do_query(
            data_files,
            query_files,
            query::EntailmentMode::Off,
            &DeOutput::CSV,
            writer,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to run {query_files:?}: {e}"));
    });
}

fn watdiv_do_query(c: &mut Criterion) {
    let Some((scale, hdt, queries_dir)) = fixtures() else {
        return;
    };
    let data_files = vec![
        hdt.to_str()
            .expect("WatDiv fixture path must be valid UTF-8")
            .to_string(),
    ];
    let instance_dir = tempdir().expect("failed to create a directory for the query instances");
    let workload = write_instances(&queries_dir, instance_dir.path());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    // The first query against a package builds its `.index.v5-rust-cache`
    // sidecar and faults the file in. Both are one-off costs that would
    // otherwise land on whichever template runs first, including when `cargo
    // bench -- <filter>` runs only one of them.
    run_do_query(
        &runtime,
        &data_files,
        &workload[0].1[..1],
        &mut null_writer(),
    );

    let mut group = c.benchmark_group(format!("watdiv_do_query/{scale}"));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    for (template, query_files) in &workload {
        group.bench_function(*template, |b| {
            let mut writer = null_writer();
            b.iter(|| run_do_query(&runtime, &data_files, query_files, &mut writer));
        });
    }
    group.finish();

    instance_dir
        .close()
        .expect("failed to clean up the query instances");
}

fn watdiv(c: &mut Criterion) {
    let Some((scale, hdt, queries_dir)) = fixtures() else {
        return;
    };
    let hdt_path = hdt
        .to_str()
        .expect("WatDiv fixture path must be valid UTF-8")
        .to_string();
    let store = AggregateHdt::new(std::slice::from_ref(&hdt_path))
        .expect("failed to register the WatDiv HDT package");

    // The first open of a package builds its `.index.v5-rust-cache` sidecar and
    // faults the file in. Both are one-off costs that would otherwise land on
    // whichever group happens to run first, including when `cargo bench --
    // <filter>` runs only one of them.
    drop(
        store
            .get_snapshot(None)
            .expect("failed to warm the WatDiv HDT index"),
    );

    bench_open(c, &scale, &store);

    let snapshot = store
        .get_snapshot(None)
        .expect("failed to open the WatDiv HDT package");
    let workload = load_workload(&queries_dir);

    let mut group = c.benchmark_group(format!("watdiv_query/{scale}"));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    for (template, queries) in &workload {
        group.bench_function(*template, |b| {
            let mut writer = null_writer();
            b.iter(|| {
                run_instances(template, queries, &snapshot, &DeOutput::CSV, &mut writer);
            });
        });
    }
    group.finish();
}

/// Time `get_snapshot` on its own.
///
/// This is the per-request cost the server pays today, because `serve` builds a
/// fresh snapshot for every query. It is tracked separately so that the effect
/// of caching HDT handles is visible without disturbing the query numbers.
fn bench_open(c: &mut Criterion, scale: &str, store: &AggregateHdt) {
    let mut group = c.benchmark_group(format!("watdiv_open/{scale}"));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("get_snapshot", |b| {
        b.iter(|| {
            drop(
                store
                    .get_snapshot(None)
                    .expect("failed to open the WatDiv HDT package"),
            );
        });
    });
    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(500));
    targets = watdiv_do_query, watdiv
}

#[cfg(not(target_os = "linux"))]
criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_millis(500));
    targets = watdiv_do_query, watdiv
}

criterion_main!(benches);
