// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! `WatDiv` query benchmarks through `de::query::do_query`.
//!
//! The workload is the one Comunica runs for its own HDT engine
//! (`performance/benchmark-watdiv-hdt`): a published `dataset.hdt` and twenty
//! query templates with five parameterised instances each. One iteration
//! answers all five instances of a template with a single `do_query` call,
//! which is what `de query` does with the five as `--sparql` files: open the
//! package, parse the queries, evaluate them and serialise the results, here
//! into a sink.
//!
//! Opening and parsing are therefore on the measured path. That is noise
//! against the complex templates (C3 takes seconds), but most of the lightest:
//! on a laptop, S7 takes 1.7 ms this way, against tens of microseconds to
//! evaluate. The light templates track the fixed cost of a query more than
//! evaluation; the complex ones track evaluation, and C3 in particular tracks
//! join order.
//!
//! The twenty templates come in four families:
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

use criterion::{Criterion, criterion_group, criterion_main};
use de::query;
#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};
use std::{
    io::{BufWriter, Sink, sink},
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::tempdir;

/// The twenty `WatDiv` templates, in family order.
const TEMPLATES: [&str; 20] = [
    "C1", "C2", "C3", "F1", "F2", "F3", "F4", "F5", "L1", "L2", "L3", "L4", "L5", "S1", "S2", "S3",
    "S4", "S5", "S6", "S7",
];

const SCALE_VAR: &str = "DE_WATDIV_SCALE";
const DEFAULT_SCALE: &str = "10";

/// Results go to a sink, as in `benches/trainmarks.rs`: serialisation still
/// runs, but the write syscall that would follow isn't part of what is tracked.
fn null_writer() -> BufWriter<Sink> {
    BufWriter::new(sink())
}

fn fixture_dir(scale: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("watdiv")
        .join(scale)
}

/// Split a `WatDiv` template file into its query instances, which are separated
/// by blank lines.
fn split_query_instances(text: &str) -> Vec<String> {
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

/// Write every template's instances to `.rq` files under `dir`, since
/// `do_query` reads queries from files, and return each template's files.
fn write_instances(queries_dir: &Path, dir: &Path) -> Vec<(&'static str, Vec<String>)> {
    TEMPLATES
        .iter()
        .map(|template| {
            let path = queries_dir.join(format!("{template}.txt"));
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
            let instances = split_query_instances(&text);
            assert!(
                !instances.is_empty(),
                "{} contains no queries",
                path.display()
            );
            let files = instances
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
fn run(
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
            &query::DeOutput::CSV,
            writer,
        )
        .await
        .unwrap_or_else(|e| panic!("failed to run {query_files:?}: {e}"));
    });
}

fn watdiv(c: &mut Criterion) {
    let scale = std::env::var(SCALE_VAR).unwrap_or_else(|_| DEFAULT_SCALE.to_string());
    let dir = fixture_dir(&scale);
    let hdt = dir.join("dataset.hdt");
    let queries_dir = dir.join("queries");

    if let Some(missing) = [&hdt, &queries_dir].into_iter().find(|path| !path.exists()) {
        eprintln!(
            "skipping WatDiv benchmarks: {} is missing.\n\
             run `make bench-watdiv` (or `WATDIV_SCALE={scale} make bench-watdiv`) to download \
             the fixtures.",
            missing.display(),
        );
        return;
    }
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
    run(
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
            b.iter(|| run(&runtime, &data_files, query_files, &mut writer));
        });
    }
    group.finish();

    instance_dir
        .close()
        .expect("failed to clean up the query instances");
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(500));
    targets = watdiv
}

#[cfg(not(target_os = "linux"))]
criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_millis(500));
    targets = watdiv
}

criterion_main!(benches);
