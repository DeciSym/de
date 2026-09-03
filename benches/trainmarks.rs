// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Regression benchmarks over the trainmarks e-commerce dataset.
//!
//! `benches/trainmarks` is a submodule of the `DeciSym` fork of the trainmarks
//! RDF benchmark suite, which compares thirteen triplestores on a synthetic
//! customers/orders/products graph. This target reuses two of its artifacts —
//! the data generator and the shared SPARQL queries — to track `de`'s own
//! numbers over time, rather than to compare `de` against other engines.
//!
//! Fixtures come from `make bench-init`, which checks out the submodule and
//! runs `scripts/gen-trainmarks-data.py` to write `benches/trainmarks/data/
//! <scale>.{nt,ttl}`. Without them this target prints how to get them and
//! measures nothing, so `cargo bench` still works on a fresh clone.
//!
//! Both serialisations are measured, because they take different routes into
//! the engine. An `.nt` input is handed to `Hdt::read_nt` more or less as it
//! stands; a `.ttl` input goes through `oxrdfio` first, so it is the only one
//! of the two that puts the RDF parser on the measured path. `de query` given
//! a non-HDT file converts it to a temporary HDT before evaluating, and that
//! route is covered too.
//!
//! Scale is `DE_BENCH_SCALE` (`medium` ~100K, `large` ~1M, `xlarge` ~10M
//! triples), defaulting to `large`. That default is a wall-clock choice:
//! `large` builds its HDT in roughly two seconds against roughly seventeen for
//! `xlarge`, so a create group of ten samples costs half a minute rather than
//! three. Drop to `medium` when a query is slow enough that `large` will not
//! finish.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use de::{create, query};
#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};
use std::{
    io::{BufWriter, Sink, sink},
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::{TempDir, tempdir};

/// The trainmarks queries `de` can answer, paired with the result format each
/// one needs.
///
/// `q6_delete_insert` is deliberately absent: it is a SPARQL Update, HDT
/// packages are immutable, and `de`'s parser rejects DELETE/INSERT outright.
/// The trainmarks report records it as N/A for `de` for the same reason.
///
/// `q5_construct` returns a graph rather than a solution sequence, so it is
/// serialised as Turtle; the other four are tabular and use CSV.
const QUERIES: [(&str, query::DeOutput); 5] = [
    ("q1_count", query::DeOutput::CSV),
    ("q2_customer_orders", query::DeOutput::CSV),
    ("q3_join_3_entities", query::DeOutput::CSV),
    ("q4_optional_aggregation", query::DeOutput::CSV),
    ("q5_construct", query::DeOutput::TURTLE),
];

const SCALE_VAR: &str = "DE_BENCH_SCALE";
const DEFAULT_SCALE: &str = "large";

/// Results go to a sink rather than to a file or `/dev/null`.
///
/// Serialisation still runs — `do_query` formats every solution through this
/// writer — but the write syscall that would follow is not part of what these
/// benchmarks are meant to track, and dropping it also avoids the
/// `/dev/null` vs `NUL` split.
fn null_writer() -> BufWriter<Sink> {
    BufWriter::new(sink())
}

fn trainmarks_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("trainmarks")
}

/// Build an HDT package from `nt` at `dest`, returning the path as the
/// `String` the `de` API takes.
fn build_hdt(runtime: &tokio::runtime::Runtime, nt: &str, dest: &Path) -> String {
    let dest = dest
        .to_str()
        .expect("temporary HDT path must be valid UTF-8")
        .to_string();
    let sources = vec![nt.to_string()];
    runtime.block_on(async {
        create::do_create(&dest, &sources)
            .await
            .expect("failed to build HDT from trainmarks fixture");
    });
    dest
}

fn bench_create(c: &mut Criterion, runtime: &tokio::runtime::Runtime, scale: &str, f: &Fixtures) {
    // A dedicated tempdir, not the one the query fixture lives in:
    // `write_hdt_to_path` clears `<name>.index.*` sidecars before writing, so
    // sharing a path with the query HDT would drop the warmed index cache
    // partway through the run.
    let tmp_dir = tempdir().expect("failed to create create-benchmark tempdir");
    let hdt_path = tmp_dir.path().join("trainmarks.hdt");
    let hdt_path = hdt_path
        .to_str()
        .expect("temporary HDT path must be valid UTF-8")
        .to_string();

    let mut group = c.benchmark_group(format!("trainmarks_create/{scale}"));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    // Throughput is reported against each fixture's own size, so the two
    // benches are not directly comparable as MiB/s: at `large` the same graph
    // is 111 MB of N-Triples and 37 MB of Turtle. Each is only meaningful
    // against its own history, which is what regression tracking needs.
    for (name, source) in [
        ("create_hdt_from_nt", &f.nt),
        ("create_hdt_from_ttl", &f.ttl),
    ] {
        let bytes = std::fs::metadata(source)
            .expect("failed to stat trainmarks fixture")
            .len();
        let sources = vec![source.clone()];
        group.throughput(Throughput::Bytes(bytes));
        group.bench_function(name, |b| {
            b.iter(|| {
                runtime.block_on(async {
                    create::do_create(&hdt_path, &sources)
                        .await
                        .expect("failed to create HDT from trainmarks fixture");
                });
            });
        });
    }
    group.finish();

    tmp_dir
        .close()
        .expect("failed to clean up create-benchmark tempdir");
}

/// Answer one query against a freshly built HDT, untimed.
///
/// The first query to touch an HDT builds its wavelet-tree index sidecar and
/// faults the package in from disk. Both costs are per-package, not per-query,
/// so paying them once here keeps them out of whichever query group happens to
/// run first — including when `cargo bench -- <filter>` runs only one of them.
/// `q1_count` is the cheapest of the five and scans the whole package, so it
/// warms the page cache as a side effect.
fn warm_hdt(runtime: &tokio::runtime::Runtime, hdt_path: &str, queries_dir: &Path) {
    let data_files = vec![hdt_path.to_string()];
    let query_files = vec![
        queries_dir
            .join("q1_count.rq")
            .to_str()
            .expect("trainmarks query path must be valid UTF-8")
            .to_string(),
    ];
    runtime.block_on(async {
        query::do_query(
            &data_files,
            &query_files,
            query::EntailmentMode::Off,
            &query::DeOutput::CSV,
            &mut null_writer(),
        )
        .await
        .expect("failed to warm the trainmarks HDT index");
    });
}

/// The `.nt` and `.ttl` serialisations of one scale.
struct Fixtures {
    nt: String,
    ttl: String,
}

fn bench_queries(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    scale: &str,
    hdt_path: &str,
    queries_dir: &Path,
) {
    let data_files = vec![hdt_path.to_string()];

    for (name, out) in &QUERIES {
        let query_file = queries_dir.join(format!("{name}.rq"));
        let query_files = vec![
            query_file
                .to_str()
                .expect("trainmarks query path must be valid UTF-8")
                .to_string(),
        ];

        let mut group = c.benchmark_group(format!("trainmarks_query/{scale}"));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(20));
        group.bench_function(*name, |b| {
            let mut writer = null_writer();
            b.iter(|| {
                runtime.block_on(async {
                    query::do_query(
                        &data_files,
                        &query_files,
                        query::EntailmentMode::Off,
                        out,
                        &mut writer,
                    )
                    .await
                    .unwrap_or_else(|e| panic!("failed to run trainmarks {name}: {e}"));
                });
            });
        });
        group.finish();
    }
}

/// Answer one query with a non-HDT file as the data source.
///
/// `do_query` converts anything that is not already HDT into a temporary
/// package before evaluating, so this measures parse plus build plus query as
/// one number — the cost a caller actually pays for `de query -d graph.ttl`.
/// Only `q1_count` runs here: every iteration rebuilds the package, so the
/// conversion dominates and the other four queries would add minutes of
/// wall clock to re-measure the same conversion.
fn bench_query_from_rdf(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    scale: &str,
    ttl: &str,
    queries_dir: &Path,
) {
    let data_files = vec![ttl.to_string()];
    let query_files = vec![
        queries_dir
            .join("q1_count.rq")
            .to_str()
            .expect("trainmarks query path must be valid UTF-8")
            .to_string(),
    ];

    let mut group = c.benchmark_group(format!("trainmarks_query/{scale}"));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.bench_function("q1_count_from_ttl", |b| {
        let mut writer = null_writer();
        b.iter(|| {
            runtime.block_on(async {
                query::do_query(
                    &data_files,
                    &query_files,
                    query::EntailmentMode::Off,
                    &query::DeOutput::CSV,
                    &mut writer,
                )
                .await
                .expect("failed to run trainmarks q1_count over Turtle");
            });
        });
    });
    group.finish();
}

fn trainmarks(c: &mut Criterion) {
    let scale = std::env::var(SCALE_VAR).unwrap_or_else(|_| DEFAULT_SCALE.to_string());
    let root = trainmarks_dir();
    let data_dir = root.join("data");
    let nt = data_dir.join(format!("{scale}.nt"));
    let ttl = data_dir.join(format!("{scale}.ttl"));
    let queries_dir = root.join("queries");

    if let Some(missing) = [&nt, &ttl, &queries_dir]
        .into_iter()
        .find(|path| !path.exists())
    {
        eprintln!(
            "skipping trainmarks benchmarks: {} is missing.\n\
             run `make bench-init` (or `BENCH_SCALE={scale} make bench-init`) to check out the \
             benches/trainmarks submodule and generate the fixtures.",
            missing.display(),
        );
        return;
    }
    let to_str = |path: &Path| {
        path.to_str()
            .expect("trainmarks fixture path must be valid UTF-8")
            .to_string()
    };
    let fixtures = Fixtures {
        nt: to_str(&nt),
        ttl: to_str(&ttl),
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    bench_create(c, &runtime, &scale, &fixtures);

    let query_dir: TempDir = tempdir().expect("failed to create query-benchmark tempdir");
    let hdt_path = build_hdt(
        &runtime,
        &fixtures.nt,
        &query_dir.path().join("trainmarks.hdt"),
    );
    warm_hdt(&runtime, &hdt_path, &queries_dir);
    bench_queries(c, &runtime, &scale, &hdt_path, &queries_dir);
    bench_query_from_rdf(c, &runtime, &scale, &fixtures.ttl, &queries_dir);

    query_dir
        .close()
        .expect("failed to clean up query-benchmark tempdir");
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(1));
    targets = trainmarks
}

#[cfg(not(target_os = "linux"))]
criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_millis(1));
    targets = trainmarks
}

criterion_main!(benches);
