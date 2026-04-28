use criterion::{Criterion, criterion_group, criterion_main};
use de::*;
#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};
use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::Path,
    time::Duration,
};
use tempfile::tempdir;

fn devnull_writer() -> BufWriter<File> {
    let null_path = if cfg!(windows) { "NUL" } else { "/dev/null" };
    let file = OpenOptions::new()
        .write(true)
        .open(null_path)
        .expect("failed to open null sink");
    BufWriter::new(file)
}

fn query(c: &mut Criterion) {
    // ######### NOTE ###########
    // requires tests/resources/superhero.ttl, run 'make init'
    // ##########################
    let source_rdf = "tests/resources/superhero.ttl".to_string();
    let query_file = "tests/resources/hero-height.rq".to_string();
    assert!(
        Path::new(&source_rdf).exists(),
        "missing benchmark fixture {source_rdf}; run `make init`"
    );
    assert!(
        Path::new(&query_file).exists(),
        "missing benchmark query fixture {query_file}; run `make init`"
    );

    let tmp_dir = tempdir().expect("failed to create benchmark tempdir");
    let test_hdt = tmp_dir.path().join("rdf.hdt");
    let test_hdt_path = test_hdt
        .to_str()
        .expect("temporary HDT path must be valid UTF-8")
        .to_string();
    let query_files = vec![query_file];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    let mut create_group = c.benchmark_group("create_hdt_from_ttl_file");
    create_group.sample_size(10);
    create_group.measurement_time(Duration::from_secs(120));
    create_group.bench_function("create_hdt", |b| {
        b.iter(|| {
            runtime.block_on(async {
                create::do_create(&test_hdt_path, std::slice::from_ref(&source_rdf))
                    .await
                    .expect("failed to create HDT from benchmark fixture");
            })
        });
    });
    create_group.finish();

    runtime.block_on(async {
        create::do_create(&test_hdt_path, std::slice::from_ref(&source_rdf))
            .await
            .expect("failed to prepare HDT fixture for query benchmarks");
    });

    let hdt_data_files = vec![test_hdt_path];
    let mut hdt_writer = devnull_writer();
    let mut hdt_group = c.benchmark_group("query_single_hdt_file");
    hdt_group.sample_size(10);
    hdt_group.measurement_time(Duration::from_secs(25));
    hdt_group.bench_function("query_hdt", |b| {
        b.iter(|| {
            runtime.block_on(async {
                query::do_query(
                    &hdt_data_files,
                    &query_files,
                    query::EntailmentMode::Off,
                    &query::DeOutput::CSV,
                    &mut hdt_writer,
                )
                .await
                .expect("failed to query HDT benchmark fixture");
            })
        });
    });
    hdt_group.finish();

    let rdf_data_files = vec![source_rdf];
    let mut rdf_writer = devnull_writer();
    let mut rdf_group = c.benchmark_group("query_single_rdf_file");
    rdf_group.sample_size(10);
    rdf_group.measurement_time(Duration::from_secs(5));
    rdf_group.bench_function("query_rdf", |b| {
        b.iter(|| {
            runtime.block_on(async {
                query::do_query(
                    &rdf_data_files,
                    &query_files,
                    query::EntailmentMode::Off,
                    &query::DeOutput::CSV,
                    &mut rdf_writer,
                )
                .await
                .expect("failed to query RDF benchmark fixture");
            })
        });
    });
    rdf_group.finish();

    tmp_dir
        .close()
        .expect("failed to clean up benchmark tempdir");
}

#[cfg(target_os = "linux")]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(1));
    targets = query
}

#[cfg(not(target_os = "linux"))]
criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_millis(1));
    targets = query
}

criterion_main!(benches);
