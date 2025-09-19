use criterion::{criterion_group, criterion_main, Criterion};
use de::*;
use pprof::criterion::{Output, PProfProfiler};
use std::{fs::OpenOptions, io::BufWriter, time::Duration};
use tempfile::tempdir;

fn query(c: &mut Criterion) {
    // ######### NOTE ###########
    // requires tests/resources/superhero.ttl, run 'make init'
    // ##########################
    let tmp_dir: tempfile::TempDir = tempdir().unwrap();
    let fname = format!("{}/rdf.hdt", tmp_dir.as_ref().display());
    let test_hdt = fname.as_str();
    let _ = std::fs::remove_file(test_hdt);
    let source_rdf = "tests/resources/superhero.ttl".to_string();

    let mut group = c.benchmark_group("create HDT from TTL file");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));
    group.bench_function("hdt create", |b| {
        b.iter(|| create::do_create(test_hdt, std::slice::from_ref(&source_rdf)));
    });
    group.finish();
    let null_path = if cfg!(windows) { "NUL" } else { "/dev/null" };
    let mut null_writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .open(null_path)
            .expect("failed to create bufwriter"),
    );
    let mut group = c.benchmark_group("query single hdt file");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(25));
    group.bench_function("hdt query", {
        |b| {
            b.iter(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        query::do_query(
                            std::slice::from_ref(&source_rdf),
                            &["tests/resources/hero-height.rq".to_string()],
                            &query::DeOutput::CSV,
                            &mut null_writer,
                        )
                        .await
                        .unwrap();
                    })
            });
        }
    });
    group.finish();

    let mut group = c.benchmark_group("query single RDF file");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("hdt query", {
        |b| {
            b.iter(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        query::do_query(
                            std::slice::from_ref(&source_rdf),
                            &["tests/resources/hero-height.rq".to_string()],
                            &query::DeOutput::CSV,
                            &mut null_writer,
                        )
                        .await
                        .unwrap();
                    })
            });
        }
    });
    group.finish();
    let _ = tmp_dir.close();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(1));
    targets = query
}
criterion_main!(benches);
