use criterion::{criterion_group, criterion_main, Criterion};
use de::*;
use pprof::criterion::{Output, PProfProfiler};
use std::time::Duration;
use tempfile::tempdir;

fn query(c: &mut Criterion) {
    // ######### NOTE ###########
    // requires dependent binaries and tests/resources/superhero.ttl
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
        b.iter(|| create::do_create(test_hdt, &[source_rdf.clone()]));
    });
    group.finish();

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
                        let _ = query::do_query(
                            &[source_rdf.clone()],
                            &vec!["tests/resources/hero-height.rq".to_string()],
                            &query::DeOutput::CSV,
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
                        let _ = query::do_query(
                            &[source_rdf.clone()],
                            &vec!["tests/resources/hero-height.rq".to_string()],
                            &query::DeOutput::CSV,
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
