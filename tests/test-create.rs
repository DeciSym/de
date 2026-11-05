// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Integration tests for `de create`.

#![allow(
    clippy::uninlined_format_args,
    clippy::single_char_pattern,
    clippy::needless_raw_string_hashes,
    clippy::wildcard_imports
)]

mod support;

use de::*;
use std::path::Path;
use support::*;
use tempfile::tempdir;

#[tokio::test]
async fn test_do_create_rdf() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };
    let new_hdt = format!("{}/rdf.hdt", tmp_dir.as_ref().display());

    assert!(
        create::do_create(&new_hdt.clone(), &["tests/resources/apple.ttl".to_string()],)
            .await
            .is_ok()
    );
    assert!(Path::new(&new_hdt).exists());
    tmp_dir.close()?;
    Ok(())
}

#[tokio::test]
async fn test_combine_and_query_two_rdfs() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };
    let new_hdt = format!("{}/combined.hdt", tmp_dir.as_ref().display());

    assert!(
        create::do_create(
            &new_hdt.clone(),
            &[
                "tests/resources/pineapple.ttl".to_string(),
                "tests/resources/banana.ttl".to_string()
            ],
        )
        .await
        .is_ok()
    );

    let data_files = vec![new_hdt];
    let query_files = vec!["tests/resources/query-color.rq".to_string()];
    let mut writer = create_test_writer();
    let res = query::do_query(
        &data_files,
        &query_files,
        query::EntailmentMode::Off,
        &query::DeOutput::CSV,
        &mut writer,
    )
    .await;
    assert!(res.is_ok());

    let output = get_output_from_writer(writer)?;
    assert_eq!(
        output.replace("\r", "").trim(),
        r#"fruit
http://example.org/Pineapple
http://example.org/Banana"#
    );
    tmp_dir.close()?;
    Ok(())
}

/// Every serialization `create` accepts yields the same graph. Each fixture
/// holds the triples of `banana.nt`, written the way that format is usually
/// written (prefixes, nesting, typed literals) rather than as N-Triples.
#[tokio::test]
async fn test_create_accepts_each_rdf_serialization() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let expected = banana_triples()?;
    for ext in ["nt", "ttl", "nq", "trig", "n3", "rdf", "jsonld"] {
        let hdt = tmp_dir.path().join(format!("banana-{ext}.hdt"));
        let hdt = hdt.to_string_lossy().to_string();
        create::do_create(&hdt, &[format!("tests/resources/banana.{ext}")])
            .await
            .map_err(|e| anyhow::anyhow!("create from .{ext}: {e}"))?;

        let mut writer = create_test_writer();
        query::do_query(
            std::slice::from_ref(&hdt),
            &["tests/resources/construct-all.rq".to_string()],
            query::EntailmentMode::Off,
            &query::DeOutput::NTRIPLE,
            &mut writer,
        )
        .await?;
        let output = get_output_from_writer(writer)?;
        assert_eq!(
            triple_set(output.as_bytes(), oxrdfio::RdfFormat::NTriples)?,
            expected,
            "graph created from banana.{ext}"
        );
    }
    Ok(())
}

/// A failed create exits non-zero, explains why on stderr, and leaves neither
/// the output file nor anything in the temporary directory.
#[test]
fn test_create_cli_failures_leave_nothing_behind() -> anyhow::Result<()> {
    let work = tempdir()?;
    let cases: [(&str, String, &str); 3] = [
        (
            "tests/resources/invalid.nt",
            work.path()
                .join("invalid.hdt")
                .to_string_lossy()
                .to_string(),
            "Parser error",
        ),
        (
            "tests/resources/missing.ttl",
            work.path()
                .join("missing.hdt")
                .to_string_lossy()
                .to_string(),
            "could not be found",
        ),
        (
            "tests/resources/banana.ttl",
            work.path()
                .join("no-such-dir/banana.hdt")
                .to_string_lossy()
                .to_string(),
            "No such file or directory",
        ),
    ];
    for (data, output_name, reason) in cases {
        let tmp = tempdir()?;
        let output = run_de_with_tmpdir(
            Path::new("."),
            tmp.path(),
            &["create", "--output-name", &output_name, "--data", data],
        )?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "create from {data} should fail");
        assert!(stderr.contains(reason), "create from {data}: {stderr}");
        assert!(
            !Path::new(&output_name).exists(),
            "{output_name} was written"
        );
        assert_eq!(
            leftover_temp_files(tmp.path())?,
            Vec::<String>::new(),
            "{data}"
        );
    }
    Ok(())
}

/// HDT holds one graph, so quads from different named graphs are only merged
/// when `--allow-merge-named-graphs` says so.
#[test]
fn test_create_cli_merges_named_graphs_only_when_allowed() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let merged = tmp_dir.path().join("merged.hdt");
    let merged = merged.to_string_lossy().to_string();
    let args = [
        "create",
        "--output-name",
        &merged,
        "--data",
        "tests/resources/graph1.nq",
        "tests/resources/graph2.nq",
    ];

    let stderr = run_de_err(Path::new("."), &args)?;
    assert!(stderr.contains("--allow-merge-named-graphs"), "{stderr}");
    assert!(!Path::new(&merged).exists());

    let mut allowed = args.to_vec();
    allowed.push("--allow-merge-named-graphs");
    run_de_ok(Path::new("."), &allowed)?;
    let output = run_de_ok_stdout(
        Path::new("."),
        &[
            "query",
            "--data",
            &merged,
            "--sparql",
            "tests/resources/query-fruit.rq",
        ],
    )?;
    assert_eq!(
        output,
        "fruit\nhttp://example.org/Banana\nhttp://example.org/Pineapple"
    );
    Ok(())
}
