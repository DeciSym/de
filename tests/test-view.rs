// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Integration tests for `de view`.

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
async fn test_view() -> anyhow::Result<()> {
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

    assert!(view::view_hdt(&[new_hdt], &mut devnull_writer()?).is_ok());

    tmp_dir.close()?;
    Ok(())
}

/// Creates `name` in `dir` from `data`, through the CLI, and returns its path.
fn create_hdt(dir: &Path, name: &str, data: &str, extra: &[&str]) -> anyhow::Result<String> {
    let hdt = dir.join(name).to_string_lossy().to_string();
    let mut args = vec!["create", "--output-name", &hdt, "--data", data];
    args.extend_from_slice(extra);
    run_de_ok(Path::new("."), &args)?;
    Ok(hdt)
}

#[tokio::test]
async fn test_view_reports_header_statistics() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let hdt = create_hdt(
        tmp_dir.path(),
        "apple.hdt",
        "tests/resources/apple.ttl",
        &[],
    )?;

    let mut writer = create_test_writer();
    view::view_hdt(std::slice::from_ref(&hdt), &mut writer)?;
    let output = get_output_from_writer(writer)?;

    // apple.ttl holds 9 triples about 2 subjects, with 7 distinct predicates.
    assert!(output.starts_with(&format!("{hdt}:\n")), "{output}");
    for line in [
        "http://rdfs.org/ns/void#triples: \"9\"",
        "http://rdfs.org/ns/void#distinctSubjects: \"2\"",
        "http://rdfs.org/ns/void#properties: \"7\"",
        "http://purl.org/HDT/hdt#triplesnumTriples: \"9\"",
    ] {
        assert!(output.contains(line), "missing {line:?} in:\n{output}");
    }
    Ok(())
}

#[tokio::test]
async fn test_view_shows_graph_iri_metadata() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let with_iri = create_hdt(
        tmp_dir.path(),
        "with-iri.hdt",
        "tests/resources/apple.ttl",
        &["--graph-iri", "http://example.org/apples"],
    )?;
    let without_iri = create_hdt(
        tmp_dir.path(),
        "plain.hdt",
        "tests/resources/apple.ttl",
        &[],
    )?;

    let output = run_de_ok_stdout(Path::new("."), &["view", "--data", &with_iri])?;
    assert!(
        output.contains("https://decisym.ai/de#graphIRI: <http://example.org/apples>"),
        "{output}"
    );
    let output = run_de_ok_stdout(Path::new("."), &["view", "--data", &without_iri])?;
    assert!(
        !output.contains("https://decisym.ai/de#graphIRI"),
        "{output}"
    );
    Ok(())
}

#[tokio::test]
async fn test_view_lists_each_file() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let apple = create_hdt(
        tmp_dir.path(),
        "apple.hdt",
        "tests/resources/apple.ttl",
        &[],
    )?;
    let banana = create_hdt(
        tmp_dir.path(),
        "banana.hdt",
        "tests/resources/banana.ttl",
        &[],
    )?;

    let output = run_de_ok_stdout(Path::new("."), &["view", "--data", &apple, &banana])?;
    let apple_at = output.find(&format!("{apple}:\n")).expect("apple heading");
    let banana_at = output
        .find(&format!("{banana}:\n"))
        .expect("banana heading");
    assert!(apple_at < banana_at, "files are shown in the order given");
    // banana.ttl holds 12 triples, apple.ttl 9.
    assert!(output[banana_at..].contains("http://rdfs.org/ns/void#triples: \"12\""));
    assert!(output[..banana_at].contains("http://rdfs.org/ns/void#triples: \"9\""));
    Ok(())
}

#[test]
fn test_view_cli_rejects_missing_and_non_hdt_files() -> anyhow::Result<()> {
    let stderr = run_de_err(
        Path::new("."),
        &["view", "--data", "tests/resources/missing.hdt"],
    )?;
    assert!(stderr.contains("could not be found"), "{stderr}");

    let stderr = run_de_err(
        Path::new("."),
        &["view", "--data", "tests/resources/apple.ttl"],
    )?;
    assert!(stderr.contains("failed to read HDT header"), "{stderr}");
    Ok(())
}
