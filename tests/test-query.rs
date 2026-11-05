// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Integration tests for `de query`.

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
async fn test_query_single_nt() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };
    let new_hdt = format!("{}/banana.hdt", tmp_dir.as_ref().display());

    assert!(
        create::do_create(&new_hdt.clone(), &["tests/resources/banana.nt".to_string()],)
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
http://example.org/Banana"#
    );
    tmp_dir.close()?;
    Ok(())
}

#[tokio::test]
async fn test_query_single_ttl() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };
    let new_hdt = format!("{}/banana.hdt", tmp_dir.as_ref().display());

    assert!(
        create::do_create(
            &new_hdt.clone(),
            &["tests/resources/banana.ttl".to_string()],
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
http://example.org/Banana"#
    );
    tmp_dir.close()?;
    Ok(())
}

#[tokio::test]
async fn test_query_results_format() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };
    let pineapple_hdt = format!("{}/pineapple.hdt", tmp_dir.as_ref().display());
    assert!(
        create::do_create(
            &pineapple_hdt.clone(),
            &["tests/resources/pineapple.ttl".to_string()],
        )
        .await
        .is_ok()
    );

    let data_files = vec![pineapple_hdt];
    let query_files = vec!["tests/resources/query-fruit-color.rq".to_string()];
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
        r#"fruit,color
http://example.org/Pineapple,yellow"#
    );

    let mut writer2 = create_test_writer();
    let res = query::do_query(
        &data_files,
        &query_files,
        query::EntailmentMode::Off,
        &query::DeOutput::TSV,
        &mut writer2,
    )
    .await;
    assert!(res.is_ok());

    let output2 = get_output_from_writer(writer2)?;
    assert_eq!(
        output2.replace("\r", "").trim(),
        "?fruit\t?color\n<http://example.org/Pineapple>\t\"yellow\""
    );

    let mut writer3 = create_test_writer();
    let res = query::do_query(
        &data_files,
        &query_files,
        query::EntailmentMode::Off,
        &query::DeOutput::JSON,
        &mut writer3,
    )
    .await;
    assert!(res.is_ok());

    let output3 = get_output_from_writer(writer3)?;
    assert_eq!(
        output3.replace("\r", "").trim(),
        r#"{"head":{"vars":["fruit","color"]},"results":{"bindings":[{"fruit":{"type":"uri","value":"http://example.org/Pineapple"},"color":{"type":"literal","value":"yellow"}}]}}"#
    );

    let mut writer4 = create_test_writer();
    let res = query::do_query(
        &data_files,
        &query_files,
        query::EntailmentMode::Off,
        &query::DeOutput::XML,
        &mut writer4,
    )
    .await;
    assert!(res.is_ok());

    let output4 = get_output_from_writer(writer4)?;
    assert_eq!(
        output4.replace("\r", "").trim(),
        r#"<?xml version="1.0"?><sparql xmlns="http://www.w3.org/2005/sparql-results#"><head><variable name="fruit"/><variable name="color"/></head><results><result><binding name="fruit"><uri>http://example.org/Pineapple</uri></binding><binding name="color"><literal>yellow</literal></binding></result></results></sparql>"#
    );

    // SELECT results are only emitted as SPARQL results formats (CSV/TSV/JSON/XML).
    let mut writer5 = create_test_writer();
    let res = query::do_query(
        &data_files,
        &query_files,
        query::EntailmentMode::Off,
        &query::DeOutput::NTRIPLE,
        &mut writer5,
    )
    .await;
    assert!(res.is_err());
    tmp_dir.close()?;

    Ok(())
}

#[tokio::test]
async fn test_readme_w3c_section_2_1_select_example() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };

    let simple_nt = tmp_dir.path().join("simple.nt");
    let simple_rq = tmp_dir.path().join("simple.rq");
    let simple_hdt = tmp_dir.path().join("simple.hdt");

    std::fs::write(
        &simple_nt,
        "<http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> \"SPARQL Tutorial\" .\n",
    )?;
    std::fs::write(
        &simple_rq,
        "SELECT ?title\nWHERE\n{\n  <http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> ?title .\n}\n",
    )?;

    let rdf_output = run_de_ok_stdout(
        tmp_dir.path(),
        &[
            "query",
            "--data",
            "simple.nt",
            "--sparql",
            "simple.rq",
            "--output",
            "csv",
        ],
    )?;
    assert_eq!(rdf_output, "title\nSPARQL Tutorial");

    run_de_ok(
        tmp_dir.path(),
        &[
            "create",
            "--output-name",
            "simple.hdt",
            "--data",
            "simple.nt",
        ],
    )?;
    assert!(simple_hdt.exists());

    let hdt_output = run_de_ok_stdout(
        tmp_dir.path(),
        &[
            "query",
            "--data",
            "simple.hdt",
            "--sparql",
            "simple.rq",
            "--output",
            "csv",
        ],
    )?;
    assert_eq!(hdt_output, "title\nSPARQL Tutorial");

    tmp_dir.close()?;
    Ok(())
}

#[tokio::test]
async fn test_query_two_rdfs() -> anyhow::Result<()> {
    let data_files = vec![
        "tests/resources/pineapple.ttl".to_string(),
        "tests/resources/banana.ttl".to_string(),
    ];

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
    Ok(())
}

#[tokio::test]
async fn test_query_two_hdts() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };

    let data_files = vec!["pineapple.ttl".to_string(), "banana.ttl".to_string()];
    let mut pkgs = vec![];
    for d in data_files {
        let new_hdt = format!(
            "{}/{}",
            tmp_dir.as_ref().display(),
            d.replace(".ttl", ".hdt")
        );
        assert!(
            create::do_create(&new_hdt.clone(), &[format!("tests/resources/{d}")],)
                .await
                .is_ok()
        );
        pkgs.push(new_hdt.clone());
    }

    let query_files = vec!["tests/resources/query-color.rq".to_string()];
    let mut writer = create_test_writer();
    let res = query::do_query(
        &pkgs,
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

/// Runs one query over `data` and returns what it wrote.
async fn query_output(
    data: &[String],
    query_file: &str,
    entailment: query::EntailmentMode,
    format: &query::DeOutput,
) -> anyhow::Result<String> {
    let mut writer = create_test_writer();
    query::do_query(
        data,
        &[query_file.to_string()],
        entailment,
        format,
        &mut writer,
    )
    .await?;
    Ok(get_output_from_writer(writer)?)
}

/// CONSTRUCT results round-trip through every RDF output format.
#[tokio::test]
async fn test_query_construct_in_each_rdf_format() -> anyhow::Result<()> {
    use oxrdfio::RdfFormat;
    let expected = banana_triples()?;
    let data = ["tests/resources/banana.ttl".to_string()];
    for (format, parse_as) in [
        (query::DeOutput::NTRIPLE, RdfFormat::NTriples),
        (query::DeOutput::NQUADS, RdfFormat::NQuads),
        (query::DeOutput::TURTLE, RdfFormat::Turtle),
        (query::DeOutput::TRIG, RdfFormat::TriG),
        (query::DeOutput::N3, RdfFormat::N3),
        (query::DeOutput::RDFXML, RdfFormat::RdfXml),
    ] {
        let output = query_output(
            &data,
            "tests/resources/construct-all.rq",
            query::EntailmentMode::Off,
            &format,
        )
        .await?;
        assert_eq!(
            triple_set(output.as_bytes(), parse_as)?,
            expected,
            "CONSTRUCT as {format:?}:\n{output}"
        );
    }
    Ok(())
}

/// ASK answers in each results format. The RDF formats have no way to say
/// true or false, so `de` warns and falls back to CSV.
#[tokio::test]
async fn test_query_ask_result_formats() -> anyhow::Result<()> {
    let data = ["tests/resources/banana.ttl".to_string()];
    for (format, expected) in [
        (query::DeOutput::CSV, "true"),
        (query::DeOutput::TSV, "true"),
        (query::DeOutput::JSON, r#"{"head":{},"boolean":true}"#),
        (
            query::DeOutput::XML,
            r#"<?xml version="1.0"?><sparql xmlns="http://www.w3.org/2005/sparql-results#"><head></head><boolean>true</boolean></sparql>"#,
        ),
        (query::DeOutput::NTRIPLE, "true"),
    ] {
        let output = query_output(
            &data,
            "tests/resources/ask-banana-yellow.rq",
            query::EntailmentMode::Off,
            &format,
        )
        .await?;
        assert_eq!(
            output.replace('\r', "").trim(),
            expected,
            "ASK as {format:?}"
        );
    }
    Ok(())
}

/// OWL RL entailment adds what the ontology implies: Banana is a Fruit, and
/// every Fruit is a Food. Here the data and the ontology are packaged into one
/// HDT, which is how they're queried today.
#[tokio::test]
async fn test_query_owl_rl_entailment() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let hdt = tmp_dir
        .path()
        .join("fruit.hdt")
        .to_string_lossy()
        .to_string();
    create::do_create(
        &hdt,
        &[
            "tests/resources/banana.ttl".to_string(),
            "tests/resources/fruit-ontology.ttl".to_string(),
        ],
    )
    .await?;
    let data = [hdt];

    let off = query_output(
        &data,
        "tests/resources/query-food.rq",
        query::EntailmentMode::Off,
        &query::DeOutput::CSV,
    )
    .await?;
    assert_eq!(
        off.replace('\r', "").trim(),
        "food",
        "nothing is a Food without entailment"
    );

    let on = query_output(
        &data,
        "tests/resources/query-food.rq",
        query::EntailmentMode::OwlRl,
        &query::DeOutput::CSV,
    )
    .await?;
    assert_eq!(on.replace('\r', "").trim(), golden("query-owl-rl.csv")?);
    Ok(())
}

/// The same entailment with the ontology passed as a separate `--data` file,
/// once as Turtle and once as HDT. The default graph is the merge of all
/// `--data` files, so the subclass axiom in one applies to the data in the
/// other.
#[tokio::test]
async fn test_query_owl_rl_entailment_across_data_files() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let ontology_hdt = tmp_dir.path().join("fruit-ontology.hdt");
    let ontology_hdt = ontology_hdt.to_string_lossy().to_string();
    create::do_create(
        &ontology_hdt,
        &["tests/resources/fruit-ontology.ttl".to_string()],
    )
    .await?;

    for ontology in [
        "tests/resources/fruit-ontology.ttl".to_string(),
        ontology_hdt,
    ] {
        let data = ["tests/resources/banana.ttl".to_string(), ontology.clone()];
        let on = query_output(
            &data,
            "tests/resources/query-food.rq",
            query::EntailmentMode::OwlRl,
            &query::DeOutput::CSV,
        )
        .await?;
        assert_eq!(
            on.replace('\r', "").trim(),
            golden("query-owl-rl.csv")?,
            "ontology from {ontology}"
        );
    }
    Ok(())
}

/// `--named-graph IRI=PATH` exposes each file as a named graph to `GRAPH ?g`,
/// and the default graph (`--data`) doesn't leak into them.
#[test]
fn test_query_cli_named_graph_bindings() -> anyhow::Result<()> {
    let output = run_de_ok_stdout(
        Path::new("."),
        &[
            "query",
            "--data",
            "tests/resources/apple.ttl",
            "--named-graph",
            "http://example.org/banana=tests/resources/banana.ttl",
            "http://example.org/pineapple=tests/resources/pineapple.ttl",
            "--sparql",
            "tests/resources/query-graphs.rq",
        ],
    )?;
    assert_eq!(output, golden("query-named-graphs.csv")?);
    Ok(())
}

#[tokio::test]
async fn test_query_mixed_hdt_and_rdf_inputs() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let banana_hdt = tmp_dir
        .path()
        .join("banana.hdt")
        .to_string_lossy()
        .to_string();
    create::do_create(&banana_hdt, &["tests/resources/banana.ttl".to_string()]).await?;

    let output = query_output(
        &[banana_hdt, "tests/resources/pineapple.ttl".to_string()],
        "tests/resources/query-color.rq",
        query::EntailmentMode::Off,
        &query::DeOutput::CSV,
    )
    .await?;
    assert_eq!(
        output.replace('\r', "").trim(),
        golden("query-mixed-inputs.csv")?
    );
    Ok(())
}

/// Several `--sparql` files run in the order given, each writing its own
/// result, header included.
#[test]
fn test_query_cli_runs_each_sparql_file_in_order() -> anyhow::Result<()> {
    let output = run_de_ok_stdout(
        Path::new("."),
        &[
            "query",
            "--data",
            "tests/resources/pineapple.ttl",
            "--sparql",
            "tests/resources/query-color.rq",
            "tests/resources/query-fruit-color.rq",
        ],
    )?;
    assert_eq!(output, golden("query-two-files.csv")?);
    Ok(())
}

/// A failed query exits non-zero, explains why on stderr and writes no results.
#[test]
fn test_query_cli_errors() -> anyhow::Result<()> {
    for (data, sparql, extra, reason) in [
        (
            "tests/resources/banana.ttl",
            "tests/resources/query-syntax-error.rq",
            None,
            "Invalid SPARQL query",
        ),
        (
            "tests/resources/missing.hdt",
            "tests/resources/query-color.rq",
            None,
            "does not exist",
        ),
        (
            "tests/resources/banana.ttl",
            "tests/resources/missing.rq",
            None,
            "could not be found",
        ),
        (
            "tests/resources/banana.ttl",
            "tests/resources/query-color.rq",
            Some("ntriple"),
            "Error during execution",
        ),
    ] {
        let mut args = vec!["query", "--data", data, "--sparql", sparql];
        if let Some(format) = extra {
            args.extend(["--output", format]);
        }
        let output = run_de_command(Path::new("."), &args)?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !output.status.success(),
            "de {} should fail",
            args.join(" ")
        );
        assert!(stderr.contains(reason), "de {}: {stderr}", args.join(" "));
        assert!(
            output.stdout.is_empty(),
            "de {} wrote results",
            args.join(" ")
        );
    }
    Ok(())
}

/// `--debug-query-plan` writes the plan as JSON to stderr and leaves the
/// results on stdout as they were.
#[test]
fn test_query_cli_debug_query_plan() -> anyhow::Result<()> {
    let args = [
        "query",
        "--quiet",
        "--data",
        "tests/resources/banana.ttl",
        "--sparql",
        "tests/resources/query-color.rq",
    ];
    let plain = run_de_command(Path::new("."), &args)?;
    let mut with_plan = args.to_vec();
    with_plan.push("--debug-query-plan");
    let debug = run_de_command(Path::new("."), &with_plan)?;

    assert!(plain.status.success() && debug.status.success());
    assert_eq!(debug.stdout, plain.stdout);
    let stderr = String::from_utf8_lossy(&debug.stderr);
    assert!(stderr.trim_start().starts_with('{'), "{stderr}");
    assert!(stderr.contains("\"plan\""), "{stderr}");
    Ok(())
}
