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
