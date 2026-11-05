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
