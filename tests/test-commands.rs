// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

mod integration {
    use de::*;
    use std::{fs::OpenOptions, io::BufWriter, path::Path};
    use tempfile::tempdir;

    fn devnull_writer() -> std::io::Result<BufWriter<std::fs::File>> {
        let path = if cfg!(windows) { "NUL" } else { "/dev/null" };
        Ok(BufWriter::new(OpenOptions::new().write(true).open(path)?))
    }

    // Helper to create a writer that captures output for testing
    fn create_test_writer() -> BufWriter<Vec<u8>> {
        BufWriter::new(Vec::new())
    }

    // Helper to extract captured output from test writer
    fn get_output_from_writer(mut writer: BufWriter<Vec<u8>>) -> std::io::Result<String> {
        use std::io::Write;
        writer.flush()?;
        let buffer = writer.into_inner()?;
        Ok(String::from_utf8_lossy(&buffer).to_string())
    }

    #[test]
    fn test_do_create_rdf() -> anyhow::Result<()> {
        let tmp_dir: tempfile::TempDir = match tempdir() {
            Ok(d) => d,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error creating temporary working dir: {:?}",
                    e
                ))
            }
        };
        let new_hdt = format!("{}/rdf.hdt", tmp_dir.as_ref().display());

        assert!(
            create::do_create(&new_hdt.clone(), &["tests/resources/apple.ttl".to_string()],)
                .is_ok()
        );
        assert!(Path::new(&new_hdt).exists());
        tmp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_view() -> anyhow::Result<()> {
        let tmp_dir: tempfile::TempDir = match tempdir() {
            Ok(d) => d,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error creating temporary working dir: {:?}",
                    e
                ))
            }
        };
        let new_hdt = format!("{}/rdf.hdt", tmp_dir.as_ref().display());

        assert!(
            create::do_create(&new_hdt.clone(), &["tests/resources/apple.ttl".to_string()],)
                .is_ok()
        );
        assert!(Path::new(&new_hdt).exists());

        assert!(view::view_hdt(&[new_hdt], &mut devnull_writer()?).is_ok());

        tmp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_query_single_nt() -> anyhow::Result<()> {
        let tmp_dir: tempfile::TempDir = match tempdir() {
            Ok(d) => d,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error creating temporary working dir: {:?}",
                    e
                ))
            }
        };
        let new_hdt = format!("{}/banana.hdt", tmp_dir.as_ref().display());

        assert!(
            create::do_create(&new_hdt.clone(), &["tests/resources/banana.nt".to_string()],)
                .is_ok()
        );

        let data_files = vec![new_hdt];
        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let mut writer = create_test_writer();
        let res = query::do_query(
            &data_files,
            &query_files,
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
                ))
            }
        };
        let new_hdt = format!("{}/banana.hdt", tmp_dir.as_ref().display());

        assert!(create::do_create(
            &new_hdt.clone(),
            &["tests/resources/banana.ttl".to_string()],
        )
        .is_ok());

        let data_files = vec![new_hdt];
        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let mut writer = create_test_writer();
        let res = query::do_query(
            &data_files,
            &query_files,
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
                ))
            }
        };
        let pineapple_hdt = format!("{}/pineapple.hdt", tmp_dir.as_ref().display());
        assert!(create::do_create(
            &pineapple_hdt.clone(),
            &["tests/resources/pineapple.ttl".to_string()],
        )
        .is_ok());

        let data_files = vec![pineapple_hdt];
        let query_files = vec!["tests/resources/query-fruit-color.rq".to_string()];
        let mut writer = create_test_writer();
        let res = query::do_query(
            &data_files,
            &query_files,
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

        // ASK queries only support CSV, TSV, JSON, or XML
        let mut writer5 = create_test_writer();
        let res = query::do_query(
            &data_files,
            &query_files,
            &query::DeOutput::NTRIPLE,
            &mut writer5,
        )
        .await;
        assert!(res.is_err());
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
                ))
            }
        };
        let new_hdt = format!("{}/combined.hdt", tmp_dir.as_ref().display());

        assert!(create::do_create(
            &new_hdt.clone(),
            &[
                "tests/resources/pineapple.ttl".to_string(),
                "tests/resources/banana.ttl".to_string()
            ],
        )
        .is_ok());

        let data_files = vec![new_hdt];
        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let mut writer = create_test_writer();
        let res = query::do_query(
            &data_files,
            &query_files,
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
                ))
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
                create::do_create(&new_hdt.clone(), &[format!("tests/resources/{d}")],).is_ok()
            );
            pkgs.push(new_hdt.clone());
        }

        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let mut writer = create_test_writer();
        let res = query::do_query(&pkgs, &query_files, &query::DeOutput::CSV, &mut writer).await;
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
}
