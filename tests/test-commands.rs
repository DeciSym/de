mod integration {
    use de::check::Dependency;
    use de::*;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::tempdir;

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
        let r2h = Arc::new(rdf2hdt::Rdf2HdtImpl());

        assert!(create::do_create(
            &new_hdt.clone(),
            &vec!["tests/resources/apple.ttl".to_string()],
            r2h.clone(),
        )
        .is_ok());
        assert!(Path::new(&new_hdt).exists());
        tmp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_do_check() -> anyhow::Result<()> {
        assert!(check::do_check(None).is_ok());

        assert!(check::do_check(Some(vec![Dependency {
            value: "NOT_A_REAL_THING".to_string(),
            dep_type: check::DependencyType::Binary,
            required: true
        }]))
        .is_err());

        assert!(check::do_check(Some(vec![Dependency {
            value: "NOT_A_REAL_THING".to_string(),
            dep_type: check::DependencyType::Env,
            required: true
        }]))
        .is_err());

        assert!(check::do_check(Some(vec![Dependency {
            value: "NOT_A_REAL_THING".to_string(),
            dep_type: check::DependencyType::File,
            required: true
        }]))
        .is_err());
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
        let r2h = Arc::new(rdf2hdt::Rdf2HdtImpl());

        assert!(create::do_create(
            &new_hdt.clone(),
            &vec!["tests/resources/apple.ttl".to_string()],
            r2h.clone(),
        )
        .is_ok());
        assert!(Path::new(&new_hdt).exists());

        assert!(view::view_hdt(&vec![new_hdt]).is_ok());

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
        let r2h = Arc::new(rdf2hdt::Rdf2HdtImpl());

        assert!(create::do_create(
            &new_hdt.clone(),
            &vec!["tests/resources/banana.nt".to_string()],
            r2h.clone(),
        )
        .is_ok());

        let data_files = vec![new_hdt];
        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let res = query::do_query(&data_files, &query_files, r2h, &query::DeOutput::CSV).await;
        assert!(res.is_ok());

        assert_eq!(
            res.unwrap().replace("\r", ""),
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
        let r2h = Arc::new(rdf2hdt::Rdf2HdtImpl());

        assert!(create::do_create(
            &new_hdt.clone(),
            &vec!["tests/resources/banana.ttl".to_string()],
            r2h.clone(),
        )
        .is_ok());

        let data_files = vec![new_hdt];
        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let res = query::do_query(&data_files, &query_files, r2h, &query::DeOutput::CSV).await;
        assert!(res.is_ok());

        assert_eq!(
            res.unwrap().replace("\r", ""),
            r#"fruit
http://example.org/Banana"#
        );
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
        let r2h = Arc::new(rdf2hdt::Rdf2HdtImpl());

        assert!(create::do_create(
            &new_hdt.clone(),
            &vec![
                "tests/resources/pineapple.ttl".to_string(),
                "tests/resources/banana.ttl".to_string()
            ],
            r2h.clone(),
        )
        .is_ok());

        let data_files = vec![new_hdt];
        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let res = query::do_query(&data_files, &query_files, r2h, &query::DeOutput::CSV).await;
        assert!(res.is_ok());

        assert_eq!(
            res.unwrap().replace("\r", ""),
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
        let r2h: Arc<rdf2hdt::Rdf2HdtImpl> = Arc::new(rdf2hdt::Rdf2HdtImpl());

        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let res = query::do_query(&data_files, &query_files, r2h, &query::DeOutput::CSV).await;
        assert!(res.is_ok());

        assert_eq!(
            res.unwrap().replace("\r", ""),
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
        let r2h: Arc<rdf2hdt::Rdf2HdtImpl> = Arc::new(rdf2hdt::Rdf2HdtImpl());

        let data_files = vec!["pineapple.ttl".to_string(), "banana.ttl".to_string()];
        let mut pkgs = vec![];
        for d in data_files {
            let new_hdt = format!(
                "{}/{}",
                tmp_dir.as_ref().display(),
                d.replace(".ttl", ".hdt")
            );
            assert!(create::do_create(
                &new_hdt.clone(),
                &vec![format!("tests/resources/{}", d),],
                r2h.clone(),
            )
            .is_ok());
            pkgs.push(new_hdt.clone());
        }

        let query_files = vec!["tests/resources/query-color.rq".to_string()];
        let res = query::do_query(&pkgs, &query_files, r2h, &query::DeOutput::CSV).await;
        assert!(res.is_ok());

        assert_eq!(
            res.unwrap().replace("\r", ""),
            r#"fruit
http://example.org/Pineapple
http://example.org/Banana"#
        );
        tmp_dir.close()?;
        Ok(())
    }
}
