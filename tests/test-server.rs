// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

#[cfg(feature = "server")]
mod server_tests {
    use de::sparql::AggregateHdt;
    use http::{Method, Request, StatusCode};
    use oxhttp::model::Body;
    use sparesults::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput};
    use std::io::Read as _;
    use std::io::Write;
    use std::path::Path;
    use tempfile::tempdir;

    // W3C SPARQL 1.1 Query examples:
    // https://www.w3.org/TR/sparql11-query/#basicpatterns
    // https://www.w3.org/TR/sparql11-query/#triplePatterns
    const DC_TITLE: &str = "<http://purl.org/dc/elements/1.1/title>";
    const BOOK1_IRI: &str = "<http://example.org/book/book1>";
    const BOOK2_IRI: &str = "<http://example.org/book/book2>";

    fn parse_boolean_json_result(body: &str) -> anyhow::Result<bool> {
        let parsed = QueryResultsParser::from_format(QueryResultsFormat::Json)
            .for_reader(std::io::Cursor::new(body.as_bytes()))?;
        match parsed {
            ReaderQueryResultsParserOutput::Boolean(value) => Ok(value),
            ReaderQueryResultsParserOutput::Solutions(_) => Err(anyhow::anyhow!(
                "expected boolean result, got solution bindings"
            )),
        }
    }

    fn parse_solution_rows_json(body: &str) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)> {
        let parsed = QueryResultsParser::from_format(QueryResultsFormat::Json)
            .for_reader(std::io::Cursor::new(body.as_bytes()))?;
        match parsed {
            ReaderQueryResultsParserOutput::Boolean(_) => Err(anyhow::anyhow!(
                "expected solution bindings, got boolean result"
            )),
            ReaderQueryResultsParserOutput::Solutions(solutions) => {
                let variables = solutions
                    .variables()
                    .iter()
                    .map(|var| var.as_str().to_string())
                    .collect::<Vec<_>>();
                let mut rows = Vec::new();
                for solution in solutions {
                    let solution = solution?;
                    let row = variables
                        .iter()
                        .map(|name| {
                            solution
                                .get(name.as_str())
                                .map_or_else(|| String::from("<UNBOUND>"), ToString::to_string)
                        })
                        .collect::<Vec<_>>();
                    rows.push(row);
                }
                rows.sort_unstable();
                Ok((variables, rows))
            }
        }
    }

    // Helper to create test HDT files
    async fn setup_test_store() -> anyhow::Result<(tempfile::TempDir, AggregateHdt)> {
        let tmp_dir = tempdir()?;

        let book1_nt = tmp_dir.path().join("book1.nt");
        let book2_nt = tmp_dir.path().join("book2.nt");
        std::fs::write(
            &book1_nt,
            format!("{BOOK1_IRI} {DC_TITLE} \"SPARQL Tutorial\" .\n"),
        )?;
        std::fs::write(
            &book2_nt,
            format!("{BOOK2_IRI} {DC_TITLE} \"The Semantic Web\" .\n"),
        )?;

        // Create test HDTs from W3C-style SPARQL example data.
        let book1_hdt = tmp_dir.path().join("book1.hdt");
        de::create::do_create(
            &book1_hdt.to_string_lossy(),
            &[book1_nt.to_string_lossy().to_string()],
        )
        .await?;

        let book2_hdt = tmp_dir.path().join("book2.hdt");
        de::create::do_create(
            &book2_hdt.to_string_lossy(),
            &[book2_nt.to_string_lossy().to_string()],
        )
        .await?;

        // Create AggregateHdt store
        let store = AggregateHdt::new(&[
            book1_hdt.to_string_lossy().to_string(),
            book2_hdt.to_string_lossy().to_string(),
        ])?;

        Ok((tmp_dir, store))
    }

    fn file_graph_uri(work_dir: &Path, name: &str) -> String {
        de::file_graph_uri_for_path(&work_dir.join(name))
            .unwrap_or_else(|_| format!("file://{}", work_dir.join(name).to_string_lossy()))
    }

    // Helper to read body from response
    fn read_body(response: http::Response<Body>) -> anyhow::Result<String> {
        let mut body = response.into_body();
        let mut content = Vec::new();
        body.read_to_end(&mut content)?;
        String::from_utf8(content).map_err(anyhow::Error::from)
    }

    fn create_large_nt_dataset(path: &Path, triples: usize) -> anyhow::Result<()> {
        let mut output = std::io::BufWriter::new(std::fs::File::create(path)?);
        for i in 0..triples {
            writeln!(
                output,
                "<http://example.org/book/book{i}> {DC_TITLE} \"SPARQL Tutorial {i}\" ."
            )?;
        }
        Ok(())
    }

    // Helper to convert HttpError to anyhow::Error
    fn handle_response(
        result: Result<http::Response<Body>, (StatusCode, String)>,
    ) -> anyhow::Result<http::Response<Body>> {
        result.map_err(|(status, msg)| anyhow::anyhow!("HTTP Error {}: {}", status, msg))
    }

    #[tokio::test]
    async fn test_sparql_query_post() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        let query = format!("SELECT ?title WHERE {{ {BOOK1_IRI} {DC_TITLE} ?title . }}");

        let mut request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost/query")
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/sparql-results+json")
            .body(Body::from(query))
            .unwrap();

        let response = handle_response(de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        ))?;

        assert_eq!(response.status(), StatusCode::OK);
        let body_text = read_body(response)?;
        let (variables, rows) = parse_solution_rows_json(&body_text)?;
        assert_eq!(variables, vec!["title"]);
        assert_eq!(rows, vec![vec![String::from("\"SPARQL Tutorial\"")]]);

        Ok(())
    }

    #[tokio::test]
    async fn test_sparql_query_respects_default_graph_uri() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        let book2_graph = file_graph_uri(tmp_dir.path(), "book2.hdt");
        let encode = |value: &str| {
            url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
        };
        let query = format!("ASK {{ {BOOK1_IRI} {DC_TITLE} \"SPARQL Tutorial\" . }}");
        let encoded_query = encode(&query);

        // Without graph scoping, the default dataset includes both HDT graphs.
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!("http://localhost/query?query={encoded_query}"))
            .header("Accept", "application/sparql-results+json")
            .body(Body::empty())
            .unwrap();

        let response = handle_response(de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        ))?;
        let body_text = read_body(response)?;
        assert!(parse_boolean_json_result(&body_text)?);

        // Scoping default graph to book2 excludes book1 triples.
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "http://localhost/query?query={encoded_query}&default-graph-uri={}",
                encode(&book2_graph)
            ))
            .header("Accept", "application/sparql-results+json")
            .body(Body::empty())
            .unwrap();

        let response = handle_response(de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        ))?;
        let body_text = read_body(response)?;
        assert!(!parse_boolean_json_result(&body_text)?);
        Ok(())
    }

    #[tokio::test]
    async fn test_sparql_query_ask() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        let query = format!("ASK {{ {BOOK1_IRI} {DC_TITLE} \"SPARQL Tutorial\" . }}");

        let mut request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost/query")
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/sparql-results+json")
            .body(Body::from(query))
            .unwrap();

        let response = handle_response(de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        ))?;

        assert_eq!(response.status(), StatusCode::OK);
        let body_text = read_body(response)?;
        assert!(parse_boolean_json_result(&body_text)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_sparql_query_streams_large_solution_set() -> anyhow::Result<()> {
        let tmp_dir = tempdir()?;

        let large_nt = tmp_dir.path().join("large.nt");
        create_large_nt_dataset(&large_nt, 50_000)?;

        let large_hdt = tmp_dir.path().join("large.hdt");
        de::create::do_create(
            &large_hdt.to_string_lossy(),
            &[large_nt.to_string_lossy().to_string()],
        )
        .await?;

        let store = AggregateHdt::new(&[large_hdt.to_string_lossy().to_string()])?;

        let encode = |value: &str| {
            url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
        };
        let query = format!("SELECT ?book WHERE {{ ?book {DC_TITLE} ?title . }}");
        let encoded_query = encode(&query);

        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!("http://localhost/query?query={encoded_query}"))
            .header("Accept", "application/sparql-results+json")
            .body(Body::empty())
            .unwrap();

        let response = handle_response(de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        ))?;

        assert_eq!(response.status(), StatusCode::OK);
        let body_text = read_body(response)?;
        let result_count = body_text.matches("http://example.org/book/book").count();
        assert_eq!(result_count, 50_000);
        Ok(())
    }

    #[tokio::test]
    async fn test_sparql_query_service_description() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        // Test GET to /query without query parameter (should return service description)
        let mut request = Request::builder()
            .method(Method::GET)
            .uri("http://localhost/query")
            .header("Accept", "text/turtle")
            .body(Body::empty())
            .unwrap();

        let response = handle_response(de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        ))?;

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("Content-Type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/turtle") || content_type.contains("turtle"));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_create_graph_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        // Test CREATE GRAPH
        let update = "CREATE GRAPH <http://example.org/newgraph>";

        let mut request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost/update")
            .header("Content-Type", "application/sparql-update")
            .body(Body::from(update))
            .unwrap();

        let result = de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        );
        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(msg.contains("SPARQL Update is not supported"));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_insert_data_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        // Test INSERT DATA to a new graph
        let update = r#"
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            INSERT DATA {
                GRAPH <http://example.org/newgraph> {
                    <http://example.org/book/book3> dc:title "The Semantic Web" .
                }
            }
        "#;

        let mut request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost/update")
            .header("Content-Type", "application/sparql-update")
            .body(Body::from(update))
            .unwrap();

        let result = de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        );
        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(msg.contains("SPARQL Update is not supported"));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_delete_data_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        // Test that DELETE DATA is forbidden (read-only for existing graphs)
        let book1_graph = file_graph_uri(tmp_dir.path(), "book1.hdt");
        let update = format!(
            r#"
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            DELETE DATA {{
                GRAPH <{}> {{
                    <http://example.org/book/book1> dc:title "SPARQL Tutorial" .
                }}
            }}
        "#,
            book1_graph
        );

        let mut request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost/update")
            .header("Content-Type", "application/sparql-update")
            .body(Body::from(update))
            .unwrap();

        // DELETE DATA should return NOT_IMPLEMENTED status.
        let result: Result<http::Response<Body>, (StatusCode, String)> = de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        );
        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(msg.contains("SPARQL Update is not supported"));

        Ok(())
    }

    fn assert_store_not_implemented(
        store: &AggregateHdt,
        tmp_dir: &tempfile::TempDir,
        method: Method,
        uri: &str,
        body: Body,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut request_builder = Request::builder().method(method).uri(uri);
        if let Some(content_type) = content_type {
            request_builder = request_builder.header("Content-Type", content_type);
        }
        let mut request = request_builder.body(body).unwrap();

        let result = de::serve::handle_request(
            &mut request,
            store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        );
        assert!(result.is_err());
        let (status, message) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(message.contains("Graph Store Protocol is not supported"));
        Ok(())
    }

    #[tokio::test]
    async fn test_store_get_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        assert_store_not_implemented(
            &store,
            &tmp_dir,
            Method::GET,
            "http://localhost/store?graph=http://example.org/book/book1",
            Body::empty(),
            None,
        )
    }

    #[tokio::test]
    async fn test_store_put_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        assert_store_not_implemented(
            &store,
            &tmp_dir,
            Method::PUT,
            "http://localhost/store?graph=http://example.org/newgraph",
            Body::from("@prefix ex: <http://example.org/> . ex:s ex:p ex:o ."),
            Some("text/turtle"),
        )
    }

    #[tokio::test]
    async fn test_store_post_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        assert_store_not_implemented(
            &store,
            &tmp_dir,
            Method::POST,
            "http://localhost/store",
            Body::from("<http://example.org/s> <http://example.org/p> <http://example.org/o> ."),
            Some("application/n-triples"),
        )
    }

    #[tokio::test]
    async fn test_store_delete_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        assert_store_not_implemented(
            &store,
            &tmp_dir,
            Method::DELETE,
            "http://localhost/store?default",
            Body::empty(),
            None,
        )
    }

    #[tokio::test]
    async fn test_store_head_not_implemented() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        assert_store_not_implemented(
            &store,
            &tmp_dir,
            Method::HEAD,
            "http://localhost/store?graph=http://example.org/any",
            Body::empty(),
            None,
        )
    }

    #[tokio::test]
    async fn test_store_prefix_path_is_not_treated_as_store() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;
        let mut request = Request::builder()
            .method(Method::GET)
            .uri("http://localhost/storehouse")
            .body(Body::empty())
            .unwrap();

        let result = de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        );
        assert!(result.is_err());
        let (status, _message) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_sparql_query() -> anyhow::Result<()> {
        let (tmp_dir, store) = setup_test_store().await?;

        // Test invalid SPARQL query
        let query = "INVALID SPARQL QUERY";

        let mut request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost/query")
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/sparql-results+json")
            .body(Body::from(query))
            .unwrap();

        // Invalid query should return an error
        let result = de::serve::handle_request(
            &mut request,
            &store,
            true,
            tmp_dir.path().to_string_lossy().to_string(),
        );
        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // Check that the error message contains some indication of parsing error
        assert!(msg.contains("expected") || msg.contains("error"));

        Ok(())
    }
}
