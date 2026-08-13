// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

#![cfg(feature = "server")]

mod mcp_tests {
    use de::mcp::tools::{
        ListDataFilesRequest, QueryResultFormat, QuerySparqlRequest, UploadRdfRequest,
        list_data_files, query_sparql, scan_data_directory, upload_rdf,
    };
    use sparesults::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput};
    use std::io::{ErrorKind, Read as _, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};
    use tempfile::{TempDir, tempdir};

    const BOOK1_IRI: &str = "<http://example.org/book/book1>";
    const BOOK2_IRI: &str = "<http://example.org/book/book2>";
    const DC_TITLE: &str = "<http://purl.org/dc/elements/1.1/title>";
    const TITLE_QUERY: &str =
        "SELECT ?title WHERE { ?book <http://purl.org/dc/elements/1.1/title> ?title . }";

    /// Data directory holding `book1.nt` plus a non-RDF file that scans must skip.
    fn setup_data_dir() -> anyhow::Result<TempDir> {
        let dir = tempdir()?;
        std::fs::write(
            dir.path().join("book1.nt"),
            format!("{BOOK1_IRI} {DC_TITLE} \"SPARQL Tutorial\" .\n"),
        )?;
        std::fs::write(dir.path().join("notes.txt"), "not RDF\n")?;
        Ok(dir)
    }

    fn data_dir_arg(dir: &TempDir) -> String {
        dir.path().to_string_lossy().into_owned()
    }

    fn file_list(files: &[&str]) -> Vec<String> {
        files.iter().map(|file| (*file).to_string()).collect()
    }

    /// Collect the `?title` bindings out of a SPARQL 1.1 JSON result document.
    fn titles(results: &serde_json::Value) -> anyhow::Result<Vec<String>> {
        let parsed = QueryResultsParser::from_format(QueryResultsFormat::Json)
            .for_reader(std::io::Cursor::new(results.to_string().into_bytes()))?;
        let ReaderQueryResultsParserOutput::Solutions(solutions) = parsed else {
            return Err(anyhow::anyhow!(
                "expected solution bindings, got a boolean result"
            ));
        };
        let mut titles = Vec::new();
        for solution in solutions {
            let solution = solution?;
            let title = solution
                .get("title")
                .ok_or_else(|| anyhow::anyhow!("solution is missing ?title"))?;
            titles.push(title.to_string());
        }
        titles.sort_unstable();
        Ok(titles)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_data_files_reports_the_queryable_files() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        let data_dir = data_dir_arg(&dir);

        let listing = list_data_files(ListDataFilesRequest {}, data_dir.clone())
            .await
            .map_err(anyhow::Error::msg)?;

        // `notes.txt` is not RDF, so the scan must leave it out.
        assert_eq!(listing.files, vec!["book1.nt".to_string()]);
        assert_eq!(listing.data_dir, data_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_sparql_queries_every_scanned_file() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        std::fs::write(
            dir.path().join("book2.nt"),
            format!("{BOOK2_IRI} {DC_TITLE} \"The Semantic Web\" .\n"),
        )?;

        let response = query_sparql(
            QuerySparqlRequest {
                query: TITLE_QUERY.to_string(),
                files: None,
            },
            data_dir_arg(&dir),
        )
        .await
        .map_err(anyhow::Error::msg)?;

        assert_eq!(response.format, QueryResultFormat::SparqlResultsJson);
        assert_eq!(
            response.files_queried,
            vec!["book1.nt".to_string(), "book2.nt".to_string()]
        );
        assert_eq!(
            titles(&response.results.expect("SELECT returns results"))?,
            vec![
                "\"SPARQL Tutorial\"".to_string(),
                "\"The Semantic Web\"".to_string()
            ]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_sparql_honors_an_explicit_file_selection() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        std::fs::write(
            dir.path().join("book2.nt"),
            format!("{BOOK2_IRI} {DC_TITLE} \"The Semantic Web\" .\n"),
        )?;

        let response = query_sparql(
            QuerySparqlRequest {
                query: TITLE_QUERY.to_string(),
                files: Some(file_list(&[" book2.nt "])),
            },
            data_dir_arg(&dir),
        )
        .await
        .map_err(anyhow::Error::msg)?;

        assert_eq!(response.files_queried, vec!["book2.nt".to_string()]);
        assert_eq!(
            titles(&response.results.expect("SELECT returns results"))?,
            vec!["\"The Semantic Web\"".to_string()]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_sparql_reports_graph_results_as_n_triples() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;

        // CONSTRUCT has no SPARQL-results-JSON serialization, so `de` falls
        // back to N-Triples and the response must say so.
        let response = query_sparql(
            QuerySparqlRequest {
                query: "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . }".to_string(),
                files: None,
            },
            data_dir_arg(&dir),
        )
        .await
        .map_err(anyhow::Error::msg)?;

        assert_eq!(response.format, QueryResultFormat::NTriples);
        assert!(response.results.is_none());
        let graph = response.graph.expect("CONSTRUCT returns a graph");
        assert!(
            graph.contains("SPARQL Tutorial"),
            "unexpected graph payload: {graph}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_sparql_rejects_bad_requests() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        let data_dir = data_dir_arg(&dir);

        let empty_query = query_sparql(
            QuerySparqlRequest {
                query: "   ".to_string(),
                files: None,
            },
            data_dir.clone(),
        )
        .await
        .unwrap_err();
        assert!(
            empty_query.contains("Query parameter cannot be empty"),
            "unexpected error: {empty_query}"
        );

        let no_selection = query_sparql(
            QuerySparqlRequest {
                query: TITLE_QUERY.to_string(),
                files: Some(file_list(&["  "])),
            },
            data_dir.clone(),
        )
        .await
        .unwrap_err();
        assert!(
            no_selection.contains("No files selected"),
            "unexpected error: {no_selection}"
        );

        // A client must not be able to reach outside the served directory.
        let escape = query_sparql(
            QuerySparqlRequest {
                query: TITLE_QUERY.to_string(),
                files: Some(file_list(&["../outside.nt"])),
            },
            data_dir,
        )
        .await
        .unwrap_err();
        assert!(
            escape.contains("only relative paths inside the data directory"),
            "unexpected error: {escape}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_sparql_reports_an_empty_data_directory() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let error = query_sparql(
            QuerySparqlRequest {
                query: TITLE_QUERY.to_string(),
                files: None,
            },
            data_dir_arg(&dir),
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("No files available to query"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upload_rdf_lands_in_uploads_and_becomes_queryable() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        let data_dir = data_dir_arg(&dir);
        let content = format!("{BOOK2_IRI} {DC_TITLE} \"The Semantic Web\" .\n");

        let upload = upload_rdf(
            UploadRdfRequest {
                rdf_content: content.clone(),
                graph_uri: Some("http://example.org/graphs/books".to_string()),
            },
            data_dir.clone(),
        )
        .await
        .map_err(anyhow::Error::msg)?;

        assert!(
            upload.path.starts_with("uploads/graph_books_")
                && Path::new(&upload.path).extension() == Some("ttl".as_ref()),
            "unexpected upload path: {}",
            upload.path
        );
        assert_eq!(upload.bytes_written, content.len() as u64);
        assert!(dir.path().join(&upload.path).is_file());

        // The upload is picked up by the scan, so a follow-up query sees it.
        assert_eq!(
            scan_data_directory(&data_dir)
                .await
                .map_err(anyhow::Error::msg)?,
            vec!["book1.nt".to_string(), upload.path.clone()]
        );

        let response = query_sparql(
            QuerySparqlRequest {
                query: TITLE_QUERY.to_string(),
                files: None,
            },
            data_dir,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        assert_eq!(
            titles(&response.results.expect("SELECT returns results"))?,
            vec![
                "\"SPARQL Tutorial\"".to_string(),
                "\"The Semantic Web\"".to_string()
            ]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upload_rdf_rejects_empty_content() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        let error = upload_rdf(
            UploadRdfRequest {
                rdf_content: "  \n".to_string(),
                graph_uri: None,
            },
            data_dir_arg(&dir),
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("RDF content cannot be empty"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_data_directory_errors_on_a_missing_directory() {
        let error = scan_data_directory("/nonexistent/de-mcp-data-dir")
            .await
            .unwrap_err();
        assert!(
            error.contains("Failed to read data directory"),
            "unexpected error: {error}"
        );
    }

    /// Drive `de serve --mcp stdio` the way a client that launches the server
    /// as a subprocess does: newline-delimited JSON-RPC in on stdin, the same
    /// out on stdout. Claude Desktop takes this path because it accepts only
    /// HTTPS for remote endpoints and so cannot reach the HTTP transport.
    #[test]
    fn serve_mcp_stdio_answers_over_pipes() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;

        let mut child = Command::new(env!("CARGO_BIN_EXE_de"))
            .args([
                "serve",
                "--mcp",
                "stdio",
                "--location",
                &dir.path().to_string_lossy(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;

        let requests = [
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"pipe-test","version":"1.0.0"}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#.to_string(),
            format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"query_sparql","arguments":{{"query":"{TITLE_QUERY}"}}}}}}"#
            ),
        ];
        {
            let mut stdin = child.stdin.take().expect("piped stdin");
            for request in &requests {
                writeln!(stdin, "{request}")?;
            }
            // Dropping stdin closes it, which is how the client signals
            // shutdown — the server drains what it has, then exits.
        }

        let output = child.wait_with_output()?;
        let mut replies = std::collections::HashMap::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if line.trim().is_empty() {
                continue;
            }
            let message: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("non-JSON line on stdout: {line}: {e}"))?;
            if let Some(id) = message["id"].as_u64() {
                replies.insert(id, message);
            }
        }

        let initialize = replies
            .get(&1)
            .ok_or_else(|| anyhow::anyhow!("no initialize reply"))?;
        assert_eq!(initialize["result"]["serverInfo"]["name"], "decisym-engine");

        let tools = replies
            .get(&2)
            .ok_or_else(|| anyhow::anyhow!("no tools/list reply"))?["result"]["tools"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(
            names.contains(&"list_data_files") && names.contains(&"query_sparql"),
            "unexpected tools over stdio: {names:?}"
        );

        let call = replies
            .get(&3)
            .ok_or_else(|| anyhow::anyhow!("no tools/call reply"))?;
        assert_eq!(
            call["result"]["structuredContent"]["results"]["results"]["bindings"][0]["title"]["value"],
            "SPARQL Tutorial"
        );
        Ok(())
    }

    /// Kills the spawned `de serve --mcp` process when the test ends, however
    /// it ends.
    struct ServerProcess {
        child: Child,
        address: String,
    }

    impl Drop for ServerProcess {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// Grab a port the OS just handed out. Racy in principle, but the window
    /// is small and it keeps concurrent test binaries off each other's ports.
    fn free_port() -> anyhow::Result<u16> {
        Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
    }

    fn start_mcp_server(data_dir: &Path) -> anyhow::Result<ServerProcess> {
        let address = format!("127.0.0.1:{}", free_port()?);
        let child = Command::new(env!("CARGO_BIN_EXE_de"))
            .args([
                "serve",
                "--mcp",
                "--location",
                &data_dir.to_string_lossy(),
                "--bind",
                &address,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut server = ServerProcess { child, address };

        let deadline = Instant::now() + Duration::from_mins(1);
        while Instant::now() < deadline {
            if let Some(status) = server.child.try_wait()? {
                return Err(anyhow::anyhow!("de serve --mcp exited early: {status}"));
            }
            if TcpStream::connect(&server.address).is_ok() {
                return Ok(server);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(anyhow::anyhow!(
            "de serve --mcp never accepted connections on {}",
            server.address
        ))
    }

    /// Minimal HTTP/1.1 POST to the MCP endpoint. Hand-rolled so the test
    /// suite does not take on an HTTP client dependency just to prove the
    /// transport is wired up.
    fn post_mcp(address: &str, session: Option<&str>, body: &str) -> anyhow::Result<String> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_mins(1)))?;

        let mut request = format!(
            "POST /mcp HTTP/1.1\r\n\
             Host: {address}\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n",
            body.len()
        );
        if let Some(session) = session {
            use std::fmt::Write as _;
            let _ = write!(request, "mcp-session-id: {session}\r\n");
        }
        request.push_str("\r\n");
        request.push_str(body);
        stream.write_all(request.as_bytes())?;
        stream.flush()?;

        // The transport answers with SSE, so stop once the JSON-RPC payload
        // frame has arrived rather than waiting for the stream to be torn
        // down. Frames carrying no payload (the `retry:` preamble, keep-alives)
        // are not what the caller is waiting for, hence `data: {` and not
        // `data:`.
        let mut response = Vec::new();
        let mut chunk = [0_u8; 8192];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    response.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&response);
                    if text.contains("\r\n\r\n") && text.contains("data: {") {
                        break;
                    }
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    fn session_id(response: &str) -> anyhow::Result<String> {
        response
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("mcp-session-id")
                    .then(|| value.trim().to_string())
            })
            .ok_or_else(|| anyhow::anyhow!("no mcp-session-id header in response:\n{response}"))
    }

    /// Pull the JSON-RPC payload out of the SSE frame so assertions can run
    /// against parsed values rather than substrings of the wire format.
    fn sse_payload(response: &str) -> anyhow::Result<serde_json::Value> {
        let frame = response
            .lines()
            .find_map(|line| line.strip_prefix("data: {"))
            .ok_or_else(|| anyhow::anyhow!("no JSON-RPC data frame in response:\n{response}"))?;
        Ok(serde_json::from_str(&format!("{{{frame}"))?)
    }

    #[test]
    fn serve_mcp_answers_the_full_initialize_and_tool_call_handshake() -> anyhow::Result<()> {
        let dir = setup_data_dir()?;
        let server = start_mcp_server(dir.path())?;

        let initialize = post_mcp(
            &server.address,
            None,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"de-test","version":"1.0.0"}}}"#,
        )?;
        assert!(
            initialize.starts_with("HTTP/1.1 200"),
            "initialize failed:\n{initialize}"
        );
        let session = session_id(&initialize)?;

        let init_result = &sse_payload(&initialize)?["result"];
        assert_eq!(init_result["serverInfo"]["name"], "decisym-engine");
        // The instructions field is the server's only whole-dataset channel;
        // a generic restatement of the description wastes it.
        let instructions = init_result["instructions"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("no instructions in initialize result"))?;
        assert!(
            instructions.contains("list_data_files") && instructions.contains("read-only"),
            "instructions do not orient the client:\n{instructions}"
        );
        assert!(init_result["capabilities"]["prompts"].is_object());

        post_mcp(
            &server.address,
            Some(&session),
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )?;

        let tools_response = post_mcp(
            &server.address,
            Some(&session),
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
        )?;
        let tools = sse_payload(&tools_response)?["result"]["tools"]
            .as_array()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tools/list returned no array:\n{tools_response}"))?;
        let named = |name: &str| -> Option<serde_json::Value> {
            tools.iter().find(|t| t["name"] == name).cloned()
        };

        let query = named("query_sparql").ok_or_else(|| anyhow::anyhow!("no query_sparql tool"))?;
        assert_eq!(query["title"], "Run SPARQL query");
        assert_eq!(query["annotations"]["readOnlyHint"], true);
        assert!(query["outputSchema"].is_object());
        // A one-line description is the common under-description failure; this
        // one has to carry when-to-use and when-not-to.
        let description = query["description"].as_str().unwrap_or_default();
        assert!(
            description.len() > 300
                && description.contains("read-only")
                && description.contains("upload_rdf"),
            "query_sparql is under-described:\n{description}"
        );

        let upload = named("upload_rdf").ok_or_else(|| anyhow::anyhow!("no upload_rdf tool"))?;
        assert_eq!(upload["annotations"]["readOnlyHint"], false);
        assert_eq!(upload["annotations"]["destructiveHint"], false);

        let list = named("list_data_files").ok_or_else(|| anyhow::anyhow!("no list tool"))?;
        assert_eq!(list["annotations"]["readOnlyHint"], true);

        let prompts_response = post_mcp(
            &server.address,
            Some(&session),
            r#"{"jsonrpc":"2.0","id":3,"method":"prompts/list","params":{}}"#,
        )?;
        let prompts = sse_payload(&prompts_response)?["result"]["prompts"]
            .as_array()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("prompts/list returned no array:\n{prompts_response}")
            })?;
        let prompt_names: Vec<&str> = prompts.iter().filter_map(|p| p["name"].as_str()).collect();
        assert!(
            prompt_names.contains(&"explore_dataset")
                && prompt_names.contains(&"describe_resource"),
            "unexpected prompts: {prompt_names:?}"
        );

        let call = post_mcp(
            &server.address,
            Some(&session),
            &format!(
                r#"{{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{{"name":"query_sparql","arguments":{{"query":"{TITLE_QUERY}"}}}}}}"#
            ),
        )?;
        // Structured content means the caller gets parsed bindings, not a
        // JSON document smuggled inside a text block.
        let structured = &sse_payload(&call)?["result"]["structuredContent"];
        assert_eq!(structured["format"], "sparql-results-json");
        assert_eq!(
            structured["results"]["results"]["bindings"][0]["title"]["value"],
            "SPARQL Tutorial"
        );
        assert_eq!(structured["files_queried"][0], "book1.nt");
        Ok(())
    }
}
