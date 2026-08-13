// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! MCP tool implementations backing [`super::McpService`].
//!
//! Each tool takes a request type and returns a typed response that is handed
//! to the client as MCP structured content, validated against the response
//! type's JSON Schema. Errors are `String` because both variants reach the
//! model as text — the error strings are written for a model to read rather
//! than for programmatic matching.

use crate::query::{DeOutput, EntailmentMode, do_query};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::io::BufWriter;
use std::path::{Component, Path};
use uuid::Uuid;

/// Subdirectory of the data directory that `upload_rdf` writes into.
pub const UPLOADS_SUBDIR: &str = "uploads";

/// File extensions the data directory scan treats as queryable RDF.
const QUERYABLE_EXTENSIONS: [&str; 3] = ["hdt", "nt", "ttl"];

/// Longest graph-URI-derived fragment kept in an uploaded file's name.
const MAX_GRAPH_NAME_SEGMENT: usize = 64;

/// Request structure for SPARQL queries
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct QuerySparqlRequest {
    /// The SPARQL 1.1 query to run. SELECT and ASK return result bindings;
    /// CONSTRUCT and DESCRIBE return an RDF graph.
    pub query: String,
    /// Files to query, as paths relative to the data directory exactly as
    /// `list_data_files` reports them (for example `cwe.hdt` or
    /// `uploads/graph_books_1a2b.ttl`). Omit to query every file in the
    /// dataset.
    #[serde(default)]
    pub files: Option<Vec<String>>,
}

/// Request structure for uploading RDF data
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UploadRdfRequest {
    /// RDF content in Turtle syntax. Other serializations are not parsed by
    /// this tool.
    pub rdf_content: String,
    /// Graph URI the content belongs to. Only its last path segment is used,
    /// and only to make the generated file name recognizable — it is not
    /// recorded as a named graph.
    #[serde(default)]
    pub graph_uri: Option<String>,
}

/// Request structure for listing the dataset's files
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ListDataFilesRequest {}

/// Serialization used for a query's payload, determined by the query form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum QueryResultFormat {
    /// SELECT and ASK results, as SPARQL 1.1 Query Results JSON.
    SparqlResultsJson,
    /// CONSTRUCT and DESCRIBE results, as N-Triples text.
    NTriples,
}

/// Response structure for SPARQL queries
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct QuerySparqlResponse {
    /// Which of `results` and `graph` carries this query's payload.
    pub format: QueryResultFormat,
    /// SPARQL 1.1 Query Results JSON for a SELECT or ASK query: `head.vars`
    /// plus `results.bindings` for SELECT, `head` plus `boolean` for ASK.
    /// Null for CONSTRUCT and DESCRIBE.
    pub results: Option<serde_json::Value>,
    /// N-Triples serialization of the graph a CONSTRUCT or DESCRIBE query
    /// produced. Null for SELECT and ASK.
    pub graph: Option<String>,
    /// Files the query actually ran against, relative to the data directory.
    /// Populated from the dataset scan when the request omitted `files`.
    pub files_queried: Vec<String>,
}

/// Response structure for uploading RDF data
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UploadRdfResponse {
    /// Path of the written file relative to the data directory. Pass this to
    /// `query_sparql`'s `files` to query the upload on its own.
    pub path: String,
    /// Bytes of Turtle written.
    pub bytes_written: u64,
}

/// Response structure for listing the dataset's files
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListDataFilesResponse {
    /// The served data directory on the server's filesystem.
    pub data_dir: String,
    /// Queryable files, as paths relative to `data_dir`, sorted. These are the
    /// values `query_sparql` accepts in `files`.
    pub files: Vec<String>,
}

/// List the RDF files that make up the dataset.
///
/// # Errors
///
/// Returns a descriptive error string if the data directory cannot be read.
pub async fn list_data_files(
    _request: ListDataFilesRequest,
    data_dir: String,
) -> Result<ListDataFilesResponse, String> {
    let files = scan_data_directory(&data_dir).await?;
    Ok(ListDataFilesResponse { data_dir, files })
}

/// Execute a SPARQL query against the RDF dataset rooted at `data_dir`.
///
/// `request.files` names files relative to `data_dir`; when absent, every
/// queryable file [`scan_data_directory`] finds is used.
///
/// # Errors
///
/// Returns a descriptive error string if the query is empty, no data files
/// are available, a selected file escapes `data_dir`, the data directory
/// cannot be scanned, the query fails to execute, or output conversion fails.
pub async fn query_sparql(
    request: QuerySparqlRequest,
    data_dir: String,
) -> Result<QuerySparqlResponse, String> {
    if request.query.trim().is_empty() {
        return Err("Query parameter cannot be empty".to_string());
    }

    // Determine which files to query.
    let selected_files = if let Some(files) = request.files {
        let selected = normalize_file_list(files);
        if selected.is_empty() {
            return Err("No files selected: please select at least one file to query, or omit `files` to query the whole dataset".to_string());
        }
        selected
    } else {
        // Use all available data files in the data directory.
        scan_data_directory(&data_dir).await?
    };

    if selected_files.is_empty() {
        return Err("No files available to query".to_string());
    }

    let mut data_files = Vec::with_capacity(selected_files.len());
    for file in &selected_files {
        data_files.push(resolve_data_file(&data_dir, file)?);
    }

    // `do_query` reads queries from disk, so stage the request body in a temp
    // file. Holding the handle until this function returns deletes it on the
    // error paths too.
    let query_file = tempfile::Builder::new()
        .prefix("sparql_query_")
        .suffix(".rq")
        .tempfile()
        .map_err(|e| format!("Failed to create query file: {e}"))?;
    tokio::fs::write(query_file.path(), &request.query)
        .await
        .map_err(|e| format!("Failed to write query file: {e}"))?;
    let query_path = query_file.path().to_string_lossy().into_owned();

    // Execute the query using spawn_blocking to handle non-Send iterators.
    let payload = tokio::task::spawn_blocking(move || {
        let mut writer = BufWriter::new(Vec::<u8>::new());
        let runtime = tokio::runtime::Handle::current();

        // Use block_on to run the async query in the blocking context.
        runtime
            .block_on(do_query(
                &data_files,
                std::slice::from_ref(&query_path),
                EntailmentMode::Off,
                &DeOutput::JSON,
                &mut writer,
            ))
            .map_err(|e| format!("Query execution error: {e}"))?;

        let bytes = writer
            .into_inner()
            .map_err(|e| format!("Writer error: {e}"))?;
        String::from_utf8(bytes).map_err(|e| format!("UTF-8 conversion error: {e}"))
    })
    .await
    .map_err(|e| format!("Task join error: {e}"))??;

    // `DeOutput::JSON` yields SPARQL Results JSON for SELECT and ASK, but
    // `de` falls back to N-Triples for the graph-producing query forms — so
    // the serialization is discovered from the payload rather than assumed.
    Ok(match serde_json::from_str(&payload) {
        Ok(results) => QuerySparqlResponse {
            format: QueryResultFormat::SparqlResultsJson,
            results: Some(results),
            graph: None,
            files_queried: selected_files,
        },
        Err(_) => QuerySparqlResponse {
            format: QueryResultFormat::NTriples,
            results: None,
            graph: Some(payload),
            files_queried: selected_files,
        },
    })
}

/// Upload RDF data to the knowledge graph, writing it into `data_dir`'s
/// [`UPLOADS_SUBDIR`] under a freshly generated file name.
///
/// # Errors
///
/// Returns a descriptive error string if the request content is empty, the
/// uploads directory cannot be created, or the file cannot be written.
pub async fn upload_rdf(
    request: UploadRdfRequest,
    data_dir: String,
) -> Result<UploadRdfResponse, String> {
    if request.rdf_content.trim().is_empty() {
        return Err("RDF content cannot be empty".to_string());
    }

    // Create uploads directory if it doesn't exist.
    let uploads_dir = Path::new(&data_dir).join(UPLOADS_SUBDIR);
    tokio::fs::create_dir_all(&uploads_dir)
        .await
        .map_err(|e| format!("Failed to create uploads directory: {e}"))?;

    // Generate filename. The graph URI is client-supplied, so only its last
    // segment is used and that is reduced to name-safe characters.
    let filename = if let Some(graph) = request.graph_uri.as_deref() {
        format!(
            "graph_{}_{}.ttl",
            sanitize_name_segment(graph.split('/').next_back().unwrap_or("default")),
            Uuid::new_v4()
        )
    } else {
        format!("upload_{}.ttl", Uuid::new_v4())
    };

    // Write the RDF data to file.
    tokio::fs::write(uploads_dir.join(&filename), &request.rdf_content)
        .await
        .map_err(|e| format!("Failed to write RDF file: {e}"))?;

    Ok(UploadRdfResponse {
        path: format!("{UPLOADS_SUBDIR}/{filename}"),
        bytes_written: request.rdf_content.len() as u64,
    })
}

/// List the queryable RDF files in `data_dir` and its [`UPLOADS_SUBDIR`], as
/// paths relative to `data_dir`, sorted for a stable listing.
///
/// A missing or unreadable uploads directory is treated as empty; a missing
/// `data_dir` is an error.
///
/// # Errors
///
/// Returns a descriptive error string if `data_dir` cannot be read.
pub async fn scan_data_directory(data_dir: &str) -> Result<Vec<String>, String> {
    let mut files = Vec::new();

    let mut entries = tokio::fs::read_dir(data_dir)
        .await
        .map_err(|e| format!("Failed to read data directory: {e}"))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("Failed to read directory entry: {e}"))?
    {
        if let Some(name) = queryable_file_name(&entry.path()) {
            files.push(name);
        }
    }

    // Also check uploads directory.
    let uploads_dir = Path::new(data_dir).join(UPLOADS_SUBDIR);
    if let Ok(mut upload_entries) = tokio::fs::read_dir(&uploads_dir).await {
        while let Some(entry) = upload_entries.next_entry().await.ok().flatten() {
            if let Some(name) = queryable_file_name(&entry.path()) {
                files.push(format!("{UPLOADS_SUBDIR}/{name}"));
            }
        }
    }

    files.sort_unstable();
    Ok(files)
}

/// File name of `path` if its extension is one this server can query.
fn queryable_file_name(path: &Path) -> Option<String> {
    let extension = path.extension()?.to_str()?;
    if !QUERYABLE_EXTENSIONS
        .iter()
        .any(|queryable| extension.eq_ignore_ascii_case(queryable))
    {
        return None;
    }
    Some(path.file_name()?.to_str()?.to_string())
}

/// Trim a client-supplied file selection, dropping blank entries.
fn normalize_file_list(files: Vec<String>) -> Vec<String> {
    files
        .into_iter()
        .map(|file| file.trim().to_string())
        .filter(|file| !file.is_empty())
        .collect()
}

/// Join a client-supplied file name onto `data_dir`.
///
/// The name is confined to the data directory: absolute paths and `..`
/// components are rejected rather than silently resolved, so a client cannot
/// use the query tool to read files the operator did not expose.
fn resolve_data_file(data_dir: &str, file: &str) -> Result<String, String> {
    let relative = Path::new(file);
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(format!(
            "Invalid file selection {file:?}: only relative paths inside the data directory can be queried"
        ));
    }
    Ok(Path::new(data_dir)
        .join(relative)
        .to_string_lossy()
        .into_owned())
}

/// Reduce a client-supplied string to characters that are safe in a file name.
fn sanitize_name_segment(segment: &str) -> String {
    let sanitized: String = segment
        .chars()
        .take(MAX_GRAPH_NAME_SEGMENT)
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QUERYABLE_EXTENSIONS, normalize_file_list, queryable_file_name, resolve_data_file,
        sanitize_name_segment,
    };
    use std::path::{MAIN_SEPARATOR, Path};

    #[test]
    fn normalize_file_list_trims_and_drops_blanks() {
        assert_eq!(
            normalize_file_list(vec![
                " a.hdt ".to_string(),
                "  ".to_string(),
                "b.ttl".to_string()
            ]),
            vec!["a.hdt".to_string(), "b.ttl".to_string()]
        );
        assert!(normalize_file_list(vec![" ".to_string()]).is_empty());
    }

    #[test]
    fn resolve_data_file_joins_relative_paths() {
        let resolved = resolve_data_file("/data", "uploads/graph.ttl").unwrap();
        assert_eq!(
            resolved,
            format!("{MAIN_SEPARATOR}data{MAIN_SEPARATOR}uploads{MAIN_SEPARATOR}graph.ttl")
        );
    }

    #[test]
    fn resolve_data_file_rejects_escapes() {
        for escape in ["../secret.ttl", "uploads/../../secret.ttl", "/etc/passwd"] {
            assert!(
                resolve_data_file("/data", escape).is_err(),
                "{escape} should not resolve"
            );
        }
    }

    #[test]
    fn sanitize_name_segment_replaces_unsafe_characters() {
        assert_eq!(sanitize_name_segment("graph-1_a"), "graph-1_a");
        assert_eq!(sanitize_name_segment("../etc/passwd"), "___etc_passwd");
        assert_eq!(sanitize_name_segment(""), "default");
        assert_eq!(sanitize_name_segment(&"x".repeat(200)).len(), 64);
    }

    #[test]
    fn queryable_file_name_matches_rdf_extensions_case_insensitively() {
        for extension in QUERYABLE_EXTENSIONS {
            let upper = extension.to_uppercase();
            assert_eq!(
                queryable_file_name(Path::new(&format!("/data/graph.{upper}"))),
                Some(format!("graph.{upper}"))
            );
        }
        assert_eq!(queryable_file_name(Path::new("/data/notes.txt")), None);
        assert_eq!(queryable_file_name(Path::new("/data/no-extension")), None);
    }
}
