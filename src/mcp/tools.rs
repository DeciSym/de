// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! MCP tool implementations backing [`super::McpService`].
//!
//! Each tool returns `Result<String, String>`; both variants are handed
//! straight back to the MCP client as text content, so the error strings are
//! written for a model to read rather than for programmatic matching.

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
    /// SPARQL query string to execute
    pub query: String,
    /// Optional list of specific files to query (comma-separated). If not provided, queries all available data.
    #[serde(default)]
    pub files: Option<String>,
}

/// Request structure for uploading RDF data
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UploadRdfRequest {
    /// RDF content in Turtle format
    pub rdf_content: String,
    /// Optional graph URI for the uploaded data
    #[serde(default)]
    pub graph_uri: Option<String>,
}

/// Execute a SPARQL query against the RDF dataset rooted at `data_dir`,
/// returning SPARQL 1.1 JSON results.
///
/// `request.files` names files relative to `data_dir`; when absent, every
/// queryable file [`scan_data_directory`] finds is used.
///
/// # Errors
///
/// Returns a descriptive error string if the query is empty, no data files
/// are available, a selected file escapes `data_dir`, the data directory
/// cannot be scanned, the query fails to execute, or output conversion fails.
pub async fn query_sparql(request: QuerySparqlRequest, data_dir: String) -> Result<String, String> {
    if request.query.trim().is_empty() {
        return Err("Query parameter cannot be empty".to_string());
    }

    // Determine which files to query.
    let selected_files = if let Some(files) = request.files {
        let selected = split_file_list(&files);
        if selected.is_empty() {
            return Err("No files selected: please select at least one file to query".to_string());
        }
        selected
    } else {
        // Use all available data files in the data directory.
        scan_data_directory(&data_dir)
            .await
            .map_err(|e| format!("Failed to scan data directory: {e}"))?
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
    tokio::task::spawn_blocking(move || {
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
    .map_err(|e| format!("Task join error: {e}"))?
}

/// Upload RDF data to the knowledge graph, writing it into `data_dir`'s
/// [`UPLOADS_SUBDIR`] under a freshly generated file name.
///
/// # Errors
///
/// Returns a descriptive error string if the request content is empty, the
/// uploads directory cannot be created, or the file cannot be written.
pub async fn upload_rdf(request: UploadRdfRequest, data_dir: String) -> Result<String, String> {
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

    Ok(format!(
        "Successfully uploaded RDF data to: {UPLOADS_SUBDIR}/{filename}"
    ))
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

/// Split a comma-separated client file selection, dropping blank entries.
fn split_file_list(files: &str) -> Vec<String> {
    files
        .split(',')
        .map(str::trim)
        .filter(|file| !file.is_empty())
        .map(ToString::to_string)
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
        QUERYABLE_EXTENSIONS, queryable_file_name, resolve_data_file, sanitize_name_segment,
        split_file_list,
    };
    use std::path::{MAIN_SEPARATOR, Path};

    #[test]
    fn split_file_list_trims_and_drops_blanks() {
        assert_eq!(
            split_file_list(" a.hdt , ,b.ttl,"),
            vec!["a.hdt".to_string(), "b.ttl".to_string()]
        );
        assert!(split_file_list(" , ").is_empty());
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
