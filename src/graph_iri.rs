//! Graph IRI and path resolution utilities.
//!
//! This module centralizes graph-identifier handling for HDT-backed datasets:
//! - Canonicalize all file paths before use.
//! - Prefer embedded HDT graph IRI metadata when present.
//! - Otherwise derive a deterministic `file://` graph IRI from the file path.
//! - Enforce that one graph IRI maps to exactly one canonical file path.

use crate::hdt_meta;
use oxrdf::NamedNode;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use url::Url;

/// Validated graph IRI wrapper.
///
/// Values are normalized through `oxrdf::NamedNode` to ensure each stored graph
/// identifier is a syntactically valid absolute IRI.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GraphIri(String);

impl GraphIri {
    /// Parses and validates an IRI string with contextual error text.
    fn parse(value: &str, context: &str) -> anyhow::Result<Self> {
        let iri = NamedNode::new(value)
            .map_err(|e| anyhow::anyhow!("{context}: {value:?}: {e}"))?
            .into_string();
        Ok(Self(iri))
    }

    /// Derives a `file://` graph IRI from a filesystem path.
    ///
    /// Relative paths are resolved against the current working directory before
    /// conversion to a file URI.
    fn from_file_path(path: &Path) -> anyhow::Result<Self> {
        let absolute = absolute_path(path)?;
        let uri = Url::from_file_path(&absolute).map_err(|()| {
            anyhow::anyhow!("Failed to convert path {} to file URI", absolute.display())
        })?;
        Self::parse(uri.as_str(), "invalid file graph IRI")
    }

    /// Resolves graph IRI from HDT metadata when available, else from path.
    fn from_hdt_path(path: &Path) -> anyhow::Result<Self> {
        if let Some(iri) = hdt_meta::read_graph_iri_metadata(path)? {
            return Self::parse(&iri, "invalid graph IRI metadata");
        }
        Self::from_file_path(path)
    }

    fn into_string(self) -> String {
        self.0
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

/// Canonical graph mapping entry used by query planning and dataset assembly.
///
/// Invariants:
/// - `canonical_path` exists and is canonicalized.
/// - `graph_iri` is syntactically valid.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedGraphPath {
    graph_iri: GraphIri,
    canonical_path: PathBuf,
}

impl ResolvedGraphPath {
    /// Returns the resolved graph IRI.
    pub(crate) fn graph_iri(&self) -> &str {
        self.graph_iri.as_str()
    }

    /// Returns the canonicalized HDT path.
    #[cfg(feature = "server")]
    pub(crate) fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }

    /// Consumes the mapping and returns `(graph_iri, canonical_path)`.
    pub(crate) fn into_parts(self) -> (String, PathBuf) {
        (self.graph_iri.into_string(), self.canonical_path)
    }
}

/// Returns an absolute path, preserving already-absolute inputs.
fn absolute_path(path: &Path) -> anyhow::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

/// Canonicalizes a path and errors if the target does not exist.
fn canonicalize_existing_path(path: &Path, kind: &str) -> anyhow::Result<PathBuf> {
    if !path.exists() {
        return Err(anyhow::anyhow!("{kind} does not exist: {}", path.display()));
    }
    path.canonicalize()
        .map_err(|e| anyhow::anyhow!("Failed to canonicalize file path {}: {e}", path.display()))
}

/// Returns the deterministic file graph IRI for a path.
///
/// This does not require the file to exist.
pub(crate) fn file_graph_uri_for_path(path: &Path) -> anyhow::Result<String> {
    GraphIri::from_file_path(path).map(GraphIri::into_string)
}

/// Resolves an HDT path to its graph IRI and canonical path.
///
/// Resolution order for graph IRI:
/// 1. Graph IRI metadata embedded in the HDT.
/// 2. Fallback to file-URI-derived IRI from the canonical path.
pub(crate) fn resolve_hdt_graph_path(path: &Path) -> anyhow::Result<ResolvedGraphPath> {
    let canonical_path = canonicalize_existing_path(path, "HDT file")?;
    let graph_iri = GraphIri::from_hdt_path(&canonical_path)?;
    Ok(ResolvedGraphPath {
        graph_iri,
        canonical_path,
    })
}

/// Resolves a user-provided named graph IRI and HDT path.
///
/// Unlike [`resolve_hdt_graph_path`], this always uses the caller-provided IRI
/// and does not read HDT metadata for graph identity.
pub(crate) fn resolve_named_graph_path(
    graph_iri: &str,
    path: &Path,
) -> anyhow::Result<ResolvedGraphPath> {
    let canonical_path = canonicalize_existing_path(path, "HDT file")?;
    let graph_iri = GraphIri::parse(graph_iri, "invalid named graph IRI")?;
    Ok(ResolvedGraphPath {
        graph_iri,
        canonical_path,
    })
}

/// Inserts a resolved graph mapping while enforcing one-IRI-to-one-file.
///
/// Duplicate graph IRIs are accepted only when they point to the same
/// canonical path. If the same graph IRI appears for different files, an error
/// is returned.
pub(crate) fn insert_graph_mapping(
    file_paths: &mut HashMap<String, PathBuf>,
    resolved: ResolvedGraphPath,
    duplicate_label: &str,
) -> anyhow::Result<()> {
    let (graph_name, canonical_path) = resolved.into_parts();
    if let Some(existing) = file_paths.get(&graph_name) {
        if existing != &canonical_path {
            return Err(anyhow::anyhow!(
                "{duplicate_label} {graph_name} maps to multiple files: {} and {}",
                existing.display(),
                canonical_path.display()
            ));
        }
        return Ok(());
    }
    file_paths.insert(graph_name, canonical_path);
    Ok(())
}
