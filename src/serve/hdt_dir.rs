//! The HDT files in a directory, as a [`DatasetSource`]: what `de serve` serves.

use super::{
    dataset::{DatasetSelection, DatasetSource, ServeDataset},
    error::{HttpError, internal_server_error},
};
use crate::sparql::AggregateHdt;
use http::StatusCode;
use log::debug;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::SystemTime,
};

/// The `.hdt` files directly in a directory, served as one dataset.
///
/// Before each query it checks the files' paths, sizes and modification
/// times, and re-reads their graph IRIs when anything changed, so files
/// added, replaced or removed while the server runs are picked up.
pub struct HdtDirectory {
    store: AggregateHdt,
    location: PathBuf,
    /// The directory, canonicalized, and its files when last read.
    synced: Mutex<Option<(PathBuf, StoreFingerprint)>>,
}

impl HdtDirectory {
    /// Serves the `.hdt` files in `location`.
    pub fn new(location: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let location = location.into();
        let hdt_paths = collect_hdt_paths(&location)?;
        let store = if hdt_paths.is_empty() {
            AggregateHdt::empty()
        } else {
            AggregateHdt::new(&hdt_paths)?
        };
        Ok(Self::with_store(store, location))
    }

    /// Serves `store`, which holds the `.hdt` files in `location`, and
    /// re-reads it from there when they change.
    pub fn with_store(store: AggregateHdt, location: impl Into<PathBuf>) -> Self {
        Self {
            store,
            location: location.into(),
            synced: Mutex::new(None),
        }
    }
}

impl DatasetSource for HdtDirectory {
    fn refresh(&self) -> Result<(), HttpError> {
        let canonical_location = self
            .location
            .canonicalize()
            .map_err(|e| internal_server_error(format!("error resolving data location: {e}")))?;
        let fingerprint = compute_store_fingerprint(&canonical_location)?;
        {
            let guard = self
                .synced
                .lock()
                .map_err(|_| internal_server_error("sync cache lock poisoned"))?;
            if guard.as_ref().is_some_and(|(location, cached)| {
                *location == canonical_location && *cached == fingerprint
            }) {
                return Ok(());
            }
        }
        self.store
            .sync(&canonical_location)
            .map_err(|e| internal_server_error(format!("error loading data files: {e}")))?;
        let mut guard = self
            .synced
            .lock()
            .map_err(|_| internal_server_error("sync cache lock poisoned"))?;
        *guard = Some((canonical_location, fingerprint));
        Ok(())
    }

    fn open(&self, selection: &DatasetSelection) -> Result<Box<dyn ServeDataset + '_>, HttpError> {
        // Optimization: Filter graphs BEFORE loading into memory by passing an explicit list.
        // This significantly reduces memory usage and load time when only a subset of graphs are
        // needed for the query.
        let graph_filter = graph_filter(selection);
        if let Some(graph_filter_ref) = graph_filter.as_ref() {
            debug!("using graph filter for query: {graph_filter_ref:?}");
        } else {
            debug!("using default graph filter for query: all graphs");
        }
        let snapshot = self.store.get_snapshot(graph_filter).map_err(|_| {
            HttpError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "data temporarily unavailable",
            )
        })?;
        Ok(Box::new(snapshot))
    }
}

/// The graphs to open for a query, or `None` for all of them.
fn graph_filter(selection: &DatasetSelection) -> Option<Vec<String>> {
    let mut selected_graphs: Vec<String> = if selection.union_default_graph {
        let mut selected = selection.default_graph_uris.clone();
        selected.extend_from_slice(&selection.named_graph_uris);
        selected
    } else if selection.default_graph_uris.is_empty() {
        selection.named_graph_uris.clone()
    } else {
        selection.default_graph_uris.clone()
    };

    selected_graphs.sort_unstable();
    selected_graphs.dedup();

    if selected_graphs.is_empty() {
        None
    } else {
        Some(selected_graphs)
    }
}

#[derive(Clone, Eq, PartialEq)]
struct HdtFileFingerprint {
    path: PathBuf,
    modified: SystemTime,
    len: u64,
}

#[derive(Clone, Eq, PartialEq)]
struct StoreFingerprint {
    files: Vec<HdtFileFingerprint>,
}

pub(super) fn collect_hdt_paths(location: &Path) -> anyhow::Result<Vec<String>> {
    let mut hdt_paths: Vec<String> = std::fs::read_dir(location)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if !entry.file_type().ok()?.is_file() {
                return None;
            }
            let path = entry.path();
            let is_hdt = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("hdt"));
            if is_hdt {
                Some(path.to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect();
    hdt_paths.sort_unstable();
    hdt_paths.dedup();
    Ok(hdt_paths)
}

fn compute_store_fingerprint(location: &Path) -> Result<StoreFingerprint, HttpError> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(location)
        .map_err(|e| internal_server_error(format!("error reading data location: {e}")))?
    {
        let entry = entry.map_err(|e| {
            internal_server_error(format!("error while iterating data location: {e}"))
        })?;
        let path = entry.path();
        if !path
            .extension()
            .is_some_and(|extension| extension.to_string_lossy().eq_ignore_ascii_case("hdt"))
        {
            continue;
        }
        let metadata = entry.metadata().map_err(|e| {
            internal_server_error(format!(
                "error reading HDT metadata for {}: {e}",
                path.display()
            ))
        })?;
        let modified = metadata.modified().map_err(|e| {
            internal_server_error(format!(
                "error reading HDT mtime for {}: {e}",
                path.display()
            ))
        })?;
        files.push(HdtFileFingerprint {
            path,
            modified,
            len: metadata.len(),
        });
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(StoreFingerprint { files })
}
