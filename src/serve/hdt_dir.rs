//! The HDT files in a directory, as a [`DatasetSource`]: what `de serve` serves.

use super::{
    dataset::{DatasetSelection, DatasetSource, ServeDataset},
    error::{HttpError, internal_server_error},
};
use crate::sparql::AggregateHdt;
use http::StatusCode;
use log::debug;
use std::{
    collections::HashSet,
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
        let explicit =
            !selection.default_graph_uris.is_empty() || !selection.named_graph_uris.is_empty();
        // Only the graphs the dataset names are opened, which saves memory
        // and time when a request needs a few of many.
        let (load, default_graphs, named_graphs) = if explicit {
            let mut load: Vec<String> = selection
                .default_graph_uris
                .iter()
                .chain(&selection.named_graph_uris)
                .cloned()
                .collect();
            load.sort_unstable();
            load.dedup();
            let default_graphs = selection.default_graph_uris.iter().cloned().collect();
            let named_graphs = selection.named_graph_uris.iter().cloned().collect();
            (Some(load), Some(default_graphs), Some(named_graphs))
        } else if selection.union_default_graph {
            (None, None, None)
        } else {
            (None, Some(HashSet::new()), None)
        };
        debug!(
            "dataset for query: load {load:?}, default graph {default_graphs:?}, named graphs {named_graphs:?}"
        );
        let mut snapshot = self.store.get_snapshot(load).map_err(|_| {
            HttpError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "data temporarily unavailable",
            )
        })?;
        snapshot.default_graphs = default_graphs;
        snapshot.named_graphs = named_graphs;
        Ok(Box::new(snapshot))
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
