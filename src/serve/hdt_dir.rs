//! The HDT files in a directory, as a [`DatasetSource`]: what `de serve` serves.

use super::{
    dataset::{DatasetSelection, DatasetSource, ServeDataset},
    error::{HttpError, internal_server_error},
};
use crate::sparql::{AggregateHdt, hdt_files};
use http::StatusCode;
use log::debug;
use oxrdf::NamedNode;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Mutex,
    time::SystemTime,
};

/// The `.hdt` files directly in a directory, or in its subdirectories too
/// with [`with_subdirectories`](Self::with_subdirectories), served as one
/// dataset.
///
/// Before each query it checks the files' paths, sizes and modification
/// times, and re-reads their graph IRIs when anything changed, so files
/// added, replaced or removed while the server runs are picked up.
pub struct HdtDirectory {
    store: AggregateHdt,
    location: PathBuf,
    recursive: bool,
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
            recursive: false,
            synced: Mutex::new(None),
        }
    }

    /// The store it serves from, as last refreshed. Call
    /// [`refresh`](DatasetSource::refresh) first to see the directory as it
    /// is now, such as after writing a file into it.
    #[must_use]
    pub fn store(&self) -> &AggregateHdt {
        &self.store
    }

    /// Also serves the `.hdt` files in the directory's subdirectories, at
    /// any depth. They are read before the first query.
    #[must_use]
    pub fn with_subdirectories(mut self) -> Self {
        self.recursive = true;
        self
    }
}

impl DatasetSource for HdtDirectory {
    fn refresh(&self) -> Result<(), HttpError> {
        let canonical_location = self
            .location
            .canonicalize()
            .map_err(|e| internal_server_error(format!("error resolving data location: {e}")))?;
        let fingerprint = compute_store_fingerprint(&canonical_location, self.recursive)?;
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
        let synced = if self.recursive {
            self.store.sync_recursive(&canonical_location)
        } else {
            self.store.sync(&canonical_location)
        };
        synced.map_err(|e| internal_server_error(format!("error loading data files: {e}")))?;
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

    /// Looks the graph up in the files' graph IRIs, read at the last
    /// refresh, without opening any file.
    fn contains_graph(&self, graph: &NamedNode) -> Result<bool, HttpError> {
        self.store
            .contains_graph_name(graph.as_str())
            .map_err(|e| internal_server_error(format!("looking for the graph {graph}: {e}")))
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
    Ok(hdt_files(location, false)?
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect())
}

fn compute_store_fingerprint(
    location: &Path,
    recursive: bool,
) -> Result<StoreFingerprint, HttpError> {
    let paths = hdt_files(location, recursive)
        .map_err(|e| internal_server_error(format!("error reading data location: {e}")))?;
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = std::fs::metadata(&path).map_err(|e| {
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
    Ok(StoreFingerprint { files })
}
