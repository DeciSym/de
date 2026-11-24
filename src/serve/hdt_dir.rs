//! The HDT files in the data directory, and noticing when they change.

use super::error::{HttpError, internal_server_error};
use crate::sparql::AggregateHdt;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::SystemTime,
};

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

static STORE_SYNC_CACHE: OnceLock<Mutex<HashMap<PathBuf, StoreFingerprint>>> = OnceLock::new();

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

pub(super) fn maybe_sync_store(store: &AggregateHdt, location: &Path) -> Result<(), HttpError> {
    let canonical_location = location
        .canonicalize()
        .map_err(|e| internal_server_error(format!("error resolving data location: {e}")))?;
    let fingerprint = compute_store_fingerprint(&canonical_location)?;

    let cache = STORE_SYNC_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let guard = cache
            .lock()
            .map_err(|_| internal_server_error("sync cache lock poisoned"))?;
        if guard
            .get(&canonical_location)
            .is_some_and(|cached| *cached == fingerprint)
        {
            return Ok(());
        }
    }

    store
        .sync(&canonical_location)
        .map_err(|e| internal_server_error(format!("error loading data files: {e}")))?;

    let mut guard = cache
        .lock()
        .map_err(|_| internal_server_error("sync cache lock poisoned"))?;
    guard.insert(canonical_location, fingerprint);
    Ok(())
}
