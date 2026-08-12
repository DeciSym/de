// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use std::path::Path;

pub mod create;
pub mod enrich;
mod graph_iri;
pub mod hdt_meta;
#[cfg(feature = "server")]
pub mod mcp;
pub mod query;
pub mod rdf2nt;
#[cfg(feature = "server")]
pub mod serve;
#[cfg(feature = "server")]
pub mod service_description;
pub mod sparql;
pub mod view;

pub fn file_graph_uri_for_path(path: &Path) -> anyhow::Result<String> {
    graph_iri::file_graph_uri_for_path(path)
}
