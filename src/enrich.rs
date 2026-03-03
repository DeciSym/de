// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use oxrdf::NamedNode;
use std::io::Write;

pub struct EnrichResult {
    pub preserve_source_as_blob: bool,
}

pub trait Enricher: Send + Sync {
    fn supported_extensions(&self) -> Vec<&str>;
    fn enrich(
        &self,
        file_path: &str,
        pkg_id: &NamedNode,
        output: &mut dyn Write,
    ) -> Result<EnrichResult, Box<dyn std::error::Error>>;
}
