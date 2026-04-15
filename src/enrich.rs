// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use oxrdf::{NamedNode, Triple};

pub trait Enricher: Send + Sync {
    fn supported_extensions(&self) -> Vec<&str>;
    fn enrich(
        &self,
        file_path: &str,
        pkg_id: &NamedNode,
    ) -> Result<Vec<Triple>, Box<dyn std::error::Error>>;
}
