// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Integration tests for `de view`.

#![allow(
    clippy::uninlined_format_args,
    clippy::single_char_pattern,
    clippy::needless_raw_string_hashes,
    clippy::wildcard_imports
)]

mod support;

use de::*;
use std::path::Path;
use support::*;
use tempfile::tempdir;

#[tokio::test]
async fn test_view() -> anyhow::Result<()> {
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ));
        }
    };
    let new_hdt = format!("{}/rdf.hdt", tmp_dir.as_ref().display());

    assert!(
        create::do_create(&new_hdt.clone(), &["tests/resources/apple.ttl".to_string()],)
            .await
            .is_ok()
    );
    assert!(Path::new(&new_hdt).exists());

    assert!(view::view_hdt(&[new_hdt], &mut devnull_writer()?).is_ok());

    tmp_dir.close()?;
    Ok(())
}
