// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Report formatting and path helpers for the W3C harness run.

use super::*;
use std::{collections::BTreeMap, fmt::Write as _, path::Path};

pub(super) fn write_report(rows: &[(String, String, String, CaseStatus)]) -> anyhow::Result<()> {
    let mut by_type = BTreeMap::<String, (usize, usize, usize, usize)>::new();
    let mut total_pass = 0usize;
    let mut total_fail = 0usize;
    let total_skip = 0usize;
    let mut total_unsupported = 0usize;

    for (_, test_type, _, status) in rows {
        let entry = by_type.entry(test_type.clone()).or_insert((0, 0, 0, 0));
        match status {
            CaseStatus::Pass => {
                entry.0 += 1;
                total_pass += 1;
            }
            CaseStatus::Fail(_) => {
                entry.1 += 1;
                total_fail += 1;
            }
            CaseStatus::Unsupported(_) => {
                entry.3 += 1;
                total_unsupported += 1;
            }
        }
    }

    let report_path = report_output_path();
    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut out = String::new();
    writeln!(&mut out, "W3C rdf-tests status report").ok();
    writeln!(&mut out, "suite_root: tests/resources/rdf-tests").ok();
    writeln!(&mut out, "total: {}", rows.len()).ok();
    writeln!(&mut out, "pass: {}", total_pass).ok();
    writeln!(&mut out, "fail: {}", total_fail).ok();
    writeln!(&mut out, "skip: {}", total_skip).ok();
    writeln!(&mut out, "unsupported: {}", total_unsupported).ok();
    writeln!(&mut out).ok();
    writeln!(&mut out, "by_type:").ok();
    for (test_type, (pass, fail, skip, unsupported)) in &by_type {
        writeln!(
            &mut out,
            "  {} => pass: {}, fail: {}, skip: {}, unsupported: {}",
            test_type, pass, fail, skip, unsupported
        )
        .ok();
    }
    writeln!(&mut out).ok();
    writeln!(&mut out, "cases:").ok();
    for (id, test_type, manifest, status) in rows {
        match status {
            CaseStatus::Pass => {
                writeln!(&mut out, "PASS\t{}\t{}\t{}", test_type, id, manifest).ok();
            }
            CaseStatus::Fail(reason) => {
                writeln!(
                    &mut out,
                    "FAIL\t{}\t{}\t{}\t{}",
                    test_type, id, manifest, reason
                )
                .ok();
            }
            CaseStatus::Unsupported(reason) => {
                writeln!(
                    &mut out,
                    "UNSUPPORTED\t{}\t{}\t{}\t{}",
                    test_type, id, manifest, reason
                )
                .ok();
            }
        }
    }

    std::fs::write(&report_path, out)?;
    Ok(())
}

/// Report destination path under `target/`.
pub(super) fn report_output_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("target/w3c-rdf-tests-report.txt")
}

/// Formats an absolute path relative to the crate root for stable report output.
pub(super) fn path_for_report(path: &Path) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// Formats a path for CLI invocation relative to the current working directory.
pub(super) fn path_for_cli(path: &Path) -> String {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    path.strip_prefix(&cwd)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}
