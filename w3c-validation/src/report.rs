// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Report formatting and path helpers for the W3C harness run.

use super::{CaseStatus, PathBuf, w3c_resources_root};
use std::{collections::BTreeMap, fmt::Write as _, path::Path};

pub(super) fn write_report(
    report_path: &Path,
    rows: &[(String, String, String, CaseStatus)],
) -> anyhow::Result<()> {
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

    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut out = String::new();
    writeln!(&mut out, "W3C rdf-tests status report").ok();
    writeln!(&mut out, "suite_root: w3c-validation/rdf-tests").ok();
    writeln!(&mut out, "total: {}", rows.len()).ok();
    writeln!(&mut out, "pass: {total_pass}").ok();
    writeln!(&mut out, "fail: {total_fail}").ok();
    writeln!(&mut out, "skip: {total_skip}").ok();
    writeln!(&mut out, "unsupported: {total_unsupported}").ok();
    writeln!(&mut out).ok();
    writeln!(&mut out, "by_type:").ok();
    for (test_type, (pass, fail, skip, unsupported)) in &by_type {
        writeln!(
            &mut out,
            "  {test_type} => pass: {pass}, fail: {fail}, skip: {skip}, unsupported: {unsupported}"
        )
        .ok();
    }
    writeln!(&mut out).ok();
    writeln!(&mut out, "cases:").ok();
    for (id, test_type, manifest, status) in rows {
        match status {
            CaseStatus::Pass => {
                writeln!(&mut out, "PASS\t{test_type}\t{id}\t{manifest}").ok();
            }
            CaseStatus::Fail(reason) => {
                writeln!(&mut out, "FAIL\t{test_type}\t{id}\t{manifest}\t{reason}").ok();
            }
            CaseStatus::Unsupported(reason) => {
                writeln!(
                    &mut out,
                    "UNSUPPORTED\t{test_type}\t{id}\t{manifest}\t{reason}"
                )
                .ok();
            }
        }
    }

    std::fs::write(report_path, out)?;
    Ok(())
}

/// Formats an absolute path as a report-friendly relative form, stripping the
/// vendored W3C resources root when possible. Falls back to the absolute path
/// if the input doesn't sit under that tree.
pub(super) fn path_for_report(path: &Path) -> String {
    let root = w3c_resources_root();
    if let Ok(rel) = path.strip_prefix(&root) {
        return Path::new("w3c-validation/rdf-tests")
            .join(rel)
            .to_string_lossy()
            .into_owned();
    }
    path.to_string_lossy().into_owned()
}

/// Formats a path for CLI invocation relative to the current working directory.
pub(super) fn path_for_cli(path: &Path) -> String {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    path.strip_prefix(&cwd)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}
