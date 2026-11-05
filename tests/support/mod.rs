// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Helpers shared by the per-subcommand integration tests (`tests/test-<subcommand>.rs`).
//!
//! Each test file is its own crate, so a helper one of them doesn't use would be dead code
//! there; hence the `allow`.

#![allow(dead_code)]

use std::{fs::OpenOptions, io::BufWriter, path::Path, process::Output};

pub fn devnull_writer() -> std::io::Result<BufWriter<std::fs::File>> {
    let path = if cfg!(windows) { "NUL" } else { "/dev/null" };
    Ok(BufWriter::new(OpenOptions::new().write(true).open(path)?))
}

// Helper to create a writer that captures output for testing
pub fn create_test_writer() -> BufWriter<Vec<u8>> {
    BufWriter::new(Vec::new())
}

// Helper to extract captured output from test writer
pub fn get_output_from_writer(mut writer: BufWriter<Vec<u8>>) -> std::io::Result<String> {
    use std::io::Write;
    writer.flush()?;
    let buffer = writer.into_inner()?;
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

pub fn run_de_command(cwd: &Path, args: &[&str]) -> anyhow::Result<Output> {
    std::process::Command::new(env!("CARGO_BIN_EXE_de"))
        .current_dir(cwd)
        .args(args)
        .output()
        .map_err(|e| anyhow::anyhow!("failed to execute de {}: {e}", args.join(" ")))
}

pub fn run_de_ok(cwd: &Path, args: &[&str]) -> anyhow::Result<()> {
    let output = run_de_command(cwd, args)?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "de {} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

pub fn run_de_ok_stdout(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = run_de_command(cwd, args)?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "de {} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .replace('\r', "")
        .trim()
        .to_string())
}
