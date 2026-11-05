// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Helpers shared by the per-subcommand integration tests (`tests/test-<subcommand>.rs`).
//!
//! Each test file is its own crate, so a helper one of them doesn't use would be dead code
//! there; hence the `allow`.

#![allow(dead_code)]

use oxrdfio::{RdfFormat, RdfParser};
use std::collections::BTreeSet;
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

/// Runs `de` with `TMPDIR` set to `tmpdir`, so a test can check what it leaves behind.
pub fn run_de_with_tmpdir(cwd: &Path, tmpdir: &Path, args: &[&str]) -> anyhow::Result<Output> {
    std::process::Command::new(env!("CARGO_BIN_EXE_de"))
        .current_dir(cwd)
        .env("TMPDIR", tmpdir)
        .args(args)
        .output()
        .map_err(|e| anyhow::anyhow!("failed to execute de {}: {e}", args.join(" ")))
}

/// Runs `de` expecting it to fail, and returns its stderr.
pub fn run_de_err(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = run_de_command(cwd, args)?;
    if output.status.success() {
        return Err(anyhow::anyhow!(
            "de {} succeeded but was expected to fail\nstdout:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stderr).to_string())
}

/// Entries left in a temporary directory, apart from the cache lock directory
/// that hdt keeps there on purpose so concurrent processes share it.
pub fn leftover_temp_files(dir: &Path) -> anyhow::Result<Vec<String>> {
    let mut left = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name().to_string_lossy().to_string();
        if name != "hdt-hybrid-cache-locks" {
            left.push(name);
        }
    }
    left.sort();
    Ok(left)
}

/// Parses RDF into a set of N-Triples statements, so that two graphs can be
/// compared whatever their serialization or statement order.
pub fn triple_set(rdf: &[u8], format: RdfFormat) -> anyhow::Result<BTreeSet<String>> {
    let mut triples = BTreeSet::new();
    for quad in RdfParser::from_format(format).for_reader(rdf) {
        let quad = quad?;
        triples.insert(format!(
            "{} {} {} .",
            quad.subject, quad.predicate, quad.object
        ));
    }
    Ok(triples)
}

/// The triples of `tests/resources/banana.nt`, the reference the format tests compare against.
pub fn banana_triples() -> anyhow::Result<BTreeSet<String>> {
    triple_set(
        &std::fs::read("tests/resources/banana.nt")?,
        RdfFormat::NTriples,
    )
}

/// A committed expected result from `tests/goldens/`, with line endings normalized.
pub fn golden(name: &str) -> anyhow::Result<String> {
    Ok(std::fs::read_to_string(format!("tests/goldens/{name}"))?
        .replace('\r', "")
        .trim()
        .to_string())
}
