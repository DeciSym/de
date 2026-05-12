//! Cross-process cache-race integration test for the `de` CLI.
//!
//! This test repeatedly launches multiple `de query` processes against the same
//! HDT path after deleting sidecar cache files. It verifies robust behavior
//! under concurrent cache creation/reads across process boundaries.
//!
//! This is intentionally **not** the W3C rdf-tests runner.
//! The upstream W3C suite execution lives in `tests/w3c-sparql.rs`.

use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
};

fn remove_hdt_cache_files(hdt_path: &Path) -> anyhow::Result<()> {
    let parent = hdt_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("HDT path has no parent: {}", hdt_path.display()))?;
    let file_name = hdt_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid HDT file name: {}", hdt_path.display()))?;

    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let entry_name = entry.file_name();
        let entry_name = entry_name.to_string_lossy();
        let cache_prefix = format!("{file_name}.index.");
        if entry_name.starts_with(&cache_prefix) {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_parallel_processes_query_same_hdt_without_cache_races() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let nt_path = tmp.path().join("dataset.nt");
    let hdt_path = tmp.path().join("dataset.hdt");
    let query_path = tmp.path().join("query.rq");

    fs::write(
        &nt_path,
        "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
    )?;
    de::create::do_create(
        hdt_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid HDT path"))?,
        &[nt_path.to_string_lossy().to_string()],
    )
    .await?;
    fs::write(&query_path, "SELECT ?s WHERE { ?s ?p ?o }")?;

    let bin = env!("CARGO_BIN_EXE_de");
    let hdt = hdt_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid HDT path"))?;
    let query = query_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid query path"))?;

    // Repeatedly clear cache and run concurrent de processes against the same HDT path.
    // This stresses cache creation/reads under cross-process contention.
    for _round in 0..5 {
        remove_hdt_cache_files(&hdt_path)?;

        let mut children = Vec::new();
        for _ in 0..6 {
            let child = Command::new(bin)
                .args(["query", "-d", hdt, "-s", query, "-o", "csv"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            children.push(child);
        }

        for child in children {
            let output = child.wait_with_output()?;
            assert!(
                output.status.success(),
                "parallel de process failed.\nstatus: {status:?}\nstderr:\n{stderr}",
                status = output.status,
                stderr = String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                stdout.contains("http://example.org/s"),
                "parallel de process returned unexpected output.\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    Ok(())
}
