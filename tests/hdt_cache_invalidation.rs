// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Cache-sidecar invalidation coverage for HDT (re-)creation.
//!
//! `de::create::write_hdt_to_path` deletes every `<name>.index.*` sidecar
//! before truncating and rewriting the HDT it is about to replace. Without
//! that, a re-created HDT can be served through an index built for the
//! previous contents: the `hdt` crate's own staleness check is size + mtime
//! based, so a rewrite landing on the same size within the filesystem's mtime
//! granularity still looks valid to it.
//!
//! These tests pin both halves of the contract — the sidecars are gone
//! immediately after a re-create, and a query that follows sees the new data —
//! for both shapes of output path the CLI accepts.

use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::Command;

use de::{create, query};

/// Two datasets whose N-Triples serializations are byte-identical in length:
/// same triple count, same IRI lengths, differing only in characters. That is
/// the adversarial shape for a size-based staleness check.
const FRUIT_A: &str = concat!(
    "<http://example.org/Banana> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Fruit> .\n",
    "<http://example.org/Banana> <http://example.org/hasColor> \"yellow\" .\n",
);
const FRUIT_B: &str = concat!(
    "<http://example.org/Cherry> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Fruit> .\n",
    "<http://example.org/Cherry> <http://example.org/hasColor> \"yellow\" .\n",
);

const FRUIT_QUERY: &str = r#"PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?fruit WHERE { ?fruit rdf:type ex:Fruit ; ex:hasColor "yellow" . }
"#;

const BANANA_CSV: &str = "fruit\nhttp://example.org/Banana";
const CHERRY_CSV: &str = "fruit\nhttp://example.org/Cherry";

/// Names of the `<file_name>.index.*` sidecars currently sitting next to
/// `hdt_path`, sorted for stable assertion output.
fn cache_sidecars(hdt_path: &Path) -> anyhow::Result<Vec<String>> {
    let parent = hdt_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = hdt_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid HDT file name: {}", hdt_path.display()))?;
    let prefix = format!("{file_name}.index.");

    let mut found: Vec<String> = fs::read_dir(parent)?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .filter(|name| name.starts_with(&prefix))
        .collect();
    found.sort();
    Ok(found)
}

fn no_sidecars() -> Vec<String> {
    Vec::new()
}

async fn query_csv(hdt_path: &Path, query_path: &Path) -> anyhow::Result<String> {
    let data_files = vec![hdt_path.to_string_lossy().into_owned()];
    let query_files = vec![query_path.to_string_lossy().into_owned()];
    let mut writer = BufWriter::new(Vec::new());
    query::do_query(
        &data_files,
        &query_files,
        query::EntailmentMode::Off,
        &query::DeOutput::CSV,
        &mut writer,
    )
    .await?;
    writer.flush()?;
    let buffer = writer.into_inner()?;
    Ok(String::from_utf8_lossy(&buffer)
        .replace('\r', "")
        .trim()
        .to_string())
}

#[tokio::test]
async fn recreating_hdt_clears_every_stale_cache_sidecar() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    let hdt_path = dir.join("fruit.hdt");
    let query_path = dir.join("fruit.rq");
    let nt_a = dir.join("a.nt");
    let nt_b = dir.join("b.nt");

    fs::write(&nt_a, FRUIT_A)?;
    fs::write(&nt_b, FRUIT_B)?;
    fs::write(&query_path, FRUIT_QUERY)?;

    let hdt_arg = hdt_path.to_string_lossy().into_owned();

    create::do_create(&hdt_arg, &[nt_a.to_string_lossy().into_owned()]).await?;
    assert_eq!(
        cache_sidecars(&hdt_path)?,
        no_sidecars(),
        "create does not pre-warm, so it should not leave a sidecar behind"
    );

    // Query so the wavelet-tree sidecar actually lands on disk; `create`
    // alone never builds one.
    assert_eq!(query_csv(&hdt_path, &query_path).await?, BANANA_CSV);
    assert!(
        !cache_sidecars(&hdt_path)?.is_empty(),
        "querying an HDT should build at least one index sidecar"
    );

    // Sidecar layouts have changed across `hdt` releases and older ones linger
    // in real working directories (see tests/resources/apple.hdt.index.*), so
    // the cleanup has to key on the `.index.` prefix, not one known suffix.
    for name in ["fruit.hdt.index.v1-1", "fruit.hdt.index.v1-rust-cache"] {
        fs::write(dir.join(name), b"stale")?;
    }

    // Neighbours that merely look similar must survive the cleanup.
    let bystanders = [
        dir.join("other.hdt.index.v5-rust-cache"),
        dir.join("fruit.hdt.meta"),
    ];
    for path in &bystanders {
        fs::write(path, b"keep me")?;
    }

    create::do_create(&hdt_arg, &[nt_b.to_string_lossy().into_owned()]).await?;

    assert_eq!(
        cache_sidecars(&hdt_path)?,
        no_sidecars(),
        "re-creating an HDT must clear every stale .index.* sidecar"
    );
    for path in &bystanders {
        assert!(
            path.exists(),
            "cleanup removed an unrelated file: {}",
            path.display()
        );
    }

    // The end-to-end guarantee the cleanup exists to protect.
    assert_eq!(
        query_csv(&hdt_path, &query_path).await?,
        CHERRY_CSV,
        "query after re-create was served stale data"
    );

    tmp.close()?;
    Ok(())
}

/// Regression test for the bare-file-name output path. `Path::parent` reports
/// `Some("")` for `fruit.hdt`, so a cleanup that skips empty parents silently
/// does nothing for `de create -o fruit.hdt` — the normal way the tool is
/// driven from the directory holding the data.
#[test]
fn recreating_hdt_named_by_bare_relative_path_clears_stale_cache_sidecars() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    fs::write(dir.join("a.nt"), FRUIT_A)?;
    fs::write(dir.join("b.nt"), FRUIT_B)?;
    fs::write(dir.join("fruit.rq"), FRUIT_QUERY)?;

    // Driving the CLI in a child process keeps the working-directory change
    // out of this test process, which is shared with every other test in the
    // binary.
    let de = env!("CARGO_BIN_EXE_de");
    let run = |args: &[&str]| -> anyhow::Result<String> {
        let output = Command::new(de).current_dir(dir).args(args).output()?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "de {} failed with status {:?}\nstderr:\n{}",
                args.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .replace('\r', "")
            .trim()
            .to_string())
    };

    let hdt_path = dir.join("fruit.hdt");
    let query_args = ["query", "-d", "fruit.hdt", "-s", "fruit.rq", "-o", "csv"];

    run(&["create", "-o", "fruit.hdt", "-d", "a.nt"])?;
    assert_eq!(run(&query_args)?, BANANA_CSV);
    assert!(
        !cache_sidecars(&hdt_path)?.is_empty(),
        "querying an HDT should build at least one index sidecar"
    );

    run(&["create", "-o", "fruit.hdt", "-d", "b.nt"])?;
    assert_eq!(
        cache_sidecars(&hdt_path)?,
        no_sidecars(),
        "re-create with a bare relative -o must clear stale .index.* sidecars"
    );

    assert_eq!(
        run(&query_args)?,
        CHERRY_CSV,
        "query after re-create was served stale data"
    );

    tmp.close()?;
    Ok(())
}
