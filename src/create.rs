// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::enrich::{EnrichCtx, EnrichOutcome, EnrichResult, Enricher};
use crate::hdt_meta;
use crate::rdf2nt::ConvertResult;
use crate::rdf2nt::OxRdfConvert;
use crate::rdf2nt::Rdf2Nt;
use anyhow::Context;
use log::{debug, error};
use oxrdf::NamedNode;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write, copy};
use std::path::Path;
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile};

/// Closure supplied by the caller that computes a stable `NamedNode`
/// identifier for a given file path (typically a content-hash IRI). Passed
/// into `files_to_rdf` so the enricher dispatch layer can hash each file
/// exactly once and share the result with every enricher.
///
/// `Send + Sync` bounds let the closure live across await points when
/// `files_to_rdf` is awaited from a multi-threaded Tokio runtime.
pub type FileIdFn<'a> = &'a (dyn Fn(&str) -> EnrichResult<NamedNode> + Send + Sync);
#[derive(Debug)]
pub struct FilesToRdfResult {
    pub combined_rdf_path: String,
    pub unknown_files: Vec<String>,
    pub named_graphs: Vec<String>,
    /// Source files that were handled by an enricher. Callers can apply their
    /// own policy (e.g. preserve as a blob alongside the extracted triples).
    pub enriched_sources: Vec<String>,
}

/// Creates a HDT file from RDF source
pub async fn do_create(hdt_name: &str, data: &[String]) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    do_create_with_options(Some(hdt_name), data, false, None, &[], None, None).await
}

/// Creates a HDT file from RDF source with explicit control over named graph merging.
///
/// `file_id_fn` is required when `enrichers` is non-empty (it computes the
/// per-file `NamedNode` the enrichers consume) and **must be** `None` when
/// `enrichers` is empty — `files_to_rdf` rejects the inconsistent
/// combination upfront rather than letting it manifest as a runtime panic
/// in a closure that "shouldn't ever be called."
pub async fn do_create_with_options(
    hdt_name: Option<&str>,
    data: &[String],
    allow_merge_named_graphs: bool,
    graph_iri: Option<&str>,
    enrichers: &[Box<dyn Enricher>],
    root_id: Option<&NamedNode>,
    file_id_fn: Option<FileIdFn<'_>>,
) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    debug!("Creating HDT...");
    // creating a tempfile to hold all the contents of the rdf input files
    let mut tmp_file = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile()
        .map_err(|e| anyhow::anyhow!("Error creating temporary file: {e:?}"))?;

    let rdf_result = files_to_rdf(
        data,
        &mut tmp_file,
        Arc::new(OxRdfConvert {}),
        enrichers,
        root_id,
        file_id_fn,
        allow_merge_named_graphs,
    )
    .await?;
    if !rdf_result.unknown_files.is_empty() {
        for f in &rdf_result.unknown_files {
            if !Path::new(f).exists() {
                error!("file {f:?} could not be found on local machine");
            }
        }
        error!(
            "unable to convert the following files: {:?}",
            rdf_result.unknown_files
        );
        error!("check 'de create --help' for list of supported file types");
        return Err(anyhow::anyhow!(
            "unsupported files detected: {:?}",
            rdf_result.unknown_files
        ));
    }
    let mut new_hdt = read_nt_hdt_safe(Path::new(&rdf_result.combined_rdf_path))?;
    if let Some(new_hdt_file) = hdt_name {
        if let Some(graph_iri) = graph_iri {
            hdt_meta::set_graph_iri_metadata_in_hdt(&mut new_hdt, graph_iri)?;
        }
        write_hdt_to_path(&new_hdt, Path::new(new_hdt_file))?;
    }
    let _ = fs::remove_file(tmp_file.path());

    Ok(new_hdt)
}

/// Write `hdt` to `path`, removing any stale `<name>.index.*` cache sidecars
/// before truncating the HDT itself. Exposed publicly so consumers that
/// build an HDT in memory (e.g. via `do_create_with_options(None, ...)`),
/// modify it (e.g. `header_mut`, custom triples), and then want to persist
/// can do so with the same on-disk semantics this crate uses internally.
///
/// The HDT crate does its own staleness check on load (size/mtime), but
/// eager cleanup keeps the on-disk state consistent and avoids rare cases
/// where mtime granularity causes a stale cache to be treated as valid
/// for a freshly-overwritten HDT.
pub fn write_hdt_to_path(hdt: &hdt::Hdt, path: &Path) -> anyhow::Result<()> {
    if let (Some(parent), Some(file_name)) = (
        path.parent().filter(|&p| !p.as_os_str().is_empty()),
        path.file_name().and_then(|n| n.to_str()),
    ) {
        let stale_prefix = format!("{file_name}.index.");
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                if let Some(entry_name) = entry.file_name().to_str()
                    && entry_name.starts_with(&stale_prefix)
                {
                    debug!("Removing stale HDT cache sidecar: {entry_name}");
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }

    let out_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let mut writer = BufWriter::new(out_file);
    hdt.write(&mut writer)?;
    writer.flush()?;
    if !path.exists() {
        return Err(anyhow::anyhow!(
            "failed to create HDT in requested location {}",
            path.display()
        ));
    }
    debug!("HDT file created at {}", path.display());
    Ok(())
}

fn read_nt_hdt_safe(path: &Path) -> anyhow::Result<hdt::Hdt> {
    hdt::Hdt::read_nt(path)
        .map_err(|e| anyhow::anyhow!("Error converting combined RDF to HDT: {e}"))
}

/// Convert an N-Triples file at `nt` into an HDT file at `dest`.
///
/// The upstream `hdt` crate's `read_nt` can panic on certain malformed
/// inputs (the W3C suite has cases shaped like
/// `bits_per_entry == 0`); this helper wraps the call in `catch_unwind`
/// and reports the panic payload as an `anyhow::Error` instead of
/// unwinding into the caller. Stale `<dest>.index.*` cache sidecars are
/// removed before write (handled inside [`write_hdt_to_path`]).
///
/// When `prewarm` is true, the resulting HDT is immediately opened
/// through [`hdt::HdtAny::open_with_threshold`] — the same dispatch the
/// query path uses — so the wavelet-tree cache (`<dest>.index.v1-1`)
/// gets built at conversion time rather than at the first query. Leave
/// it `false` when the next consumer is `AggregateHdt::new`/`get_snapshot`,
/// which already opens via the same dispatch and would just rebuild the
/// cache redundantly.
pub fn nt_file_to_hdt(nt: &Path, dest: &Path, prewarm: bool) -> anyhow::Result<()> {
    let hdt_conversion =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hdt::Hdt::read_nt(nt)));
    let hdt = match hdt_conversion {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => {
            return Err(anyhow::anyhow!(
                "error converting NT file {} to HDT: {e}",
                nt.display()
            ));
        }
        Err(panic_payload) => {
            let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                *s
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.as_str()
            } else {
                "unknown panic while reading RDF"
            };
            return Err(anyhow::anyhow!(
                "panic converting NT file {} to HDT: {panic_msg}",
                nt.display()
            ));
        }
    };

    write_hdt_to_path(&hdt, dest)?;

    if prewarm {
        hdt::HdtAny::open_with_threshold(dest, None).map_err(|e| {
            anyhow::anyhow!("failed to pre-warm HDT cache for {}: {e}", dest.display())
        })?;
    }
    Ok(())
}

fn ensure_nt_line_boundary(out_file: &NamedTempFile) -> anyhow::Result<()> {
    let out_len = out_file
        .as_file()
        .metadata()
        .map_err(|e| anyhow::anyhow!("Error inspecting temporary RDF file: {e}"))?
        .len();
    if out_len == 0 {
        return Ok(());
    }

    let mut probe = File::open(out_file.path())
        .map_err(|e| anyhow::anyhow!("Error opening temporary RDF file for boundary check: {e}"))?;
    probe
        .seek(SeekFrom::End(-1))
        .map_err(|e| anyhow::anyhow!("Error seeking temporary RDF file for boundary check: {e}"))?;
    let mut last_byte = [0_u8; 1];
    probe
        .read_exact(&mut last_byte)
        .map_err(|e| anyhow::anyhow!("Error reading temporary RDF file for boundary check: {e}"))?;

    if !matches!(last_byte[0], b'\n' | b'\r') {
        let mut out = out_file.as_file();
        out.write_all(b"\n").map_err(|e| {
            anyhow::anyhow!("Error writing newline separator to temporary RDF file: {e}")
        })?;
    }
    Ok(())
}

/// Converts a list of RDF files to `NTriple` RDF
/// returns the name of the file containing combined `NTriple` RDF, the names of any unhandled files,
/// and any files that should be preserved as blobs.
///
/// `file_id_fn` is invoked exactly once per file that matches an enricher, so
/// the produced `NamedNode` can be reused inside the enricher without
/// re-hashing the file contents. It must be `Some(_)` whenever `enrichers`
/// is non-empty; `files_to_rdf` returns `Err` upfront on the inconsistent
/// combination rather than letting an enricher dispatch run into a missing
/// id-generator at the bottom of the loop.
#[allow(clippy::too_many_lines)]
pub async fn files_to_rdf(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
    enrichers: &[Box<dyn Enricher>],
    root_id: Option<&NamedNode>,
    file_id_fn: Option<FileIdFn<'_>>,
    allow_merge_named_graphs: bool,
) -> anyhow::Result<FilesToRdfResult, anyhow::Error> {
    // Pair invariant: enrichers and file_id_fn must agree. If an enricher
    // is registered, the dispatch path needs an id-generator; if no
    // enricher is registered, no id-generator should be passed. Catching
    // this here (rather than via an `unreachable!()` closure at the call
    // site) means a future caller that adds an enricher without wiring an
    // id-generator gets a real error instead of a panic.
    if !enrichers.is_empty() && file_id_fn.is_none() {
        return Err(anyhow::anyhow!(
            "files_to_rdf: enrichers were registered but no file_id_fn was provided"
        ));
    }
    if enrichers.is_empty() && file_id_fn.is_some() {
        return Err(anyhow::anyhow!(
            "files_to_rdf: file_id_fn was provided but no enrichers are registered"
        ));
    }
    // Reject ambiguous enricher configurations up front: a single extension
    // claimed by more than one enricher is a caller bug. The previous
    // Vec-order-wins dispatch silently masked this — here we fail explicitly
    // so a misconfigured default set can't ship undetected.
    let mut claimed: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (idx, enricher) in enrichers.iter().enumerate() {
        for ext in enricher.supported_extensions() {
            if let Some(prev_idx) = claimed.insert(ext, idx) {
                return Err(anyhow::anyhow!(
                    "ambiguous enricher configuration: extension \"{ext}\" \
                     claimed by enrichers at positions {prev_idx} and {idx}"
                ));
            }
        }
    }
    let mut nt_files = vec![];
    let mut files_to_convert = vec![];
    let mut unrecognized_files = vec![];
    let mut enriched_sources: Vec<String> = vec![];

    for file in data {
        let path = Path::new(&file);
        if !path.exists() {
            unrecognized_files.push(file.clone());
            continue;
        }

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        // Check if any enricher handles this extension
        let matched_enricher = enrichers
            .iter()
            .find(|e| e.supported_extensions().contains(&ext));

        if let Some(enricher) = matched_enricher {
            debug!("Enriching file: {file}");
            // Safe `expect` — the enricher/file_id_fn invariant was checked
            // upfront, so reaching this branch with `enrichers` non-empty
            // implies `file_id_fn` is `Some(_)`.
            let file_id_fn = file_id_fn
                .expect("enricher/file_id_fn invariant: non-empty enrichers require file_id_fn");
            let file_id =
                file_id_fn(file).with_context(|| format!("error computing file id for {file}"))?;
            let ctx = EnrichCtx {
                file_path: file,
                file_id: &file_id,
                root_id,
            };
            let outcome = enricher
                .enrich(&ctx)
                .await
                .with_context(|| format!("error enriching file {file}"))?;
            match outcome {
                EnrichOutcome::Declined => {
                    debug!("Enricher declined {file}, routing to converter");
                    files_to_convert.push(file.clone());
                }
                EnrichOutcome::Triples(triples) => {
                    for triple in &triples {
                        writeln!(out_file, "{triple} .").with_context(|| {
                            format!("error writing enriched triples for {file}")
                        })?;
                    }
                    enriched_sources.push(file.clone());
                }
            }
        }
        // Check for triples, this is the preferred RDF format and no additional conversion is required
        else if Path::new(file)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("nt"))
        {
            debug!("Adding RDF triples to graph");
            nt_files.push(file.clone());
        } else {
            files_to_convert.push(file.clone());
        }
    }

    let conv_res = if files_to_convert.is_empty() {
        ConvertResult::default()
    } else {
        let r = converter
            .convert_to_nt(files_to_convert, out_file.as_file())
            .map_err(|e| anyhow::anyhow!("Error converting file(s) to NT: {e}"))?;
        unrecognized_files.extend(r.unhandled.iter().cloned());
        r
    };

    let have_enriched_output = !enriched_sources.is_empty();

    // Gate the multi-graph merge before the orchestrator commits to building
    // a combined output. Distinct graph regions = each named graph in the
    // converted slice + the default graph if any input contributes to it.
    // Default-graph contributions come from three sources:
    //   - converted formats with default-graph quads (`conv_res.has_default_graph_triples`)
    //   - `.nt` direct-copy inputs (N-Triples has no graph context)
    //   - enricher outputs (`Triple`, not `Quad` — implicitly default-graph)
    // The `convert_to_nt` trait stays observational; the policy decision
    // lives here because this is the earliest point with full visibility
    // across all three input categories.
    let has_default_graph_triples =
        conv_res.has_default_graph_triples || !nt_files.is_empty() || have_enriched_output;
    let distinct_regions = conv_res.named_graphs.len() + usize::from(has_default_graph_triples);
    if !allow_merge_named_graphs && distinct_regions > 1 {
        let mut regions: Vec<String> = conv_res.named_graphs.iter().cloned().collect();
        regions.sort_unstable();
        if has_default_graph_triples {
            regions.push("<default graph>".to_string());
        }
        return Err(anyhow::anyhow!(
            "multiple graphs detected during create ({regions:?}). HDT output is single-graph. \
Use --allow-merge-named-graphs to explicitly merge these graphs into the output graph."
        ));
    }

    let combined_rdf_path = if nt_files.len() > 1 || conv_res.converted != 0 || have_enriched_output
    {
        for nt_file in nt_files {
            ensure_nt_line_boundary(out_file)?;
            let source = File::open(&nt_file)
                .map_err(|e| anyhow::anyhow!("Error opening file {nt_file:?}: {e:?}"))?;
            let mut source_reader = BufReader::new(source);

            copy(&mut source_reader, out_file)
                .with_context(|| format!("error copying file {nt_file:?}"))?;
        }
        out_file
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in temp file path"))?
            .to_string()
    } else if nt_files.len() == 1 && conv_res.converted == 0 {
        nt_files[0].clone()
    } else {
        out_file
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in temp file path"))?
            .to_string()
    };

    let mut named_graphs: Vec<String> = conv_res.named_graphs.into_iter().collect();
    named_graphs.sort_unstable();

    Ok(FilesToRdfResult {
        combined_rdf_path,
        unknown_files: unrecognized_files,
        named_graphs,
        enriched_sources,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{do_create, do_create_with_options, files_to_rdf};
    use crate::enrich::{EnrichCtx, EnrichError, EnrichOutcome, EnrichResult, Enricher};
    use crate::hdt_meta;
    use async_trait::async_trait;
    use futures::FutureExt;
    use oxrdf::{NamedNode, Triple};
    use std::fs::{self, write};
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use tempfile::Builder;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_fails_when_multiple_named_graphs_are_merged_without_override()
    -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("multi.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> <http://example.org/g1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g2> .\n",
        )?;

        let result = do_create(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
        )
        .await;
        assert!(result.is_err());
        let msg = result.expect_err("expected error").to_string();
        assert!(msg.contains("--allow-merge-named-graphs"));
        Ok(())
    }

    #[tokio::test]
    async fn create_allows_multiple_named_graph_merge_with_override() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("multi.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> <http://example.org/g1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g2> .\n",
        )?;

        let result = do_create_with_options(
            Some(
                out_hdt
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            ),
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            true,
            None,
            &[],
            None,
            None,
        )
        .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn create_fails_when_named_graph_and_default_graph_are_merged_without_override()
    -> anyhow::Result<()> {
        // One named graph + at least one default-graph quad is still a merge
        // into a single-graph HDT output and should require the override,
        // even though `named_graphs.len() == 1`. Regression guard for the
        // case the previous `> 1` check missed.
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("default_plus_named.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g> .\n",
        )?;

        let result = do_create(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
        )
        .await;
        assert!(result.is_err());
        let msg = result.expect_err("expected error").to_string();
        assert!(msg.contains("--allow-merge-named-graphs"));
        assert!(msg.contains("<default graph>"));
        Ok(())
    }

    #[tokio::test]
    async fn create_allows_named_graph_and_default_graph_merge_with_override() -> anyhow::Result<()>
    {
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("default_plus_named.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g> .\n",
        )?;

        let result = do_create_with_options(
            Some(
                out_hdt
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            ),
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            true,
            None,
            &[],
            None,
            None,
        )
        .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn create_succeeds_with_only_default_graph_quads() -> anyhow::Result<()> {
        // Default-graph-only input is not a merge — no override needed.
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("default_only.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> .\n",
        )?;

        let result = do_create(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
        )
        .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn create_succeeds_with_only_one_named_graph() -> anyhow::Result<()> {
        // A single named graph with no default-graph activity is not a merge;
        // no override should be required.
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("single_named.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> <http://example.org/g> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g> .\n",
        )?;

        let result = do_create(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
        )
        .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn create_writes_graph_iri_metadata_when_provided() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("single.nt");
        let out_hdt = tmp.path().join("out.hdt");
        let graph_iri = "http://example.org/g";
        write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        do_create_with_options(
            Some(
                out_hdt
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            ),
            &[nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            false,
            Some(graph_iri),
            &[],
            None,
            None,
        )
        .await?;

        let found = hdt_meta::read_graph_iri_metadata(&out_hdt)?;
        assert_eq!(found.as_deref(), Some(graph_iri));
        Ok(())
    }

    #[tokio::test]
    async fn create_rejects_invalid_graph_iri_metadata() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("single.nt");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        let result = do_create_with_options(
            Some(
                out_hdt
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            ),
            &[nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            false,
            Some("not an iri"),
            &[],
            None,
            None,
        )
        .await;
        assert!(result.is_err());
        let msg = result.expect_err("expected invalid graph IRI").to_string();
        assert!(msg.contains("invalid graph IRI metadata"));
        Ok(())
    }

    #[tokio::test]
    async fn create_invalid_nt_returns_error_without_panic() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let bad_nt = tmp.path().join("bad.nt");
        let out_hdt = tmp.path().join("out.hdt");
        write(&bad_nt, "invalid triple")?;

        let out_hdt_s = out_hdt.to_string_lossy().to_string();
        let data = vec![bad_nt.to_string_lossy().to_string()];
        let result = AssertUnwindSafe(do_create(&out_hdt_s, &data))
            .catch_unwind()
            .await;
        assert!(
            result.is_ok(),
            "do_create should return Err instead of panicking"
        );
        let create_result = result.expect("catch_unwind should not fail");
        assert!(create_result.is_err(), "invalid NT should return error");
        Ok(())
    }

    #[tokio::test]
    async fn create_multiple_nt_without_trailing_newline_is_handled() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let first_nt = tmp.path().join("first.nt");
        let second_nt = tmp.path().join("second.nt");
        let out_hdt = tmp.path().join("out.hdt");

        // Intentionally omit trailing newline in first file to validate separator handling.
        write(
            &first_nt,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .",
        )?;
        write(
            &second_nt,
            "<http://example.org/s2> <http://example.org/p> <http://example.org/o2> .\n",
        )?;

        let out_hdt_s = out_hdt.to_string_lossy().to_string();
        let data = vec![
            first_nt.to_string_lossy().to_string(),
            second_nt.to_string_lossy().to_string(),
        ];
        let result = AssertUnwindSafe(do_create(&out_hdt_s, &data))
            .catch_unwind()
            .await;
        assert!(
            result.is_ok(),
            "do_create should not panic on valid NT inputs"
        );
        let create_result = result.expect("catch_unwind should not fail");
        assert!(
            create_result.is_ok(),
            "valid multi-file NT create should succeed"
        );
        assert!(
            fs::metadata(out_hdt).is_ok(),
            "output HDT should be created"
        );
        Ok(())
    }

    #[tokio::test]
    async fn files_to_rdf_single_nt_reuses_input_path() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("single.nt");
        write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        let mut out_file = tempfile::Builder::new().suffix(".nt").tempfile()?;
        let root_id = NamedNode::new("http://example.org/pkg1").unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(MockEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);
        let result = files_to_rdf(
            &[nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            &mut out_file,
            Arc::new(crate::rdf2nt::OxRdfConvert {}),
            &enrichers,
            Some(&root_id),
            file_id_fn,
            true,
        )
        .await?;

        assert!(result.unknown_files.is_empty());
        assert_eq!(
            result.combined_rdf_path,
            nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
        );
        assert_eq!(out_file.as_file().metadata()?.len(), 0);
        Ok(())
    }

    struct MockEnricher;

    #[async_trait]
    impl Enricher for MockEnricher {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        async fn enrich(&self, _ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            Ok(EnrichOutcome::Triples(vec![Triple::new(
                NamedNode::new("http://example.org/mock-subject")?,
                NamedNode::new("http://example.org/type")?,
                NamedNode::new("http://example.org/Mock")?,
            )]))
        }
    }

    /// Returns a deterministic per-path id for tests, so the same path always
    /// maps to the same `NamedNode`.
    fn test_file_id(path: &str) -> EnrichResult<NamedNode> {
        Ok(NamedNode::new(format!("http://example.org/file/{path}"))?)
    }

    #[tokio::test]
    async fn test_files_to_rdf_with_mock_enricher() {
        let tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        let mock_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let root_id = NamedNode::new("http://example.org/pkg1").unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(MockEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            Some(&root_id),
            file_id_fn,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        let contents = std::fs::read_to_string(result.combined_rdf_path).unwrap();
        assert!(contents.contains("<http://example.org/Mock>"));
    }

    #[tokio::test]
    async fn test_files_to_rdf_empty_enrichers_backward_compat() {
        let tmp = Builder::new().suffix(".nt").tempfile().unwrap();
        std::fs::write(
            tmp.path(),
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )
        .unwrap();
        let nt_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let file_id_fn: Option<FileIdFn> = None;

        let result = files_to_rdf(
            std::slice::from_ref(&nt_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &[],
            None,
            file_id_fn,
            true,
        )
        .await
        .unwrap();

        assert!(result.unknown_files.is_empty());
        assert!(result.enriched_sources.is_empty());
        // single NT file optimization: combined_rdf_path should be the original file
        assert_eq!(result.combined_rdf_path, nt_path);
    }

    #[tokio::test]
    async fn test_files_to_rdf_mixed_enricher_and_rdf() {
        // Create a .mock file
        let mock_tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        let mock_path = mock_tmp.path().to_str().unwrap().to_string();

        // Create an .nt file
        let nt_tmp = Builder::new().suffix(".nt").tempfile().unwrap();
        std::fs::write(
            nt_tmp.path(),
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )
        .unwrap();
        let nt_path = nt_tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let root_id = NamedNode::new("http://example.org/pkg1").unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(MockEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);

        let result = files_to_rdf(
            &[mock_path.clone(), nt_path],
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            Some(&root_id),
            file_id_fn,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        let contents = std::fs::read_to_string(result.combined_rdf_path).unwrap();
        assert!(contents.contains("<http://example.org/Mock>"));
        assert!(contents.contains("<http://example.org/o>"));
    }

    #[tokio::test]
    async fn test_files_to_rdf_enricher_without_root_id() {
        let tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        let mock_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(MockEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        assert!(result.unknown_files.is_empty());
        let contents = std::fs::read_to_string(result.combined_rdf_path).unwrap();
        assert!(contents.contains("<http://example.org/Mock>"));
    }

    struct DeclineEnricher;

    #[async_trait]
    impl Enricher for DeclineEnricher {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        async fn enrich(&self, _ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            Ok(EnrichOutcome::Declined)
        }
    }

    struct FailingParseEnricher;

    #[async_trait]
    impl Enricher for FailingParseEnricher {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            Err(EnrichError::parse(ctx.file_path, "synthetic parse failure"))
        }
    }

    #[tokio::test]
    async fn test_files_to_rdf_preserves_enricher_error_source_chain() {
        // Regression test for the stringification anti-pattern. The dispatcher
        // must wrap `EnrichError` as the *source* of the returned
        // `anyhow::Error` (not flatten it into the message), so callers can
        // downcast to recover the typed variant.
        let tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        let mock_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(FailingParseEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);

        let err = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
            true,
        )
        .await
        .expect_err("FailingParseEnricher should propagate an error");

        // The outer anyhow context must mention the file path.
        assert!(
            err.to_string().contains(&mock_path),
            "context message missing file path: {err}"
        );

        // Crucial: the underlying EnrichError must be reachable via downcast.
        let source = err
            .chain()
            .find_map(|e| e.downcast_ref::<EnrichError>())
            .expect("EnrichError must survive as a source, not be stringified");
        assert!(
            matches!(source, EnrichError::Parse { .. }),
            "expected EnrichError::Parse variant, got {source:?}"
        );
    }

    #[tokio::test]
    async fn test_files_to_rdf_rejects_duplicate_extension_claims() {
        // Two enrichers both claim ".mock". Dispatch must refuse to proceed
        // rather than let Vec ordering silently pick a winner.
        let tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        let mock_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let enrichers: Vec<Box<dyn Enricher>> =
            vec![Box::new(MockEnricher), Box::new(DeclineEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);

        let err = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
            true,
        )
        .await
        .expect_err("duplicate extension claim must be rejected");

        let msg = err.to_string();
        assert!(
            msg.contains("ambiguous"),
            "error should flag ambiguity: {msg}"
        );
        assert!(
            msg.contains("mock"),
            "error should name the conflicting extension: {msg}"
        );
        assert!(
            msg.contains('0') && msg.contains('1'),
            "error should point at both enricher positions: {msg}"
        );
    }

    #[tokio::test]
    async fn test_files_to_rdf_enricher_declines_falls_through_to_converter() {
        // Write an NT-shaped body into a .mock file. Because the enricher
        // declines, the file is passed to the generic converter, which here
        // is `OxRdfConvert`. That converter doesn't handle `.mock`, so the
        // file lands in `unhandled_files` — demonstrating that decline does
        // route to the fallback path.
        let tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        std::fs::write(
            tmp.path(),
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )
        .unwrap();
        let mock_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(DeclineEnricher)];
        let file_id_fn: Option<FileIdFn> = Some(&test_file_id);

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
            true,
        )
        .await
        .unwrap();

        assert!(result.enriched_sources.is_empty());
        assert_eq!(result.unknown_files, vec![mock_path]);
    }
}
