// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::enrich::{EnrichCtx, EnrichOutcome, EnrichResult, Enricher};
use crate::rdf2nt::ConvertResult;
use crate::rdf2nt::OxRdfConvert;
use crate::rdf2nt::Rdf2Nt;
use anyhow::Context;
use log::*;
use oxrdf::NamedNode;
use std::fs::{self, File, OpenOptions};
use std::io::{copy, BufReader, BufWriter, Write};
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
    pub rdf_path: String,
    pub unhandled_files: Vec<String>,
    /// Source files that were handled by an enricher. Callers can apply their
    /// own policy (e.g. preserve as a blob alongside the extracted triples).
    pub enriched_sources: Vec<String>,
}

/// Creates a HDT file from RDF source
pub async fn do_create(hdt_name: &str, data: &[String]) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    debug!("Creating HDT...");
    // creating a tempfile to hold all the contents of the rdf input files
    let mut tmp_file = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile()
        .map_err(|e| anyhow::anyhow!("Error creating temporary file: {:?}", e))?;

    // No enrichers are wired in this path, so the file_id closure is never
    // invoked. A panic closure documents that expectation at the type level.
    let file_id_fn: FileIdFn = &|_| unreachable!("no enrichers registered");
    let result = files_to_rdf(
        data,
        &mut tmp_file,
        Arc::new(OxRdfConvert {}),
        &[],
        None,
        file_id_fn,
    )
    .await?;
    if !result.unhandled_files.is_empty() {
        for f in &result.unhandled_files {
            if !Path::new(f).exists() {
                error!("file {f:?} could not be found on local machine");
            }
        }
        error!(
            "unable to convert the following files: {:?}",
            result.unhandled_files
        );
        error!("check 'de create --help' for list of supported file types");
        return Err(anyhow::anyhow!(
            "unsupported files detected: {:?}",
            result.unhandled_files
        ));
    }

    let new_hdt = hdt::Hdt::read_nt(Path::new(&result.rdf_path))
        .map_err(|e| anyhow::anyhow!("Error converting combined RDF to HDT: {e}"))?;

    let out_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(hdt_name)?;
    let mut writer = BufWriter::new(out_file);
    new_hdt.write(&mut writer)?;
    writer.flush()?;

    let _ = fs::remove_file(tmp_file.path());

    if !Path::new(hdt_name).exists() {
        return Err(anyhow::anyhow!(
            "failed to create HDT in requested location {hdt_name}"
        ));
    }
    // Prints location of HDT assuming HDT is generated
    debug!("HDT file created at {hdt_name}");
    Ok(new_hdt)
}

/// Converts a list of RDF files to NTriple RDF
/// returns the name of the file containing combined NTriple RDF, the names of any unhandled files,
/// and any files that should be preserved as blobs.
///
/// `file_id_fn` is invoked exactly once per file that matches an enricher, so
/// the produced `NamedNode` can be reused inside the enricher without
/// re-hashing the file contents.
pub async fn files_to_rdf(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
    enrichers: &[Box<dyn Enricher>],
    root_id: Option<&NamedNode>,
    file_id_fn: FileIdFn<'_>,
) -> anyhow::Result<FilesToRdfResult> {
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

    for file in data.iter() {
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
        else if file.ends_with(".nt") {
            debug!("Adding RDF triples to graph");
            nt_files.push(file.clone());
        } else {
            files_to_convert.push(file.clone());
        }
    }

    let conv_res = if !files_to_convert.is_empty() {
        let r = converter
            .convert_to_nt(files_to_convert, out_file.as_file())
            .context("error converting file(s) to NT")?;
        unrecognized_files.extend(r.unhandled.clone());
        r
    } else {
        ConvertResult::default()
    };

    // optimization attempt. If only one NTriple file provided don't do an additional file copy otherwise
    // inefficient when creating an HDT file from one large file
    if nt_files.len() > 1 || conv_res.converted != 0 || !enriched_sources.is_empty() {
        for nt_file in nt_files {
            let source =
                File::open(&nt_file).with_context(|| format!("error opening file {nt_file:?}"))?;
            let mut source_reader = BufReader::new(source);

            copy(&mut source_reader, out_file)
                .with_context(|| format!("error copying file {nt_file:?}"))?;
        }
    } else if nt_files.len() == 1 && conv_res.converted == 0 {
        return Ok(FilesToRdfResult {
            rdf_path: nt_files[0].clone(),
            unhandled_files: unrecognized_files,
            enriched_sources,
        });
    }

    Ok(FilesToRdfResult {
        rdf_path: out_file
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in temp file path"))?
            .to_string(),
        unhandled_files: unrecognized_files,
        enriched_sources,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::{EnrichCtx, EnrichError, EnrichOutcome, EnrichResult, Enricher};
    use async_trait::async_trait;
    use oxrdf::{NamedNode, Triple};
    use tempfile::Builder;

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
        let file_id_fn: FileIdFn = &test_file_id;

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            Some(&root_id),
            file_id_fn,
        )
        .await
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        let contents = std::fs::read_to_string(result.rdf_path).unwrap();
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
        let file_id_fn: FileIdFn = &|_| unreachable!("no enrichers registered");

        let result = files_to_rdf(
            std::slice::from_ref(&nt_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &[],
            None,
            file_id_fn,
        )
        .await
        .unwrap();

        assert!(result.unhandled_files.is_empty());
        assert!(result.enriched_sources.is_empty());
        // single NT file optimization: rdf_path should be the original file
        assert_eq!(result.rdf_path, nt_path);
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
        let file_id_fn: FileIdFn = &test_file_id;

        let result = files_to_rdf(
            &[mock_path.clone(), nt_path],
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            Some(&root_id),
            file_id_fn,
        )
        .await
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        let contents = std::fs::read_to_string(result.rdf_path).unwrap();
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
        let file_id_fn: FileIdFn = &test_file_id;

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
        )
        .await
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        assert!(result.unhandled_files.is_empty());
        let contents = std::fs::read_to_string(result.rdf_path).unwrap();
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
        let file_id_fn: FileIdFn = &test_file_id;

        let err = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
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
        let file_id_fn: FileIdFn = &test_file_id;

        let err = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
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
            msg.contains("0") && msg.contains("1"),
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
        let file_id_fn: FileIdFn = &test_file_id;

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            None,
            file_id_fn,
        )
        .await
        .unwrap();

        assert!(result.enriched_sources.is_empty());
        assert_eq!(result.unhandled_files, vec![mock_path]);
    }
}
