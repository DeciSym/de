// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::enrich::Enricher;
use crate::rdf2nt::ConvertResult;
use crate::rdf2nt::OxRdfConvert;
use crate::rdf2nt::Rdf2Nt;
use log::*;
use oxrdf::NamedNode;
use std::fs::{self, File, OpenOptions};
use std::io::{copy, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile};

pub struct FilesToRdfResult {
    pub rdf_path: String,
    pub unhandled_files: Vec<String>,
    /// Source files that were handled by an enricher. Callers can apply their
    /// own policy (e.g. preserve as a blob alongside the extracted triples).
    pub enriched_sources: Vec<String>,
}

/// Creates a HDT file from RDF source
pub fn do_create(hdt_name: &str, data: &[String]) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    debug!("Creating HDT...");
    // creating a tempfile to hold all the contents of the rdf input files
    let mut tmp_file = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile()
        .map_err(|e| anyhow::anyhow!("Error creating temporary file: {:?}", e))?;

    let result = files_to_rdf(data, &mut tmp_file, Arc::new(OxRdfConvert {}), &[], None)?;
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
/// and any files that should be preserved as blobs
pub fn files_to_rdf(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
    enrichers: &[Box<dyn Enricher>],
    pkg_id: Option<&NamedNode>,
) -> anyhow::Result<FilesToRdfResult> {
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
            if let Some(id) = pkg_id {
                debug!("Enriching file: {file}");
                let triples = enricher
                    .enrich(file, id)
                    .map_err(|e| anyhow::anyhow!("Error enriching file {file}: {e}"))?;
                if triples.is_empty() {
                    // Enricher declined — let the generic converter handle the file.
                    debug!("Enricher produced no triples for {file}, routing to converter");
                    files_to_convert.push(file.clone());
                } else {
                    for triple in &triples {
                        writeln!(out_file, "{triple} .").map_err(|e| {
                            anyhow::anyhow!("Error writing enriched triples for {file}: {e}")
                        })?;
                    }
                    enriched_sources.push(file.clone());
                }
            } else {
                warn!("Enricher matched for {file} but no pkg_id provided, skipping enrichment");
                unrecognized_files.push(file.clone());
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
            .map_err(|e| anyhow::anyhow!("Error converting file(s) to NT: {e}"))?;
        unrecognized_files.extend(r.unhandled.clone());
        r
    } else {
        ConvertResult::default()
    };

    // optimization attempt. If only one NTriple file provided don't do an additional file copy otherwise
    // inefficient when creating an HDT file from one large file
    if nt_files.len() > 1 || conv_res.converted != 0 || !enriched_sources.is_empty() {
        for nt_file in nt_files {
            let source = File::open(&nt_file)
                .map_err(|e| anyhow::anyhow!("Error opening file {:?}: {:?}", nt_file, e))?;
            let mut source_reader = BufReader::new(source);

            copy(&mut source_reader, out_file)
                .map_err(|e| anyhow::anyhow!("Error copying file {:?}: {:?}", &nt_file, e))?;
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
    use crate::enrich::Enricher;
    use oxrdf::{NamedNode, Triple};
    use tempfile::Builder;

    struct MockEnricher;

    impl Enricher for MockEnricher {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        fn enrich(
            &self,
            _file_path: &str,
            _pkg_id: &NamedNode,
        ) -> Result<Vec<Triple>, Box<dyn std::error::Error>> {
            Ok(vec![Triple::new(
                NamedNode::new("http://example.org/mock-subject")?,
                NamedNode::new("http://example.org/type")?,
                NamedNode::new("http://example.org/Mock")?,
            )])
        }
    }

    #[test]
    fn test_files_to_rdf_with_mock_enricher() {
        let tmp = Builder::new().suffix(".mock").tempfile().unwrap();
        let mock_path = tmp.path().to_str().unwrap().to_string();

        let mut out_file = Builder::new()
            .suffix(".nt")
            .append(true)
            .tempfile()
            .unwrap();
        let pkg_id = NamedNode::new("http://example.org/pkg1").unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(MockEnricher)];

        let result = files_to_rdf(
            std::slice::from_ref(&mock_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            Some(&pkg_id),
        )
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        let contents = std::fs::read_to_string(result.rdf_path).unwrap();
        assert!(contents.contains("<http://example.org/Mock>"));
    }

    #[test]
    fn test_files_to_rdf_empty_enrichers_backward_compat() {
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

        let result = files_to_rdf(
            std::slice::from_ref(&nt_path),
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &[],
            None,
        )
        .unwrap();

        assert!(result.unhandled_files.is_empty());
        assert!(result.enriched_sources.is_empty());
        // single NT file optimization: rdf_path should be the original file
        assert_eq!(result.rdf_path, nt_path);
    }

    #[test]
    fn test_files_to_rdf_mixed_enricher_and_rdf() {
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
        let pkg_id = NamedNode::new("http://example.org/pkg1").unwrap();
        let enrichers: Vec<Box<dyn Enricher>> = vec![Box::new(MockEnricher)];

        let result = files_to_rdf(
            &[mock_path.clone(), nt_path],
            &mut out_file,
            Arc::new(OxRdfConvert {}),
            &enrichers,
            Some(&pkg_id),
        )
        .unwrap();

        assert_eq!(result.enriched_sources, vec![mock_path]);
        let contents = std::fs::read_to_string(result.rdf_path).unwrap();
        assert!(contents.contains("<http://example.org/Mock>"));
        assert!(contents.contains("<http://example.org/o>"));
    }
}
