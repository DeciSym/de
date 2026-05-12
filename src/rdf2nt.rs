// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use bzip2::bufread::MultiBzDecoder;
use flate2::bufread::MultiGzDecoder;
use log::{debug, error, warn};
use oxrdf::GraphName::DefaultGraph;
use oxrdf::TripleRef;
use oxrdfio::RdfFormat::{self, NTriples};
use oxrdfio::RdfSerializer;
use oxrdfio::{RdfParseError, RdfParser};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use url::Url;

const IO_BUF: usize = 1 << 20;

/// Open `file` for reading, transparently decoding `.gz` and `.bz2` wrappers.
fn open_rdf_reader(file: &Path) -> io::Result<Box<dyn Read>> {
    let fp = File::open(file)?;
    let buffered = BufReader::with_capacity(IO_BUF, fp);
    match file.extension().and_then(|e| e.to_str()) {
        Some(e) if e.eq_ignore_ascii_case("gz") => Ok(Box::new(BufReader::with_capacity(
            IO_BUF,
            MultiGzDecoder::new(buffered),
        ))),
        Some(e) if e.eq_ignore_ascii_case("bz2") => Ok(Box::new(BufReader::with_capacity(
            IO_BUF,
            MultiBzDecoder::new(buffered),
        ))),
        _ => Ok(Box::new(buffered)),
    }
}

/// Strip a trailing `.gz`/`.bz2` so the inner RDF extension drives format detection.
fn format_path_for(file: &Path) -> PathBuf {
    match file.extension().and_then(|e| e.to_str()) {
        Some(e) if e.eq_ignore_ascii_case("gz") || e.eq_ignore_ascii_case("bz2") => {
            file.with_extension("")
        }
        _ => file.to_path_buf(),
    }
}

/// Trait for different RDF libraries to implement for converting a list of files into `NTriple` RDF
/// returns stats on converted data via `ConvertResult`.
///
/// `Send + Sync` bounds let `Arc<dyn Rdf2Nt>` live across await points inside
/// the async `files_to_rdf` dispatcher.
pub trait Rdf2Nt: Send + Sync {
    fn convert_to_nt(
        &self,
        file_paths: Vec<String>,
        output_file: &std::fs::File,
    ) -> anyhow::Result<ConvertResult>;
}

#[derive(Debug, Default)]
/// Object for returning stats of converted RDF files
pub struct ConvertResult {
    pub converted: usize,
    pub unhandled: Vec<String>,
    pub named_graphs: BTreeSet<String>,
    /// True if any quad in the default graph (`GraphName::DefaultGraph`) was
    /// observed. Tracked separately because the default graph has no IRI and
    /// therefore can't live in `named_graphs`. Consumers counting distinct
    /// graph regions (e.g. to gate a merge-into-single-HDT operation) should
    /// add `usize::from(has_default_graph_triples)` to `named_graphs.len()`.
    pub has_default_graph_triples: bool,
}

/// `Rdf2Nt` implementation using oxrdf and oxrdfio crates
pub struct OxRdfConvert {}

impl Rdf2Nt for OxRdfConvert {
    fn convert_to_nt(
        &self,
        file_paths: Vec<String>,
        output_file: &std::fs::File,
    ) -> anyhow::Result<ConvertResult> {
        let mut res = ConvertResult::default();
        let mut dest_writer = BufWriter::new(output_file);
        for file in &file_paths {
            let path = Path::new(file);
            let source_reader = open_rdf_reader(path)
                .map_err(|e| anyhow::anyhow!("Error opening file {file:?}: {e:?}"))?;

            debug!("converting {} to nt format", &file);

            let mut serializer =
                RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
            let v = std::time::Instant::now();
            let fmt_path = format_path_for(path);
            let fmt_ext = fmt_path.extension().and_then(|ext| ext.to_str());
            let rdf_format = if fmt_ext.is_some_and(|e| e.eq_ignore_ascii_case("owl")) {
                // OWL files should be in XML format: https://www.w3.org/TR/owl-xmlsyntax/
                RdfFormat::RdfXml
            } else if let Some(format) = fmt_ext.and_then(RdfFormat::from_extension) {
                format
            } else {
                res.unhandled.push(file.clone());
                continue;
            };
            let base_iri = Path::new(file)
                .canonicalize()
                .ok()
                .and_then(|path| Url::from_file_path(path).ok())
                .map(|url| url.to_string());
            // TODO oxrdfio does offer split_file_for_parallel_parsing() which greatly improves performance, but only available for NT or NQ formats
            let quads = if let Some(base_iri) = base_iri {
                match RdfParser::from_format(rdf_format).with_base_iri(&base_iri) {
                    Ok(parser) => parser.for_reader(source_reader),
                    Err(_) => RdfParser::from_format(rdf_format).for_reader(source_reader),
                }
            } else {
                RdfParser::from_format(rdf_format).for_reader(source_reader)
            };
            let mut warned_named_graph_merge = false;
            for q in quads {
                let q = match q {
                    Ok(v) => v,
                    Err(RdfParseError::Io(v)) => {
                        // I/O error while reading file
                        return Err(anyhow::anyhow!("Error reading file {file}: {v}"));
                    }
                    Err(RdfParseError::Syntax(syn_err)) => {
                        if rdf_format == RdfFormat::RdfXml {
                            // XML file extensions are not guaranteed to be RdfXML
                            res.unhandled.push(file.clone());
                            break;
                        }
                        // based on file extension, should have been able to parse
                        error!("syntax error for RDF file {file}: {syn_err}");
                        return Err(anyhow::anyhow!(
                            "syntax error for RDF file {file}: {syn_err}"
                        ));
                    }
                };
                if q.graph_name == DefaultGraph {
                    res.has_default_graph_triples = true;
                } else {
                    if !warned_named_graph_merge {
                        warn!("HDT does not support named graphs, merging triples for {file}");
                        warned_named_graph_merge = true;
                    }
                    res.named_graphs.insert(q.graph_name.to_string());
                }
                serializer.serialize_triple(TripleRef::new(
                    q.subject.as_ref(),
                    q.predicate.as_ref(),
                    q.object.as_ref(),
                ))?;
            }

            serializer.finish()?;
            res.converted += 1;
            debug!("Convert time: {:?}", v.elapsed());
        }
        dest_writer.flush()?;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{OxRdfConvert, Rdf2Nt};
    use std::fs;
    use std::io::Write;
    use tempfile::Builder;

    const APPLE_TTL: &str = "tests/resources/apple.ttl";
    const APPLE_TRIPLES: usize = 9;

    #[test]
    fn convert_to_nt_handles_relative_iris_in_paths_with_spaces() -> anyhow::Result<()> {
        let temp_dir = Builder::new().prefix("rdf2nt with spaces").tempdir()?;
        let input_path = temp_dir.path().join("input.ttl");
        fs::write(&input_path, "<s> <p> <o> .\n")?;

        let output = Builder::new().suffix(".nt").tempfile()?;
        let converter = OxRdfConvert {};
        let result = converter.convert_to_nt(
            vec![input_path.to_string_lossy().to_string()],
            output.as_file(),
        )?;

        assert_eq!(result.converted, 1);
        assert!(result.unhandled.is_empty());

        let output_data = fs::read_to_string(output.path())?;
        assert!(
            output_data.contains("file:///"),
            "expected output triples to resolve against a file:/// base IRI"
        );
        Ok(())
    }

    fn count_triples_in(nt_file: &tempfile::NamedTempFile) -> usize {
        let reader = BufReader::new(nt_file.reopen().expect("reopen tmp nt"));
        RdfParser::from_format(NTriples)
            .for_reader(reader)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse nt")
            .len()
    }

    #[test]
    fn gzipped_ttl_input() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let gz_path = tmp.path().join("apple.ttl.gz");
        let source = std::fs::read(APPLE_TTL)?;
        let mut enc =
            flate2::write::GzEncoder::new(File::create(&gz_path)?, flate2::Compression::default());
        enc.write_all(&source)?;
        enc.finish()?;

        let out = tempfile::Builder::new().suffix(".nt").tempfile()?;
        let res = OxRdfConvert {}
            .convert_to_nt(vec![gz_path.to_string_lossy().into_owned()], &out.reopen()?)?;
        assert_eq!(res.converted, 1);
        assert!(res.unhandled.is_empty());
        assert_eq!(count_triples_in(&out), APPLE_TRIPLES);
        Ok(())
    }

    #[test]
    fn bzipped_ttl_input() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let bz_path = tmp.path().join("apple.ttl.bz2");
        let source = std::fs::read(APPLE_TTL)?;
        let mut enc =
            bzip2::write::BzEncoder::new(File::create(&bz_path)?, bzip2::Compression::default());
        enc.write_all(&source)?;
        enc.finish()?;

        let out = tempfile::Builder::new().suffix(".nt").tempfile()?;
        let res = OxRdfConvert {}
            .convert_to_nt(vec![bz_path.to_string_lossy().into_owned()], &out.reopen()?)?;
        assert_eq!(res.converted, 1);
        assert!(res.unhandled.is_empty());
        assert_eq!(count_triples_in(&out), APPLE_TRIPLES);
        Ok(())
    }
}
