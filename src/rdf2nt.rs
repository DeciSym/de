// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use log::{debug, error, warn};
use oxrdf::GraphName::DefaultGraph;
use oxrdf::TripleRef;
use oxrdfio::RdfFormat::{self, NTriples};
use oxrdfio::RdfSerializer;
use oxrdfio::{RdfParseError, RdfParser};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

/// Trait for different RDF libraries to implement for converting a list of files into NTriple RDF
/// returns stats on converted data via ConvertResult
pub trait Rdf2Nt {
    fn convert_to_nt(
        &self,
        file_paths: Vec<String>,
        output_file: &std::fs::File,
    ) -> anyhow::Result<ConvertResult>;
}

#[derive(Debug, Default)]
/// Object for returning stats of converted RDF files
pub struct ConvertResult {
    pub converted: i32,
    pub unhandled: Vec<String>,
}

/// Rdf2Nt implementation using oxrdf and oxrdfio crates
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
            let source = std::fs::File::open(file)
                .map_err(|e| anyhow::anyhow!("Error opening file {:?}: {:?}", file, e))?;
            let source_reader = BufReader::new(source);

            debug!("converting {} to nt format", &file);

            let mut serializer =
                RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
            let v = std::time::Instant::now();
            let rdf_format = match Path::new(&file)
                .extension()
                .and_then(|ext| ext.to_str())
                .and_then(RdfFormat::from_extension)
            {
                Some(format) => format,
                None if file.ends_with(".owl") => {
                    // OWL files should be in XML format: https://www.w3.org/TR/owl-xmlsyntax/
                    RdfFormat::RdfXml
                }
                None => {
                    res.unhandled.push(file.to_string());
                    continue;
                }
            };
            // TODO oxrdfio does offer split_file_for_parallel_parsing() which greatly improves performance, but only available for NT or NQ formats
            let quads = RdfParser::from_format(rdf_format).for_reader(source_reader);
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
                            res.unhandled.push(file.to_string());
                            break;
                        } else {
                            // based on file extension, should have been able to parse
                            error!("syntax error for RDF file {file}: {syn_err}");
                            return Err(anyhow::anyhow!(
                                "syntax error for RDF file {file}: {syn_err}"
                            ));
                        }
                    }
                };
                if q.graph_name != DefaultGraph {
                    warn!("HDT does not support named graphs, merging triples for {file}");
                }
                serializer.serialize_triple(TripleRef::new(
                    q.subject.as_ref(),
                    q.predicate.as_ref(),
                    q.object.as_ref(),
                ))?
            }

            serializer.finish()?;
            res.converted += 1;
            debug!("Convert time: {:?}", v.elapsed());
        }
        dest_writer.flush()?;
        Ok(res)
    }
}
