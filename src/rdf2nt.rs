use log::{debug, error, warn};
use oxigraph::io::RdfFormat::{self, NTriples};
use oxigraph::io::RdfSerializer;
use oxigraph::io::{RdfParseError, RdfParser};
use oxigraph::model::GraphName::DefaultGraph;
use oxigraph::model::TripleRef;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

pub trait Rdf2Nt {
    fn convert_to_nt(
        &self,
        file_paths: Vec<String>,
        output_file: &std::fs::File,
    ) -> anyhow::Result<ConvertResult, anyhow::Error>;
}

#[derive(Debug, Default)]
pub struct ConvertResult {
    pub converted: i32,
    pub unhandled: Vec<String>,
}

pub struct OxRdfConvert {}

impl Rdf2Nt for OxRdfConvert {
    fn convert_to_nt(
        &self,
        file_paths: Vec<String>,
        output_file: &std::fs::File,
    ) -> anyhow::Result<ConvertResult, anyhow::Error> {
        let mut res = ConvertResult::default();
        let mut dest_writer = BufWriter::new(output_file);
        for file in file_paths {
            let source = match std::fs::File::open(&file) {
                Ok(f) => f,
                Err(e) => return Err(anyhow::anyhow!("Error opening file {:?}: {:?}", file, e)),
            };
            let source_reader = BufReader::new(source);

            debug!("converting {} to nt format", &file);

            let mut serializer =
                RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
            let v = std::time::Instant::now();
            let rdf_format = if let Some(t) =
                RdfFormat::from_extension(Path::new(&file).extension().unwrap().to_str().unwrap())
            {
                t
            } else if file.ends_with(".owl") {
                // OWL files should be in XML format: https://www.w3.org/TR/owl-xmlsyntax/
                RdfFormat::RdfXml
            } else {
                res.unhandled.push(file.clone());
                continue;
            };
            let quads = RdfParser::from_format(rdf_format).for_reader(source_reader);
            for q in quads {
                let q = match q {
                    Ok(v) => v,
                    Err(e) => {
                        match e {
                            RdfParseError::Io(v) => {
                                // I/O error while reading file
                                return Err(anyhow::anyhow!("Error reading file {file}: {v}"));
                            }
                            RdfParseError::Syntax(syn_err) => {
                                if rdf_format == RdfFormat::RdfXml {
                                    // XML file extensions are not guaranteed to be RdfXML
                                    res.unhandled.push(file.clone());
                                    continue;
                                } else {
                                    // based on file extension, should have been able to parse
                                    error!("syntax error for RDF file {file}: {syn_err}");
                                    return Err(anyhow::anyhow!(
                                        "syntax error for RDF file {file}: {syn_err}"
                                    ));
                                }
                            }
                        }
                    }
                };
                if q.graph_name != DefaultGraph {
                    warn!("HDT does not support named graphs, merging triples for {file}");
                }
                serializer.serialize_triple(TripleRef {
                    subject: q.subject.as_ref(),
                    predicate: q.predicate.as_ref(),
                    object: q.object.as_ref(),
                })?
            }

            serializer.finish()?;
            res.converted += 1;
            debug!("Convert time: {:?}", v.elapsed());
        }
        dest_writer.flush()?;
        Ok(res)
    }
}
