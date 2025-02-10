use log::{debug, warn};
use oxigraph::io::RdfFormat::*;
use oxigraph::io::RdfParser;
use oxigraph::io::RdfSerializer;
use oxigraph::model::TripleRef;
use oxrdf::GraphName::DefaultGraph;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;

pub trait Rdf2Nt {
    fn convert_to_nt(
        &self,
        file_paths: Vec<String>,
        out_file: std::fs::File,
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
        out_file: std::fs::File,
    ) -> anyhow::Result<ConvertResult, anyhow::Error> {
        let mut res = ConvertResult::default();
        let mut dest_writer = BufWriter::new(out_file);
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
            let rdf_format = if file.ends_with(".nq") {
                NQuads
            } else if file.ends_with(".ttl") {
                Turtle
            } else if file.ends_with(".n3") {
                N3
            } else if file.ends_with(".xml") {
                RdfXml
            } else if file.ends_with(".trig") {
                TriG
            } else {
                res.unhandled.push(file.clone());
                continue;
            };
            let quads = RdfParser::from_format(rdf_format)
                .for_reader(source_reader)
                .collect::<Result<Vec<_>, _>>()?;
            for q in quads.iter() {
                if q.graph_name.to_string() != DefaultGraph.to_string() {
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
