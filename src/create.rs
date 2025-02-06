// This file handles the create subcommand

use crate::rdf2hdt::Rdf2Hdt;
use log::*;
use oxigraph::io::RdfFormat::*;
use oxigraph::io::RdfParser;
use oxigraph::io::RdfSerializer;
use oxigraph::model::TripleRef;
use oxrdf::GraphName::DefaultGraph;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::io::{copy, BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, Builder, NamedTempFile};

pub fn do_create(
    hdt_name: &str,
    data: &Vec<String>,
    r2h: Arc<dyn Rdf2Hdt>,
) -> anyhow::Result<String, anyhow::Error> {
    debug!("Creating HDT...");
    // Creating a tempdir to be passed to panoplia as the directory.
    let tmp_dir: tempfile::TempDir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: {:?}",
                e
            ))
        }
    };
    // creating a tempfile hold all the contents of the rdf inputs files
    let mut tmp_file = match Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(tmp_dir.path())
    {
        Ok(f) => f,
        Err(e) => return Err(anyhow::anyhow!("Error creating temporary file: {:?}", e)),
    };

    let (combined_rdf_path, unknown_files) = files_to_rdf(data, &mut tmp_file)?;
    if unknown_files.len() != 0 {
        for f in unknown_files.clone().iter() {
            if !Path::new(f).exists() {
                error!("file {:?} could not be found on local machine", f);
            }
        }
        error!("unable to convert the following files: {:?}", unknown_files);
        error!("check 'de create --help' for list of supported file types");
        return Err(anyhow::anyhow!(
            "unsupported files detected: {:?}",
            unknown_files
        ));
    }

    debug!("Running RDF2HDT");
    match r2h.convert(Path::new(&combined_rdf_path), Path::new(hdt_name)) {
        Err(e) => return Err(e),
        Ok(_) => debug!("RDF2HDT WORKED"),
    }

    fs::remove_file(tmp_file.path()).expect("didnt remove tempfile containing all RDF Data");

    assert!(Path::exists(Path::new(hdt_name)));
    // Prints location of HDT assuming HDT is generated
    debug!("HDT file created at {}", hdt_name);

    Ok("".to_string())
}

pub fn files_to_rdf(
    data: &Vec<String>,
    out_file: &mut NamedTempFile,
) -> anyhow::Result<(String, Vec<String>), anyhow::Error> {
    let num_files = data.len();
    let mut converted_files = 0;
    let mut nt_files = vec![];

    let mut unrecognized_files = vec![];

    for file in data.iter() {
        let path = Path::new(&file);
        if !path.exists() {
            unrecognized_files.push(file.clone());
            continue;
        }

        // Check for triples, this is the preferred RDF format and no additional conversion is required
        if file.ends_with(".nt") {
            debug!("Adding RDF triples to graph");
            nt_files.push(file.clone());
        } else {
            let source = match File::open(file) {
                Ok(f) => f,
                Err(e) => return Err(anyhow::anyhow!("Error opening file {:?}: {:?}", file, e)),
            };
            let source_reader = BufReader::new(source);

            debug!("converting {} to nt format", &file);

            let mut dest_writer = BufWriter::new(out_file.as_file());
            let mut serializer =
                RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());

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
                unrecognized_files.push(file.clone());
                continue;
            };
            let quads = RdfParser::from_format(rdf_format)
                .for_reader(source_reader)
                .collect::<Result<Vec<_>, _>>()?;
            for q in quads.iter() {
                if q.graph_name.to_string() != DefaultGraph.to_string() {
                    warn! {"HDT does not support named graphs, merging triples for {file}"}
                }
                serializer.serialize_triple(TripleRef {
                    subject: q.subject.as_ref(),
                    predicate: q.predicate.as_ref(),
                    object: q.object.as_ref(),
                })?
            }

            serializer.finish()?;
            dest_writer.flush()?;
            converted_files += 1;
        }
    }

    // optimization here. If only one NTriple file provided don't do an additional file copy otherwise
    // inefficient when creating an HDT file from one large file
    if nt_files.len() > 1 || converted_files != 0 {
        for nt_file in nt_files {
            let source = match File::open(&nt_file) {
                Ok(f) => f,
                Err(e) => return Err(anyhow::anyhow!("Error opening file {:?}: {:?}", nt_file, e)),
            };
            let mut source_reader = BufReader::new(source);

            match copy(&mut source_reader, out_file) {
                Ok(g) => g,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Error copying file {:?}: {:?} ",
                        &nt_file,
                        e
                    ))
                }
            };
        }
    } else if nt_files.len() == 1 && converted_files == 0 {
        return Ok((data[0].clone(), unrecognized_files));
    }

    return Ok((
        out_file.path().to_str().unwrap().to_string(),
        unrecognized_files,
    ));
}

#[cfg(test)]
mod tests {}
