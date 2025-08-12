// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

// This file handles the query subcommand

use crate::create;
use crate::rdf2nt::OxRdfConvert;
use anyhow::Error;
use log::*;
use oxrdfio::RdfFormat;
use oxrdfio::RdfSerializer;
use sparesults::QueryResultsFormat;
use sparesults::QueryResultsSerializer;
use spareval::QueryResults;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs, vec};
use tempfile::{tempdir, Builder, NamedTempFile};

use hdt::sparql::query;
use hdt::sparql::HdtDataset;

#[derive(clap::ValueEnum, Clone, Default, Debug, PartialEq)]
pub enum DeOutput {
    #[default]
    /// https://www.w3.org/TR/sparql11-results-csv-tsv/
    CSV,

    /// https://www.w3.org/TR/sparql11-results-csv-tsv/
    TSV,

    /// https://www.w3.org/TR/sparql11-results-json/
    JSON,

    /// https://www.w3.org/TR/rdf-sparql-XMLres
    XML,

    /// https://w3c.github.io/N3/spec/
    N3,

    /// https://www.w3.org/TR/n-quads/
    NQUADS,

    /// https://www.w3.org/TR/rdf-syntax-grammar/
    RDFXML,

    /// https://www.w3.org/TR/n-triples/
    NTRIPLE,

    /// https://www.w3.org/TR/trig/
    TRIG,

    /// https://www.w3.org/TR/turtle/
    TURTLE,
}
// Function that will be used to handle querying multiple local HDTs
pub async fn do_query(
    data_files: &[String],
    query_files: &Vec<String>,
    out: &DeOutput,
) -> anyhow::Result<String, anyhow::Error> {
    debug!("Executing querying ...");

    // fail fast on input validation before files start getting mounted
    for rq in query_files.clone() {
        let path = Path::new(&rq);
        if !path.exists() {
            error!("query file {rq:?} could not be found on local machine");
            return Err(anyhow::anyhow!(
                "query file {:?} could not be found on local machine",
                rq
            ));
        }
    }

    let (dir_path_vec, hdt_path_vec, e) = handle_files(data_files.to_owned()).await;

    if e.is_some() {
        file_cleanup(dir_path_vec.clone()).await;
        return Err(anyhow::anyhow!(
            "Error reading data files: {:?}",
            e.unwrap()
        ));
    }

    let dataset = HdtDataset::new(
        &hdt_path_vec
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>(),
    )?;

    let mut output = String::new();
    for rq in query_files {
        let mut f = File::open(rq)?;
        let mut buffer = String::new();

        f.read_to_string(&mut buffer)?;
        let qr = match query(&buffer, dataset.clone()) {
            Ok(r) => r,
            Err(e) => {
                error!("problem executing the hdt query: {e}");
                file_cleanup(dir_path_vec.clone()).await;
                return Err(anyhow::anyhow!("{e}"));
            }
        };

        let results_tmp_file = NamedTempFile::new().unwrap();

        match qr {
            QueryResults::Solutions(query_solution_iter) => {
                let result_format = if *out == DeOutput::CSV {
                    QueryResultsFormat::Csv
                } else if *out == DeOutput::TSV {
                    QueryResultsFormat::Tsv
                } else if *out == DeOutput::JSON {
                    QueryResultsFormat::Json
                } else if *out == DeOutput::XML {
                    QueryResultsFormat::Xml
                } else {
                    error!("ASK queries support only CSV, TSV, JSON, or XML");
                    return Err(anyhow::anyhow!(
                        "ASK queries support only CSV, TSV, JSON, or XML"
                    ));
                };
                let results_writer = QueryResultsSerializer::from_format(result_format);
                let mut serializer = results_writer
                    .serialize_solutions_to_writer(
                        &results_tmp_file,
                        query_solution_iter.variables().into(),
                    )
                    .unwrap();
                for s in query_solution_iter {
                    let s = s?;
                    serializer.serialize(&s).expect("fixme2");
                }
            }
            QueryResults::Boolean(result) => {
                let result_format = if *out == DeOutput::CSV {
                    QueryResultsFormat::Csv
                } else if *out == DeOutput::TSV {
                    QueryResultsFormat::Tsv
                } else if *out == DeOutput::JSON {
                    QueryResultsFormat::Json
                } else if *out == DeOutput::XML {
                    QueryResultsFormat::Xml
                } else {
                    warn!(
                        "ASK queries support only CSV, TSV, JSON, or XML. Defaulting to CSV format"
                    );
                    QueryResultsFormat::Csv
                };
                let results_writer = QueryResultsSerializer::from_format(result_format);
                results_writer
                    .serialize_boolean_to_writer(&results_tmp_file, result)
                    .expect("fixme");
            }
            QueryResults::Graph(query_triple_iter) => {
                let result_format = if *out == DeOutput::N3 {
                    RdfFormat::N3
                } else if *out == DeOutput::NQUADS {
                    RdfFormat::NQuads
                } else if *out == DeOutput::NTRIPLE {
                    RdfFormat::NTriples
                } else if *out == DeOutput::RDFXML {
                    RdfFormat::RdfXml
                } else if *out == DeOutput::TRIG {
                    RdfFormat::TriG
                } else if *out == DeOutput::TURTLE {
                    RdfFormat::Turtle
                } else {
                    warn!("CONSTRUCT and DESCRIBE queries only support NQ, NT, RDFXML, TRIG, and TTL formats. Defaulting to NTriple format");
                    RdfFormat::NTriples
                };
                let mut serializer =
                    RdfSerializer::from_format(result_format).for_writer(&results_tmp_file);
                for triple in query_triple_iter {
                    let triple = triple?;
                    serializer.serialize_triple(&triple)?
                }
                serializer.finish()?;
            }
        };

        let file = File::open(results_tmp_file);
        let reader = BufReader::new(file.unwrap());
        for line in reader.lines() {
            let l = line.unwrap();
            println!("{l}");
            if output.is_empty() {
                output = l.clone();
            } else {
                output = format!("{output}\n{l}");
            }
        }
    }

    // TODO this needs to be run on success and before any return Err()
    file_cleanup(dir_path_vec.clone()).await;

    Ok(output)
}

async fn handle_files(files: Vec<String>) -> (Vec<String>, Vec<String>, Option<anyhow::Error>) {
    let mut dir_path_vec: Vec<String> = vec![]; // This is holding the path to the tempfiles that havent been removed from disk
    let mut hdt_path_vec: Vec<String> = vec![]; // This is holding all the paths to the hdt files. this needs to stay
    let tmp_dir = match tempdir() {
        Ok(d) => d,
        Err(e) => {
            return (
                dir_path_vec,
                hdt_path_vec,
                Some(anyhow::anyhow!(
                    "Error creating temporary working dir: {:?}",
                    e
                )),
            )
        }
    };
    let t_path = tmp_dir.path(); // Getting the tempdir path.
    dir_path_vec.push(t_path.to_str().unwrap().to_string());

    // Creating TempFile to hold the hdt contents
    let mut rdf_tempfile: NamedTempFile = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(t_path)
        .unwrap();

    let (combined_rdf_path, unknown_files) =
        match create::files_to_rdf(&files.clone(), &mut rdf_tempfile, Arc::new(OxRdfConvert {})) {
            Ok((p, u)) => (p, u),
            Err(e) => {
                return (
                    dir_path_vec,
                    hdt_path_vec,
                    Some(Error::msg(format!("error processing files to RDF {e}"))),
                );
            }
        };

    for file in unknown_files.iter() {
        if !Path::new(file).exists() {
            return (
                dir_path_vec,
                hdt_path_vec,
                Some(Error::msg(format!("unable to locate local file {file}"))),
            );
        }
        if file.ends_with(".hdt") {
            hdt_path_vec.push(file.clone())
        }
        // should be able to query plain rdf files directly
        else {
            return (
                dir_path_vec,
                hdt_path_vec,
                Some(anyhow::anyhow!("unrecognized file type: {file}")),
            );
        }
    }

    let meta = std::fs::metadata(rdf_tempfile.path()).unwrap();

    let converted_rdf = if meta.len() == 0 {
        Path::new(&combined_rdf_path)
    } else {
        rdf_tempfile.path()
    };

    if meta.len() != 0 || rdf_tempfile.path() != Path::new(&combined_rdf_path) {
        // Creating TempFile to hold the hdt contents
        let named_tempfile: NamedTempFile = Builder::new()
            .suffix(".hdt")
            .append(true)
            .tempfile_in(t_path)
            .unwrap();

        debug!("Running RDF2HDT");

        match hdt::Hdt::read_nt(Path::new(converted_rdf.to_str().unwrap())) {
            Ok(hdt_conv) => {
                let mut buf = BufWriter::new(&named_tempfile);
                match hdt_conv.write(&mut buf) {
                    Ok(_) => {}
                    Err(e) => {
                        return (
                            dir_path_vec,
                            hdt_path_vec,
                            Some(anyhow::anyhow!("failed to write converted HDT file: {e}")),
                        );
                    }
                }
            }
            Err(e) => error!(
                "error converting plain RDF file {:?} to HDT: {e}",
                rdf_tempfile.path()
            ),
        };
        hdt_path_vec.push(named_tempfile.path().to_str().unwrap().to_string());
        let _ = named_tempfile.keep();
        let _ = tmp_dir.keep();
    }

    if hdt_path_vec.is_empty() {
        error!("no files to query")
    }
    (dir_path_vec, hdt_path_vec, None)
}

// performs directory removal for a list of directories
pub async fn file_cleanup(dirs: Vec<String>) {
    // TODO need to either change everything to pass a vec of strings or change other functions to pass a vec
    debug!("Cleaning up environment");
    for dir in dirs.iter() {
        if let Err(e) = fs::remove_dir_all(dir) {
            error!("Failed to remove directory {dir:?}: {e:?}")
        };
    }
}

#[cfg(test)]
mod tests {}
