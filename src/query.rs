// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::create;
use crate::rdf2nt::OxRdfConvert;
use crate::sparql;
use anyhow::Error;
use log::*;
use oxrdfio::RdfFormat;
use oxrdfio::RdfSerializer;
use sparesults::QueryResultsFormat;
use sparesults::QueryResultsSerializer;
use spareval::QueryResults;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, Builder, NamedTempFile};

#[derive(clap::ValueEnum, Clone, Default, Debug, PartialEq)]
pub enum DeOutput {
    #[default]
    /// <https://www.w3.org/TR/sparql11-results-csv-tsv/>
    CSV,

    /// <https://www.w3.org/TR/sparql11-results-csv-tsv/>
    TSV,

    /// <https://www.w3.org/TR/sparql11-results-json/>
    JSON,

    /// <https://www.w3.org/TR/rdf-sparql-XMLres>
    XML,

    /// <https://w3c.github.io/N3/spec/>
    N3,

    /// <https://www.w3.org/TR/n-quads/>
    NQUADS,

    /// <https://www.w3.org/TR/rdf-syntax-grammar/>
    RDFXML,

    /// <https://www.w3.org/TR/n-triples/>
    NTRIPLE,

    /// <https://www.w3.org/TR/trig/>
    TRIG,

    /// <https://www.w3.org/TR/turtle/>
    TURTLE,
}
/// Execute a list of sparql queries over a list of RDF files. Non-HDT data files are converted to temporary HDT files before query execution
pub async fn do_query<W: Write>(
    data_files: &[String],
    query_files: &[String],
    out: &DeOutput,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    debug!("Executing querying ...");

    // fail fast on input validation
    for rq in query_files {
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

    if let Some(e) = e {
        file_cleanup(dir_path_vec.clone()).await;
        return Err(anyhow::anyhow!("Error reading data files: {e}",));
    }

    let dataset = sparql::AggregateHdt::new(&hdt_path_vec)
        .map_err(|e| anyhow::anyhow!("error initializting HDT files: {e}"))?;
    let snapshot = dataset
        .get_snapshot(None)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    for rq in query_files {
        let mut f = File::open(rq)?;
        let mut buffer = String::new();

        f.read_to_string(&mut buffer)?;
        let qr = match sparql::query(&buffer, &snapshot, None) {
            Ok(r) => r,
            Err(e) => {
                error!("problem executing the hdt query: {e}");
                file_cleanup(dir_path_vec.clone()).await;
                return Err(anyhow::anyhow!("{e}"));
            }
        };

        match qr {
            QueryResults::Solutions(query_solution_iter) => {
                let result_format = match out {
                    DeOutput::CSV => QueryResultsFormat::Csv,
                    DeOutput::TSV => QueryResultsFormat::Tsv,
                    DeOutput::JSON => QueryResultsFormat::Json,
                    DeOutput::XML => QueryResultsFormat::Xml,
                    _ => {
                        error!("ASK queries support only CSV, TSV, JSON, or XML");
                        return Err(anyhow::anyhow!(
                            "ASK queries support only CSV, TSV, JSON, or XML"
                        ));
                    }
                };
                let results_writer = QueryResultsSerializer::from_format(result_format);
                let mut serializer = results_writer.serialize_solutions_to_writer(
                    &mut *writer,
                    query_solution_iter.variables().into(),
                )?;
                for s in query_solution_iter {
                    let s = s?;
                    serializer.serialize(&s).map_err(|e| {
                        error!("error serializing query solutions to desired output format: {e}");
                        anyhow::anyhow!(
                            "error serializing query solutions to desired output format: {e}"
                        )
                    })?;
                }
                serializer.finish()?;
            }
            QueryResults::Boolean(result) => {
                let result_format = match out {
                    DeOutput::CSV => QueryResultsFormat::Csv,
                    DeOutput::TSV => QueryResultsFormat::Tsv,
                    DeOutput::JSON => QueryResultsFormat::Json,
                    DeOutput::XML => QueryResultsFormat::Xml,
                    _ => {
                        warn!(
                            "ASK queries support only CSV, TSV, JSON, or XML. Defaulting to CSV format"
                        );
                        QueryResultsFormat::Csv
                    }
                };
                let results_writer = QueryResultsSerializer::from_format(result_format);
                results_writer
                    .serialize_boolean_to_writer(&mut *writer, result)
                    .map_err(|e| {
                        error!("error serializing query solutions to desired output format: {e}");
                        anyhow::anyhow!(
                            "error serializing query solutions to desired output format: {e}"
                        )
                    })?;
            }
            QueryResults::Graph(query_triple_iter) => {
                let result_format = match out {
                    DeOutput::N3 => RdfFormat::N3,
                    DeOutput::NQUADS => RdfFormat::NQuads,
                    DeOutput::NTRIPLE => RdfFormat::NTriples,
                    DeOutput::RDFXML => RdfFormat::RdfXml,
                    DeOutput::TRIG => RdfFormat::TriG,
                    DeOutput::TURTLE => RdfFormat::Turtle,
                    _ => {
                        warn!("CONSTRUCT and DESCRIBE queries only support NQ, NT, RDFXML, TRIG, and TTL formats. Defaulting to NTriple format");
                        RdfFormat::NTriples
                    }
                };
                let mut serializer =
                    RdfSerializer::from_format(result_format).for_writer(&mut *writer);
                for triple in query_triple_iter {
                    let triple = triple?;
                    serializer.serialize_triple(&triple)?
                }
                serializer.finish()?;
            }
        };
    }
    writer.flush()?;

    // TODO this needs to be run on success and before any return Err()
    file_cleanup(dir_path_vec.clone()).await;

    Ok(())
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

    // Creating TempFile to hold the hdt contents
    let mut rdf_tempfile: NamedTempFile = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(t_path)
        .unwrap();

    let mut files_to_convert = vec![];
    for f in &files {
        if f.ends_with(".hdt") {
            hdt_path_vec.push(f.to_string())
        } else {
            files_to_convert.push(f.to_string());
        }
    }

    let (combined_rdf_path, unknown_files) = match create::files_to_rdf(
        &files_to_convert,
        &mut rdf_tempfile,
        Arc::new(OxRdfConvert {}),
    ) {
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
            hdt_path_vec.push(file.to_string())
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
        dir_path_vec.push(t_path.to_str().unwrap().to_string());
        let _ = tmp_dir.keep();
    }

    if hdt_path_vec.is_empty() {
        error!("no files to query")
    }
    (dir_path_vec, hdt_path_vec, None)
}

// performs directory removal for a list of directories
pub async fn file_cleanup(dirs: Vec<String>) {
    debug!("Cleaning up environment");
    for dir in dirs.iter() {
        if let Err(e) = fs::remove_dir_all(dir) {
            error!("Failed to remove directory {dir:?}: {e:?}")
        };
    }
}

#[cfg(test)]
mod tests {}
