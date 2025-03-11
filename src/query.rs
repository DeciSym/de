// This file handles the query subcommand

use crate::create;
use crate::rdf2hdt::Rdf2Hdt;
use crate::rdf2nt::OxRdfConvert;
use anyhow::Error;
use log::*;
use oxigraph::io::RdfFormat;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::{fs, vec};
use tempfile::{tempdir, Builder, NamedTempFile};

use oxigraph::sparql::dataset::HDTDatasetView;
use oxigraph::sparql::evaluate_hdt_query;
use oxigraph::sparql::results::QueryResultsFormat;
use oxigraph::sparql::QueryOptions;

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
    r2h: Arc<dyn Rdf2Hdt>,
    out: &DeOutput,
) -> anyhow::Result<String, anyhow::Error> {
    debug!("Executing querying ...");

    // fail fast on input validation before files start getting mounted
    for rq in query_files.clone() {
        let path = Path::new(&rq);
        if !path.exists() {
            error!("query file {:?} could not be found on local machine", rq);
            return Err(anyhow::anyhow!(
                "query file {:?} could not be found on local machine",
                rq
            ));
        }
    }

    let (dir_path_vec, hdt_path_vec, e) = handle_files(data_files.to_owned(), r2h).await;

    if e.is_some() {
        file_cleanup(dir_path_vec.clone()).await;
        return Err(anyhow::anyhow!(
            "Error reading data files: {:?}",
            e.unwrap()
        ));
    }

    let dataset = HDTDatasetView::new(&hdt_path_vec);

    let mut output = String::new();
    for rq in query_files {
        // let dataset = HDTDatasetView::new(hdt_path_vec.clone());
        let sparql_query_string = match fs::read_to_string(rq) {
            Ok(s) => s,
            Err(e) => {
                file_cleanup(dir_path_vec.clone()).await;
                return Err(anyhow::anyhow!("Error reading query file {rq}: {:?}", e));
            }
        };

        let res = match evaluate_hdt_query(
            dataset.clone(),
            sparql_query_string.as_str(),
            QueryOptions::default(),
            false,
            [],
        ) {
            Ok((r, _explaination)) => r,
            Err(e) => {
                error!("problem executing the hdt query: {e}");
                file_cleanup(dir_path_vec.clone()).await;
                return Err(e.into());
            }
        };

        let results_tmp_file = NamedTempFile::new().unwrap();
        let results = match res {
            Ok(qr) => match out {
                DeOutput::CSV | DeOutput::TSV | DeOutput::JSON | DeOutput::XML => {
                    let result_format = if *out == DeOutput::CSV {
                        QueryResultsFormat::Csv
                    } else if *out == DeOutput::TSV {
                        QueryResultsFormat::Tsv
                    } else if *out == DeOutput::JSON {
                        QueryResultsFormat::Json
                    } else {
                        QueryResultsFormat::Xml
                    };
                    qr.write(&results_tmp_file, result_format)
                }
                DeOutput::N3
                | DeOutput::NQUADS
                | DeOutput::NTRIPLE
                | DeOutput::RDFXML
                | DeOutput::TRIG
                | DeOutput::TURTLE => {
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
                    } else {
                        RdfFormat::Turtle
                    };
                    qr.write_graph(&results_tmp_file, result_format)
                }
            },
            Err(e) => {
                error!("error evaluating the hdt query: {e}");
                file_cleanup(dir_path_vec.clone()).await;
                return Err(e.into());
            }
        };

        match results {
            Ok(s) => {
                let file = File::open(s);
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
            Err(e) => {
                error!("Error processing query: {:?}", e);
            }
        };
    }

    // TODO this needs to be run on success and before any return Err()
    file_cleanup(dir_path_vec.clone()).await;

    Ok(output)
}

async fn handle_files(
    files: Vec<String>,
    r2h: Arc<dyn Rdf2Hdt>,
) -> (Vec<String>, Vec<String>, Option<anyhow::Error>) {
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
    let t_path: std::path::PathBuf = tmp_dir.into_path(); // Getting the tempdir path.
    dir_path_vec.push(t_path.to_str().unwrap().to_string());

    // Creating TempFile to hold the hdt contents
    let mut rdf_tempfile: NamedTempFile = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(t_path.clone())
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
            .tempfile_in(t_path.clone())
            .unwrap();

        debug!("Running RDF2HDT");
        match r2h.convert(converted_rdf, named_tempfile.path()) {
            Err(e) => {
                error!(
                    "error converting plain RDF file {:?} to HDT: {e}",
                    rdf_tempfile.path()
                );
                return (dir_path_vec, hdt_path_vec, Some(e));
            }
            Ok(_) => debug!("RDF2HDT WORKED"),
        }
        hdt_path_vec.push(named_tempfile.path().to_str().unwrap().to_string());
        let _ = named_tempfile.keep();
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
            error!("Failed to remove directory {:?}: {:?}", dir, e)
        };
    }
}

#[cfg(test)]
mod tests {}
