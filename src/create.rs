// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

// This file handles the create subcommand

use crate::rdf2nt::ConvertResult;
use crate::rdf2nt::OxRdfConvert;
use crate::rdf2nt::Rdf2Nt;
use log::*;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::{copy, BufReader};
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, Builder, NamedTempFile};

pub fn do_create(hdt_name: &str, data: &[String]) -> anyhow::Result<String, anyhow::Error> {
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

    let (combined_rdf_path, unknown_files) =
        files_to_rdf(data, &mut tmp_file, Arc::new(OxRdfConvert {}))?;
    if !unknown_files.is_empty() {
        for f in unknown_files.clone().iter() {
            if !Path::new(f).exists() {
                error!("file {f:?} could not be found on local machine");
            }
        }
        error!("unable to convert the following files: {unknown_files:?}");
        error!("check 'de create --help' for list of supported file types");
        return Err(anyhow::anyhow!(
            "unsupported files detected: {:?}",
            unknown_files
        ));
    }

    match hdt::Hdt::read_nt(Path::new(&combined_rdf_path)) {
        Ok(hdt_conv) => {
            let out_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(hdt_name)?;
            let mut buf = BufWriter::new(out_file);
            hdt_conv.write(&mut buf)?
        }
        Err(e) => return Err(anyhow::anyhow!("Error converting combined RDF to HDT: {e}")),
    };

    fs::remove_file(tmp_file.path()).expect("didnt remove tempfile containing all RDF Data");

    assert!(Path::exists(Path::new(hdt_name)));
    // Prints location of HDT assuming HDT is generated
    debug!("HDT file created at {hdt_name}");

    Ok("".to_string())
}

pub fn files_to_rdf(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
) -> anyhow::Result<(String, Vec<String>), anyhow::Error> {
    // let mut converted_files = 0;
    let mut nt_files = vec![];
    let mut files_to_convert = vec![];
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
            files_to_convert.push(file.clone());
        }
    }

    let conv_res = if !files_to_convert.is_empty() {
        match converter.convert_to_nt(files_to_convert, out_file.as_file()) {
            Ok(r) => {
                unrecognized_files.extend(r.unhandled.clone());
                r
            }
            Err(e) => return Err(anyhow::anyhow!("Error converting file(s) to NT: {e}")),
        }
    } else {
        ConvertResult::default()
    };

    // optimization attempt. If only one NTriple file provided don't do an additional file copy otherwise
    // inefficient when creating an HDT file from one large file
    if nt_files.len() > 1 || conv_res.converted != 0 {
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
    } else if nt_files.len() == 1 && conv_res.converted == 0 {
        return Ok((nt_files[0].clone(), unrecognized_files));
    }

    Ok((
        out_file.path().to_str().unwrap().to_string(),
        unrecognized_files,
    ))
}

#[cfg(test)]
mod tests {}
