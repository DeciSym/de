// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::rdf2nt::ConvertResult;
use crate::rdf2nt::OxRdfConvert;
use crate::rdf2nt::Rdf2Nt;
use log::*;
use std::fs::{self, File, OpenOptions};
use std::io::{copy, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile};

/// Creates a HDT file from RDF source
pub fn do_create(hdt_name: &str, data: &[String]) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    debug!("Creating HDT...");
    // creating a tempfile to hold all the contents of the rdf input files
    let mut tmp_file = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile()
        .map_err(|e| anyhow::anyhow!("Error creating temporary file: {:?}", e))?;

    let (combined_rdf_path, unknown_files) =
        files_to_rdf(data, &mut tmp_file, Arc::new(OxRdfConvert {}))?;
    if !unknown_files.is_empty() {
        for f in &unknown_files {
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

    let new_hdt = hdt::Hdt::read_nt(Path::new(&combined_rdf_path))
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
    let _ = hdt::Hdt::new_hybrid_cache(Path::new(hdt_name))
        .map_err(|e| anyhow::anyhow!("failed to create cache file: {e}"))?;

    Ok(new_hdt)
}

/// Converts a list of RDF files to NTriple RDF
/// returns the name of the file containing combined NTriple RDF and the names of any unhandled files
pub fn files_to_rdf(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
) -> anyhow::Result<(String, Vec<String>), anyhow::Error> {
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
    if nt_files.len() > 1 || conv_res.converted != 0 {
        for nt_file in nt_files {
            let source = File::open(&nt_file)
                .map_err(|e| anyhow::anyhow!("Error opening file {:?}: {:?}", nt_file, e))?;
            let mut source_reader = BufReader::new(source);

            copy(&mut source_reader, out_file)
                .map_err(|e| anyhow::anyhow!("Error copying file {:?}: {:?}", &nt_file, e))?;
        }
    } else if nt_files.len() == 1 && conv_res.converted == 0 {
        return Ok((nt_files[0].clone(), unrecognized_files));
    }

    Ok((
        out_file
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in temp file path"))?
            .to_string(),
        unrecognized_files,
    ))
}

#[cfg(test)]
mod tests {}
