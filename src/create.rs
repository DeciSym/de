// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::hdt_meta;
use crate::rdf2nt::ConvertResult;
use crate::rdf2nt::OxRdfConvert;
use crate::rdf2nt::Rdf2Nt;
use log::*;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write, copy};
use std::path::Path;
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile};

/// Creates a HDT file from RDF source
pub fn do_create(hdt_name: &str, data: &[String]) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    do_create_with_options(hdt_name, data, false, None)
}

/// Creates a HDT file from RDF source with explicit control over named graph merging.
pub fn do_create_with_options(
    hdt_name: &str,
    data: &[String],
    allow_merge_named_graphs: bool,
    graph_iri: Option<&str>,
) -> anyhow::Result<hdt::Hdt, anyhow::Error> {
    debug!("Creating HDT...");
    // creating a tempfile to hold all the contents of the rdf input files
    let mut tmp_file = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile()
        .map_err(|e| anyhow::anyhow!("Error creating temporary file: {:?}", e))?;

    let rdf_result = files_to_rdf_with_stats(data, &mut tmp_file, Arc::new(OxRdfConvert {}))?;
    if !rdf_result.unknown_files.is_empty() {
        for f in &rdf_result.unknown_files {
            if !Path::new(f).exists() {
                error!("file {f:?} could not be found on local machine");
            }
        }
        error!(
            "unable to convert the following files: {:?}",
            rdf_result.unknown_files
        );
        error!("check 'de create --help' for list of supported file types");
        return Err(anyhow::anyhow!(
            "unsupported files detected: {:?}",
            rdf_result.unknown_files
        ));
    }
    if !allow_merge_named_graphs && rdf_result.named_graphs.len() > 1 {
        return Err(anyhow::anyhow!(
            "multiple named graphs detected during create ({:?}). HDT output is single-graph. \
Use --allow-merge-named-graphs to explicitly merge these graphs into the output graph.",
            rdf_result.named_graphs
        ));
    }

    let mut new_hdt = read_nt_hdt_safe(Path::new(&rdf_result.combined_rdf_path))?;
    if let Some(graph_iri) = graph_iri {
        hdt_meta::set_graph_iri_metadata_in_hdt(&mut new_hdt, graph_iri)?;
    }

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
    Ok(new_hdt)
}

fn read_nt_hdt_safe(path: &Path) -> anyhow::Result<hdt::Hdt> {
    hdt::Hdt::read_nt(path)
        .map_err(|e| anyhow::anyhow!("Error converting combined RDF to HDT: {e}"))
}

fn ensure_nt_line_boundary(out_file: &NamedTempFile) -> anyhow::Result<()> {
    let out_len = out_file
        .as_file()
        .metadata()
        .map_err(|e| anyhow::anyhow!("Error inspecting temporary RDF file: {e}"))?
        .len();
    if out_len == 0 {
        return Ok(());
    }

    let mut probe = File::open(out_file.path())
        .map_err(|e| anyhow::anyhow!("Error opening temporary RDF file for boundary check: {e}"))?;
    probe
        .seek(SeekFrom::End(-1))
        .map_err(|e| anyhow::anyhow!("Error seeking temporary RDF file for boundary check: {e}"))?;
    let mut last_byte = [0_u8; 1];
    probe
        .read_exact(&mut last_byte)
        .map_err(|e| anyhow::anyhow!("Error reading temporary RDF file for boundary check: {e}"))?;

    if !matches!(last_byte[0], b'\n' | b'\r') {
        let mut out = out_file.as_file();
        out.write_all(b"\n").map_err(|e| {
            anyhow::anyhow!("Error writing newline separator to temporary RDF file: {e}")
        })?;
    }
    Ok(())
}

struct FilesToRdfResult {
    combined_rdf_path: String,
    unknown_files: Vec<String>,
    named_graphs: Vec<String>,
}

/// Converts a list of RDF files to NTriple RDF
/// returns the name of the file containing combined NTriple RDF and the names of any unhandled files
pub fn files_to_rdf(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
) -> anyhow::Result<(String, Vec<String>), anyhow::Error> {
    let result = files_to_rdf_with_stats(data, out_file, converter)?;
    Ok((result.combined_rdf_path, result.unknown_files))
}

fn files_to_rdf_with_stats(
    data: &[String],
    out_file: &mut NamedTempFile,
    converter: Arc<dyn Rdf2Nt>,
) -> anyhow::Result<FilesToRdfResult, anyhow::Error> {
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
        unrecognized_files.extend(r.unhandled.iter().cloned());
        r
    } else {
        ConvertResult::default()
    };

    let combined_rdf_path = if nt_files.len() > 1 || conv_res.converted != 0 {
        for nt_file in nt_files {
            ensure_nt_line_boundary(out_file)?;
            let source = File::open(&nt_file)
                .map_err(|e| anyhow::anyhow!("Error opening file {:?}: {:?}", nt_file, e))?;
            let mut source_reader = BufReader::new(source);

            copy(&mut source_reader, out_file)
                .map_err(|e| anyhow::anyhow!("Error copying file {:?}: {:?}", &nt_file, e))?;
        }
        out_file
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in temp file path"))?
            .to_string()
    } else if nt_files.len() == 1 && conv_res.converted == 0 {
        nt_files[0].clone()
    } else {
        out_file
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in temp file path"))?
            .to_string()
    };

    let mut named_graphs: Vec<String> = conv_res.named_graphs.into_iter().collect();
    named_graphs.sort_unstable();

    Ok(FilesToRdfResult {
        combined_rdf_path,
        unknown_files: unrecognized_files,
        named_graphs,
    })
}

#[cfg(test)]
mod tests {
    use super::{do_create, do_create_with_options, files_to_rdf};
    use crate::hdt_meta;
    use std::fs::{self, write};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn create_fails_when_multiple_named_graphs_are_merged_without_override() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("multi.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> <http://example.org/g1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g2> .\n",
        )?;

        let result = do_create(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
        );
        assert!(result.is_err());
        let msg = result.expect_err("expected error").to_string();
        assert!(msg.contains("--allow-merge-named-graphs"));
        Ok(())
    }

    #[test]
    fn create_allows_multiple_named_graph_merge_with_override() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nq_path = tmp.path().join("multi.nq");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nq_path,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> <http://example.org/g1> .\n\
             <http://example.org/s2> <http://example.org/p> <http://example.org/o2> <http://example.org/g2> .\n",
        )?;

        let result = do_create_with_options(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nq_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            true,
            None,
        );
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn create_writes_graph_iri_metadata_when_provided() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("single.nt");
        let out_hdt = tmp.path().join("out.hdt");
        let graph_iri = "http://example.org/g";
        write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        do_create_with_options(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            false,
            Some(graph_iri),
        )?;

        let found = hdt_meta::read_graph_iri_metadata(&out_hdt)?;
        assert_eq!(found.as_deref(), Some(graph_iri));
        Ok(())
    }

    #[test]
    fn create_rejects_invalid_graph_iri_metadata() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("single.nt");
        let out_hdt = tmp.path().join("out.hdt");
        write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        let result = do_create_with_options(
            out_hdt
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid output path"))?,
            &[nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            false,
            Some("not an iri"),
        );
        assert!(result.is_err());
        let msg = result.expect_err("expected invalid graph IRI").to_string();
        assert!(msg.contains("invalid graph IRI metadata"));
        Ok(())
    }

    #[test]
    fn create_invalid_nt_returns_error_without_panic() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let bad_nt = tmp.path().join("bad.nt");
        let out_hdt = tmp.path().join("out.hdt");
        write(&bad_nt, "invalid triple")?;

        let out_hdt_s = out_hdt.to_string_lossy().to_string();
        let data = vec![bad_nt.to_string_lossy().to_string()];
        let result = std::panic::catch_unwind(|| do_create(&out_hdt_s, &data));
        assert!(
            result.is_ok(),
            "do_create should return Err instead of panicking"
        );
        let create_result = result.expect("catch_unwind should not fail");
        assert!(create_result.is_err(), "invalid NT should return error");
        Ok(())
    }

    #[test]
    fn create_multiple_nt_without_trailing_newline_is_handled() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let first_nt = tmp.path().join("first.nt");
        let second_nt = tmp.path().join("second.nt");
        let out_hdt = tmp.path().join("out.hdt");

        // Intentionally omit trailing newline in first file to validate separator handling.
        write(
            &first_nt,
            "<http://example.org/s1> <http://example.org/p> <http://example.org/o1> .",
        )?;
        write(
            &second_nt,
            "<http://example.org/s2> <http://example.org/p> <http://example.org/o2> .\n",
        )?;

        let out_hdt_s = out_hdt.to_string_lossy().to_string();
        let data = vec![
            first_nt.to_string_lossy().to_string(),
            second_nt.to_string_lossy().to_string(),
        ];
        let result = std::panic::catch_unwind(|| do_create(&out_hdt_s, &data));
        assert!(
            result.is_ok(),
            "do_create should not panic on valid NT inputs"
        );
        let create_result = result.expect("catch_unwind should not fail");
        assert!(
            create_result.is_ok(),
            "valid multi-file NT create should succeed"
        );
        assert!(
            fs::metadata(out_hdt).is_ok(),
            "output HDT should be created"
        );
        Ok(())
    }

    #[test]
    fn files_to_rdf_single_nt_reuses_input_path() -> anyhow::Result<()> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("single.nt");
        write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        let mut out_file = tempfile::Builder::new().suffix(".nt").tempfile()?;
        let (combined_rdf_path, unknown_files) = files_to_rdf(
            &[nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
                .to_string()],
            &mut out_file,
            Arc::new(crate::rdf2nt::OxRdfConvert {}),
        )?;

        assert!(unknown_files.is_empty());
        assert_eq!(
            combined_rdf_path,
            nt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid input path"))?
        );
        assert_eq!(out_file.as_file().metadata()?.len(), 0);
        Ok(())
    }
}
