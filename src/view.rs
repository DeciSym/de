// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use anyhow::anyhow;
use log::{debug, error};
use std::{
    io::{BufWriter, Write},
    path::Path,
};

/// display some HDT file statistics
pub fn show_content<W: Write>(
    hdt_files: &[String],
    indent: &str,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    debug!("Getting HDT info ...");

    for f in hdt_files {
        let path = Path::new(f);
        if !path.exists() {
            error!(
                "file {} could not be found on local machine",
                path.display()
            );
            return Err(anyhow!(
                "file {} could not be found on local machine",
                path.display()
            ));
        }
        let h = match hdt::header::Header::read_from_hdt_path(path) {
            Ok(v) => v,
            Err(e) => {
                error!("failed to read HDT header for file {f}: {e}");
                return Err(anyhow!("error reading header for HDT file {f}: {e}"));
            }
        };
        writeln!(writer, "{indent}{f}:")?;
        for t in h.body {
            writeln!(writer, "{indent}\t{}: {:?}", t.predicate, t.object)?;
        }
    }

    writer.flush()?;
    Ok(())
}

pub fn view_hdt<W: Write>(hdt_files: &[String], writer: &mut BufWriter<W>) -> anyhow::Result<()> {
    show_content(hdt_files, "", writer)
}

#[cfg(test)]
mod tests {
    use std::io::BufWriter;

    use crate::view;
    #[test]
    fn test_view() {
        let mut stdout_writer = BufWriter::new(Vec::new());
        view::view_hdt(
            &["tests/resources/apple.hdt".to_string()],
            &mut stdout_writer,
        )
        .expect("failed to load hdt file");
    }

    #[test]
    fn test_view_missing_file_returns_error() {
        let mut stdout_writer = BufWriter::new(Vec::new());
        let missing = vec!["tests/resources/does-not-exist.hdt".to_string()];
        let err = view::view_hdt(&missing, &mut stdout_writer).expect_err("expected missing file");
        assert!(err.to_string().contains("could not be found"));
    }
}
