// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use anyhow::anyhow;
use hdt::containers::ControlInfo;
use hdt::header::Header;
use log::{debug, error};
use std::{
    io::{BufWriter, Write},
    path::Path,
};

/// display some HDT file statistics
pub fn show_content<W: Write>(
    hdt_files: &[String],
    indent: String,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<(), anyhow::Error> {
    debug!("Getting HDT info ...");

    for f in hdt_files {
        let path = Path::new(f);
        if !path.exists() {
            error!("file {path:?} could not be found on local machine");
            return Err(anyhow!(
                "file {:?} could not be found on local machine",
                path
            ));
        }
        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                return Err(anyhow!("error opening HDT file {path:?}: {e}"));
            }
        };
        let mut reader = std::io::BufReader::new(file);
        // seek past the start of the file, nothing in here worth displaying
        match ControlInfo::read(&mut reader) {
            Ok(_) => {}
            Err(e) => {
                error!("failed to read HDT control info for file {f}: {e}");
                return Err(anyhow!("error reading control info for HDT file {f}: {e}"));
            }
        };
        let h = match Header::read(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                error!("failed to read HDT header for file {f}: {e}");
                return Err(anyhow!("error reading header for HDT file {f}: {e}"));
            }
        };
        writeln!(writer, "{indent}{f}:")?;
        for t in h.body {
            writeln!(writer, "{indent}\t{}: {:?}", t.predicate, t.object)?
        }
    }

    writer.flush()?;
    Ok(())
}

pub fn view_hdt<W: Write>(hdt_files: &[String], writer: &mut BufWriter<W>) -> anyhow::Result<()> {
    match show_content(hdt_files, String::new(), writer) {
        Ok(_) => {}
        Err(e) => return Err(e),
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::BufWriter;

    use crate::view;
    #[test]
    fn test_view() -> anyhow::Result<()> {
        let mut stdout_writer = BufWriter::new(Vec::new());
        view::view_hdt(
            &["tests/resources/apple.hdt".to_string()],
            &mut stdout_writer,
        )
        .expect("failed to load hdt file");
        Ok(())
    }
}
