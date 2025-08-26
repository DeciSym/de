// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use anyhow::anyhow;
use hdt::containers::ControlInfo;
use hdt::header::Header;
use log::{debug, error};
use std::path::Path;

/// display some HDT file statistics
pub fn show_content(hdt_files: &[String], indent: String) -> anyhow::Result<(), anyhow::Error> {
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
        println!("{indent}{f}:");
        for t in h.body {
            println!("{indent}\t{}: {:?}", t.predicate, t.object)
        }
    }

    Ok(())
}

pub fn view_hdt(hdt_files: &[String]) -> anyhow::Result<String, anyhow::Error> {
    debug!("Calling view::show_content");
    match show_content(hdt_files, String::new()) {
        Ok(_) => {}
        Err(e) => return Err(e),
    };

    Ok("".to_string())
}

#[cfg(test)]
mod tests {
    use crate::view;
    #[test]
    fn test_view() -> anyhow::Result<()> {
        view::view_hdt(&["tests/resources/apple.hdt".to_string()])
            .expect("failed to load hdt file");
        Ok(())
    }
}
