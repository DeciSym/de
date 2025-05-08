// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use anyhow::anyhow;
use hdt::containers::ControlInfo;
use hdt::header::Header;
use log::{debug, error};
use std::path::Path;

pub fn show_content(hdt_files: &[String], indent: String) -> anyhow::Result<(), anyhow::Error> {
    debug!("Getting HDT info ...");

    for f in hdt_files {
        let path = Path::new(f);
        if !path.exists() {
            error!("file {:?} could not be found on local machine", path);
            return Err(anyhow!(
                "file {:?} could not be found on local machine",
                path
            ));
        }
        let file = std::fs::File::open(path).expect("error opening file");
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
            let o = format!("{:?}", t.object);
            let s: Vec<&str> = o.split("\"").collect();

            match t.predicate.as_str() {
                "http://rdfs.org/ns/void#distinctObjects"
                | "http://rdfs.org/ns/void#distinctSubjects"
                | "http://rdfs.org/ns/void#properties"
                | "http://rdfs.org/ns/void#triples"
                | "http://purl.org/HDT/hdt#hdtSize"
                | "http://purl.org/HDT/hdt#originalSize" => {
                    println!("{indent}\t{}: {}", t.predicate, s[1])
                }
                _ => {}
            }
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
