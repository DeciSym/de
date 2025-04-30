// file handles calls to rdf2hdt library

use log::*;
use rdf2hdt::builder::Options;
use std::path::Path;
pub trait Rdf2Hdt: Sync + Send {
    fn convert(&self, source: &Path, dest: &Path) -> anyhow::Result<(), anyhow::Error>;
}

pub struct RustRdfToHdt {}

impl Rdf2Hdt for RustRdfToHdt {
    fn convert(&self, source: &Path, dest: &Path) -> Result<(), anyhow::Error> {
        match rdf2hdt::builder::build_hdt(
            vec![source.to_str().unwrap().to_string()],
            dest.to_str().unwrap(),
            Options::default(),
        ) {
            Ok(_) => {}
            Err(e) => return Err(anyhow::anyhow!(format!("error building hdt: {}", e))),
        }

        Ok(())
    }
}

/*
pub struct Rdf2HdtImpl();

impl Rdf2Hdt for Rdf2HdtImpl {
    fn convert(&self, source: &Path, dest: &Path) -> anyhow::Result<(), anyhow::Error> {
        debug!("Running RDF2HDT binary");
        match File::open(source) {
            Ok(f) => {
                if f.metadata().unwrap().len() == 0 {
                    error!("file {source:?} is empty, nothing to convert");
                    return Err(anyhow::anyhow!("empty file"));
                }
            }
            Err(e) => return Err(anyhow::anyhow!("error opening source file: {e}")),
        }
        let mut r2h = Command::new("rdf2hdt"); //using rdf2hdt-ccp to handle conversion from rdf to hdt
        r2h.args([
            "-i",
            "-f",
            "nt",
            source.to_str().unwrap(),
            dest.to_str().unwrap(),
        ]);

        match r2h.output() {
            Err(e) => return Err(anyhow::anyhow!("Error running rdf2hdt: {:?}", e)),
            Ok(s) => {
                debug!("{}", String::from_utf8_lossy(&s.stdout));
                debug!("{}", String::from_utf8_lossy(&s.stderr));
                if !s.status.success() {
                    io::stderr().write_all(&s.stderr).unwrap();
                    return Err(anyhow::anyhow!(
                        "rdf2hdt command returned non-zero status code: {:?}",
                        s.status.code().unwrap()
                    ));
                }
            }
        };

        Ok(())
    }
}
*/

// This should only be used by serve_query, where the server is never expected to have to perform conversions
pub struct NoopRdf2Hdt();
impl Rdf2Hdt for NoopRdf2Hdt {
    fn convert(&self, source: &Path, _dest: &Path) -> anyhow::Result<(), anyhow::Error> {
        if source.is_file() && source.extension().unwrap() == "hdt" {
            return Ok(());
        }
        error!("this mock rdf2hdt implementation should never be called");
        Err(anyhow::anyhow!("rdf2hdt is not implemented"))
    }
}
