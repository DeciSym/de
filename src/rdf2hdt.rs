// file handles calls to rdf2hdt library

use log::*;
use std::io::{self, Write};
use std::{path::Path, process::Command};
pub trait Rdf2Hdt: Sync + Send {
    fn convert(&self, source: &Path, dest: &Path) -> anyhow::Result<(), anyhow::Error>;
}

pub struct Rdf2HdtImpl();

impl Rdf2Hdt for Rdf2HdtImpl {
    fn convert(&self, source: &Path, dest: &Path) -> anyhow::Result<(), anyhow::Error> {
        debug!("Running RDF2HDT binary");
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

        return Ok(());
    }
}

#[cfg(test)]
pub mod mock {

    use super::Rdf2Hdt;
    use std::path::Path;

    pub struct MockRdf2Hdt();
    impl Rdf2Hdt for MockRdf2Hdt {
        fn convert(&self, _source: &Path, _dest: &Path) -> anyhow::Result<(), anyhow::Error> {
            return Ok(());
        }
    }
}
