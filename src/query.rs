// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::create;
use crate::rdf2nt::OxRdfConvert;
use crate::sparql;
use anyhow::Error;
use log::*;
use oxrdfio::RdfFormat;
use oxrdfio::RdfSerializer;
use sparesults::QueryResultsFormat;
use sparesults::QueryResultsSerializer;
use spareval::QueryResults;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, Builder, NamedTempFile};

#[derive(clap::ValueEnum, Clone, Default, Debug, PartialEq)]
pub enum DeOutput {
    #[default]
    /// <https://www.w3.org/TR/sparql11-results-csv-tsv/>
    CSV,

    /// <https://www.w3.org/TR/sparql11-results-csv-tsv/>
    TSV,

    /// <https://www.w3.org/TR/sparql11-results-json/>
    JSON,

    /// <https://www.w3.org/TR/rdf-sparql-XMLres>
    XML,

    /// <https://w3c.github.io/N3/spec/>
    N3,

    /// <https://www.w3.org/TR/n-quads/>
    NQUADS,

    /// <https://www.w3.org/TR/rdf-syntax-grammar/>
    RDFXML,

    /// <https://www.w3.org/TR/n-triples/>
    NTRIPLE,

    /// <https://www.w3.org/TR/trig/>
    TRIG,

    /// <https://www.w3.org/TR/turtle/>
    TURTLE,
}

struct QueryDirCleanup {
    dirs: Option<Vec<String>>,
}

impl QueryDirCleanup {
    fn new(dirs: Vec<String>) -> Self {
        Self { dirs: Some(dirs) }
    }
}

impl Drop for QueryDirCleanup {
    fn drop(&mut self) {
        if let Some(dirs) = self.dirs.take() {
            for dir in dirs.iter() {
                if let Err(e) = std::fs::remove_dir_all(dir) {
                    error!("Failed to remove directory {dir:?}: {e:?}");
                }
            }
        }
    }
}
/// Execute a list of sparql queries over a list of RDF files. Non-HDT data files are converted to temporary HDT files before query execution
pub async fn do_query<W: Write>(
    data_files: &[String],
    query_files: &[String],
    out: &DeOutput,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    debug!("Executing querying ...");

    // fail fast on input validation
    for rq in query_files {
        let path = Path::new(&rq);
        if !path.exists() {
            error!("query file {rq:?} could not be found on local machine");
            return Err(anyhow::anyhow!(
                "query file {:?} could not be found on local machine",
                rq
            ));
        }
    }

    let (dir_path_vec, hdt_path_vec, e) = handle_files(data_files.to_owned()).await;
    let _cleanup_guard = QueryDirCleanup::new(dir_path_vec);

    if let Some(e) = e {
        return Err(anyhow::anyhow!("Error reading data files: {e}",));
    }

    let dataset = sparql::AggregateHdt::new(&hdt_path_vec)
        .map_err(|e| anyhow::anyhow!("error initializting HDT files: {e}"))?;
    let snapshot = dataset
        .get_snapshot(None)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    for rq in query_files {
        let mut f = File::open(rq)?;
        let mut buffer = String::new();

        f.read_to_string(&mut buffer)?;
        let qr = match sparql::query(&buffer, &snapshot, None) {
            Ok(r) => r,
            Err(e) => {
                error!("problem executing the hdt query: {e}");
                return Err(anyhow::anyhow!("{e}"));
            }
        };

        match qr {
            QueryResults::Solutions(query_solution_iter) => {
                let result_format = match out {
                    DeOutput::CSV => QueryResultsFormat::Csv,
                    DeOutput::TSV => QueryResultsFormat::Tsv,
                    DeOutput::JSON => QueryResultsFormat::Json,
                    DeOutput::XML => QueryResultsFormat::Xml,
                    _ => {
                        error!("ASK queries support only CSV, TSV, JSON, or XML");
                        return Err(anyhow::anyhow!(
                            "ASK queries support only CSV, TSV, JSON, or XML"
                        ));
                    }
                };
                let results_writer = QueryResultsSerializer::from_format(result_format);
                let mut serializer = results_writer.serialize_solutions_to_writer(
                    &mut *writer,
                    query_solution_iter.variables().into(),
                )?;
                for s in query_solution_iter {
                    let s = s?;
                    serializer.serialize(&s).map_err(|e| {
                        error!("error serializing query solutions to desired output format: {e}");
                        anyhow::anyhow!(
                            "error serializing query solutions to desired output format: {e}"
                        )
                    })?;
                }
                serializer.finish()?;
            }
            QueryResults::Boolean(result) => {
                let result_format = match out {
                    DeOutput::CSV => QueryResultsFormat::Csv,
                    DeOutput::TSV => QueryResultsFormat::Tsv,
                    DeOutput::JSON => QueryResultsFormat::Json,
                    DeOutput::XML => QueryResultsFormat::Xml,
                    _ => {
                        warn!(
                            "ASK queries support only CSV, TSV, JSON, or XML. Defaulting to CSV format"
                        );
                        QueryResultsFormat::Csv
                    }
                };
                let results_writer = QueryResultsSerializer::from_format(result_format);
                results_writer
                    .serialize_boolean_to_writer(&mut *writer, result)
                    .map_err(|e| {
                        error!("error serializing query solutions to desired output format: {e}");
                        anyhow::anyhow!(
                            "error serializing query solutions to desired output format: {e}"
                        )
                    })?;
            }
            QueryResults::Graph(query_triple_iter) => {
                let result_format = match out {
                    DeOutput::N3 => RdfFormat::N3,
                    DeOutput::NQUADS => RdfFormat::NQuads,
                    DeOutput::NTRIPLE => RdfFormat::NTriples,
                    DeOutput::RDFXML => RdfFormat::RdfXml,
                    DeOutput::TRIG => RdfFormat::TriG,
                    DeOutput::TURTLE => RdfFormat::Turtle,
                    _ => {
                        warn!("CONSTRUCT and DESCRIBE queries only support NQ, NT, RDFXML, TRIG, and TTL formats. Defaulting to NTriple format");
                        RdfFormat::NTriples
                    }
                };
                let mut serializer =
                    RdfSerializer::from_format(result_format).for_writer(&mut *writer);
                for triple in query_triple_iter {
                    let triple = triple?;
                    serializer.serialize_triple(&triple)?
                }
                serializer.finish()?;
            }
        };
    }
    writer.flush()?;

    Ok(())
}

async fn handle_files(files: Vec<String>) -> (Vec<String>, Vec<String>, Option<anyhow::Error>) {
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
    let t_path = tmp_dir.path(); // Getting the tempdir path.

    // Creating TempFile to hold the hdt contents
    let mut rdf_tempfile: NamedTempFile = match Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(t_path)
    {
        Ok(tf) => tf,
        Err(e) => {
            return (
                dir_path_vec,
                hdt_path_vec,
                Some(anyhow::anyhow!(
                    "Failed to create temporary RDF file in {:?}: {e}",
                    t_path
                )),
            );
        }
    };

    let mut files_to_convert = vec![];
    for f in &files {
        if f.ends_with(".hdt") {
            hdt_path_vec.push(f.to_string())
        } else {
            files_to_convert.push(f.to_string());
        }
    }

    let (combined_rdf_path, unknown_files) = match create::files_to_rdf(
        &files_to_convert,
        &mut rdf_tempfile,
        Arc::new(OxRdfConvert {}),
    ) {
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
            hdt_path_vec.push(file.to_string())
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

    let meta = match std::fs::metadata(rdf_tempfile.path()) {
        Ok(m) => m,
        Err(e) => {
            return (
                dir_path_vec,
                hdt_path_vec,
                Some(anyhow::anyhow!(
                    "Error getting metadata for temporary RDF file {:?}: {e}",
                    rdf_tempfile.path()
                )),
            );
        }
    };

    let converted_rdf = if meta.len() == 0 {
        Path::new(&combined_rdf_path)
    } else {
        rdf_tempfile.path()
    };

    if meta.len() != 0 || rdf_tempfile.path() != Path::new(&combined_rdf_path) {
        // Creating TempFile to hold the hdt contents
        let named_tempfile: NamedTempFile = match Builder::new()
            .suffix(".hdt")
            .append(true)
            .tempfile_in(t_path)
        {
            Ok(tf) => tf,
            Err(e) => {
                return (
                    dir_path_vec,
                    hdt_path_vec,
                    Some(anyhow::anyhow!(
                        "Failed to create temporary HDT file in {:?}: {e}",
                        t_path
                    )),
                );
            }
        };

        debug!("Running RDF2HDT");

        let converted_rdf_path = match converted_rdf.to_str() {
            Some(path) => path,
            None => {
                return (
                    dir_path_vec,
                    hdt_path_vec,
                    Some(anyhow::anyhow!(
                        "Temporary RDF path is not valid UTF-8: {:?}",
                        converted_rdf
                    )),
                );
            }
        };
        let hdt_conversion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            hdt::Hdt::read_nt(Path::new(converted_rdf_path))
        }));

        match hdt_conversion {
            Ok(Ok(hdt_conv)) => {
                let mut buf = BufWriter::new(&named_tempfile);
                match hdt_conv.write(&mut buf) {
                    Ok(_) => {}
                    Err(e) => {
                        return (
                            dir_path_vec,
                            hdt_path_vec,
                            Some(anyhow::anyhow!("failed to write converted HDT file: {e}")),
                        );
                    }
                }
            }
            Ok(Err(e)) => {
                return (
                    dir_path_vec,
                    hdt_path_vec,
                    Some(anyhow::anyhow!(
                        "error converting plain RDF file {:?} to HDT: {e}",
                        rdf_tempfile.path()
                    )),
                );
            }
            Err(panic_err) => {
                let panic_msg = if let Some(msg) = panic_err.downcast_ref::<&str>() {
                    *msg
                } else if let Some(msg) = panic_err.downcast_ref::<String>() {
                    msg.as_str()
                } else {
                    "unknown panic while reading RDF"
                };
                return (
                    dir_path_vec,
                    hdt_path_vec,
                    Some(anyhow::anyhow!(
                        "panic converting plain RDF file {:?} to HDT: {}",
                        rdf_tempfile.path(),
                        panic_msg
                    )),
                );
            }
        }
        hdt_path_vec.push(named_tempfile.path().to_string_lossy().to_string());
        let _ = named_tempfile.keep();
        dir_path_vec.push(t_path.to_string_lossy().to_string());
        let _ = tmp_dir.keep();
    }

    if hdt_path_vec.is_empty() {
        error!("no files to query")
    }
    (dir_path_vec, hdt_path_vec, None)
}

// performs directory removal for a list of directories
pub async fn file_cleanup(dirs: Vec<String>) {
    debug!("Cleaning up environment");
    for dir in dirs.iter() {
        if let Err(e) = fs::remove_dir_all(dir) {
            error!("Failed to remove directory {dir:?}: {e:?}")
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::io::{self, BufWriter, Write};
    use tempfile::tempdir;
    use tokio::sync::{Mutex, MutexGuard};

    static TMPDIR_LOCK: Mutex<()> = Mutex::const_new(());

    async fn lock_tmpdir_async() -> MutexGuard<'static, ()> {
        TMPDIR_LOCK.lock().await
    }

    fn lock_tmpdir_sync() -> MutexGuard<'static, ()> {
        TMPDIR_LOCK.blocking_lock()
    }

    #[derive(Default)]
    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("intentional test write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("intentional test write failure"))
        }
    }

    struct TmpDirEnvGuard(Option<OsString>);

    impl TmpDirEnvGuard {
        fn new(prev_tmpdir: Option<OsString>) -> Self {
            Self(prev_tmpdir)
        }
    }

    impl Drop for TmpDirEnvGuard {
        fn drop(&mut self) {
            match self.0.take() {
                Some(v) => env::set_var("TMPDIR", v),
                None => env::remove_var("TMPDIR"),
            }
        }
    }

    fn dir_count(path: &str) -> anyhow::Result<usize> {
        Ok(fs::read_dir(path)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|f| f.is_dir()))
            .count())
    }

    #[tokio::test]
    async fn test_do_query_cleans_tmp_on_serialize_failure() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let tmp_root = work_dir.path().join("tmp");
        fs::create_dir(&tmp_root)?;

        let prev_tmpdir = env::var_os("TMPDIR");
        env::set_var("TMPDIR", &tmp_root);
        let _tmpdir_guard = TmpDirEnvGuard::new(prev_tmpdir);

        let data_path = work_dir.path().join("dataset.nt");
        fs::write(
            &data_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        let query_path = work_dir.path().join("query.rq");
        fs::write(&query_path, "SELECT * WHERE { ?s ?p ?o }")?;

        let before = dir_count(&tmp_root.to_string_lossy())?;

        let data_files = vec![data_path.to_string_lossy().to_string()];
        let query_files = vec![query_path.to_string_lossy().to_string()];
        let mut writer = BufWriter::new(FailingWriter);
        let res = do_query(&data_files, &query_files, &DeOutput::CSV, &mut writer).await;
        assert!(res.is_err());

        let after = dir_count(&tmp_root.to_string_lossy())?;
        assert_eq!(before, after);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_files_returns_error_on_rdf_to_hdt_failure() {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = match tempdir() {
            Ok(d) => d,
            Err(e) => panic!("failed to create temp dir: {e}"),
        };

        let invalid_nt = work_dir.path().join("invalid.nt");
        match fs::write(&invalid_nt, "invalid triple line") {
            Ok(()) => {}
            Err(e) => panic!("failed to write invalid dataset: {e}"),
        };

        let (dir_path_vec, hdt_path_vec, err) =
            handle_files(vec![invalid_nt.to_string_lossy().to_string()]).await;
        assert!(err.is_some());
        assert!(hdt_path_vec.is_empty());
        assert!(dir_path_vec.is_empty());
        let err = err.expect("handle_files should fail when RDF -> HDT conversion fails");
        assert!(
            err.to_string().contains("converting plain RDF file")
                || err.to_string().contains("Error converting file(s) to NT"),
            "Expected file conversion or HDT conversion failure"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_handle_files_rejects_non_utf8_tmpdir_without_panic() {
        let _tmpdir_lock = lock_tmpdir_sync();
        use std::os::unix::ffi::OsStringExt;
        use std::process::id;

        let invalid_tmp = {
            let mut name = b"de-non-utf8-tmp-".to_vec();
            name.extend(id().to_string().as_bytes());
            name.push(0xFF);
            let mut path = std::env::temp_dir();
            path.push(std::ffi::OsString::from_vec(name));
            path
        };

        let data_dir = tempdir().expect("failed to create temp dir");
        let _ = fs::remove_dir_all(&invalid_tmp);
        fs::create_dir(&invalid_tmp).expect("failed to create non-utf8 tmp dir");

        let prev_tmpdir = env::var_os("TMPDIR");
        env::set_var("TMPDIR", &invalid_tmp);
        let _tmpdir_guard = TmpDirEnvGuard::new(prev_tmpdir);

        let dataset = data_dir.path().join("dataset.nt");
        fs::write(
            &dataset,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )
        .expect("failed to write dataset");

        let files = vec![dataset.to_string_lossy().to_string()];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(handle_files(files))
        }));

        assert!(result.is_ok(), "handle_files should not panic");
        let (_dir_paths, _hdt_paths, err) = result.expect("handle_files panicked");
        let err = err.expect("non-utf8 temporary path should become an error");
        assert!(
            err.to_string().contains("UTF-8"),
            "Expected UTF-8 related temporary path error"
        );
    }
}
