// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::create;
use crate::rdf2nt::OxRdfConvert;
use crate::sparql;
use log::*;
use oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};
use oxrdfio::RdfFormat;
use oxrdfio::RdfParser;
use oxrdfio::RdfSerializer;
use reasonable::reasoner::Reasoner;
use sparesults::QueryResultsFormat;
use sparesults::QueryResultsSerializer;
use spareval::{QueryResults, QueryableDataset};
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile, tempdir};
use url::Url;

#[cfg(test)]
static TEST_TEMP_ROOT_OVERRIDE: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

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

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EntailmentMode {
    #[default]
    Off,
    OwlRl,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryExecutionOptions {
    pub debug_query_plan: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamedGraphBinding {
    pub graph_iri: String,
    pub data_file: String,
}

#[derive(Default)]
struct QueryDatasetLocalFiles {
    default_data_files: Vec<String>,
    named_graphs: Vec<NamedGraphBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DatasetExecutionKey {
    default_data_files: Vec<String>,
    named_graphs: Vec<NamedGraphBinding>,
    default_source_graphs: Vec<NamedGraphBinding>,
}

struct PreparedDataset {
    snapshot: sparql::AggregateHdtSnapshot,
    _cleanup_guard: QueryDirCleanup,
}

#[derive(Debug)]
struct PreparedQueryInputs {
    cleanup_dirs: Vec<String>,
    hdt_paths: Vec<String>,
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
    entailment_mode: EntailmentMode,
    out: &DeOutput,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    do_query_with_dataset_with_options(
        data_files,
        &[],
        query_files,
        entailment_mode,
        QueryExecutionOptions::default(),
        out,
        writer,
    )
    .await
}

/// Execute a list of sparql queries over a list of RDF files with optional named-graph bindings.
/// Non-HDT data files are converted to temporary HDT files before query execution.
pub async fn do_query_with_dataset<W: Write>(
    data_files: &[String],
    named_graph_bindings: &[NamedGraphBinding],
    query_files: &[String],
    entailment_mode: EntailmentMode,
    out: &DeOutput,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    do_query_with_dataset_with_options(
        data_files,
        named_graph_bindings,
        query_files,
        entailment_mode,
        QueryExecutionOptions::default(),
        out,
        writer,
    )
    .await
}

/// Execute a list of sparql queries over a list of RDF files with optional named-graph bindings.
/// Non-HDT data files are converted to temporary HDT files before query execution.
pub async fn do_query_with_dataset_with_options<W: Write>(
    data_files: &[String],
    named_graph_bindings: &[NamedGraphBinding],
    query_files: &[String],
    entailment_mode: EntailmentMode,
    options: QueryExecutionOptions,
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

    let mut prepared_by_key: HashMap<DatasetExecutionKey, usize> = HashMap::new();
    let mut prepared_datasets: Vec<PreparedDataset> = Vec::new();

    for rq in query_files {
        let query_path = PathBuf::from(rq);
        let mut f = File::open(&query_path)?;
        let mut buffer = String::new();
        f.read_to_string(&mut buffer)?;

        let (parsed_query, query_dataset_files) =
            parse_query_and_extract_dataset_local_files(&buffer, &query_path)?;
        let dataset_key =
            build_dataset_execution_key(data_files, named_graph_bindings, &query_dataset_files)?;

        let dataset_idx = if let Some(idx) = prepared_by_key.get(&dataset_key).copied() {
            idx
        } else {
            let mut dir_path_vec = Vec::new();
            let mut source_hdt_cache = HashMap::new();
            let mut hdt_path_vec = Vec::new();
            for source_file in &dataset_key.default_data_files {
                let hdt_path = prepare_source_hdt_path(
                    source_file,
                    entailment_mode,
                    &mut source_hdt_cache,
                    &mut dir_path_vec,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Error reading data files: {e}"))?;
                hdt_path_vec.push(hdt_path);
            }
            hdt_path_vec.sort_unstable();
            hdt_path_vec.dedup();

            let mut named_hdt_graphs = Vec::new();
            for binding in &dataset_key.named_graphs {
                let hdt_path = prepare_source_hdt_path(
                    &binding.data_file,
                    entailment_mode,
                    &mut source_hdt_cache,
                    &mut dir_path_vec,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Error reading named graph files: {e}"))?;
                named_hdt_graphs.push((binding.graph_iri.clone(), hdt_path));
            }
            for binding in &dataset_key.default_source_graphs {
                if named_hdt_graphs
                    .iter()
                    .any(|(iri, _)| iri == &binding.graph_iri)
                {
                    continue;
                }
                let hdt_path = prepare_source_hdt_path(
                    &binding.data_file,
                    entailment_mode,
                    &mut source_hdt_cache,
                    &mut dir_path_vec,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Error reading named graph files: {e}"))?;
                named_hdt_graphs.push((binding.graph_iri.clone(), hdt_path));
            }

            let cleanup_guard = QueryDirCleanup::new(dir_path_vec);
            named_hdt_graphs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            named_hdt_graphs.dedup();

            let dataset = sparql::AggregateHdt::new_with_mappings(&hdt_path_vec, &named_hdt_graphs)
                .map_err(|e| anyhow::anyhow!("error initializting HDT files: {e}"))?;
            let snapshot = dataset
                .get_snapshot(None)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            let idx = prepared_datasets.len();
            prepared_datasets.push(PreparedDataset {
                snapshot,
                _cleanup_guard: cleanup_guard,
            });
            prepared_by_key.insert(dataset_key, idx);
            idx
        };
        let prepared = &prepared_datasets[dataset_idx];
        run_query_against_dataset(parsed_query, &prepared.snapshot, out, writer, options)?;
    }
    writer.flush()?;

    Ok(())
}

/// Evaluate a parsed SPARQL query against any [`QueryableDataset`] and serialize the
/// results to `writer` using the requested output format.
pub fn run_query_against_dataset<'a, W, D>(
    parsed: Query,
    dataset: D,
    out: &DeOutput,
    writer: &mut BufWriter<W>,
    options: QueryExecutionOptions,
) -> anyhow::Result<()>
where
    W: Write,
    D: QueryableDataset<'a>,
{
    let qr = sparql::query_parsed_with_debug_plan(parsed, dataset, options.debug_query_plan)
        .map_err(|e| {
            error!("problem executing the hdt query: {e}");
            anyhow::anyhow!("{e}")
        })?;
    write_query_results(qr, out, writer)
}

fn write_query_results<W: Write>(
    qr: QueryResults<'_>,
    out: &DeOutput,
    writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    match qr {
        QueryResults::Solutions(query_solution_iter) => {
            let result_format = match out {
                DeOutput::CSV => QueryResultsFormat::Csv,
                DeOutput::TSV => QueryResultsFormat::Tsv,
                DeOutput::JSON => QueryResultsFormat::Json,
                DeOutput::XML => QueryResultsFormat::Xml,
                _ => {
                    error!("SELECT queries support only CSV, TSV, JSON, or XML");
                    return Err(anyhow::anyhow!(
                        "SELECT queries support only CSV, TSV, JSON, or XML"
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
                    warn!(
                        "CONSTRUCT and DESCRIBE queries only support NQ, NT, RDFXML, TRIG, and TTL formats. Defaulting to NTriple format"
                    );
                    RdfFormat::NTriples
                }
            };
            let mut serializer = RdfSerializer::from_format(result_format).for_writer(&mut *writer);
            for triple in query_triple_iter {
                let triple = triple?;
                serializer.serialize_triple(&triple)?
            }
            serializer.finish()?;
        }
    };
    Ok(())
}

/// Local-file references decoded from a SPARQL query's `FROM` and
/// `FROM NAMED` clauses. Each entry's `graph_iri` is the IRI exactly as it
/// appeared in the query (callers that need a canonical IRI re-derive it
/// themselves), and `data_file` is the percent-decoded local path
/// (lossy-converted on non-UTF-8 paths). Paths are returned without
/// canonicalization or symlink resolution — different downstream pipelines
/// have different opinions on whether to normalize, so this helper stays
/// out of that decision.
///
/// Non-`file://` IRIs are dropped; the helper only surfaces references that
/// resolve to a local path.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct QueryDatasetFiles {
    /// `FROM <file://...>` bindings, in query order, deduped by
    /// `(graph_iri, data_file)`.
    pub from_default: Vec<NamedGraphBinding>,
    /// `FROM NAMED <iri>` bindings whose IRI resolves to a local file,
    /// in query order, deduped by `(graph_iri, data_file)`.
    pub from_named: Vec<NamedGraphBinding>,
}

/// Parse `query_text` against `query_path`'s directory base IRI, returning
/// the parsed query and the local-file references its `FROM`/`FROM NAMED`
/// clauses pulled in. Exposed for downstream crates
/// so they don't have to reimplement the dataset
/// extraction themselves; they can use the returned bindings to feed their
/// own dpkg/mount pipeline.
pub fn parse_query_and_extract_dataset_files(
    query_text: &str,
    query_path: &Path,
) -> anyhow::Result<(spargebra::Query, QueryDatasetFiles)> {
    // Surface canonicalize/IO failure as a real error rather than papering
    // over it with a synthetic `http://example.com/` base. Callers that load
    // queries from disk have already proved the file exists upstream; if
    // canonicalize fails here it's a TOCTOU or permission problem the user
    // needs to know about, not a parser problem to swallow with a fake IRI.
    // Hand-authored queries that legitimately have no `query_path` (e.g. a
    // gRPC server's WHERE snippets) should call `query_base_iri` directly
    // and decide on their own fallback rather than going through this helper.
    let base_iri = query_base_iri(query_path).ok_or_else(|| {
        anyhow::anyhow!(
            "could not derive base IRI for query at {:?}: canonicalize failed",
            query_path
        )
    })?;
    let query = sparql::parse_query(query_text, &base_iri)
        .map_err(|e| anyhow::anyhow!("Invalid SPARQL query {:?}: {e}", query_path))?;

    let mut files = QueryDatasetFiles::default();
    let mut seen_default = HashSet::new();
    let mut seen_named = HashSet::new();

    if let Some(dataset) = query.dataset() {
        for iri in &dataset.default {
            if let Some(path) = file_uri_to_local_path(iri.as_str()) {
                let binding = NamedGraphBinding {
                    graph_iri: iri.as_str().to_string(),
                    data_file: path.to_string_lossy().into_owned(),
                };
                if seen_default.insert((binding.graph_iri.clone(), binding.data_file.clone())) {
                    files.from_default.push(binding);
                }
            }
        }
        if let Some(named) = &dataset.named {
            for iri in named {
                if let Some(path) = file_uri_to_local_path(iri.as_str()) {
                    let binding = NamedGraphBinding {
                        graph_iri: iri.as_str().to_string(),
                        data_file: path.to_string_lossy().into_owned(),
                    };
                    if seen_named.insert((binding.graph_iri.clone(), binding.data_file.clone())) {
                        files.from_named.push(binding);
                    }
                }
            }
        }
    }

    Ok((query, files))
}

fn parse_query_and_extract_dataset_local_files(
    query_text: &str,
    query_path: &Path,
) -> anyhow::Result<(spargebra::Query, QueryDatasetLocalFiles)> {
    // Apply de's path canonicalization on top of the shared raw-extraction
    // helper. `parse_query_and_extract_dataset_files` purposefully avoids
    // canonicalizing — pushing it here keeps the public API neutral while
    // preserving the canonical-path semantics de's `build_dataset_execution_key`
    // expects.
    let (query, raw) = parse_query_and_extract_dataset_files(query_text, query_path)?;

    let mut files = QueryDatasetLocalFiles::default();
    let mut seen_default = HashSet::new();
    let mut seen_named = HashSet::new();
    for binding in raw.from_default {
        let path = normalize_local_path_string(Path::new(&binding.data_file));
        if seen_default.insert(path.clone()) {
            files.default_data_files.push(path);
        }
    }
    for binding in raw.from_named {
        let path = normalize_local_path_string(Path::new(&binding.data_file));
        let normalized = NamedGraphBinding {
            graph_iri: binding.graph_iri,
            data_file: path,
        };
        if seen_named.insert((normalized.graph_iri.clone(), normalized.data_file.clone())) {
            files.named_graphs.push(normalized);
        }
    }

    Ok((query, files))
}

fn normalize_local_path_string(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn normalize_named_graph_binding(binding: NamedGraphBinding) -> NamedGraphBinding {
    NamedGraphBinding {
        graph_iri: binding.graph_iri,
        data_file: normalize_local_path_string(Path::new(&binding.data_file)),
    }
}

fn file_uri_to_local_path(uri: &str) -> Option<PathBuf> {
    let parsed = Url::parse(uri).ok()?;
    if parsed.scheme() != "file" {
        return None;
    }
    parsed.to_file_path().ok()
}

/// Returns the base IRI to use when parsing a SPARQL query loaded from
/// `query_path`, derived as `file://<canonical-parent-dir>/`.
///
/// This matches the W3C convention where relative IRIs in a query
/// (`<ng-01.ttl>`, `<friends.ttl>`, etc.) resolve against the directory the
/// query lives in. Callers that hand-author queries with no relative IRIs
/// (e.g., the gRPC server's `WHERE` snippets) can fall back to a synthetic
/// `http://example.com/` base when this returns `None`.
///
/// Exposed for downstream crates
pub fn query_base_iri(query_path: &Path) -> Option<String> {
    let canonical = query_path.canonicalize().ok()?;
    let parent = canonical.parent()?;
    Url::from_directory_path(parent)
        .ok()
        .map(|url| url.to_string())
}

fn local_path_to_file_uri(path: &Path) -> anyhow::Result<String> {
    crate::file_graph_uri_for_path(path)
}

fn build_dataset_execution_key(
    data_files: &[String],
    named_graph_bindings: &[NamedGraphBinding],
    query_dataset_files: &QueryDatasetLocalFiles,
) -> anyhow::Result<DatasetExecutionKey> {
    let mut default_data_files: Vec<String> = data_files
        .iter()
        .map(|f| normalize_local_path_string(Path::new(f)))
        .collect();
    for file in &query_dataset_files.default_data_files {
        default_data_files.push(normalize_local_path_string(Path::new(file)));
    }

    let mut named_graphs = named_graph_bindings
        .iter()
        .cloned()
        .map(normalize_named_graph_binding)
        .collect::<Vec<_>>();
    for binding in query_dataset_files.named_graphs.iter().cloned() {
        named_graphs.push(normalize_named_graph_binding(binding));
    }

    let mut default_source_graphs = Vec::new();
    // Preserve a stable graph IRI mapping for FROM <file://...> sources.
    // The query dataset may reference source file URIs, while execution runs on converted HDT files.
    // Adding these mappings keeps query-specified dataset IRIs resolvable during evaluation.
    for file in &query_dataset_files.default_data_files {
        let data_file = normalize_local_path_string(Path::new(file));
        let graph_iri = local_path_to_file_uri(Path::new(&data_file)).map_err(|e| {
            anyhow::anyhow!(
                "failed to derive graph IRI for default dataset source {data_file}: {e}"
            )
        })?;
        default_source_graphs.push(NamedGraphBinding {
            graph_iri,
            data_file,
        });
    }

    default_data_files.sort_unstable();
    default_data_files.dedup();
    named_graphs.sort_unstable_by(|a, b| {
        a.graph_iri
            .cmp(&b.graph_iri)
            .then(a.data_file.cmp(&b.data_file))
    });
    named_graphs.dedup();
    default_source_graphs.sort_unstable_by(|a, b| {
        a.graph_iri
            .cmp(&b.graph_iri)
            .then(a.data_file.cmp(&b.data_file))
    });
    default_source_graphs.dedup();

    Ok(DatasetExecutionKey {
        default_data_files,
        named_graphs,
        default_source_graphs,
    })
}

fn is_hdt_file_path(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("hdt"))
}

async fn prepare_source_hdt_path(
    source_file: &str,
    entailment_mode: EntailmentMode,
    source_hdt_cache: &mut HashMap<String, String>,
    dir_path_vec: &mut Vec<String>,
) -> anyhow::Result<String> {
    if let Some(cached) = source_hdt_cache.get(source_file) {
        return Ok(cached.clone());
    }

    let prepared = handle_files(vec![source_file.to_string()], entailment_mode).await?;
    dir_path_vec.extend(prepared.cleanup_dirs);
    let hdt_paths = prepared.hdt_paths;

    let resolved_hdt_path = match hdt_paths.len() {
        0 => {
            let (dir, path) = create_empty_hdt_for_named_graph()?;
            dir_path_vec.push(dir);
            path
        }
        1 => hdt_paths[0].clone(),
        _ => {
            return Err(anyhow::anyhow!(
                "multiple prepared HDT paths for source {:?}: {:?}",
                source_file,
                hdt_paths
            ));
        }
    };

    source_hdt_cache.insert(source_file.to_string(), resolved_hdt_path.clone());
    Ok(resolved_hdt_path)
}

async fn handle_files(
    files: Vec<String>,
    entailment_mode: EntailmentMode,
) -> anyhow::Result<PreparedQueryInputs> {
    let mut dir_path_vec: Vec<String> = vec![]; // Paths scheduled for cleanup via QueryDirCleanup
    let mut hdt_path_vec: Vec<String> = vec![]; // Paths to prepared/queryable HDT files
    let tmp_dir = query_work_dir_tempdir()?;
    let t_path = tmp_dir.path(); // Getting the tempdir path.

    // Creating TempFile to hold the hdt contents
    let mut rdf_tempfile: NamedTempFile = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(t_path)
        .map_err(|e| anyhow::anyhow!("Failed to create temporary RDF file in {:?}: {e}", t_path))?;

    let mut files_to_convert = vec![];
    for f in &files {
        if is_hdt_file_path(f) {
            hdt_path_vec.push(f.to_string())
        } else {
            files_to_convert.push(f.to_string());
        }
    }

    // Querying always merges all input data into one HDT for SPARQL evaluation,
    // so the multi-graph gate is intentionally bypassed here. No enrichers
    // are wired in this path, so `file_id_fn` is `None` — the
    // enrichers/file_id_fn invariant in `files_to_rdf` enforces the pairing.
    let result = match create::files_to_rdf(
        &files_to_convert,
        &mut rdf_tempfile,
        Arc::new(OxRdfConvert {}),
        &[],
        None,
        None,
        true,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => return Err(anyhow::anyhow!("error processing files to RDF {e}")),
    };

    for file in result.unknown_files.iter() {
        if !Path::new(file).exists() {
            return Err(anyhow::anyhow!("unable to locate local file {file}"));
        }
        if is_hdt_file_path(file) {
            hdt_path_vec.push(file.to_string())
        }
        // should be able to query plain rdf files directly
        else {
            return Err(anyhow::anyhow!("unrecognized file type: {file}"));
        }
    }

    let meta = std::fs::metadata(rdf_tempfile.path()).map_err(|e| {
        anyhow::anyhow!(
            "Error getting metadata for temporary RDF file {:?}: {e}",
            rdf_tempfile.path()
        )
    })?;

    let converted_rdf = if meta.len() == 0 {
        Path::new(&result.combined_rdf_path)
    } else {
        rdf_tempfile.path()
    };

    let had_rdf_input =
        meta.len() != 0 || rdf_tempfile.path() != Path::new(&result.combined_rdf_path);
    let mut source_for_hdt = if had_rdf_input {
        Some(converted_rdf.to_path_buf())
    } else {
        None
    };

    if entailment_mode == EntailmentMode::OwlRl {
        let entailment_source = source_for_hdt
            .clone()
            .unwrap_or_else(|| rdf_tempfile.path().to_path_buf());
        source_for_hdt = Some(
            materialize_entailment_closure_nt(&hdt_path_vec, &entailment_source, t_path)
                .map_err(|e| anyhow::anyhow!("entailment materialization failed: {e}"))?,
        );
        hdt_path_vec.clear();
    }

    if let Some(source_for_hdt) = source_for_hdt {
        // Creating TempFile to hold the hdt contents
        let named_tempfile: NamedTempFile = Builder::new()
            .suffix(".hdt")
            .append(true)
            .tempfile_in(t_path)
            .map_err(|e| {
                anyhow::anyhow!("Failed to create temporary HDT file in {:?}: {e}", t_path)
            })?;

        debug!("Running RDF2HDT");

        let converted_rdf_path = match source_for_hdt.to_str() {
            Some(path) => path,
            None => {
                return Err(anyhow::anyhow!(
                    "Temporary RDF path is not valid UTF-8: {:?}",
                    source_for_hdt
                ));
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
                        return Err(anyhow::anyhow!("failed to write converted HDT file: {e}"));
                    }
                }
                buf.flush().map_err(|e| {
                    anyhow::anyhow!(
                        "failed to flush converted HDT tempfile {:?}: {e}",
                        named_tempfile.path()
                    )
                })?;
                drop(buf);
            }
            Ok(Err(e)) => {
                return Err(anyhow::anyhow!(
                    "error converting plain RDF file {:?} to HDT: {e}",
                    rdf_tempfile.path()
                ));
            }
            Err(panic_err) => {
                let panic_msg = if let Some(msg) = panic_err.downcast_ref::<&str>() {
                    *msg
                } else if let Some(msg) = panic_err.downcast_ref::<String>() {
                    msg.as_str()
                } else {
                    "unknown panic while reading RDF"
                };
                return Err(anyhow::anyhow!(
                    "panic converting plain RDF file {:?} to HDT: {}",
                    rdf_tempfile.path(),
                    panic_msg
                ));
            }
        }
        let (_, persisted_hdt_path) = named_tempfile
            .keep()
            .map_err(|e| anyhow::anyhow!("failed to persist converted HDT tempfile: {e}"))?;
        hdt_path_vec.push(persisted_hdt_path.to_string_lossy().to_string());
        let persisted_tmp_dir = tmp_dir.keep();
        dir_path_vec.push(persisted_tmp_dir.to_string_lossy().to_string());
    }

    if hdt_path_vec.is_empty() {
        error!("no files to query")
    }
    Ok(PreparedQueryInputs {
        cleanup_dirs: dir_path_vec,
        hdt_paths: hdt_path_vec,
    })
}

fn query_work_dir_tempdir() -> anyhow::Result<tempfile::TempDir> {
    fn ensure_utf8_tempdir(dir: tempfile::TempDir) -> anyhow::Result<tempfile::TempDir> {
        if dir.path().to_str().is_none() {
            return Err(anyhow::anyhow!(
                "Error creating temporary working dir: UTF-8 path required, got {:?}",
                dir.path()
            ));
        }
        Ok(dir)
    }

    #[cfg(test)]
    {
        let maybe_root = TEST_TEMP_ROOT_OVERRIDE
            .lock()
            .map_err(|_| anyhow::anyhow!("temporary directory override lock poisoned"))?
            .clone();
        if let Some(root) = maybe_root {
            let dir = tempfile::tempdir_in(root)
                .map_err(|e| anyhow::anyhow!("Error creating temporary working dir: {:?}", e))?;
            return ensure_utf8_tempdir(dir);
        }
    }

    let dir =
        tempdir().map_err(|e| anyhow::anyhow!("Error creating temporary working dir: {:?}", e))?;
    ensure_utf8_tempdir(dir)
}

fn create_empty_hdt_for_named_graph() -> anyhow::Result<(String, String)> {
    let tmp_dir = query_work_dir_tempdir()?;
    let dir_path = tmp_dir
        .path()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("temporary working dir path is not valid UTF-8"))?
        .to_string();
    let empty_nt = Builder::new()
        .suffix(".nt")
        .append(true)
        .tempfile_in(tmp_dir.path())?;
    let hdt = hdt::Hdt::read_nt(empty_nt.path())
        .map_err(|e| anyhow::anyhow!("failed to create empty HDT for named graph: {e}"))?;
    let empty_hdt = Builder::new()
        .suffix(".hdt")
        .append(true)
        .tempfile_in(tmp_dir.path())?;
    let mut writer = BufWriter::new(&empty_hdt);
    hdt.write(&mut writer)?;
    writer.flush()?;
    drop(writer);
    let (_, persisted_hdt_path) = empty_hdt
        .keep()
        .map_err(|e| anyhow::anyhow!("failed to persist empty HDT tempfile: {e}"))?;
    let empty_hdt_path = persisted_hdt_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("empty HDT path is not valid UTF-8"))?
        .to_string();
    let _persisted_tmp_dir = tmp_dir.keep();
    Ok((dir_path, empty_hdt_path))
}

pub fn parse_named_graph_bindings(
    raw_bindings: &[String],
) -> anyhow::Result<Vec<NamedGraphBinding>> {
    let mut parsed = Vec::new();
    let mut seen = HashSet::new();
    for raw in raw_bindings {
        let (graph_iri_raw, data_file_raw) = raw.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("invalid --named-graph value {raw:?}; expected IRI=PATH")
        })?;
        let graph_iri_raw = graph_iri_raw.trim();
        let data_file = data_file_raw.trim();
        if graph_iri_raw.is_empty() || data_file.is_empty() {
            return Err(anyhow::anyhow!(
                "invalid --named-graph value {raw:?}; expected non-empty IRI and PATH"
            ));
        }
        let graph_iri = NamedNode::new(graph_iri_raw)
            .map_err(|e| anyhow::anyhow!("invalid --named-graph IRI {graph_iri_raw:?}: {e}"))?
            .into_string();
        let binding = NamedGraphBinding {
            graph_iri,
            data_file: data_file.to_string(),
        };
        if seen.insert((binding.graph_iri.clone(), binding.data_file.clone())) {
            parsed.push(binding);
        }
    }
    Ok(parsed)
}

/// Materialize the OWL-RL/RDFS entailment closure over the union of the given
/// HDT files plus an optional N-Triples file, and write the result to a fresh
/// `.entailed.nt` file inside `temp_dir`. The returned path is persisted (the
/// caller owns its lifetime via `temp_dir`).
///
/// `rdf_nt_path` may point at a non-existent file — in that case only the HDT
/// paths contribute to the closure.
///
/// Exposed for downstream crates
pub fn materialize_entailment_closure_nt(
    hdt_paths: &[String],
    rdf_nt_path: &Path,
    temp_dir: &Path,
) -> anyhow::Result<PathBuf> {
    // Cap the in-flight `Vec<Triple>` at CHUNK triples and feed the reasoner
    // in batches. Each flush drains the buffer into `reasoner.load_triples`,
    // which extends `Reasoner::base`/`input` (no `reason()` is called between
    // flushes, so `is_materialized` stays false and every batch takes the
    // `add_base_triples` path). A single `reason()` after all input is loaded
    // runs the datafrog fixpoint once over the full union, producing the
    // same closure as the previous "buffer everything, single load+reason"
    // pattern but with peak `Vec<Triple>` memory bounded by CHUNK regardless
    // of total input size.
    const CHUNK: usize = 1_000_000;

    let mut reasoner = Reasoner::new();
    let mut buf: Vec<Triple> = Vec::with_capacity(CHUNK);

    let flush = |buf: &mut Vec<Triple>, reasoner: &mut Reasoner| {
        if buf.is_empty() {
            return;
        }
        reasoner.load_triples(std::mem::take(buf));
        buf.reserve(CHUNK);
    };

    for path in hdt_paths {
        let hdt = hdt::HdtAny::open_with_threshold(Path::new(path), None)
            .map_err(|e| anyhow::anyhow!("failed to read HDT {path}: {e}"))?;
        for [s, p, o] in hdt.triples_all() {
            buf.push(hdt_raw_triple_to_oxrdf(&s, &p, &o)?);
            if buf.len() == CHUNK {
                flush(&mut buf, &mut reasoner);
            }
        }
    }

    if rdf_nt_path.exists() {
        let parser =
            RdfParser::from_format(RdfFormat::NTriples).for_reader(File::open(rdf_nt_path)?);
        for quad in parser {
            let quad = quad?;
            buf.push(Triple::new(quad.subject, quad.predicate, quad.object));
            if buf.len() == CHUNK {
                flush(&mut buf, &mut reasoner);
            }
        }
    }
    flush(&mut buf, &mut reasoner);
    reasoner.reason();

    let entailed_nt = Builder::new()
        .suffix(".entailed.nt")
        .tempfile_in(temp_dir)?;
    let entailed_path = entailed_nt.path().to_path_buf();
    let mut out = BufWriter::new(&entailed_nt);
    let mut serializer = RdfSerializer::from_format(RdfFormat::NTriples).for_writer(&mut out);
    for triple in reasoner.view_output() {
        serializer.serialize_triple(triple.as_ref())?;
    }
    serializer.finish()?;
    drop(out);
    let _ = entailed_nt.keep()?;

    Ok(entailed_path)
}

fn hdt_raw_triple_to_oxrdf(s: &str, p: &str, o: &str) -> anyhow::Result<Triple> {
    let subject_term = sparql::hdt_bgp_str_to_term(s)
        .map_err(|e| anyhow::anyhow!("failed to parse HDT subject term {s:?}: {e}"))?;
    let predicate_term = sparql::hdt_bgp_str_to_term(p)
        .map_err(|e| anyhow::anyhow!("failed to parse HDT predicate term {p:?}: {e}"))?;
    let object_term = sparql::hdt_bgp_str_to_term(o)
        .map_err(|e| anyhow::anyhow!("failed to parse HDT object term {o:?}: {e}"))?;

    let subject = match subject_term {
        Term::NamedNode(node) => NamedOrBlankNode::from(node),
        Term::BlankNode(node) => NamedOrBlankNode::from(node),
        Term::Literal(_) => {
            return Err(anyhow::anyhow!(
                "invalid literal subject in HDT triple: {s:?}"
            ));
        }
    };
    let predicate = match predicate_term {
        Term::NamedNode(node) => node,
        _ => {
            return Err(anyhow::anyhow!(
                "invalid non-IRI predicate in HDT triple: {p:?}"
            ));
        }
    };

    Ok(Triple::new(subject, predicate, object_term))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs;
    use std::io::{self, BufWriter, Write};
    use tempfile::tempdir;
    use tokio::sync::{Mutex, MutexGuard};
    use url::Url;

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

    struct TempRootOverrideGuard(Option<PathBuf>);

    impl TempRootOverrideGuard {
        fn new(override_root: Option<PathBuf>) -> anyhow::Result<Self> {
            let mut guard = TEST_TEMP_ROOT_OVERRIDE
                .lock()
                .map_err(|_| anyhow::anyhow!("temporary directory override lock poisoned"))?;
            let previous = guard.clone();
            *guard = override_root;
            Ok(Self(previous))
        }
    }

    impl Drop for TempRootOverrideGuard {
        fn drop(&mut self) {
            if let Ok(mut guard) = TEST_TEMP_ROOT_OVERRIDE.lock() {
                *guard = self.0.take();
            }
        }
    }

    fn dir_entries(path: &Path) -> anyhow::Result<HashSet<String>> {
        Ok(fs::read_dir(path)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|f| f.is_dir()))
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect())
    }

    #[tokio::test]
    async fn test_do_query_cleans_tmp_on_serialize_failure() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let tmp_root = work_dir.path().join("tmp");
        fs::create_dir(&tmp_root)?;

        let _tmpdir_guard = TempRootOverrideGuard::new(Some(tmp_root.clone()))?;

        let data_path = work_dir.path().join("dataset.nt");
        fs::write(
            &data_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;

        let query_path = work_dir.path().join("query.rq");
        fs::write(&query_path, "SELECT * WHERE { ?s ?p ?o }")?;

        let before = dir_entries(&tmp_root)?;

        let data_files = vec![data_path.to_string_lossy().to_string()];
        let query_files = vec![query_path.to_string_lossy().to_string()];
        let mut writer = BufWriter::new(FailingWriter);
        let res = do_query(
            &data_files,
            &query_files,
            EntailmentMode::Off,
            &DeOutput::CSV,
            &mut writer,
        )
        .await;
        assert!(res.is_err());

        let after = dir_entries(&tmp_root)?;
        let leaked = after.difference(&before).cloned().collect::<Vec<_>>();
        assert!(
            leaked.is_empty(),
            "query leaked temporary directory entries: {leaked:?}"
        );

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

        let err = handle_files(
            vec![invalid_nt.to_string_lossy().to_string()],
            EntailmentMode::Off,
        )
        .await
        .expect_err("handle_files should fail when RDF -> HDT conversion fails");
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

        let _tmpdir_guard =
            TempRootOverrideGuard::new(Some(invalid_tmp.clone())).expect("failed to set tmp dir");

        let dataset = data_dir.path().join("dataset.nt");
        fs::write(
            &dataset,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )
        .expect("failed to write dataset");

        let files = vec![dataset.to_string_lossy().to_string()];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(handle_files(files, EntailmentMode::Off))
        }));

        assert!(result.is_ok(), "handle_files should not panic");
        let err = result
            .expect("handle_files panicked")
            .expect_err("non-utf8 temporary path should become an error");
        assert!(
            err.to_string().contains("UTF-8"),
            "Expected UTF-8 related temporary path error"
        );
    }

    #[tokio::test]
    async fn test_named_graph_binding_does_not_mutate_hdt_file() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let data_path = work_dir.path().join("dataset.nt");
        fs::write(
            &data_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;
        let hdt_path = work_dir.path().join("dataset.hdt");
        create::do_create(
            hdt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid HDT path"))?,
            &[data_path.to_string_lossy().to_string()],
        )
        .await?;

        let query_path = work_dir.path().join("query.rq");
        fs::write(
            &query_path,
            "SELECT ?s WHERE { GRAPH <http://example.org/g> { ?s ?p ?o } }",
        )?;

        let before = fs::read(&hdt_path)?;
        let named_bindings = vec![NamedGraphBinding {
            graph_iri: "http://example.org/g".to_string(),
            data_file: hdt_path.to_string_lossy().to_string(),
        }];
        let mut output = Vec::new();
        {
            let mut writer = BufWriter::new(&mut output);
            do_query_with_dataset(
                &[],
                &named_bindings,
                &[query_path.to_string_lossy().to_string()],
                EntailmentMode::Off,
                &DeOutput::CSV,
                &mut writer,
            )
            .await?;
        }
        let after = fs::read(&hdt_path)?;
        assert_eq!(before, after, "query should not mutate input HDT files");

        let rendered = String::from_utf8(output)?;
        assert!(rendered.contains("http://example.org/s"));
        Ok(())
    }

    #[tokio::test]
    async fn test_entailment_mode_applies_to_hdt_only_inputs() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let data_path = work_dir.path().join("dataset.nt");
        fs::write(
            &data_path,
            "<http://ex/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/B> .\n\
             <http://ex/B> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://ex/C> .\n",
        )?;
        let hdt_path = work_dir.path().join("dataset.hdt");
        create::do_create(
            hdt_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid HDT path"))?,
            &[data_path.to_string_lossy().to_string()],
        )
        .await?;

        let query_path = work_dir.path().join("query.rq");
        fs::write(
            &query_path,
            "SELECT ?o WHERE { <http://ex/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }",
        )?;
        let data_files = vec![hdt_path.to_string_lossy().to_string()];
        let query_files = vec![query_path.to_string_lossy().to_string()];

        let mut off_out = Vec::new();
        {
            let mut writer = BufWriter::new(&mut off_out);
            do_query(
                &data_files,
                &query_files,
                EntailmentMode::Off,
                &DeOutput::CSV,
                &mut writer,
            )
            .await?;
        }

        let mut on_out = Vec::new();
        {
            let mut writer = BufWriter::new(&mut on_out);
            do_query(
                &data_files,
                &query_files,
                EntailmentMode::OwlRl,
                &DeOutput::CSV,
                &mut writer,
            )
            .await?;
        }

        let off = String::from_utf8(off_out)?;
        let on = String::from_utf8(on_out)?;
        assert!(off.contains("http://ex/B"));
        assert!(!off.contains("http://ex/C"));
        assert!(on.contains("http://ex/C"));
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_graph_iri_metadata_in_data_files_errors() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let a_nt = work_dir.path().join("a.nt");
        let b_nt = work_dir.path().join("b.nt");
        fs::write(&a_nt, "<http://ex/s1> <http://ex/p> <http://ex/o1> .\n")?;
        fs::write(&b_nt, "<http://ex/s2> <http://ex/p> <http://ex/o2> .\n")?;

        let a_hdt = work_dir.path().join("a.hdt");
        let b_hdt = work_dir.path().join("b.hdt");
        create::do_create_with_options(
            Some(
                a_hdt
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid HDT path"))?,
            ),
            &[a_nt.to_string_lossy().to_string()],
            false,
            Some("http://ex/g"),
            &[],
            None,
            None,
        )
        .await?;
        create::do_create_with_options(
            Some(
                b_hdt
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid HDT path"))?,
            ),
            &[b_nt.to_string_lossy().to_string()],
            false,
            Some("http://ex/g"),
            &[],
            None,
            None,
        )
        .await?;

        let query_path = work_dir.path().join("query.rq");
        fs::write(&query_path, "SELECT ?s WHERE { ?s <http://ex/p> ?o }")?;
        let data_files = vec![
            a_hdt.to_string_lossy().to_string(),
            b_hdt.to_string_lossy().to_string(),
        ];
        let query_files = vec![query_path.to_string_lossy().to_string()];

        let mut out = Vec::new();
        let mut writer = BufWriter::new(&mut out);
        let result = do_query(
            &data_files,
            &query_files,
            EntailmentMode::Off,
            &DeOutput::CSV,
            &mut writer,
        )
        .await;
        assert!(result.is_err());
        let msg = result
            .expect_err("expected duplicate graph IRI error")
            .to_string();
        assert!(msg.contains("duplicate graph IRI"));
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_optional_query_filters_rows_when_graph_variable_is_reused_in_optional()
    -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let data_path = work_dir.path().join("dataset.nt");
        fs::write(&data_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")?;

        let query_path = work_dir.path().join("query.rq");
        fs::write(
            &query_path,
            "SELECT ?g ?o WHERE { GRAPH ?g { ?s <http://ex/p> ?o OPTIONAL { ?s <http://ex/p> ?g } } }",
        )?;

        let named_bindings = vec![NamedGraphBinding {
            graph_iri: "http://ex/g1".to_string(),
            data_file: data_path.to_string_lossy().to_string(),
        }];
        let mut output = Vec::new();
        {
            let mut writer = BufWriter::new(&mut output);
            do_query_with_dataset(
                &[],
                &named_bindings,
                &[query_path.to_string_lossy().to_string()],
                EntailmentMode::Off,
                &DeOutput::CSV,
                &mut writer,
            )
            .await?;
        }
        let rendered = String::from_utf8(output)?;
        let lines = rendered
            .lines()
            .map(|line| line.trim_end_matches('\r'))
            .collect::<Vec<_>>();
        assert_eq!(lines, vec!["g,o"]);
        Ok(())
    }

    #[test]
    fn test_parse_named_graph_bindings_rejects_invalid_iri() {
        let err = parse_named_graph_bindings(&["not an iri=tests/resources/fruit.nt".to_string()])
            .expect_err("expected invalid IRI");
        assert!(
            err.to_string().contains("invalid --named-graph IRI"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_named_graph_bindings_deduplicates_exact_entries() -> anyhow::Result<()> {
        let parsed = parse_named_graph_bindings(&[
            "http://example.org/g=tests/resources/fruit.nt".to_string(),
            "http://example.org/g=tests/resources/fruit.nt".to_string(),
        ])?;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].graph_iri, "http://example.org/g");
        assert_eq!(parsed[0].data_file, "tests/resources/fruit.nt");
        Ok(())
    }

    #[tokio::test]
    async fn test_from_default_graph_is_not_exposed_as_named_graph() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let data_path = work_dir.path().join("dataset.nt");
        fs::write(&data_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")?;

        let data_uri = Url::from_file_path(&data_path)
            .map_err(|_| anyhow::anyhow!("failed to build file URI for test dataset"))?
            .to_string();

        let query_path = work_dir.path().join("query.rq");
        fs::write(
            &query_path,
            format!("SELECT ?g FROM <{data_uri}> WHERE {{ GRAPH ?g {{ ?s ?p ?o }} }}"),
        )?;

        let mut output = Vec::new();
        {
            let mut writer = BufWriter::new(&mut output);
            do_query(
                &[],
                &[query_path.to_string_lossy().to_string()],
                EntailmentMode::Off,
                &DeOutput::CSV,
                &mut writer,
            )
            .await?;
        }

        let rendered = String::from_utf8(output)?;
        let lines = rendered
            .lines()
            .map(|line| line.trim_end_matches('\r'))
            .collect::<Vec<_>>();
        assert_eq!(lines, vec!["g"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_from_and_from_named_same_file_do_not_conflict() -> anyhow::Result<()> {
        let _tmpdir_lock = lock_tmpdir_async().await;
        let work_dir = tempdir()?;
        let data_path = work_dir.path().join("dataset.nt");
        fs::write(&data_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")?;

        let data_uri = Url::from_file_path(&data_path)
            .map_err(|_| anyhow::anyhow!("failed to build file URI for test dataset"))?
            .to_string();

        let query_path = work_dir.path().join("query.rq");
        fs::write(
            &query_path,
            format!(
                "SELECT ?s FROM <{data_uri}> FROM NAMED <{data_uri}> \
                 WHERE {{ {{ ?s ?p ?o }} UNION {{ GRAPH ?g {{ ?s ?p ?o }} }} }}"
            ),
        )?;

        let mut output = Vec::new();
        {
            let mut writer = BufWriter::new(&mut output);
            do_query(
                &[],
                &[query_path.to_string_lossy().to_string()],
                EntailmentMode::Off,
                &DeOutput::CSV,
                &mut writer,
            )
            .await?;
        }

        let rendered = String::from_utf8(output)?;
        assert!(rendered.starts_with("s"), "unexpected output: {rendered}");
        Ok(())
    }

    #[test]
    fn test_file_uri_to_local_path_decodes_percent_escaped_paths() -> anyhow::Result<()> {
        let work_dir = tempdir()?;
        let path_with_space = work_dir.path().join("with space.nt");
        fs::write(
            &path_with_space,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;
        let uri = Url::from_file_path(&path_with_space)
            .map_err(|_| anyhow::anyhow!("failed to build file URI"))?
            .to_string();

        let resolved = file_uri_to_local_path(&uri)
            .ok_or_else(|| anyhow::anyhow!("file URI should resolve to local path"))?;
        assert_eq!(resolved, path_with_space);
        Ok(())
    }
}
