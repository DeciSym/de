// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use clap::{Parser, Subcommand};
use de::*;
use log::error;
use std::io::{BufWriter, Write, stdout};

#[derive(Parser)]
#[command(author, version, about="CLI tool for creating and querying HDT files", long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a HDT file from source data
    Create {
        #[clap(short, long)]
        /// Name of output file. File extension should be .hdt
        output_name: String,
        #[clap(short, long, num_args = 1..)]
        /// Path to data files to be added to Graph (Acceptable inputs are as follows: RDF)
        data: Vec<String>,
        #[clap(long, default_value_t = false)]
        /// Explicitly allow merging multiple named graphs into one output HDT graph
        allow_merge_named_graphs: bool,
        #[clap(long)]
        /// Optional graph IRI metadata to store in the HDT header
        graph_iri: Option<String>,
    },
    /// Query HDT and RDF files using SPARQL query format
    Query {
        #[clap(short, long, num_args = 1..)]
        /// local HDT and RDF files to be queried
        data: Vec<String>,
        #[clap(long, num_args = 1..)]
        /// Named graph bindings in IRI=PATH format (repeatable)
        named_graph: Vec<String>,
        #[clap(short, long, num_args = 1.., required = true)]
        /// Path to SPARQL query file. (should end in .rq)
        sparql: Vec<String>,
        #[clap(long, default_value_t, value_enum)]
        /// Entailment mode used during query data preparation
        entailment: query::EntailmentMode,
        #[clap(long, default_value_t = false)]
        /// Print query plan JSON to stderr for each query
        debug_query_plan: bool,
        /// Output to return the query results as using https://docs.rs/oxigraph/0.4.3/oxigraph/sparql/results/enum.QueryResultsFormat.html and https://crates.io/crates/oxrdfio
        #[clap(short, long, default_value_t, value_enum)]
        output: query::DeOutput,
    },
    /// Start a server to listen for /sparql, /update and /store API's. HDT's are read-only
    /// per spec, so new graphs (i.e. files) can be uploaded, but existing graphs can NOT be
    /// modified or deleted through /update or /store overwrite/delete requests.
    #[cfg(feature = "server")]
    Serve {
        /// Directory in which the data should be persisted
        ///
        /// If not present, an in-memory storage will be used.
        #[arg(short, long, value_hint = clap::ValueHint::DirPath)]
        location: String,
        /// Host and port to listen to
        #[arg(short, long, default_value = "localhost:7878", value_hint = clap::ValueHint::Hostname)]
        bind: String,
    },
    /// Use to view info about an HDT file
    View {
        #[clap(short, long, num_args = 1.., required = true)]
        /// Path to HDT files
        data: Vec<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    env_logger::Builder::new()
        .filter_level(cli.verbose.log_level_filter())
        .init();
    let mut stdout_writer = BufWriter::new(stdout());
    let result = run_command(&cli.command, &mut stdout_writer).await;
    let flush_result = flush_stdout_writer(&mut stdout_writer);
    match result {
        Ok(_) => {
            if let Err(error) = flush_result {
                error!("Error flushing standard output: {error:?}");
                std::process::exit(exitcode::UNAVAILABLE);
            }
            std::process::exit(exitcode::OK)
        }
        Err(e) => {
            error!("Error during execution: {e:?}");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    }
}

async fn run_command<W: Write>(
    command: &Commands,
    stdout_writer: &mut BufWriter<W>,
) -> anyhow::Result<()> {
    match command {
        Commands::Query {
            data,
            named_graph,
            sparql,
            entailment,
            debug_query_plan,
            output,
        } => {
            let named_graph_bindings = query::parse_named_graph_bindings(named_graph)?;
            query::do_query_with_dataset_with_options(
                data,
                &named_graph_bindings,
                sparql,
                *entailment,
                query::QueryExecutionOptions {
                    debug_query_plan: *debug_query_plan,
                },
                output,
                stdout_writer,
            )
            .await
        }
        Commands::Create {
            output_name,
            data,
            allow_merge_named_graphs,
            graph_iri,
        } => create::do_create_with_options(
            output_name,
            data,
            *allow_merge_named_graphs,
            graph_iri.as_deref(),
        )
        .map(|_| ()),
        Commands::View { data } => view::view_hdt(data, stdout_writer),
        #[cfg(feature = "server")]
        Commands::Serve { location, bind } => de::serve::serve(location.to_owned(), bind),
    }
}

fn flush_stdout_writer<W: Write>(writer: &mut W) -> std::io::Result<()> {
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::flush_stdout_writer;
    use std::io::{self, Error, ErrorKind, Write};

    #[derive(Debug)]
    struct FailingFlushWriter;

    impl Write for FailingFlushWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(Error::new(ErrorKind::BrokenPipe, "simulated broken pipe"))
        }
    }

    #[test]
    fn test_flush_stdout_writer_returns_error_on_broken_pipe() {
        let mut writer = FailingFlushWriter;
        let result = flush_stdout_writer(&mut writer);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::BrokenPipe);
    }
}
