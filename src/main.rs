// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use clap::{Parser, Subcommand};
use de::{create, query, view};
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
        /// Output to return the query results as using <https://docs.rs/oxigraph/0.4.3/oxigraph/sparql/results/enum.QueryResultsFormat.html> and <https://crates.io/crates/oxrdfio>
        #[clap(short, long, default_value_t, value_enum)]
        output: query::DeOutput,
    },
    /// Serve the HDT files in a directory as a read-only SPARQL endpoint
    ///
    /// Queries go to /query, by the SPARQL 1.1 Protocol (GET, or POST as a form or as a
    /// query), and / is a query page. HDT files added to the directory, replaced or removed
    /// are picked up between queries. /update and /store answer 501 Not Implemented, since
    /// HDT files are read-only.
    #[cfg(feature = "server")]
    Serve {
        /// Directory holding the .hdt files to serve
        #[arg(short, long, value_hint = clap::ValueHint::DirPath)]
        location: String,
        /// Host and port to listen to
        #[arg(short, long, default_value = "localhost:7878", value_hint = clap::ValueHint::Hostname)]
        bind: String,
        /// Allow requests from web pages on any origin (CORS)
        #[arg(long)]
        cors: bool,
        /// Cancel a query that runs longer than this many seconds, such as 30
        /// or 0.5. One cancelled before its first results gets a 503, and one
        /// cancelled later has its response cut off. Default: no limit.
        #[arg(long, value_name = "SECONDS", value_parser = parse_seconds)]
        timeout_s: Option<std::time::Duration>,
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
        Ok(()) => {
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
            Some(output_name),
            data,
            *allow_merge_named_graphs,
            graph_iri.as_deref(),
            &[],
            None,
            None,
        )
        .await
        .map(|_| ()),
        Commands::View { data } => view::view_hdt(data, stdout_writer),
        #[cfg(feature = "server")]
        Commands::Serve {
            location,
            bind,
            cors,
            timeout_s,
        } => {
            let mut options = de::serve::ServiceOptions::default();
            options.cors = *cors;
            options.query_timeout = *timeout_s;
            serve_until_stopped(location.to_owned(), bind.to_owned(), options).await
        }
    }
}

/// A number of seconds over 0, such as `30` or `0.5`.
#[cfg(feature = "server")]
fn parse_seconds(value: &str) -> Result<std::time::Duration, String> {
    let seconds: f64 = value.parse().map_err(|e| format!("{e}"))?;
    if seconds <= 0.0 {
        return Err("must be over 0".into());
    }
    std::time::Duration::try_from_secs_f64(seconds).map_err(|e| e.to_string())
}

/// Runs the server until it fails, or until SIGINT or SIGTERM ends the
/// process with exit status 0.
///
/// The server has no shutdown of its own, so the signal handler is what makes
/// the exit clean rather than a kill: systemd's stop then reports success,
/// and tools that record a process until it exits, such as heaptrack in
/// `scripts/bench-memory.py`, get a complete recording.
#[cfg(feature = "server")]
async fn serve_until_stopped(
    location: String,
    bind: String,
    options: de::serve::ServiceOptions,
) -> anyhow::Result<()> {
    let server = tokio::task::spawn_blocking(move || de::serve::serve(&location, &bind, options));
    tokio::select! {
        result = server => result?,
        () = shutdown_signal() => Ok(()),
    }
}

#[cfg(feature = "server")]
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
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
