// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use clap::{Parser, Subcommand};
use de::*;
use log::error;
use std::io::{stdout, BufWriter};

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
    },
    /// Query HDT and RDF files using SPARQL query format
    Query {
        #[clap(short, long, num_args = 1..)]
        /// local HDT and RDF files to be queried
        data: Vec<String>,
        #[clap(short, long, num_args = 1.., required = true)]
        /// Path to SPARQL query file. (should end in .rq)
        sparql: Vec<String>,
        /// Output to return the query results as using https://docs.rs/oxigraph/0.4.3/oxigraph/sparql/results/enum.QueryResultsFormat.html and https://crates.io/crates/oxrdfio
        #[clap(short, long, default_value_t, value_enum)]
        output: query::DeOutput,
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
    // Matching CLI input to commands
    let result = match &cli.command {
        Commands::Query {
            data,
            sparql,
            output,
        } => query::do_query(data, sparql, output, &mut stdout_writer).await,
        Commands::Create { output_name, data } => match create::do_create(output_name, data) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        },
        Commands::View { data } => match view::view_hdt(data, &mut stdout_writer) {
            Ok(v) => Ok(v),
            Err(e) => Err(e),
        },
    };

    match result {
        Ok(_) => std::process::exit(exitcode::OK),
        Err(e) => {
            error!("Error during execution: {e:?}");
            std::process::exit(exitcode::UNAVAILABLE);
        }
    }
}
