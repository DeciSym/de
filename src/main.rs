// This code handles the CLI

use clap::{Parser, Subcommand};
use de::*;
use log::error;
use std::sync::Arc;
// Building clap structs using derive
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
    /// Check that required CLI dependencies are installed on the system
    Check {},
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    env_logger::Builder::new()
        .filter_level(cli.verbose.log_level_filter())
        .init();

    let r2h = Arc::new(rdf2hdt::Rdf2HdtImpl());

    // Matching CLI input to commands
    let result = match &cli.command {
        Commands::Query {
            data,
            sparql,
            output,
        } => query::do_query(data, sparql, r2h, output).await,
        Commands::Create { output_name, data } => {
            create::do_create(output_name, data.as_ref(), r2h)
        }
        Commands::View { data } => match view::view_hdt(data) {
            Ok(v) => Ok(v),
            Err(e) => Err(e),
        },
        Commands::Check {} => check::do_check(None),
    };

    match result {
        Ok(_) => std::process::exit(exitcode::OK),
        Err(e) => {
            error!("Error during execution: {:?}", e);
            std::process::exit(exitcode::UNAVAILABLE);
        }
    }
}
