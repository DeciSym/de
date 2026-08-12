// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Model Context Protocol (MCP) server exposing `de`'s SPARQL query and RDF
//! upload capabilities to MCP clients over the Streamable HTTP transport.
//!
//! The service is rooted at a single data directory: `query_sparql` runs
//! against the RDF/HDT files found there (or an explicit subset of them) and
//! `upload_rdf` drops new Turtle files into its `uploads/` subdirectory, where
//! subsequent queries pick them up.
//!
//! Gated behind the `mcp` feature. Reachable from the CLI as
//! `de serve --location <dir> --mcp`, and usable directly as a library:
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use de::mcp::McpService;
//!
//! McpService::new("/srv/graphs".to_string())
//!     .serve("localhost:7878")
//!     .await
//! # }
//! ```

pub mod server;
pub mod tools;

pub use server::{MCP_ENDPOINT_PATH, McpServerInfo, McpService};
