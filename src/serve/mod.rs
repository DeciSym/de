//! The SPARQL endpoint behind `de serve`.
//!
//! The request handling started as a copy of `oxigraph serve`
//! (oxigraph's `cli/src/main.rs`), adapted to a store of read-only HDT files.
//!
//! * `server` runs the HTTP server; `service` routes each request.
//! * `protocol` reads the request (URL, body, query parameters), and
//!   `negotiate` picks the result format from the Accept header.
//! * `stream` evaluates the query on a worker thread and streams the results.
//! * `hdt_dir` finds the HDT files in the data directory, and notices changes.
//! * `ui` is the query page; `error` builds the error responses.

mod error;
mod hdt_dir;
mod negotiate;
mod protocol;
mod server;
mod service;
mod stream;
mod ui;

pub use server::serve;
pub use service::handle_request;
