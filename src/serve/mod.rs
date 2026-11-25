//! The SPARQL endpoint behind `de serve`, for other servers to reuse.
//!
//! A [`SparqlService`] answers requests from a [`DatasetSource`], such as an
//! [`HdtDirectory`], with [`Routes`] of its own tried first, such as the
//! [`Yasgui`] query page. It takes and returns its own [`RequestBody`] and
//! [`ResponseBody`], so it runs on any HTTP server. `de serve` runs it on
//! axum and hyper with [`axum::start`]; [`axum::router`] mounts it in an axum
//! app of a server's own, and [`axum::spawn`] runs it on a server of its own.
//!
//! ```no_run
//! use de::serve::{HdtDirectory, ServerConfig, ServiceOptions, SparqlService, Yasgui};
//!
//! let mut options = ServiceOptions::default();
//! options.cors = true;
//! let service = SparqlService::new(HdtDirectory::new("data")?)
//!     .with_options(options)
//!     .with_routes(Yasgui);
//! let server = de::serve::axum::spawn(service, "localhost:7878", &ServerConfig::default())?;
//! // Serves until shut down, or dropped.
//! server.shutdown();
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! The request handling started as a copy of `oxigraph serve`
//! (oxigraph's `cli/src/main.rs`), adapted to a store of read-only HDT files.
//!
//! * `service` routes each request, and adds CORS headers.
//! * `protocol` reads the request (URL, body, query parameters), and
//!   `negotiate` picks the result format from the Accept header.
//! * `stream` evaluates the query on a worker thread and streams the results,
//!   cancelling the query past its timeout or once its response is dropped.
//! * `dataset` is what a data source implements; `hdt_dir` is de's.
//! * `axum` runs a service on axum and hyper, with `server`'s settings and
//!   `de serve`'s service; `ui` is the query page.
//! * `body` and `error` are the request and response types.

pub mod axum;
mod body;
mod dataset;
mod error;
mod hdt_dir;
mod negotiate;
mod protocol;
mod server;
mod service;
mod stream;
mod ui;

pub use body::{RequestBody, ResponseBody};
pub use dataset::{DatasetSelection, DatasetSource, EvalOptions, ServeDataset};
pub use error::HttpError;
pub use hdt_dir::HdtDirectory;
pub use server::ServerConfig;
pub use service::{Routes, ServiceOptions, SparqlService};
pub use ui::Yasgui;
