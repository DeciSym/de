// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

//! Conformance cases and runtime contracts for a SPARQL endpoint.
//!
//! `de serve` and the servers built on `de` run the same cases, so a
//! downstream server can show that it behaves like `de` without running
//! `de`'s benchmarks. The cases are black-box: they build HTTP requests and
//! check the responses, and never see the server's types.
//!
//! ```ignore
//! use endpoint_kit::{InProcess, KitResponse, Suite};
//!
//! let report = Suite::new("my server, in process")
//!     .expect_failure("w3c/query_multiple_dataset", "FROM isn't overridden yet")
//!     .run(|fixtures, settings| {
//!         // Load every graph of `fixtures` under its IRI, then:
//!         let service = build_my_service(fixtures, settings)?;
//!         Ok(Box::new(InProcess::new(move |request| {
//!             // Convert the request to the server's types, call it, and wrap
//!             // the response's body as a reader.
//!             service.handle(request)
//!         })))
//!     })?;
//! report.write("target/endpoint-kit-report.txt".as_ref())?;
//! report.check()?;
//! ```
//!
//! The factory is called twice, with two fixture sets: small graphs for the
//! protocol cases, then larger ones for the contracts. Use a fresh server
//! each time. [`SpawnedServer`] and [`HttpTarget`] run the same cases against
//! a server binary over a socket.
//!
//! See `cases.rs` for where each case comes from, and `contracts.rs` for the
//! runtime properties checked.

mod alloc;
mod cases;
mod contracts;
mod fixtures;
mod http1;
mod report;
mod target;

pub use alloc::PeakAlloc;
pub use fixtures::{FixtureGraph, FixtureSet};
pub use report::{CaseResult, ExpectedFailure, Outcome, Report};
pub use target::{
    HttpTarget, InProcess, KitResponse, ServerProcess, SpawnedServer, Target, free_port,
};

use std::sync::Arc;
use std::time::Duration;

/// What the server under test offers beyond SPARQL queries.
// Independent yes-or-no capabilities, which a set of flags describes best.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug)]
pub struct Capabilities {
    /// With no dataset in the request, the default graph is the union of
    /// every graph in the store. True for `de serve`.
    pub union_default_graph: bool,
    /// SPARQL Update on `/update`. Without it, `/update` must answer 501.
    pub update: bool,
    /// The Graph Store Protocol on `/store`. Without it, `/store` must answer 501.
    pub graph_store: bool,
    /// CORS: requests from web pages on other origins are allowed. Only
    /// checked when set, since it's off by default in `de serve`.
    pub cors: bool,
}

impl Default for Capabilities {
    /// A read-only server whose default graph is the union of its graphs,
    /// which is what `de serve` is.
    fn default() -> Self {
        Self {
            union_default_graph: true,
            update: false,
            graph_store: false,
            cors: false,
        }
    }
}

/// How the factory should configure the server, where it can.
#[derive(Clone, Debug)]
pub struct ServiceSettings {
    /// The longest a query may run. A server without a timeout ignores this,
    /// and `contract/timeout_stops_evaluation` fails.
    pub query_timeout: Duration,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(1),
        }
    }
}

/// A configured run of the cases.
pub struct Suite {
    label: String,
    capabilities: Capabilities,
    settings: ServiceSettings,
    allocator: Option<&'static PeakAlloc>,
    expected_failures: Vec<ExpectedFailure>,
}

impl Suite {
    /// A suite whose report is labelled `label`, such as "de serve, in process".
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            capabilities: Capabilities::default(),
            settings: ServiceSettings::default(),
            allocator: None,
            expected_failures: Vec::new(),
        }
    }

    #[must_use]
    pub fn capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    #[must_use]
    pub fn settings(mut self, settings: ServiceSettings) -> Self {
        self.settings = settings;
        self
    }

    /// The process's global allocator, which lets the memory contract run.
    #[must_use]
    pub fn allocator(mut self, allocator: &'static PeakAlloc) -> Self {
        self.allocator = Some(allocator);
        self
    }

    /// Records a case known to fail, and why. See [`ExpectedFailure`].
    #[must_use]
    pub fn expect_failure(mut self, id: &'static str, reason: &'static str) -> Self {
        self.expected_failures.push(ExpectedFailure { id, reason });
        self
    }

    /// Runs every case against servers the factory builds over the kit's
    /// fixtures, and reports the outcomes. Check them with [`Report::check`].
    pub fn run<F>(&self, mut factory: F) -> anyhow::Result<Report>
    where
        F: FnMut(&FixtureSet, &ServiceSettings) -> anyhow::Result<Box<dyn Target>>,
    {
        let dir = tempfile::Builder::new().prefix("endpoint-kit-").tempdir()?;
        let mut results = Vec::new();
        for (fixtures, cases) in [
            (
                fixtures::protocol(&dir.path().join("protocol"))?,
                cases::protocol_cases(),
            ),
            (
                fixtures::contracts(&dir.path().join("contracts"))?,
                contracts::contract_cases(),
            ),
        ] {
            let target: Arc<dyn Target> = Arc::from(factory(&fixtures, &self.settings)?);
            let _stop = StopOnDrop(Arc::clone(&target));
            let ctx = cases::Ctx {
                target: &target,
                settings: &self.settings,
                allocator: self.allocator,
            };
            for case in cases {
                let outcome = match (case.skip_unless)(&self.capabilities) {
                    Some(reason) => Outcome::Skip(reason.to_string()),
                    None => (case.run)(&ctx),
                };
                results.push(CaseResult {
                    id: case.id,
                    description: case.description,
                    outcome,
                });
            }
        }
        Ok(Report {
            label: self.label.clone(),
            results,
            expected_failures: self.expected_failures.clone(),
        })
    }
}

/// Stops a target when the cases that use it are done, panicking or not.
struct StopOnDrop(Arc<dyn Target>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop();
    }
}
