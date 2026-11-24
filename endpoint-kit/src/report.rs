// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Case outcomes, the report file, and the check against expected failures.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;

/// What happened when a case ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Passed, with what was measured when there's something worth showing
    /// (a contract's margin), or an empty string.
    Pass(String),
    Fail(String),
    /// The case couldn't say either way: it doesn't apply to this server, or
    /// what it needs (a counting allocator, the server's CPU time) isn't there.
    Skip(String),
}

/// A case known to fail on the server under test, and why.
///
/// The check is strict both ways: a failure that isn't listed fails the run,
/// and so does a listed case that passes, so that the fix that makes it pass
/// also removes it from the list.
#[derive(Clone, Debug)]
pub struct ExpectedFailure {
    pub id: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Debug)]
pub struct CaseResult {
    pub id: &'static str,
    pub description: &'static str,
    pub outcome: Outcome,
}

/// The outcome of every case in a run.
#[derive(Clone, Debug)]
pub struct Report {
    pub label: String,
    pub results: Vec<CaseResult>,
    pub expected_failures: Vec<ExpectedFailure>,
}

impl Report {
    fn expected(&self, id: &str) -> Option<&ExpectedFailure> {
        self.expected_failures.iter().find(|e| e.id == id)
    }

    /// The report as text: a summary, then one line per case.
    #[must_use]
    pub fn render(&self) -> String {
        let count =
            |pred: fn(&Outcome) -> bool| self.results.iter().filter(|r| pred(&r.outcome)).count();
        let pass = count(|o| matches!(o, Outcome::Pass(_)));
        let fail = count(|o| matches!(o, Outcome::Fail(_)));
        let skip = count(|o| matches!(o, Outcome::Skip(_)));
        let mut out = String::new();
        let _ = writeln!(out, "endpoint-kit report: {}", self.label);
        let _ = writeln!(
            out,
            "{} cases: {pass} pass, {fail} fail ({} expected), {skip} skipped",
            self.results.len(),
            self.results
                .iter()
                .filter(|r| matches!(r.outcome, Outcome::Fail(_)) && self.expected(r.id).is_some())
                .count(),
        );
        let _ = writeln!(out);
        for result in &self.results {
            let (status, detail) = match (&result.outcome, self.expected(result.id)) {
                (Outcome::Pass(note), None) => ("PASS", note.clone()),
                (Outcome::Pass(_), Some(_)) => {
                    ("PASS*", "listed as an expected failure".to_string())
                }
                (Outcome::Fail(why), Some(e)) => {
                    ("XFAIL", format!("{why} [expected: {}]", e.reason))
                }
                (Outcome::Fail(why), None) => ("FAIL", why.clone()),
                (Outcome::Skip(why), _) => ("SKIP", why.clone()),
            };
            let _ = writeln!(out, "{status:<6} {}  {}", result.id, result.description);
            if !detail.is_empty() {
                let _ = writeln!(out, "       {detail}");
            }
        }
        out
    }

    /// Writes [`render`](Self::render) to `path`.
    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, self.render())?;
        Ok(())
    }

    /// Fails unless every failure is an expected one and every expected
    /// failure still fails.
    pub fn check(&self) -> anyhow::Result<()> {
        let ids: BTreeSet<&str> = self.results.iter().map(|r| r.id).collect();
        let mut problems = Vec::new();
        for expected in &self.expected_failures {
            if !ids.contains(expected.id) {
                problems.push(format!(
                    "{}: listed as an expected failure, but no case has this id",
                    expected.id
                ));
            }
        }
        for result in &self.results {
            match (&result.outcome, self.expected(result.id)) {
                (Outcome::Fail(why), None) => problems.push(format!("{}: {why}", result.id)),
                (Outcome::Pass(_), Some(_)) => problems.push(format!(
                    "{}: passes now; remove it from the expected failures",
                    result.id
                )),
                _ => {}
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "endpoint-kit ({}): {} problem(s):\n  {}",
                self.label,
                problems.len(),
                problems.join("\n  ")
            ))
        }
    }
}
