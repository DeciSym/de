// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Runtime contracts: properties of how the server runs a query, rather than
//! of the answer.
//!
//! They are quick (seconds) and use wide margins, so that they hold on any
//! machine and in debug builds: they catch a change of kind, such as results
//! held in memory instead of streamed, joins run in the written order, or a
//! query that keeps a core busy after nobody is waiting for it. Finer
//! regressions are the benchmarks' job.
//!
//! The CPU-time contracts read `/proc/<pid>/stat`, so they need Linux and a
//! server whose process the target names ([`ServerProcess`]). They assume
//! Linux's fixed 100 ticks per second in that file.

use crate::Capabilities;
use crate::cases::Case;
use crate::cases::{
    Answer, Ctx, RESULTS_JSON, RESULTS_TSV, get, outcome, query_uri, send, solutions,
};
use crate::fixtures::{
    JOIN_CHAINS, JOIN_P, JOIN_Q, JOIN_R, LABEL, LARGE_TRIPLES, SLOW_TRIPLES, WEIGHT,
};
use crate::report::Outcome;
use crate::target::{ServerProcess, Target};
use http::header::ACCEPT;
use std::io::Read;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// How far a measured CPU rate may stay above the rate before the request,
/// in cores, for the server to count as idle.
const IDLE_MARGIN: f64 = 0.3;
/// How far it must rise while the slow query runs for the contract to be
/// able to tell running from stopped.
const BUSY_MARGIN: f64 = 0.5;
/// How long a stopped query gets to release its core.
const GRACE: Duration = Duration::from_secs(1);

fn always(_: &Capabilities) -> Option<&'static str> {
    None
}

pub(crate) fn contract_cases() -> Vec<Case> {
    vec![
        Case {
            id: "contract/large_result_complete",
            description: "50,000 rows arrive, all of them",
            skip_unless: always,
            run: |ctx| outcome(large_result_complete(ctx)),
        },
        Case {
            id: "contract/stream_bounded_memory",
            description: "streaming 10x more rows doesn't raise peak heap by more than 4 MiB",
            skip_unless: always,
            run: stream_bounded_memory,
        },
        Case {
            id: "contract/join_order",
            description: "a join written in a bad order still runs in a good one (10 s budget)",
            skip_unless: always,
            run: join_order,
        },
        Case {
            id: "contract/timeout_stops_evaluation",
            description: "a query past the server's timeout ends, and frees its core",
            skip_unless: always,
            run: timeout_stops_evaluation,
        },
        Case {
            id: "contract/disconnect_stops_evaluation",
            description: "a query whose client went away frees its core",
            skip_unless: always,
            run: disconnect_stops_evaluation,
        },
    ]
}

/// `SELECT ?item ?label … LIMIT limit` over the large graph.
fn labels_query(limit: usize) -> String {
    format!("SELECT ?item ?label WHERE {{ ?item <{LABEL}> ?label }} LIMIT {limit}")
}

/// A COUNT over pairs of the slow graph's values: one solution, after
/// `SLOW_TRIPLES`² comparisons, and memory that doesn't grow with them.
pub(crate) fn slow_query() -> String {
    format!(
        "SELECT (COUNT(*) AS ?n) WHERE {{ ?a <{WEIGHT}> ?x . ?b <{WEIGHT}> ?y . FILTER(?x != ?y) }}"
    )
}

/// Sends a TSV query and counts the rows as they stream in, keeping none of them.
// Counting newlines byte by byte is fast enough here, and saves a dependency.
#[allow(clippy::naive_bytecount)]
fn count_tsv_rows(ctx: &Ctx<'_>, query: &str) -> Result<usize, String> {
    let mut response = ctx
        .target
        .send(get(
            &query_uri(query, &[]),
            &[(ACCEPT.as_str(), RESULTS_TSV)],
        ))
        .map_err(|e| format!("{e:#}"))?;
    if !response.status.is_success() {
        return Err(format!("the server answered {}", response.status));
    }
    let mut buffer = vec![0; 64 * 1024];
    let mut lines = 0;
    loop {
        match response.body.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => lines += buffer[..n].iter().filter(|b| **b == b'\n').count(),
            Err(e) => return Err(format!("the body was cut off after {lines} lines: {e}")),
        }
    }
    // The first line names the variables.
    Ok(lines.saturating_sub(1))
}

fn large_result_complete(ctx: &Ctx<'_>) -> Result<(), String> {
    let rows = count_tsv_rows(ctx, &labels_query(LARGE_TRIPLES))?;
    if rows == LARGE_TRIPLES {
        Ok(())
    } else {
        Err(format!("got {rows} rows, expected {LARGE_TRIPLES}"))
    }
}

fn stream_bounded_memory(ctx: &Ctx<'_>) -> Outcome {
    const SMALL: usize = 5_000;
    const ALLOWANCE: usize = 4 * 1024 * 1024;
    if ctx.target.server_process() != ServerProcess::InProcess {
        return Outcome::Skip("needs a server in this process, to count its allocations".into());
    }
    let Some(allocator) = ctx.allocator else {
        return Outcome::Skip(
            "no counting allocator: install endpoint_kit::PeakAlloc as the global allocator".into(),
        );
    };
    let measure = |limit: usize| -> Result<usize, String> {
        allocator.reset_peak();
        let before = allocator.current();
        let rows = count_tsv_rows(ctx, &labels_query(limit))?;
        if rows != limit {
            return Err(format!("got {rows} rows, expected {limit}"));
        }
        Ok(allocator.peak().saturating_sub(before))
    };
    // The first query pays for lazily built state: thread pools, caches.
    let measured = measure(SMALL).and_then(|_| Ok((measure(SMALL)?, measure(LARGE_TRIPLES)?)));
    match measured {
        Err(why) => Outcome::Fail(why),
        Ok((small, large)) if large > small + ALLOWANCE => Outcome::Fail(format!(
            "peak heap while answering {LARGE_TRIPLES} rows was {}, {} more than for {SMALL} rows: \
             the server holds results instead of streaming them",
            mib(large),
            mib(large - small)
        )),
        Ok((small, large)) => Outcome::Pass(format!(
            "peak heap {} for {SMALL} rows, {} for {LARGE_TRIPLES} (allowance: +{})",
            mib(small),
            mib(large),
            mib(ALLOWANCE)
        )),
    }
}

#[allow(clippy::cast_precision_loss)]
fn mib(bytes: usize) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Runs `f` on a thread of its own and waits up to `deadline` for it.
/// `None` means it was still running; it is left to finish on its own.
fn with_deadline<T: Send + 'static>(
    deadline: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> Option<(T, Duration)> {
    let (tx, rx) = mpsc::channel();
    let started = Instant::now();
    std::thread::spawn(move || {
        let value = f();
        let _ = tx.send((value, started.elapsed()));
    });
    rx.recv_timeout(deadline).ok()
}

/// Sends a query and reads the whole answer, on the calling thread.
fn fetch(target: &Arc<dyn Target>, query: &str) -> Result<Answer, String> {
    let ctx = Ctx {
        target,
        settings: &crate::ServiceSettings::default(),
        allocator: None,
    };
    send(
        &ctx,
        get(&query_uri(query, &[]), &[(ACCEPT.as_str(), RESULTS_JSON)]),
    )
}

fn join_order(ctx: &Ctx<'_>) -> Outcome {
    // In a good order the join answers in well under a second, even in a
    // debug build; in the written order it takes minutes.
    const BUDGET: Duration = Duration::from_secs(10);
    let query = format!(
        "SELECT (COUNT(*) AS ?n) WHERE {{ ?a <{JOIN_P}> ?x . ?b <{JOIN_R}> ?y . ?a <{JOIN_Q}> ?b }}"
    );
    // Untimed: the first query over a graph can pay for opening it, such as
    // building an index, which isn't what this contract is about.
    if let Err(why) = fetch(ctx.target, &format!("ASK {{ ?a <{JOIN_Q}> ?b }}")) {
        return Outcome::Fail(why);
    }
    let target = Arc::clone(ctx.target);
    let Some((answer, elapsed)) = with_deadline(BUDGET, move || fetch(&target, &query)) else {
        return Outcome::Fail(format!(
            "no answer within {BUDGET:?}: in a good order this join takes milliseconds, so the \
             evaluator likely joined the first two patterns, which share no variable, first"
        ));
    };
    let result = answer.and_then(|answer| {
        answer.expect_success()?;
        let (_, rows) = solutions(&answer)?;
        let expected = vec![vec![format!(
            "\"{JOIN_CHAINS}\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        )]];
        if rows == expected {
            Ok(())
        } else {
            Err(format!("counted {rows:?}, expected {expected:?}"))
        }
    });
    match result {
        Ok(()) => Outcome::Pass(format!("answered in {elapsed:.1?} (budget: {BUDGET:?})")),
        Err(why) => Outcome::Fail(format!("{why} (after {elapsed:?})")),
    }
}

/// CPU time used by the process so far, in seconds.
#[allow(clippy::cast_precision_loss)]
fn cpu_seconds(process: ServerProcess) -> Option<f64> {
    let path = match process {
        ServerProcess::InProcess => "/proc/self/stat".to_string(),
        ServerProcess::Pid(pid) => format!("/proc/{pid}/stat"),
        ServerProcess::Unknown => return None,
    };
    let stat = std::fs::read_to_string(path).ok()?;
    // The command name, field 2, can hold spaces; it ends at the last ')'.
    // After it come the state (field 3), …, utime (14) and stime (15).
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / 100.0)
}

/// The process's CPU use over `window`, in cores.
fn cpu_rate(process: ServerProcess, window: Duration) -> Option<f64> {
    let before = cpu_seconds(process)?;
    let started = Instant::now();
    std::thread::sleep(window);
    Some((cpu_seconds(process)? - before) / started.elapsed().as_secs_f64())
}

fn timeout_stops_evaluation(ctx: &Ctx<'_>) -> Outcome {
    let process = ctx.target.server_process();
    let Some(baseline) = cpu_rate(process, Duration::from_millis(300)) else {
        return Outcome::Skip(
            "can't read the server's CPU time (needs Linux and a known process)".into(),
        );
    };
    let timeout = ctx.settings.query_timeout;
    let deadline = timeout + Duration::from_secs(3);
    let target = Arc::clone(ctx.target);
    let query = slow_query();
    let Some((answer, elapsed)) = with_deadline(deadline, move || fetch(&target, &query)) else {
        return Outcome::Fail(format!(
            "the response hadn't ended {deadline:?} after the request: the {timeout:?} query \
             timeout didn't stop a query over {SLOW_TRIPLES}² pairs"
        ));
    };
    if let Ok(answer) = &answer
        && answer.status.is_success()
        && answer.body.is_ok()
    {
        return if elapsed < timeout {
            Outcome::Skip(format!(
                "the slow query finished in {elapsed:?}, inside the {timeout:?} timeout, so this \
                 machine is too fast for it to test anything"
            ))
        } else {
            Outcome::Fail(format!(
                "answered in full after {elapsed:?}, past the {timeout:?} timeout"
            ))
        };
    }
    idle_after(process, baseline, "the timeout")
}

fn disconnect_stops_evaluation(ctx: &Ctx<'_>) -> Outcome {
    let process = ctx.target.server_process();
    if process != ServerProcess::InProcess {
        // Over a socket, noticing that the client left depends on the HTTP
        // stack; in process, the server sees its response body dropped.
        return Outcome::Skip("needs a server in this process".into());
    }
    let Some(baseline) = cpu_rate(process, Duration::from_millis(300)) else {
        return Outcome::Skip("can't read this process's CPU time (needs Linux)".into());
    };
    let response = match ctx.target.send(get(
        &query_uri(&slow_query(), &[]),
        &[(ACCEPT.as_str(), RESULTS_JSON)],
    )) {
        Ok(response) => response,
        Err(e) => return Outcome::Fail(format!("{e:#}")),
    };
    let busy = cpu_rate(process, Duration::from_millis(300)).unwrap_or(0.0);
    if busy < baseline + BUSY_MARGIN {
        return Outcome::Skip(format!(
            "the slow query used {busy:.2} cores against {baseline:.2} before it, too little to \
             tell running from stopped"
        ));
    }
    drop(response);
    idle_after(process, baseline, "the client went away")
}

fn idle_after(process: ServerProcess, baseline: f64, event: &str) -> Outcome {
    std::thread::sleep(GRACE);
    match cpu_rate(process, Duration::from_millis(500)) {
        None => Outcome::Skip("can't read the server's CPU time".into()),
        Some(rate) if rate > baseline + IDLE_MARGIN => Outcome::Fail(format!(
            "{GRACE:?} after {event}, the server was using {rate:.2} cores against {baseline:.2} \
             before the request: the query is still running"
        )),
        Some(rate) => Outcome::Pass(format!(
            "{rate:.2} cores {GRACE:?} after {event}, against {baseline:.2} before the request"
        )),
    }
}
