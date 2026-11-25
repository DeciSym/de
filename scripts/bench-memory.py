#!/usr/bin/env python3
"""Measure the heap memory `de` uses on the benchmark fixtures, and compare two runs.

The Benchmarks workflow (.github/workflows/bench.yml) runs this on a change and on
its base commit, one after the other on one runner, compares the two, and keeps
the change's heaptrack recordings as an artifact:

    scripts/bench-memory.py run BENCH --de PATH [--root DIR] --out DIR
        Run each workload of BENCH (trainmarks, watdiv or sparql_http) once
        under heaptrack, in a `de` process of its own, on the fixtures that the
        criterion benchmark of the same name reads from --root, a checkout of
        de. Writes each workload's recording to --out, and --out/memory.json
        with what heaptrack_print reports for it: peak heap, calls to
        allocation functions, temporary allocations, memory leaked and peak RSS.

        sparql_http measures `de serve` (built with the server feature) on the
        BSBM fixtures of benches/sparql_http.rs: each workload starts a server
        under heaptrack, waits for it to report ready on NOTIFY_SOCKET, sends
        its requests over HTTP and stops it with SIGTERM.

    scripts/bench-memory.py compare BASELINE CURRENT [--threshold PCT]
                                    [--fail-above PCT] [--base-commit SHA]
        Print a Markdown table comparing two runs' memory.json (a --out
        directory, or the file), and append it to the job summary when run in
        GitHub Actions. Exits 1 when a workload's peak heap grew by more than
        --fail-above percent, or when a sparql_http run fails one of its own
        checks, which need no baseline: the server's peak heap must not grow
        with the number of queries it has answered, nor with the size of a
        result it streams.

A local A/B looks like:

    scripts/bench-memory.py run trainmarks --de ../main/target/release/de --out /tmp/mem-main
    scripts/bench-memory.py run trainmarks --de target/release/de --out /tmp/mem-mine
    scripts/bench-memory.py compare /tmp/mem-main /tmp/mem-mine

Open a recording with `heaptrack --analyze FILE` (heaptrack_gui) to see where
the memory goes, or `heaptrack_print -f FILE`; `heaptrack_print -f NEW -d OLD`
shows what changed between two of them.

Each workload gets its own process because peak heap is a high-water mark for
the whole process, which criterion's repeated in-process iterations would blur.
Two runs on one machine agree to within 0.1% on allocations and 2% on peak
heap, far closer than timings. Allocation counts are the same on any machine,
but peak heap isn't: a GitHub runner measured up to 11% less than a developer
machine. So compare peak heap only between runs on one machine, as CI does,
with the same RAYON_NUM_THREADS: the size of rayon's thread pool changes how
much memory is live at once.
"""

import argparse
import json
import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

RESULTS_FILE = "memory.json"
# Smaller changes than these are never marked, whatever the percentage: a
# WatDiv query peaks at about 0.5 MB, where 5% is a 25 KB buffer.
MIN_HEAP_CHANGE = 256_000
MIN_ALLOCATION_CHANGE = 1_000
SETTINGS_PREFIX = "DE_"
SETTINGS_VARS = ("RAYON_NUM_THREADS",)

# sparql_http: how long a server may take to report ready, and its checks.
SERVER_START_SECONDS = 120
# Answering the explore mix five times may peak this much higher than
# answering it once, in percent, before it counts as memory kept per query.
GROWTH_LIMIT = 10
# Streaming every triple of the dataset may peak this much above a server
# that answered one ASK. At 1,000 products the dataset is ~372K triples, so
# a server that held the results instead of streaming them would be far over.
STREAM_ALLOWANCE = 32_000_000
JSON_RESULTS = "application/sparql-results+json"

# What heaptrack_print's summary says, and the key each line is stored under.
SUMMARY_LINES = {
    "calls to allocation functions": "allocations",
    "temporary memory allocations": "temporary",
    "peak heap memory consumption": "peak_heap",
    "peak RSS (including heaptrack overhead)": "peak_rss",
    "total memory leaked": "leaked",
}
SIZE_UNITS = {"B": 1, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}

# The trainmarks queries the criterion benchmark runs, with the output format
# each needs (benches/trainmarks.rs). q5 is a CONSTRUCT, so it's Turtle.
TRAINMARKS_QUERIES = [
    ("q1_count", "csv"),
    ("q2_customer_orders", "csv"),
    ("q3_join_3_entities", "csv"),
    ("q4_optional_aggregation", "csv"),
    ("q5_construct", "turtle"),
]
# The twenty WatDiv templates, in family order (benches/watdiv.rs).
WATDIV_TEMPLATES = [
    "C1", "C2", "C3", "F1", "F2", "F3", "F4", "F5", "L1", "L2",
    "L3", "L4", "L5", "S1", "S2", "S3", "S4", "S5", "S6", "S7",
]


def parse_size(text):
    """Bytes from heaptrack's sizes, such as 610.74K or 1.20G."""
    match = re.fullmatch(r"([0-9.]+)([BKMGT]?)", text.strip())
    if not match:
        raise ValueError(f"unexpected size {text!r}")
    return float(match.group(1)) * SIZE_UNITS[match.group(2) or "B"]


def fmt_bytes(n):
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if n >= scale:
            return f"{n / scale:.4g} {unit}"
    return f"{n:.0f} B"


def fmt_count(n):
    for unit, scale in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if n >= scale:
            return f"{n / scale:.4g}{unit}"
    return f"{n:.0f}"


def heaptrack_summary(recording):
    """The summary heaptrack_print gives for one recording."""
    output = subprocess.run(
        ["heaptrack_print", "-f", str(recording), "--print-peaks", "0", "--print-allocators", "0",
         "--print-temporary", "0", "--print-leaks", "0"],
        capture_output=True, text=True, check=True,
    ).stdout
    summary = {}
    for line in output.splitlines():
        label, sep, value = line.partition(": ")
        key = SUMMARY_LINES.get(label.strip())
        if not sep or not key:
            continue
        value = value.split(" (")[0]
        summary[key] = int(value) if key in ("allocations", "temporary") else parse_size(value)
    missing = set(SUMMARY_LINES.values()) - summary.keys()
    if missing:
        raise RuntimeError(f"heaptrack_print didn't report {', '.join(sorted(missing))} for {recording}")
    return summary


def run_de(de, args, check=True):
    """Run de untraced, for warming the HDT index cache."""
    subprocess.run([de, *args], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=check)


def measure(de, args, out, name):
    """Run de once under heaptrack and return what heaptrack_print reports."""
    for old in out.glob(f"{name}.*"):
        old.unlink()
    started = time.monotonic()
    # --record-only: don't open heaptrack_gui afterwards, which heaptrack does
    # whenever a display is available.
    done = subprocess.run(["heaptrack", "--record-only", "-o", str(out / name), de, *args],
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    seconds = time.monotonic() - started
    recordings = sorted(p for p in out.glob(f"{name}.*") if p.suffix in (".gz", ".zst"))
    if done.returncode != 0 or not recordings:
        sys.exit(f"error: `de {' '.join(args)}` failed under heaptrack (exit {done.returncode}):\n{done.stderr[-2000:]}")
    summary = heaptrack_summary(recordings[0])
    summary["seconds"] = round(seconds, 2)
    summary["recording"] = recordings[0].name
    print(f"{name}: peak heap {fmt_bytes(summary['peak_heap'])}, {fmt_count(summary['allocations'])} allocations, "
          f"{seconds:.1f} s under heaptrack", flush=True)
    return summary


def trainmarks_workloads(root, tmp):
    """(name, de arguments) for each trainmarks workload, and the untraced warm-up."""
    scale = os.environ.get("DE_BENCH_SCALE", "large")
    data = root / "benches" / "trainmarks" / "data"
    queries = root / "benches" / "trainmarks" / "queries"
    nt, ttl = data / f"{scale}.nt", data / f"{scale}.ttl"
    for path in (nt, ttl, queries):
        if not path.exists():
            sys.exit(f"error: {path} is missing; run `python3 scripts/gen-trainmarks-data.py {scale}`")
    hdt = tmp / "trainmarks.hdt"
    workloads = [
        ("create_hdt_from_nt", ["create", "-o", str(hdt), "-d", str(nt)]),
        ("create_hdt_from_ttl", ["create", "-o", str(tmp / "trainmarks-ttl.hdt"), "-d", str(ttl)]),
    ]
    # The queries read the HDT built from the N-Triples, with its index cache
    # already written, as the criterion benchmark does (warm_hdt).
    warm = ["query", "-d", str(hdt), "-s", str(queries / "q1_count.rq")]
    workloads += [(name, ["query", "-d", str(hdt), "-s", str(queries / f"{name}.rq"), "-o", fmt])
                  for name, fmt in TRAINMARKS_QUERIES]
    workloads.append(("q1_count_from_ttl", ["query", "-d", str(ttl), "-s", str(queries / "q1_count.rq")]))
    return workloads, (2, warm)


def split_instances(text):
    """A WatDiv template file's query instances, which blank lines separate."""
    return [block.strip() + "\n" for block in re.split(r"\n\s*\n", text) if block.strip()]


def watdiv_workloads(root, tmp):
    scale = os.environ.get("DE_WATDIV_SCALE", "10")
    fixtures = root / "benches" / "watdiv" / scale
    dataset, queries = fixtures / "dataset.hdt", fixtures / "queries"
    if not dataset.exists() or not queries.is_dir():
        sys.exit(f"error: {fixtures} has no dataset.hdt or queries/; run `scripts/fetch-watdiv-data.sh {scale}`")
    workloads = []
    for template in WATDIV_TEMPLATES:
        files = []
        for i, instance in enumerate(split_instances((queries / f"{template}.txt").read_text())):
            path = tmp / f"{template}-{i}.rq"
            path.write_text(instance)
            files.append(str(path))
        # One process answers all five instances of a template, as one
        # iteration of the criterion benchmark does.
        workloads.append((template, ["query", "-d", str(dataset), "-s", *files]))
    warm = ["query", "-d", str(dataset), "-s", str(tmp / "S1-0.rq")]
    return workloads, (0, warm)


BENCHES = {"trainmarks": trainmarks_workloads, "watdiv": watdiv_workloads}
SERVER_BENCHES = ("sparql_http",)


def bsbm_fixtures(root):
    """The BSBM dataset and query instances that benches/sparql_http.rs reads."""
    count = os.environ.get("DE_BSBM_PRODUCT_COUNT", "1000")
    fixtures = root / "benches" / "bsbm" / count
    dataset, manifest = fixtures / "dataset.nt", fixtures / "queries" / "manifest.tsv"
    if not dataset.exists() or not manifest.exists():
        sys.exit(f"error: {fixtures} has no dataset.nt or queries/manifest.tsv; "
                 f"run `python3 scripts/gen-bsbm-data.py {count}`")
    queries = []
    for line in manifest.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            name, file, form = line.split("\t")
            accept = "text/turtle" if form == "graph" else JSON_RESULTS
            queries.append((name, (fixtures / "queries" / file).read_text(), accept))
    return dataset, queries


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Server:
    """`de serve` on a free port, optionally under heaptrack, stopped with SIGTERM.

    The server sends READY=1 to NOTIFY_SOCKET once it listens, as it does for
    systemd. The socket asks for the sender's credentials, which name the de
    process itself rather than the heaptrack script that started it.
    """

    def __init__(self, de, serve_dir, recording=None):
        self.port = free_port()
        self.scratch = Path(tempfile.mkdtemp(prefix="bench-memory-server-"))
        notify_path = str(self.scratch / "notify")
        self.notify = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.notify.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        self.notify.bind(notify_path)
        command = [de, "serve", "--location", str(serve_dir), "--bind", f"127.0.0.1:{self.port}"]
        if recording:
            # --record-only: don't open heaptrack_gui afterwards.
            command = ["heaptrack", "--record-only", "-o", str(recording), *command]
        self.log = open(self.scratch / "server.log", "w+")
        self.process = subprocess.Popen(command, env=dict(os.environ, NOTIFY_SOCKET=notify_path),
                                        stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.pid = self._wait_ready(command)

    def _wait_ready(self, command):
        deadline = time.monotonic() + SERVER_START_SECONDS
        while True:
            self.notify.settimeout(max(deadline - time.monotonic(), 0.1))
            try:
                data, ancillary, _, _ = self.notify.recvmsg(256, socket.CMSG_SPACE(struct.calcsize("3i")))
            except socket.timeout:
                self.close()
                sys.exit(f"error: `{' '.join(command)}` didn't report ready within {SERVER_START_SECONDS} s:\n"
                         f"{self.log_tail()}")
            if data.strip() != b"READY=1":
                continue
            for level, kind, payload in ancillary:
                if level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS:
                    return struct.unpack("3i", payload[:struct.calcsize("3i")])[0]
            self.close()
            sys.exit("error: the server's READY=1 came without its credentials")

    def get(self, query, accept):
        """Sends a query and reads the answer to the end, keeping none of it."""
        url = f"http://127.0.0.1:{self.port}/query?" + urllib.parse.urlencode({"query": query})
        request = urllib.request.Request(url, headers={"Accept": accept})
        with urllib.request.urlopen(request, timeout=600) as response:
            while response.read(1 << 16):
                pass

    def log_tail(self):
        self.log.flush()
        self.log.seek(0)
        return self.log.read()[-2000:]

    def close(self):
        """Stops the server and waits for it, and for heaptrack, to exit."""
        if self.process.poll() is None:
            try:
                os.kill(getattr(self, "pid", self.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=300)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.notify.close()
        self.log.close()
        shutil.rmtree(self.scratch, ignore_errors=True)
        return self.process.returncode

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def explore(server, queries, rounds):
    for _ in range(rounds):
        for _, query, accept in queries:
            server.get(query, accept)


# (name, what the workload sends to a fresh server). Each workload's peak heap
# includes opening the HDT for its first query, as `idle` shows.
#
# `construct_all` has no check of its own, only the comparison with the base:
# spareval's CONSTRUCT keeps every triple it has emitted in a hash set, to
# drop duplicates, and clears it only past 1,048,576 triples. So its peak
# grows with the result, wherever the query runs; `stream_all`, a SELECT of
# the same triples, is what shows whether the server streams.
SPARQL_HTTP_WORKLOADS = [
    ("idle", lambda server, queries: server.get("ASK {}", JSON_RESULTS)),
    ("explore_x1", lambda server, queries: explore(server, queries, 1)),
    ("explore_x5", lambda server, queries: explore(server, queries, 5)),
    ("stream_all", lambda server, queries: server.get("SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
                                                      "text/tab-separated-values")),
    ("construct_all", lambda server, queries: server.get("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
                                                         "application/n-triples")),
]


def measure_server(de, serve_dir, out, name, drive):
    """Run one server workload under heaptrack and return what heaptrack_print reports."""
    for old in out.glob(f"{name}.*"):
        old.unlink()
    started = time.monotonic()
    server = Server(de, serve_dir, recording=out / name)
    try:
        drive(server)
    except Exception as e:
        server.close()
        sys.exit(f"error: workload {name} failed: {e}")
    status = server.close()
    seconds = time.monotonic() - started
    recordings = sorted(p for p in out.glob(f"{name}.*") if p.suffix in (".gz", ".zst"))
    if status != 0 or not recordings:
        sys.exit(f"error: `de serve` under heaptrack exited with {status} in workload {name}"
                 + ("" if recordings else ", leaving no recording"))
    summary = heaptrack_summary(recordings[0])
    summary["seconds"] = round(seconds, 2)
    summary["recording"] = recordings[0].name
    print(f"{name}: peak heap {fmt_bytes(summary['peak_heap'])}, {fmt_count(summary['allocations'])} allocations, "
          f"{seconds:.1f} s under heaptrack", flush=True)
    return summary


def run_sparql_http(de, root, tmp, out):
    dataset, queries = bsbm_fixtures(root)
    serve_dir = tmp / "serve"
    serve_dir.mkdir()
    # Built by this de, untraced, in a directory of its own: the server serves
    # every .hdt file in the directory it's given.
    run_de(de, ["create", "-o", str(serve_dir / "dataset.hdt"), "-d", str(dataset)])
    # Untraced: the first query writes the HDT's index cache next to it, as the
    # criterion benchmark's first query does.
    with Server(de, serve_dir) as server:
        server.get("ASK { ?s ?p ?o }", JSON_RESULTS)
    return {name: measure_server(de, serve_dir, out, name, lambda server: drive(server, queries))
            for name, drive in SPARQL_HTTP_WORKLOADS}


def sparql_http_checks(workloads):
    """(check, passed, detail) for each check a sparql_http run can make on its own."""
    checks = []
    once, five = workloads.get("explore_x1"), workloads.get("explore_x5")
    if once and five:
        growth = change(once["peak_heap"], five["peak_heap"])
        grew = five["peak_heap"] - once["peak_heap"]
        passed = growth <= GROWTH_LIMIT or grew < MIN_HEAP_CHANGE
        checks.append(("Peak heap doesn't grow with queries answered", passed,
                       f"explore mix five times: {fmt_bytes(five['peak_heap'])}, once: {fmt_bytes(once['peak_heap'])} "
                       f"({growth:+.1f}%, limit {GROWTH_LIMIT}%)"))
    idle, stream = workloads.get("idle"), workloads.get("stream_all")
    if idle and stream:
        extra = stream["peak_heap"] - idle["peak_heap"]
        checks.append(("Results stream instead of being held", extra <= STREAM_ALLOWANCE,
                       f"SELECT of every triple: {fmt_bytes(stream['peak_heap'])}, "
                       f"one ASK: {fmt_bytes(idle['peak_heap'])} "
                       f"(+{fmt_bytes(max(extra, 0))}, allowance {fmt_bytes(STREAM_ALLOWANCE)})"))
    return checks


def command_output(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def run(args):
    if not shutil.which("heaptrack") or not shutil.which("heaptrack_print"):
        sys.exit("error: heaptrack and heaptrack_print must be on PATH (apt install heaptrack)")
    de = str(Path(args.de).resolve())
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    env = os.environ.get
    results = {
        "bench": args.bench,
        "commit": command_output("git", "-C", str(args.root), "rev-parse", "HEAD"),
        "de": command_output(de, "--version"),
        "heaptrack": command_output("heaptrack", "--version"),
        "run_id": env("GITHUB_RUN_ID"),
        "run_url": f"{env('GITHUB_SERVER_URL')}/{env('GITHUB_REPOSITORY')}/actions/runs/{env('GITHUB_RUN_ID')}"
        if env("GITHUB_RUN_ID") else None,
        "settings": {k: v for k, v in sorted(os.environ.items()) if k.startswith(SETTINGS_PREFIX) or k in SETTINGS_VARS},
        "workloads": {},
    }
    with tempfile.TemporaryDirectory(prefix=f"bench-memory-{args.bench}-") as tmp:
        if args.bench == "sparql_http":
            results["workloads"] = run_sparql_http(de, Path(args.root), Path(tmp), out)
        else:
            workloads, (warm_after, warm) = BENCHES[args.bench](Path(args.root), Path(tmp))
            for i, (name, de_args) in enumerate(workloads):
                if i == warm_after:
                    run_de(de, warm)
                results["workloads"][name] = measure(de, de_args, out, name)
    (out / RESULTS_FILE).write_text(json.dumps(results, indent=2) + "\n")
    print(f"wrote {out / RESULTS_FILE}")
    return 0


def load(arg):
    path = Path(arg)
    path = path / RESULTS_FILE if path.is_dir() else path
    return json.loads(path.read_text()) if path.is_file() else None


def describe(results):
    commit = (results.get("commit") or "unknown")[:7]
    where = f"[run {results['run_id']}]({results['run_url']})" if results.get("run_url") else "a local run"
    return f"{where} on `{commit}`"


def change(base, cur):
    return (cur / base - 1) * 100 if base else 0.0


def compare(args):
    current = load(args.current)
    if not current:
        sys.exit(f"error: no {RESULTS_FILE} at {args.current}")
    baseline = load(args.baseline) or {}
    base_loads = baseline.get("workloads", {})
    out = [f"## Memory: {current.get('bench', '')}", ""]
    if base_loads:
        against = f"its base, `{args.base_commit[:7]}`" if args.base_commit else describe(baseline)
        out.append(f"This run, {describe(current)}, against {against}: one `de` process per workload under heaptrack.")
        out.append("")
        out.append(f"Changes over {args.threshold:g}% in peak heap or in calls to allocation functions are marked, "
                   f"if they are also over {fmt_bytes(MIN_HEAP_CHANGE)} or {MIN_ALLOCATION_CHANGE:,} allocations. "
                   "Peak RSS includes heaptrack's own overhead.")
        if baseline.get("settings") != current.get("settings"):
            out += ["", f"> The settings differ: {baseline.get('settings')} then, {current.get('settings')} now."]
    elif args.base_commit:
        out.append(f"No memory results for the base, `{args.base_commit[:7]}`, so this is {describe(current)} on its own.")
    else:
        out.append(f"No baseline to compare with, so this is {describe(current)} on its own.")
    out += ["", "| Workload | Peak heap | Change | Allocations | Change | Temporary | Peak RSS | |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    grew, failed = [], 0
    for name, cur in current["workloads"].items():
        base = base_loads.get(name)
        heap, allocs = fmt_bytes(cur["peak_heap"]), fmt_count(cur["allocations"])
        if not base:
            out.append(f"| `{name}` | {heap} | | {allocs} | | {fmt_count(cur['temporary'])} | {fmt_bytes(cur['peak_rss'])} | new |")
            continue
        heap_change = change(base["peak_heap"], cur["peak_heap"])
        alloc_change = change(base["allocations"], cur["allocations"])
        heap_counts = abs(cur["peak_heap"] - base["peak_heap"]) >= MIN_HEAP_CHANGE
        alloc_counts = abs(cur["allocations"] - base["allocations"]) >= MIN_ALLOCATION_CHANGE
        heap_marked = heap_counts and abs(heap_change) >= args.threshold
        alloc_marked = alloc_counts and abs(alloc_change) >= args.threshold
        marks = []
        if heap_marked:
            marks.append("**more heap**" if heap_change > 0 else "less heap")
        if alloc_marked:
            marks.append("**more allocations**" if alloc_change > 0 else "fewer allocations")
        if (heap_marked and heap_change > 0) or (alloc_marked and alloc_change > 0):
            fails = args.fail_above is not None and heap_counts and heap_change > args.fail_above
            failed += fails
            grew.append((name, base, cur, heap_change, alloc_change, fails))
        out.append(f"| `{name}` | {heap} | {heap_change:+.1f}% | {allocs} | {alloc_change:+.1f}% | "
                   f"{fmt_count(cur['temporary'])} | {fmt_bytes(cur['peak_rss'])} | {', '.join(marks)} |")
    checks = sparql_http_checks(current["workloads"]) if current.get("bench") in SERVER_BENCHES else []
    if checks:
        out += ["", "This run's own checks, which need no baseline:", "", "| Check | | |", "| --- | --- | --- |"]
        out += [f"| {check} | {'pass' if passed else '**fail**'} | {detail} |" for check, passed, detail in checks]
    report = "\n".join(out) + "\n"
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(report)
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    for name, base, cur, heap_change, alloc_change, fails in grew:
        message = (f"{name}: peak heap {fmt_bytes(base['peak_heap'])} -> {fmt_bytes(cur['peak_heap'])} "
                   f"({heap_change:+.1f}%), allocations {fmt_count(base['allocations'])} -> "
                   f"{fmt_count(cur['allocations'])} ({alloc_change:+.1f}%)")
        if in_actions:
            level, title = ("error", "Memory regression") if fails else ("warning", "More memory")
            print(f"::{level} title={title}::{message}")
        else:
            print(f"{'regression' if fails else 'more memory'}: {message}")
    for check, passed, detail in checks:
        if not passed:
            print(f"::error title=Memory check failed::{check}: {detail}" if in_actions
                  else f"check failed: {check}: {detail}")
    failed_checks = sum(not passed for _, passed, _ in checks)
    if failed:
        print(f"failing: {failed} workloads' peak heap grew by more than {args.fail_above:g}% (--fail-above)",
              file=sys.stderr)
    if failed_checks:
        print(f"failing: {failed_checks} of this run's own checks failed", file=sys.stderr)
    return 1 if failed or failed_checks else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run", help="measure one build's memory on a benchmark's fixtures")
    run_parser.add_argument("bench", choices=sorted([*BENCHES, *SERVER_BENCHES]))
    run_parser.add_argument("--de", required=True, help="the de binary to measure")
    run_parser.add_argument("--root", default=".", help="the de checkout whose fixtures to use (default: .)")
    run_parser.add_argument("--out", required=True, help="directory for the recordings and memory.json")
    cmp_parser = sub.add_parser("compare", help="compare two runs")
    cmp_parser.add_argument("baseline", help="the baseline's memory.json, or its directory (missing means none)")
    cmp_parser.add_argument("current", help="this run's memory.json, or its directory")
    cmp_parser.add_argument("--threshold", type=float, default=5.0,
                            help="smallest change to mark, in percent (default: 5)")
    cmp_parser.add_argument("--fail-above", type=float, help="exit 1 when a peak heap grew by more than this percent")
    cmp_parser.add_argument("--base-commit", help="the commit the current run builds on")
    args = parser.parse_args()
    return run(args) if args.command == "run" else compare(args)


if __name__ == "__main__":
    sys.exit(main())
