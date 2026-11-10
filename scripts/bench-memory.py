#!/usr/bin/env python3
"""Measure the heap memory `de` uses on the benchmark fixtures, and compare two runs.

The Benchmarks workflow (.github/workflows/bench.yml) runs this on a change and on
its base commit, one after the other on one runner, compares the two, and keeps
the change's heaptrack recordings as an artifact:

    scripts/bench-memory.py run BENCH --de PATH [--root DIR] --out DIR
        Run each workload of BENCH (trainmarks or watdiv) once under heaptrack,
        in a `de` process of its own, on the fixtures that the criterion
        benchmark of the same name reads from --root, a checkout of de. Writes
        each workload's recording to --out, and --out/memory.json with what
        heaptrack_print reports for it: peak heap, calls to allocation
        functions, temporary allocations, memory leaked and peak RSS.

    scripts/bench-memory.py compare BASELINE CURRENT [--threshold PCT]
                                    [--fail-above PCT] [--base-commit SHA]
        Print a Markdown table comparing two runs' memory.json (a --out
        directory, or the file), and append it to the job summary when run in
        GitHub Actions. Exits 1 when a workload's peak heap grew by more than
        --fail-above percent.

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
import subprocess
import sys
import tempfile
import time
from pathlib import Path

RESULTS_FILE = "memory.json"
# Smaller changes than these are never marked, whatever the percentage: a
# WatDiv query peaks at about 0.5 MB, where 5% is a 25 KB buffer.
MIN_HEAP_CHANGE = 256_000
MIN_ALLOCATION_CHANGE = 1_000
SETTINGS_PREFIX = "DE_"
SETTINGS_VARS = ("RAYON_NUM_THREADS",)

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
    if failed:
        print(f"failing: {failed} workloads' peak heap grew by more than {args.fail_above:g}% (--fail-above)",
              file=sys.stderr)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run", help="measure one build's memory on a benchmark's fixtures")
    run_parser.add_argument("bench", choices=sorted(BENCHES))
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
