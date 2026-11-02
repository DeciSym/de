#!/usr/bin/env python3
"""Record criterion results, and compare them with an earlier run.

The Benchmarks workflow (.github/workflows/bench.yml) uploads every run's
criterion estimates as an artifact, then compares them with the newest run on
main. This script does both halves:

    scripts/bench-compare.py record target/criterion
        Write bench-run.json beside the results: the commit, the CI run, the
        toolchain and the machine, so that a later comparison can say what it
        is comparing against.

    scripts/bench-compare.py compare BASELINE CURRENT [--threshold PCT]
                                                      [--fail-above PCT]
        Print a Markdown table comparing two sets of results, and append it to
        the job summary when run in GitHub Actions. BASELINE and CURRENT are
        criterion output directories: target/criterion, or a downloaded
        artifact. Each reads the latest run (criterion's `new`) unless a
        criterion baseline name follows a colon, so a local A/B looks like:

            cargo bench --bench trainmarks -- --save-baseline main   # on main
            cargo bench --bench trainmarks -- --save-baseline mine   # on a branch
            scripts/bench-compare.py compare target/criterion:main target/criterion:mine

A difference counts as slower or faster only when it is larger than
--threshold percent and the two medians' 95% confidence intervals don't
overlap. GitHub's runners are shared virtual machines, so identical code moves
by a few percent from run to run, and more when the runner's CPU model differs.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RUN_FILE = "bench-run.json"

# Environment variables that pick fixtures or scales, such as DE_BENCH_SCALE.
# Results are comparable only when these match.
SETTINGS_PREFIX = "DE_"


def parse_tree(arg):
    """Split `DIR[:NAME]` into a directory and a criterion baseline name."""
    path, sep, name = arg.rpartition(":")
    if not sep or not path or not name or "/" in name:
        return Path(arg), "new"
    return Path(path), name


def load_results(root, baseline):
    """Map each benchmark's id to its median and the median's 95% CI, in ns."""
    results = {}
    for estimates in sorted(root.glob(f"**/{baseline}/estimates.json")):
        try:
            bench_id = json.loads((estimates.parent / "benchmark.json").read_text())["full_id"]
            median = json.loads(estimates.read_text())["median"]
            ci = median["confidence_interval"]
            results[bench_id] = (median["point_estimate"], ci["lower_bound"], ci["upper_bound"])
        except (OSError, KeyError, TypeError, ValueError) as e:
            print(f"warning: skipping {estimates.parent}: {e}", file=sys.stderr)
    return results


def read_run(root):
    """The bench-run.json `record` wrote for a result tree, or {}."""
    for path in [root / RUN_FILE, *sorted(root.glob(f"**/{RUN_FILE}"))]:
        if path.is_file():
            return json.loads(path.read_text())
    return {}


def command_output(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def cpu_model():
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return command_output("sysctl", "-n", "machdep.cpu.brand_string")


def record(args):
    root = Path(args.dir)
    results = load_results(root, "new")
    if not results:
        sys.exit(f"error: no criterion results under {root}; did the benchmarks run?")

    env = os.environ.get
    run_url = None
    if env("GITHUB_RUN_ID"):
        run_url = f"{env('GITHUB_SERVER_URL')}/{env('GITHUB_REPOSITORY')}/actions/runs/{env('GITHUB_RUN_ID')}"
    run = {
        "commit": env("GITHUB_SHA") or command_output("git", "rev-parse", "HEAD"),
        "ref": env("GITHUB_REF") or command_output("git", "rev-parse", "--abbrev-ref", "HEAD"),
        "run_id": env("GITHUB_RUN_ID"),
        "run_url": run_url,
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rustc": command_output("rustc", "--version"),
        "cpu": cpu_model(),
        "cpus": os.cpu_count(),
        "settings": {k: v for k, v in sorted(os.environ.items()) if k.startswith(SETTINGS_PREFIX)},
        "benchmarks": len(results),
    }
    (root / RUN_FILE).write_text(json.dumps(run, indent=2) + "\n")
    print(f"recorded {len(results)} benchmarks on {run['cpu']} ({run['rustc']})")


def fmt_time(ns):
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.4g} {unit}"
    return f"{ns:.4g} ns"


def describe(run, root, name):
    """One line saying which run a result tree came from."""
    if not run:
        return f"`{root}`" + (f" (baseline `{name}`)" if name != "new" else "")
    commit = (run.get("commit") or "unknown")[:7]
    where = f"[run {run['run_id']}]({run['run_url']})" if run.get("run_url") else "a local run"
    when = (run.get("recorded_at") or "")[:10]
    return f"{where} on `{commit}`" + (f", {when}" if when else "")


def compare(args):
    base_root, base_name = parse_tree(args.baseline)
    cur_root, cur_name = parse_tree(args.current)
    current = load_results(cur_root, cur_name)
    if not current:
        sys.exit(f"error: no criterion results under {cur_root} for `{cur_name}`")
    baseline = load_results(base_root, base_name) if base_root.is_dir() else {}
    base_run, cur_run = read_run(base_root), read_run(cur_root)

    rows, slower, faster = [], [], []
    for bench_id in sorted(current.keys() | baseline.keys()):
        cur, base = current.get(bench_id), baseline.get(bench_id)
        if base is None:
            rows.append((bench_id, "", fmt_time(cur[0]), "", "new"))
            continue
        if cur is None:
            rows.append((bench_id, fmt_time(base[0]), "", "", "not run"))
            continue
        change = (cur[0] / base[0] - 1) * 100
        overlap = cur[1] <= base[2] and base[1] <= cur[2]
        verdict = ""
        if abs(change) >= args.threshold and not overlap:
            verdict = "**slower**" if change > 0 else "faster"
            (slower if change > 0 else faster).append((bench_id, base[0], cur[0], change))
        rows.append((bench_id, fmt_time(base[0]), fmt_time(cur[0]), f"{change:+.1f}%", verdict))

    out = ["## Benchmarks", ""]
    if baseline:
        out.append(f"This run, {describe(cur_run, cur_root, cur_name)}, against {describe(base_run, base_root, base_name)}.")
        out.append("")
        out.append(
            f"{len(slower)} slower and {len(faster)} faster, counting changes over "
            f"{args.threshold:g}% whose 95% confidence intervals don't overlap. "
            "Medians of criterion's samples."
        )
        notes = []
        if base_run.get("cpu") and cur_run.get("cpu") and base_run["cpu"] != cur_run["cpu"]:
            notes.append(
                f"The baseline ran on a different CPU ({base_run['cpu']}; this run: {cur_run['cpu']}), "
                "so some of the difference may be the machine."
            )
        if base_run.get("rustc") and cur_run.get("rustc") and base_run["rustc"] != cur_run["rustc"]:
            notes.append(f"The toolchain changed: {base_run['rustc']} then, {cur_run['rustc']} now.")
        if base_run.get("settings") is not None and cur_run.get("settings") is not None:
            if base_run["settings"] != cur_run["settings"]:
                notes.append(f"The settings changed: {base_run['settings']} then, {cur_run['settings']} now.")
        if notes:
            out += ["", *(f"> {note}" for note in notes)]
    else:
        out.append(
            f"No baseline to compare with, so this is {describe(cur_run, cur_root, cur_name)} on its own. "
            "Baselines come from this workflow's successful runs on main."
        )
    out += ["", "| Benchmark | Baseline | This run | Change | |", "| --- | ---: | ---: | ---: | --- |"]
    out += [f"| `{bench_id}` | {b} | {c} | {pct} | {verdict} |" for bench_id, b, c, pct, verdict in rows]
    report = "\n".join(out) + "\n"

    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(report)
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    failed = 0
    for bench_id, base, cur, change in slower:
        fails = args.fail_above is not None and change > args.fail_above
        failed += fails
        message = f"{bench_id}: {fmt_time(base)} -> {fmt_time(cur)} ({change:+.1f}%)"
        if in_actions:
            level, title = ("error", "Benchmark regression") if fails else ("warning", "Benchmark slower")
            print(f"::{level} title={title}::{message}")
        else:
            print(f"{'regression' if fails else 'slower'}: {message}")

    if failed:
        print(f"failing: {failed} slower by more than {args.fail_above:g}% (--fail-above)", file=sys.stderr)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    rec = sub.add_parser("record", help="write bench-run.json next to criterion results")
    rec.add_argument("dir", help="criterion output directory, usually target/criterion")

    cmp = sub.add_parser("compare", help="compare two sets of criterion results")
    cmp.add_argument("baseline", help="baseline results: DIR or DIR:NAME (missing DIR means no baseline)")
    cmp.add_argument("current", help="current results: DIR or DIR:NAME")
    cmp.add_argument("--threshold", type=float, default=10.0, help="smallest change to report, in percent (default: 10)")
    cmp.add_argument("--fail-above", type=float, help="exit 1 when a benchmark is slower by more than this percent")

    args = parser.parse_args()
    if args.command == "record":
        record(args)
        return 0
    return compare(args)


if __name__ == "__main__":
    sys.exit(main())
