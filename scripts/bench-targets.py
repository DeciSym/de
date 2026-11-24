#!/usr/bin/env python3
"""Choose the benchmark targets a change needs, for .github/workflows/bench.yml.

    scripts/bench-targets.py [--base SHA] [--head REF] [--all]

Prints two lines for $GITHUB_OUTPUT: `matrix=`, the matrix entries (bench,
fixtures, cargo-args, bsbm) of the targets to run, as JSON, and `any=`, true
when there is at least one. With --all, or without a base commit to compare
with, every target runs. When run in GitHub Actions it also writes which
targets run, and why, to the job summary.

A target runs unless every file the change touches is one that target doesn't
use. Each target lists those files below, as fnmatch patterns (in which `*`
also matches `/`). A file no list mentions, such as a new module, runs every
target: a forgotten entry costs a benchmark run, not a missed regression.

To add a target, add it to TARGETS with the command that prepares its
fixtures.
"""

import argparse
import fnmatch
import json
import os
import subprocess
import sys

# Every target uses these, whatever else the lists below say: the benchmark
# workflow and the scripts that run and compare the benchmarks.
USED_BY_ALL = [
    ".github/workflows/bench.yml",
    "scripts/bench-compare.py",
    "scripts/bench-memory.py",
    "scripts/bench-targets.py",
]

# No target uses these.
UNUSED_BY_ALL = [
    "*.md",
    ".config/*",
    ".github/*",
    "Makefile",
    "endpoint-kit/*",
    "scripts/Dockerfile",
    "scripts/download-sample-bench.sh",
    "templates/*",
    "tests/*",
    "w3c-validation/*",
]

# The SPARQL endpoint, which only sparql_http runs.
SERVER = ["src/serve.rs", "src/serve/*", "src/service_description.rs"]
# Building HDT files from RDF, which only trainmarks times. The other targets
# build their fixture HDT untimed, or download it.
CREATE = ["src/create.rs", "src/enrich.rs", "src/rdf2nt.rs"]

TARGETS = [
    {
        # `de create` and `de query`, through the library and the binary.
        "bench": "trainmarks",
        "fixtures": 'python3 scripts/gen-trainmarks-data.py "$DE_BENCH_SCALE"',
        "unused": [
            *SERVER,
            "src/view.rs",
            "benches/sparql_http.rs",
            "benches/watdiv.rs",
            "scripts/fetch-watdiv-data.sh",
            "scripts/gen-bsbm-data.py",
        ],
    },
    {
        # `de query` on a downloaded HDT file.
        "bench": "watdiv",
        "fixtures": 'scripts/fetch-watdiv-data.sh "$DE_WATDIV_SCALE"',
        "unused": [
            *SERVER,
            *CREATE,
            "src/view.rs",
            "benches/sparql_http.rs",
            "benches/trainmarks",
            "benches/trainmarks.rs",
            "scripts/gen-bsbm-data.py",
            "scripts/gen-trainmarks-data.py",
        ],
    },
    {
        # `de serve`, over HTTP. It evaluates through src/sparql.rs, not
        # src/query.rs, which is `de query`'s path.
        "bench": "sparql_http",
        "fixtures": 'python3 scripts/gen-bsbm-data.py "$DE_BSBM_PRODUCT_COUNT"',
        "cargo-args": "--features server",
        "bsbm": True,
        "unused": [
            *CREATE,
            "src/query.rs",
            "src/view.rs",
            "benches/trainmarks",
            "benches/trainmarks.rs",
            "benches/watdiv.rs",
            "scripts/fetch-watdiv-data.sh",
            "scripts/gen-trainmarks-data.py",
        ],
    },
]


def matches(path, patterns):
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def uses(target, path):
    """Whether `target` can be affected by a change to `path`."""
    if matches(path, USED_BY_ALL):
        return True
    return not matches(path, UNUSED_BY_ALL) and not matches(path, target["unused"])


def changed_files(base, head):
    """The files that differ between `base` and `head`, or None if git can't tell."""
    done = subprocess.run(["git", "diff", "--name-only", base, head], capture_output=True, text=True)
    if done.returncode != 0:
        print(f"warning: can't diff {base}..{head}, so every target runs: {done.stderr.strip()}", file=sys.stderr)
        return None
    return [line for line in done.stdout.splitlines() if line]


def choose(files):
    """[(target, files that make it run)], with None for files meaning "run everything"."""
    if files is None:
        return [(target, None) for target in TARGETS]
    return [(target, [f for f in files if uses(target, f)]) for target in TARGETS]


def summary(chosen, files, base):
    lines = ["## Benchmark targets", ""]
    if files is None:
        lines.append("Every target runs: no base commit to compare with, or a run started by hand.")
    else:
        lines.append(f"{len(files)} files changed since `{base[:7]}`. A target runs when one of them is "
                     "a file it uses (scripts/bench-targets.py).")
        lines += ["", "| Target | Runs | Because of |", "| --- | --- | --- |"]
        for target, why in chosen:
            shown = ", ".join(f"`{f}`" for f in why[:3]) + (f" and {len(why) - 3} more" if len(why) > 3 else "")
            lines.append(f"| `{target['bench']}` | {'yes' if why else 'no'} | {shown} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default="", help="the commit to compare with (empty: run every target)")
    parser.add_argument("--head", default="HEAD", help="the commit under test (default: HEAD)")
    parser.add_argument("--all", action="store_true", help="run every target")
    args = parser.parse_args()

    files = None if args.all or not args.base else changed_files(args.base, args.head)
    chosen = choose(files)
    include = [{k: v for k, v in target.items() if k != "unused"}
               for target, why in chosen if why is None or why]
    print(f"matrix={json.dumps({'include': include}, separators=(',', ':'))}")
    print(f"any={'true' if include else 'false'}")
    report = summary(chosen, files, args.base)
    print(report, file=sys.stderr)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
