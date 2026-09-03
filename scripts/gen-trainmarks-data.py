#!/usr/bin/env python3
"""Generate the N-Triples fixture the trainmarks benchmark suite runs against.

The trainmarks submodule (``benches/trainmarks``) ships ``generate_data.py``,
which writes both Turtle and N-Triples at all three scales -- roughly 1.7 GB of
files. The benchmarks read one scale, so this wrapper imports the submodule's
generator and emits just that one. Both serialisations are written: the
benchmarks build HDT from the ``.nt`` and, separately, from the ``.ttl``, so
that the RDF parser stays on the measured path.

The generator is deterministic (``random.seed(42)`` at import time), so the same
scale always yields byte-identical output and timings stay comparable across
machines and across runs. It is not, however, byte-identical to the file
trainmarks' own ``generate_data.py`` writes: that script draws all three scales
from one RNG stream, so its ``large.nt`` starts where ``medium`` left off. The
two are statistically the same graph and the query timings match, but do not
expect the files to hash the same.

Usage:
    scripts/gen-trainmarks-data.py [medium|large|xlarge] [--force]
"""

import argparse
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRAINMARKS = os.path.join(REPO_ROOT, "benches", "trainmarks")
DATA_DIR = os.path.join(TRAINMARKS, "data")

# Customer/product/order counts per scale, copied from the trainmarks
# generator's __main__ block (which hard-codes them inline rather than
# exposing them as constants we could import).
SCALES = {
    "medium": (1_000, 200, 13_000),
    "large": (10_000, 2_000, 133_000),
    "xlarge": (100_000, 10_000, 1_335_000),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scale", nargs="?", default="large", choices=sorted(SCALES))
    parser.add_argument(
        "--force",
        action="store_true",
        help="regenerate even if the .nt file is already present",
    )
    args = parser.parse_args()

    if not os.path.isfile(os.path.join(TRAINMARKS, "generate_data.py")):
        sys.exit(
            f"trainmarks submodule not checked out at {TRAINMARKS}\n"
            "run: git submodule update --init benches/trainmarks"
        )

    outputs = {ext: os.path.join(DATA_DIR, f"{args.scale}.{ext}") for ext in ("nt", "ttl")}
    if all(os.path.isfile(p) for p in outputs.values()) and not args.force:
        for path in outputs.values():
            size_mb = os.path.getsize(path) / 1024 / 1024
            print(f"{path} already present ({size_mb:.1f} MB)")
        print("use --force to regenerate")
        return

    sys.path.insert(0, TRAINMARKS)
    import generate_data

    os.makedirs(DATA_DIR, exist_ok=True)
    n_customers, n_products, n_orders = SCALES[args.scale]

    t0 = time.time()
    triples = generate_data.generate_triples(n_customers, n_products, n_orders)
    print(f"generated {len(triples)} triples in {time.time() - t0:.1f}s")

    for ext, writer in (("nt", generate_data.write_ntriples), ("ttl", generate_data.write_turtle)):
        path = outputs[ext]
        t0 = time.time()
        writer(triples, path)
        size_mb = os.path.getsize(path) / 1024 / 1024
        print(f"wrote {path} ({size_mb:.1f} MB) in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
