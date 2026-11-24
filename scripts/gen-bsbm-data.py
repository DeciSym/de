#!/usr/bin/env python3
"""Generate the Berlin SPARQL Benchmark fixtures the ``bsbm`` bench target reads.

Comunica's ``benchmark-bsbm-hdt`` drives BSBM through jbr, which runs the
official ``bsbmtools`` generator and then the official Java test driver against a
live SPARQL endpoint. ``de``'s benchmarks are in-process criterion targets, so
the driver half does not transfer: this script keeps the generator (so the data
really is BSBM data) and replaces the driver's parameter pool with a
deterministic substitution derived from the generated graph.

What it writes, under ``benches/bsbm/<product-count>/``:

    dataset.nt          the generated graph, N-Triples
    queries/*.rq        five concrete instances of each of the twelve
                        "explore" query templates
    queries/manifest.tsv  name, file and result form of each instance

The twelve templates are bsbmtools' own (``queries/explore/query*.txt``); only
the ``%Placeholder%`` values are ours. The test driver draws those at random
from pools it builds while generating; we draw them from the finished graph
instead, choosing values that are known to occur so that the queries do real
join work rather than bottoming out on an empty index lookup. The choice is
seeded and the generator is deterministic, so a given product count always
yields the same fixtures.

The ``bsbmtools`` archive is downloaded once into ``benches/bsbm/.tools``.
Java is required to run the generator; nothing else here needs it.

Usage:
    scripts/gen-bsbm-data.py [PRODUCT_COUNT] [--force]

Product count 1000 is Comunica's ``benchmark-bsbm-hdt`` (~372K triples) and
10000 is its ``benchmark-bsbm-hdt-10k`` (~3.5M triples).
"""

import argparse
import io
import os
import random
import re
import shutil
import subprocess
import sys
import urllib.request
import zipfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BSBM_ROOT = os.path.join(REPO_ROOT, "benches", "bsbm")
TOOLS_DIR = os.path.join(BSBM_ROOT, ".tools")
TOOLS_HOME = os.path.join(TOOLS_DIR, "bsbmtools-0.2")
TOOLS_URL = (
    "https://downloads.sourceforge.net/project/bsbmtools/"
    "bsbmtools/bsbmtools-0.2/bsbmtools-v0.2.zip"
)

BSBM = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
REV = "http://purl.org/stuff/rev#"
XSD_DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime"

# Result form per template, which decides the serialiser the bench uses.
# Nine DESCRIBEs and twelve CONSTRUCTs; the other ten are SELECTs.
QUERY_FORMS = {
    1: "select", 2: "select", 3: "select", 4: "select",
    5: "select", 6: "select", 7: "select", 8: "select",
    9: "graph", 10: "select", 11: "select", 12: "graph",
}

INSTANCES_PER_TEMPLATE = 5
SEED = 42


def log(msg):
    print(msg, flush=True)


def ensure_tools():
    """Download and unpack bsbmtools if it is not already there."""
    generator = os.path.join(TOOLS_HOME, "lib", "bsbm.jar")
    if os.path.exists(generator):
        return
    if shutil.which("java") is None:
        sys.exit(
            "error: java is required to run the BSBM generator "
            "(only for fixture generation, not to build or run de)"
        )
    os.makedirs(TOOLS_DIR, exist_ok=True)
    log(f"downloading bsbmtools from {TOOLS_URL}")
    with urllib.request.urlopen(TOOLS_URL) as response:
        payload = response.read()
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        archive.extractall(TOOLS_DIR)
    if not os.path.exists(generator):
        sys.exit(f"error: {generator} missing after unpacking bsbmtools")


def generate_dataset(product_count, dest_dir):
    """Run the bsbmtools generator, writing <dest_dir>/dataset.nt."""
    dataset = os.path.join(dest_dir, "dataset.nt")
    os.makedirs(dest_dir, exist_ok=True)
    # The generator reads titlewords.txt and givennames.txt relative to the
    # working directory, so it has to run from inside the tools tree; output
    # paths are absolute to land in the fixture directory anyway.
    classpath = os.pathsep.join(
        [".", os.path.join("lib", "*")]
    )
    cmd = [
        "java", "-cp", classpath, "-Xmx2G", "benchmark.generator.Generator",
        "-pc", str(product_count),
        "-s", "nt",
        "-fn", os.path.splitext(dataset)[0],
        "-dir", os.path.join(dest_dir, "td_data"),
    ]
    log(f"generating BSBM data at product count {product_count}")
    subprocess.run(cmd, cwd=TOOLS_HOME, check=True)
    if not os.path.exists(dataset):
        sys.exit(f"error: generator did not write {dataset}")
    return dataset


TRIPLE_RE = re.compile(r"^(\S+)\s+(\S+)\s+(.*)\s+\.\s*$")


def parse_dataset(path):
    """One pass over the N-Triples file, collecting just what the templates need.

    The generator emits one well-formed triple per line with no blank nodes and
    no multi-line literals, so a regex split is enough and avoids pulling in an
    RDF parser for a fixture script.
    """
    products = {}          # product IRI -> {"type": [...], "feature": [...], "num1/2/3": int, "label": str}
    offers = set()
    reviews = set()
    valid_to = []
    # Labels are collected for every subject and attached afterwards: the
    # generator does not group a subject's triples, so a product's rdfs:label
    # can appear before the triple that first identifies it as a product.
    labels = {}

    def product_slot(subject):
        return products.setdefault(
            subject,
            {"type": [], "feature": [], "num1": None, "num2": None, "num3": None, "label": None},
        )

    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            match = TRIPLE_RE.match(line)
            if match is None:
                continue
            subject, predicate, obj = match.groups()
            subject = subject[1:-1] if subject.startswith("<") else subject
            predicate = predicate[1:-1] if predicate.startswith("<") else predicate

            if predicate == BSBM + "productFeature":
                product_slot(subject)["feature"].append(obj[1:-1])
            elif predicate == RDF_TYPE:
                # Only products, not the ProductFeature and ProductType
                # resources that also carry "Product" in their class IRI.
                type_iri = obj[1:-1] if obj.startswith("<") else obj
                if type_iri == BSBM + "Product" or "/instances/ProductType" in type_iri:
                    product_slot(subject)["type"].append(type_iri)
            elif predicate.startswith(BSBM + "productPropertyNumeric"):
                which = predicate[len(BSBM) + len("productPropertyNumeric"):]
                if which in ("1", "2", "3"):
                    product_slot(subject)["num" + which] = int(literal_value(obj))
            elif predicate == BSBM + "price":
                offers.add(subject)
            elif predicate == REV + "reviewer":
                reviews.add(subject)
            elif predicate == BSBM + "validTo":
                valid_to.append(literal_value(obj))
            elif predicate == RDFS_LABEL:
                labels[subject] = literal_value(obj)

    for iri, product in products.items():
        product["label"] = labels.get(iri)

    return products, sorted(offers), sorted(reviews), sorted(valid_to)


def literal_value(obj):
    """The lexical form of an N-Triples literal, or the raw token if not one."""
    if not obj.startswith('"'):
        return obj
    return obj[1:obj.rindex('"')]


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return 0
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def pick_anchor_products(products, count, rng):
    """Products rich enough to instantiate every template that needs one."""
    usable = sorted(
        iri for iri, p in products.items()
        if len(p["feature"]) >= 3
        and p["type"]
        and p["label"]
        and p["num1"] is not None
        and p["num2"] is not None
        and p["num3"] is not None
    )
    if len(usable) < count:
        sys.exit(
            f"error: only {len(usable)} products have the properties the BSBM "
            f"templates need; generate at a larger product count"
        )
    return rng.sample(usable, count)


def pick_word(products, rng):
    """The most common word across product labels, for query 6's regex.

    BSBM draws label words from an 89K-word list, so at a thousand products
    almost every word is unique; picking the most frequent one is the only way
    to give the regex scan something to match. Ties are broken by sorting, so
    the choice is stable for a given graph.
    """
    counts = {}
    for product in products.values():
        if not product["label"]:
            continue
        for word in set(re.findall(r"[A-Za-z]{4,}", product["label"])):
            counts[word] = counts.get(word, 0) + 1
    if not counts:
        return "the"
    best = max(counts.values())
    return rng.choice(sorted(word for word, n in counts.items() if n == best))


def build_parameters(products, offers, reviews, valid_to, rng):
    """Five substitution maps per template, in template order."""
    anchors = pick_anchor_products(products, INSTANCES_PER_TEMPLATE, rng)
    nums1 = [p["num1"] for p in products.values() if p["num1"] is not None]
    nums2 = [p["num2"] for p in products.values() if p["num2"] is not None]
    nums3 = [p["num3"] for p in products.values() if p["num3"] is not None]
    # Thresholds that keep roughly three quarters of the graph on the matching
    # side of each FILTER: selective enough to be a real predicate, loose enough
    # that the join underneath still has work to do.
    x_threshold = percentile(nums1, 0.25)
    y_upper = percentile(nums3, 0.75)
    y_lower = percentile(nums2, 0.25)
    mid_date = valid_to[len(valid_to) // 4] if valid_to else "2008-06-01T00:00:00"
    current_date = f'"{mid_date}"^^<{XSD_DATETIME}>'
    word = pick_word(products, rng)

    instances = []
    for index, product_iri in enumerate(anchors):
        product = products[product_iri]
        features = sorted(product["feature"])
        # A leaf type where one exists: the generator gives every product
        # bsbm:Product plus one or more ProductTypeN, and only the latter is
        # selective enough to be worth substituting.
        types = sorted(t for t in product["type"] if t != BSBM + "Product")
        product_type = types[0] if types else sorted(product["type"])[0]
        instances.append({
            "%ProductXYZ%": f"<{product_iri}>",
            "%ProductType%": f"<{product_type}>",
            "%ProductFeature1%": f"<{features[0]}>",
            "%ProductFeature2%": f"<{features[1]}>",
            "%ProductFeature3%": f"<{features[2]}>",
            "%OfferXYZ%": f"<{offers[(index * len(offers)) // INSTANCES_PER_TEMPLATE]}>",
            "%ReviewXYZ%": f"<{reviews[(index * len(reviews)) // INSTANCES_PER_TEMPLATE]}>",
            "%currentDate%": current_date,
            "%word1%": word,
            # Query 3 reads %y% as an upper bound on productPropertyNumeric3;
            # query 4 reads it as a lower bound on productPropertyNumeric2.
            # One value cannot serve both, so each template gets its own.
            "%x%": str(x_threshold),
            "%y%": str(y_upper),
            "%y@4%": str(y_lower),
        })
    return instances


def instantiate(template_text, params, query_number):
    text = template_text
    if query_number == 4:
        text = text.replace("%y%", params["%y@4%"])
    for placeholder, value in params.items():
        if placeholder == "%y@4%":
            continue
        text = text.replace(placeholder, value)
    leftover = re.findall(r"%[A-Za-z0-9]+%", text)
    if leftover:
        sys.exit(f"error: query {query_number} still has placeholders {leftover}")
    return text


def write_queries(dest_dir, instances):
    queries_dir = os.path.join(dest_dir, "queries")
    if os.path.exists(queries_dir):
        shutil.rmtree(queries_dir)
    os.makedirs(queries_dir)
    manifest = []
    for number in range(1, 13):
        template_path = os.path.join(
            TOOLS_HOME, "queries", "explore", f"query{number}.txt"
        )
        with open(template_path, "r", encoding="utf-8") as handle:
            template_text = handle.read()
        for index, params in enumerate(instances, start=1):
            name = f"q{number:02d}_{index}"
            filename = f"{name}.rq"
            with open(os.path.join(queries_dir, filename), "w", encoding="utf-8") as out:
                out.write(instantiate(template_text, params, number))
            manifest.append((name, filename, QUERY_FORMS[number]))
    with open(os.path.join(queries_dir, "manifest.tsv"), "w", encoding="utf-8") as out:
        out.write("# name\tfile\tform\n")
        for row in manifest:
            out.write("\t".join(row) + "\n")
    return len(manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product_count", nargs="?", type=int, default=1000)
    parser.add_argument(
        "--force", action="store_true",
        help="regenerate even if the fixtures are already present",
    )
    args = parser.parse_args()

    dest_dir = os.path.join(BSBM_ROOT, str(args.product_count))
    dataset = os.path.join(dest_dir, "dataset.nt")
    manifest = os.path.join(dest_dir, "queries", "manifest.tsv")

    if not args.force and os.path.exists(dataset) and os.path.exists(manifest):
        log(f"bsbm-{args.product_count} fixtures present at {dest_dir}")
        return

    ensure_tools()
    if args.force or not os.path.exists(dataset):
        generate_dataset(args.product_count, dest_dir)
    else:
        log(f"reusing {dataset}")

    log("deriving query parameters from the generated graph")
    products, offers, reviews, valid_to = parse_dataset(dataset)
    if not offers or not reviews:
        sys.exit("error: generated graph has no offers or no reviews")
    rng = random.Random(SEED)
    instances = build_parameters(products, offers, reviews, valid_to, rng)
    written = write_queries(dest_dir, instances)

    log(f"bsbm-{args.product_count} ready:")
    log(f"  {os.path.getsize(dataset) // (1024 * 1024)} MiB  {dataset}")
    log(f"  {written} query instances in {os.path.join(dest_dir, 'queries')}")


if __name__ == "__main__":
    main()
