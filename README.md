[![Latest Version](https://img.shields.io/crates/v/de.svg)](https://crates.io/crates/de)
[![Lint](https://github.com/DeciSym/de/actions/workflows/format_check.yml/badge.svg)](https://github.com/DeciSym/de/actions/workflows/format_check.yml)
[![Build](https://github.com/DeciSym/de/actions/workflows/test_build.yml/badge.svg)](https://github.com/DeciSym/de/actions/workflows/test_build.yml)
[![Documentation](https://docs.rs/de/badge.svg)](https://docs.rs/de/)

# DeciSym Engine (`de`)

`de` is a command-line tool for creating, querying, and inspecting RDF
data in [HDT](http://www.rdfhdt.org/) (Header, Dictionary, Triples) format.

It is intended for workflows where RDF data needs compact storage and
SPARQL querying over both RDF and HDT inputs.

## Installation

Run directly from a local clone (no install required):

```sh
git clone https://github.com/DeciSym/de.git
cd de
cargo run -- --help
```

Install the CLI from crates.io:

```sh
cargo install de
```

Install with server command enabled:

```sh
cargo install --features server de
```

Install the CLI from a local clone:

```sh
cargo install --path .
```

Docker image:

```sh
docker run --rm decisym/de:latest --help
```

## Example

The example below corresponds to the SPARQL 1.1 Query Recommendation:

- §2.1 Triple Patterns:
  <https://www.w3.org/TR/sparql11-query/#basicpatterns>

From the repository root, create the example input data (`simple.nt`):

```nt
<http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> "SPARQL Tutorial" .
```

Create the query (`simple.rq`):

```sparql
SELECT ?title
WHERE
{
  <http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> ?title .
}
```

Run directly against RDF:

```sh
cargo run -- query --data simple.nt --sparql simple.rq --output csv
```

Output:

```csv
title
SPARQL Tutorial
```

Convert to HDT and run the same query:

```sh
cargo run -- create --output-name simple.hdt --data simple.nt
cargo run -- query --data simple.hdt --sparql simple.rq --output csv
```

Output:

```csv
title
SPARQL Tutorial
```

## Command Reference

Use CLI help (and installed man pages, if available in your
environment) as the canonical command reference:

```sh
cargo run -- --help
cargo run -- <command> --help
```

If you installed the CLI:

```sh
de --help
de <command> --help
```

Examples:

```sh
cargo run -- create --help
cargo run -- query --help
cargo run -- view --help
```

## Development

Run core checks:

```sh
cargo fmt --check
cargo clippy --all-features --all-targets -- -D warnings
cargo test --all-features
```

Run W3C RDF/SPARQL integration tests:

```sh
cargo test --all-features --test w3c-sparql
```

## Benchmarks

The `trainmarks` criterion suite covers `create` and `query` over the synthetic
e-commerce graph from [trainmarks](https://github.com/DeciSym/trainmarks),
checked out as a submodule at `benches/trainmarks`. It exists to catch
performance regressions at a realistic dataset size, not to compare `de`
against other engines.

It measures four things: building an HDT from N-Triples, building one from
Turtle (the only path that runs the RDF parser), the five queries against a
prebuilt HDT, and one query given a Turtle file directly, which `de` converts
to a temporary package before evaluating.

`make bench` first checks out the submodule and generates the fixtures the
suite reads:

```sh
make bench
```

The dataset scale is `BENCH_SCALE` — `medium` (~100K triples), `large` (~1M,
the default) or `xlarge` (~10M):

```sh
BENCH_SCALE=medium make bench
```

`make bench` passes the scale through to the suite as `DE_BENCH_SCALE`, which
is also what to set when driving `cargo bench` directly. The fixtures and the
benchmark must agree on it, since the scale is part of every benchmark id
(`trainmarks_query/large/q3_join_3_entities`) and criterion compares each run
against the stored baseline for that id:

```sh
make bench-init                          # once, to lay down the fixtures
DE_BENCH_SCALE=large cargo bench --bench trainmarks
DE_BENCH_SCALE=large cargo bench --bench trainmarks -- q3_join_3_entities
```

Without the fixtures the suite prints how to get them and measures nothing, so
`cargo bench` still works on a fresh clone.

The queries are trainmarks' own `q1`–`q5`, shared verbatim with the other
engines in that report. `q6_delete_insert` is omitted: it is a SPARQL Update,
and HDT packages are immutable.
