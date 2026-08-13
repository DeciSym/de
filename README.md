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

## MCP Server

With the `mcp` feature enabled, `serve` can expose the data directory to
[Model Context Protocol](https://modelcontextprotocol.io) clients instead of
serving the SPARQL HTTP API:

```sh
cargo run --features server -- serve --location ./data --bind localhost:7878 --mcp
```

The Streamable HTTP transport is mounted at `/mcp` (so, above,
`http://localhost:7878/mcp`).

### Tools

Every tool returns MCP structured content validated against a published
output schema, and carries the behavior hints (`readOnlyHint`,
`destructiveHint`, `idempotentHint`) clients use to decide what may run
without asking.

- `list_data_files` — report the data directory and the RDF files in it, as
  the relative paths `query_sparql` accepts. Read-only.
- `query_sparql` — run a SPARQL 1.1 query over the whole dataset, or over the
  files named in `files`. SELECT and ASK come back as SPARQL 1.1 Results JSON
  in `results`, CONSTRUCT and DESCRIBE as N-Triples in `graph`, discriminated
  by `format`. File names resolve inside the data directory; absolute paths
  and `..` are rejected. Read-only — SPARQL UPDATE is not supported.
- `upload_rdf` — write Turtle into the data directory's `uploads/`
  subdirectory, where the next query picks it up. Additive: existing files are
  never edited or replaced.

### Prompts

- `explore_dataset` — survey an unfamiliar dataset: enumerate its files, then
  probe the classes and predicates actually present.
- `describe_resource` — retrieve every statement about one IRI, in both
  directions.

### As a library

```rust
use de::mcp::McpService;

async fn serve_mcp(data_dir: String) -> anyhow::Result<()> {
    McpService::new(data_dir).serve("localhost:7878").await
}
```

- `with_server_info` reports the embedding application's own name, version,
  and dataset instructions to clients during MCP initialization.
- `with_allowed_hosts` sets the `Host` header allow-list. The transport
  rejects unlisted hosts to blunt DNS rebinding, and `serve` can only infer
  the authority from a named bind address — a wildcard bind such as
  `0.0.0.0:1337` accepts loopback only until you name the externally
  reachable hostnames here.
- `with_max_request_body_bytes` caps inbound request bodies (32 MiB by
  default, sized for inline Turtle uploads).

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
