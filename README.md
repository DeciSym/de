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

Install with the MCP server enabled as well (`mcp` implies `server`):

```sh
cargo install --features mcp de
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
cargo run --features mcp -- serve --location ./data --bind localhost:7878 --mcp
```

The Streamable HTTP transport is mounted at `/mcp` (so, above,
`http://localhost:7878/mcp`) and offers two tools:

- `query_sparql` — run a SPARQL query over the RDF/HDT files in `--location`,
  or over a comma-separated subset of them, and return SPARQL 1.1 JSON
  results. File names are resolved inside the data directory; paths that
  escape it are rejected.
- `upload_rdf` — write Turtle content into the data directory's `uploads/`
  subdirectory, where subsequent queries pick it up.

The same service is available to downstream crates as a library:

```rust
use de::mcp::McpService;

async fn serve_mcp(data_dir: String) -> anyhow::Result<()> {
    McpService::new(data_dir).serve("localhost:7878").await
}
```

Use `McpService::with_server_info` to report the embedding application's own
name and version to clients during MCP initialization.

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
