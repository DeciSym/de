[![Latest Version](https://img.shields.io/crates/v/de.svg)](https://crates.io/crates/de)
[![Lint](https://github.com/DeciSym/de/actions/workflows/format_check.yml/badge.svg)](https://github.com/DeciSym/de/actions/workflows/format_check.yml)
[![Build](https://github.com/DeciSym/de/actions/workflows/test_build.yml/badge.svg)](https://github.com/DeciSym/de/actions/workflows/test_build.yml)
[![Documentation](https://docs.rs/de/badge.svg)](https://docs.rs/de/)

# DeciSym Engine

**DeciSym Engine (`de`)** is a command-line tool for creating, querying, and inspecting RDF data in the [HDT (Header, Dictionary, Triples)](http://www.rdfhdt.org/) format. It enables efficient semantic data workflows using SPARQL and supports a variety of RDF and result serialization formats.

## Features

- Convert RDF data into compact, indexed `.hdt` files
- Query RDF and HDT files using SPARQL
- View metadata and statistics for HDT files
- Supports multiple output formats including CSV, JSON, Turtle, and more
- Simple CLI interface with verbosity control

## Installation

Download the latest release version and install the .deb
```bash
cargo install de
```

Or clone and build from source (requires Rust and Cargo):

```bash
git clone https://github.com/DeciSym/de.git
cd de
cargo build --release
```

## Usage Overview

```bash
de [OPTIONS] <COMMAND>
```

Available commands:

- `create` – Convert RDF data into an HDT file
- `query` – Execute SPARQL queries on HDT/RDF data
- `view` – View metadata and statistics for an HDT file
- `check` – Check for required CLI dependencies
- `help` – Show command-specific help

Use `de <COMMAND> --help` for detailed usage.

## Commands

### `create`

Convert RDF data into a `.hdt` file.

```bash
de create --output-name data.hdt --data example.ttl
```

#### Options:

- `-o, --output-name <OUTPUT_NAME>`: Name of the output HDT file (should end in `.hdt`) **[required]**
- `-d, --data <DATA>`: One or more RDF source files (e.g., `.ttl`, `.nt`) to include in the HDT
- `-v, --verbose`: Increase verbosity
- `-q, --quiet`: Suppress output
- `-h, --help`: Show help

#### Example execution 

```bash
de create --output-name apple.hdt --data apple.ttl
```
ex apple.ttl:
```
@prefix ex: <http://example.org/>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.

ex:Apple rdf:type ex:Fruit;
  rdfs:label "Apple";
  ex:variety "Red Delicious";
  ex:color "Red";
  ex:weight "150 grams";
  ex:origin "United States";
  ex:isOrganic true.

ex:Fruit rdf:type rdfs:Class;
  rdfs:label "Fruit".
```
---

### `query`

Execute a SPARQL query over RDF and/or HDT files.

```bash
de query --data data.hdt --sparql query.rq --output json
```

#### Options:

- `-d, --data <DATA>`: One or more RDF or HDT files to query
- `-s, --sparql <SPARQL>`: Path to SPARQL query file (`.rq`) **[required]**
- `-o, --output <OUTPUT>`: Output format for results (default: `csv`)

  Supported formats:
  - `csv`, `tsv`: [SPARQL CSV/TSV](https://www.w3.org/TR/sparql11-results-csv-tsv/)
  - `json`: [SPARQL Results JSON](https://www.w3.org/TR/sparql11-results-json/)
  - `xml`: [SPARQL Results XML](https://www.w3.org/TR/rdf-sparql-XMLres/)
  - `n3`: [Notation3](https://w3c.github.io/N3/spec/)
  - `nquads`: [N-Quads](https://www.w3.org/TR/n-quads/)
  - `rdfxml`: [RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/)
  - `ntriple`: [N-Triples](https://www.w3.org/TR/n-triples/)
  - `trig`: [TriG](https://www.w3.org/TR/trig/)
  - `turtle`: [Turtle](https://www.w3.org/TR/turtle/)

- `-v, --verbose`: Increase verbosity
- `-q, --quiet`: Suppress output
- `-h, --help`: Show help

#### Example execution:

```bash
de create --output-name apple.hdt --data apple.ttl
```

```bash
de query --data apple.hdt --sparql query-color.rq
```

output:
```
fruit,color
http://example.org/Apple,Red
```

ex apple.ttl:
```
@prefix ex: <http://example.org/>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.

ex:Apple rdf:type ex:Fruit;
  rdfs:label "Apple";
  ex:variety "Red Delicious";
  ex:hasColor "Red";
  ex:weight "150 grams";
  ex:origin "United States";
  ex:isOrganic true.

ex:Fruit rdf:type rdfs:Class;
  rdfs:label "Fruit".
```

ex query-color.rq:
```
PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?fruit ?color
WHERE {
  ?fruit ex:hasColor ?color 
}

ORDER BY DESC(?fruit)
```
---

### `view`

Print metadata and statistics about an HDT file.

```bash
de view --data data.hdt
```

#### Options:

- `-d, --data <DATA>`: One or more HDT files
- `-v, --verbose`: Increase verbosity
- `-q, --quiet`: Suppress output
- `-h, --help`: Show help

---

## Logging

Control verbosity with:

- `-v`, `--verbose`: Use multiple times (`-vv`, `-vvv`) for more detail
- `-q`, `--quiet`: Suppress non-critical output

---


