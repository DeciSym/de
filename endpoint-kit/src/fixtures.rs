// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! The data the cases query.
//!
//! The kit writes each graph as an N-Triples file and gives the consumer the
//! file, the graph's IRI and its size. How the data reaches the server (an
//! HDT per graph, an encrypted package, anything else) is up to the consumer,
//! but each graph must keep its IRI: the cases name graphs in
//! `default-graph-uri` and `named-graph-uri`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// One graph of a fixture set.
#[derive(Clone, Debug)]
pub struct FixtureGraph {
    /// A short name, usable as a file stem.
    pub name: String,
    /// The graph's IRI. The server must serve the graph under this IRI.
    pub iri: String,
    /// The graph's triples, as N-Triples.
    pub ntriples: PathBuf,
    /// How many triples the graph has.
    pub triples: usize,
}

/// A set of graphs, in a directory of their own.
#[derive(Clone, Debug)]
pub struct FixtureSet {
    /// A directory the consumer may write into, holding the N-Triples files.
    pub dir: PathBuf,
    pub graphs: Vec<FixtureGraph>,
}

pub(crate) const FOAF_DOCUMENT: &str = "http://xmlns.com/foaf/0.1/Document";
const DC_TITLE: &str = "http://purl.org/dc/terms/title";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// The graphs the W3C SPARQL 1.1 Protocol tests name. The originals are RDF
/// documents on kasei.us that the server is expected to fetch; here they are
/// served from the store under the same IRIs.
pub(crate) const DATA1: &str = "http://kasei.us/2009/09/sparql/data/data1.rdf";
pub(crate) const DATA2: &str = "http://kasei.us/2009/09/sparql/data/data2.rdf";
pub(crate) const DATA3: &str = "http://kasei.us/2009/09/sparql/data/data3.rdf";
/// A graph that no case names in its dataset, so that a server which puts
/// every graph in a protocol-specified dataset gets caught.
pub(crate) const OTHER: &str = "http://example.org/endpoint-kit/graph/other";

/// The large graph of the contract fixtures: one label per item.
pub(crate) const LARGE: &str = "http://example.org/endpoint-kit/graph/large";
pub(crate) const LABEL: &str = "http://example.org/endpoint-kit/label";
pub(crate) const LARGE_TRIPLES: usize = 50_000;

/// Values whose pairs make a long, CPU-bound and memory-light query: 2,000²
/// pairs run for minutes in a debug build of `de` and for seconds in a release
/// one, either way well past the one-second timeout the timeout contract asks
/// for.
pub(crate) const SLOW: &str = "http://example.org/endpoint-kit/graph/slow";
pub(crate) const WEIGHT: &str = "http://example.org/endpoint-kit/weight";
pub(crate) const SLOW_TRIPLES: usize = 2_000;

/// Chains `?a p ?x . ?a q ?b . ?b r ?y` that are cheap to join along their
/// shared variables and slow to join in the order they're written.
pub(crate) const JOIN: &str = "http://example.org/endpoint-kit/graph/join";
pub(crate) const JOIN_P: &str = "http://example.org/endpoint-kit/p";
pub(crate) const JOIN_Q: &str = "http://example.org/endpoint-kit/q";
pub(crate) const JOIN_R: &str = "http://example.org/endpoint-kit/r";
pub(crate) const JOIN_CHAINS: usize = 5_000;

/// A graph IRI that no fixture set has.
pub(crate) const MISSING: &str = "http://example.org/endpoint-kit/graph/missing";

/// The protocol fixtures' graphs: each one's name, and its IRI.
pub(crate) const PROTOCOL_GRAPHS: [(&str, &str); 4] = [
    ("data1", DATA1),
    ("data2", DATA2),
    ("data3", DATA3),
    ("other", OTHER),
];

/// The triples of the protocol fixture graph `name`, whose IRI is `iri`, in
/// N-Triples syntax without the final ` .`.
pub(crate) fn protocol_triples(name: &str, iri: &str) -> [String; 2] {
    // Each document describes itself, as the W3C update tests' INSERT DATA
    // does: `<data1.rdf> a foaf:Document`.
    [
        format!("<{iri}> <{RDF_TYPE}> <{FOAF_DOCUMENT}>"),
        format!("<{iri}> <{DC_TITLE}> \"{name}\""),
    ]
}

pub(crate) fn protocol(dir: &Path) -> anyhow::Result<FixtureSet> {
    let mut graphs = Vec::new();
    for (name, iri) in PROTOCOL_GRAPHS {
        let triples = protocol_triples(name, iri);
        let mut ntriples = String::new();
        for triple in &triples {
            writeln!(ntriples, "{triple} .")?;
        }
        graphs.push(write_graph(dir, name, iri, &ntriples, triples.len())?);
    }
    Ok(FixtureSet {
        dir: dir.to_path_buf(),
        graphs,
    })
}

pub(crate) fn contracts(dir: &Path) -> anyhow::Result<FixtureSet> {
    let mut large = String::new();
    for i in 0..LARGE_TRIPLES {
        writeln!(
            large,
            "<http://example.org/endpoint-kit/item/{i}> <{LABEL}> \"item {i}\" ."
        )?;
    }
    let mut join = String::new();
    for i in 0..JOIN_CHAINS {
        writeln!(
            join,
            "<http://example.org/endpoint-kit/a/{i}> <{JOIN_P}> \"x{i}\" ."
        )?;
        writeln!(
            join,
            "<http://example.org/endpoint-kit/a/{i}> <{JOIN_Q}> <http://example.org/endpoint-kit/b/{i}> ."
        )?;
        writeln!(
            join,
            "<http://example.org/endpoint-kit/b/{i}> <{JOIN_R}> \"y{i}\" ."
        )?;
    }
    Ok(FixtureSet {
        dir: dir.to_path_buf(),
        graphs: vec![
            write_graph(dir, "large", LARGE, &large, LARGE_TRIPLES)?,
            slow_graph(dir)?,
            write_graph(dir, "join", JOIN, &join, JOIN_CHAINS * 3)?,
        ],
    })
}

/// The slow graph alone, for the timeout contract's server.
pub(crate) fn timeout(dir: &Path) -> anyhow::Result<FixtureSet> {
    Ok(FixtureSet {
        dir: dir.to_path_buf(),
        graphs: vec![slow_graph(dir)?],
    })
}

fn slow_graph(dir: &Path) -> anyhow::Result<FixtureGraph> {
    let mut slow = String::new();
    for i in 0..SLOW_TRIPLES {
        writeln!(
            slow,
            "<http://example.org/endpoint-kit/slow/{i}> <{WEIGHT}> \"{i}\" ."
        )?;
    }
    write_graph(dir, "slow", SLOW, &slow, SLOW_TRIPLES)
}

fn write_graph(
    dir: &Path,
    name: &str,
    iri: &str,
    ntriples: &str,
    triples: usize,
) -> anyhow::Result<FixtureGraph> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{name}.nt"));
    std::fs::write(&path, ntriples)?;
    Ok(FixtureGraph {
        name: name.to_string(),
        iri: iri.to_string(),
        ntriples: path,
        triples,
    })
}
