// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).
//
//! Helpers for `de`-specific HDT header metadata.
//!
//! This module handles the `https://decisym.ai/de#graphIRI` metadata triple stored
//! in an HDT header. The read path inspects header metadata from an existing HDT
//! file. The write path updates an in-memory [`hdt::Hdt`] header only; persistence
//! occurs only when the caller serializes that value.

use hdt::containers::rdf::{Id, Term, Triple};
use hdt::header::Header;
use oxrdf::NamedNode;
use std::path::Path;

const GRAPH_META_PREDICATE: &str = "https://decisym.ai/de#graphIRI";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const HDT_DATASET: &str = "http://purl.org/HDT/hdt#Dataset";
const VOID_DATASET: &str = "http://rdfs.org/ns/void#Dataset";
const FALLBACK_META_SUBJECT_BNODE: &str = "deGraphMeta";

fn is_graph_meta_triple(triple: &Triple) -> bool {
    triple.predicate == GRAPH_META_PREDICATE
}

fn dataset_subject(header: &Header) -> Option<Id> {
    for triple in &header.body {
        if triple.predicate != RDF_TYPE {
            continue;
        }
        match (&triple.subject, &triple.object) {
            (subject, Term::Id(Id::Named(object)))
                if object == HDT_DATASET || object == VOID_DATASET =>
            {
                return Some(subject.clone());
            }
            _ => {}
        }
    }
    None
}

fn read_header_only(path: &Path) -> anyhow::Result<Header> {
    Header::read_from_hdt_path(path)
        .map_err(|e| anyhow::anyhow!("error reading HDT header {:?}: {e}", path))
}

/// Reads the `de` graph IRI metadata from an HDT file header.
///
/// Returns `Ok(Some(graph_iri))` when a metadata triple with predicate
/// `https://decisym.ai/de#graphIRI` is present and has an IRI object.
/// Returns `Ok(None)` when no such triple exists.
///
/// # Errors
///
/// Returns an error if the HDT file cannot be opened or its header cannot be
/// parsed.
pub fn read_graph_iri_metadata(path: &Path) -> anyhow::Result<Option<String>> {
    let header = read_header_only(path)?;
    for triple in header.body {
        if is_graph_meta_triple(&triple)
            && let Term::Id(Id::Named(iri)) = triple.object
        {
            return Ok(Some(iri));
        }
    }
    Ok(None)
}

/// Sets `de` graph IRI metadata on an in-memory HDT value.
///
/// Existing `https://decisym.ai/de#graphIRI` triples are removed and replaced
/// with a single triple using `graph_iri` as object.
///
/// Subject selection:
/// - Uses an existing header subject typed as `hdt:Dataset` or `void:Dataset`
///   (named node or blank node).
/// - Falls back to a deterministic blank node subject (`deGraphMeta`) if no
///   dataset subject exists.
///
/// This function mutates only the provided in-memory [`hdt::Hdt`] value.
/// It does not write to disk; the caller must serialize the HDT to persist
/// changes.
///
/// # Errors
///
/// Returns an error when:
/// - `graph_iri` is empty.
/// - `graph_iri` is not a valid IRI.
/// - recomputing serialized header length fails.
pub fn set_graph_iri_metadata_in_hdt(hdt: &mut hdt::Hdt, graph_iri: &str) -> anyhow::Result<()> {
    if graph_iri.is_empty() {
        return Err(anyhow::anyhow!("graph IRI metadata cannot be empty"));
    }
    let graph_iri = NamedNode::new(graph_iri)
        .map_err(|e| anyhow::anyhow!("invalid graph IRI metadata {graph_iri:?}: {e}"))?
        .into_string();

    let header = hdt.header_mut();
    header.body.retain(|t| !is_graph_meta_triple(t));
    let subject = dataset_subject(header)
        .unwrap_or_else(|| Id::Blank(FALLBACK_META_SUBJECT_BNODE.to_string()));
    header.body.insert(Triple::new(
        subject,
        GRAPH_META_PREDICATE.to_string(),
        Term::Id(Id::Named(graph_iri)),
    ));
    hdt.recompute_header_length()
        .map_err(|e| anyhow::anyhow!("error updating HDT header length: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hdt::containers::rdf::Literal;
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use tempfile::tempdir;

    fn build_test_hdt() -> anyhow::Result<hdt::Hdt> {
        let tmp = tempdir()?;
        let nt_path = tmp.path().join("input.nt");
        std::fs::write(
            &nt_path,
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n",
        )?;
        hdt::Hdt::read_nt(&nt_path).map_err(|e| anyhow::anyhow!("failed to build test HDT: {e}"))
    }

    #[test]
    fn set_graph_iri_metadata_uses_fallback_blank_subject_without_dataset_subject()
    -> anyhow::Result<()> {
        let mut hdt = build_test_hdt()?;
        hdt.header_mut().body.clear();

        set_graph_iri_metadata_in_hdt(&mut hdt, "http://example.org/graph")?;

        let mut found_count = 0usize;
        for triple in &hdt.header().body {
            if triple.predicate != GRAPH_META_PREDICATE {
                continue;
            }
            found_count += 1;
            match (&triple.subject, &triple.object) {
                (Id::Blank(subject), Term::Id(Id::Named(object))) => {
                    assert_eq!(subject, FALLBACK_META_SUBJECT_BNODE);
                    assert_eq!(object, "http://example.org/graph");
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "metadata triple had unexpected subject/object form"
                    ));
                }
            }
        }

        assert_eq!(found_count, 1, "expected exactly one metadata triple");
        Ok(())
    }

    #[test]
    fn read_graph_iri_metadata_ignores_non_iri_metadata_object() -> anyhow::Result<()> {
        let mut hdt = build_test_hdt()?;
        hdt.header_mut().body.clear();
        hdt.header_mut().body.insert(Triple::new(
            Id::Named("http://example.org/dataset".to_string()),
            GRAPH_META_PREDICATE.to_string(),
            Term::Literal(Literal::new("not-an-iri".to_string())),
        ));
        hdt.recompute_header_length()
            .map_err(|e| anyhow::anyhow!("failed to recompute header length: {e}"))?;

        let tmp = tempdir()?;
        let hdt_path = tmp.path().join("meta.hdt");
        let file = File::create(&hdt_path)?;
        let mut writer = BufWriter::new(file);
        hdt.write(&mut writer)?;
        writer.flush()?;

        let found = read_graph_iri_metadata(&hdt_path)?;
        assert!(
            found.is_none(),
            "expected malformed metadata object to be ignored"
        );
        Ok(())
    }

    #[test]
    fn set_graph_iri_metadata_replaces_existing_metadata_without_duplicates() -> anyhow::Result<()>
    {
        let mut hdt = build_test_hdt()?;
        hdt.header_mut().body.clear();
        hdt.header_mut().body.insert(Triple::new(
            Id::Named("http://example.org/dataset".to_string()),
            GRAPH_META_PREDICATE.to_string(),
            Term::Id(Id::Named("http://example.org/old-graph".to_string())),
        ));
        hdt.recompute_header_length()
            .map_err(|e| anyhow::anyhow!("failed to recompute header length: {e}"))?;

        set_graph_iri_metadata_in_hdt(&mut hdt, "http://example.org/new-graph")?;

        let graph_meta: Vec<&Triple> = hdt
            .header()
            .body
            .iter()
            .filter(|triple| triple.predicate == GRAPH_META_PREDICATE)
            .collect();
        assert_eq!(
            graph_meta.len(),
            1,
            "expected exactly one metadata triple after replace"
        );
        match &graph_meta[0].object {
            Term::Id(Id::Named(object)) => assert_eq!(object, "http://example.org/new-graph"),
            _ => return Err(anyhow::anyhow!("metadata triple object was not an IRI id")),
        }
        Ok(())
    }

    #[test]
    fn set_graph_iri_metadata_prefers_named_dataset_subject_over_fallback() -> anyhow::Result<()> {
        let mut hdt = build_test_hdt()?;
        hdt.header_mut().body.clear();
        hdt.header_mut().body.insert(Triple::new(
            Id::Named("http://example.org/dataset".to_string()),
            RDF_TYPE.to_string(),
            Term::Id(Id::Named(HDT_DATASET.to_string())),
        ));
        hdt.recompute_header_length()
            .map_err(|e| anyhow::anyhow!("failed to recompute header length: {e}"))?;

        set_graph_iri_metadata_in_hdt(&mut hdt, "http://example.org/graph")?;

        let meta = hdt
            .header()
            .body
            .iter()
            .find(|triple| triple.predicate == GRAPH_META_PREDICATE)
            .ok_or_else(|| anyhow::anyhow!("missing graph metadata triple"))?;
        match (&meta.subject, &meta.object) {
            (Id::Named(subject), Term::Id(Id::Named(object))) => {
                assert_eq!(subject, "http://example.org/dataset");
                assert_eq!(object, "http://example.org/graph");
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "metadata triple did not use named dataset subject"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn set_graph_iri_metadata_uses_blank_dataset_subject_when_present() -> anyhow::Result<()> {
        let mut hdt = build_test_hdt()?;
        hdt.header_mut().body.clear();
        hdt.header_mut().body.insert(Triple::new(
            Id::Blank("datasetNode".to_string()),
            RDF_TYPE.to_string(),
            Term::Id(Id::Named(VOID_DATASET.to_string())),
        ));
        hdt.recompute_header_length()
            .map_err(|e| anyhow::anyhow!("failed to recompute header length: {e}"))?;

        set_graph_iri_metadata_in_hdt(&mut hdt, "http://example.org/graph")?;

        let meta = hdt
            .header()
            .body
            .iter()
            .find(|triple| triple.predicate == GRAPH_META_PREDICATE)
            .ok_or_else(|| anyhow::anyhow!("missing graph metadata triple"))?;
        match (&meta.subject, &meta.object) {
            (Id::Blank(subject), Term::Id(Id::Named(object))) => {
                assert_eq!(subject, "datasetNode");
                assert_eq!(object, "http://example.org/graph");
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "metadata triple did not use existing blank dataset subject"
                ));
            }
        }
        Ok(())
    }
}
