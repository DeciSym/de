// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use async_trait::async_trait;
use oxrdf::{IriParseError, NamedNode, Triple};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EnrichError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    InvalidIri(#[from] IriParseError),

    #[error("unsupported content in {path}: {reason}")]
    UnsupportedContent { path: String, reason: String },

    #[error("parse error in {path}: {message}")]
    Parse { path: String, message: String },

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub type EnrichResult<T> = Result<T, EnrichError>;

impl EnrichError {
    /// Attach a path to an `io::Error`.
    pub fn io(path: impl Into<String>, source: std::io::Error) -> Self {
        EnrichError::Io {
            path: path.into(),
            source,
        }
    }

    /// Record an unsupported-content condition.
    pub fn unsupported(path: impl Into<String>, reason: impl Into<String>) -> Self {
        EnrichError::UnsupportedContent {
            path: path.into(),
            reason: reason.into(),
        }
    }

    /// Record a parse error.
    pub fn parse(path: impl Into<String>, message: impl Into<String>) -> Self {
        EnrichError::Parse {
            path: path.into(),
            message: message.into(),
        }
    }

    /// Wrap a boxed error without `Send + Sync` bounds into the `Other` variant.
    /// Used when calling helpers that return `Box<dyn std::error::Error>`.
    #[must_use]
    pub fn from_boxed(e: &dyn std::error::Error) -> Self {
        EnrichError::Other(e.to_string().into())
    }
}

/// Result of invoking an [`Enricher`] on a single file.
///
/// The two variants disambiguate "enricher handled the file and produced these
/// triples (possibly zero)" from "enricher saw the file and chose not to
/// handle it". The caller uses the latter to fall through to a generic
/// converter.
#[derive(Debug)]
pub enum EnrichOutcome {
    /// The enricher handled the file. The inner `Vec` may be empty if the
    /// file legitimately had nothing to extract (e.g. an OOXML archive with
    /// no `docProps/core.xml`).
    Triples(Vec<Triple>),
    /// The enricher handled the file and also describes how it produced
    /// `content` (e.g. the language-model requests it made). `provenance` is
    /// written with `content` but kept apart from it, so callers can tell
    /// what was extracted from the account of how.
    TriplesWithProvenance {
        content: Vec<Triple>,
        provenance: Vec<Triple>,
    },
    /// The enricher saw the file and declined to handle it (e.g. the content
    /// was already the target RDF format). The caller should fall through to
    /// generic conversion.
    Declined,
}

impl EnrichOutcome {
    /// The extracted content, or `None` if the enricher declined the file.
    #[must_use]
    pub fn content(&self) -> Option<&[Triple]> {
        match self {
            Self::Triples(content) | Self::TriplesWithProvenance { content, .. } => Some(content),
            Self::Declined => None,
        }
    }

    /// Mutable access to the extracted content, or `None` if the enricher
    /// declined the file.
    pub fn content_mut(&mut self) -> Option<&mut Vec<Triple>> {
        match self {
            Self::Triples(content) | Self::TriplesWithProvenance { content, .. } => Some(content),
            Self::Declined => None,
        }
    }

    /// `(content, provenance)`, or `None` if the enricher declined the file.
    #[must_use]
    pub fn into_parts(self) -> Option<(Vec<Triple>, Vec<Triple>)> {
        match self {
            Self::Triples(content) => Some((content, Vec::new())),
            Self::TriplesWithProvenance {
                content,
                provenance,
            } => Some((content, provenance)),
            Self::Declined => None,
        }
    }
}

/// Per-file context handed to [`Enricher::enrich`].
///
/// Grouping the parameters in a struct keeps the trait's signature stable as
/// new optional inputs (limits, configuration, cancellation) are added.
pub struct EnrichCtx<'a> {
    /// Path to the source file on disk.
    pub file_path: &'a str,
    /// Stable identifier for this file (typically a content-hash IRI),
    /// precomputed by the caller so enrichers don't re-hash the file.
    pub file_id: &'a NamedNode,
    /// Optional root/parent node that generated triples may be linked back
    /// to — e.g. as the object of `dcterms:hasPart` on the file's own
    /// subject, or as the subject in SBOM-style enrichers that emit
    /// component information about the artifact. `None` means the enricher
    /// should produce file-local triples only.
    pub root_id: Option<&'a NamedNode>,
}

impl EnrichCtx<'_> {
    /// A node for this run, `<root>/<path>`, or `None` without a root. What an
    /// enricher records about one run belongs on such a node rather than on
    /// [`Self::file_id`], which every package holding the file shares.
    ///
    /// # Errors
    ///
    /// Returns an error if `<root>/<path>` isn't a valid IRI.
    pub fn run_node(&self, path: &str) -> Result<Option<NamedNode>, IriParseError> {
        self.root_id.map(|root| run_iri(root, path)).transpose()
    }

    /// [`Self::run_node`] for this file: `<root>/<kind>/<key>`, where the key
    /// is the last non-empty segment of [`Self::file_id`] (the content hash,
    /// for content-addressed ids).
    ///
    /// # Errors
    ///
    /// Returns an error if the node's IRI isn't valid.
    pub fn file_run_node(&self, kind: &str) -> Result<Option<NamedNode>, IriParseError> {
        let id = self.file_id.as_str().trim_end_matches(['/', '#']);
        let key = id.rsplit_once(['/', '#']).map_or(id, |(_, key)| key);
        self.run_node(&format!("{kind}/{key}"))
    }
}

/// `root` extended by `path`, without doubling a trailing `/` or `#`.
///
/// # Errors
///
/// Returns an error if the result isn't a valid IRI.
pub fn run_iri(root: &NamedNode, path: &str) -> Result<NamedNode, IriParseError> {
    let root = root.as_str();
    let sep = if root.ends_with(['/', '#']) { "" } else { "/" };
    NamedNode::new(format!("{root}{sep}{path}"))
}

#[async_trait]
pub trait Enricher: Send + Sync {
    fn supported_extensions(&self) -> Vec<&str>;
    /// Short, stable identifier that callers record for files this enricher
    /// handles, e.g. `docx`. It ends up in package provenance, so keep it fixed
    /// across releases and unique among the enrichers a build uses. Wrappers
    /// return the name of the enricher they wrap.
    fn name(&self) -> &str;
    /// Extract triples from `ctx.file_path`.
    ///
    /// Return [`EnrichOutcome::Triples`] (possibly empty) when the file was
    /// handled. Return [`EnrichOutcome::Declined`] to let the caller fall
    /// through to the generic converter — typical when the file is already in
    /// the target RDF format.
    async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triple(s: &str) -> Triple {
        Triple::new(
            NamedNode::new_unchecked(format!("http://example.org/{s}")),
            NamedNode::new_unchecked("http://example.org/p"),
            NamedNode::new_unchecked("http://example.org/o"),
        )
    }

    #[test]
    fn outcome_keeps_content_apart_from_provenance() {
        let plain = EnrichOutcome::Triples(vec![triple("a")]);
        assert_eq!(plain.content(), Some(&[triple("a")][..]));
        assert_eq!(plain.into_parts(), Some((vec![triple("a")], Vec::new())));

        let mut described = EnrichOutcome::TriplesWithProvenance {
            content: vec![triple("a")],
            provenance: vec![triple("how")],
        };
        described.content_mut().unwrap().push(triple("b"));
        assert_eq!(described.content(), Some(&[triple("a"), triple("b")][..]));
        assert_eq!(
            described.into_parts(),
            Some((vec![triple("a"), triple("b")], vec![triple("how")]))
        );

        assert_eq!(EnrichOutcome::Declined.content(), None);
        assert_eq!(EnrichOutcome::Declined.into_parts(), None);
    }

    #[test]
    fn run_nodes_hang_off_the_root() -> Result<(), IriParseError> {
        let file = NamedNode::new_unchecked("https://decisym.ai/data/dcdb80c5");
        let root = NamedNode::new_unchecked("https://decisym.ai/data/2d47b137");
        let ctx = EnrichCtx {
            file_path: "report.docx",
            file_id: &file,
            root_id: Some(&root),
        };
        assert_eq!(
            ctx.run_node("llm-agent/5f2c")?.unwrap().as_str(),
            "https://decisym.ai/data/2d47b137/llm-agent/5f2c"
        );
        assert_eq!(
            ctx.file_run_node("llm")?.unwrap().as_str(),
            "https://decisym.ai/data/2d47b137/llm/dcdb80c5"
        );
        // A trailing separator on the file id doesn't leave the key empty.
        let slashed = NamedNode::new_unchecked("https://decisym.ai/data/dcdb80c5/");
        let slashed_ctx = EnrichCtx {
            file_id: &slashed,
            ..ctx
        };
        assert_eq!(
            slashed_ctx.file_run_node("llm")?.unwrap().as_str(),
            "https://decisym.ai/data/2d47b137/llm/dcdb80c5"
        );
        // A path that can't be part of an IRI is an error, not a missing root.
        assert!(ctx.run_node("not an iri").is_err());
        let rootless = EnrichCtx {
            root_id: None,
            ..ctx
        };
        assert_eq!(rootless.file_run_node("llm")?, None);
        Ok(())
    }

    #[test]
    fn run_iri_keeps_a_single_separator() {
        for (root, expected) in [
            ("https://example.org/pkg", "https://example.org/pkg/create"),
            ("https://example.org/pkg/", "https://example.org/pkg/create"),
            ("https://example.org/pkg#", "https://example.org/pkg#create"),
        ] {
            let root = NamedNode::new_unchecked(root);
            assert_eq!(run_iri(&root, "create").unwrap().as_str(), expected);
        }
    }
}
