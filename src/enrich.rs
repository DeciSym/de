// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use async_trait::async_trait;
use oxrdf::{IriParseError, NamedNode, Triple};
use std::sync::{Mutex, PoisonError};

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
    /// The enricher saw the file and declined to handle it (e.g. the content
    /// was already the target RDF format). The caller should fall through to
    /// generic conversion.
    Declined,
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
    /// Where to report how the output was produced, for a caller that
    /// records provenance. `None` when the caller doesn't collect reports.
    pub report: Option<&'a EnrichReport>,
}

/// A request an enricher made to a language model, reported through
/// [`EnrichReport::llm_call`]. Fields the enricher can't determine stay
/// `None`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LlmCall {
    /// The model the enricher asked for.
    pub model: String,
    /// The model the server says it used, which can differ from `model`
    /// (for example an alias resolved to a snapshot).
    pub served_model: Option<String>,
    /// The server's fingerprint of the backend configuration that answered.
    pub system_fingerprint: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    /// BLAKE3 hex digest of the prompt's instructions (the system prompt).
    pub prompt_digest: Option<String>,
    /// BLAKE3 hex digest of the raw response text.
    pub response_digest: Option<String>,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
}

/// Collects what enrichers report about how they produced their output,
/// handed to them through [`EnrichCtx::report`].
#[derive(Debug, Default)]
pub struct EnrichReport {
    llm_calls: Mutex<Vec<LlmCall>>,
}

impl EnrichReport {
    /// Record a request made to a language model.
    pub fn llm_call(&self, call: LlmCall) {
        self.llm_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }

    /// The language-model requests recorded so far, in order, leaving the
    /// report empty.
    pub fn take_llm_calls(&self) -> Vec<LlmCall> {
        std::mem::take(
            &mut *self
                .llm_calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }
}

#[async_trait]
pub trait Enricher: Send + Sync {
    fn supported_extensions(&self) -> Vec<&str>;
    /// Name that callers record for files this enricher handles. Defaults to
    /// the implementing type's path, e.g. `de_enrichers::enrichers::docx::DocxEnricher`.
    /// Wrappers should forward the name of the enricher they wrap.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
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

    struct NamedByDefault;

    #[async_trait]
    impl Enricher for NamedByDefault {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["txt"]
        }

        async fn enrich(&self, _ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            Ok(EnrichOutcome::Declined)
        }
    }

    #[test]
    fn name_defaults_to_the_implementing_type() {
        let enricher: Box<dyn Enricher> = Box::new(NamedByDefault);
        assert_eq!(enricher.name(), std::any::type_name::<NamedByDefault>());
        assert!(enricher.name().ends_with("::NamedByDefault"));
    }

    #[test]
    fn report_hands_back_calls_in_order() {
        let report = EnrichReport::default();
        for model in ["a", "b"] {
            report.llm_call(LlmCall {
                model: model.to_string(),
                ..LlmCall::default()
            });
        }
        let models: Vec<_> = report
            .take_llm_calls()
            .into_iter()
            .map(|call| call.model)
            .collect();
        assert_eq!(models, ["a", "b"]);
        assert!(report.take_llm_calls().is_empty());
    }
}
