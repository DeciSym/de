// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use async_trait::async_trait;
use oxrdf::{IriParseError, NamedNode, Triple};
use std::collections::HashMap;
use std::path::Path;

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
/// handle it". When every enricher that saw a file declines it, the caller
/// falls through to a generic converter.
#[derive(Debug)]
pub enum EnrichOutcome {
    /// The enricher handled the file. `content` is what it extracted, and may
    /// be empty if the file legitimately had nothing to extract (e.g. an OOXML
    /// archive with no `docProps/core.xml`). `provenance` describes how the
    /// content was produced (e.g. the language-model requests it made). It's
    /// written with `content` but kept apart from it, so callers can tell what
    /// was extracted from the account of how.
    Enriched {
        content: Vec<Triple>,
        provenance: Vec<Triple>,
    },
    /// The enricher saw the file and declined to handle it, because the
    /// content isn't the kind it reads (e.g. it is already RDF). If no other
    /// enricher handles the file either, it falls through to generic
    /// conversion.
    Declined,
}

impl EnrichOutcome {
    /// [`Self::Enriched`] with `content` and no provenance.
    #[must_use]
    pub fn enriched(content: Vec<Triple>) -> Self {
        Self::Enriched {
            content,
            provenance: Vec::new(),
        }
    }
}

/// Per-file context handed to [`Enricher::enrich`].
///
/// Grouping the parameters in a struct keeps the trait's signature stable as
/// new optional inputs (limits, configuration, cancellation) are added. It is
/// `#[non_exhaustive]` for the same reason: build one with [`Self::new`].
#[non_exhaustive]
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
    /// The node for this enricher's run over this file,
    /// `<root>/enrich/<name>/<key>` (see [`enrichment_iri`]), minted by
    /// [`EnricherSet`]; `None` without a root. Provenance the enricher returns
    /// about how it produced its content belongs on it, so that enrichers
    /// working on the same file never describe their runs on the same node.
    pub activity: Option<&'a NamedNode>,
}

impl<'a> EnrichCtx<'a> {
    /// A context for `file_path` with no [`Self::activity`].
    #[must_use]
    pub fn new(file_path: &'a str, file_id: &'a NamedNode, root_id: Option<&'a NamedNode>) -> Self {
        Self {
            file_path,
            file_id,
            root_id,
            activity: None,
        }
    }

    /// This context with [`Self::activity`] set to `activity`.
    #[must_use]
    pub fn with_activity(mut self, activity: Option<&'a NamedNode>) -> Self {
        self.activity = activity;
        self
    }
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
        self.run_node(&format!("{kind}/{}", file_key(self.file_id)))
    }
}

/// The last non-empty segment of `file_id`: the content hash, for
/// content-addressed ids.
fn file_key(file_id: &NamedNode) -> &str {
    let id = file_id.as_str().trim_end_matches(['/', '#']);
    id.rsplit_once(['/', '#']).map_or(id, |(_, key)| key)
}

/// `<root>/enrich/<enricher>/<key>`: the node [`EnricherSet`] mints for one
/// enricher's run over one file and hands it as [`EnrichCtx::activity`]. The
/// key is the last non-empty segment of `file_id`, as for
/// [`EnrichCtx::file_run_node`]. Public so that others can name the same
/// node, e.g. to attach statistics to it.
///
/// # Errors
///
/// Returns an error if the node's IRI isn't valid.
pub fn enrichment_iri(
    root: &NamedNode,
    enricher: &str,
    file_id: &NamedNode,
) -> Result<NamedNode, IriParseError> {
    run_iri(root, &format!("enrich/{enricher}/{}", file_key(file_id)))
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

/// Extracts triples from files of the kinds it reads.
///
/// Several enrichers can read the same kind of file, and every one that
/// claims a file runs on it, each adding its own triples: see
/// [`EnricherSet`].
#[async_trait]
pub trait Enricher: Send + Sync {
    /// Extensions of the files this enricher reads, without the dot. They
    /// are matched case-insensitively, and [`Self::enrich`] is only called
    /// for files that have one of them.
    fn supported_extensions(&self) -> Vec<&str>;
    /// Short, stable identifier that callers record for files this enricher
    /// handles, e.g. `datacube`. It ends up in package provenance, so keep it
    /// fixed across releases. It must be unique among the enrichers a run
    /// uses and, since it is part of [`EnrichCtx::activity`], use only ASCII
    /// letters, digits, `-`, `_` and `.`. Wrappers return the name of the
    /// enricher they wrap.
    fn name(&self) -> &str;
    /// Names of enrichers this one replaces on the files it handles. When it
    /// returns [`EnrichOutcome::Enriched`] for a file, those are skipped for
    /// that file, and [`EnricherSet`] runs it before them. For example, a
    /// `CycloneDX` enricher supersedes a generic JSON one. Names that aren't
    /// in the set are ignored. Wrappers return what the enricher they wrap
    /// returns, as for [`Self::name`].
    fn supersedes(&self) -> &[&str] {
        &[]
    }
    /// Extract triples from `ctx.file_path`.
    ///
    /// Return [`EnrichOutcome::Enriched`] when the file was handled, with any
    /// provenance describing how the content was produced. Return
    /// [`EnrichOutcome::Declined`] when the content isn't the kind this
    /// enricher reads, e.g. because it is already in an RDF format.
    async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome>;
}

/// Why enrichers can't run together as an [`EnricherSet`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EnricherSetError {
    #[error("two enrichers are named \"{0}\"; names must be unique within a run")]
    DuplicateName(String),

    #[error(
        "enricher name \"{0}\" must be non-empty and use only ASCII letters, digits, '-', '_' and '.'"
    )]
    InvalidName(String),

    #[error("enrichers supersede each other in a cycle: {}", .0.join(", "))]
    SupersedesCycle(Vec<String>),
}

/// What one enricher did with one file; see [`EnricherSet::enrich_file`].
#[derive(Debug)]
pub struct EnricherRun {
    /// The enricher's [`Enricher::name`].
    pub enricher: String,
    pub result: RunResult,
}

/// The result of one [`EnricherRun`].
#[derive(Debug)]
#[non_exhaustive]
pub enum RunResult {
    /// The enricher handled the file; see [`EnrichOutcome::Enriched`].
    Enriched {
        content: Vec<Triple>,
        provenance: Vec<Triple>,
    },
    /// The enricher declined the file; see [`EnrichOutcome::Declined`].
    Declined,
    /// The enricher wasn't run because `by`, which supersedes it, had
    /// already enriched the file.
    Superseded { by: String },
    /// The enricher returned an error.
    Failed(EnrichError),
}

/// [`RunResult`] without its triples: what callers record about a run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunStatus {
    /// The enricher returned this many content and provenance triples.
    Enriched {
        content: usize,
        provenance: usize,
    },
    Declined,
    Superseded {
        by: String,
    },
    /// The enricher failed with this message.
    Failed(String),
}

impl From<&RunResult> for RunStatus {
    fn from(result: &RunResult) -> Self {
        match result {
            RunResult::Enriched {
                content,
                provenance,
            } => RunStatus::Enriched {
                content: content.len(),
                provenance: provenance.len(),
            },
            RunResult::Declined => RunStatus::Declined,
            RunResult::Superseded { by } => RunStatus::Superseded { by: by.clone() },
            RunResult::Failed(e) => RunStatus::Failed(e.to_string()),
        }
    }
}

/// Enrichers checked and ordered to run together on the same files.
///
/// Every enricher that claims a file's extension runs on it, and the file's
/// triples are the union of what they return. An enricher always runs after
/// the enrichers that supersede it (see [`Enricher::supersedes`]), and is
/// skipped for a file one of them enriched.
pub struct EnricherSet<'e> {
    /// The enrichers in run order.
    order: Vec<&'e dyn Enricher>,
    /// For each enricher's name, the names of the enrichers in the set that
    /// supersede it.
    superseded_by: HashMap<&'e str, Vec<&'e str>>,
}

impl<'e> EnricherSet<'e> {
    /// Check `enrichers` can run together and work out their order: each
    /// enricher after every enricher that supersedes it, and otherwise in the
    /// order given.
    ///
    /// # Errors
    ///
    /// Returns an error if two enrichers share a name, a name can't be part
    /// of an IRI path, or enrichers supersede each other in a cycle.
    pub fn new(enrichers: &'e [Box<dyn Enricher>]) -> Result<Self, EnricherSetError> {
        let mut index: HashMap<&str, usize> = HashMap::new();
        for (i, enricher) in enrichers.iter().enumerate() {
            let name = enricher.name();
            if !is_valid_name(name) {
                return Err(EnricherSetError::InvalidName(name.to_string()));
            }
            if index.insert(name, i).is_some() {
                return Err(EnricherSetError::DuplicateName(name.to_string()));
            }
        }

        // `replaces[i]` holds the positions of the enrichers `i` supersedes;
        // `waiting_on[j]` counts the enrichers superseding `j` that haven't
        // been placed in the run order yet.
        let mut replaces: Vec<Vec<usize>> = vec![Vec::new(); enrichers.len()];
        let mut waiting_on = vec![0usize; enrichers.len()];
        let mut superseded_by: HashMap<&'e str, Vec<&'e str>> = HashMap::new();
        for (i, enricher) in enrichers.iter().enumerate() {
            for target in enricher.supersedes() {
                if let Some(&j) = index.get(target)
                    && !replaces[i].contains(&j)
                {
                    replaces[i].push(j);
                    waiting_on[j] += 1;
                    superseded_by
                        .entry(enrichers[j].name())
                        .or_default()
                        .push(enricher.name());
                }
            }
        }

        let mut placed = vec![false; enrichers.len()];
        let mut order: Vec<&'e dyn Enricher> = Vec::with_capacity(enrichers.len());
        while order.len() < enrichers.len() {
            let Some(next) = (0..enrichers.len()).find(|&i| !placed[i] && waiting_on[i] == 0)
            else {
                let cycle = (0..enrichers.len())
                    .filter(|&i| !placed[i])
                    .map(|i| enrichers[i].name().to_string())
                    .collect();
                return Err(EnricherSetError::SupersedesCycle(cycle));
            };
            placed[next] = true;
            order.push(enrichers[next].as_ref());
            for &j in &replaces[next] {
                waiting_on[j] -= 1;
            }
        }

        Ok(Self {
            order,
            superseded_by,
        })
    }

    /// Whether the set has no enrichers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// The enrichers in the order they run.
    pub fn enrichers(&self) -> impl Iterator<Item = &'e dyn Enricher> + '_ {
        self.order.iter().copied()
    }

    /// Whether any enricher in the set claims `file_path`'s extension.
    #[must_use]
    pub fn accepts(&self, file_path: &str) -> bool {
        extension(file_path).is_some_and(|ext| self.order.iter().any(|e| claims(*e, ext)))
    }

    /// Run every enricher that claims `file_path`'s extension, in order, and
    /// report what each one did.
    ///
    /// Each enricher gets its own [`EnrichCtx::activity`] when `root_id` is
    /// set. An enricher is reported as [`RunResult::Superseded`], without
    /// being run, when one that supersedes it has already enriched the file.
    /// A failing enricher doesn't stop the others; its error is reported as
    /// [`RunResult::Failed`] for the caller to act on.
    pub async fn enrich_file(
        &self,
        file_path: &str,
        file_id: &NamedNode,
        root_id: Option<&NamedNode>,
    ) -> Vec<EnricherRun> {
        let Some(ext) = extension(file_path) else {
            return Vec::new();
        };
        let mut enriched: Vec<&str> = Vec::new();
        let mut runs = Vec::new();
        for enricher in self.order.iter().copied().filter(|e| claims(*e, ext)) {
            let name = enricher.name();
            let superseded = self
                .superseded_by
                .get(name)
                .and_then(|by| by.iter().find(|by| enriched.contains(by)));
            let result = if let Some(by) = superseded {
                RunResult::Superseded {
                    by: (*by).to_string(),
                }
            } else {
                match root_id
                    .map(|root| enrichment_iri(root, name, file_id))
                    .transpose()
                {
                    Ok(activity) => {
                        let ctx = EnrichCtx::new(file_path, file_id, root_id)
                            .with_activity(activity.as_ref());
                        match enricher.enrich(&ctx).await {
                            Ok(EnrichOutcome::Enriched {
                                content,
                                provenance,
                            }) => {
                                enriched.push(name);
                                RunResult::Enriched {
                                    content,
                                    provenance,
                                }
                            }
                            Ok(EnrichOutcome::Declined) => RunResult::Declined,
                            Err(e) => RunResult::Failed(e),
                        }
                    }
                    Err(e) => RunResult::Failed(e.into()),
                }
            };
            runs.push(EnricherRun {
                enricher: name.to_string(),
                result,
            });
        }
        runs
    }
}

/// `path`'s extension, if it has one that is valid UTF-8.
fn extension(path: &str) -> Option<&str> {
    Path::new(path).extension().and_then(|e| e.to_str())
}

/// Whether `enricher` claims files with extension `ext`.
fn claims(enricher: &dyn Enricher, ext: &str) -> bool {
    enricher
        .supported_extensions()
        .iter()
        .any(|claimed| claimed.eq_ignore_ascii_case(ext))
}

/// Whether `name` can be an enricher's name: it becomes a path segment of
/// [`EnrichCtx::activity`], so it can't be `.`, `..` or the like either.
fn is_valid_name(name: &str) -> bool {
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        && name.bytes().any(|b| b != b'.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_nodes_hang_off_the_root() -> Result<(), IriParseError> {
        let file = NamedNode::new_unchecked("https://decisym.ai/data/dcdb80c5");
        let root = NamedNode::new_unchecked("https://decisym.ai/data/2d47b137");
        let ctx = EnrichCtx::new("report.docx", &file, Some(&root));
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

    #[test]
    fn enrichment_iris_name_the_enricher_and_the_file() -> Result<(), IriParseError> {
        let root = NamedNode::new_unchecked("https://decisym.ai/data/2d47b137");
        let file = NamedNode::new_unchecked("https://decisym.ai/data/dcdb80c5");
        assert_eq!(
            enrichment_iri(&root, "datacube", &file)?.as_str(),
            "https://decisym.ai/data/2d47b137/enrich/datacube/dcdb80c5"
        );
        Ok(())
    }

    enum Behavior {
        Enrich,
        Decline,
        Fail,
    }

    /// Claims `.mock` files. When it enriches, it returns one triple whose
    /// object is its `ctx.activity`, or "none".
    struct Fake {
        name: &'static str,
        supersedes: &'static [&'static str],
        behavior: Behavior,
    }

    #[async_trait]
    impl Enricher for Fake {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        fn name(&self) -> &str {
            self.name
        }

        fn supersedes(&self) -> &[&str] {
            self.supersedes
        }

        async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            match self.behavior {
                Behavior::Decline => Ok(EnrichOutcome::Declined),
                Behavior::Fail => Err(EnrichError::parse(ctx.file_path, "synthetic failure")),
                Behavior::Enrich => Ok(EnrichOutcome::enriched(vec![Triple::new(
                    NamedNode::new(format!("https://example.org/{}", self.name))?,
                    NamedNode::new("https://example.org/activity")?,
                    oxrdf::Literal::new_simple_literal(
                        ctx.activity.map_or("none", NamedNode::as_str),
                    ),
                )])),
            }
        }
    }

    fn fake(
        name: &'static str,
        supersedes: &'static [&'static str],
        behavior: Behavior,
    ) -> Box<dyn Enricher> {
        Box::new(Fake {
            name,
            supersedes,
            behavior,
        })
    }

    fn names(set: &EnricherSet<'_>) -> Vec<String> {
        set.enrichers().map(|e| e.name().to_string()).collect()
    }

    fn statuses(runs: &[EnricherRun]) -> Vec<(&str, RunStatus)> {
        runs.iter()
            .map(|run| (run.enricher.as_str(), RunStatus::from(&run.result)))
            .collect()
    }

    /// The activity a [`Fake`] reported, from the triple it returned.
    fn activity(run: &EnricherRun) -> String {
        match &run.result {
            RunResult::Enriched { content, .. } => match &content[0].object {
                oxrdf::Term::Literal(l) => l.value().to_string(),
                other => panic!("unexpected object {other}"),
            },
            other => panic!("{} didn't enrich: {other:?}", run.enricher),
        }
    }

    #[test]
    fn enrichers_run_after_the_ones_that_supersede_them() -> Result<(), EnricherSetError> {
        let enrichers = vec![
            fake("json", &[], Behavior::Enrich),
            fake("xml", &[], Behavior::Enrich),
            fake("cyclonedx", &["json"], Behavior::Enrich),
            fake("spdx", &["json", "not-in-the-set"], Behavior::Enrich),
        ];
        let set = EnricherSet::new(&enrichers)?;
        assert_eq!(names(&set), ["xml", "cyclonedx", "spdx", "json"]);
        Ok(())
    }

    #[test]
    fn sets_reject_duplicate_and_invalid_names_and_cycles() {
        let duplicate = vec![
            fake("json", &[], Behavior::Enrich),
            fake("json", &[], Behavior::Decline),
        ];
        assert!(matches!(
            EnricherSet::new(&duplicate),
            Err(EnricherSetError::DuplicateName(name)) if name == "json"
        ));

        for bad in ["", "two words", "a/b", "..", "naïve"] {
            let enrichers = vec![fake(bad, &[], Behavior::Enrich)];
            assert!(
                matches!(
                    EnricherSet::new(&enrichers),
                    Err(EnricherSetError::InvalidName(_))
                ),
                "{bad:?} should be rejected"
            );
        }

        let cycle = vec![
            fake("a", &["b"], Behavior::Enrich),
            fake("b", &["a"], Behavior::Enrich),
            fake("c", &[], Behavior::Enrich),
        ];
        let Err(EnricherSetError::SupersedesCycle(names)) = EnricherSet::new(&cycle) else {
            panic!("a cycle should be rejected");
        };
        assert_eq!(names, ["a", "b"]);

        let own_tail = vec![fake("a", &["a"], Behavior::Enrich)];
        assert!(matches!(
            EnricherSet::new(&own_tail),
            Err(EnricherSetError::SupersedesCycle(_))
        ));
    }

    #[tokio::test]
    async fn every_claiming_enricher_runs_unless_superseded()
    -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedNode::new("https://decisym.ai/data/dcdb80c5")?;
        let root = NamedNode::new("https://decisym.ai/data/2d47b137")?;
        let enrichers = vec![
            fake("generic", &[], Behavior::Enrich),
            fake("specific", &["generic"], Behavior::Enrich),
            fake("other", &[], Behavior::Enrich),
        ];
        let set = EnricherSet::new(&enrichers)?;

        // Extensions match whatever their case. Once `specific` has run,
        // `generic` takes its listed place again, ahead of `other`.
        let runs = set.enrich_file("report.MOCK", &file, Some(&root)).await;
        assert_eq!(
            statuses(&runs),
            [
                (
                    "specific",
                    RunStatus::Enriched {
                        content: 1,
                        provenance: 0
                    }
                ),
                (
                    "generic",
                    RunStatus::Superseded {
                        by: "specific".to_string()
                    }
                ),
                (
                    "other",
                    RunStatus::Enriched {
                        content: 1,
                        provenance: 0
                    }
                ),
            ]
        );
        // Each run gets its own activity.
        assert_eq!(
            activity(&runs[0]),
            "https://decisym.ai/data/2d47b137/enrich/specific/dcdb80c5"
        );
        assert_eq!(
            activity(&runs[2]),
            "https://decisym.ai/data/2d47b137/enrich/other/dcdb80c5"
        );

        // Files no enricher claims see none of them.
        assert!(set.accepts("report.Mock"));
        assert!(!set.accepts("report.docx"));
        assert!(!set.accepts("README"));
        assert!(set.enrich_file("report.docx", &file, None).await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn declines_and_failures_dont_stop_the_others() -> Result<(), Box<dyn std::error::Error>>
    {
        let file = NamedNode::new("https://decisym.ai/data/dcdb80c5")?;
        let enrichers = vec![
            fake("generic", &[], Behavior::Enrich),
            fake("specific", &["generic"], Behavior::Decline),
            fake("broken", &[], Behavior::Fail),
        ];
        let set = EnricherSet::new(&enrichers)?;
        let runs = set.enrich_file("report.mock", &file, None).await;
        assert_eq!(
            statuses(&runs),
            [
                ("specific", RunStatus::Declined),
                // A superseding enricher that declines stands in for nothing.
                (
                    "generic",
                    RunStatus::Enriched {
                        content: 1,
                        provenance: 0
                    }
                ),
                (
                    "broken",
                    RunStatus::Failed("parse error in report.mock: synthetic failure".to_string())
                ),
            ]
        );
        // Without a root there is no activity.
        assert_eq!(activity(&runs[1]), "none");
        Ok(())
    }
}
