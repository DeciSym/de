// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Enrichers turn the files of a package into triples, in three stages:
//!
//! 1. [`FileEnricher`]s read what a file's format says, and are chosen by the
//!    file's extension: a document's metadata, a workbook's tables.
//! 2. [`ContentEnricher`]s read what a document means, such as the test plan
//!    or test report it is. They run on a file after every file enricher, and
//!    share a [`FileCache`] with them, so a parse or a classification of the
//!    file is made once.
//! 3. [`Reconciler`]s run once, over everything the other stages and the
//!    converter wrote, and link nodes that name the same thing, such as a
//!    report's test event and its plan's.
//!
//! What every enricher has, a name and the enrichers it supersedes, is the
//! [`Enricher`] trait the three share. The package's graph is the merge of
//! what every enricher that ran returned. [`EnricherSet`] checks a set of
//! enrichers, each a [`Stage`], and runs them.

use async_trait::async_trait;
use oxrdf::{Graph, IriParseError, NamedNode, Triple};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

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

/// What an enricher returns: a [`FileEnricher`] or [`ContentEnricher`] for a
/// single file, or a [`Reconciler`] for a package.
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

/// Per-file context handed to [`FileEnricher::enrich`] and
/// [`ContentEnricher::enrich`].
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
    /// Values shared by the enrichers that run on this file, such as a parse
    /// of it: see [`FileCache`]. [`EnricherSet`] gives every run on a file the
    /// same cache; it's `None` when an enricher is run on its own. Read
    /// through it with [`Self::cached`] either way.
    pub cache: Option<&'a FileCache>,
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
            cache: None,
        }
    }

    /// This context with [`Self::activity`] set to `activity`.
    #[must_use]
    pub fn with_activity(mut self, activity: Option<&'a NamedNode>) -> Self {
        self.activity = activity;
        self
    }

    /// This context with [`Self::cache`] set to `cache`.
    #[must_use]
    pub fn with_cache(mut self, cache: &'a FileCache) -> Self {
        self.cache = Some(cache);
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

    /// The value of type `T` for this file: the one in [`Self::cache`], or
    /// else what `make` computes, which the cache keeps for the enrichers
    /// that run after this one.
    pub fn cached<T: Any + Send + Sync>(&self, make: impl FnOnce() -> T) -> Arc<T> {
        match self.cache {
            Some(cache) => cache.get_or_insert_with(make),
            None => Arc::new(make()),
        }
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

/// `<root>/reconcile/<reconciler>`: the node [`EnricherSet`] mints for a
/// reconciler's run over a package, and hands it as
/// [`ReconcileCtx::activity`].
///
/// # Errors
///
/// Returns an error if the node's IRI isn't valid.
pub fn reconciliation_iri(root: &NamedNode, reconciler: &str) -> Result<NamedNode, IriParseError> {
    run_iri(root, &format!("reconcile/{reconciler}"))
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

/// Values shared by the enrichers that run on one file, such as a parse of
/// it or the kind of document it is, so that work is done once for the
/// file. A value is keyed by its type, so give each kind of value a type of
/// its own. A value that can fail to compute can be kept as a `Result`, so
/// the failure is shared too. [`EnricherSet`] gives every run on a file the
/// same cache; see [`EnrichCtx::cached`].
#[derive(Default)]
pub struct FileCache {
    values: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl FileCache {
    /// The value of type `T`, if there is one.
    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let value = self
            .values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&TypeId::of::<T>())
            .cloned()?;
        value.downcast::<T>().ok()
    }

    /// Keep `value`, in place of any value of its type, and return it.
    pub fn insert<T: Any + Send + Sync>(&self, value: T) -> Arc<T> {
        let value = Arc::new(value);
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(TypeId::of::<T>(), value.clone());
        value
    }

    /// The value of type `T`, which `make` computes the first time.
    pub fn get_or_insert_with<T: Any + Send + Sync>(&self, make: impl FnOnce() -> T) -> Arc<T> {
        match self.get::<T>() {
            Some(value) => value,
            None => self.insert(make()),
        }
    }
}

/// What every enricher has, whatever its stage: a name, and the enrichers it
/// replaces. An enricher also implements the trait of its stage,
/// [`FileEnricher`], [`ContentEnricher`] or [`Reconciler`], and is handed to
/// [`EnricherSet`] as a [`Stage`].
pub trait Enricher: Send + Sync {
    /// Short, stable identifier that callers record for files this enricher
    /// handles, e.g. `datacube`. It ends up in package provenance, so keep it
    /// fixed across releases. It must be unique among the enrichers a run
    /// uses, whatever their stages, and, since it is part of
    /// [`EnrichCtx::activity`], use only ASCII letters, digits, `-`, `_` and
    /// `.`. Wrappers return the name of the enricher they wrap.
    fn name(&self) -> &str;
    /// Names of the enrichers of its own stage that this one replaces. When
    /// it returns [`EnrichOutcome::Enriched`], those are skipped, and
    /// [`EnricherSet`] runs it before them. For example, a `CycloneDX`
    /// enricher supersedes a generic JSON one. Names that aren't in the set,
    /// or are of another stage, are ignored. Wrappers return what the
    /// enricher they wrap returns, as for [`Self::name`].
    fn supersedes(&self) -> &[&str] {
        &[]
    }
}

/// The first stage: what a file's format says, such as a document's
/// metadata or a workbook's tables, read from every file with an extension
/// it claims.
#[async_trait]
pub trait FileEnricher: Enricher {
    /// Extensions of the files this enricher reads, without the dot. They
    /// are matched case-insensitively, and [`Self::enrich`] is only called
    /// for files that have one of them.
    fn supported_extensions(&self) -> Vec<&str>;
    /// Extract triples from `ctx.file_path`.
    ///
    /// Return [`EnrichOutcome::Enriched`] when the file was handled, with any
    /// provenance describing how the content was produced. Return
    /// [`EnrichOutcome::Declined`] when the content isn't the kind this
    /// enricher reads, e.g. because it is already in an RDF format.
    async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome>;
}

/// The second stage: what a document means, such as the test plan or test
/// report it is. A content enricher runs on a file after every file
/// enricher, and shares their [`EnrichCtx::cache`], so a parse or a
/// classification of the file is made once.
#[async_trait]
pub trait ContentEnricher: Enricher {
    /// Extensions of the files this enricher can read, as for
    /// [`FileEnricher::supported_extensions`].
    fn supported_extensions(&self) -> Vec<&str>;
    /// Whether this enricher reads the document in `ctx`: a cheap check,
    /// such as of a classification in [`EnrichCtx::cache`]. When it's false,
    /// the run is reported as declined, and [`Self::enrich`] isn't called.
    /// By default, every file with one of the extensions.
    fn handles(&self, _ctx: &EnrichCtx<'_>) -> bool {
        true
    }
    /// Extract what the document in `ctx.file_path` means. Return
    /// [`EnrichOutcome::Declined`] when it turns out not to be the kind of
    /// document this enricher reads.
    async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome>;
}

/// The third stage: links between nodes that name the same thing, such as a
/// report's test event and its plan's, made once over everything the other
/// stages and the converter wrote.
#[async_trait]
pub trait Reconciler: Enricher {
    /// The triples linking what `ctx.graph` names more than once, with any
    /// provenance. Return [`EnrichOutcome::Declined`] when there's nothing in
    /// the graph for this reconciler.
    async fn reconcile(&self, ctx: &ReconcileCtx<'_>) -> EnrichResult<EnrichOutcome>;
}

/// What [`Reconciler::reconcile`] reads. Like [`EnrichCtx`], it is
/// `#[non_exhaustive]`: build one with [`Self::new`].
#[non_exhaustive]
pub struct ReconcileCtx<'a> {
    /// Everything written for the package, by every enricher and the
    /// converter.
    pub graph: &'a Graph,
    /// The package's root node, as in [`EnrichCtx::root_id`].
    pub root_id: Option<&'a NamedNode>,
    /// The node for this reconciler's run, `<root>/reconcile/<name>` (see
    /// [`reconciliation_iri`]), minted by [`EnricherSet`]; `None` without a
    /// root.
    pub activity: Option<&'a NamedNode>,
}

impl<'a> ReconcileCtx<'a> {
    /// A context for `graph` with no [`Self::activity`].
    #[must_use]
    pub fn new(graph: &'a Graph, root_id: Option<&'a NamedNode>) -> Self {
        Self {
            graph,
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

/// The stage an enricher runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageKind {
    /// A [`FileEnricher`].
    File,
    /// A [`ContentEnricher`].
    Content,
    /// A [`Reconciler`].
    Reconcile,
}

/// An enricher of any stage: what callers hand [`EnricherSet`].
pub enum Stage {
    File(Box<dyn FileEnricher>),
    Content(Box<dyn ContentEnricher>),
    Reconcile(Box<dyn Reconciler>),
}

impl Stage {
    /// The enricher, as what every stage has.
    #[must_use]
    pub fn enricher(&self) -> &dyn Enricher {
        match self {
            Stage::File(enricher) => enricher.as_ref(),
            Stage::Content(enricher) => enricher.as_ref(),
            Stage::Reconcile(reconciler) => reconciler.as_ref(),
        }
    }

    /// The stage the enricher runs in.
    #[must_use]
    pub fn kind(&self) -> StageKind {
        match self {
            Stage::File(_) => StageKind::File,
            Stage::Content(_) => StageKind::Content,
            Stage::Reconcile(_) => StageKind::Reconcile,
        }
    }
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

/// What one enricher did with one file, or a reconciler with a package; see
/// [`EnricherSet::enrich_file`] and [`EnricherSet::reconcile`].
#[derive(Debug)]
pub struct EnricherRun {
    /// The enricher's [`Enricher::name`].
    pub enricher: String,
    /// The enricher's stage.
    pub stage: StageKind,
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

/// Enrichers checked and ordered to run together.
///
/// Every file and content enricher that claims a file's extension runs on
/// it, the file enrichers first, and the file's triples are the union of
/// what they return. Within a stage, an enricher runs after the enrichers
/// that supersede it (see [`Enricher::supersedes`]), and is skipped for a
/// file one of them enriched. Reconcilers run once, over everything written:
/// see [`Self::reconcile`]. A failing enricher never stops the others.
pub struct EnricherSet<'e> {
    /// Every enricher, in run order: the file enrichers, the content
    /// enrichers, then the reconcilers.
    order: Vec<&'e dyn Enricher>,
    /// Each stage's enrichers, in run order.
    files: Vec<&'e dyn FileEnricher>,
    content: Vec<&'e dyn ContentEnricher>,
    reconcilers: Vec<&'e dyn Reconciler>,
    /// For each enricher's name, the names of the enrichers of its stage
    /// that supersede it.
    superseded_by: HashMap<&'e str, Vec<&'e str>>,
}

impl<'e> EnricherSet<'e> {
    /// Check `stages` can run together, and work out each stage's order: each
    /// enricher after every enricher of its stage that supersedes it, and
    /// otherwise in the order given.
    ///
    /// # Errors
    ///
    /// Returns an error if two enrichers share a name, whatever their stages,
    /// a name can't be part of an IRI path, or enrichers of a stage supersede
    /// each other in a cycle.
    pub fn new(stages: &'e [Stage]) -> Result<Self, EnricherSetError> {
        let mut names = HashSet::new();
        for stage in stages {
            let name = stage.enricher().name();
            if !is_valid_name(name) {
                return Err(EnricherSetError::InvalidName(name.to_string()));
            }
            if !names.insert(name) {
                return Err(EnricherSetError::DuplicateName(name.to_string()));
            }
        }

        let (mut files, mut content, mut reconcilers) = (vec![], vec![], vec![]);
        for stage in stages {
            let enricher = stage.enricher();
            match stage {
                Stage::File(file) => files.push((file.as_ref(), enricher)),
                Stage::Content(reader) => content.push((reader.as_ref(), enricher)),
                Stage::Reconcile(reconciler) => reconcilers.push((reconciler.as_ref(), enricher)),
            }
        }
        let mut superseded_by = HashMap::new();
        let files = in_run_order(&files, &mut superseded_by)?;
        let content = in_run_order(&content, &mut superseded_by)?;
        let reconcilers = in_run_order(&reconcilers, &mut superseded_by)?;
        let order = (files.iter().map(|&(_, enricher)| enricher))
            .chain(content.iter().map(|&(_, enricher)| enricher))
            .chain(reconcilers.iter().map(|&(_, enricher)| enricher))
            .collect();
        Ok(Self {
            order,
            files: files.into_iter().map(|(file, _)| file).collect(),
            content: content.into_iter().map(|(reader, _)| reader).collect(),
            reconcilers: reconcilers
                .into_iter()
                .map(|(reconciler, _)| reconciler)
                .collect(),
            superseded_by,
        })
    }

    /// Whether the set has no enrichers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.content.is_empty() && self.reconcilers.is_empty()
    }

    /// Whether the set has reconcilers, to run over a whole package.
    #[must_use]
    pub fn has_reconcilers(&self) -> bool {
        !self.reconcilers.is_empty()
    }

    /// The enrichers in the order they run: the file enrichers, the content
    /// enrichers, then the reconcilers.
    pub fn enrichers(&self) -> impl Iterator<Item = &'e dyn Enricher> + '_ {
        self.order.iter().copied()
    }

    /// Whether any file or content enricher claims `file_path`'s extension.
    #[must_use]
    pub fn accepts(&self, file_path: &str) -> bool {
        extension(file_path).is_some_and(|ext| self.per_file().any(|e| e.claims(ext)))
    }

    /// Run every file enricher, then every content enricher, that claims
    /// `file_path`'s extension, in order, and report what each one did.
    ///
    /// Each enricher gets its own [`EnrichCtx::activity`] when `root_id` is
    /// set, and all of them share one [`FileCache`]. An enricher is reported
    /// as [`RunResult::Superseded`], without being run, when one of its stage
    /// that supersedes it has already enriched the file, and a content
    /// enricher as [`RunResult::Declined`] when it doesn't
    /// [handle](ContentEnricher::handles) the file. A failing enricher
    /// doesn't stop the others; its error is reported as
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
        let claiming: Vec<PerFile<'e>> = self.per_file().filter(|e| e.claims(ext)).collect();
        let cache = FileCache::default();
        let mut enriched: Vec<&str> = Vec::new();
        let mut runs = Vec::with_capacity(claiming.len());
        for enricher in claiming {
            let name = enricher.name();
            let result = if let Some(by) = self.superseding(name, &enriched) {
                RunResult::Superseded { by: by.to_string() }
            } else {
                match root_id
                    .map(|root| enrichment_iri(root, name, file_id))
                    .transpose()
                {
                    Ok(activity) => {
                        let ctx = EnrichCtx::new(file_path, file_id, root_id)
                            .with_activity(activity.as_ref())
                            .with_cache(&cache);
                        if enricher.handles(&ctx) {
                            run_result(enricher.enrich(&ctx).await, name, &mut enriched)
                        } else {
                            RunResult::Declined
                        }
                    }
                    Err(e) => RunResult::Failed(e.into()),
                }
            };
            runs.push(EnricherRun {
                enricher: name.to_string(),
                stage: enricher.kind(),
                result,
            });
        }
        runs
    }

    /// Run every reconciler over `graph`, everything written for a package,
    /// in order, and report what each one did.
    ///
    /// Each reconciler gets its own [`ReconcileCtx::activity`] when `root_id`
    /// is set. As in [`Self::enrich_file`], a reconciler is skipped when one
    /// that supersedes it has already enriched, and a failing reconciler
    /// doesn't stop the others.
    pub async fn reconcile(&self, graph: &Graph, root_id: Option<&NamedNode>) -> Vec<EnricherRun> {
        let mut enriched: Vec<&str> = Vec::new();
        let mut runs = Vec::with_capacity(self.reconcilers.len());
        for &reconciler in &self.reconcilers {
            let name = reconciler.name();
            let result = if let Some(by) = self.superseding(name, &enriched) {
                RunResult::Superseded { by: by.to_string() }
            } else {
                match root_id
                    .map(|root| reconciliation_iri(root, name))
                    .transpose()
                {
                    Ok(activity) => {
                        let ctx =
                            ReconcileCtx::new(graph, root_id).with_activity(activity.as_ref());
                        run_result(reconciler.reconcile(&ctx).await, name, &mut enriched)
                    }
                    Err(e) => RunResult::Failed(e.into()),
                }
            };
            runs.push(EnricherRun {
                enricher: name.to_string(),
                stage: StageKind::Reconcile,
                result,
            });
        }
        runs
    }

    /// The file and content enrichers, in the order they run.
    fn per_file(&self) -> impl Iterator<Item = PerFile<'e>> + '_ {
        let files = self.files.iter().map(|&e| PerFile::File(e));
        files.chain(self.content.iter().map(|&e| PerFile::Content(e)))
    }

    /// The enricher of `name`'s stage that supersedes it and has already
    /// enriched, if any.
    fn superseding(&self, name: &str, enriched: &[&str]) -> Option<&'e str> {
        self.superseded_by
            .get(name)?
            .iter()
            .copied()
            .find(|by| enriched.contains(by))
    }
}

/// A file or content enricher, as [`EnricherSet::enrich_file`] runs them.
#[derive(Clone, Copy)]
enum PerFile<'e> {
    File(&'e dyn FileEnricher),
    Content(&'e dyn ContentEnricher),
}

impl<'e> PerFile<'e> {
    fn name(self) -> &'e str {
        match self {
            Self::File(e) => e.name(),
            Self::Content(e) => e.name(),
        }
    }

    fn kind(self) -> StageKind {
        match self {
            Self::File(_) => StageKind::File,
            Self::Content(_) => StageKind::Content,
        }
    }

    /// Whether the enricher claims files with extension `ext`.
    fn claims(self, ext: &str) -> bool {
        let claimed = match self {
            Self::File(e) => e.supported_extensions(),
            Self::Content(e) => e.supported_extensions(),
        };
        claimed
            .iter()
            .any(|claimed| claimed.eq_ignore_ascii_case(ext))
    }

    fn handles(self, ctx: &EnrichCtx<'_>) -> bool {
        match self {
            Self::File(_) => true,
            Self::Content(e) => e.handles(ctx),
        }
    }

    async fn enrich(self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
        match self {
            Self::File(e) => e.enrich(ctx).await,
            Self::Content(e) => e.enrich(ctx).await,
        }
    }
}

/// `enrichers`, each with its view as an [`Enricher`], in run order: each
/// after every one of them that supersedes it, and otherwise in the order
/// given. Records in `superseded_by`, for each enricher, the names of those
/// superseding it.
fn in_run_order<'e, T: ?Sized>(
    enrichers: &[(&'e T, &'e dyn Enricher)],
    superseded_by: &mut HashMap<&'e str, Vec<&'e str>>,
) -> Result<Vec<(&'e T, &'e dyn Enricher)>, EnricherSetError> {
    let count = enrichers.len();
    let index: HashMap<&str, usize> = enrichers
        .iter()
        .enumerate()
        .map(|(i, &(_, enricher))| (enricher.name(), i))
        .collect();

    // `replaces[i]` holds the positions of the enrichers `i` supersedes;
    // `waiting_on[j]` counts the enrichers superseding `j` that haven't been
    // placed in the run order yet.
    let mut replaces: Vec<Vec<usize>> = vec![Vec::new(); count];
    let mut waiting_on = vec![0usize; count];
    for (i, &(_, enricher)) in enrichers.iter().enumerate() {
        for target in enricher.supersedes() {
            if let Some(&j) = index.get(target)
                && !replaces[i].contains(&j)
            {
                replaces[i].push(j);
                waiting_on[j] += 1;
                superseded_by
                    .entry(enrichers[j].1.name())
                    .or_default()
                    .push(enricher.name());
            }
        }
    }

    let mut placed = vec![false; count];
    let mut order = Vec::with_capacity(count);
    while order.len() < count {
        let Some(next) = (0..count).find(|&i| !placed[i] && waiting_on[i] == 0) else {
            let cycle = (0..count)
                .filter(|&i| !placed[i])
                .map(|i| enrichers[i].1.name().to_string())
                .collect();
            return Err(EnricherSetError::SupersedesCycle(cycle));
        };
        placed[next] = true;
        order.push(enrichers[next]);
        for &j in &replaces[next] {
            waiting_on[j] -= 1;
        }
    }
    Ok(order)
}

/// The [`RunResult`] for what the enricher `name` returned, noting it in
/// `enriched` when it enriched.
fn run_result<'e>(
    returned: EnrichResult<EnrichOutcome>,
    name: &'e str,
    enriched: &mut Vec<&'e str>,
) -> RunResult {
    match returned {
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

/// `path`'s extension, if it has one that is valid UTF-8.
fn extension(path: &str) -> Option<&str> {
    Path::new(path).extension().and_then(|e| e.to_str())
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        assert_eq!(
            reconciliation_iri(&root, "test-events")?.as_str(),
            "https://decisym.ai/data/2d47b137/reconcile/test-events"
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

    impl Enricher for Fake {
        fn name(&self) -> &str {
            self.name
        }

        fn supersedes(&self) -> &[&str] {
            self.supersedes
        }
    }

    #[async_trait]
    impl FileEnricher for Fake {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
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

    fn fake(name: &'static str, supersedes: &'static [&'static str], behavior: Behavior) -> Stage {
        Stage::File(Box::new(Fake {
            name,
            supersedes,
            behavior,
        }))
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

        // Names are unique across stages too.
        let parses = Arc::new(AtomicUsize::new(0));
        let across = vec![
            fake("json", &[], Behavior::Enrich),
            reads("json", "x", &[], &parses),
        ];
        assert!(matches!(
            EnricherSet::new(&across),
            Err(EnricherSetError::DuplicateName(name)) if name == "json"
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

    /// What a [`Parse`] made of a file: its stem, standing in for the kind of
    /// document it is.
    struct Parsed(String);

    /// Parses `path`, counting the parses in `parses`.
    fn parse(path: &str, parses: &AtomicUsize) -> Parsed {
        parses.fetch_add(1, Ordering::SeqCst);
        let stem = Path::new(path).file_stem().and_then(|s| s.to_str());
        Parsed(stem.unwrap_or_default().to_string())
    }

    /// A triple saying `subject` came of `ctx`'s run.
    fn ran(subject: &str, ctx: &EnrichCtx<'_>) -> EnrichResult<Triple> {
        Ok(Triple::new(
            NamedNode::new(format!("https://example.org/{subject}"))?,
            NamedNode::new("https://example.org/activity")?,
            oxrdf::Literal::new_simple_literal(ctx.activity.map_or("none", NamedNode::as_str)),
        ))
    }

    /// A file enricher that parses `.mock` files, into the cache.
    struct Parse(Arc<AtomicUsize>);

    impl Enricher for Parse {
        fn name(&self) -> &'static str {
            "parse"
        }
    }

    #[async_trait]
    impl FileEnricher for Parse {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            let parsed = ctx.cached(|| parse(ctx.file_path, &self.0));
            Ok(EnrichOutcome::enriched(vec![ran(&parsed.0, ctx)?]))
        }
    }

    /// A content enricher of the `.mock` documents of one kind.
    struct Reads {
        name: &'static str,
        kind: &'static str,
        supersedes: &'static [&'static str],
        parses: Arc<AtomicUsize>,
    }

    impl Enricher for Reads {
        fn name(&self) -> &str {
            self.name
        }

        fn supersedes(&self) -> &[&str] {
            self.supersedes
        }
    }

    #[async_trait]
    impl ContentEnricher for Reads {
        fn supported_extensions(&self) -> Vec<&str> {
            vec!["mock"]
        }

        fn handles(&self, ctx: &EnrichCtx<'_>) -> bool {
            ctx.cached(|| parse(ctx.file_path, &self.parses)).0 == self.kind
        }

        async fn enrich(&self, ctx: &EnrichCtx<'_>) -> EnrichResult<EnrichOutcome> {
            Ok(EnrichOutcome::enriched(vec![ran(self.name, ctx)?]))
        }
    }

    fn reads(
        name: &'static str,
        kind: &'static str,
        supersedes: &'static [&'static str],
        parses: &Arc<AtomicUsize>,
    ) -> Stage {
        Stage::Content(Box::new(Reads {
            name,
            kind,
            supersedes,
            parses: parses.clone(),
        }))
    }

    const ENRICHED: RunStatus = RunStatus::Enriched {
        content: 1,
        provenance: 0,
    };

    #[tokio::test]
    async fn content_enrichers_run_after_file_enrichers_and_share_their_parse()
    -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedNode::new("https://decisym.ai/data/dcdb80c5")?;
        let parses = Arc::new(AtomicUsize::new(0));
        // Listed first, the content enrichers still run after the file
        // enricher.
        let stages = vec![
            reads("report", "report", &[], &parses),
            reads("plan", "plan", &[], &parses),
            Stage::File(Box::new(Parse(parses.clone()))),
        ];
        let set = EnricherSet::new(&stages)?;
        assert_eq!(names(&set), ["parse", "report", "plan"]);

        let runs = set.enrich_file("report.mock", &file, None).await;
        assert_eq!(
            statuses(&runs),
            [
                ("parse", ENRICHED),
                ("report", ENRICHED),
                // Not its kind of document, so it isn't run.
                ("plan", RunStatus::Declined),
            ]
        );
        let stages: Vec<StageKind> = runs.iter().map(|run| run.stage).collect();
        assert_eq!(
            stages,
            [StageKind::File, StageKind::Content, StageKind::Content]
        );
        // One parse served all three.
        assert_eq!(parses.load(Ordering::SeqCst), 1);

        // The next file gets a cache of its own.
        let runs = set.enrich_file("plan.mock", &file, None).await;
        assert_eq!(statuses(&runs)[2], ("plan", ENRICHED));
        assert_eq!(parses.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn supersedes_holds_within_a_stage() -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedNode::new("https://decisym.ai/data/dcdb80c5")?;
        let parses = Arc::new(AtomicUsize::new(0));
        let stages = vec![
            fake("metadata", &[], Behavior::Enrich),
            reads("generic", "report", &[], &parses),
            // Supersedes the generic reader, but not the file enricher: that
            // one is of another stage.
            reads("report", "report", &["generic", "metadata"], &parses),
        ];
        let set = EnricherSet::new(&stages)?;
        assert_eq!(names(&set), ["metadata", "report", "generic"]);

        let runs = set.enrich_file("report.mock", &file, None).await;
        assert_eq!(
            statuses(&runs),
            [
                ("metadata", ENRICHED),
                ("report", ENRICHED),
                (
                    "generic",
                    RunStatus::Superseded {
                        by: "report".to_string()
                    }
                ),
            ]
        );
        Ok(())
    }

    /// Says how many triples the graph has, on its activity.
    struct Count;

    impl Enricher for Count {
        fn name(&self) -> &'static str {
            "count"
        }
    }

    #[async_trait]
    impl Reconciler for Count {
        async fn reconcile(&self, ctx: &ReconcileCtx<'_>) -> EnrichResult<EnrichOutcome> {
            if ctx.graph.is_empty() {
                return Ok(EnrichOutcome::Declined);
            }
            let said = format!(
                "{} triples, on {}",
                ctx.graph.len(),
                ctx.activity.map_or("none", NamedNode::as_str)
            );
            Ok(EnrichOutcome::enriched(vec![Triple::new(
                NamedNode::new("https://example.org/count")?,
                NamedNode::new("https://example.org/activity")?,
                oxrdf::Literal::new_simple_literal(said),
            )]))
        }
    }

    #[tokio::test]
    async fn reconcilers_run_once_over_everything_written() -> Result<(), Box<dyn std::error::Error>>
    {
        let file = NamedNode::new("https://decisym.ai/data/dcdb80c5")?;
        let root = NamedNode::new("https://decisym.ai/data/2d47b137")?;
        let stages = vec![
            Stage::Reconcile(Box::new(Count)),
            fake("json", &[], Behavior::Enrich),
        ];
        let set = EnricherSet::new(&stages)?;
        assert!(set.has_reconcilers());
        assert_eq!(names(&set), ["json", "count"]);

        // A reconciler runs on no file.
        let runs = set.enrich_file("report.mock", &file, None).await;
        assert_eq!(statuses(&runs), [("json", ENRICHED)]);

        let mut graph = Graph::new();
        if let RunResult::Enriched { content, .. } = &runs[0].result {
            graph.extend(content.iter().cloned());
        }
        let runs = set.reconcile(&graph, Some(&root)).await;
        assert_eq!(statuses(&runs), [("count", ENRICHED)]);
        assert_eq!(runs[0].stage, StageKind::Reconcile);
        assert_eq!(
            activity(&runs[0]),
            "1 triples, on https://decisym.ai/data/2d47b137/reconcile/count"
        );

        // Nothing to reconcile.
        let runs = set.reconcile(&Graph::new(), None).await;
        assert_eq!(statuses(&runs), [("count", RunStatus::Declined)]);
        Ok(())
    }

    #[test]
    fn a_cache_keeps_one_value_of_each_type() {
        let cache = FileCache::default();
        assert!(cache.get::<Parsed>().is_none());
        let made = cache.get_or_insert_with(|| Parsed("report".to_string()));
        let again = cache.get_or_insert_with(|| Parsed("plan".to_string()));
        assert_eq!(again.0, "report");
        assert!(Arc::ptr_eq(&made, &again));
        cache.insert(Parsed("plan".to_string()));
        assert_eq!(
            cache.get::<Parsed>().map(|p| p.0.clone()).as_deref(),
            Some("plan")
        );
        // A context without a cache computes the value every time.
        let file = NamedNode::new_unchecked("https://decisym.ai/data/dcdb80c5");
        let ctx = EnrichCtx::new("report.mock", &file, None);
        assert_eq!(ctx.cached(|| Parsed("report".to_string())).0, "report");
        assert_eq!(ctx.cached(|| Parsed("plan".to_string())).0, "plan");
    }
}
