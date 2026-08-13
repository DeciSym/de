// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! MCP service definition and Streamable HTTP transport wiring.

#![allow(clippy::doc_markdown)]

use anyhow::Context as _;
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::router::prompt::PromptRouter,
    handler::server::router::tool::ToolRouter,
    handler::server::tool::schema_for_output,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerInfo},
    prompt_handler, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use std::sync::Arc;

use super::tools::{
    ListDataFilesRequest, ListDataFilesResponse, QuerySparqlRequest, QuerySparqlResponse,
    UploadRdfRequest, UploadRdfResponse,
};

/// Path the Streamable HTTP transport is mounted at.
pub const MCP_ENDPOINT_PATH: &str = "/mcp";

/// Default cap on an inbound JSON-RPC body. Raised well above rmcp's 4 MiB
/// default because `upload_rdf` carries whole Turtle documents inline.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Host authorities the transport accepts without configuration. rmcp rejects
/// everything else to blunt DNS-rebinding attacks on locally bound servers.
const LOOPBACK_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// Bind hosts that name every interface rather than a reachable authority, so
/// no `Host` header value can be inferred from them.
const WILDCARD_HOSTS: [&str; 3] = ["0.0.0.0", "::", ""];

const DEFAULT_DESCRIPTION: &str =
    "DeciSym Engine MCP Server - Provides SPARQL query and RDF upload capabilities";

/// Orientation handed to clients at initialization. This is the one place the
/// server can explain the dataset as a whole rather than one tool at a time,
/// so it carries the facts that shape every query: where data lives, what is
/// writable, and how uploads become queryable.
const DEFAULT_INSTRUCTIONS: &str = "\
This server exposes one directory of RDF as a SPARQL endpoint.

Start with `list_data_files` — it reports the exact file names `query_sparql` \
accepts, and the dataset's vocabulary is not knowable in advance. Prefer \
running one query over the whole dataset to running many per-file queries; \
narrow to specific files only when you know a file holds what you need.

The dataset is read-only through SPARQL. There is no update, insert, or delete \
tool, and HDT files cannot be modified in place. The only way to add data is \
`upload_rdf`, which writes a new Turtle file under `uploads/`; it never edits \
or replaces an existing file. An upload becomes visible to the next \
`query_sparql` call with no reload step.

Query results are returned as structured content: SELECT and ASK as SPARQL 1.1 \
Results JSON under `results`, CONSTRUCT and DESCRIBE as N-Triples text under \
`graph`. Check the `format` field rather than assuming a shape.";

/// Identity this server reports to clients during MCP initialization.
///
/// [`Default`] describes the `de` crate itself; downstream crates embedding
/// [`McpService`] should override it via [`McpService::with_server_info`] so
/// clients see the host application's name and version rather than `de`'s.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerInfo {
    /// Machine-readable server name reported in `initialize`.
    pub name: String,
    /// Server version reported in `initialize`.
    pub version: String,
    /// Human-readable display name.
    pub title: Option<String>,
    /// Short summary of what the server offers.
    pub description: Option<String>,
    /// Homepage for the server.
    pub website_url: Option<String>,
    /// Dataset-level orientation handed to the client's model at
    /// initialization. This is the server's only whole-dataset channel —
    /// per-tool guidance belongs in the tool descriptions instead.
    pub instructions: Option<String>,
}

impl Default for McpServerInfo {
    fn default() -> Self {
        Self {
            name: "decisym-engine".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            title: Some("DeciSym Engine MCP Server".to_string()),
            description: Some(DEFAULT_DESCRIPTION.to_string()),
            website_url: Some("https://decisym.ai".to_string()),
            instructions: Some(DEFAULT_INSTRUCTIONS.to_string()),
        }
    }
}

/// DeciSym MCP Service.
///
/// Provides SPARQL query and RDF upload capabilities over the MCP protocol,
/// scoped to a single data directory.
#[derive(Clone)]
pub struct McpService {
    data_dir: Arc<String>,
    info: Arc<McpServerInfo>,
    allowed_hosts: Option<Arc<Vec<String>>>,
    max_request_body_bytes: usize,
    #[allow(dead_code)] // Used by the #[tool_router] macro
    tool_router: ToolRouter<Self>,
    #[allow(dead_code)] // Used by the #[prompt_router] macro
    prompt_router: PromptRouter<Self>,
}

impl McpService {
    /// Create a new MCP service serving RDF/HDT data out of `data_dir`.
    #[must_use]
    pub fn new(data_dir: String) -> Self {
        Self {
            data_dir: Arc::new(data_dir),
            info: Arc::new(McpServerInfo::default()),
            allowed_hosts: None,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            tool_router: Self::tool_router(),
            prompt_router: Self::prompt_router(),
        }
    }

    /// Override the identity and instructions reported to clients during MCP
    /// initialization.
    #[must_use]
    pub fn with_server_info(mut self, info: McpServerInfo) -> Self {
        self.info = Arc::new(info);
        self
    }

    /// Replace the `Host` header allow-list used for DNS-rebinding defense.
    ///
    /// Entries are authorities, with or without a port (`"graphs.example.com"`,
    /// `"graphs.example.com:7878"`). Setting this is required to serve clients
    /// that reach the server under a name [`McpService::serve`] cannot infer
    /// from the bind address — notably any wildcard bind such as `0.0.0.0`.
    #[must_use]
    pub fn with_allowed_hosts(mut self, allowed_hosts: Vec<String>) -> Self {
        self.allowed_hosts = Some(Arc::new(allowed_hosts));
        self
    }

    /// Override the maximum inbound request body size, in bytes.
    ///
    /// Bodies above the limit are rejected with `413 Payload Too Large`. The
    /// default is [`DEFAULT_MAX_REQUEST_BODY_BYTES`]; raise it if `upload_rdf`
    /// must accept larger Turtle documents.
    #[must_use]
    pub fn with_max_request_body_bytes(mut self, max_request_body_bytes: usize) -> Self {
        self.max_request_body_bytes = max_request_body_bytes;
        self
    }

    /// Data directory this service queries and uploads into.
    #[must_use]
    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    /// Start the MCP server on every interface at `port`.
    ///
    /// Prefer [`McpService::serve`] when the listen address should be
    /// restricted; this binds `0.0.0.0` and therefore exposes the data
    /// directory to anything that can reach the host. Because a wildcard bind
    /// names no reachable authority, clients connecting under a hostname must
    /// be admitted with [`McpService::with_allowed_hosts`].
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP listener cannot bind to `port` or the HTTP
    /// server fails to start.
    pub async fn start(self, port: u16) -> anyhow::Result<()> {
        self.serve(&format!("0.0.0.0:{port}")).await
    }

    /// Start the MCP server bound to `bind` (a `host:port` pair), serving the
    /// Streamable HTTP transport at [`MCP_ENDPOINT_PATH`].
    ///
    /// Runs until the process receives Ctrl-C, at which point in-flight
    /// sessions are cancelled and the listener is closed.
    ///
    /// # Errors
    ///
    /// Returns an error if `bind` cannot be resolved or bound, or if the HTTP
    /// server fails while running.
    pub async fn serve(self, bind: &str) -> anyhow::Result<()> {
        log::info!("Data directory: {}", self.data_dir);
        log::info!("Available tools: list_data_files, query_sparql, upload_rdf");

        let allowed_hosts = self.resolved_allowed_hosts(bind);
        log::info!("Accepting requests for hosts: {}", allowed_hosts.join(", "));
        let max_request_body_bytes = self.max_request_body_bytes;

        let ct = tokio_util::sync::CancellationToken::new();

        let service = StreamableHttpService::new(
            move || Ok(self.clone()),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(ct.child_token())
                .with_allowed_hosts(allowed_hosts)
                .with_max_request_body_bytes(max_request_body_bytes),
        );

        let router = axum::Router::new().nest_service(MCP_ENDPOINT_PATH, service);
        let tcp_listener = tokio::net::TcpListener::bind(bind)
            .await
            .with_context(|| format!("could not bind MCP server to {bind}"))?;

        // Report the resolved address: `bind` may name a host, and a `:0` port
        // only becomes a real one after the listener is up.
        match tcp_listener.local_addr() {
            Ok(addr) => eprintln!("Listening for MCP requests at http://{addr}{MCP_ENDPOINT_PATH}"),
            Err(e) => log::warn!("could not resolve local address for {bind}: {e}"),
        }

        axum::serve(tcp_listener, router)
            .with_graceful_shutdown(async move {
                tokio::signal::ctrl_c().await.ok();
                ct.cancel();
                log::info!("MCP server shutting down");
            })
            .await
            .context("MCP server failed")
    }

    /// Host authorities the transport will accept for `bind`.
    ///
    /// An explicit [`McpService::with_allowed_hosts`] list wins outright.
    /// Otherwise loopback is allowed, plus `bind`'s own authority when it
    /// names one — a wildcard bind names no authority, so a client reaching
    /// the server by hostname needs the explicit list and is warned about here
    /// rather than being met with an opaque rejection at request time.
    fn resolved_allowed_hosts(&self, bind: &str) -> Vec<String> {
        if let Some(configured) = &self.allowed_hosts {
            return configured.as_ref().clone();
        }

        let mut hosts: Vec<String> = LOOPBACK_HOSTS
            .iter()
            .map(|&host| host.to_string())
            .collect();
        match bind_host(bind) {
            Some(host) if !WILDCARD_HOSTS.contains(&host) => {
                if !hosts.iter().any(|known| known == host) {
                    hosts.push(host.to_string());
                }
                hosts.push(bind.to_string());
            }
            _ => log::warn!(
                "{bind} binds every interface, so only loopback Host headers are accepted; \
                 pass the externally reachable hostname(s) to `with_allowed_hosts` to serve \
                 remote clients"
            ),
        }
        hosts
    }
}

/// Host portion of a `host:port` bind address, unwrapping IPv6 brackets.
fn bind_host(bind: &str) -> Option<&str> {
    if let Some(rest) = bind.strip_prefix('[') {
        return rest.split_once(']').map(|(host, _)| host);
    }
    Some(bind.rsplit_once(':').map_or(bind, |(host, _)| host))
}

/// Tool router implementation using rmcp macros.
///
/// Descriptions are the primary integration surface — they are what a client's
/// model reads when deciding whether to call a tool — so each states what the
/// tool does, when to reach for it, when not to, and what comes back. Worked
/// examples deliberately live in the prompts instead: a description is paid for
/// on every request, a prompt only when asked for.
#[tool_router]
impl McpService {
    /// List the RDF files that make up this dataset, as paths relative to the
    /// data directory. Call this before any query that targets specific files:
    /// the file names are chosen by the operator and cannot be guessed, and
    /// the values it returns are exactly what `query_sparql` accepts in
    /// `files`. Also the cheapest way to confirm an `upload_rdf` landed, or to
    /// see whether the dataset is empty. Returns the resolved data directory
    /// and the sorted file list; it does not read or parse the files, so it
    /// says nothing about their vocabulary or size — use `query_sparql` for
    /// that.
    #[tool(
        title = "List data files",
        output_schema = schema_for_output::<ListDataFilesResponse>(),
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
    )]
    async fn list_data_files(
        &self,
        params: Parameters<ListDataFilesRequest>,
    ) -> Result<CallToolResult, McpError> {
        structured(super::tools::list_data_files(params.0, self.data_dir.as_ref().clone()).await)
    }

    /// Run a SPARQL 1.1 query over the RDF and HDT files in this dataset. Use
    /// it for every read of the data: SELECT and ASK come back as SPARQL 1.1
    /// Results JSON in `results`, CONSTRUCT and DESCRIBE as N-Triples text in
    /// `graph`, discriminated by the `format` field. Query the whole dataset
    /// by omitting `files`; pass `files` only to narrow to names reported by
    /// `list_data_files`, which are resolved inside the data directory —
    /// absolute paths and `..` are rejected. This tool is read-only: SPARQL
    /// UPDATE, INSERT, and DELETE are not supported and HDT files cannot be
    /// modified, so use `upload_rdf` to add data. Do not guess at classes or
    /// predicates; the dataset's vocabulary is whatever the operator loaded,
    /// so probe it first (the `explore_dataset` prompt does this).
    #[tool(
        title = "Run SPARQL query",
        output_schema = schema_for_output::<QuerySparqlResponse>(),
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
    )]
    async fn query_sparql(
        &self,
        params: Parameters<QuerySparqlRequest>,
    ) -> Result<CallToolResult, McpError> {
        structured(super::tools::query_sparql(params.0, self.data_dir.as_ref().clone()).await)
    }

    /// Add RDF to the dataset by writing Turtle into the data directory's
    /// `uploads/` subdirectory. This is the only way to introduce new data —
    /// the query tool cannot write, and existing files are never edited or
    /// replaced, so an upload is always additive and cannot destroy prior
    /// data. Content must be Turtle; other serializations are not parsed.
    /// `graph_uri` is optional and only makes the generated file name
    /// recognizable — it does not record a named graph, so do not rely on it
    /// to partition data. Returns the new file's path relative to the data
    /// directory; the file is queryable by the next `query_sparql` call with
    /// no reload, either as part of the whole dataset or by passing that path
    /// in `files`.
    #[tool(
        title = "Upload RDF",
        output_schema = schema_for_output::<UploadRdfResponse>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn upload_rdf(
        &self,
        params: Parameters<UploadRdfRequest>,
    ) -> Result<CallToolResult, McpError> {
        structured(super::tools::upload_rdf(params.0, self.data_dir.as_ref().clone()).await)
    }
}

/// Convert a tool outcome into an MCP result.
///
/// Always `Ok`: a failed tool is reported as a tool-level error rather than a
/// JSON-RPC error, because the request routed fine and the explanation is for
/// the caller to read — MCP clients render protocol errors opaquely. The
/// `Result` is kept so this drops straight into the tool method signature.
#[allow(clippy::unnecessary_wraps)]
fn structured<T: serde::Serialize>(outcome: Result<T, String>) -> Result<CallToolResult, McpError> {
    match outcome {
        Ok(value) => match serde_json::to_value(value) {
            Ok(json) => Ok(CallToolResult::structured(json)),
            Err(e) => Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "Failed to serialize tool result: {e}"
            ))])),
        },
        Err(e) => Ok(CallToolResult::error(vec![ContentBlock::text(e)])),
    }
}

/// ServerHandler implementation
#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for McpService {
    fn get_info(&self) -> ServerInfo {
        let mut implementation =
            Implementation::new(self.info.name.clone(), self.info.version.clone());
        if let Some(title) = &self.info.title {
            implementation = implementation.with_title(title.clone());
        }
        if let Some(description) = &self.info.description {
            implementation = implementation.with_description(description.clone());
        }
        if let Some(website_url) = &self.info.website_url {
            implementation = implementation.with_website_url(website_url.clone());
        }

        let info = ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .build(),
        )
        .with_server_info(implementation);

        match &self.info.instructions {
            Some(instructions) => info.with_instructions(instructions.clone()),
            None => info,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{McpService, bind_host};

    #[test]
    fn bind_host_extracts_the_authority() {
        assert_eq!(bind_host("localhost:7878"), Some("localhost"));
        assert_eq!(bind_host("0.0.0.0:1337"), Some("0.0.0.0"));
        assert_eq!(bind_host("[::1]:7878"), Some("::1"));
        assert_eq!(bind_host("[::]:7878"), Some("::"));
        assert_eq!(bind_host("example.com"), Some("example.com"));
    }

    #[test]
    fn named_binds_admit_their_own_authority() {
        let hosts = McpService::new("/data".to_string()).resolved_allowed_hosts("graphs.test:7878");
        assert!(hosts.contains(&"graphs.test".to_string()));
        assert!(hosts.contains(&"graphs.test:7878".to_string()));
        assert!(hosts.contains(&"localhost".to_string()));
    }

    #[test]
    fn wildcard_binds_fall_back_to_loopback_only() {
        let hosts = McpService::new("/data".to_string()).resolved_allowed_hosts("0.0.0.0:1337");
        assert_eq!(hosts, vec!["localhost", "127.0.0.1", "::1"]);
    }

    #[test]
    fn explicit_allowed_hosts_win() {
        let hosts = McpService::new("/data".to_string())
            .with_allowed_hosts(vec!["graphs.example.com".to_string()])
            .resolved_allowed_hosts("0.0.0.0:1337");
        assert_eq!(hosts, vec!["graphs.example.com"]);
    }
}
