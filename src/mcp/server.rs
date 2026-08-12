// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! MCP service definition and Streamable HTTP transport wiring.

#![allow(clippy::doc_markdown)]

use anyhow::Context as _;
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::router::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use std::sync::Arc;

use super::tools::{QuerySparqlRequest, UploadRdfRequest};

/// Path the Streamable HTTP transport is mounted at.
pub const MCP_ENDPOINT_PATH: &str = "/mcp";

const DEFAULT_DESCRIPTION: &str =
    "DeciSym Engine MCP Server - Provides SPARQL query and RDF upload capabilities";

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
    /// Usage guidance handed to the client's model.
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
            instructions: Some(DEFAULT_DESCRIPTION.to_string()),
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
    #[allow(dead_code)] // Used by the #[tool_router] macro
    tool_router: ToolRouter<Self>,
}

impl McpService {
    /// Create a new MCP service serving RDF/HDT data out of `data_dir`.
    #[must_use]
    pub fn new(data_dir: String) -> Self {
        Self {
            data_dir: Arc::new(data_dir),
            info: Arc::new(McpServerInfo::default()),
            tool_router: Self::tool_router(),
        }
    }

    /// Override the identity reported to clients during MCP initialization.
    #[must_use]
    pub fn with_server_info(mut self, info: McpServerInfo) -> Self {
        self.info = Arc::new(info);
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
    /// directory to anything that can reach the host.
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
        log::info!("Available tools: query_sparql, upload_rdf");

        let ct = tokio_util::sync::CancellationToken::new();

        let service = StreamableHttpService::new(
            move || Ok(self.clone()),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig {
                cancellation_token: ct.child_token(),
                ..Default::default()
            },
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
}

/// Tool router implementation using rmcp macros
#[tool_router]
impl McpService {
    /// Execute a SPARQL query against the RDF dataset
    #[tool(
        description = "Execute a SPARQL query against the RDF dataset. Query all available data files or specify specific files to query."
    )]
    async fn query_sparql(
        &self,
        params: Parameters<QuerySparqlRequest>,
    ) -> Result<CallToolResult, McpError> {
        let data_dir = self.data_dir.as_ref().clone();
        match super::tools::query_sparql(params.0, data_dir).await {
            Ok(result) => Ok(CallToolResult::success(vec![Content::text(result)])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e)])),
        }
    }

    /// Upload RDF data to the knowledge graph
    #[tool(
        description = "Upload RDF data in Turtle format to the knowledge graph. Optionally specify a graph URI."
    )]
    async fn upload_rdf(
        &self,
        params: Parameters<UploadRdfRequest>,
    ) -> Result<CallToolResult, McpError> {
        let data_dir = self.data_dir.as_ref().clone();
        match super::tools::upload_rdf(params.0, data_dir).await {
            Ok(result) => Ok(CallToolResult::success(vec![Content::text(result)])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e)])),
        }
    }
}

/// ServerHandler implementation
#[tool_handler]
impl ServerHandler for McpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            server_info: Implementation {
                name: self.info.name.clone(),
                version: self.info.version.clone(),
                title: self.info.title.clone(),
                description: self.info.description.clone(),
                icons: None,
                website_url: self.info.website_url.clone(),
            },
            instructions: self.info.instructions.clone(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
