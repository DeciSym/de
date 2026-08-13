// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! MCP prompts exposed by [`super::McpService`].
//!
//! Prompts are protocol-native, user-invoked templates: unlike tool
//! descriptions they cost nothing until a client asks for one, which makes
//! them the right home for the multi-step SPARQL recipes that would otherwise
//! bloat every request as tool-description examples.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{PromptMessage, Role};
use rmcp::{prompt, prompt_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::server::McpService;

/// Arguments for the `describe_resource` prompt.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DescribeResourceRequest {
    /// The IRI to describe, without angle brackets — for example
    /// `http://example.org/book/book1`.
    pub iri: String,
}

#[prompt_router(vis = "pub(crate)")]
impl McpService {
    /// Survey an unfamiliar dataset: what files it holds, which classes and
    /// predicates appear, and roughly how large it is.
    #[prompt(
        name = "explore_dataset",
        title = "Explore the dataset",
        description = "Starting point for an unfamiliar dataset: enumerate its files, then probe the classes and predicates actually present before writing real queries."
    )]
    pub async fn explore_dataset(&self) -> Vec<PromptMessage> {
        vec![PromptMessage::new_text(
            Role::User,
            "Survey the RDF dataset exposed by this server, working in this order:\n\
             \n\
             1. Call `list_data_files` to see which files make up the dataset.\n\
             2. Run this query to find which classes are populated:\n\
             \n\
             ```sparql\n\
             SELECT ?class (COUNT(?s) AS ?count) WHERE {\n  \
             ?s a ?class .\n\
             } GROUP BY ?class ORDER BY DESC(?count) LIMIT 25\n\
             ```\n\
             \n\
             3. Run this query to find which predicates are used:\n\
             \n\
             ```sparql\n\
             SELECT ?p (COUNT(*) AS ?count) WHERE {\n  \
             ?s ?p ?o .\n\
             } GROUP BY ?p ORDER BY DESC(?count) LIMIT 25\n\
             ```\n\
             \n\
             Then summarize what the dataset appears to describe, naming the \
             classes and predicates you would use to query it. Do not guess at \
             vocabulary that did not appear in the results."
                .to_string(),
        )]
    }

    /// Retrieve everything the dataset asserts about one IRI, in both
    /// directions.
    #[prompt(
        name = "describe_resource",
        title = "Describe a resource",
        description = "Retrieve every statement about one IRI — both the properties it carries and the statements that point at it."
    )]
    pub async fn describe_resource(
        &self,
        params: Parameters<DescribeResourceRequest>,
    ) -> Vec<PromptMessage> {
        let iri = params.0.iri;
        vec![PromptMessage::new_text(
            Role::User,
            format!(
                "Describe the resource <{iri}> using the SPARQL tools on this server.\n\
                 \n\
                 Outgoing statements:\n\
                 \n\
                 ```sparql\n\
                 SELECT ?p ?o WHERE {{ <{iri}> ?p ?o . }}\n\
                 ```\n\
                 \n\
                 Incoming statements:\n\
                 \n\
                 ```sparql\n\
                 SELECT ?s ?p WHERE {{ ?s ?p <{iri}> . }}\n\
                 ```\n\
                 \n\
                 Summarize what the resource is and how it connects to the rest \
                 of the graph. If both queries return no rows, say the IRI is \
                 absent from the dataset rather than inferring what it might be."
            ),
        )]
    }
}
