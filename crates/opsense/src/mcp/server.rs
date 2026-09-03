//! rmcp server handler — wraps an `OpsenseClient` and exposes it as MCP tools.

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{Implementation, ProtocolVersion, ServerCapabilities, ServerInfo};
use rmcp::schemars;
use rmcp::transport::stdio;
use rmcp::{tool, tool_handler, tool_router, ServiceExt};
use serde::Deserialize;

use crate::client::OpsenseClient;

use super::tools;

/// Build the MCP server around a GraphQL client. Spawns on stdio.
pub async fn serve(client: OpsenseClient) -> std::io::Result<()> {
    let server = OpsenseMcpServer::new(client);
    let service = server.serve(stdio()).await.map_err(stdio_err)?;
    service.waiting().await.map_err(stdio_err)?;
    Ok(())
}

fn stdio_err<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
}

// ─────────────────────────────────────────────────────────────────────────────
// Param structs (must derive JsonSchema for #[tool] macro)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetAttributeParams {
    #[schemars(description = "Attribute name")]
    pub name: String,
    #[schemars(description = "Attribute value")]
    pub value: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RemoveAttributeParams {
    #[schemars(description = "Attribute name")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryTimeseriesParams {
    #[schemars(description = "Station/node id")]
    pub node: String,
    #[schemars(description = "From ts (unix seconds, inclusive)")]
    pub from_ts: Option<i64>,
    #[schemars(description = "To ts (unix seconds, inclusive)")]
    pub to_ts: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReloadParams {
    /// JSON array of component objects, each: `{"type": "...", "id": "...", "config": {...}, "inputs": [...]}`.
    #[schemars(description = "JSON array of component objects")]
    pub components_json: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct OpsenseMcpServer {
    client: std::sync::Arc<OpsenseClient>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl OpsenseMcpServer {
    pub fn new(client: OpsenseClient) -> Self {
        Self {
            client: std::sync::Arc::new(client),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Snapshot of the current pipeline (nodes + stations).")]
    async fn opsense_status(&self) -> Result<String, String> {
        tools::status(&self.client).await
    }

    #[tool(description = "List all in-memory attributes (template variables).")]
    async fn opsense_attributes(&self) -> Result<String, String> {
        tools::attributes(&self.client).await
    }

    #[tool(description = "Set an attribute. Warns when OPSENSE_ATTR_<NAME> env is also set.")]
    async fn opsense_set_attribute(
        &self,
        Parameters(p): Parameters<SetAttributeParams>,
    ) -> Result<String, String> {
        tools::set_attribute(&self.client, &p.name, &p.value).await
    }

    #[tool(description = "Remove an attribute. Returns true when the key existed.")]
    async fn opsense_remove_attribute(
        &self,
        Parameters(p): Parameters<RemoveAttributeParams>,
    ) -> Result<String, String> {
        tools::remove_attribute(&self.client, &p.name).await
    }

    #[tool(description = "Query observations from a TimeseriesStation in a time window.")]
    async fn opsense_query_timeseries(
        &self,
        Parameters(p): Parameters<QueryTimeseriesParams>,
    ) -> Result<String, String> {
        tools::query_timeseries(&self.client, &p.node, p.from_ts, p.to_ts).await
    }

    #[tool(
        description = "Push a full component list to the server. The server validates each component via typetag; invalid types produce a GraphQL error."
    )]
    async fn opsense_reload(
        &self,
        Parameters(p): Parameters<ReloadParams>,
    ) -> Result<String, String> {
        tools::reload_from_json(&self.client, &p.components_json).await
    }
}

#[tool_handler]
impl ServerHandler for OpsenseMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::default(),
            instructions: Some(
                "opsense MCP — thin client to `opsense serve`. All tools are 1 GraphQL round-trip. \
                 To edit pipeline, use opsense_reload with the full new component list."
                    .to_string(),
            ),
        }
    }
}
