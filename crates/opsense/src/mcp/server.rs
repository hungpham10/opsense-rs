//! rmcp server handler — wraps an `OpsenseClient` and exposes it as MCP tools.

use rmcp::handler::server::ServerHandler;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ProtocolVersion, ServerCapabilities, ServerInfo};
use rmcp::schemars;
use rmcp::transport::stdio;
use rmcp::{ServiceExt, tool, tool_handler, tool_router};
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
    std::io::Error::other(e.to_string())
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
    #[schemars(description = "From ts (unix seconds, inclusive). Omit → bounded default window")]
    pub from_ts: Option<i64>,
    #[schemars(description = "To ts (unix seconds, inclusive). Omit → now")]
    pub to_ts: Option<i64>,
    /// Server từ chối vượt trần (10k) và báo `truncated` khi cắt.
    #[schemars(description = "Max rows (default 1000, hard cap 10000)")]
    pub limit: Option<i64>,
    /// Lọc server-side, vd `"order"`, `"summary"`, `"raw"`.
    #[schemars(description = "Filter by signal: order|summary|raw|utilization…")]
    pub signal: Option<String>,
    /// Lọc server-side theo `labels.kind`, vd `"trading_step"`, `"snapshot"`.
    #[schemars(description = "Filter by labels.kind, e.g. trading_step")]
    pub label_kind: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OrdersParams {
    #[schemars(description = "Station id holding trading state, e.g. \"grid\"")]
    pub node: String,
    /// `open` | `closed`; bỏ trống → cả hai.
    #[schemars(description = "Filter by labels.status: open|closed")]
    pub status: Option<String>,
    #[schemars(description = "From ts (unix seconds, inclusive)")]
    pub from_ts: Option<i64>,
    #[schemars(description = "To ts (unix seconds, inclusive)")]
    pub to_ts: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetConfigParams {
    /// Node id (vd "grid"). Bỏ trống → toàn bộ pipeline.
    #[schemars(description = "Node id; omit for all components")]
    pub id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetParamParams {
    /// Node id, vd "grid".
    #[schemars(description = "Node id, e.g. \"grid\"")]
    pub id: String,
    /// JSON pointer into the live config, vd "params.sl_pct" or "script_path".
    #[schemars(description = "JSON pointer, e.g. \"params.sl_pct\"")]
    pub path: String,
    /// JSON literal, e.g. 0.02 | "trading" | true.
    #[schemars(description = "JSON literal value")]
    pub value: String,
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

    /// Đọc cấu hình đang chạy (kể cả `params` của script Rhai). Đây là bước
    /// **đọc trước khi sửa** — không có nó thì mọi lần sửa đều phải gửi lại danh
    /// sách node đầy đủ qua `opsense_reload`.
    #[tool(
        description = "Read live component config (type, inputs, full config incl. params). Call this BEFORE editing anything."
    )]
    async fn opsense_get_config(
        &self,
        Parameters(p): Parameters<GetConfigParams>,
    ) -> Result<String, String> {
        tools::get_config(&self.client, p.id.as_deref()).await
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

    #[tool(
        description = "Query observations from a TimeseriesStation. Bounded server-side (limit cap 10000, window cap 30 days) and reports `truncated`. Filter server-side with `signal` / `label_kind` instead of pulling everything and filtering yourself."
    )]
    async fn opsense_query_timeseries(
        &self,
        Parameters(p): Parameters<QueryTimeseriesParams>,
    ) -> Result<String, String> {
        tools::query_timeseries(
            &self.client,
            &p.node,
            p.from_ts,
            p.to_ts,
            p.limit,
            p.signal.as_deref(),
            p.label_kind.as_deref(),
        )
        .await
    }

    /// State giao dịch nằm trong **station** (không mất khi restart): lệnh
    /// `signal = "order"` (`labels.status = open|closed`) và cursor T+N
    /// (`labels.kind = "trading_step"`).
    #[tool(
        description = "Trading state in a station: orders (labels.status open/closed) with optional status filter. One call instead of pulling the station and filtering by hand."
    )]
    async fn opsense_orders(
        &self,
        Parameters(p): Parameters<OrdersParams>,
    ) -> Result<String, String> {
        tools::orders(
            &self.client,
            &p.node,
            p.status.as_deref(),
            p.from_ts,
            p.to_ts,
        )
        .await
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

    /// Sửa MỘT thành phần. **Dùng cái này thay `opsense_reload`** khi chỉ cần
    /// đổi một param: reload thay toàn bộ danh sách node nên thiếu một node là
    /// mất node đó.
    #[tool(
        description = "Patch ONE field of a node's live config (JSON pointer, e.g. \"params.sl_pct\" = 0.02). Server re-reads current config, patches, validates everything, then reloads. Preferred over opsense_reload."
    )]
    async fn opsense_set_param(
        &self,
        Parameters(p): Parameters<SetParamParams>,
    ) -> Result<String, String> {
        tools::set_param(&self.client, &p.id, &p.path, &p.value).await
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
                 To EDIT a config: 1) opsense_get_config to read it, 2) opsense_set_param with a \
                 JSON pointer (e.g. \"params.sl_pct\") — preferred; opsense_reload replaces the WHOLE \
                 node list and loses any node you forget to include. \
                 Runtime state (orders, T+N cursor, snapshots) lives in stations — read it with \
                 opsense_query_timeseries."
                    .to_string(),
            ),
        }
    }
}
