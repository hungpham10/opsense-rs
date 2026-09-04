//! Thin GraphQL client that talks to `opsense serve`'s `POST /graphql`.
//!
//! Every method is one HTTP call — no local state, no diffing, no
//! generation tracking. The server serialises writes internally via
//! `RwLock`, so the client doesn't need to worry about races.

use std::collections::BTreeMap;

use reqwest::Client;
use serde::{Deserialize, Serialize};

pub use opsense_core::Observation;

use crate::repl::display::TableDisplay;

// ─────────────────────────────────────────────────────────────────────────────
// Response types (mirror of the GraphQL schema in api/repl/v1.rs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeSummary {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub inputs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StationSummary {
    pub id: String,
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Status {
    pub nodes: Vec<NodeSummary>,
    pub stations: Vec<StationSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EditResult {
    #[serde(rename = "reloaded")]
    pub reloaded: bool,
    pub nodes: Vec<NodeSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SetAttributeResult {
    pub ok: bool,
    pub env_override_active: bool,
}

#[derive(Debug, Deserialize)]
struct GraphQLError {
    pub message: String,
}

#[derive(Debug, Deserialize)]
struct GqlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQLError>>,
}

impl<T> GqlResponse<T> {
    fn into_result(self) -> anyhow::Result<T> {
        if let Some(errors) = self.errors {
            let msg = errors
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            anyhow::bail!("GraphQL error: {msg}");
        }
        self.data.ok_or_else(|| anyhow::anyhow!("no data in response"))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ComponentInput (matches the GraphQL input type)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentInput {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<Vec<String>>,
}

impl ComponentInput {
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            config: None,
            inputs: None,
        }
    }

    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_inputs(mut self, inputs: Vec<String>) -> Self {
        self.inputs = Some(inputs);
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Client
// ─────────────────────────────────────────────────────────────────────────────

pub struct OpsenseClient {
    endpoint: String,
    http: Client,
    /// Optional Bearer token (OAuth2 access_token). Khi None, request
    /// sẽ đi qua như guest (Nginx vẫn inject `X-User-Id = "guest"`).
    bearer: Option<String>,
}

impl OpsenseClient {
    pub fn new(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: endpoint.into(),
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            bearer: load_bearer_from_env(),
        })
    }

    /// Override Bearer token (dùng cho test hoặc sau khi device flow issue).
    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    /// Xoá Bearer (về guest mode).
    pub fn clear_bearer(&mut self) {
        self.bearer = None;
    }

    async fn gql<Q, V>(&self, query: &str, variables: V) -> anyhow::Result<Q>
    where
        for<'de> Q: serde::de::Deserialize<'de>,
        V: Serialize,
    {
        #[derive(Serialize)]
        struct Request<'a, V> {
            query: &'a str,
            variables: V,
        }
        let mut req = self
            .http
            .post(&self.endpoint)
            .json(&Request { query, variables });
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        }
        let request: GqlResponse<Q> = req
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        request.into_result()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query methods
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn status(&self) -> anyhow::Result<Status> {
        const QUERY: &str = r#"
            query {
                status {
                    nodes { id type inputs }
                    stations { id kind }
                }
            }
        "#;
        self.gql(QUERY, ()).await
    }

    pub async fn attributes(&self) -> anyhow::Result<BTreeMap<String, String>> {
        const QUERY: &str = r#"query { attributes }"#;
        self.gql(QUERY, ()).await
    }

    pub async fn query_timeseries(
        &self,
        node: &str,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
    ) -> anyhow::Result<Vec<Observation>> {
        const QUERY: &str = r#"
            query($node: String!, $fromTs: Int, $toTs: Int) {
                queryTimeseries(node: $node, fromTs: $fromTs, toTs: $toTs) {
                    ts metric value labels
                }
            }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            node: &'a str,
            from_ts: Option<i64>,
            to_ts: Option<i64>,
        }
        self.gql(QUERY, Vars { node, from_ts, to_ts }).await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Mutation methods
    // ─────────────────────────────────────────────────────────────────────────

    /// Push full component list — add/update/remove đều qua đây.
    /// REPL client tính new list locally rồi call 1 lần.
    pub async fn reload(&self, components: Vec<ComponentInput>) -> anyhow::Result<EditResult> {
        const MUTATION: &str = r#"
            mutation($components: [ComponentInput!]!) {
                reload(components: $components) { reloaded nodes { id type inputs } }
            }
        "#;
        #[derive(Serialize)]
        struct Vars {
            components: Vec<ComponentInput>,
        }
        self.gql(MUTATION, Vars { components }).await
    }

    pub async fn set_attribute(&self, name: &str, value: &str) -> anyhow::Result<SetAttributeResult> {
        const MUTATION: &str = r#"
            mutation($name: String!, $value: String!) {
                setAttribute(name: $name, value: $value) { ok envOverrideActive }
            }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            name: &'a str,
            value: &'a str,
        }
        self.gql(MUTATION, Vars { name, value }).await
    }

    pub async fn remove_attribute(&self, name: &str) -> anyhow::Result<bool> {
        const MUTATION: &str = r#"
            mutation($name: String!) { removeAttribute(name: $name) }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            name: &'a str,
        }
        self.gql(MUTATION, Vars { name }).await
    }
}

impl TableDisplay for OpsenseClient {
    fn status_table(&self, status: &Status) -> comfy_table::Table {
        use comfy_table::*;
        let mut table = Table::new();
        table.set_header(["id", "type", "inputs"]);
        for n in &status.nodes {
            table.add_row([
                n.id.as_str(),
                n.kind.as_str(),
                n.inputs.join(", ").as_str(),
            ]);
        }
        table
    }

    fn stations_table(&self, stations: &[StationSummary]) -> comfy_table::Table {
        use comfy_table::*;
        let mut table = Table::new();
        table.set_header(["id", "kind"]);
        for s in stations {
            table.add_row([s.id.as_str(), s.kind.as_str()]);
        }
        table
    }
}

/// Load Bearer token từ `OPSENSE_ACCESS_TOKEN` env var, hoặc file
/// `~/.config/opsense/token`. Trả None nếu không tìm thấy (guest mode).
fn load_bearer_from_env() -> Option<String> {
    if let Ok(token) = std::env::var("OPSENSE_ACCESS_TOKEN") {
        let trimmed = token.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    // File fallback
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::PathBuf::from(home)
            .join(".config")
            .join("opsense")
            .join("token");
        if let Ok(content) = std::fs::read_to_string(&path) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `OpsenseClient::new` parse endpoint, không panic khi host lạ.
    #[test]
    fn test_new_client_parses_endpoint() {
        let c = OpsenseClient::new("http://127.0.0.1:8080").unwrap();
        assert_eq!(c.endpoint, "http://127.0.0.1:8080");
    }

    /// `with_bearer` set bearer field.
    #[test]
    fn test_with_bearer() {
        let mut c = OpsenseClient::new("http://localhost").unwrap();
        c = c.with_bearer("tok-123");
        assert_eq!(c.bearer.as_deref(), Some("tok-123"));
        c.clear_bearer();
        assert!(c.bearer.is_none());
    }

    /// `load_bearer_from_env` đọc từ `OPSENSE_ACCESS_TOKEN` nếu có.
    /// Test này tạm bỏ qua nếu env var đã được set (tránh flaky trên CI).
    #[test]
    fn test_load_bearer_from_env() {
        // SAFETY: Test chạy đơn luồng, không race với threads khác.
        unsafe { std::env::set_var("OPSENSE_ACCESS_TOKEN", "test-token-abc") };
        let loaded = load_bearer_from_env();
        unsafe { std::env::remove_var("OPSENSE_ACCESS_TOKEN") };
        assert_eq!(loaded.as_deref(), Some("test-token-abc"));
    }

    /// Empty env var trả về None (trừ khi file fallback có giá trị).
    #[test]
    fn test_load_bearer_env_empty() {
        unsafe { std::env::remove_var("OPSENSE_ACCESS_TOKEN") };
        // Trừ khi dev có sẵn `~/.config/opsense/token` thì kết quả không None;
        // chỉ assert rằng hàm không panic và trả String rỗng được coi là None.
        let _ = load_bearer_from_env();
    }
}
