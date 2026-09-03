//! MCP tool implementations. Each tool = 1 GraphQL round-trip via `OpsenseClient`.
//!
//! Returns `Result<String, String>` — `String` impls `IntoContents` so the
//! `#[tool]` macro auto-wraps it as a `CallToolResult::success`. The `Err`
//! arm is rendered as a text error message (still success transport-wise;
//! MCP doesn't distinguish).

use crate::client::graphql::ComponentInput;
use crate::client::OpsenseClient;

fn json_dump<T: serde::Serialize>(v: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(v)
}

pub async fn status(client: &OpsenseClient) -> Result<String, String> {
    client
        .status()
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|s| json_dump(&s).map_err(|e| format!("{e}")))
}

pub async fn attributes(client: &OpsenseClient) -> Result<String, String> {
    client
        .attributes()
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|m| json_dump(&m).map_err(|e| format!("{e}")))
}

pub async fn set_attribute(client: &OpsenseClient, name: &str, value: &str) -> Result<String, String> {
    client
        .set_attribute(name, value)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| json_dump(&r).map_err(|e| format!("{e}")))
}

pub async fn remove_attribute(client: &OpsenseClient, name: &str) -> Result<String, String> {
    client
        .remove_attribute(name)
        .await
        .map(|removed| format!("removed={removed}"))
        .map_err(|e| format!("{e:#}"))
}

pub async fn query_timeseries(
    client: &OpsenseClient,
    node: &str,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
) -> Result<String, String> {
    client
        .query_timeseries(node, from_ts, to_ts)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|obs| json_dump(&obs).map_err(|e| format!("{e}")))
}

/// `components_json` is a JSON array of component objects. Each element:
/// `{ "type": "...", "id": "...", "config": {...}, "inputs": [...] }`.
pub async fn reload_from_json(
    client: &OpsenseClient,
    components_json: &str,
) -> Result<String, String> {
    let components: Vec<serde_json::Value> =
        serde_json::from_str(components_json).map_err(|e| format!("invalid JSON array: {e}"))?;
    let mut parsed: Vec<ComponentInput> = Vec::with_capacity(components.len());
    for (i, v) in components.into_iter().enumerate() {
        match serde_json::from_value::<ComponentInput>(v) {
            Ok(c) => parsed.push(c),
            Err(e) => return Err(format!("component[{i}]: {e}")),
        }
    }
    client
        .reload(parsed)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| json_dump(&r).map_err(|e| format!("{e}")))
}
