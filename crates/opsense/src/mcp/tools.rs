//! MCP tool implementations. Each tool = 1 GraphQL round-trip via `OpsenseClient`.
//!
//! Returns `Result<String, String>` — `String` impls `IntoContents` so the
//! `#[tool]` macro auto-wraps it as a `CallToolResult::success`. The `Err`
//! arm is rendered as a text error message (still success transport-wise;
//! MCP doesn't distinguish).

use crate::client::OpsenseClient;
use crate::client::graphql::ComponentInput;

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

/// Cấu hình **đang chạy** (`params`, `script_path`, …). Bỏ `id` → tất cả node.
///
/// Đọc trước khi sửa: `opsense_reload` nhận danh sách node đầy đủ, nên không
/// đọc thì sửa một param cũng phải dựng lại cả pipeline.
pub async fn get_config(client: &OpsenseClient, id: Option<&str>) -> Result<String, String> {
    client
        .components(id)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|c| json_dump(&c).map_err(|e| format!("{e}")))
}

pub async fn set_attribute(
    client: &OpsenseClient,
    name: &str,
    value: &str,
) -> Result<String, String> {
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
    limit: Option<i64>,
    signal: Option<&str>,
    label_kind: Option<&str>,
) -> Result<String, String> {
    client
        .query_station(node, from_ts, to_ts, limit, signal, label_kind)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|c| json_dump(&c).map_err(|e| format!("{e}")))
}

/// State giao dịch trong một station: lệnh (`signal = "order"`) **và** cursor T+N
/// (`labels.kind = "trading_step"`).
///
/// Cursor cố ý **không** lọc theo `signal` được: nó là `signal = "summary"`, nên
/// lọc `signal = "order"` sẽ âm thầm rơi mất cursor — tức mất đúng thứ agent
/// cần để biết "T+N đã chạy tới nến nào". Vì vậy lấy cả hai loại rồi lọc ở đây.
/// `status` chỉ áp cho lệnh (cursor không có status).
pub async fn orders(
    client: &OpsenseClient,
    node: &str,
    status: Option<&str>,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
) -> Result<String, String> {
    let raw = query_timeseries(client, node, from_ts, to_ts, Some(2000), None, None).await?;
    let mut v: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("query result: {e}"))?;
    if let Some(obs) = v
        .get_mut("observations")
        .and_then(|o| o.as_array_mut())
    {
        obs.retain(|o| {
            let kind = o.get("labels").and_then(|l| l.get("kind")).and_then(|k| k.as_str());
            let signal = o.get("signal").and_then(|s| s.as_str());
            if kind == Some("trading_step") {
                return true; // cursor T+N — không có `status`
            }
            if signal != Some("order") {
                return false;
            }
            match status {
                None => true,
                // Chỉ lệnh mới mang `labels.status`.
                Some(want) => o
                    .get("labels")
                    .and_then(|l| l.get("status"))
                    .and_then(|s| s.as_str())
                    == Some(want),
            }
        });
    }
    json_dump(&v).map_err(|e| format!("{e}"))
}

/// Sửa **một** thành phần của một node (vd `params.sl_pct` → `0.02`).
///
/// Đường sửa mặc định: `opsense_reload` nhận danh sách node **đầy đủ**, thiếu một
/// node là mất node đó. Ở đây server tự đọc cấu hình hiện tại, patch đúng một chỗ,
/// validate lại toàn bộ rồi mới reload.
pub async fn set_param(
    client: &OpsenseClient,
    id: &str,
    path: &str,
    value: &str,
) -> Result<String, String> {
    client
        .patch_component(id, path, value)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| json_dump(&r).map_err(|e| format!("{e}")))
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
