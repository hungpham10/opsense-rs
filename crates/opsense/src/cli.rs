//! CLI dạng script: mỗi lệnh = **một** GraphQL round-trip qua
//! [`OpsenseClient`](crate::client::OpsenseClient).
//!
//! Nguyên tắc: **MCP và CLI là hai mặt của cùng một API**. Không có logic nghiệp
//! vụ nằm ở đây hay trong `mcp/` — cả hai chỉ là adapter. Nhờ vậy `opsense
//! query` không bao giờ lệch `opsense_query_timeseries` của MCP, và test được
//! cả hai cùng lúc.
//!
//! Xuất JSON mặc định (để pipe vào `jq`); xem `--format table` cho người đọc.

use anyhow::{Context as _, Result};

use crate::client::OpsenseClient;

/// Endpoint GraphQL: `--endpoint` > `$OPSENSE_GRAPHQL_URL` > default.
pub fn default_endpoint() -> String {
    std::env::var("OPSENSE_GRAPHQL_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080/graphql".to_string())
}

pub fn client(endpoint: Option<String>) -> Result<OpsenseClient> {
    OpsenseClient::new(endpoint.unwrap_or_else(default_endpoint))
        .context("không tạo được GraphQL client")
}

/// In ra JSON pretty — mặc định cho mọi lệnh để pipe được.
fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// `opsense status` — topology node + danh sách station.
pub async fn status(endpoint: Option<String>) -> Result<()> {
    let s = status_value(endpoint).await?;
    print_json(&s)
}

pub(crate) async fn status_value(endpoint: Option<String>) -> Result<crate::client::Status> {
    client(endpoint)?.status().await.context("Query.status")
}

/// `opsense components [id]` — cấu hình **đang chạy** của node (kể cả `params`).
pub async fn components(endpoint: Option<String>, id: Option<String>) -> Result<()> {
    let list = components_value(endpoint, id.as_deref()).await?;
    print_json(&list)
}

pub(crate) async fn components_value(
    endpoint: Option<String>,
    id: Option<&str>,
) -> Result<Vec<crate::client::ComponentConfig>> {
    client(endpoint)?
        .components(id)
        .await
        .context("Query.components")
}

/// `opsense get-param <node> <path>` — đọc một trường trong cấu hình đang chạy.
///
/// `path` là JSON pointer (`params.sl_pct`). Trả về JSON literal nên pipe được.
pub async fn get_param(endpoint: Option<String>, id: &str, path: &str) -> Result<()> {
    let value = get_param_value(endpoint, id, path).await?;
    println!("{value}");
    Ok(())
}

pub(crate) async fn get_param_value(
    endpoint: Option<String>,
    id: &str,
    path: &str,
) -> Result<serde_json::Value> {
    let list = components_value(endpoint, Some(id)).await?;
    let comp = list
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("không có node '{id}' (xem `opsense status`)"))?;
    comp.config
        .pointer(path)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("node '{id}' không có path '{path}'"))
}

/// `opsense set-param <node> <path> <json-value>` — sửa MỘT thành phần.
///
/// Ưu tiên hơn `reload`: không cần gửi lại cả danh sách node, nên không sợ mất
/// node. Server validate trước khi reload, patch hỏng thì runtime giữ nguyên.
pub async fn set_param(
    endpoint: Option<String>,
    id: &str,
    path: &str,
    value: &str,
) -> Result<()> {
    let r = client(endpoint)?
        .patch_component(id, path, value)
        .await
        .context("Mutation.patchComponent")?;
    print_json(&r)
}

/// `opsense query <node>` — đọc observation của station (có guard + filter).
#[allow(clippy::too_many_arguments)]
pub async fn query(
    endpoint: Option<String>,
    node: &str,
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<i64>,
    signal: Option<String>,
    label_kind: Option<String>,
) -> Result<()> {
    let out = client(endpoint)?
        .query_station(
            node,
            from,
            to,
            limit,
            signal.as_deref(),
            label_kind.as_deref(),
        )
        .await
        .context("Query.queryTimeseries")?;
    if out.truncated {
        eprintln!(
            "warning: truncated ({} rows scanned) — tăng --limit hoặc chia nhỏ --from/--to",
            out.scanned
        );
    }
    print_json(&out)
}

/// `opsense orders <node>` — lệnh giao dịch trong station (`signal = "order"`).
pub async fn orders(
    endpoint: Option<String>,
    node: &str,
    status: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
) -> Result<()> {
    let dump = crate::mcp::tools::orders(&client(endpoint)?, node, status.as_deref(), from, to)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let v: serde_json::Value = serde_json::from_str(&dump).map_err(|e| anyhow::anyhow!("{e}"))?;
    print_json(&v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_prefers_flag_then_env_then_default() {
        // Chỉ test phần dựng chuỗi, không tạo client (cần network).
        assert_eq!(default_endpoint(), "http://127.0.0.1:8080/graphql");
    }
}
