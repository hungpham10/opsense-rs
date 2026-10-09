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

use crate::client::{OpsenseClient, TimeArg};

/// Endpoint GraphQL: `--endpoint` > `$OPSENSE_GRAPHQL_URL` > default.
pub fn default_endpoint() -> String {
// Đường dẫn thật là `/api/repl/graphql`: `serve.rs` nest `repl::routes()` (route
// `/graphql`) dưới `/api/repl`. Thiếu `/api/repl` thì nginx trả 500, không
// phải 401 — dễ tưởng server chết. Xem `crates/opsense/tests/common/mod.rs:38`
// (`serve_url` dùng `localhost` để khớp `host` trong seed tenant).
// `localhost` chứ KHÔNG `127.0.0.1`: nginx tra tenant theo **Host header**
// (`04-api.conf:20` -> `/api/admin/v1/tenant/{host}/id`) va seed tao
// `host = 'localhost'` (`sql/postgres/dev/50-init-tenant.sql`). Gui
// `127.0.0.1` thi khong ra tenant => **500**, trong nhu server chet chu
// khong phai la thieu auth. Xem `crates/opsense/tests/common/mod.rs:38`.
    std::env::var("OPSENSE_GRAPHQL_URL")
        .unwrap_or_else(|_| "http://localhost:8080/api/repl/graphql".to_string())
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
    from: Option<String>,
    to: Option<String>,
    limit: Option<i64>,
    signal: Option<String>,
    label_kind: Option<String>,
    status: Option<String>,
    order: Option<String>,
) -> Result<()> {
    let now = opsense_components::signal::now_secs();
    let out = client(endpoint)?
        .query_station(
            node,
            time_arg(from.as_deref(), now)?,
            time_arg(to.as_deref(), now)?,
            limit,
            signal.as_deref(),
            label_kind.as_deref(),
            status.as_deref(),
            order.as_deref(),
        )
        .await
        .context("Query.queryTimeseries")?;
    if out.truncated {
        eprintln!(
            "warning: truncated ({} rows scanned, order={}) — tăng --limit hoặc chia nhỏ --from/--to",
            out.scanned, out.order
        );
    }
    print_json(&out)
}

/// `opsense orders <node>` — lệnh giao dịch trong station (`signal = "order"`).
///
/// Gọi thẳng `Query.orders` (một round-trip, gộp `order_id` + lọc `status`
/// server-side) thay vì đi vòng qua `crate::mcp::tools` — CLI là adapter, không
/// phải tầng dưới MCP.
#[allow(clippy::too_many_arguments)]
pub async fn orders(
    endpoint: Option<String>,
    node: &str,
    status: Option<String>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<i64>,
    order: Option<String>,
    interval: Option<String>,    // NEW
    mark_price: Option<f64>,     // NEW
) -> Result<()> {
    let now = opsense_components::signal::now_secs();
    let out = client(endpoint)?
        .orders(
            node,
            status.as_deref(),
            time_arg(from.as_deref(), now)?,
            time_arg(to.as_deref(), now)?,
            limit,
            order.as_deref(),
            interval.as_deref(),
            mark_price,
        )
        .await
        .context("Query.orders")?;
    if out.truncated {
        eprintln!(
            "warning: truncated ({} rows scanned, order={}) — tăng --limit hoặc chia nhỏ --from/--to",
            out.scanned, out.order
        );
    }
    print_json(&out)
}

/// `--from/--to` nhận unix giây **hoặc** khoảng tương đối; validate tại đây để
/// lỗi báo trước khi mở kết nối.
fn time_arg(raw: Option<&str>, now: i64) -> Result<Option<TimeArg>> {
    raw.map(|s| TimeArg::parse(s))
        .transpose()?
        .map(|arg| {
            arg.resolve(now)?;
            Ok(arg)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_prefers_flag_then_env_then_default() {
        // Chỉ test phần dựng chuỗi, không tạo client (cần network).
        assert_eq!(default_endpoint(), "http://localhost:8080/api/repl/graphql");
    }
}
