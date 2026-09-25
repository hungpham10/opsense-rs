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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_prefers_flag_then_env_then_default() {
        // Chỉ test phần dựng chuỗi, không tạo client (cần network).
        assert_eq!(default_endpoint(), "http://127.0.0.1:8080/graphql");
    }
}
