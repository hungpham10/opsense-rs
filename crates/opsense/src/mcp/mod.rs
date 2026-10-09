//! MCP thin client — speaks to `opsense serve` via GraphQL.
//!
//! Every MCP tool is a single GraphQL round-trip via [`OpsenseClient`]. No
//! local state, no caching (the server holds the truth). 11 tools:
//!   - opsense_status            → Query.status
//!   - opsense_get_config        → Query.components (đọc trước khi sửa)
//!   - opsense_attributes        → Query.attributes
//!   - opsense_set_attribute     → Mutation.setAttribute
//!   - opsense_remove_attribute  → Mutation.removeAttribute
//!   - opsense_query_timeseries  → Query.queryTimeseries
//!   - opsense_orders            → Query.orders (lệnh + cursor T+N, **optional PnL aggregation**)
//!   - opsense_set_param         → Mutation.patchComponent (sửa 1 param — mặc định)
//   - opsense_reload            → Mutation.reload (thay TOÀN BỘ danh sách node)
//!   - opsense_clear_station     → Mutation.clearStation (xoá 1 station)
//!   - opsense_clear_all_stations→ Mutation.clearAllStations (reset phiên trading)

pub mod server;
pub mod tools;

use std::io;

use crate::client::OpsenseClient;

// Đường dẫn thật là `/api/repl/graphql` — `serve.rs` nest `repl::routes()`
// (route `/graphql`) dưới `/api/repl`. Thiếu `/api/repl` thì nginx trả
// **500**, không phải 401, nên trông như server chết thay vì thiếu auth.
// `localhost` chứ KHÔNG `127.0.0.1`: nginx tra tenant theo **Host header**
// (`04-api.conf:20` -> `/api/admin/v1/tenant/{host}/id`) va seed tao
// `host = 'localhost'` (`sql/postgres/dev/50-init-tenant.sql`). Gui
// `127.0.0.1` thi khong ra tenant => **500**, trong nhu server chet chu
// khong phai la thieu auth. Xem `crates/opsense/tests/common/mod.rs:38`.
const DEFAULT_ENDPOINT: &str = "http://localhost:8080/api/repl/graphql";

/// Spin up the MCP server on stdio. Connects to `opsense serve` GraphQL
/// at `endpoint` (env `OPSENSE_GRAPHQL_URL` overrides default).
pub async fn run(endpoint: Option<String>) -> io::Result<()> {
    let endpoint = endpoint
        .or_else(|| std::env::var("OPSENSE_GRAPHQL_URL").ok())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

    let client = OpsenseClient::new(&endpoint).map_err(|e| io::Error::other(e.to_string()))?;

    server::serve(client).await
}
