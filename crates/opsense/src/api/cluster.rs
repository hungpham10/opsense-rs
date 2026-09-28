//! HTTP của tầng cluster — **chỉ quan sát**.
//!
//! `GET /api/cluster/v1/nodes` trả view mà node này có về mesh: ai còn sống,
//! ai đang nghi ngờ, ai đã chết, và mỗi node chạy version nào. Đây là *quan
//! sát*, không phải quyết định — "ai là master" thuộc tầng raft và **không**
//! được suy ra từ dữ liệu ở đây.
//!
//! Endpoint nằm trong `/api/*` nên đi qua cùng OIDC với phần còn lại của API:
//! đây là dữ liệu về hạ tầng, không phải endpoint nội bộ giữa các node. Phần
//! trao đổi giữa các node (`/internal/*`) là chuyện PR sau — nó cần secret riêng,
//! không đi qua OIDC.

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use serde::Serialize;

use crate::api::AppState;

#[derive(Debug, Serialize)]
struct NodesResponse {
    /// Mesh có đang bật không. `false` = config không khai `[gossip].node_id` —
    /// trạng thái mặc định, không phải lỗi.
    enabled: bool,
    /// Node này tự nhận là ai (rỗng khi mesh tắt).
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    /// Mọi trạng thái mà tầng gossip đang tin. Sắp theo `node_id` cho output ổn
    /// định.
    nodes: Vec<opsense_mlib::gossip::NodeInfo>,
    /// Node nào đang chạy version khác — thứ *quan sát được*, báo ra chứ không
    /// kết luận gì.
    version_mismatches: std::collections::BTreeMap<String, String>,
    /// View đã ổn định đủ `settle_secs` chưa. Dùng để *trì hoãn* hành động,
    /// không phải để quyết định master.
    settled: bool,
}

async fn list_nodes(State(state): State<AppState>) -> Json<NodesResponse> {
    match state.cluster() {
        Some(mesh) => Json(NodesResponse {
            enabled: true,
            node_id: Some(mesh.node_id().to_string()),
            nodes: mesh.nodes(),
            version_mismatches: mesh.version_mismatches(),
            settled: mesh.is_settled(),
        }),
        None => Json(NodesResponse {
            enabled: false,
            node_id: None,
            nodes: Vec::new(),
            version_mismatches: Default::default(),
            settled: false,
        }),
    }
}

/// `/health/live` — liveness cho **peer** gọi, không cần token.
///
/// Tách khỏi `/health` vì hai tầng khác nhau: `/health` trả lời câu hỏi "tiến
/// trình này còn sống" (đủ để nginx, supervisor, container healthcheck dùng),
/// còn `/health/live` là **điều kiện để coi node là sống trong mesh** — và điều
/// kiện đó phải là thứ mà node kia kiểm tra, không phải thứ ta tự khai.
async fn live() -> &'static str {
    "ok"
}

pub fn routes(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/v1/nodes", get(list_nodes))
        .with_state(state)
}

/// Route liveness, gắn ở gốc (không phải dưới `/api`).
pub fn live_routes() -> Router<AppState> {
    Router::new().route("/health/live", get(live))
}
