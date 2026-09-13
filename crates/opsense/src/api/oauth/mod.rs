//! OAuth2 Device Authorization Grant (RFC 8628) cho REPL/MCP client.
//!
//! Mount dưới `/api/oauth`. Mỗi endpoint gọi sang `Admin` entity ở
//! `crates/opsense-model/src/entities/admin/{device,session}.rs`.
//!
//! Flow:
//! 1. CLI → POST /api/oauth/v1/device/code  (issues device_code + user_code)
//! 2. User mở browser, nhập user_code  → POST /api/oauth/v1/device/verify
//!    (Bearer JWT từ Nginx; X-User-Id + X-Tenant-Id từ Nginx)
//! 3. CLI poll → POST /api/oauth/v1/device/token → nhận access_token
//! 4. (Optional) POST /api/oauth/v1/session/issue → Ed25519 keypair
//!
//! Bearer middleware (Nginx hoặc Axum layer) validate access_token; trong
//! plan này assume Nginx đã validate và inject X-User-Id header.

pub mod metrics;
mod v1;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Json;

use super::AppState;
use crate::api::oauth::metrics::OAuthMetricsSnapshot;

pub fn routes() -> Router<AppState> {
    Router::new()
        .nest("/v1", v1::routes())
        .route("/metrics/oauth", get(metrics_handler))
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.oauth_metrics.snapshot();
    (StatusCode::OK, Json(snap))
}

// Re-export for use in AppState
pub use self::metrics::OAuthMetrics;

// Compile-time guard: đảm bảo OAuthMetricsSnapshot chỉ serialize thuộc tính
// đã khai báo (chống regression).
const _: fn() = || {
    let _: OAuthMetricsSnapshot = OAuthMetricsSnapshot {
        device_code_issued: 0,
        device_code_approved: 0,
        device_code_denied: 0,
        access_token_issued: 0,
        access_token_refreshed: 0,
        long_session_issued: 0,
    };
};
