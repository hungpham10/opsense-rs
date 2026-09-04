//! 9 OAuth2 endpoints dưới `/api/oauth/v1/`.
//!
//! Authentication:
//! - Endpoints KHÔNG yêu cầu Bearer (public cho CLI/console): device/code, device (form),
//!   device/token, token/refresh
//! - Endpoints yêu cầu Bearer (Nginx inject `X-User-Id` + `X-Tenant-Id`):
//!   device/verify, token/revoke, session/*
//!
//! Refs: RFC 8628 §3 (Device Authorization Grant) + RFC 6749 §6 (Refresh) + §11 (Revocation).

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, http::HeaderMap};
use serde::{Deserialize, Serialize};

use opsense_model::entities::admin::{sha256_hex, Admin};
use sqlx::Row;

use crate::api::AppState;
use crate::api::admin::AdminHeaders;

// =========================================================================
// Response/Request types
// =========================================================================

#[derive(Serialize, Debug)]
struct DeviceCodeResponse {
    device_code:       String,
    user_code:         String,
    verification_uri:  String,
    expires_in:        i64,
    interval:          i32,
}

#[derive(Deserialize, Debug)]
struct DeviceVerifyRequest {
    user_code: String,
}

#[derive(Serialize, Debug)]
struct DeviceVerifyResponse {
    user_id:  String,
    status:   String,
}

#[derive(Deserialize, Debug)]
struct DeviceTokenRequest {
    device_code: String,
    grant_type:  String, // "urn:ietf:params:oauth:grant-type:device_code"
}

#[derive(Serialize, Debug)]
struct DeviceTokenResponse {
    access_token:  String,
    refresh_token: String,
    token_type:    String,
    expires_in:    i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id:    Option<String>,
}

#[derive(Serialize, Debug)]
struct OAuthError {
    error:             String,
    error_description: String,
}

#[derive(Deserialize, Debug)]
struct RefreshRequest {
    refresh_token: String,
}

#[derive(Deserialize, Debug)]
struct RevokeRequest {
    token:         String,
    #[serde(default)]
    #[allow(dead_code)]
    token_type_hint: Option<String>,
}

#[derive(Deserialize, Debug)]
struct SessionIssueRequest {
    #[serde(default)]
    #[allow(dead_code)]
    expires_in_secs: Option<i64>, // optional override (mặc định 8h)
}

#[derive(Serialize, Debug)]
struct SessionIssueResponse {
    session_id:      String,
    private_key:     String,
    expires_in:      i64,
}

#[derive(Serialize, Debug)]
struct SessionListEntry {
    session_id:   String,
    status:       String,
    expires_at:   String,
    last_used_at: Option<String>,
    created_at:   String,
}

#[derive(Deserialize, Debug)]
struct SessionRevokeRequest {
    session_id: String,
}

#[derive(Serialize, Debug)]
struct OkResponse {
    ok:     bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

// =========================================================================
// Router
// =========================================================================

pub fn routes() -> Router<AppState> {
    Router::new()
        // ---- device flow (RFC 8628) ----
        .route("/device/code",  post(device_code))
        .route("/device",       get(device_form))
        .route("/device/verify", post(device_verify))
        .route("/device/token", post(device_token))
        // ---- token management ----
        .route("/token/refresh", post(token_refresh))
        .route("/token/revoke",  post(token_revoke))
        // ---- long session (Ed25519 keypair) ----
        .route("/session/issue",  post(session_issue))
        .route("/session/revoke", post(session_revoke))
        .route("/session/list",   get(session_list))
}

// =========================================================================
// Helpers
// =========================================================================

fn admin(state: &AppState) -> Arc<Admin> {
    state.admin_entity.clone()
}

fn oauth_error(status: StatusCode, error: &str, description: &str) -> Response {
    let body = OAuthError {
        error:             error.to_string(),
        error_description: description.to_string(),
    };
    (status, Json(body)).into_response()
}

fn ok(detail: Option<&str>) -> Response {
    Json(OkResponse { ok: true, detail: detail.map(String::from) }).into_response()
}

/// Extract user_id từ `X-User-Id` header (Nginx inject sau khi validate Bearer).
fn extract_user_id(headers: &HeaderMap) -> Result<String, Response> {
    headers
        .get("x-user-id")
        .or_else(|| headers.get("x-auth-user-id"))
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| oauth_error(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "Missing X-User-Id header (Bearer required)",
        ))
}

// =========================================================================
// Handlers
// =========================================================================

/// `POST /api/oauth/v1/device/code` — RFC 8628 §3.1
/// Body: `{}` (optional client_id). Trả về device_code + user_code.
async fn device_code(State(state): State<AppState>) -> Response {
    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1, // fallback cho dev
    };

    // verification_uri: nơi user sẽ mở browser để nhập user_code.
    // Thường là `https://<host>/api/oauth/v1/device` (do Nginx serve form).
    let verification_uri = std::env::var("OAUTH_VERIFICATION_URI")
        .unwrap_or_else(|_| "/api/oauth/v1/device".to_string());

    let admin = admin(&state);
    match admin.issue_device_code(tenant_id, &verification_uri).await {
        Ok(info) => {
            state.oauth_metrics.inc_device_code_issued();
            Json(DeviceCodeResponse {
                device_code:       info.device_code,
                user_code:         info.user_code,
                verification_uri:  info.verification_uri,
                expires_in:        info.expires_in_secs,
                interval:          info.interval_secs,
            }).into_response()
        }
        Err(e) => oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("issue_device_code failed: {e}"),
        ),
    }
}

/// `GET /api/oauth/v1/device` — HTML form cho user nhập user_code.
async fn device_form() -> Response {
    let html = r#"<!DOCTYPE html>
<html>
<head><title>Opsense Device Authorization</title></head>
<body>
<h1>Opsense Device Authorization</h1>
<p>Nhập mã user_code hiển thị trên CLI của bạn:</p>
<form method="POST" action="/api/oauth/v1/device/verify" id="f">
  <input type="text" name="user_code" placeholder="user_code" required />
  <button type="submit">Authorize</button>
</form>
<script>
document.getElementById('f').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const r = await fetch('/api/oauth/v1/device/verify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_code: fd.get('user_code') }),
  });
  const j = await r.json();
  document.body.innerHTML = r.ok
    ? '<h2>Authorized!</h2><p>You can close this tab and return to the CLI.</p>'
    : '<h2>Error</h2><pre>' + JSON.stringify(j, null, 2) + '</pre>';
});
</script>
</body>
</html>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/html; charset=utf-8")
        .body(axum::body::Body::from(html))
        .unwrap()
}

/// `POST /api/oauth/v1/device/verify` — user duyệt device code (yêu cầu Bearer).
/// Body: `{ "user_code": "..." }`.
async fn device_verify(
    State(state): State<AppState>,
    AdminHeaders { tenant_id, .. }: AdminHeaders,
    headers: HeaderMap,
    Json(payload): Json<DeviceVerifyRequest>,
) -> Response {
    let user_id = match extract_user_id(&headers) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let tenant: i64 = tenant_id.into();

    let admin = admin(&state);
    match admin.approve_device_code(tenant, &user_id, &payload.user_code).await {
        Ok(_) => {
            state.oauth_metrics.inc_device_code_approved();
            Json(DeviceVerifyResponse {
                user_id,
                status: "approved".to_string(),
            }).into_response()
        }
        Err(e) => {
            state.oauth_metrics.inc_device_code_denied();
            let msg = format!("{e}");
            let code = if msg.contains("expired") {
                ("expired_token", StatusCode::GONE)
            } else if msg.contains("already") {
                ("invalid_grant", StatusCode::CONFLICT)
            } else {
                ("invalid_request", StatusCode::BAD_REQUEST)
            };
            oauth_error(code.1, code.0, &msg)
        }
    }
}

/// `POST /api/oauth/v1/device/token` — CLI poll lấy token (RFC 8628 §3.4).
/// Body: `{ "device_code": "...", "grant_type": "urn:ietf:params:oauth:grant-type:device_code" }`.
async fn device_token(
    State(state): State<AppState>,
    Json(payload): Json<DeviceTokenRequest>,
) -> Response {
    if payload.grant_type != "urn:ietf:params:oauth:grant-type:device_code" {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            "Expected grant_type=urn:ietf:params:oauth:grant-type:device_code",
        );
    }
    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1,
    };

    let admin = admin(&state);
    match admin.poll_device_token(tenant_id, &payload.device_code).await {
        Ok(info) => {
            state.oauth_metrics.inc_access_token_issued();
            Json(DeviceTokenResponse {
                access_token:  info.access_token,
                refresh_token: info.refresh_token,
                token_type:    "Bearer".to_string(),
                expires_in:    8 * 3600,
                session_id:    info.session_id,
            }).into_response()
        }
        Err(e) => {
            let msg = format!("{e}");
            let (code, http) = match msg.as_str() {
                "authorization_pending" => ("authorization_pending", StatusCode::ACCEPTED),
                "slow_down"             => ("slow_down",             StatusCode::ACCEPTED),
                "authorization_expired" => ("expired_token",         StatusCode::GONE),
                "access_denied"         => ("access_denied",         StatusCode::FORBIDDEN),
                _                       => ("invalid_grant",         StatusCode::BAD_REQUEST),
            };
            oauth_error(http, code, &msg)
        }
    }
}

/// `POST /api/oauth/v1/token/refresh` — RFC 6749 §6.
/// Body: `{ "refresh_token": "..." }`. Trả access_token + refresh_token mới.
async fn token_refresh(
    State(state): State<AppState>,
    Json(payload): Json<RefreshRequest>,
) -> Response {

    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1,
    };

    let pool = state.connector.database(tenant_id);
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => return oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("acquire conn: {e}"),
        ),
    };

    let row = sqlx::query(
        "SELECT user_id, token_id, expires_at, revoked_at \
         FROM sys_user WHERE token_hash = ?1",
    )
    .bind(sha256_hex(payload.refresh_token.as_bytes()))
    .fetch_optional(&mut *conn)
    .await
    .ok()
    .flatten();

    let Some(row) = row else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "Refresh token not found");
    };
    let revoked: Option<String> = row.try_get(3).ok();
    if revoked.is_some() {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "Refresh token revoked");
    }

    // Sinh access_token mới, update sys_user
    let admin = admin(&state);
    let user_id: String = match row.try_get(0) {
        Ok(s) => s,
        Err(e) => return oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("read user_id: {e}"),
        ),
    };

    let new_access = match admin.insert_short_session(tenant_id, &user_id).await {
        Ok(s) => s,
        Err(e) => return oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("issue short session: {e}"),
        ),
    };
    state.oauth_metrics.inc_access_token_refreshed();

    Json(DeviceTokenResponse {
        access_token:  new_access.access_token,
        refresh_token: payload.refresh_token, // refresh token reuse (rotation là Phase sau)
        token_type:    "Bearer".to_string(),
        expires_in:    new_access.expires_in_secs,
        session_id:    Some(new_access.session_id),
    }).into_response()
}

/// `POST /api/oauth/v1/token/revoke` — RFC 7009.
async fn token_revoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<RevokeRequest>,
) -> Response {

    let user_id = match extract_user_id(&headers) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1,
    };

    let pool = state.connector.database(tenant_id);
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => return oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("acquire conn: {e}"),
        ),
    };

    let token_hash = sha256_hex(payload.token.as_bytes());
    let _ = sqlx::query(
        "UPDATE sys_user SET revoked_at = CURRENT_TIMESTAMP \
         WHERE tenant_id = ?1 AND user_id = ?2 AND token_hash = ?3 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(&user_id)
    .bind(&token_hash)
    .execute(&mut *conn)
    .await;

    ok(Some("token revoked"))
}

/// `POST /api/oauth/v1/session/issue` — sinh Ed25519 keypair.
async fn session_issue(
    State(state): State<AppState>,
    headers: HeaderMap,
    _body: Option<Json<SessionIssueRequest>>,
) -> Response {
    let user_id = match extract_user_id(&headers) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1,
    };

    let admin = admin(&state);
    match admin.issue_long_session(tenant_id, &user_id).await {
        Ok(info) => {
            state.oauth_metrics.inc_long_session_issued();
            Json(SessionIssueResponse {
                session_id:  info.session_id,
                private_key: info.private_key,
                expires_in:  info.expires_in_secs,
            }).into_response()
        }
        Err(e) => oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("issue_long_session failed: {e}"),
        ),
    }
}

/// `POST /api/oauth/v1/session/revoke` — revoke 1 long session.
async fn session_revoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SessionRevokeRequest>,
) -> Response {
    let user_id = match extract_user_id(&headers) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1,
    };

    let admin = admin(&state);
    match admin.revoke_long_session(tenant_id, &user_id, &payload.session_id).await {
        Ok(()) => ok(Some("session revoked")),
        Err(e) => oauth_error(
            StatusCode::NOT_FOUND,
            "invalid_request",
            &format!("{e}"),
        ),
    }
}

/// `GET /api/oauth/v1/session/list` — list long sessions của user.
async fn session_list(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let user_id = match extract_user_id(&headers) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let tenant_id: i64 = match state.variable("DEFAULT_TENANT_ID").await {
        Ok(id) => id,
        Err(_) => 1,
    };

    let admin = admin(&state);
    match admin.list_long_sessions(tenant_id, &user_id).await {
        Ok(items) => {
            let resp: Vec<SessionListEntry> = items
                .into_iter()
                .map(|s| SessionListEntry {
                    session_id:   s.session_id,
                    status:       s.status,
                    expires_at:   s.expires_at.to_rfc3339(),
                    last_used_at: s.last_used_at.map(|d| d.to_rfc3339()),
                    created_at:   s.created_at.to_rfc3339(),
                })
                .collect();
            Json(resp).into_response()
        }
        Err(e) => oauth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("list_long_sessions failed: {e}"),
        ),
    }
}

// Suppress unused-import warning khi chưa dùng hết
#[allow(dead_code)]
fn _unused(_: &str) {}
