//! Integration test — full OIDC + OAuth2 Device Flow.
//!
//! Flow:
//!   Browser → Nginx (lua-resty-openidc) → Dex (OIDC) → callback
//!   → Bearer JWT qua Nginx → Axum (opsense-serve qua UDS)
//!   → /api/oauth/v1/device/{code,verify,token,session/issue}
//!
//! Test approach: dùng reqwest + cookie jar để drive Dex login form, lấy
//! `id_token`, sau đó đi qua device flow thật. Cover toàn bộ Nginx →
//! lua-resty-openidc → UDS → Axum chain.
//!
//! In integration mode (`CI=true`), failure panics so the workflow
//! cannot silently go green. On local dev without compose, skip gracefully.

mod common;

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::redirect::Policy;
use serde::Deserialize;

/// Wait for serve to be healthy. Returns `true` if ready, `false` to skip.
async fn ensure_serve(client: &reqwest::Client) -> bool {
    match common::wait_for_health(client, 10).await {
        Ok(()) => true,
        Err(_) if common::integration_mode() => {
            panic!("serve not reachable — CI requires `docker compose up`")
        }
        Err(_) => {
            eprintln!("skipping: serve not reachable — run `docker compose up` first");
            false
        }
    }
}

/// Wait for Dex to be reachable. Returns `true` if ready, `false` to skip.
async fn ensure_dex() -> bool {
    match common::wait_for_dex(5).await {
        Ok(()) => true,
        Err(_) if common::integration_mode() => {
            panic!("Dex not reachable — CI requires `docker compose up`")
        }
        Err(_) => {
            eprintln!("skipping: Dex not reachable — check OPSENSE_DEX_ISSUER or run `docker compose up` first");
            false
        }
    }
}

/// Wait for DB to be reachable. Returns `true` if ready, `false` to skip.
async fn ensure_db() -> bool {
    let db_dsn = std::env::var("DB_DSN").unwrap_or_else(|_| {
        "postgres://opsense:opsense123@localhost:5432/opsense".into()
    });
    match sqlx::PgPool::connect(&db_dsn).await {
        Ok(_) => true,
        Err(e) if common::integration_mode() => {
            panic!("cannot connect to postgres — CI requires DB: {e}")
        }
        Err(e) => {
            eprintln!("skipping: cannot connect to postgres: {e}");
            false
        }
    }
}

// =========================================================================
// Response types
// =========================================================================

#[derive(Deserialize, Debug)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    #[allow(dead_code)]
    verification_uri: String,
    #[allow(dead_code)]
    expires_in: i64,
    #[allow(dead_code)]
    interval: i32,
}

#[derive(Deserialize, Debug)]
struct DeviceVerifyResponse {
    #[allow(dead_code)]
    user_id: String,
    status: String,
}

#[derive(Deserialize, Debug)]
struct DeviceTokenResponse {
    access_token: String,
    refresh_token: String,
    #[allow(dead_code)]
    token_type: String,
    #[allow(dead_code)]
    expires_in: i64,
    #[allow(dead_code)]
    session_id: Option<String>,
}

#[derive(Deserialize, Debug)]
struct SessionIssueResponse {
    session_id: String,
    private_key: String,
    #[allow(dead_code)]
    expires_in: i64,
}

#[derive(Deserialize, Debug)]
struct SessionListEntry {
    session_id: String,
}

// =========================================================================
// Helpers
// =========================================================================

/// URL của opsense-serve (qua Nginx) từ host. Default http://localhost:8080.
fn serve_url() -> String {
    common::serve_url()
}

// =========================================================================
// Tests
// =========================================================================

#[tokio::test]
async fn oauth_full_flow_dex_nginx_axum() {
    // Client chấp nhận redirect tự động (cho Dex flow).
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .redirect(Policy::limited(10))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("reqwest client");

    // Skip gracefully nếu compose chưa chạy.
    if !ensure_serve(&client).await {
        return;
    }
    if !ensure_dex().await {
        return;
    }

    // 1. Hit `/api/oauth/v1/device/code` qua Nginx (no auth) → lấy device_code
    let resp = client
        .post(format!("{}/api/oauth/v1/device/code", serve_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("device/code request");

    // Check status trước khi deserialize — in body nếu lỗi để debug
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        panic!("device/code returned {status}: {body}");
    }

    let device: DeviceCodeResponse = resp.json().await.expect("device/code JSON");
    assert!(!device.device_code.is_empty());
    assert!(!device.user_code.is_empty());

    // 2. Dex login → id_token (full OIDC flow qua Nginx redirect → Dex)
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    assert!(!id_token.is_empty());

    // 3. POST /device/verify với Bearer id_token
    //    Nginx verify RS256 JWT bằng JWKS từ Dex, inject X-User-Id từ `sub`.
    //    Axum approve device_code → user 'dev-user' linked.
    let verify_resp = client
        .post(format!("{}/api/oauth/v1/device/verify", serve_url()))
        .bearer_auth(&id_token)
        .header("X-Tenant-Id", "1")
        .json(&serde_json::json!({"user_code": device.user_code}))
        .send()
        .await
        .expect("device/verify request");
    let verify_status = verify_resp.status();
    let verify_body = verify_resp.text().await.expect("device/verify body");
    if !verify_status.is_success() {
        panic!("device/verify returned {verify_status}: {verify_body}");
    }
    let verify: DeviceVerifyResponse =
        serde_json::from_str(&verify_body).expect("device/verify JSON");
    assert_eq!(verify.status, "approved");

    // 4. Poll /device/token → access_token + refresh_token
    let mut attempts = 0;
    let token = loop {
        attempts += 1;
        let resp = client
            .post(format!("{}/api/oauth/v1/device/token", serve_url()))
            .json(&serde_json::json!({
                "device_code": device.device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            }))
            .send()
            .await
            .expect("device/token request");

        if resp.status().is_success() {
            break resp.json::<DeviceTokenResponse>().await.expect("device/token JSON");
        }
        if attempts >= 12 {
            panic!("device/token poll exhausted ({} attempts)", attempts);
        }
        // authorization_pending → 202 Accepted (giữ interval 5s)
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    assert!(!token.access_token.is_empty());
    assert!(token.access_token.starts_with("abt_"), "access_token phải có prefix abt_ để Nginx introspect được");
    assert!(!token.refresh_token.is_empty());

    // 5. POST /session/issue với Bearer access_token → Ed25519 keypair
    let issue_resp = client
        .post(format!("{}/api/oauth/v1/session/issue", serve_url()))
        .bearer_auth(&token.access_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("session/issue request");
    let issue_status = issue_resp.status();
    let issue_body = issue_resp.text().await.expect("session/issue body");
    if !issue_status.is_success() {
        panic!("session/issue returned {issue_status}: {issue_body}");
    }
    let session: SessionIssueResponse =
        serde_json::from_str(&issue_body).expect("session/issue JSON");
    assert!(!session.session_id.is_empty());
    assert!(!session.private_key.is_empty());

    // 6. GET /session/list với Bearer access_token → verify có session vừa issue
    let list_resp = client
        .get(format!("{}/api/oauth/v1/session/list", serve_url()))
        .bearer_auth(&token.access_token)
        .send()
        .await
        .expect("session/list request");
    let list_status = list_resp.status();
    let list_body = list_resp.text().await.expect("session/list body");
    if !list_status.is_success() {
        panic!("session/list returned {list_status}: {list_body}");
    }
    let list: Vec<SessionListEntry> =
        serde_json::from_str(&list_body).expect("session/list JSON");
    assert!(
        list.iter().any(|s| s.session_id == session.session_id),
        "session_id not in list: {:?}",
        list
    );

    // 7. POST /session/revoke → cleanup
    let resp = client
        .post(format!("{}/api/oauth/v1/session/revoke", serve_url()))
        .bearer_auth(&token.access_token)
        .json(&serde_json::json!({"session_id": session.session_id}))
        .send()
        .await
        .expect("session/revoke request");
    assert!(resp.status().is_success());

    // 8. DB sanity: /device/verify ở bước 3 phải ghi row sys_user.
    //    Nginx inject X-User-Id từ `id_token.sub` (sub của Dex là chuỗi
    //    opaque, KHÔNG phải username/email), nên assert trên chính sub đó.
    let sub = id_token
        .split('.')
        .nth(1)
        .map(|p| URL_SAFE_NO_PAD.decode(p))
        .and_then(|r| r.ok())
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v.get("sub").and_then(|s| s.as_str()).map(String::from))
        .expect("decode sub from id_token");
    assert!(!sub.is_empty());

    if !ensure_db().await {
        return;
    }
    let db_dsn = std::env::var("DB_DSN").unwrap_or_else(|_| {
        "postgres://opsense:opsense123@localhost:5432/opsense".into()
    });
    let pool = sqlx::PgPool::connect(&db_dsn).await.expect("connect pool");
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM sys_user WHERE tenant_id = 1 AND user_id = $1",
    )
    .bind(&sub)
    .fetch_one(&pool)
    .await
    .expect("query sys_user");
    assert!(count > 0, "expected sys_user row for sub '{sub}'");
}

/// Smoke test: verify JWT signature helper compile được (sanity cho test deps).
#[test]
fn jwt_smoke_compile() {
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({"sub": "test"}),
        &EncodingKey::from_secret(b"smoke"),
    )
    .expect("encode");
    assert!(token.split('.').count() == 3);
}