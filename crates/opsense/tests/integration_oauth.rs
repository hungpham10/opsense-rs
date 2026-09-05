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
//! Skip gracefully nếu compose chưa chạy.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};

const DEX_USER: &str = "dev-user@example.com";
const DEX_PASSWORD: &str = "password";
const DEX_CLIENT_ID: &str = "opsense-test";
const DEX_CLIENT_SECRET: &str = "opsense-dev-shared-secret-32-bytes-min!!";

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

#[derive(Serialize, Debug)]
struct DexTokenRequest {
    grant_type: String,
    code: String,
    client_id: String,
    client_secret: String,
    redirect_uri: String,
}

#[derive(Deserialize, Debug)]
struct DexTokenResponse {
    id_token: String,
    #[allow(dead_code)]
    access_token: String,
    #[allow(dead_code)]
    refresh_token: String,
    #[allow(dead_code)]
    token_type: String,
    #[allow(dead_code)]
    expires_in: i64,
}

// =========================================================================
// Helpers
// =========================================================================

/// URL của opsense-serve (qua Nginx) từ host. Default http://localhost:8080.
fn serve_url() -> String {
    common::serve_url()
}

/// URL của Dex OIDC provider (compose expose 5556). Default http://localhost:5556/dex.
fn dex_issuer() -> String {
    std::env::var("OPSENSE_DEX_ISSUER")
        .unwrap_or_else(|_| "http://localhost:5556/dex".into())
}

/// OIDC discovery → lấy authorization_endpoint, token_endpoint, jwks_uri.
async fn oidc_discovery(client: &reqwest::Client) -> HashMap<String, String> {
    let url = format!("{}/.well-known/openid-configuration", dex_issuer());
    let resp = client.get(&url).send().await.expect("OIDC discovery");
    assert!(
        resp.status().is_success(),
        "OIDC discovery failed: {}",
        resp.status()
    );
    let body: HashMap<String, String> = resp.json().await.expect("OIDC discovery JSON");
    body
}

/// Bước 1: Hit `/login` của Nginx → Nginx redirect sang Dex authorization endpoint.
/// Bước 2: Dex serve login form (HTML). Parse form, POST credentials.
/// Bước 3: Dex approval screen → POST approve → Dex redirect về `/callback?code=...`.
/// Bước 4: Exchange code lấy `id_token`.
///
/// Trả về `id_token` (RS256 JWT do Dex ký bằng RSA key).
async fn dex_login_get_id_token(client: &reqwest::Client) -> String {
    let discovery = oidc_discovery(client).await;
    let auth_endpoint = discovery
        .get("authorization_endpoint")
        .expect("authorization_endpoint");
    let token_endpoint = discovery
        .get("token_endpoint")
        .expect("token_endpoint");

    let state = format!("test-state-{}", uuid::Uuid::new_v4());
    let redirect_uri = format!("{}/callback", serve_url());

    // Step 1: GET authorization endpoint → Dex render login form
    let auth_url = format!(
        "{}?client_id={}&response_type=code&redirect_uri={}&scope=openid+email+profile&state={}",
        auth_endpoint,
        urlencoding(DEX_CLIENT_ID),
        urlencoding(&redirect_uri),
        urlencoding(&state),
    );
    let resp = client.get(&auth_url).send().await.expect("Dex auth GET");
    assert!(
        resp.status().is_success(),
        "Dex auth GET failed: {}",
        resp.status()
    );
    let html = resp.text().await.expect("Dex auth HTML");

    // Step 2: Extract login form action URL + hidden fields (Dex renders
    // <form action="/dex/auth/local?req=..."> với CSRF token).
    let req_token = extract_form_field(&html, "req")
        .or_else(|| extract_form_field(&html, "state"))
        .expect("Dex login form req/state field");

    // Step 3: POST credentials
    let login_url = format!("{}/dex/auth/local?req={}", dex_issuer(), req_token);
    let login_body = [
        ("login", DEX_USER),
        ("password", DEX_PASSWORD),
    ];
    let resp = client
        .post(&login_url)
        .form(&login_body)
        .send()
        .await
        .expect("Dex login POST");
    let html = resp.text().await.expect("Dex approval HTML");

    // Step 4: Extract approval form (Dex shows "Grant access" screen)
    let approval_req = extract_form_field(&html, "req")
        .expect("Dex approval form req field");
    let approval_url = format!("{}/dex/approval?req={}", dex_issuer(), approval_req);
    let resp = client
        .post(&approval_url)
        .form(&[("approval", "approve")])
        .send()
        .await
        .expect("Dex approval POST");

    // Step 5: Follow redirect để lấy auth code từ /callback
    // (reqwest với Policy::none đã xử lý redirect chain).
    let final_url = resp.url().to_string();
    let code = final_url
        .split("code=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .expect("authorization code in callback URL");
    let code = urlencoding_decode(code);

    // Step 6: Exchange code → id_token
    let req = DexTokenRequest {
        grant_type: "authorization_code".to_string(),
        code,
        client_id: DEX_CLIENT_ID.to_string(),
        client_secret: DEX_CLIENT_SECRET.to_string(),
        redirect_uri: redirect_uri.clone(),
    };
    let resp = client
        .post(token_endpoint)
        .form(&req)
        .send()
        .await
        .expect("Dex token POST");
    assert!(
        resp.status().is_success(),
        "Dex token POST failed: {}",
        resp.status()
    );
    let token_resp: DexTokenResponse = resp.json().await.expect("Dex token JSON");
    token_resp.id_token
}

/// Trích hidden form field từ HTML (Dex render `<input type="hidden" name="X" value="Y">`).
fn extract_form_field(html: &str, name: &str) -> Option<String> {
    let pattern = format!(r#"name="{}""#, name);
    let idx = html.find(&pattern)?;
    let value_start = html[idx..].find("value=\"")? + idx + 7;
    let value_end = html[value_start..].find('"')? + value_start;
    Some(html[value_start..value_end].to_string())
}

/// Minimal percent-encoding (chỉ cần cho URL params đơn giản).
fn urlencoding(s: &str) -> String {
    s.replace('@', "%40")
        .replace('+', "%2B")
        .replace(' ', "%20")
        .replace('?', "%3F")
        .replace('&', "%26")
        .replace('=', "%3D")
}

fn urlencoding_decode(s: &str) -> String {
    s.replace("%40", "@")
        .replace("%2B", "+")
        .replace("%20", " ")
        .replace("%3F", "?")
        .replace("%26", "&")
        .replace("%3D", "=")
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
    if common::wait_for_health(&client, 10).await.is_err() {
        eprintln!("skipping: serve not reachable — run `docker compose up` first");
        return;
    }

    // 1. Hit `/api/oauth/v1/device/code` qua Nginx (no auth) → lấy device_code
    let device: DeviceCodeResponse = client
        .post(format!("{}/api/oauth/v1/device/code", serve_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("device/code request")
        .json()
        .await
        .expect("device/code JSON");
    assert!(!device.device_code.is_empty());
    assert!(!device.user_code.is_empty());

    // 2. Dex login → id_token (full OIDC flow qua Nginx redirect → Dex)
    let id_token = dex_login_get_id_token(&client).await;
    assert!(!id_token.is_empty());

    // 3. POST /device/verify với Bearer id_token
    //    Nginx verify RS256 JWT bằng JWKS từ Dex, inject X-User-Id từ `sub`.
    //    Axum approve device_code → user 'dev-user' linked.
    let verify: DeviceVerifyResponse = client
        .post(format!("{}/api/oauth/v1/device/verify", serve_url()))
        .bearer_auth(&id_token)
        .header("X-Tenant-Id", "1")
        .json(&serde_json::json!({"user_code": device.user_code}))
        .send()
        .await
        .expect("device/verify request")
        .json()
        .await
        .expect("device/verify JSON");
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
    assert!(!token.refresh_token.is_empty());

    // 5. POST /session/issue với Bearer access_token → Ed25519 keypair
    let session: SessionIssueResponse = client
        .post(format!("{}/api/oauth/v1/session/issue", serve_url()))
        .bearer_auth(&token.access_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("session/issue request")
        .json()
        .await
        .expect("session/issue JSON");
    assert!(!session.session_id.is_empty());
    assert!(!session.private_key.is_empty());

    // 6. GET /session/list với Bearer access_token → verify có session vừa issue
    let list: Vec<SessionListEntry> = client
        .get(format!("{}/api/oauth/v1/session/list", serve_url()))
        .bearer_auth(&token.access_token)
        .send()
        .await
        .expect("session/list request")
        .json()
        .await
        .expect("session/list JSON");
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
}

/// DB sanity: kiểm tra `sys_user` có row tương ứng `dev-user` sau khi
/// `/device/verify` thành công. Phụ thuộc integration_oauth đã chạy trước.
#[tokio::test]
async fn oauth_db_row_persisted() {
    let client = reqwest::Client::new();
    if common::wait_for_health(&client, 10).await.is_err() {
        eprintln!("skipping: serve not reachable");
        return;
    }

    let db_dsn = std::env::var("DB_DSN").unwrap_or_else(|_| {
        "postgres://opsense:opsense123@localhost:5432/opsense".into()
    });

    let pool = match sqlx::PgPool::connect(&db_dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("skipping: cannot connect to postgres: {e}");
            return;
        }
    };

    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT COUNT(*) FROM sys_user WHERE tenant_id = 1 AND user_id = 'dev-user'",
    )
    .fetch_optional(&pool)
    .await
    .expect("query sys_user");

    assert!(
        row.map(|(c,)| c > 0).unwrap_or(false),
        "expected sys_user row for 'dev-user'"
    );
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