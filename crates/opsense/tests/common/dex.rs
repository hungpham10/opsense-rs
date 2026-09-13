//! Shared Dex OIDC login helper cho integration tests.
//!
// Module được include bởi nhiều test binary — không phải binary nào cũng
// dùng hết mọi helper, nên tắt dead_code cho cả module.
#![allow(dead_code)]
//!
//! Flow (qua Nginx lua-resty-openidc → Dex):
//!   1. GET authorization endpoint → Dex render login form
//!   2. POST credentials → approval screen
//!   3. POST approve → redirect `/callback?code=...`
//!   4. Exchange code → `id_token` (RS256 JWT, Nginx verify bằng JWKS)

use std::time::Duration;

use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};

pub const DEX_USER: &str = "dev-user@example.com";
pub const DEX_PASSWORD: &str = "password";
pub const DEX_CLIENT_ID: &str = "opsense-test";
pub const DEX_CLIENT_SECRET: &str = "opsense-dev-shared-secret-32-bytes-min!!";

/// Client dùng cho Dex login flow (cookie store + auto redirect).
pub fn login_client() -> reqwest::Client {
    reqwest::Client::builder()
        .cookie_store(true)
        .redirect(Policy::limited(10))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("reqwest client")
}

/// URL của Dex OIDC provider (compose expose 5556). Default http://localhost:5556/dex.
pub fn dex_issuer() -> String {
    std::env::var("OPSENSE_DEX_ISSUER")
        .unwrap_or_else(|_| "http://localhost:5556/dex".into())
}

/// OIDC discovery → lấy authorization_endpoint, token_endpoint, jwks_uri.
///
/// Một số field trong `openid-configuration` là array (vd:
/// `scopes_supported`, `response_types_supported`, …), nên ta parse vào
/// `serde_json::Value` thay vì `HashMap<String, String>` để tránh lỗi
/// "invalid type: sequence, expected a string" từ reqwest decoder.
pub async fn oidc_discovery(client: &reqwest::Client) -> serde_json::Value {
    let url = format!("{}/.well-known/openid-configuration", dex_issuer());
    let resp = client.get(&url).send().await.expect("OIDC discovery");
    assert!(
        resp.status().is_success(),
        "OIDC discovery failed: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.expect("OIDC discovery JSON");
    body
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
        access_token: String,
    // Dex chỉ cấp refresh_token khi request scope `offline_access` — helper
    // này chỉ cần id_token nên field để Option.
            refresh_token: Option<String>,
        token_type: String,
        expires_in: i64,
}

/// Bước 1: Hit `/login` của Nginx → Nginx redirect sang Dex authorization endpoint.
/// Bước 2: Dex serve login form (HTML). Parse form, POST credentials.
/// Bước 3: Dex approval screen → POST approve → Dex redirect về `/callback?code=...`.
/// Bước 4: Exchange code lấy `id_token`.
///
/// Trả về `id_token` (RS256 JWT do Dex ký bằng RSA key).
pub async fn dex_login_get_id_token(client: &reqwest::Client) -> String {
    let discovery = oidc_discovery(client).await;
    let auth_endpoint = discovery
        .get("authorization_endpoint")
        .and_then(|v| v.as_str())
        .expect("authorization_endpoint");
    let token_endpoint = discovery
        .get("token_endpoint")
        .and_then(|v| v.as_str())
        .expect("token_endpoint");

    let state = format!("test-state-{}", uuid::Uuid::new_v4());
    let redirect_uri = format!("{}/callback", super::serve_url());

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

    // Step 2: Extract login form action URL (Dex render
    // `<form action="/dex/auth/local/login?back=&state=...">` — token nằm
    // trong query của action, không phải hidden input). Fallback: hidden
    // field `req`/`state` (Dex bản cũ) rồi POST credentials tới action đó.
    let login_url = match extract_form_action(&html) {
        Some(action) => absolute_url(&dex_issuer(), &action),
        None => {
            let req_token = extract_form_field(&html, "req")
                .or_else(|| extract_form_field(&html, "state"))
                .unwrap_or_else(|| {
                    panic!(
                        "Dex login form req/state field not found; HTML:\n{}",
                        &html[..html.len().min(4000)]
                    )
                });
            format!("{}/auth/local?req={}", dex_issuer(), req_token)
        }
    };
    // Forward toàn bộ hidden input của form (CSRF token `_csrf` ở Dex mới —
    // thiếu nó Dex render lại trang login và flow chết âm thầm).
    let hidden = extract_hidden_fields(&html);
    let mut login_body: Vec<(&str, &str)> = hidden
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    login_body.push(("login", DEX_USER));
    login_body.push(("password", DEX_PASSWORD));
    let resp = client
        .post(&login_url)
        .form(&login_body)
        .send()
        .await
        .expect("Dex login POST");
    // Login POST trả 303 → /dex/approval?req=...&hmac=... (theo Policy::limited).
    // The approval URL chứa `hmac` bắt buộc — nếu thiếu Dex 500. Vì form
    // approval không có `action` attribute, an toàn nhất là dùng URL mà
    // reqwest vừa follow tới (chứa đầy đủ query).
    let approval_landing_url = resp.url().to_string();
    let status = resp.status();
    let html = resp.text().await.expect("Dex approval HTML");
    assert!(
        status.is_success(),
        "Dex login POST failed: {status}; HTML:\n{}",
        &html[..html.len().min(2000)]
    );

    // Step 4: Extract approval form (Dex shows "Grant access" screen).
    // Ưu tiên `action` nếu form có; nếu không, POST về chính URL landing
    // (chứa `hmac`) — không tự dựng lại `/dex/approval?req=...` vì thiếu
    // `hmac` Dex 500.
    let approval_url = match extract_form_action(&html) {
        Some(action) => absolute_url(&dex_issuer(), &action),
        None => approval_landing_url,
    };
    let hidden = extract_hidden_fields(&html);
    let mut approval_body: Vec<(&str, &str)> = hidden
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    // Form đầu tiên (approve) đã có hidden `approval=approve`. Fallback khi
    // form không có: thêm thủ công.
    if !approval_body.iter().any(|(n, _)| *n == "approval") {
        approval_body.push(("approval", "approve"));
    }
    let resp = client
        .post(&approval_url)
        .form(&approval_body)
        .send()
        .await
        .expect("Dex approval POST");

    // Step 5: Follow redirect để lấy auth code từ /callback
    // (reqwest với Policy::none đã xử lý redirect chain).
    let status = resp.status();
    let final_url = resp.url().to_string();
    let body = resp.text().await.expect("Dex approval final body");
    let code = final_url
        .split("code=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .unwrap_or_else(|| {
            panic!(
                "authorization code in callback URL; status={status}; final_url={final_url}; body:\n{}",
                &body[..body.len().min(2000)]
            )
        });
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

/// Trích URL của form đầu tiên trong HTML (`<form ... action="...">`).
fn extract_form_action(html: &str) -> Option<String> {
    let idx = html.find(r#"action=""#)? + 8;
    let end = html[idx..].find('"')? + idx;
    // HTML entity trong query (VD `back=&amp;state=...`)
    Some(html[idx..end].replace("&amp;", "&"))
}

///Resolve action tương đối (`/dex/...`) thành absolute URL dựa trên origin
/// của Dex issuer.
fn absolute_url(issuer: &str, action: &str) -> String {
    if action.starts_with("http") {
        return action.to_string();
    }
    let (scheme, rest) = issuer.split_once("://").unwrap_or(("http", issuer));
    let host = rest.split('/').next().unwrap_or(rest);
    format!("{}://{}{}", scheme, host, action)
}

/// Trích hidden input `(name, value)` từ form ĐẦU TIÊN trong HTML.
/// Cắt tại `</form>` đầu tiên để tránh lấy field của các form khác
/// (VD form approve + form reject trên trang approval của Dex).
fn extract_hidden_fields(html: &str) -> Vec<(String, String)> {
    let first_form_end = html.find("</form>").unwrap_or(html.len());
    let form_html = &html[..first_form_end];
    let mut out = Vec::new();
    let mut rest = form_html;
    while let Some(pos) = rest.find(r#"type="hidden""#) {
        let tail = &rest[pos..];
        let Some(nidx) = tail.find(r#"name=""#) else {
            break;
        };
        let nstart = nidx + 6;
        let Some(nend) = tail[nstart..].find('"') else {
            break;
        };
        let name = &tail[nstart..nstart + nend];
        let after_name = nstart + nend + 1;
        let Some(vrel) = tail[after_name..].find(r#"value=""#) else {
            break;
        };
        let vstart = after_name + vrel + 7;
        let Some(vend) = tail[vstart..].find('"') else {
            break;
        };
        out.push((name.to_string(), tail[vstart..vstart + vend].to_string()));
        rest = &tail[vstart + vend..];
    }
    out
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
