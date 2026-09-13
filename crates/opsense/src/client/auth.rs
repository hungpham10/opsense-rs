//! Device Authorization Grant client (RFC 8628) — gọi `/api/oauth/v1/*`.
//!
//! Flow:
//! 1. `request_device_code(host)` → `{ device_code, user_code, verification_uri }`
//! 2. In ra terminal cho user mở browser, nhập `user_code`.
//! 3. `poll_token(host, device_code)` lặp lại mỗi `interval` giây cho
//!    đến khi user duyệt. Trả `(access_token, refresh_token)`.
//!
//! Sau khi có token, ghi vào `~/.config/opsense/token` (đã có
//! `load_bearer_from_env` đọc lại khi `OpsenseClient::new`).

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Clone)]
pub struct DeviceCodeRequest {}

#[derive(Deserialize, Debug, Clone)]
pub struct DeviceCodeResponse {
    pub device_code:      String,
    pub user_code:        String,
    pub verification_uri: String,
    pub expires_in:       i64,
    pub interval:         i32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DeviceTokenResponse {
    pub access_token:  String,
    pub refresh_token: String,
    pub token_type:    String,
    pub expires_in:    i64,
}

#[derive(Deserialize, Debug, Clone)]
struct OAuthErrorBody {
    error: String,
    #[allow(dead_code)]
    error_description: String,
}

const POLL_TIMEOUT_SECS: u64 = 600; // 10 phút — match device_code expires_in

/// Bước 1: yêu cầu device_code từ `host` (ví dụ: `https://opsense.example.com`).
pub async fn request_device_code(host: &str) -> Result<DeviceCodeResponse> {
    let url = format!("{host}/api/oauth/v1/device/code");
    let resp = Client::new()
        .post(&url)
        .json(&DeviceCodeRequest {})
        .send()
        .await
        .context("POST /device/code")?
        .error_for_status()
        .context("/device/code returned non-2xx")?
        .json::<DeviceCodeResponse>()
        .await
        .context("decode device_code response")?;
    Ok(resp)
}

/// Bước 3: poll `/device/token` cho đến khi user duyệt (hoặc timeout).
pub async fn poll_token(
    host: &str,
    device_code: &str,
    interval_secs: u64,
) -> Result<DeviceTokenResponse> {
    let url = format!("{host}/api/oauth/v1/device/token");
    let client = Client::new();
    let start = std::time::Instant::now();
    let mut current_interval = interval_secs;

    loop {
        if start.elapsed().as_secs() > POLL_TIMEOUT_SECS {
            bail!("Timed out waiting for user authorization ({}s)", POLL_TIMEOUT_SECS);
        }

        let resp = client
            .post(&url)
            .json(&serde_json::json!({
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            }))
            .send()
            .await
            .context("POST /device/token")?;

        let status = resp.status();
        if status.is_success() {
            return resp
                .json::<DeviceTokenResponse>()
                .await
                .context("decode device_token response");
        }

        // Lỗi RFC 8628 §3.5:
        // - authorization_pending: tiếp tục poll
        // - slow_down: tăng interval +5s
        // - expired_token / access_denied: dừng
        let body: OAuthErrorBody = resp.json().await.unwrap_or(OAuthErrorBody {
            error: "unknown".into(),
            error_description: String::new(),
        });

        match body.error.as_str() {
            "authorization_pending" => {
                tokio::time::sleep(Duration::from_secs(current_interval)).await;
            }
            "slow_down" => {
                current_interval += 5;
                tokio::time::sleep(Duration::from_secs(current_interval)).await;
            }
            "expired_token" => bail!("Device code expired (user took too long)"),
            "access_denied" => bail!("User denied the authorization request"),
            other => bail!("Device flow error: {other}"),
        }
    }
}

/// Lưu access_token vào `~/.config/opsense/token`. File mode 0600.
pub fn save_token_to_disk(token: &str) -> Result<std::path::PathBuf> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow!("HOME not set"))?;
    let dir = std::path::PathBuf::from(home)
        .join(".config")
        .join("opsense");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create dir {}", dir.display()))?;

    let path = dir.join("token");
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    f.write_all(token.as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

/// Convenience: chạy full device flow và lưu token.
pub async fn login_and_save_token(host: &str) -> Result<String> {
    let info = request_device_code(host).await?;
    eprintln!(
        "Open this URL in your browser:\n  {}{}\n\nAnd enter code: {}\n",
        host, info.verification_uri, info.user_code
    );

    let token = poll_token(host, &info.device_code, info.interval as u64).await?;
    save_token_to_disk(&token.access_token)?;
    Ok(token.access_token)
}
