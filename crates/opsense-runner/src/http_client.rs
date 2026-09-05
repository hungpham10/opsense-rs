//! HTTP client tới serve (Runner → Serve).
//!
//! Gắn `Authorization: Bearer <admin_token>` vào mọi request.
//! Dùng cho cả `RemoteAuth::verify_signature` (cache miss → lookup
//! `/api/admin/v1/session/resolve`) và các fetch data API.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ServeClient {
    base_url:   String,
    admin_token: String,
    http:       reqwest::Client,
}

impl ServeClient {
    /// Tạo client. `base_url` không có trailing slash (VD: `https://opsense.example.com`).
    pub fn new(base_url: String, admin_token: String, timeout_secs: u64) -> Result<Self> {
        if base_url.trim().is_empty() {
            anyhow::bail!("ServeClient base_url is empty");
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            admin_token,
            http,
        })
    }

    /// URL gốc (không trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `GET <base>/<path>` kèm Bearer.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let resp = self.http
            .get(&url)
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("GET {url} returned non-2xx"))?
            .json::<T>()
            .await
            .with_context(|| format!("decode GET {url} response"))?;
        Ok(resp)
    }

    /// `POST <base>/<path>` kèm Bearer + JSON body.
    pub async fn post<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> Result<T> {
        let url = self.url(path);
        let resp = self.http
            .post(&url)
            .bearer_auth(&self.admin_token)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?
            .error_for_status()
            .with_context(|| format!("POST {url} returned non-2xx"))?
            .json::<T>()
            .await
            .with_context(|| format!("decode POST {url} response"))?;
        Ok(resp)
    }

    fn url(&self, path: &str) -> String {
        let path = if path.starts_with('/') { &path[1..] } else { path };
        format!("{}/{}", self.base_url, path)
    }
}

// =========================================================================
// DTOs cho `/api/admin/v1/session/resolve`
// =========================================================================

#[derive(Debug, Clone, Serialize)]
pub struct SessionResolveRequest<'a> {
    pub session_id: &'a str,
}

/// Response từ `POST /api/admin/v1/session/resolve`.
///
/// Serve chỉ trả `private_key` (base64). Nếu `active = false` thì
/// session không tồn tại / đã revoke / hết hạn.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SessionResolveResponse {
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub session_id: Option<String>,
    /// base64(32 bytes Ed25519 secret).
    #[serde(default)]
    pub private_key: Option<String>,
    #[serde(default)]
    pub expires_in: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_trailing_slash() {
        let c = ServeClient::new("https://x/".into(), "t".into(), 30).unwrap();
        assert_eq!(c.base_url(), "https://x");
    }

    #[test]
    fn empty_base_rejected() {
        assert!(ServeClient::new("".into(), "t".into(), 30).is_err());
    }
}
