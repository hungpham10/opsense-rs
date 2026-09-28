//! HTTP client tới serve (Runner → Serve).
//!
//! Gắn `Authorization: Bearer <admin_token>` vào mọi request.
//! Dùng cho cả `RemoteAuth::verify_signature` (cache miss → lookup
//! `/api/admin/v1/session/resolve`) và các fetch data API.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct ServeClient {
    base_url: String,
    admin_token: String,
    http: reqwest::Client,
}

impl ServeClient {
    /// Tạo client. `base_url` không có trailing slash (VD: `https://opsense.example.com`).
    pub fn new(base_url: String, admin_token: String, timeout_secs: u64) -> Result<Self> {
        if base_url.trim().is_empty() {
            anyhow::bail!("ServeClient base_url is empty");
        }
        // Cài crypto provider **trước khi** dựng client. `reqwest` chỉ tự chọn
        // provider khi `__rustls-ring` bật; workspace này cố ý tắt nó (xem
        // `Cargo.toml` của workspace) và ghim `aws-lc-rs`, nên không cài thì
        // `build()` panic `No provider set`.
        //
        // Binary `opsense` đã cài ở `main()`, nhưng crate này dùng lại được —
        // embedder không có `main` và sẽ chết ngay ở dòng này. Cài tại chỗ dựng
        // client giống `opsense-components::http::client_for`: idempotent, không
        // panic, và giữ provider đã có nếu ai cài trước.
        opsense_mlib::tls::install_default_crypto_provider();
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
        let resp = self
            .http
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
        let resp = self
            .http
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
        let path = if let Some(stripped) = path.strip_prefix('/') {
            stripped
        } else {
            path
        };
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

    /// Regression: `ServeClient::new` phải **tự** cài crypto provider.
    ///
    /// `reqwest` chỉ tự chọn provider khi `__rustls-ring` bật — ở build chỉ có
    /// `opsense-runner` thì nó tắt, nên không có lời gọi cài provider thì
    /// `Client::builder().build()` panic `No provider set`. Ở build cả workspace
    /// thì `object_store` (qua `mlib/parquet`) bật `ring` và che mất lỗi, nên
    /// `trims_trailing_slash` phía trên **không** bắt được hồi quy — assert này
    /// thì bắt: provider phải được cài **trong process này**, không phụ thuộc
    /// may mà có crate nào bật `ring` hay không.
    #[test]
    fn new_installs_a_crypto_provider_in_this_process() {
        let _client = ServeClient::new("http://x".into(), "t".into(), 30).expect("dựng client");
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "client dựng được mà provider chưa cài ⇒ đang chỉ may mà `__rustls-ring` bật"
        );
    }
}
