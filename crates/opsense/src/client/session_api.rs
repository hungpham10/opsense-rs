//! Service Account (long session) client — gọi `/api/oauth/v1/session/*`.
//!
//! Keypair Ed25519 do serve mint. Client (REPL) chỉ nhận về `private_key`,
//! lưu vào `~/.config/opsense/sessions/<session_id>.json` (mode 0600) rồi
//! dùng để ký request gRPC tới Runner.
//!
//! Runner verify Ed25519 bằng `public_key` có sẵn trong `session_id`
//! (= base64(public_key)); cache miss thì gọi `resolve_session` để derive
//! lại public_key từ private_key.

use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Response từ `POST /api/oauth/v1/session/issue`.
#[derive(Deserialize, Debug, Clone)]
pub struct IssueSessionResponse {
    pub session_id:  String,
    pub private_key: String,
    pub expires_in:  i64,
}

/// Response từ `GET /api/oauth/v1/session/list`.
#[derive(Deserialize, Debug, Clone)]
pub struct SessionListEntry {
    pub session_id:   String,
    pub status:       String,
    pub expires_at:   String,
    pub last_used_at: Option<String>,
    pub created_at:   String,
}

/// Request body cho `POST /api/oauth/v1/session/revoke`.
#[derive(Serialize, Debug, Clone)]
struct RevokeSessionRequest<'a> {
    session_id: &'a str,
}

/// Lưu trên disk: `~/.config/opsense/sessions/<session_id>.json` (mode 0600).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionFile {
    pub session_id:  String,
    pub private_key: String,
    /// ISO 8601 / RFC 3339.
    pub expires_at:  DateTime<Utc>,
    pub created_at:  DateTime<Utc>,
}

/// Trả về `~/.config/opsense/sessions/`.
pub fn sessions_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME not set"))?;
    let dir = PathBuf::from(home)
        .join(".config")
        .join("opsense")
        .join("sessions");
    std::fs::create_dir_all(&dir).with_context(|| format!("create dir {}", dir.display()))?;
    Ok(dir)
}

fn session_file_path(session_id: &str) -> Result<PathBuf> {
    Ok(sessions_dir()?.join(format!("{session_id}.json")))
}

pub fn save_session_to_disk(s: &SessionFile) -> Result<PathBuf> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let path = session_file_path(&s.session_id)?;
    let body = serde_json::to_vec_pretty(s).context("encode session file")?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    f.write_all(&body)
        .with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

pub fn load_session_from_disk(session_id: &str) -> Result<SessionFile> {
    let path = session_file_path(session_id)?;
    let body = std::fs::read(&path)
        .with_context(|| format!("read session file {}", path.display()))?;
    let s: SessionFile = serde_json::from_slice(&body)
        .with_context(|| format!("decode session file {}", path.display()))?;
    Ok(s)
}

pub fn list_sessions_on_disk() -> Result<Vec<SessionFile>> {
    let dir = sessions_dir()?;
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let body = std::fs::read(&path).ok();
        if let Some(body) = body {
            if let Ok(s) = serde_json::from_slice::<SessionFile>(&body) {
                out.push(s);
            }
        }
    }
    Ok(out)
}

pub fn delete_session_from_disk(session_id: &str) -> Result<()> {
    let path = session_file_path(session_id)?;
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("remove {}", path.display()))?;
    }
    Ok(())
}

/// POST /api/oauth/v1/session/issue
pub async fn issue_session(host: &str, bearer: &str) -> Result<IssueSessionResponse> {
    let url = format!("{host}/api/oauth/v1/session/issue");
    let resp = Client::new()
        .post(&url)
        .bearer_auth(bearer)
        .send()
        .await
        .context("POST /session/issue")?
        .error_for_status()
        .context("/session/issue returned non-2xx")?
        .json::<IssueSessionResponse>()
        .await
        .context("decode session/issue response")?;
    // Validate shape: private_key phải là base64 decode được thành 32 bytes.
    let pk_bytes = URL_SAFE_NO_PAD
        .decode(&resp.private_key)
        .context("private_key not valid URL_SAFE_NO_PAD base64")?;
    if pk_bytes.len() != 32 {
        bail!(
            "private_key decoded to {} bytes, expected 32 (Ed25519 secret)",
            pk_bytes.len()
        );
    }
    Ok(resp)
}

/// GET /api/oauth/v1/session/list
pub async fn list_sessions_remote(host: &str, bearer: &str) -> Result<Vec<SessionListEntry>> {
    let url = format!("{host}/api/oauth/v1/session/list");
    let resp = Client::new()
        .get(&url)
        .bearer_auth(bearer)
        .send()
        .await
        .context("GET /session/list")?
        .error_for_status()
        .context("/session/list returned non-2xx")?
        .json::<Vec<SessionListEntry>>()
        .await
        .context("decode session/list response")?;
    Ok(resp)
}

/// POST /api/oauth/v1/session/revoke
pub async fn revoke_session(host: &str, bearer: &str, session_id: &str) -> Result<()> {
    let url = format!("{host}/api/oauth/v1/session/revoke");
    let resp = Client::new()
        .post(&url)
        .bearer_auth(bearer)
        .json(&RevokeSessionRequest { session_id })
        .send()
        .await
        .context("POST /session/revoke")?
        .error_for_status()
        .context("/session/revoke returned non-2xx")?;
    let _ = resp.bytes().await;
    Ok(())
}
