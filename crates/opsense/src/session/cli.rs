//! `opsense session` subcommand.
//!
//! Workflow:
//! - `opsense session issue [--host URL]`
//!     POST /api/oauth/v1/session/issue (Bearer từ `OPSENSE_ACCESS_TOKEN` hoặc
//!     `~/.config/opsense/token`) → lưu `~/.config/opsense/sessions/<id>.json`.
//! - `opsense session list [--host URL]`
//!     In danh sách session trên serve (cũ + mới) + danh sách file trên disk.
//! - `opsense session revoke <session_id> [--host URL]`
//!     Revoke phía serve + xoá file local.
//! - `opsense session resolve <session_id>`
//!     In `private_key` ra stdout (để import sang máy khác thủ công).
//! - `opsense session import <session_id> <private_key> [--expires-at RFC3339]`
//!     Tạo file local từ thông tin import thủ công (không gọi serve).

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Utc};

use crate::client::session_api as api;
use crate::session::store;

const DEFAULT_HOST: &str = "http://127.0.0.1:8080";

#[derive(Debug, Clone)]
pub struct SessionCmd {
    pub action: SessionAction,
    pub host:   Option<String>,
}

#[derive(Debug, Clone)]
pub enum SessionAction {
    Issue,
    List,
    Revoke(String),
    Resolve(String),
    Import { session_id: String, private_key: String, expires_at: Option<String> },
}

pub fn run(cmd: SessionCmd) -> Result<()> {
    // Run async inside a fresh runtime — `main` cũng có runtime riêng nhưng ta
    // không muốn block khi gọi từ dispatcher khác (tests, future sub-shells).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build current-thread runtime")?;
    rt.block_on(run_async(cmd))
}

async fn run_async(cmd: SessionCmd) -> Result<()> {
    let host = resolve_host(cmd.host.as_deref());
    let bearer = load_bearer()?;

    match cmd.action {
        SessionAction::Issue => issue(&host, &bearer).await,
        SessionAction::List => list(&host, &bearer).await,
        SessionAction::Revoke(id) => revoke(&host, &bearer, &id).await,
        SessionAction::Resolve(id) => resolve(&id).await,
        SessionAction::Import { session_id, private_key, expires_at } => {
            import(session_id, private_key, expires_at)
        }
    }
}

fn resolve_host(explicit: Option<&str>) -> String {
    if let Some(h) = explicit {
        return h.to_string();
    }
    if let Ok(h) = std::env::var("OPSENSE_HOST") {
        let trimmed = h.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    DEFAULT_HOST.to_string()
}

fn load_bearer() -> Result<String> {
    if let Ok(t) = std::env::var("OPSENSE_ACCESS_TOKEN") {
        let trimmed = t.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::PathBuf::from(home)
            .join(".config")
            .join("opsense")
            .join("token");
        if let Ok(content) = std::fs::read_to_string(&path) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }
    bail!(
        "no Bearer token: set OPSENSE_ACCESS_TOKEN or run `opsense repl --endpoint <host>` to log in first"
    )
}

async fn issue(host: &str, bearer: &str) -> Result<()> {
    eprintln!("Requesting Ed25519 keypair from {host}/api/oauth/v1/session/issue …");
    let resp = api::issue_session(host, bearer).await?;

    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(resp.expires_in);

    let file = store::SessionFile {
        session_id:  resp.session_id.clone(),
        private_key: resp.private_key.clone(),
        expires_at,
        created_at:  now,
    };
    let path = store::save_session_to_disk(&file)?;

    eprintln!("Issued session:");
    println!("  session_id : {}", resp.session_id);
    println!("  expires_at : {expires_at}");
    println!("  saved to   : {}", path.display());
    eprintln!(
        "\nUse this session_id as `x-session-id` gRPC metadata.\n\
         Copy {} to the Runner host (or import via `opsense session import` there).",
        path.display()
    );
    Ok(())
}

async fn list(host: &str, bearer: &str) -> Result<()> {
    let remote = match api::list_sessions_remote(host, bearer).await {
        Ok(rs) => rs,
        Err(e) => {
            eprintln!("(warn) list remote failed: {e}");
            Vec::new()
        }
    };

    let local = store::list_sessions_on_disk()?;

    println!("{:<48}  {:<7}  {:<10}  {:<25}  {}",
        "session_id", "source", "status", "expires_at", "last_used_at");
    println!("{}", "-".repeat(110));
    for s in &remote {
        println!("{:<48}  {:<7}  {:<10}  {:<25}  {}",
            s.session_id, "remote", s.status, s.expires_at,
            s.last_used_at.as_deref().unwrap_or("-"));
    }
    for f in &local {
        println!("{:<48}  {:<7}  {:<10}  {:<25}  -",
            f.session_id, "local", "?", f.expires_at.to_rfc3339());
    }
    Ok(())
}

async fn revoke(host: &str, bearer: &str, session_id: &str) -> Result<()> {
    eprintln!("Revoking {session_id} on {host} …");
    api::revoke_session(host, bearer, session_id).await?;
    store::delete_session_from_disk(session_id)?;
    eprintln!("Revoked + removed local file.");
    Ok(())
}

async fn resolve(session_id: &str) -> Result<()> {
    let f = store::load_session_from_disk(session_id)
        .with_context(|| format!("session {session_id} not found on disk"))?;
    // Print private_key (base64) ra stdout — caller pipe sang máy khác.
    println!("{}", f.private_key);
    Ok(())
}

fn import(
    session_id: String,
    private_key: String,
    expires_at: Option<String>,
) -> Result<()> {
    // Validate private_key shape trước khi lưu.
    let pk_bytes = URL_SAFE_NO_PAD
        .decode(private_key.trim())
        .context("private_key not valid URL_SAFE_NO_PAD base64")?;
    if pk_bytes.len() != 32 {
        bail!(
            "private_key decoded to {} bytes, expected 32 (Ed25519 secret)",
            pk_bytes.len()
        );
    }
    let expires_at: DateTime<Utc> = match expires_at {
        Some(s) => DateTime::parse_from_rfc3339(&s)
            .with_context(|| format!("invalid --expires-at: {s}"))?
            .with_timezone(&Utc),
        None => Utc::now() + chrono::Duration::hours(8),
    };

    let file = store::SessionFile {
        session_id:  session_id.clone(),
        private_key,
        expires_at,
        created_at:  Utc::now(),
    };
    let path = store::save_session_to_disk(&file)?;
    eprintln!("Imported session {session_id} → {}", path.display());
    Ok(())
}
