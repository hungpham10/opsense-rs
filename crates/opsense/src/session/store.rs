//! On-disk session file storage.
//!
//! Lưu trữ tại `~/.config/opsense/sessions/<session_id>.json` (mode 0600).
//! HTTP API thì sống ở `crate::client::session_api`.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
