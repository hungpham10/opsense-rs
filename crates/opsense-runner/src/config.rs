//! Runner configuration knobs.

use std::net::SocketAddr;
use std::path::PathBuf;

/// Knobs for the runner standalone process and the embedded service.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// gRPC bind address (host:port).
    pub bind: SocketAddr,

    /// Kernel binary to spawn per session.
    pub kernel_command: PathBuf,

    /// Extra arguments passed to the kernel binary.
    pub kernel_args: Vec<String>,

    /// Idle window before the background sweeper kills a kernel process.
    pub idle_timeout_secs: u64,

    /// Sweeper period — how often the registry scans for stale sessions.
    pub sweep_interval_secs: u64,

    /// gRPC message ceiling (mirrors the framed-protocol cap).
    pub max_message_size: usize,

    /// Whether to verify Ed25519 signatures on incoming RPCs.
    pub auth_enabled: bool,

    // ---- Service-Account (cross-machine) ----
    /// Serve base URL (VD: `https://opsense.example.com`). Khi set + có
    /// `admin_token` thì Runner dùng `RemoteAuth` (verify cache miss thì
    /// hỏi serve `/api/admin/v1/session/resolve`).
    pub serve_url: Option<String>,

    /// Admin Bearer gắn vào mọi request tới serve.
    pub admin_token: Option<String>,

    /// Timeout HTTP mặc định (giây) khi gọi serve.
    pub serve_http_timeout_secs: u64,

    /// Cache size (số session_id) cho public_key LRU.
    pub pubkey_cache_capacity: usize,
}

/// File JSON load cùng tên nếu tồn tại (`~/.config/opsense/runner.json`).
/// Các field đều optional; nếu thiếu thì dùng default + env override.
#[derive(Debug, Default, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RunnerConfigFile {
    bind:                    Option<String>,
    kernel:                  Option<String>,
    #[serde(default)]
    kernel_args:             Vec<String>,
    idle_timeout_secs:       Option<u64>,
    sweep_interval_secs:     Option<u64>,
    serve_url:               Option<String>,
    admin_token:             Option<String>,
    serve_http_timeout_secs: Option<u64>,
    pubkey_cache_capacity:   Option<usize>,
}

impl RunnerConfig {
    /// Đường dẫn config mặc định: `$OPSENSE_RUNNER_CONFIG` hoặc
    /// `$HOME/.config/opsense/runner.json`. Trả `None` nếu không có.
    pub fn default_config_path() -> Option<PathBuf> {
        if let Ok(p) = std::env::var("OPSENSE_RUNNER_CONFIG") {
            let trimmed = p.trim();
            if !trimmed.is_empty() {
                return Some(PathBuf::from(trimmed));
            }
        }
        let home = std::env::var_os("HOME")?;
        Some(
            PathBuf::from(home)
                .join(".config")
                .join("opsense")
                .join("runner.json"),
        )
    }

    /// Load từ file JSON nếu có, sau đó apply env overrides.
    /// Trả về `Self` với `auth_enabled = true` và các field cross-machine
    /// (`serve_url`, `admin_token`) lấy từ file / env.
    pub fn load() -> Self {
        let mut cfg = Self::default();
        if let Some(path) = Self::default_config_path() {
            if let Ok(body) = std::fs::read(&path) {
                match serde_json::from_slice::<RunnerConfigFile>(&body) {
                    Ok(file) => {
                        if let Some(b) = file.bind {
                            if let Ok(addr) = b.parse() {
                                cfg.bind = addr;
                            }
                        }
                        if let Some(k) = file.kernel {
                            cfg.kernel_command = resolve_kernel_binary(&k);
                        }
                        if !file.kernel_args.is_empty() {
                            cfg.kernel_args = file.kernel_args;
                        }
                        if let Some(v) = file.idle_timeout_secs { cfg.idle_timeout_secs = v; }
                        if let Some(v) = file.sweep_interval_secs { cfg.sweep_interval_secs = v; }
                        if let Some(v) = file.serve_url { cfg.serve_url = Some(v); }
                        if let Some(v) = file.admin_token { cfg.admin_token = Some(v); }
                        if let Some(v) = file.serve_http_timeout_secs { cfg.serve_http_timeout_secs = v; }
                        if let Some(v) = file.pubkey_cache_capacity { cfg.pubkey_cache_capacity = v; }
                    }
                    Err(e) => {
                        eprintln!("warn: failed to parse {}: {e}", path.display());
                    }
                }
            }
        }
        // Env overrides (luôn thắng file).
        if let Ok(v) = std::env::var("OPSENSE_RUNNER_BIND") {
            if let Ok(addr) = v.parse() {
                cfg.bind = addr;
            }
        }
        if let Ok(v) = std::env::var("OPSENSE_KERNEL") {
            cfg.kernel_command = resolve_kernel_binary(&v);
        }
        if let Ok(v) = std::env::var("OPSENSE_SERVE_URL") {
            cfg.serve_url = Some(v);
        }
        if let Ok(v) = std::env::var("OPSENSE_ADMIN_TOKEN") {
            cfg.admin_token = Some(v);
        }
        cfg
    }
}

impl Default for RunnerConfig {
    fn default() -> Self {
        let name = std::env::var("OPSENSE_KERNEL").unwrap_or_else(|_| "opsense-kernel-echo".into());
        Self {
            bind: "0.0.0.0:50051".parse().unwrap(),
            kernel_command: resolve_kernel_binary(&name),
            kernel_args: Vec::new(),
            idle_timeout_secs: 1800,
            sweep_interval_secs: 30,
            max_message_size: 256 * 1024 * 1024,
            auth_enabled: true,
            serve_url: None,
            admin_token: None,
            serve_http_timeout_secs: 30,
            pubkey_cache_capacity: 1024,
        }
    }
}

/// Resolve a kernel binary: env var → sibling of current exe → workspace target → PATH.
///
/// Mirrors `opsense-session::lifecycle::resolve_kernel_binary` (now removed).
#[must_use]
pub fn resolve_kernel_binary(name: &str) -> PathBuf {
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let candidate = dir.join(name);
        if candidate.exists() {
            return candidate;
        }
    }
    let local = std::path::Path::new("target/debug").join(name);
    if local.exists() {
        return local;
    }
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_parses() {
        let cfg = RunnerConfig::default();
        assert_eq!(cfg.bind.port(), 50051);
        assert!(cfg.auth_enabled);
    }

    #[test]
    fn file_overrides_bind_and_serve() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runner.json");
        std::fs::write(&path, br#"{
            "bind": "127.0.0.1:60000",
            "serve_url": "https://example.com",
            "admin_token": "abt_xxx"
        }"#).unwrap();
        // SAFETY: tests trong crate này không chạy song song với nhau về env var.
        unsafe { std::env::set_var("OPSENSE_RUNNER_CONFIG", &path); }
        let cfg = RunnerConfig::load();
        unsafe { std::env::remove_var("OPSENSE_RUNNER_CONFIG"); }

        assert_eq!(cfg.bind.port(), 60000);
        assert_eq!(cfg.serve_url.as_deref(), Some("https://example.com"));
        assert_eq!(cfg.admin_token.as_deref(), Some("abt_xxx"));
    }
}
