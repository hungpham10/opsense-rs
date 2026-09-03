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