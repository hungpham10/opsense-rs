use std::net::SocketAddr;
use std::path::PathBuf;

use opsense_runner::{RunnerConfig, build_auth};

/// Standalone runner: delegate to `opsense-runner`.
///
/// - `kernel_command` / `kernel_args` truyền từ CLI thắng mọi config.
/// - Phần còn lại (bind nếu `None`, serve_url, admin_token, ...) đọc từ
///   `~/.config/opsense/runner.json` rồi apply env overrides.
/// - Auth: nếu `serve_url` + `admin_token` có → `RemoteAuth`; ngược lại
///   `LocalAuth`. Nếu `auth_enabled = false` thì `None` (mở).
///
/// # Errors
/// Bind/serve failures hoặc lỗi build auth backend.
pub async fn run(
    bind: Option<SocketAddr>,
    kernel_command: Option<PathBuf>,
    kernel_args: Vec<String>,
) -> anyhow::Result<()> {
    let mut cfg = RunnerConfig::load();
    // Apply CLI overrides (luôn thắng file + env).
    if let Some(b) = bind {
        cfg.bind = b;
    }
    if let Some(k) = kernel_command {
        cfg.kernel_command = k;
    }
    if !kernel_args.is_empty() {
        cfg.kernel_args = kernel_args;
    }

    let auth = build_auth(&cfg)?;
    let bind = cfg.bind;

    opsense_runner::run(bind, cfg, auth).await
}

/// gRPC health check for `opsense runner` command.
/// Connects to `bind` via tonic channel, calls KernelRunner::Health RPC, exits 0 on success.
pub async fn health_check(bind: SocketAddr) -> anyhow::Result<()> {
    use opsense_proto::pb::kernel_runner_client::KernelRunnerClient;
    use opsense_proto::pb::HealthRequest;

    let channel = tonic::transport::Channel::from_shared(format!("http://{bind}"))
        .map_err(|e| anyhow::anyhow!("invalid bind address {bind}: {e}"))?
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("failed to connect to runner at {bind}: {e}"))?;

    let mut client = KernelRunnerClient::new(channel);
    let request = tonic::Request::new(HealthRequest {});
    let _response = client
        .health(request)
        .await
        .map_err(|e| anyhow::anyhow!("health RPC failed: {e}"))?;

    Ok(())
}
