use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use opsense_runner::{RunnerConfig, auth::LocalAuth};

/// Standalone runner: delegate to `opsense-runner`.
///
/// # Errors
/// Bind/serve failures or kernel backend construction failures.
pub async fn run(
    bind: SocketAddr,
    kernel_command: PathBuf,
    kernel_args: Vec<String>,
) -> std::io::Result<()> {
    let cfg = RunnerConfig {
        bind,
        kernel_command,
        kernel_args,
        ..RunnerConfig::default()
    };
    opsense_runner::run(
        bind,
        cfg,
        Some(Arc::new(LocalAuth::new())),
    )
    .await
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}
