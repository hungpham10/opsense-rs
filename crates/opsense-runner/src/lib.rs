//! Opsense runner: a standalone execution worker exposing the
//! [`opsense_proto::pb::kernel_runner_server::KernelRunner`] gRPC service.
//!
//! The runner is a thin translator: every gRPC call maps onto the same
//! [`KernelBackend`] the local path uses, so kernels keep speaking framed
//! stdio IPC and never see gRPC (checklist §4/§8). Sessions are kernel
//! processes owned by this process; a runner crash never takes the serve
//! gateway down and vice versa.

pub mod server;

use std::net::SocketAddr;

use anyhow::{Context, Result};

use opsense_proto::pb::kernel_runner_server::KernelRunnerServer;

/// The gRPC service handle: hosts mount this into their own tonic
/// [`tonic::transport::Server`] wherever they want (`routes`/serving code in
/// the opsense binary does exactly that); this crate only supplies the
/// service.
#[must_use]
pub fn kernel_runner_service(
    cfg: opsense_session::KernelConfig,
) -> KernelRunnerServer<server::RunnerService> {
    server::RunnerService::new(cfg).with_limits()
}

/// Standalone runner process: host the service until Ctrl-C, then release
/// every kernel it spawned.
///
/// # Errors
/// Bind/serve failures or kernel backend construction failures.
pub async fn run(bind: SocketAddr) -> Result<()> {
    let cfg = opsense_session::KernelConfig::default();
    tracing::info!(
        "opsense runner starting on {bind} (kernel: {:?})",
        cfg.command
    );
    server::serve(bind, cfg)
        .await
        .context("runner server failed")
}
