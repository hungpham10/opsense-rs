//! Opsense runner: a standalone execution worker exposing the
//! [`opsense_proto::pb::kernel_runner_server::KernelRunner`] gRPC service.
//!
//! The runner is a thin translator: every gRPC call maps onto the same
//! [`KernelBackend`] the local path uses, so kernels keep speaking framed
//! stdio IPC and never see gRPC (checklist §4/§8). Sessions are kernel
//! processes owned by this process; a runner crash never takes the serve
//! gateway down and vice versa.
//!
//! # Phase 4 — Auth
//!
//! Every RPC carries Ed25519 metadata (`x-session-id`, `x-timestamp`,
//! `x-nonce`, `x-signature`). The [`auth::LocalAuth`] implementation
//! verifies in-process; `resolve_private_key` returns `None` until the
//! future phase wires the server-API lookup.
//!
//! # Implicit keepalive
//!
//! Every RPC with a `session_id` calls `SessionRegistry::touch()` —
//! callers never need an explicit `:session ping`.

pub mod auth;
pub mod backend;
pub mod config;
pub mod server;
pub mod session;

pub use auth::{Auth, AuthContext, LocalAuth};
pub use backend::{EchoBackend, HealthInfo, KernelBackend, KernelOutput, LocalBackend};
pub use config::{RunnerConfig, resolve_kernel_binary};
pub use server::{RunnerService, serve};
pub use session::{SessionMeta, SessionRegistry};

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use opsense_proto::pb::kernel_runner_server::KernelRunnerServer;

/// The gRPC service handle: hosts mount this into their own tonic
/// [`tonic::transport::Server`] wherever they want (`serve` does
/// exactly that); this crate only supplies the service.
#[must_use]
pub fn kernel_runner_service(
    registry: Arc<SessionRegistry>,
    cfg: RunnerConfig,
    auth: Option<Arc<dyn Auth>>,
) -> KernelRunnerServer<RunnerService> {
    RunnerService::new(registry, cfg, auth).with_limits()
}

/// Standalone runner process: host the service until Ctrl-C, then release
/// every kernel it spawned. Auth is optional — pass `None` for open
/// deployments or `Some(auth)` for tests / custom auth.
///
/// # Errors
/// Bind/serve failures or kernel backend construction failures.
pub async fn run(
    bind: SocketAddr,
    cfg: RunnerConfig,
    auth: Option<Arc<dyn Auth>>,
) -> Result<()> {
    tracing::info!(
        "opsense runner starting on {bind} (kernel: {:?})",
        cfg.kernel_command
    );
    server::serve(bind, cfg, auth).await
}
