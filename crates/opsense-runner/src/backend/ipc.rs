//! `IpcKernelBackend`: spawn one kernel process per session, drive via
//! framed stdio. The backend owns the process map itself so the
//! [`crate::session::SessionRegistry`] can layer auth + idle sweeper on
//! top without duplicating lifecycle logic.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result};
use opsense_proto::host::{ExecOutcome, KernelConnection};
use opsense_proto::pb::{CodeRequest, SessionParams};
use tokio::process::{Child, ChildStdin, ChildStdout};

use crate::backend::{HealthInfo, KernelBackend};
use crate::config::resolve_kernel_binary;

/// One spawned kernel process paired with its framed connection.
struct KernelProc {
    child: Child,
    conn: KernelConnection<ChildStdout, ChildStdin>,
}

impl Drop for KernelProc {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// IPC backend: spawns one kernel process per session.
pub struct IpcKernelBackend {
    command: PathBuf,
    args: Vec<String>,
    kernels: tokio::sync::Mutex<HashMap<String, KernelProc>>,
}

impl IpcKernelBackend {
    #[must_use]
    pub fn new(command: PathBuf, args: Vec<String>) -> Self {
        Self {
            command,
            args,
            kernels: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Build from environment: `OPSENSE_KERNEL` env var, then
    /// `target/debug/opsense-kernel-echo`, then `PATH`.
    #[must_use]
    pub fn from_env() -> Self {
        let cmd = std::env::var("OPSENSE_KERNEL")
            .ok()
            .and_then(|v| if v.is_empty() { None } else { Some(PathBuf::from(v)) })
            .unwrap_or_else(|| resolve_kernel_binary("opsense-kernel-echo"));
        Self::new(cmd, Vec::new())
    }
}

#[async_trait::async_trait]
impl KernelBackend for IpcKernelBackend {
    fn kind(&self) -> &'static str {
        "ipc"
    }

    async fn health(&self) -> Result<HealthInfo> {
        Ok(HealthInfo {
            name: self.kind().into(),
            ok: true,
            detail: format!("command {:?} args {:?}", self.command, self.args),
            packages: vec![],
        })
    }

    async fn start(&self, params: SessionParams) -> Result<String> {
        let mut child = tokio::process::Command::new(&self.command)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn kernel {:?}", self.command))?;
        let stdin = child.stdin.take().context("kernel stdin unavailable")?;
        let stdout = child.stdout.take().context("kernel stdout unavailable")?;
        let mut conn = KernelConnection::new(stdout, stdin);
        conn.handshake()
            .await
            .context("kernel handshake")?;
        let id = conn
            .start_session(params.clone())
            .await
            .context("starting kernel session")?;
        anyhow::ensure!(
            id == params.session_id,
            "kernel echoed session id {id} but host expected {}",
            params.session_id
        );
        self.kernels
            .lock()
            .await
            .insert(id.clone(), KernelProc { child, conn });
        Ok(id)
    }

    async fn execute(&self, session_id: &str, req: CodeRequest) -> Result<ExecOutcome> {
        let mut kernels = self.kernels.lock().await;
        let proc = kernels
            .get_mut(session_id)
            .with_context(|| format!("no kernel for session {session_id}"))?;
        Ok(proc.conn.execute(req).await?)
    }

    async fn interrupt(&self, session_id: &str, request_id: &str) -> Result<()> {
        let mut kernels = self.kernels.lock().await;
        let proc = kernels
            .get_mut(session_id)
            .with_context(|| format!("no kernel for session {session_id}"))?;
        Ok(proc.conn.interrupt(session_id, request_id).await?)
    }

    async fn close(&self, session_id: &str) -> Result<()> {
        let mut proc = self
            .kernels
            .lock()
            .await
            .remove(session_id)
            .with_context(|| format!("no kernel for session {session_id}"))?;
        let _ = proc.conn.close_session(session_id).await;
        let _ = proc.child.start_kill();
        let _ = proc.child.wait().await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let procs: Vec<KernelProc> = self.kernels.lock().await.drain().map(|(_, p)| p).collect();
        for mut proc in procs {
            let _ = proc.conn.close_session("").await;
            let _ = proc.child.start_kill();
            let _ = proc.child.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_proto::pb::{CodeRequest, SessionParams};

    #[tokio::test]
    async fn ipc_backend_kind_is_ipc() {
        // Note: this only asserts kind — spawning a real kernel is
        // covered by integration tests (need echo binary built).
        let backend = IpcKernelBackend::from_env();
        assert_eq!(backend.kind(), "ipc");
    }

    #[tokio::test]
    async fn ipc_backend_start_fails_without_kernel() {
        let backend = IpcKernelBackend::new(PathBuf::from("/nonexistent/kernel"), vec![]);
        let res = backend
            .start(SessionParams {
                session_id: "s1".into(),
                ..Default::default()
            })
            .await;
        assert!(res.is_err());
    }
}