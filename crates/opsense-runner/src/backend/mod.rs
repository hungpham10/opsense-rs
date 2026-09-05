//! Execution backend abstraction.
//!
//! The runner owns one [`KernelBackend`] and routes every gRPC call through it.
//! `IpcKernelBackend` spawns one kernel process per session and drives it via
//! the framed stdio protocol from `opsense-proto`.

use std::collections::HashMap;
use std::process::Stdio;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use opsense_proto::host::KernelConnection;
use opsense_proto::pb::{CodeRequest, HealthStatus, SessionParams, Value, value};
use tokio::process::{Child, ChildStdin, ChildStdout};

mod arrow;
mod ipc;

pub use arrow::{
    DATASET_CHUNK_ROWS, chunk_record_batch, record_batch_to_segment, segment_to_record_batch,
};
pub use ipc::IpcKernelBackend;

/// Re-export the wire types so callers depend only on this crate.
pub use opsense_proto::pb;
pub use opsense_proto::host::ExecOutcome;

/// Connection-layer health snapshot.
#[derive(Debug, Clone)]
pub struct HealthInfo {
    pub name: String,
    pub ok: bool,
    pub detail: String,
    pub packages: Vec<String>,
}

impl From<HealthStatus> for HealthInfo {
    fn from(h: HealthStatus) -> Self {
        Self {
            name: h.kernel_name,
            ok: h.ok,
            detail: h.detail,
            packages: h
                .packages
                .into_iter()
                .map(|p| format!("{} {}", p.name, p.version))
                .collect(),
        }
    }
}

/// Abstraction over an execution backend. The runner only ever talks to one
/// backend through this trait.
#[async_trait]
pub trait KernelBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn health(&self) -> Result<HealthInfo>;
    async fn start(&self, params: SessionParams) -> Result<String>;
    async fn execute(&self, session_id: &str, req: CodeRequest) -> Result<ExecOutcome>;
    async fn interrupt(&self, session_id: &str, request_id: &str) -> Result<()>;
    async fn close(&self, session_id: &str) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;
}

/// In-process `echo` backend: no kernel process, no interpreter, no protobuf
/// bindings. It mirrors the external `opsense-kernel-echo` binary so unit tests
/// and the runner's own e2e can run without spawning anything.
pub struct EchoBackend;

#[async_trait]
impl KernelBackend for EchoBackend {
    fn kind(&self) -> &'static str {
        "echo"
    }

    async fn health(&self) -> Result<HealthInfo> {
        Ok(HealthInfo {
            name: "echo".into(),
            ok: true,
            detail: "in-process echo backend".into(),
            packages: vec![],
        })
    }

    async fn start(&self, params: SessionParams) -> Result<String> {
        Ok(params.session_id)
    }

    async fn execute(&self, _session_id: &str, req: CodeRequest) -> Result<ExecOutcome> {
        Ok(ExecOutcome {
            value: Some(Value {
                kind: Some(value::Kind::Text(format!("echo: {}", req.code))),
            }),
            ..Default::default()
        })
    }

    async fn interrupt(&self, _session_id: &str, _request_id: &str) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Captured result of one execution, host-side (REPL/MCP agnostic).
#[derive(Debug, Default)]
pub struct KernelOutput {
    pub stdout: String,
    pub dataframe: Option<RecordBatch>,
    pub text: Option<String>,
    pub plots: Vec<u8>,
    pub error: Option<String>,
}

impl KernelOutput {
    /// Build from a raw [`ExecOutcome`].
    #[must_use]
    pub fn from_outcome(outcome: &ExecOutcome) -> Self {
        let mut dataframe = None;
        let mut text = None;
        let mut plots = Vec::new();
        if let Some(value) = &outcome.value {
            use opsense_proto::pb::value::Kind;
            match &value.kind {
                Some(Kind::Dataframe(df)) => {
                    dataframe = segment_to_record_batch(&df.arrow_ipc).ok()
                }
                Some(Kind::Text(t)) => text = Some(t.clone()),
                Some(Kind::Artifact(a)) if a.mime.contains("image") => plots = a.data.clone(),
                _ => {}
            }
        }
        Self {
            stdout: outcome.stdout(),
            dataframe,
            text,
            plots,
            error: outcome
                .error
                .as_ref()
                .map(|e| format!("{}: {}", e.kind, e.message)),
        }
    }

    #[must_use]
    pub fn ok(&self) -> bool {
        self.error.is_none()
    }
}

struct KernelProc {
    child: Child,
    conn: KernelConnection<ChildStdout, ChildStdin>,
}

impl Drop for KernelProc {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Local IPC backend: every session is a spawned kernel process reached over
/// framed stdio.
pub struct LocalBackend {
    command: std::path::PathBuf,
    args: Vec<String>,
    kernels: tokio::sync::Mutex<HashMap<String, KernelProc>>,
}

impl LocalBackend {
    #[must_use]
    pub fn new(command: std::path::PathBuf, args: Vec<String>) -> Self {
        Self {
            command,
            args,
            kernels: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl KernelBackend for LocalBackend {
    fn kind(&self) -> &'static str {
        "local-ipc"
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
        conn.handshake().await.context("kernel handshake")?;
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
        proc.conn.execute(req).await
    }

    async fn interrupt(&self, session_id: &str, request_id: &str) -> Result<()> {
        let mut kernels = self.kernels.lock().await;
        let proc = kernels
            .get_mut(session_id)
            .with_context(|| format!("no kernel for session {session_id}"))?;
        proc.conn.interrupt(session_id, request_id).await
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
