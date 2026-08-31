//! Execution backend abstraction ([`KernelBackend`]) and the local IPC
//! implementation ([`LocalIpcBackend`]) that spawns a kernel process and talks
//! the framed stdio protocol from `opsense-proto`.
//!
//! One kernel process per session keeps isolation simple and lets a crashed
//! kernel take down only its own session — never the host (checklist §11).

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use arrow::compute::concat_batches;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use opsense_proto::host::{ExecOutcome, KernelConnection};
use opsense_proto::pb::{
    value, CodeRequest, DatasetAck, DatasetHeader, HealthStatus, SessionParams, Value,
};
use tokio::process::{Child, ChildStdin, ChildStdout};

/// Re-export the wire types so callers depend only on this crate.
pub use opsense_proto::pb;

/// How to spawn the kernel process for a [`LocalIpcBackend`].
#[derive(Debug, Clone)]
pub struct KernelConfig {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub allow_fs: bool,
    pub allow_net: bool,
    pub max_memory_mb: u64,
}

impl Default for KernelConfig {
    fn default() -> Self {
        // Default to the zero-runtime-dependency `echo` kernel so the host
        // starts without requiring a Python/Julia interpreter or their protobuf
        // bindings (which are not always available to build/install). An
        // explicit `OPSENSE_KERNEL` selects a different external kernel binary.
        // The legacy `OPSENSE_KERNEL_PYTHON` no longer overrides the default —
        // it only locates the Python kernel binary for the opt-in `:py` path
        // (see `SessionManager::python_backend`).
        let name = match std::env::var("OPSENSE_KERNEL") {
            Ok(v) => v,
            Err(_) => "opsense-kernel-echo".to_string(),
        };
        Self {
            command: crate::lifecycle::resolve_kernel_binary(&name, "OPSENSE_KERNEL"),
            args: vec![],
            allow_fs: false,
            allow_net: false,
            max_memory_mb: 2048,
        }
    }
}

impl KernelConfig {
    /// Build from an explicit command path (tests point this at the echo bin).
    #[must_use]
    pub fn for_command(command: impl Into<PathBuf>) -> Self {
        Self {
            command: command.into(),
            ..Self::default()
        }
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

/// Abstraction over an execution backend. Implemented by [`LocalIpcBackend`]
/// today; a gRPC runner client is a drop-in later (Phase 5) without touching
/// `Session`/`REPL`/MCP.
#[async_trait]
pub trait KernelBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn health(&self) -> Result<HealthInfo>;
    async fn start_session(&self, params: SessionParams) -> Result<String>;
    async fn execute(&self, session_id: &str, req: CodeRequest) -> Result<ExecOutcome>;
    async fn send_dataset(
        &self,
        session_id: &str,
        header: DatasetHeader,
        chunks: Vec<Bytes>,
    ) -> Result<DatasetAck>;
    async fn interrupt(&self, session_id: &str, request_id: &str) -> Result<()>;
    async fn close_session(&self, session_id: &str) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;
}

struct KernelProc {
    child: Child,
    conn: KernelConnection<ChildStdout, ChildStdin>,
}

impl Drop for KernelProc {
    fn drop(&mut self) {
        // Checklist §1: cleanup process on drop. Killing the child is enough;
        // waiting would block a Drop.
        let _ = self.child.start_kill();
    }
}

/// Local IPC backend: every session is a spawned kernel process reached over
/// framed stdio.
pub struct LocalIpcBackend {
    cfg: KernelConfig,
    // tokio Mutex: guards are held across awaits while an operation drives
    // its session's connection; sessions are independent so contention is nil.
    kernels: tokio::sync::Mutex<HashMap<String, KernelProc>>,
}

impl LocalIpcBackend {
    #[must_use]
    pub fn new(cfg: KernelConfig) -> Self {
        Self {
            cfg,
            kernels: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl KernelBackend for LocalIpcBackend {
    fn kind(&self) -> &'static str {
        "local-ipc"
    }

    async fn health(&self) -> Result<HealthInfo> {
        Ok(HealthInfo {
            name: self.kind().into(),
            ok: true,
            detail: format!("command {:?} args {:?}", self.cfg.command, self.cfg.args),
            packages: vec![],
        })
    }

    async fn start_session(&self, params: SessionParams) -> Result<String> {
        let mut child = tokio::process::Command::new(&self.cfg.command)
            .args(&self.cfg.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn kernel {:?}", self.cfg.command))?;
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

    async fn send_dataset(
        &self,
        session_id: &str,
        header: DatasetHeader,
        chunks: Vec<Bytes>,
    ) -> Result<DatasetAck> {
        let mut kernels = self.kernels.lock().await;
        let proc = kernels
            .get_mut(session_id)
            .with_context(|| format!("no kernel for session {session_id}"))?;
        proc.conn.send_dataset(header, chunks).await
    }

    async fn interrupt(&self, session_id: &str, request_id: &str) -> Result<()> {
        let mut kernels = self.kernels.lock().await;
        let proc = kernels
            .get_mut(session_id)
            .with_context(|| format!("no kernel for session {session_id}"))?;
        proc.conn.interrupt(session_id, request_id).await
    }

    async fn close_session(&self, session_id: &str) -> Result<()> {
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

/// In-process `echo` backend: no kernel process, no interpreter, no protobuf
/// bindings. It mirrors the external `opsense-kernel-echo` binary's behaviour
/// (returns the executed code prefixed with `echo: `) so the host starts with
/// zero runtime dependencies. This is the default backend unless an explicit
/// `OPSENSE_KERNEL` selects an external kernel (`init_session_manager`).
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

    async fn start_session(&self, params: SessionParams) -> Result<String> {
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

    async fn send_dataset(
        &self,
        _session_id: &str,
        header: DatasetHeader,
        _chunks: Vec<Bytes>,
    ) -> Result<DatasetAck> {
        Ok(DatasetAck {
            dataset_ref: header.dataset_ref,
            rows: header.rows,
            ok: true,
            error: String::new(),
        })
    }

    async fn interrupt(&self, _session_id: &str, _request_id: &str) -> Result<()> {
        Ok(())
    }

    async fn close_session(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Rows per ARROW frame when streaming a dataset into a kernel (~0.5 MB of
/// f64 columns); keeps any single frame well under wire limits without going
/// row-by-row.
pub const DATASET_CHUNK_ROWS: usize = 64_000;

/// Split a RecordBatch into N complete Arrow IPC stream segments of at most
/// [`DATASET_CHUNK_ROWS`] rows each. Small batches produce exactly one
/// segment; never row-by-row serialization (checklist §7).
///
/// # Errors
/// Arrow encode failures.
pub fn chunk_record_batch(rb: &RecordBatch) -> Result<Vec<Bytes>> {
    let total = rb.num_rows();
    let mut segments = Vec::with_capacity(total.div_ceil(DATASET_CHUNK_ROWS).max(1));
    let mut offset = 0;
    while offset < total {
        let len = DATASET_CHUNK_ROWS.min(total - offset);
        segments.push(record_batch_to_segment(&rb.slice(offset, len))?);
        offset += len;
    }
    if segments.is_empty() {
        segments.push(record_batch_to_segment(rb)?);
    }
    Ok(segments)
}

/// Encode one RecordBatch as a complete Arrow IPC stream segment.
///
/// # Errors
/// Arrow encode failures.
pub fn record_batch_to_segment(rb: &RecordBatch) -> Result<Bytes> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &rb.schema())?;
        writer.write(rb)?;
        writer.finish()?;
    }
    Ok(Bytes::from(buf))
}

/// Decode an Arrow IPC stream segment back to a RecordBatch.
///
/// # Errors
/// Arrow decode / empty-stream failures.
pub fn segment_to_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)?;
    let schema = reader.schema().clone();
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    match batches.len() {
        0 => Err(anyhow!("empty arrow stream segment")),
        1 => Ok(batches.remove(0)),
        _ => Ok(concat_batches(&schema, &batches)?),
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use opsense_proto::pb::value;

    #[tokio::test]
    async fn echo_backend_echoes_code_without_a_process() {
        let backend = EchoBackend;
        assert_eq!(backend.kind(), "echo");

        let outcome = backend
            .execute(
                "s1",
                CodeRequest {
                    request_id: "r1".into(),
                    session_id: "s1".into(),
                    code: "1 + 1".into(),
                    input_names: vec![],
                    timeout_ms: 1_000,
                },
            )
            .await
            .unwrap();

        let text = match outcome.value.unwrap().kind.unwrap() {
            value::Kind::Text(t) => t,
            other => panic!("unexpected value {other:?}"),
        };
        assert_eq!(text, "echo: 1 + 1");

        // Health/dataset/lifecycle are no-ops that still succeed.
        assert!(backend.health().await.unwrap().ok);
        let ack = backend
            .send_dataset(
                "s1",
                DatasetHeader {
                    session_id: "s1".into(),
                    dataset_ref: "@1".into(),
                    rows: 3,
                    cols: 1,
                    columns: vec!["n".into()],
                },
                vec![],
            )
            .await
            .unwrap();
        assert!(ack.ok);
        backend.close_session("s1").await.unwrap();
        backend.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn echo_backend_health_returns_ok() {
        let backend = EchoBackend;
        let info = backend.health().await.unwrap();
        assert!(info.ok);
        assert_eq!(info.name, "echo");
        assert!(info.packages.is_empty());
    }

    #[tokio::test]
    async fn echo_backend_start_session_returns_id() {
        let backend = EchoBackend;
        let id = backend
            .start_session(SessionParams {
                session_id: "my-session".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(id, "my-session");
    }

    #[tokio::test]
    async fn echo_backend_interrupt_is_noop() {
        let backend = EchoBackend;
        backend.interrupt("s1", "r1").await.unwrap();
    }

    #[tokio::test]
    async fn echo_backend_close_session_is_noop() {
        let backend = EchoBackend;
        backend.close_session("s1").await.unwrap();
        // even for unknown session ids
        backend.close_session("never-existed").await.unwrap();
    }

    #[test]
    fn kernel_config_default_uses_opsense_kernel_env() {
        // The default constructor calls resolve_kernel_binary which reads env
        // vars. Just exercise it to ensure no panic.
        let cfg = KernelConfig::default();
        assert!(!cfg.command.as_os_str().is_empty());
        assert_eq!(cfg.max_memory_mb, 2048);
        assert!(!cfg.allow_fs);
        assert!(!cfg.allow_net);
    }

    #[test]
    fn kernel_config_for_command_uses_given_path() {
        let cfg = KernelConfig::for_command("/tmp/my-kernel");
        assert_eq!(cfg.command, std::path::PathBuf::from("/tmp/my-kernel"));
    }

    #[test]
    fn datset_chunk_rows_constant_is_correct() {
        assert_eq!(DATASET_CHUNK_ROWS, 64_000);
    }

    fn make_batch(rows: usize) -> arrow::record_batch::RecordBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                std::sync::Arc::new(Float64Array::from(
                    (0..rows).map(|i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn record_batch_segment_roundtrip_preserves_rows() {
        let batch = make_batch(5);
        let bytes = record_batch_to_segment(&batch).unwrap();
        let back = segment_to_record_batch(&bytes).unwrap();
        assert_eq!(back.num_rows(), 5);
        assert_eq!(back.num_columns(), 2);
    }

    #[test]
    fn record_batch_to_segment_empty_batch_produces_valid_bytes() {
        let batch = make_batch(0);
        let bytes = record_batch_to_segment(&batch).unwrap();
        let back = segment_to_record_batch(&bytes);
        // empty arrow stream segment is treated as error (no batches).
        assert!(back.is_err());
    }

    #[test]
    fn segment_to_record_batch_garbage_bytes_returns_error() {
        let result = segment_to_record_batch(b"not a valid arrow stream");
        assert!(result.is_err());
    }

    #[test]
    fn chunk_record_batch_small_batch_is_one_segment() {
        let batch = make_batch(100);
        let segments = chunk_record_batch(&batch).unwrap();
        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn chunk_record_batch_exact_chunk_boundary() {
        let rows = DATASET_CHUNK_ROWS;
        let batch = make_batch(rows);
        let segments = chunk_record_batch(&batch).unwrap();
        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn chunk_record_batch_one_over_chunk_boundary_splits() {
        let rows = DATASET_CHUNK_ROWS + 1;
        let batch = make_batch(rows);
        let segments = chunk_record_batch(&batch).unwrap();
        assert_eq!(segments.len(), 2);
    }

    #[test]
    fn chunk_record_batch_multiple_chunks() {
        let rows = DATASET_CHUNK_ROWS * 3 + 100;
        let batch = make_batch(rows);
        let segments = chunk_record_batch(&batch).unwrap();
        assert_eq!(segments.len(), 4);
    }

    #[test]
    fn chunk_record_batch_empty_batch_returns_one_segment() {
        let batch = make_batch(0);
        let segments = chunk_record_batch(&batch).unwrap();
        // The implementation guarantees at least one segment.
        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn kernel_output_ok_returns_true_when_no_error() {
        let out = KernelOutput::default();
        assert!(out.ok());
    }

    #[test]
    fn kernel_output_ok_returns_false_when_error_present() {
        let out = KernelOutput { error: Some("boom".into()), ..Default::default() };
        assert!(!out.ok());
    }

    #[test]
    fn kernel_output_from_outcome_extracts_text() {
        use opsense_proto::pb::Value;
        let outcome = opsense_proto::host::ExecOutcome {
            value: Some(Value {
                kind: Some(value::Kind::Text("hello".into())),
            }),
            ..Default::default()
        };
        let out = KernelOutput::from_outcome(&outcome);
        assert_eq!(out.text.as_deref(), Some("hello"));
        assert_eq!(out.stdout, "");
        assert!(out.ok());
    }

    #[test]
    fn kernel_output_from_outcome_extracts_artifact_plot() {
        use opsense_proto::pb::{value, Artifact};
        let outcome = opsense_proto::host::ExecOutcome {
            value: Some(Value {
                kind: Some(value::Kind::Artifact(Artifact {
                    name: "plot".into(),
                    mime: "image/png".into(),
                    data: vec![1, 2, 3, 4],
                })),
            }),
            ..Default::default()
        };
        let out = KernelOutput::from_outcome(&outcome);
        assert_eq!(out.plots, vec![1, 2, 3, 4]);
    }

    #[test]
    fn kernel_output_from_outcome_ignores_non_image_artifact() {
        use opsense_proto::pb::{value, Artifact};
        let outcome = opsense_proto::host::ExecOutcome {
            value: Some(Value {
                kind: Some(value::Kind::Artifact(Artifact {
                    name: "data".into(),
                    mime: "text/plain".into(),
                    data: vec![1, 2, 3],
                })),
            }),
            ..Default::default()
        };
        let out = KernelOutput::from_outcome(&outcome);
        assert!(out.plots.is_empty());
        assert!(out.text.is_none());
    }

    #[test]
    fn kernel_output_from_outcome_extracts_dataframe() {
        use opsense_proto::pb::{value, DataFrame};
        let batch = make_batch(3);
        let mut buf = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
                .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let outcome = opsense_proto::host::ExecOutcome {
            value: Some(Value {
                kind: Some(value::Kind::Dataframe(DataFrame {
                    arrow_ipc: buf,
                    rows: batch.num_rows() as i64,
                    cols: batch.num_columns() as i64,
                    columns: batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect(),
                })),
            }),
            ..Default::default()
        };
        let out = KernelOutput::from_outcome(&outcome);
        let df = out.dataframe.expect("dataframe");
        assert_eq!(df.num_rows(), 3);
    }

    #[test]
    fn kernel_output_from_outcome_extracts_error() {
        use opsense_proto::pb::ErrorEvent;
        let outcome = opsense_proto::host::ExecOutcome {
            error: Some(ErrorEvent {
                kind: "syntax".into(),
                message: "bad code".into(),
            }),
            ..Default::default()
        };
        let out = KernelOutput::from_outcome(&outcome);
        assert_eq!(out.error.as_deref(), Some("syntax: bad code"));
        assert!(!out.ok());
    }

    #[test]
    fn kernel_output_from_outcome_handles_dataframe_with_invalid_ipc() {
        use opsense_proto::pb::{value, DataFrame};
        let outcome = opsense_proto::host::ExecOutcome {
            value: Some(Value {
                kind: Some(value::Kind::Dataframe(DataFrame {
                    arrow_ipc: vec![0xFFu8; 32],
                    rows: 0,
                    cols: 0,
                    columns: vec![],
                })),
            }),
            ..Default::default()
        };
        let out = KernelOutput::from_outcome(&outcome);
        // invalid IPC is silently dropped — dataframe stays None.
        assert!(out.dataframe.is_none());
    }

    #[test]
    fn health_info_from_health_status_maps_fields() {
        use opsense_proto::pb::{HealthStatus, PackageInfo};
        let status = HealthStatus {
            ok: true,
            kernel_name: "test-kernel".into(),
            kernel_version: "1.0.0".into(),
            packages: vec![
                PackageInfo {
                    name: "pandas".into(),
                    available: true,
                    version: "2.0".into(),
                },
                PackageInfo {
                    name: "numpy".into(),
                    available: true,
                    version: "1.24".into(),
                },
            ],
            detail: "all good".into(),
        };
        let info = HealthInfo::from(status);
        assert_eq!(info.name, "test-kernel");
        assert!(info.ok);
        assert_eq!(info.detail, "all good");
        assert_eq!(info.packages.len(), 2);
        assert!(info.packages[0].contains("pandas"));
    }
}
