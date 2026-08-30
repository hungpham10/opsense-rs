//! gRPC runner backend ([`GrpcRunnerBackend`]): talks to a remote
//! `opsense-runner` over the `KernelRunner` service. Drop-in implementation
//! of [`KernelBackend`] — sessions, REPL and MCP cannot tell it apart from
//! the local IPC backend (checklist §3/§5).

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use opsense_proto::host::ExecOutcome;
use opsense_proto::pb::kernel_runner_client::KernelRunnerClient;
use opsense_proto::pb::{
    exec_event, CloseRequest, CodeRequest, DatasetAck, DatasetChunk, DatasetHeader, HealthRequest,
    InterruptRequest, SessionParams,
};
use tonic::transport::Channel;

use crate::backend::{HealthInfo, KernelBackend};

pub struct GrpcRunnerBackend {
    addr: String,
    client: tokio::sync::Mutex<KernelRunnerClient<Channel>>,
}

impl GrpcRunnerBackend {
    /// Connect to a runner at `addr` (`host:port`, no scheme).
    ///
    /// # Errors
    /// Connection failures.
    pub async fn connect(addr: &str) -> Result<Self> {
        let endpoint = if addr.contains("://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        // Large dataset/result payloads ride single gRPC messages; lift the
        // 4 MiB default to match the framed protocol cap.
        const MAX_MESSAGE: usize = 256 * 1024 * 1024;
        let channel = Channel::from_shared(endpoint.clone())
            .with_context(|| format!("invalid runner endpoint {endpoint}"))?
            .connect()
            .await
            .with_context(|| format!("connecting to runner at {endpoint}"))?;
        let mut client = KernelRunnerClient::new(channel);
        client = client
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        Ok(Self {
            addr: addr.to_string(),
            client: tokio::sync::Mutex::new(client),
        })
    }

    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    async fn client(&self) -> tokio::sync::MutexGuard<'_, KernelRunnerClient<Channel>> {
        self.client.lock().await
    }
}

fn status_to_error(status: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!("runner {}: {}", status.code(), status.message())
}

#[async_trait]
impl KernelBackend for GrpcRunnerBackend {
    fn kind(&self) -> &'static str {
        "grpc-runner"
    }

    async fn health(&self) -> Result<HealthInfo> {
        let mut client = self.client().await;
        let status = client
            .health(HealthRequest {})
            .await
            .map_err(status_to_error)?
            .into_inner();
        Ok(HealthInfo {
            name: format!("{}@{}", status.kernel_name, self.addr),
            ok: status.ok,
            detail: status.detail,
            packages: status
                .packages
                .into_iter()
                .map(|p| format!("{} {}", p.name, p.version))
                .collect(),
        })
    }

    async fn start_session(&self, params: SessionParams) -> Result<String> {
        let mut client = self.client().await;
        let handle = client
            .start_session(params)
            .await
            .map_err(status_to_error)?
            .into_inner();
        Ok(handle.session_id)
    }

    async fn execute(&self, _session_id: &str, req: CodeRequest) -> Result<ExecOutcome> {
        let mut client = self.client().await;
        let mut stream = client
            .execute(req.clone())
            .await
            .map_err(status_to_error)?
            .into_inner();

        let mut outcome = ExecOutcome::default();
        loop {
            let event = tokio::time::timeout(
                std::time::Duration::from_millis(if req.timeout_ms > 0 {
                    req.timeout_ms
                } else {
                    u64::MAX
                }),
                stream.message(),
            )
            .await;
            let message = match event {
                Ok(message) => message.context("reading exec stream")?,
                Err(_elapsed) => {
                    // Mirror the local driver's timeout semantics.
                    return Ok(ExecOutcome {
                        timed_out: true,
                        ..ExecOutcome::default()
                    });
                }
            };
            let Some(event) = message else {
                anyhow::bail!("runner closed the exec stream");
            };
            match event.event {
                Some(exec_event::Event::Error(err)) => outcome.error = Some(err),
                Some(exec_event::Event::ResultValue(value)) => outcome.value = Some(value),
                Some(exec_event::Event::Done(true)) => return Ok(outcome),
                // stdout/stderr/artifact/dataframe events pass through.
                Some(other) => outcome.events.push(opsense_proto::pb::ExecEvent {
                    request_id: req.request_id.clone(),
                    event: Some(other),
                }),
                None => {}
            }
        }
    }

    async fn send_dataset(
        &self,
        session_id: &str,
        header: DatasetHeader,
        chunks: Vec<Bytes>,
    ) -> Result<DatasetAck> {
        anyhow::ensure!(!chunks.is_empty(), "send_dataset requires >= 1 chunk");
        let total = chunks.len();
        let session_id = session_id.to_string();
        let stream = chunks
            .into_iter()
            .enumerate()
            .map(move |(seq, payload)| DatasetChunk {
                session_id: session_id.clone(),
                dataset_ref: header.dataset_ref.clone(),
                seq: seq as u64,
                last: seq + 1 == total,
                arrow_ipc: payload.to_vec(),
            });

        let mut client = self.client().await;
        let ack = client
            .send_dataset(futures_util::stream::iter(stream))
            .await
            .map_err(status_to_error)?
            .into_inner();
        Ok(ack)
    }

    async fn interrupt(&self, session_id: &str, request_id: &str) -> Result<()> {
        let mut client = self.client().await;
        client
            .interrupt(InterruptRequest {
                session_id: session_id.to_string(),
                request_id: request_id.to_string(),
            })
            .await
            .map_err(status_to_error)?;
        Ok(())
    }

    async fn close_session(&self, session_id: &str) -> Result<()> {
        let mut client = self.client().await;
        client
            .close_session(CloseRequest {
                session_id: session_id.to_string(),
            })
            .await
            .map_err(status_to_error)?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        // Kernels are owned by the runner process; closing the channel is all
        // this side can do.
        Ok(())
    }
}
