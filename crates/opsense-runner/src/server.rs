//! gRPC service implementation: translate `KernelRunner` calls onto the
//! local kernel backend.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use opsense_proto::pb::kernel_runner_server::KernelRunner;
use opsense_proto::pb::{
    exec_event, Ack, CloseRequest as PbCloseRequest, CodeRequest, DatasetAck, DatasetChunk,
    DatasetHeader, ErrorEvent, ExecEvent, HealthRequest, HealthStatus, InterruptRequest,
    SessionHandle, SessionParams,
};
use opsense_session::{KernelBackend, KernelConfig, LocalIpcBackend};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub use opsense_proto::pb;

type Grpc<T> = Result<Response<T>, Status>;
/// Server-streaming Execute response.
pub type ExecEventStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<ExecEvent, Status>> + Send>>;

fn internal(err: anyhow::Error) -> Status {
    // Preserve the root cause verbatim in the status message (checklist §11).
    Status::internal(format!("{err:#}"))
}

pub struct RunnerService {
    backend: Arc<LocalIpcBackend>,
}

/// gRPC message ceiling mirroring the framed-protocol cap (large datasets /
/// result DataFrames travel as single messages).
pub const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

impl RunnerService {
    #[must_use]
    pub fn new(cfg: KernelConfig) -> Self {
        Self {
            backend: Arc::new(LocalIpcBackend::new(cfg)),
        }
    }

    /// The service wrapped with matching message-size limits, ready to mount.
    #[must_use]
    pub fn with_limits(self) -> opsense_proto::pb::kernel_runner_server::KernelRunnerServer<Self> {
        opsense_proto::pb::kernel_runner_server::KernelRunnerServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE)
    }
}

#[tonic::async_trait]
impl KernelRunner for RunnerService {
    type ExecuteStream = ExecEventStream;

    async fn start_session(&self, request: Request<SessionParams>) -> Grpc<SessionHandle> {
        let params = request.into_inner();
        let id = self.backend.start_session(params).await.map_err(internal)?;
        Ok(Response::new(SessionHandle { session_id: id }))
    }

    async fn execute(&self, request: Request<CodeRequest>) -> Grpc<Self::ExecuteStream> {
        let req = request.into_inner();
        let outcome = self
            .backend
            .execute(&req.session_id, req.clone())
            .await
            .map_err(internal)?;
        // The connection driver folds `done` into `events`; re-order so the
        // stream always ends with done after any result/error payloads.
        let mut events: Vec<Result<ExecEvent, Status>> = outcome
            .events
            .into_iter()
            .filter(|event| !matches!(event.event, Some(exec_event::Event::Done(_))))
            .map(Ok)
            .collect();
        let push = |events: &mut Vec<Result<ExecEvent, Status>>, event: exec_event::Event| {
            events.push(Ok(ExecEvent {
                request_id: req.request_id.clone(),
                event: Some(event),
            }));
        };
        if let Some(v) = outcome.value {
            push(&mut events, exec_event::Event::ResultValue(v));
        }
        if outcome.timed_out {
            push(
                &mut events,
                exec_event::Event::Error(ErrorEvent {
                    kind: "timeout".into(),
                    message: "host deadline elapsed; interrupt delivered".into(),
                }),
            );
        }
        if let Some(err) = outcome.error {
            push(&mut events, exec_event::Event::Error(err));
        }
        push(&mut events, exec_event::Event::Done(true));

        let (tx, rx) = tokio::sync::mpsc::channel(events.len().max(1));
        tokio::spawn(async move {
            for event in events {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn send_dataset(
        &self,
        request: Request<tonic::Streaming<DatasetChunk>>,
    ) -> Grpc<DatasetAck> {
        let mut stream = request.into_inner();
        let mut session_id = String::new();
        let mut dataset_ref = String::new();
        let mut chunks: Vec<bytes::Bytes> = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk: DatasetChunk = chunk.map_err(|e| Status::invalid_argument(e.to_string()))?;
            if session_id.is_empty() {
                session_id = chunk.session_id;
                dataset_ref = chunk.dataset_ref;
            }
            chunks.push(chunk.arrow_ipc.into());
        }
        if chunks.is_empty() {
            return Err(Status::invalid_argument(
                "SendDataset requires at least one chunk",
            ));
        }

        let header = DatasetHeader {
            session_id,
            dataset_ref,
            rows: 0, // the kernel reports actual row counts in the ack
            cols: 0,
            columns: vec![],
        };
        let session_id = header.session_id.clone();
        let ack = self
            .backend
            .send_dataset(&session_id, header, chunks)
            .await
            .map_err(internal)?;
        Ok(Response::new(ack))
    }

    async fn interrupt(&self, request: Request<InterruptRequest>) -> Grpc<Ack> {
        let req = request.into_inner();
        self.backend
            .interrupt(&req.session_id, &req.request_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    async fn close_session(&self, request: Request<PbCloseRequest>) -> Grpc<Ack> {
        let req = request.into_inner();
        self.backend
            .close_session(&req.session_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    async fn health(&self, _request: Request<HealthRequest>) -> Grpc<HealthStatus> {
        let info = self.backend.health().await.map_err(internal)?;
        Ok(Response::new(HealthStatus {
            ok: info.ok,
            kernel_name: format!("runner/{}", info.name),
            kernel_version: env!("CARGO_PKG_VERSION").into(),
            packages: vec![],
            detail: info.detail,
        }))
    }
}

/// Bind and serve until Ctrl-C (or `shutdown` fires); afterwards every kernel
/// process owned by this runner is released.
///
/// # Errors
/// Propagates bind/serve failures.
pub async fn serve(bind: SocketAddr, cfg: KernelConfig) -> Result<()> {
    let service = RunnerService::new(cfg);
    let shutdown_backend = Arc::clone(&service.backend);
    let result = tonic::transport::Server::builder()
        .add_service(pb::kernel_runner_server::KernelRunnerServer::new(service))
        .serve_with_shutdown(bind, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("runner shutting down");
        })
        .await;
    shutdown_backend.shutdown().await?;
    result?;
    Ok(())
}
