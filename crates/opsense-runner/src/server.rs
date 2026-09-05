//! gRPC service implementation: translate `KernelRunner` calls onto the
//! local IPC backend, layered with Ed25519 auth verify + implicit
//! keepalive.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use opsense_proto::pb::kernel_runner_server::KernelRunner;
use opsense_proto::pb::{
    Ack, CloseRequest, CodeRequest, ErrorEvent, ExecEvent, HealthRequest, HealthStatus,
    InterruptRequest, SessionHandle, SessionParams, VerifyRequest, VerifyResponse, exec_event,
    PingRequest, Pong,
};

use crate::auth::{Auth, AuthContext};
use crate::config::RunnerConfig;
use crate::session::SessionRegistry;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub use opsense_proto::pb;

type Grpc<T> = Result<Response<T>, Status>;
/// Server-streaming Execute response.
pub type ExecEventStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<ExecEvent, Status>> + Send>>;

fn internal(err: anyhow::Error) -> Status {
    Status::internal(format!("{err:#}"))
}

pub struct RunnerService {
    registry: Arc<SessionRegistry>,
    cfg: crate::RunnerConfig,
    auth: Option<Arc<dyn Auth>>,
}

/// gRPC message ceiling mirroring the framed-protocol cap (large datasets /
/// result DataFrames travel as single messages).
pub const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

impl RunnerService {
    #[must_use]
    pub fn new(
        registry: Arc<SessionRegistry>,
        cfg: crate::RunnerConfig,
        auth: Option<Arc<dyn Auth>>,
    ) -> Self {
        Self {
            registry,
            cfg,
            auth,
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

    async fn start(&self, request: Request<SessionParams>) -> Grpc<SessionHandle> {
        let auth_ctx = AuthContext::try_from(request.metadata()).ok();
        let params = request.into_inner();

        let id = self
            .registry
            .start(params.clone(), auth_ctx.as_ref())
            .await
            .map_err(internal)?;

        if params.require_challenge {
            let auth = self.auth.as_ref().ok_or_else(|| {
                internal(anyhow::anyhow!(
                    "require_challenge=true but runner has no auth backend"
                ))
            })?;

            let challenge = auth
                .create_challenge(&id)
                .await
                .map_err(internal)?;

            self.registry
                .attach_challenge(&id, challenge.plaintext.clone(), Some(params.requested_role.clone()))
                .await;

            Ok(Response::new(SessionHandle {
                session_id: id,
                challenge: challenge.ciphertext,
            }))
        } else {
            Ok(Response::new(SessionHandle {
                session_id: id,
                challenge: vec![],
            }))
        }
    }

    async fn verify(&self, request: Request<VerifyRequest>) -> Grpc<VerifyResponse> {
        let req = request.into_inner();
        match self
            .registry
            .verify_challenge(&req.session_id, &req.response)
            .await
        {
            Ok(Some(role)) => Ok(Response::new(VerifyResponse {
                ok: true,
                role,
                error: String::new(),
            })),
            Ok(None) => Ok(Response::new(VerifyResponse {
                ok: false,
                role: String::new(),
                error: "no pending challenge for this session".into(),
            })),
            Err(e) => Ok(Response::new(VerifyResponse {
                ok: false,
                role: String::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn execute(&self, request: Request<CodeRequest>) -> Grpc<Self::ExecuteStream> {
        let auth_ctx = AuthContext::try_from(request.metadata()).ok();
        let req = request.into_inner();
        let outcome = self
            .registry
            .execute(&req.session_id, req.clone(), auth_ctx.as_ref())
            .await
            .map_err(internal)?;
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

    async fn interrupt(&self, request: Request<InterruptRequest>) -> Grpc<Ack> {
        let auth_ctx = AuthContext::try_from(request.metadata()).ok();
        let req = request.into_inner();
        self.registry
            .interrupt(&req.session_id, &req.request_id, auth_ctx.as_ref())
            .await
            .map_err(internal)?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    async fn close(&self, request: Request<CloseRequest>) -> Grpc<Ack> {
        let auth_ctx = AuthContext::try_from(request.metadata()).ok();
        let req = request.into_inner();
        self.registry
            .close(&req.session_id, auth_ctx.as_ref())
            .await
            .map_err(internal)?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Grpc<Pong> {
        let auth_ctx = AuthContext::try_from(request.metadata()).ok();
        let req = request.into_inner();
        if let (Some(ctx), Some(auth)) = (auth_ctx, &self.auth)
            && auth
                .verify_signature(&ctx.session_id, "Ping", ctx.timestamp, ctx.nonce, &ctx.signature)
                .await
                .unwrap_or(false)
        {
            self.registry.touch(&req.session_id).await;
        }
        Ok(Response::new(Pong {
            alive: true,
            server_time: chrono::Utc::now().timestamp(),
        }))
    }

    async fn health(&self, _request: Request<HealthRequest>) -> Grpc<HealthStatus> {
        let info = self.registry.health().await.map_err(internal)?;
        Ok(Response::new(HealthStatus {
            ok: info.ok,
            kernel_name: format!("runner/{}", info.name),
            kernel_version: env!("CARGO_PKG_VERSION").into(),
            packages: vec![],
            detail: info.detail,
        }))
    }
}

impl RunnerService {
    #[must_use]
    pub fn registry(&self) -> &SessionRegistry {
        &self.registry
    }
}

/// Bind and serve until Ctrl-C (or `shutdown` fires); afterwards every kernel
/// process owned by this runner is released.
///
/// # Errors
/// Propagates bind/serve failures.
pub async fn serve(bind: SocketAddr, cfg: RunnerConfig, auth: Option<Arc<dyn Auth>>) -> Result<()> {
    let backend = Arc::new(crate::backend::IpcKernelBackend::from_env());
    let registry = Arc::new(SessionRegistry::new(backend, auth.clone(), cfg.clone()));
    let service = RunnerService::new(registry, cfg, auth);
    let result = tonic::transport::Server::builder()
        .add_service(pb::kernel_runner_server::KernelRunnerServer::new(service))
        .serve_with_shutdown(bind, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("runner shutting down");
        })
        .await;
    result?;
    Ok(())
}
