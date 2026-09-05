//! End-to-end over real gRPC: spin up the runner service backed by the echo
//! kernel binary and drive it with the generated client — proving the full
//! serve ↔ runner ↔ kernel chain minus the Python kernel itself.
//!
//! Phase 4: tests the new design — auth (disabled for these tests via
//! `None`), `Start`/`Execute`/`Interrupt`/`Close`/`Ping`/`Health` RPCs,
//! implicit keepalive. `send_dataset` is removed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use opsense_proto::pb::kernel_runner_client::KernelRunnerClient;
use opsense_proto::pb::value as pb_value;
use opsense_proto::pb::{
    CloseRequest, CodeRequest, HealthRequest, PingRequest, SessionHandle,
    SessionParams, InterruptRequest, exec_event,
};
use opsense_runner::backend::IpcKernelBackend;
use opsense_runner::config::{resolve_kernel_binary, RunnerConfig};
use opsense_runner::server::RunnerService;
use opsense_runner::session::SessionRegistry;

/// Echo kernel binary: resolved the same way `RunnerConfig::default()`
/// resolves a kernel name — keeps the health `detail` reproducible
/// across machines instead of leaking a CI-specific absolute path.
fn echo_bin() -> Option<std::path::PathBuf> {
    let p = resolve_kernel_binary("opsense-kernel-echo");
    if p.exists() { Some(p) } else { None }
}

async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    // Grab a free port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let bin = echo_bin().expect("echo kernel binary must be built for this test");
    let cfg = RunnerConfig::default();
    let backend = Arc::new(IpcKernelBackend::new(bin, vec![]));
    let registry = Arc::new(SessionRegistry::new(backend, None, cfg.clone()));
    let service = RunnerService::new(registry, cfg, None);

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.with_limits())
            .serve(addr)
            .await
            .expect("runner serve");
    });
    // Give tonic a moment to accept connections.
    tokio::time::sleep(Duration::from_millis(200)).await;
    (addr, handle)
}

#[tokio::test]
async fn grpc_roundtrip_start_execute_ping_close() {
    let Some(_bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let (addr, server) = start_server().await;

    let mut client = KernelRunnerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    // Health.
    let health = client
        .health(tonic::Request::new(HealthRequest {}))
        .await
        .expect("health")
        .into_inner();
    assert!(health.ok);
    assert!(health.kernel_name.starts_with("runner/"));

    // Start session.
    let handle: SessionHandle = client
        .start(tonic::Request::new(SessionParams {
            session_id: "grpc-s1".into(),
            ..SessionParams::default()
        }))
        .await
        .expect("start")
        .into_inner();
    assert_eq!(handle.session_id, "grpc-s1");

    // Execute: text result streamed back.
    let mut stream = client
        .execute(tonic::Request::new(CodeRequest {
            request_id: "r1".into(),
            session_id: "grpc-s1".into(),
            code: "1 + 1".into(),
            input_names: vec![],
            timeout_ms: 5_000,
        }))
        .await
        .expect("execute")
        .into_inner();
    let mut text = None;
    while let Some(event) = stream.message().await.expect("event") {
        match event.event {
            Some(exec_event::Event::ResultValue(v)) => {
                if let Some(pb_value::Kind::Text(t)) = v.kind {
                    text = Some(t);
                }
            }
            Some(exec_event::Event::Done(true)) => break,
            _ => {}
        }
    }
    assert_eq!(text.as_deref(), Some("echo: 1 + 1"));

    // Ping (implicit keepalive + monitoring).
    let pong = client
        .ping(tonic::Request::new(PingRequest {
            session_id: "grpc-s1".into(),
        }))
        .await
        .expect("ping")
        .into_inner();
    assert!(pong.alive);
    assert!(pong.server_time > 0);

    // Interrupt when idle answers Ack directly.
    let ack = client
        .interrupt(tonic::Request::new(InterruptRequest {
            session_id: "grpc-s1".into(),
            request_id: "none".into(),
        }))
        .await
        .expect("interrupt")
        .into_inner();
    assert!(ack.ok);

    // Close session.
    let ack = client
        .close(tonic::Request::new(CloseRequest {
            session_id: "grpc-s1".into(),
        }))
        .await
        .expect("close")
        .into_inner();
    assert!(ack.ok);

    server.abort();
}

#[tokio::test]
async fn grpc_health_returns_runner_info() {
    let Some(_bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let (addr, server) = start_server().await;

    let mut client = KernelRunnerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    let health = client
        .health(tonic::Request::new(HealthRequest {}))
        .await
        .expect("health")
        .into_inner();
    assert!(health.ok);
    assert_eq!(health.kernel_name, "runner/ipc");
    assert_eq!(health.detail, "command \"target/debug/opsense-kernel-echo\" args []");

    server.abort();
}
