//! Roundtrip-latency smoke per backend. Ignored by default — run with:
//!   cargo test -p opsense-session --test latency_bench -- --ignored --nocapture

use std::sync::Arc as StdArc;
use std::time::{Duration, Instant};

use opsense_proto::pb::SessionParams;
use opsense_session::{GrpcRunnerBackend, KernelBackend, KernelConfig, LocalIpcBackend};

fn echo_bin() -> Option<std::path::PathBuf> {
    if let Some(p) = std::env::var_os("OPSENSE_KERNEL_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/opsense-kernel-echo");
    p.canonicalize().ok()
}

async fn bench<F, Fut>(name: &str, rounds: u32, mut op: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    op().await.expect("warmup");
    let started = Instant::now();
    for _ in 0..rounds {
        op().await.expect("round");
    }
    let total = started.elapsed();
    println!(
        "{name}: {:?} avg over {rounds} rounds ({total:?} total)",
        total / rounds
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn roundtrip_latency_per_backend() {
    let Some(bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let rounds = 200;

    // --- local IPC (backend driven directly; SessionManager owns its own
    // runtime and must not nest inside this tokio context) ---
    let local = StdArc::new(LocalIpcBackend::new(KernelConfig::for_command(&bin)));
    let sid = local
        .start_session(SessionParams {
            session_id: "bench-local".into(),
            ..SessionParams::default()
        })
        .await
        .unwrap();
    {
        let mut n = 0u32;
        bench("local-ipc execute", rounds, || {
            n += 1;
            let local = StdArc::clone(&local);
            let sid = sid.clone();
            async move {
                local
                    .execute(
                        &sid,
                        opsense_proto::pb::CodeRequest {
                            request_id: format!("bench-{n}"),
                            session_id: sid.clone(),
                            code: "1".into(),
                            input_names: vec![],
                            timeout_ms: 5_000,
                        },
                    )
                    .await
                    .map(|_| ())
            }
        })
        .await;
    }
    local.shutdown().await.unwrap();

    // --- grpc runner ---
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let server = tokio::spawn(async move {
        let service = opsense_runner_service(addr, KernelConfig::for_command(bin));
        tonic::transport::Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let backend = StdArc::new(GrpcRunnerBackend::connect(&addr.to_string()).await.unwrap());
    let sid = backend
        .start_session(SessionParams {
            session_id: "bench".into(),
            ..SessionParams::default()
        })
        .await
        .unwrap();
    let mut n = 0u32;
    bench("grpc-runner execute", rounds, || {
        n += 1;
        let backend = StdArc::clone(&backend);
        let sid = sid.clone();
        async move {
            backend
                .execute(
                    &sid,
                    opsense_proto::pb::CodeRequest {
                        request_id: format!("bench-{n}"),
                        session_id: sid.clone(),
                        code: "1".into(),
                        input_names: vec![],
                        timeout_ms: 5_000,
                    },
                )
                .await
                .map(|_| ())
        }
    })
    .await;
    let _ = backend.shutdown().await;
    server.abort();
}

/// Build the runner gRPC service without depending on opsense-runner internals.
fn opsense_runner_service(
    _addr: std::net::SocketAddr,
    cfg: KernelConfig,
) -> opsense_proto::pb::kernel_runner_server::KernelRunnerServer<
    impl opsense_proto::pb::kernel_runner_server::KernelRunner,
> {
    opsense_runner::kernel_runner_service(cfg)
}
