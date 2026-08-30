//! End-to-end over real gRPC: spin up the runner service backed by the echo
//! kernel binary and drive it with the generated client — proving the full
//! serve ↔ runner ↔ kernel chain minus the Python kernel itself.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use opsense_proto::pb::kernel_runner_client::KernelRunnerClient;
use opsense_proto::pb::value as pb_value;
use opsense_proto::pb::{
    exec_event, CloseRequest, CodeRequest, DatasetChunk, DatasetHeader, HealthRequest,
    SessionHandle, SessionParams,
};
use opsense_runner::server::RunnerService;
use opsense_session::backend::chunk_record_batch;
use opsense_session::{GrpcRunnerBackend, KernelBackend, KernelConfig};

/// Echo kernel binary from the workspace target dir; skip when absent.
fn echo_bin() -> Option<std::path::PathBuf> {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/opsense-kernel-echo");
    p.canonicalize().ok()
}

async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    // Grab a free port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let cfg = KernelConfig::for_command(
        echo_bin().expect("echo kernel binary must be built for this test"),
    );
    let handle = tokio::spawn(async move {
        let service = RunnerService::new(cfg);
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
async fn grpc_roundtrip_start_execute_dataset_close() {
    let Some(bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let _ = bin;
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
        .start_session(tonic::Request::new(SessionParams {
            session_id: "grpc-s1".into(),
            ..SessionParams::default()
        }))
        .await
        .expect("start session")
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

    // SendDataset: two chunks of Arrow IPC through client-streaming gRPC.
    let segments = arrow_segments();
    let out = tonic::Request::new(futures_util::stream::iter(
        segments
            .into_iter()
            .enumerate()
            .map(|(i, seg)| DatasetChunk {
                session_id: "grpc-s1".into(),
                dataset_ref: "@1".into(),
                seq: i as u64,
                last: i == 2,
                arrow_ipc: seg.to_vec(),
            }),
    ));
    let ack = client
        .send_dataset(out)
        .await
        .expect("send dataset")
        .into_inner();
    assert!(ack.ok, "{}", ack.error);
    assert_eq!(ack.rows, 9); // three 3-row batches

    // df directive echoes the dataset back as a DataFrame value.
    let mut stream = client
        .execute(tonic::Request::new(CodeRequest {
            request_id: "r2".into(),
            session_id: "grpc-s1".into(),
            code: "df".into(),
            input_names: vec![],
            timeout_ms: 5_000,
        }))
        .await
        .expect("df execute")
        .into_inner();
    let mut rows = 0i64;
    while let Some(event) = stream.message().await.expect("event") {
        match event.event {
            Some(exec_event::Event::ResultValue(v)) => {
                if let Some(pb_value::Kind::Dataframe(df)) = v.kind {
                    rows = df.rows;
                }
            }
            Some(exec_event::Event::Done(true)) => break,
            _ => {}
        }
    }
    assert_eq!(rows, 9);

    // Interrupt when idle answers Ack directly.
    let ack = client
        .interrupt(tonic::Request::new(opsense_proto::pb::InterruptRequest {
            session_id: "grpc-s1".into(),
            request_id: "none".into(),
        }))
        .await
        .expect("interrupt")
        .into_inner();
    assert!(ack.ok);

    // Close session.
    let ack = client
        .close_session(tonic::Request::new(CloseRequest {
            session_id: "grpc-s1".into(),
        }))
        .await
        .expect("close")
        .into_inner();
    assert!(ack.ok);

    server.abort();
}

/// Three 3-row RecordBatches, each its own Arrow IPC stream segment.
fn arrow_segments() -> Vec<Bytes> {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    (0..3)
        .map(|round| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            let _ = round;
            Bytes::from(buf)
        })
        .collect()
}

#[tokio::test]
async fn grpc_backend_roundtrips_chunked_million_rows() {
    let Some(bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let _ = bin;
    let (addr, server) = start_server().await;

    // The exact backend a remote session would use.
    let backend = GrpcRunnerBackend::connect(&addr.to_string())
        .await
        .expect("connect");
    let sid = backend
        .start_session(SessionParams {
            session_id: "grpc-big".into(),
            ..SessionParams::default()
        })
        .await
        .expect("start");

    let batch = arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ])),
        vec![std::sync::Arc::new(arrow::array::Int64Array::from(
            (0..1_000_000i64).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    let segments = chunk_record_batch(&batch).expect("chunking");
    assert!(
        segments.len() > 10,
        "expected many chunks, got {}",
        segments.len()
    );

    let header = DatasetHeader {
        session_id: sid.clone(),
        dataset_ref: "@big".into(),
        rows: 1_000_000,
        cols: 1,
        columns: vec!["n".into()],
    };
    let ack = backend
        .send_dataset(&sid, header, segments)
        .await
        .expect("send dataset");
    assert!(ack.ok, "{}", ack.error);
    assert_eq!(ack.rows, 1_000_000);

    let outcome = backend
        .execute(
            &sid,
            CodeRequest {
                request_id: "r1".into(),
                session_id: sid.clone(),
                code: "df".into(),
                input_names: vec![],
                timeout_ms: 30_000,
            },
        )
        .await
        .expect("df over grpc");
    assert!(outcome.ok(), "{outcome:?}");
    match outcome.value.expect("dataframe").kind {
        Some(pb_value::Kind::Dataframe(df)) => assert_eq!(df.rows, 1_000_000),
        other => panic!("unexpected value {other:?}"),
    }
    server.abort();
}
