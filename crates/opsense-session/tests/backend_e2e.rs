//! End-to-end: `Session` + `LocalIpcBackend` driving the real echo-kernel
//! binary. Proves the REPL's execution path works over IPC without any
//! embedded interpreter.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use opsense_core::config::{
    Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
};
use opsense_session::{KernelBackend, KernelConfig, LocalIpcBackend, SessionManager};

fn test_config() -> Config {
    Config {
        engine: EngineConfig::default(),
        capacity: HashMap::new(),
        sources: SourcesConfig::default(),
        attributes: HashMap::new(),
        storage: StorageConfig::default(),
        pipeline: None,
        session: SessionConfig::default(),
        repl: ReplConfig::default(),
    }
}

/// Locate the echo kernel binary: `OPSENSE_KERNEL_BIN`, else the workspace
/// target dir. Tests skip gracefully when it has not been built yet.
fn echo_bin() -> Option<std::path::PathBuf> {
    if let Some(p) = std::env::var_os("OPSENSE_KERNEL_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/opsense-kernel-echo");
    p.canonicalize().ok()
}

fn echo_backend(bin: &std::path::Path) -> Arc<LocalIpcBackend> {
    Arc::new(LocalIpcBackend::new(KernelConfig::for_command(bin)))
}

#[test]
fn session_executes_over_local_ipc_backend() {
    let Some(bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let manager = Arc::new(SessionManager::new(test_config(), echo_backend(&bin)));
    let session = manager.create_session().expect("session with kernel");

    // Text result round-trip.
    let out = manager
        .block_on(session.execute_with("1 + 1", HashMap::new()))
        .expect("execute");
    assert!(out.ok(), "{out:?}");
    assert_eq!(out.text.as_deref(), Some("echo: 1 + 1"));

    // Dataset injection + dataframe result.
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
        ],
    )
    .unwrap();

    let mut inputs = HashMap::new();
    inputs.insert("@1".to_string(), batch);
    let out = manager
        .block_on(session.execute_with("df", inputs))
        .expect("dataset roundtrip");
    assert!(out.ok(), "{out:?}");
    let back = out.dataframe.expect("dataframe back");
    assert_eq!(back.num_rows(), 3);
    assert_eq!(back.num_columns(), 2);

    // Kernel-reported failure surfaces as output.error, not Err.
    let out = manager
        .block_on(session.execute_with("err:python_exception:boom", HashMap::new()))
        .expect("error directive");
    assert!(!out.ok());
    assert!(out.error.unwrap().contains("boom"));

    // Closing releases the kernel process; further executes fail cleanly.
    assert!(manager.close_session(session.id()));
    let err = manager
        .block_on(session.execute_with("print:x", HashMap::new()))
        .expect_err("closed session must fail");
    assert!(err.to_string().contains("no kernel for session"));
}

#[test]
fn backend_health_reports_local_ipc() {
    let backend = echo_backend(std::path::Path::new("/bin/true"));
    let info = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { backend.health().await.unwrap() });
    assert_eq!(info.name, "local-ipc");
}

#[test]
fn million_row_dataset_streams_in_chunks() {
    let Some(bin) = echo_bin() else {
        eprintln!("skipping: opsense-kernel-echo not built");
        return;
    };
    let manager = Arc::new(SessionManager::new(test_config(), echo_backend(&bin)));
    let session = manager.create_session().expect("session");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        vec![Arc::new(arrow::array::Int64Array::from(
            (0..1_000_000i64).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();

    let mut inputs = HashMap::new();
    inputs.insert("@big".to_string(), batch);
    let out = manager
        .block_on(session.execute_with("df", inputs))
        .expect("chunked transfer");
    assert!(out.ok(), "{out:?}");
    let back = out.dataframe.expect("dataframe back");
    assert_eq!(back.num_rows(), 1_000_000);
}
