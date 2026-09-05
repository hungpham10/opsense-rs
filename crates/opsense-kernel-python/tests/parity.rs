//! Parity suite: drives the real Python kernel launcher through the same
//! lifecycle assertions as the echo-kernel harness, proving the language
//! swap does not change the wire contract (checklist §8).
//!
//! Skips gracefully when the interpreter lacks numpy/pandas/pyarrow/protobuf.

use std::process::{Command, Stdio};

use anyhow::Result;
use opsense_proto::host::KernelConnection;
use opsense_proto::pb::{CodeRequest, SessionParams, value};
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, ChildStdin, ChildStdout};

type Conn = KernelConnection<ChildStdout, ChildStdin>;

struct Kernel {
    child: Child,
    conn: Conn,
}

impl Drop for Kernel {
    fn drop(&mut self) {
        if let Some(mut stdin) = self.child.stdin.take() {
            drop(stdin.shutdown());
        }
        let _ = self.child.start_kill();
    }
}

/// Does the system interpreter have everything the kernel needs?
fn python_stack_available() -> bool {
    let probe = Command::new("python3")
        .args([
            "-c",
            "import numpy,pandas,pyarrow; from google.protobuf import descriptor",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    matches!(probe, Ok(status) if status.success())
}

async fn spawn_kernel() -> Result<Kernel> {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_opsense-kernel-python"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let stdin = child.stdin.take().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    Ok(Kernel {
        child,
        conn: KernelConnection::new(stdout, stdin),
    })
}

fn code_req(session: &str, request: &str, code: &str) -> CodeRequest {
    CodeRequest {
        request_id: request.into(),
        session_id: session.into(),
        code: code.into(),
        input_names: vec![],
        timeout_ms: 10_000,
    }
}

#[tokio::test]
async fn python_kernel_parity_lifecycle() {
    if !python_stack_available() {
        eprintln!("skipping: python3 missing numpy/pandas/pyarrow/protobuf");
        return;
    }
    let mut k = spawn_kernel().await.expect("spawn");

    // Handshake speaks the same protocol as echo.
    let welcome = k.conn.handshake().await.expect("handshake");
    assert_eq!(welcome.kernel_name, "python");
    assert_eq!(welcome.protocol_version, opsense_proto::PROTOCOL_VERSION);

    let health = k.conn.health().await.expect("health");
    assert!(health.ok, "{}", health.detail);
    assert!(
        health
            .packages
            .iter()
            .any(|p| p.name == "pandas" && p.available),
        "pandas probed"
    );

    k.conn
        .start_session(SessionParams {
            session_id: "py-s1".into(),
            ..SessionParams::default()
        })
        .await
        .expect("start session");

    // Real Python execution with captured `result`.
    let out = k
        .conn
        .execute(code_req("py-s1", "r1", "result = 40 + 2"))
        .await
        .expect("execute");
    assert!(out.ok(), "{out:?}");
    match &out.value.expect("captured result").kind {
        Some(value::Kind::Text(text)) => assert_eq!(text, "42"),
        other => panic!("unexpected value {other:?}"),
    }

    // Error directive mirrors echo semantics.
    let failed = k
        .conn
        .execute(code_req("py-s1", "r3", "err:python_exception:boom"))
        .await
        .expect("err directive");
    assert!(!failed.ok());
    assert_eq!(failed.error.expect("error").kind, "python_exception");

    k.conn.shutdown().await.expect("shutdown");
}
