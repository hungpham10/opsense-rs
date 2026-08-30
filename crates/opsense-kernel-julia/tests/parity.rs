//! Parity suite: drives the real Julia kernel launcher through the same
//! lifecycle assertions as the Python/echo-kernel harness, proving the
//! language swap does not change the wire contract (checklist §8).
//!
//! Skips gracefully when the system `julia` interpreter is not installed.

use std::process::{Command, Stdio};

use anyhow::Result;
use opsense_proto::host::KernelConnection;
use opsense_proto::pb::{value, CodeRequest, SessionParams};
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

/// Is the system `julia` interpreter on PATH?
fn julia_available() -> bool {
    let probe = Command::new("julia")
        .args(["-e", "println(VERSION)"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    matches!(probe, Ok(status) if status.success())
}

async fn spawn_kernel() -> Result<Kernel> {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_opsense-kernel-julia"))
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
        timeout_ms: 30_000,
    }
}

#[tokio::test]
async fn julia_kernel_parity_lifecycle() {
    if !julia_available() {
        eprintln!("skipping: julia interpreter not on PATH");
        return;
    }
    let mut k = spawn_kernel().await.expect("spawn");

    // Handshake speaks the same protocol as python/echo.
    let welcome = k.conn.handshake().await.expect("handshake");
    assert_eq!(welcome.kernel_name, "julia");
    assert_eq!(welcome.protocol_version, opsense_proto::PROTOCOL_VERSION);

    let health = k.conn.health().await.expect("health");
    assert!(health.ok, "{}", health.detail);

    k.conn
        .start_session(SessionParams {
            session_id: "jl-s1".into(),
            ..SessionParams::default()
        })
        .await
        .expect("start session");

    // Regression: user `print`/`println` must become `stdout_line` events,
    // not raw bytes that desync the host frame decoder (the
    // "declared frame length ... exceeds cap" failure). The exact repro from
    // the bug report uses `print` (no trailing newline).
    let out = k
        .conn
        .execute(code_req("jl-s1", "r1", "print(\"hello\")"))
        .await
        .expect("execute with print");
    assert!(out.ok(), "{out:?}");
    assert_eq!(out.stdout(), "hello", "captured print output");

    let out = k
        .conn
        .execute(code_req("jl-s1", "r2", "println(\"world\")"))
        .await
        .expect("execute with println");
    assert!(out.ok(), "{out:?}");
    assert_eq!(out.stdout(), "world", "captured println output");

    // A third request must still succeed — a desync would have poisoned the
    // stream and made this execute fail with a frame-length error.
    let out = k
        .conn
        .execute(code_req("jl-s1", "r3", "1 + 1"))
        .await
        .expect("third execute after prints");
    assert!(out.ok(), "{out:?}");
    match &out.value.expect("captured result").kind {
        Some(value::Kind::Text(text)) => assert_eq!(text, "2"),
        other => panic!("unexpected value {other:?}"),
    }

    k.conn.shutdown().await.expect("shutdown");
}
