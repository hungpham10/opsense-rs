//! End-to-end harness: spawns the real echo-kernel binary and drives its
//! full lifecycle through [`KernelConnection`] — the same driver the local
//! IPC backend and the runner use. These tests lock the wire contract before
//! any language kernel is written.

use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::Result;
use opsense_proto::host::KernelConnection;
use opsense_proto::pb::{CodeRequest, Envelope, SessionParams, envelope};
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

type Conn = KernelConnection<ChildStdout, ChildStdin>;

struct Kernel {
    child: Child,
    conn: Conn,
}

impl Drop for Kernel {
    fn drop(&mut self) {
        // stdin closed on drop -> kernel exits its read loop.
        if let Some(mut stdin) = self.child.stdin.take() {
            drop(stdin.shutdown());
        }
        let _ = self.child.start_kill();
    }
}

async fn spawn_kernel() -> Kernel {
    let mut child = Command::new(env!("CARGO_BIN_EXE_opsense-kernel-echo"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn echo kernel");
    let stdin = child.stdin.take().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    Kernel {
        child,
        conn: KernelConnection::new(stdout, stdin),
    }
}

fn code_req(session: &str, request: &str, code: &str) -> CodeRequest {
    CodeRequest {
        request_id: request.to_string(),
        session_id: session.to_string(),
        code: code.to_string(),
        input_names: vec![],
        timeout_ms: 10_000,
    }
}

async fn start_session(conn: &mut Conn, id: &str) -> Result<()> {
    let echoed = conn
        .start_session(SessionParams {
            session_id: id.to_string(),
            ..SessionParams::default()
        })
        .await?;
    assert_eq!(echoed, id);
    Ok(())
}

#[tokio::test]
async fn lifecycle_handshake_health_session_shutdown() {
    let mut k = spawn_kernel().await;

    let welcome = k.conn.handshake().await.expect("handshake");
    assert_eq!(welcome.kernel_name, "echo");
    assert_eq!(welcome.protocol_version, opsense_proto::PROTOCOL_VERSION);

    let health = k.conn.health().await.expect("health");
    assert!(health.ok);
    assert_eq!(health.kernel_name, "echo");

    start_session(&mut k.conn, "s1")
        .await
        .expect("start session");
    k.conn.close_session("s1").await.expect("close session");

    // Protocol mismatch is rejected by the host-side check too; here just
    // confirm graceful shutdown round-trips an ack and exits cleanly.
    k.conn.shutdown().await.expect("shutdown");
    let status = tokio::time::timeout(Duration::from_secs(3), k.child.wait())
        .await
        .expect("kernel exit within timeout")
        .expect("wait status");
    assert!(status.success());
}

#[tokio::test]
async fn execute_print_text_and_error_directives() {
    let mut k = spawn_kernel().await;
    k.conn.handshake().await.unwrap();
    start_session(&mut k.conn, "s1").await.unwrap();

    let printed = k
        .conn
        .execute(code_req("s1", "r1", "print:hello world"))
        .await
        .expect("execute print");
    assert!(printed.ok(), "{printed:?}");
    assert_eq!(printed.stdout(), "hello world");

    let text = k
        .conn
        .execute(code_req("s1", "r2", "1 + 1"))
        .await
        .expect("execute text");
    assert!(text.ok());
    match &text.value.expect("text result").kind {
        Some(opsense_proto::pb::value::Kind::Text(t)) => assert_eq!(t, "echo: 1 + 1"),
        other => panic!("unexpected value {other:?}"),
    }

    let failed = k
        .conn
        .execute(code_req("s1", "r3", "err:python_exception:boom"))
        .await
        .expect("execute error directive");
    assert!(!failed.ok());
    let err = failed.error.expect("error event");
    assert_eq!(err.kind, "python_exception");
    assert_eq!(err.message, "boom");

    k.conn.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupt_cancels_running_sleep() {
    let mut k = spawn_kernel().await;
    k.conn.handshake().await.unwrap();
    start_session(&mut k.conn, "s1").await.unwrap();

    let req = code_req("s1", "r-long", "sleep:3000");
    k.conn
        .send(&Envelope {
            msg: Some(envelope::Msg::CodeRequest(req)),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    k.conn.interrupt("s1", "r-long").await.unwrap();

    // The interrupt answers through the execute stream: ErrorEvent(cancelled)
    // followed by done — no separate Ack in between.
    let mut cancelled = false;
    loop {
        let env = tokio::time::timeout(Duration::from_secs(2), k.conn.recv())
            .await
            .expect("event within timeout")
            .expect("connection alive")
            .expect("decode");
        match env.msg {
            Some(envelope::Msg::ExecEvent(ev)) => match ev.event {
                Some(opsense_proto::pb::exec_event::Event::Error(err)) => {
                    assert_eq!(err.kind, "cancelled");
                    cancelled = true;
                }
                Some(opsense_proto::pb::exec_event::Event::Done(true)) => break,
                _ => {}
            },
            other => panic!("unexpected envelope during cancel: {other:?}"),
        }
    }
    assert!(cancelled);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "interrupt must cut the sleep short"
    );

    // Connection stays usable afterwards.
    let after = k
        .conn
        .execute(code_req("s1", "r-after", "still alive"))
        .await
        .expect("execute after interrupt");
    assert!(after.ok());

    k.conn.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_timeout_fires_interrupt_and_recovers() {
    let mut k = spawn_kernel().await;
    k.conn.handshake().await.unwrap();
    start_session(&mut k.conn, "s1").await.unwrap();

    let mut req = code_req("s1", "r-timeout", "sleep:5000");
    req.timeout_ms = 200;
    let started = Instant::now();
    let outcome = k.conn.execute(req).await.expect("execute with timeout");
    assert!(outcome.timed_out);
    assert!(started.elapsed() < Duration::from_secs(2));

    // Drain the cancellation tail (ErrorEvent + done) so the connection is
    // clean again, then prove recovery.
    loop {
        let env = tokio::time::timeout(Duration::from_secs(2), k.conn.recv())
            .await
            .expect("tail event")
            .expect("alive")
            .expect("decode");
        if matches!(
            env.msg,
            Some(envelope::Msg::ExecEvent(ev))
                if matches!(ev.event, Some(opsense_proto::pb::exec_event::Event::Done(true)))
        ) {
            break;
        }
    }
    let recovered = k
        .conn
        .execute(code_req("s1", "r-next", "ok"))
        .await
        .expect("post-timeout execute");
    assert!(recovered.ok());

    k.conn.shutdown().await.unwrap();
}

#[tokio::test]
async fn kernel_crash_surfaces_as_connection_eof() {
    let mut k = spawn_kernel().await;
    k.conn.handshake().await.unwrap();

    k.child.kill().await.expect("kill kernel");
    let outcome = k
        .conn
        .execute(code_req("s1", "r1", "print:x"))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    // Either send or recv fails with a broken pipe / closed connection —
    // never a hang, never success.
    assert!(
        outcome.is_err(),
        "execute after kill must fail: {outcome:?}"
    );
}
