//! Integration test — REPL qua `rexpect` PTY.
//!
//! Test approach: spawn `opsense repl --runner <endpoint>` trong PTY, verify
//! echo kernel round-trip end-to-end (multi-line input → block buffer → execute).
//!
//! In integration mode (`OPSENSE_INTEGRATION`), any failure panics so the
//! workflow cannot silently go green. On local dev without compose, skip.

mod common;

use std::net::TcpStream;
use std::time::Duration;

const REPL_PROMPT: &str = "opsense>";
const ECHO_CONNECTED: &str = "session ready";
const ECHO_RESULT_PREFIX: &str = "echo:";

/// True when a TCP connect to `host:port` succeeds within 2s.
///
/// Do not use bash `</dev/tcp/…>`: that opens the socket for reading and
/// waits for EOF, so a live gRPC server can look down (timeout 124).
fn runner_reachable(endpoint: &str) -> bool {
    let Ok(addr) = endpoint.parse() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok()
}

fn build_repl_session(runner_endpoint: &str) -> Option<rexpect::session::PtySession> {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_opsense"));
    cmd.args(["repl", "--runner", &format!("http://{runner_endpoint}")]);
    // rexpect cannot answer reedline's cursor-position query; force the
    // plain stdin reader so the prompt is a literal matchable string.
    cmd.env("OPSENSE_REPL_PLAIN", "1");
    rexpect::session::spawn_command(cmd, Some(30_000)).ok()
}

/// Wait for the REPL process to exit and assert status 0.
///
/// `exp_eof` is unreliable here: on Linux, reading a PTY master after the
/// child exits yields `EIO` (`Uncategorized`), which rexpect discards instead
/// of treating as EOF, so it would time out even on a clean exit.
fn wait_clean_exit(p: &rexpect::session::PtySession) -> Result<(), String> {
    use nix::sys::wait::WaitStatus;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match p.process.status() {
            Some(WaitStatus::Exited(_, 0)) => return Ok(()),
            Some(WaitStatus::StillAlive) | None if std::time::Instant::now() > deadline => {
                return Err("repl did not exit within 10s".into());
            }
            Some(WaitStatus::StillAlive) | None => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Some(other) => return Err(format!("repl exited with {other:?}")),
        }
    }
}

#[test]
fn repl_runner_mode_runs_code_and_exits() {
    let runner_endpoint = common::runner_endpoint("echo");

    if !runner_reachable(&runner_endpoint) {
        if common::integration_mode() {
            panic!("cannot connect to echo runner at {runner_endpoint} — CI requires it");
        }
        eprintln!("skipping: cannot connect to runner at {runner_endpoint}");
        return;
    }

    // Spawn REPL.
    let mut p = match build_repl_session(&runner_endpoint) {
        Some(p) => p,
        None => {
            if common::integration_mode() {
                panic!("failed to spawn repl — CI requires it");
            }
            eprintln!("skipping: failed to spawn repl");
            return;
        }
    };

    // Đợi prompt ban đầu.
    p.exp_string(REPL_PROMPT).expect("initial prompt");

    // Chọn echo kernel.
    p.send_line(":echo").expect("send :echo");
    p.exp_string(ECHO_CONNECTED)
        .expect("connected to echo kernel");

    // Chạy code.
    p.send_line("hello world").expect("send code");
    p.exp_string(ECHO_RESULT_PREFIX).expect("got echo result");

    // Thoát.
    p.send_line(":exit").expect("send :exit");
    wait_clean_exit(&p).expect("repl exits cleanly");
}

#[test]
fn repl_block_mode_accumulates_then_executes() {
    let runner_endpoint = common::runner_endpoint("echo");

    if !runner_reachable(&runner_endpoint) {
        if common::integration_mode() {
            panic!("cannot connect to echo runner at {runner_endpoint} — CI requires it");
        }
        eprintln!("skipping: runner not reachable");
        return;
    }

    let mut p = match build_repl_session(&runner_endpoint) {
        Some(p) => p,
        None => {
            if common::integration_mode() {
                panic!("failed to spawn repl — CI requires it");
            }
            eprintln!("skipping: failed to spawn repl");
            return;
        }
    };

    p.exp_string(REPL_PROMPT).expect("prompt");
    p.send_line(":echo").expect("kernel");
    p.exp_string(ECHO_CONNECTED).expect("connected");
    p.send_line(":block").expect("enter block mode");

    // Gửi multi-line; dòng trống trigger execute.
    p.send_line("line1").expect("send line1");
    p.send_line("line2").expect("send line2");
    p.send_line("line3").expect("send line3");
    p.send_line("").expect("send empty line → execute");

    // Verify echo kernel trả về full buffer.
    p.exp_string(ECHO_RESULT_PREFIX).expect("echo result");

    p.send_line(":exit").expect("exit");
    wait_clean_exit(&p).expect("repl exits");
}
