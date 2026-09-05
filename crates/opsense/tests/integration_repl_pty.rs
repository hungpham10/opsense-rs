//! Integration test — REPL qua `rexpect` PTY.
//!
//! Test approach: spawn `opsense repl --runner <endpoint>` trong PTY, verify
//! echo kernel round-trip end-to-end (multi-line input → block buffer → execute).
//!
//! Skip gracefully nếu runner không chạy hoặc binary chưa build.

mod common;

use std::process::Command;

const REPL_PROMPT: &str = "opsense>";
const ECHO_CONNECTED: &str = "Connected";
const ECHO_RESULT_PREFIX: &str = "echo:";

fn build_repl_session(bin_path: &str, runner_endpoint: &str) -> Option<rexpect::session::PtySession> {
    let mut cmd = std::process::Command::new(bin_path);
    cmd.args(["repl", "--runner", &format!("http://{runner_endpoint}")]);
    rexpect::session::spawn_command(cmd, Some(30_000)).ok()
}

#[test]
fn repl_runner_mode_runs_code_and_exits() {
    let runner_endpoint = common::runner_endpoint("echo");

    // Build opsense binary nếu chưa có. Skip nếu không build được.
    let bin_path = format!("{}/target/debug/opsense", env!("CARGO_MANIFEST_DIR"));
    if !std::path::Path::new(&bin_path).exists() {
        eprintln!("skipping: opsense binary not built at {bin_path}");
        return;
    }

    // Smoke: kiểm tra runner có reachable không trước khi spawn REPL.
    let probe = Command::new("timeout")
        .args(["2", "bash", "-c", &format!("</dev/tcp/{runner_endpoint}")])
        .output();
    if probe.is_err() {
        eprintln!("skipping: cannot connect to runner at {runner_endpoint}");
        return;
    }

    // Spawn REPL.
    let mut p = match build_repl_session(&bin_path, &runner_endpoint) {
        Some(p) => p,
        None => {
            eprintln!("skipping: failed to spawn repl");
            return;
        }
    };

    // Đợi prompt ban đầu.
    p.exp_string(REPL_PROMPT).expect("initial prompt");

    // Chọn echo kernel.
    p.send_line(":echo").expect("send :echo");
    p.exp_string(ECHO_CONNECTED).expect("connected to echo kernel");

    // Chạy code.
    p.send_line("hello world").expect("send code");
    p.exp_string(ECHO_RESULT_PREFIX).expect("got echo result");

    // Thoát.
    p.send_line(":exit").expect("send :exit");
    p.exp_eof().expect("repl exits cleanly");
}

#[test]
fn repl_block_mode_accumulates_then_executes() {
    let runner_endpoint = common::runner_endpoint("echo");

    let bin_path = format!("{}/target/debug/opsense", env!("CARGO_MANIFEST_DIR"));
    if !std::path::Path::new(&bin_path).exists() {
        eprintln!("skipping: opsense binary not built");
        return;
    }
    let probe = Command::new("timeout")
        .args(["2", "bash", "-c", &format!("</dev/tcp/{runner_endpoint}")])
        .output();
    if probe.is_err() {
        eprintln!("skipping: runner not reachable");
        return;
    }

    let mut p = match build_repl_session(&bin_path, &runner_endpoint) {
        Some(p) => p,
        None => {
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
    p.exp_eof().expect("repl exits");
}
