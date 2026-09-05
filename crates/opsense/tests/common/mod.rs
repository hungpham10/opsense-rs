//! Shared helpers cho integration tests trong `crates/opsense/tests/`.
//!
//! Test đọc env để biết endpoint (do GH Action / Makefile export):
//!
//! | Biến                     | Mặc định                       |
//! |--------------------------|---------------------------------|
//! | `OPSENSE_SERVE_URL`      | `http://127.0.0.1:8080`         |
//! | `OPSENSE_RUNNER_ECHO`    | `opsense-runner:50051`          |
//! | `OPSENSE_RUNNER_PYTHON`  | `opsense-runner-python:50051`   |
//! | `OPSENSE_RUNNER_JULIA`   | `opsense-runner-julia:50051`    |
//!
//! Trên host, các runner DNS là service name trong compose network. Khi chạy
//! ngoài compose, set DNS thật (vd `127.0.0.1:50051`).
//!
//! Convention: mỗi test trong file integration_* trước tiên gọi
//! `wait_for_pipeline` để skip gracefully khi stack chưa chạy — dev có thể
//! chạy `cargo test --test integration_health` trước khi `docker compose up`
//! mà không panic khó hiểu.

use std::time::{Duration, Instant};

use reqwest::Client;

pub fn serve_url() -> String {
    // Mặc định dùng `localhost` (không phải `127.0.0.1`) để Host header
    // khớp với `host` trong seed `sql/postgres/dev/50-init-tenant.sql`.
    // Nginx trong `04-api.conf` lookup tenant theo Host header.
    std::env::var("OPSENSE_SERVE_URL").unwrap_or_else(|_| "http://localhost:8080".into())
}

#[allow(dead_code)] // dùng trong integration_runner_grpc.rs / integration_repl_pty.rs
pub fn runner_endpoint(kind: &str) -> String {
    let var = format!("OPSENSE_RUNNER_{}", kind.to_uppercase());
    std::env::var(&var).unwrap_or_else(|_| format!("opsense-runner-{kind}:50051"))
}

/// Poll `GET /health` đến khi 200 hoặc timeout. Trả `Err` nếu timeout.
#[allow(dead_code)] // dùng trong integration_*.rs
pub async fn wait_for_health(client: &Client, timeout_secs: u64) -> anyhow::Result<()> {
    let url = format!("{}/health", serve_url());
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("serve not healthy at {url} after {timeout_secs}s")
}

/// Poll `POST /api/repl/graphql { status { nodes { id } } }` đến khi 200 hoặc timeout.
#[allow(dead_code)] // dùng trong integration_*.rs
pub async fn wait_for_pipeline(client: &Client, timeout_secs: u64) -> anyhow::Result<()> {
    let url = format!("{}/api/repl/graphql", serve_url());
    let body = serde_json::json!({"query": "{ status { nodes { id } } }"});
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if let Ok(resp) = client.post(&url).json(&body).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("pipeline not ready at {url} after {timeout_secs}s")
}

/// TCP probe tới `127.0.0.1:<port>` — dùng cho runner (gRPC). Trả Ok nếu
/// connect được, Err nếu timeout.
#[allow(dead_code)] // dùng trong integration_runner_grpc.rs / integration_repl_pty.rs
pub async fn wait_for_tcp_port(port: u16, timeout_secs: u64) -> anyhow::Result<()> {
    use tokio::net::TcpStream;
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_secs(1),
            TcpStream::connect(("127.0.0.1", port)),
        )
        .await
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("tcp 127.0.0.1:{port} not listening after {timeout_secs}s")
}