//! Integration test — RunnerClient gRPC tới 3 runner (echo / python / julia).
//!
//! Test approach: connect + execute qua `RunnerClient`, assert result.
//! In integration mode (`CI=true`), connection failure panics so the
//! workflow cannot silently go green. On local dev without runners,
//  skip gracefully.

mod common;

use opsense::client::grpc::RunnerClient;
use opsense_proto::pb::SessionParams;

const SESSION_ID: &str = "smoke-test";

async fn connect_runner(endpoint: &str, session_id: &str) -> Option<RunnerClient> {
    match RunnerClient::connect(
        endpoint,
        SessionParams {
            session_id: session_id.to_string(),
            ..Default::default()
        },
    )
    .await
    {
        Ok(c) => Some(c),
        Err(e) => {
            if common::integration_mode() {
                panic!("cannot connect to runner at {endpoint} — CI requires it: {e}");
            }
            None
        }
    }
}

#[tokio::test]
async fn echo_runner_executes() {
    let endpoint = format!("http://{}", common::runner_endpoint("echo"));
    let mut client = match connect_runner(&endpoint, &format!("{SESSION_ID}-echo")).await {
        Some(c) => c,
        None => return,
    };

    let outcome = client.execute("hello world").await.expect("execute");
    assert!(outcome.ok(), "echo kernel should succeed: {outcome:?}");
    assert_eq!(outcome.text(), Some("echo: hello world"));

    client.close().await.expect("close");
}

#[tokio::test]
async fn python_runner_executes() {
    let endpoint = format!("http://{}", common::runner_endpoint("python"));
    let mut client = match connect_runner(&endpoint, &format!("{SESSION_ID}-python")).await {
        Some(c) => c,
        None => return,
    };

    let outcome = client.execute("1 + 1").await.expect("execute");
    assert!(outcome.ok(), "python kernel should succeed: {outcome:?}");
    let text = outcome.text().unwrap_or("");
    assert!(text.contains('2'), "python '1+1' should produce '2', got: {text}");

    client.close().await.expect("close");
}

#[tokio::test]
async fn julia_runner_executes() {
    let endpoint = format!("http://{}", common::runner_endpoint("julia"));
    let mut client = match connect_runner(&endpoint, &format!("{SESSION_ID}-julia")).await {
        Some(c) => c,
        None => return,
    };

    let outcome = client.execute("1 + 1").await.expect("execute");
    assert!(outcome.ok(), "julia kernel should succeed: {outcome:?}");
    let text = outcome.text().unwrap_or("");
    assert!(text.contains('2'), "julia '1+1' should produce '2', got: {text}");

    client.close().await.expect("close");
}

#[tokio::test]
async fn health_returns_runner_info() {
    let endpoint = format!("http://{}", common::runner_endpoint("echo"));
    let mut client = match connect_runner(&endpoint, &format!("{SESSION_ID}-health")).await {
        Some(c) => c,
        None => return,
    };

    let health = client.health().await.expect("health");
    assert!(!health.kernel_name.is_empty());

    client.close().await.ok();
}