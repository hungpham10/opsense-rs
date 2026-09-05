//! Integration test — RunnerClient gRPC tới 3 runner (echo / python / julia).
//!
//! Test approach: connect + execute qua `RunnerClient`, assert result.
//! Skip gracefully nếu runner không chạy.

mod common;

use opsense::client::grpc::RunnerClient;
use opsense_proto::pb::SessionParams;

const SESSION_ID: &str = "smoke-test";

#[tokio::test]
async fn echo_runner_executes() {
    let endpoint = format!("http://{}", common::runner_endpoint("echo"));
    let mut client = match RunnerClient::connect(
        &endpoint,
        SessionParams {
            session_id: format!("{SESSION_ID}-echo"),
            ..Default::default()
        },
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: cannot connect to echo runner at {endpoint}: {e}");
            return;
        }
    };

    let outcome = client.execute("hello world").await.expect("execute");
    assert!(outcome.ok(), "echo kernel should succeed: {outcome:?}");
    assert_eq!(outcome.text(), Some("echo: hello world"));

    client.close().await.expect("close");
}

#[tokio::test]
async fn python_runner_executes() {
    let endpoint = format!("http://{}", common::runner_endpoint("python"));
    let mut client = match RunnerClient::connect(
        &endpoint,
        SessionParams {
            session_id: format!("{SESSION_ID}-python"),
            ..Default::default()
        },
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: cannot connect to python runner at {endpoint}: {e}");
            return;
        }
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
    let mut client = match RunnerClient::connect(
        &endpoint,
        SessionParams {
            session_id: format!("{SESSION_ID}-julia"),
            ..Default::default()
        },
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: cannot connect to julia runner at {endpoint}: {e}");
            return;
        }
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
    let mut client = match RunnerClient::connect(
        &endpoint,
        SessionParams {
            session_id: format!("{SESSION_ID}-health"),
            ..Default::default()
        },
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: cannot connect to echo runner at {endpoint}: {e}");
            return;
        }
    };

    let health = client.health().await.expect("health");
    assert!(!health.kernel_name.is_empty());

    client.close().await.ok();
}