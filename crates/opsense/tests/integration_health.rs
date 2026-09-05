//! Smoke test Tầng 1: HTTP `/health` + GraphQL `Query.status`.
//!
//! Verify rằng `opsense-serve` đáp ứng cơ bản sau khi `docker compose up`.
//! Skip gracefully nếu serve không chạy (dev chạy `cargo test` trước khi
//! compose up vẫn pass).

mod common;

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let client = reqwest::Client::new();
    if common::wait_for_health(&client, 30).await.is_err() {
        eprintln!("skipping: serve not reachable — run `docker compose up` first");
        return;
    }
    let resp = client
        .get(format!("{}/health", common::serve_url()))
        .send()
        .await
        .expect("health request");
    assert!(resp.status().is_success(), "health status: {}", resp.status());
    let body: serde_json::Value = resp.json().await.expect("health json");
    assert_eq!(body["ok"], serde_json::json!(true), "health body: {body}");
}

#[tokio::test]
async fn graphql_status_returns_nodes_and_stations() {
    let client = reqwest::Client::new();
    if common::wait_for_pipeline(&client, 30).await.is_err() {
        eprintln!("skipping: pipeline not ready — run `docker compose up` first");
        return;
    }
    let resp = client
        .post(format!("{}/api/repl/graphql", common::serve_url()))
        .json(&serde_json::json!({
            "query": "{ status { nodes { id } stations { id kind } } }"
        }))
        .send()
        .await
        .expect("graphql request");
    assert!(resp.status().is_success(), "graphql status: {}", resp.status());
    let body: serde_json::Value = resp.json().await.expect("graphql json");

    // `data.status` phải là object (resolve không lỗi).
    let status = &body["data"]["status"];
    assert!(status.is_object(), "status should be object, got: {status}");

    // `nodes` + `stations` là array (có thể rỗng nếu config không load station).
    assert!(status["nodes"].is_array(), "nodes must be array");
    assert!(status["stations"].is_array(), "stations must be array");
}