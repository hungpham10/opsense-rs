//! Smoke test Tầng 1: HTTP `/health` + GraphQL `Query.status`.
//!
//! Verify rằng `opsense-serve` đáp ứng cơ bản sau khi `docker compose up`.
//! In integration mode (`CI=true`), failure panics so the workflow
//! cannot silently go green. On local dev without compose, skip gracefully.

mod common;

async fn ensure_health(client: &reqwest::Client) -> bool {
    match common::wait_for_health(client, 30).await {
        Ok(()) => true,
        Err(_) if common::integration_mode() => {
            panic!("serve not reachable — CI requires `docker compose up`")
        }
        Err(_) => {
            eprintln!("skipping: serve not reachable — run `docker compose up` first");
            false
        }
    }
}

async fn ensure_pipeline(client: &reqwest::Client, bearer: &str) -> bool {
    match common::wait_for_pipeline(client, 30, bearer).await {
        Ok(()) => true,
        Err(_) if common::integration_mode() => {
            panic!("pipeline not ready — CI requires `docker compose up`")
        }
        Err(_) => {
            eprintln!("skipping: pipeline not ready — run `docker compose up` first");
            false
        }
    }
}

async fn ensure_dex(_client: &reqwest::Client) -> bool {
    match common::wait_for_dex(10).await {
        Ok(()) => true,
        Err(_) if common::integration_mode() => {
            panic!("Dex not reachable — CI requires `docker compose up`")
        }
        Err(_) => {
            eprintln!("skipping: Dex not reachable — run `docker compose up` first");
            false
        }
    }
}

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let client = reqwest::Client::new();
    if !ensure_health(&client).await {
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
    // `/api/repl/graphql` đi qua Nginx lua-resty-openidc — default
    // `$jwt_required = 2` nên không có Bearer JWT sẽ bị 401. Đăng nhập Dex
    // trước để lấy id_token rồi gọi graphql kèm Bearer.
    let client = common::dex::login_client();
    if !ensure_health(&client).await {
        return;
    }
    if !ensure_dex(&client).await {
        return;
    }
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    if !ensure_pipeline(&client, &id_token).await {
        return;
    }
    let resp = client
        .post(format!("{}/api/repl/graphql", common::serve_url()))
        .bearer_auth(&id_token)
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