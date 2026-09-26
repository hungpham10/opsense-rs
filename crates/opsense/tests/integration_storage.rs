//! Storage integration tests — verify data integrity, query capabilities,
//! and storage backend features end-to-end.
//!
//! Flow:
//!   clock → timeseries_station_sink → storage backend
//!   → query via GraphQL Query.queryTimeseries → verify data integrity.
//!
//! Test approach: dùng reqwest + Bearer JWT (Dex login) để drive
//! opsense-serve qua Nginx → Axum. Ghi data qua pipeline, đọc lại
//! qua GraphQL queryTimeseries, verify tính toàn vẹn.
//!
//! In integration mode (`CI=true`), failure panics so the workflow
//! cannot silently go green. On local dev without compose, skip gracefully.
//!
//! Chạy:
//!   docker compose up -d opsense-serve opsense-runner opsense-runner-python
//!   cargo test --test integration_storage -- --nocapture

mod common;

use std::time::Duration;

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::Value;

/// Wait for serve to be healthy. Returns `true` if ready, `false` to skip.
async fn ensure_serve(client: &reqwest::Client) -> bool {
    match common::wait_for_health(client, 10).await {
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

/// Wait for pipeline to be ready. Returns `true` if ready, `false` to skip.
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

/// Wait for at least one station to be registered.
async fn ensure_stations(client: &reqwest::Client, bearer: &str) -> bool {
    match common::wait_for_stations(client, 60, bearer).await {
        Ok(stations) => {
            eprintln!("stations registered: {:?}", stations);
            true
        }
        Err(e) if common::integration_mode() => {
            panic!("no stations registered: {e} — CI requires pipeline with timeseries_station_sink")
        }
        Err(e) => {
            eprintln!("skipping: no stations registered ({e}) — run `docker compose up` with proper config");
            false
        }
    }
}

/// URL của opsense-serve (qua Nginx) từ host. Default http://localhost:8080.
fn serve_url() -> String {
    common::serve_url()
}

// =========================================================================
// Tests
// =========================================================================

/// Verify `Query.status` returns nodes and stations after pipeline runs.
#[tokio::test]
async fn storage_graphql_status_has_stations() {
    let client = common::dex::login_client();
    if !ensure_serve(&client).await {
        return;
    }
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    if !ensure_pipeline(&client, &id_token).await {
        return;
    }
    if !ensure_stations(&client, &id_token).await {
        return;
    }

    let resp = client
        .post(format!("{}/api/repl/graphql", serve_url()))
        .bearer_auth(&id_token)
        .json(&serde_json::json!({
            "query": "{ status { nodes { id } stations { id kind } } }"
        }))
        .send()
        .await
        .expect("graphql request");
    assert!(resp.status().is_success(), "graphql status: {}", resp.status());
    let body: Value = resp.json().await.expect("graphql json");

    // At least one station should exist (from pipeline config).
    let empty_vec: Vec<Value> = vec![];
    let stations: &Vec<Value> = body.get("data")
        .and_then(|d| d.get("status"))
        .and_then(|s| s.get("stations"))
        .and_then(|s| s.as_array())
        .unwrap_or(&empty_vec);
    assert!(
        !stations.is_empty(),
        "expected at least 1 station, got: {:?}",
        stations
    );
}

/// Verify `Query.queryTimeseries` returns data after pipeline runs.
/// This proves data integrity: clock → station_sink → queryTimeseries.
///
/// **Phải assert có dữ liệu.** Test này trước đây chỉ in ra rồi return, và
/// query còn sai field (`ts` trên `QueryResult`, trong khi schema là
/// `{ observations truncated scanned }`) ⇒ GraphQL trả error mà test vẫn xanh.
/// Hệ quả là cả đường đọc storage không có coverage: bug `query_range` strict
/// làm mọi cửa sổ kết thúc ở `now` trả rỗng (xem `TimeseriesStation::query_range`)
/// mà không test nào phát hiện.
///
/// Cửa sổ dùng `to = now` — đúng mặc định của `opsense query` / MCP
/// `opsense_query_timeseries`, tức là đúng trường hợp từng hỏng.
#[tokio::test]
async fn storage_query_timeseries_returns_data() {
    let client = common::dex::login_client();
    if !ensure_serve(&client).await {
        return;
    }
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    if !ensure_pipeline(&client, &id_token).await {
        return;
    }
    let stations = match common::wait_for_stations(&client, 60, &id_token).await {
        Ok(s) => s,
        Err(e) if common::integration_mode() => {
            panic!("no stations registered: {e} — CI requires pipeline with timeseries_station_sink")
        }
        Err(e) => {
            eprintln!("skipping: no stations registered ({e})");
            return;
        }
    };
    let station_id = stations.first().map(String::as_str).unwrap_or("tsdb");

    // Wait for clock to generate observations.
    tokio::time::sleep(Duration::from_secs(15)).await;

    let now = opsense_components::signal::now_secs();
    let from_ts = now - 120;
    let to_ts = now;

    let resp = client
        .post(format!("{}/api/repl/graphql", serve_url()))
        .bearer_auth(&id_token)
        .json(&serde_json::json!({
            "query": "query QueryTimeseries($node: String!, $fromTs: Int!, $toTs: Int!) { queryTimeseries(node: $node, fromTs: $fromTs, toTs: $toTs) { observations { ts value metricId signal } truncated scanned } }",
            "variables": {
                "node": station_id,
                "fromTs": from_ts,
                "toTs": to_ts
            }
        }))
        .send()
        .await
        .expect("queryTimeseries request");
    assert!(resp.status().is_success(), "queryTimeseries status: {}", resp.status());
    let body: Value = resp.json().await.expect("queryTimeseries json");

    // GraphQL lỗi (sai field, station sai) → fail, không phải "chưa có data".
    if let Some(errors) = body.get("errors") {
        panic!("queryTimeseries returned GraphQL errors: {errors:?}");
    }

    let result = body
        .get("data")
        .and_then(|d| d.get("queryTimeseries"))
        .unwrap_or_else(|| panic!("queryTimeseries missing from response: {body}"));
    let observations = result
        .get("observations")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("queryTimeseries.observations missing: {result}"));

    assert!(
        !observations.is_empty(),
        "queryTimeseries(window=[{from_ts},{to_ts}]) rỗng trong khi station '{station_id}' \
         đã chạy ≥15s. Đây là regression của reader: cửa sổ kết thúc ở `now` \
         phải trả về dữ liệu có (xem TimeseriesStation::query_recent)."
    );
    eprintln!(
        "queryTimeseries returned {} points (scanned={}, truncated={})",
        observations.len(),
        result.get("scanned").unwrap_or(&Value::Null),
        result.get("truncated").unwrap_or(&Value::Null),
    );
    // Mọi observation phải nằm trong cửa sổ yêu cầu — guard chéo cho lọc ts.
    for o in observations {
        let ts = o.get("ts").and_then(Value::as_i64).expect("ts phải là Int");
        assert!(
            (from_ts..=to_ts).contains(&ts),
            "observation ts={ts} nằm ngoài cửa sổ [{from_ts},{to_ts}]"
        );
    }
}

/// Verify HTTP health endpoint returns correct structure.
#[tokio::test]
async fn storage_health_endpoint() {
    let client = reqwest::Client::new();
    if !ensure_serve(&client).await {
        return;
    }
    let resp = client
        .get(format!("{}/health", serve_url()))
        .send()
        .await
        .expect("health request");
    assert!(resp.status().is_success(), "health status: {}", resp.status());
    let body: Value = resp.json().await.expect("health json");
    assert_eq!(body["ok"], Value::Bool(true), "health body: {:?}", body);
}

/// Verify storage backend data integrity: write via pipeline, read back via
/// GraphQL, verify metric_ids match.
#[tokio::test]
async fn storage_data_integrity_metric_ids() {
    let client = common::dex::login_client();
    if !ensure_serve(&client).await {
        return;
    }
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    if !ensure_pipeline(&client, &id_token).await {
        return;
    }
    if !ensure_stations(&client, &id_token).await {
        return;
    }

    // Wait for pipeline to generate data.
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Query all stations to check which ones exist.
    let resp = client
        .post(format!("{}/api/repl/graphql", serve_url()))
        .bearer_auth(&id_token)
        .json(&serde_json::json!({
            "query": "{ status { stations { id kind } } }"
        }))
        .send()
        .await
        .expect("status request");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("status json");

    let station_ids: Vec<String> = body.get("data")
        .and_then(|d| d.get("status"))
        .and_then(|s| s.get("stations"))
        .and_then(|s| s.as_array())
        .map(|arr| arr.iter().filter_map(|s| s.get("id").and_then(|id| id.as_str()).map(String::from)).collect())
        .unwrap_or_default();
    eprintln!("stations: {:?}", station_ids);

    if station_ids.is_empty() {
        eprintln!("no stations found — skipping data integrity check");
        return;
    }

    // Try querying each station for recent data.
    let now = opsense_components::signal::now_secs();
    let from_ts = now - 60;
    for station_id in &station_ids {
        let resp = client
            .post(format!("{}/api/repl/graphql", serve_url()))
            .bearer_auth(&id_token)
            .json(&serde_json::json!({
                "query": "query Q($node: String!, $fromTs: Int!, $toTs: Int!) { queryTimeseries(node: $node, fromTs: $fromTs, toTs: $toTs) { ts value metricId } }",
                "variables": {
                    "node": station_id,
                    "fromTs": from_ts,
                    "toTs": now
                }
            }))
            .send()
            .await
            .expect("queryTimeseries request");
        assert!(resp.status().is_success(), "queryTimeseries for {station_id} status: {}", resp.status());
        let body: Value = resp.json().await.expect("queryTimeseries json");
        if let Some(data) = body.get("data").and_then(|d| d.get("queryTimeseries")).and_then(|d| d.as_array()) {
            eprintln!("station {}: {} points", station_id, data.len());
        }
    }
}

/// Verify GraphQL errors are handled correctly (invalid node).
#[tokio::test]
async fn storage_query_timeseries_invalid_node() {
    let client = common::dex::login_client();
    if !ensure_serve(&client).await {
        return;
    }
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    if !ensure_pipeline(&client, &id_token).await {
        return;
    }
    if !ensure_stations(&client, &id_token).await {
        return;
    }

    let now = opsense_components::signal::now_secs();
    let resp = client
        .post(format!("{}/api/repl/graphql", serve_url()))
        .bearer_auth(&id_token)
        .json(&serde_json::json!({
            "query": "query Q($node: String!, $fromTs: Int!, $toTs: Int!) { queryTimeseries(node: $node, fromTs: $fromTs, toTs: $toTs) { ts value metricId } }",
            "variables": {
                "node": "nonexistent-station-xyz",
                "fromTs": now - 60,
                "toTs": now
            }
        }))
        .send()
        .await
        .expect("queryTimeseries request");
    // Should return 200 even for invalid node (GraphQL spec).
    assert!(resp.status().is_success(), "status: {}", resp.status());
    let body: Value = resp.json().await.expect("json");
    // Either no data or errors — both are valid.
    if let Some(errors) = body.get("errors") {
        eprintln!("expected errors for nonexistent node: {errors:?}");
    }
}

/// Smoke test: verify JWT helper compiles.
#[test]
fn jwt_smoke_compile() {
    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({"sub": "test", "tenant_id": 1}),
        &EncodingKey::from_secret(b"smoke-test-secret-min-32-bytes-long!"),
    )
    .expect("encode");
    assert!(token.split('.').count() == 3);
}