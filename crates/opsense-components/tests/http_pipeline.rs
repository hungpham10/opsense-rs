//! End-to-end loop for the HTTP source node: a mock TCP endpoint answers
//! a JSON array of observations, the bindings evaluate, the request is
//! rendered and sent, and the parsed observations land in the node's own
//! `Timeseries` station.
//!
//! ```text
//! clock -> http(observations) -> output
//! ```
//!
//! This file is the minimum-viable smoke test for the rewritten `http.rs`:
//! it only covers the happy path (the body parses, the station receives the
//! batch, the bindings are interpolated). Storage tier, fallback, backfill
//! and failing-endpoint semantics are exercised in the other `http_*` tests
//! (currently `#[ignore]`'d while the old APIs are gone).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opsense_components::http::HttpSource;
use opsense_components::signal;
use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_libs::vector::components::clock::Clock;
use opsense_libs::vector::components::output::Output;

use opsense_core::Config;
use opsense_core::Context;
use opsense_model::secret::Secret;

/// Serve a canned response to every request and record each request line.
async fn spawn_mock(
    status_line: &'static str,
    body: &'static str,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));

    let reqs = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let reqs = reqs.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.ends_with(b"\r\n\r\n") {
                    let n = sock.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                if let Some(line_end) = buf.iter().position(|&b| b == b'\n') {
                    reqs.lock().unwrap().push(
                        String::from_utf8_lossy(&buf[..line_end])
                            .trim_end()
                            .to_string(),
                    );
                }
                let resp = format!(
                    "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (addr, requests)
}

async fn context_with_attributes(attributes: HashMap<String, String>) -> Arc<Context> {
    // `Config`'s sub-structs all carry `#[serde(default)]`, so a JSON `{}`
    // parses cleanly into a default config; we then patch `attributes` so the
    // test's lookup table matches what the production code reads.
    let mut cfg: Config = serde_json::from_str("{}").expect("default config");
    cfg.attributes = attributes;
    let secret = Secret::new().await.expect("Secret::new");
    Arc::new(Context::new(&cfg, Arc::new(secret)))
}

const BODY: &str = r#"[{"ts":1700000001,"metric_id":"api_rps","kind":"metric","signal":"rate","value":42.0,"labels":{"dc":"hcm"}}]"#;

#[tokio::test]
async fn http_fetch_writes_observations_into_own_station() {
    let (addr, requests) = spawn_mock("HTTP/1.1 200 OK", BODY).await;

    let ctx =
        context_with_attributes(HashMap::from([("site".to_string(), "hcm".to_string())])).await;

    // url: port from `payload.port` would need the clock to forward `port`; the
    // simpler shape for this smoke test is a constant URL with one
    // interpolation point — the attribute `site`.
    let mut fetch = HttpSource::new(
        "fetch-ok",
        &["clock"],
        &format!("http://{addr}/metrics/{{{{site}}}}"),
    );
    fetch.bindings = HashMap::from([("site".to_string(), "attr(\"site\")".to_string())]);
    fetch.interval_secs = 1;
    fetch.timeout_secs = 5;

    let drain = Output {
        id: "drain".to_string(),
        inputs: vec!["fetch-ok".to_string()],
    };

    let mut runtime = Runtime::new();
    runtime.set_context(ctx.clone());
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(Clock::new(Duration::from_millis(100))),
        Arc::new(fetch),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("valid graph");

    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_millis(500)).await;
    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    // 1. request went out, with `site` rendered into the path.
    let last = {
        let seen = requests.lock().unwrap();
        seen.last().expect("mock endpoint was called").clone()
    };
    assert!(
        last.starts_with("GET /metrics/hcm "),
        "request line: {last}"
    );

    // 2. node station registered and received the parsed observation.
    let station = ctx
        .station::<Arc<tokio::sync::RwLock<opsense_core::TimeseriesStation>>>("fetch-ok")
        .await
        .expect("http source must own a station");
    let rows = station
        .write()
        .await
        .query_range(1700000001, 1700000001)
        .unwrap_or_default();
    assert_eq!(rows.len(), 1, "parsed observation must be stored");
    assert_eq!(rows[0].value, 42.0);
    assert_eq!(rows[0].metric_id, "api_rps");
    assert_eq!(rows[0].labels.get("dc").map(String::as_str), Some("hcm"));
}

#[tokio::test]
async fn http_component_deserializes_from_config() {
    let minimal: Box<dyn Component> = serde_json::from_value(serde_json::json!({
        "type": "http_source",
        "id": "fetch",
        "inputs": ["clock"],
        "url": "http://svc/api",
    }))
    .expect("minimal node must parse with documented defaults");
    assert_eq!(minimal.id(), "fetch");

    let full: Box<dyn Component> = serde_json::from_value(serde_json::json!({
        "type": "http_source",
        "id": "prom",
        "inputs": ["clock"],
        "url": "{{prom_url}}/api/v1/query_range",
        "method": "POST",
        "headers": {"Authorization": "Bearer {{token}}"},
        "body": null,
        "bindings": {"from": "sub_secs(ts(), interval())", "to": "ts()"},
        "interval_secs": 30,
        "timeout_secs": 5,
        "station": true,
    }))
    .expect("every documented field must parse");
    assert_eq!(full.id(), "prom");

    // Unknown field rejected by `#[serde(deny_unknown_fields)]`.
    let unknown = serde_json::json!({
        "type": "http_source",
        "id": "x",
        "inputs": [],
        "url": "http://x",
        "wat": 1,
    });
    assert!(serde_json::from_value::<Box<dyn Component>>(unknown).is_err());
}

// Touch the symbol so dead-code lints stay quiet on the smoke test.
#[allow(dead_code)]
fn _signal_used() {
    let _ = signal::tick(0);
}
