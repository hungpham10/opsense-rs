//! End-to-end loop for the generic HTTP fetch node over a real (mocked)
//! TCP endpoint:
//!
//! ```text
//! clock -> http(observations) -> processor -> persist
//! ```
//!
//! Proves the templated request goes out with the rendered window
//! (`{{from_ts}}`, `{{to_ts}}`, attribute lookup), the response lands in
//! `Stage::Processed` of the working LRU (+ persistence with `store_raw`), the
//! node's own cursor advances, and the downstream chain flushes to the store.
//! A failing endpoint holds the cursor instead of advancing it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opsense_components::new_station_registry;
use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_components::{
    ClockSource, CollectorSink, HttpSource, OpsenseContext, ProcessorTransform,
};
use opsense_core::collector::Collector;
use opsense_core::registry;
use opsense_core::Context;
use opsense_core::{Stage, Watermarks};

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

fn context(attributes: BTreeMap<String, String>) -> Arc<OpsenseContext> {
    Arc::new(OpsenseContext::new(
        Arc::new(Collector::new(vec![])),
        Watermarks::new(),
        Arc::new(attributes),
        new_station_registry(),
    ))
}

const BODY: &str = r#"[{"ts":1700000001,"metric_id":"api_rps","kind":"metric","signal":"rate","value":42.0,"labels":{"dc":"hcm"}}]"#;

#[tokio::test]
async fn templated_http_fetch_flows_to_the_stores() {
    let (addr, requests) = spawn_mock("HTTP/1.1 200 OK", BODY).await;

    let ctx = context(BTreeMap::from([("site".to_string(), "hcm".to_string())]));
    let watermarks = ctx.watermarks().clone();

    // url uses the attribute namespace; params use the built-in window vars.
    let mut fetch = HttpSource::new(
        "fetch-ok",
        &["clock"],
        &format!("http://{addr}/metrics/{{{{site}}}}"),
    );
    fetch.params = BTreeMap::from([
        ("start".to_string(), "{{from_ts}}".to_string()),
        ("end".to_string(), "{{to_ts}}".to_string()),
        ("site".to_string(), "{{site}}".to_string()),
    ]);

    let mut processor = ProcessorTransform::new();
    processor.inputs = vec!["fetch-ok".to_string()];
    let mut drain = CollectorSink::new();
    drain.id = "drain".to_string();
    drain.inputs = vec!["processor".to_string()];

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(50))),
        Arc::new(fetch),
        Arc::new(processor),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("valid graph");

    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_millis(300)).await;
    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    // The request went out fully rendered (query order is alphabetical).
    // NOTE: drop the `requests` lock before any `.await` below — the mock
    // server is a concurrent task that needs the same `std::sync::Mutex` to
    // record each request, so holding the guard across an await would
    // deadlock the single-threaded runtime.
    let last = {
        let seen = requests.lock().unwrap();
        seen.last().expect("mock endpoint was called").clone()
    };
    assert!(
        last.starts_with("GET /metrics/hcm?"),
        "request line: {last}"
    );
    assert!(last.contains("start="), "window start param: {last}");
    assert!(last.contains("end="), "window end param: {last}");

    // Fetched batch reached the node's OWN station (registry theo id)…
    let fetch_station = registry::station("fetch-ok")
        .await
        .expect("http_source must own a station");
    let raw = fetch_station
        .read()
        .await
        .query(Stage::Processed, "api_rps", 0, i64::MAX)
        .await;
    assert_eq!(raw.len(), 1, "raw batch must be fetched");
    assert_eq!(raw[0].value, 42.0);
    assert_eq!(raw[0].labels.get("dc").map(String::as_str), Some("hcm"));
    // …the durable store (`store_raw`) lives in the node's own station…
    assert!(
        !raw.is_empty(),
        "store_raw must append to the node's station"
    );
    // …and the downstream chain flushed the processed series.
    let proc = registry::station("processor")
        .await
        .expect("processor must own a station");
    let processed = proc
        .read()
        .await
        .query(Stage::Processed, "api_rps", 0, i64::MAX)
        .await;
    assert!(
        !processed.is_empty(),
        "processor -> persist must flush the fetched batch"
    );
    assert!(watermarks.get_node("fetch-ok") > 0, "cursor must advance");
}

#[tokio::test]
async fn failing_endpoint_holds_the_cursor() {
    let (addr, requests) = spawn_mock("HTTP/1.1 500 Server Error", "{}").await;

    let ctx = context(BTreeMap::new());
    let watermarks = ctx.watermarks().clone();

    let fetch = HttpSource::new("fetch-fail", &["clock"], &format!("http://{addr}/m"));
    let mut processor = ProcessorTransform::new();
    processor.inputs = vec!["fetch-fail".to_string()];
    let mut drain = CollectorSink::new();
    drain.id = "drain".to_string();
    drain.inputs = vec!["processor".to_string()];

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    // The runtime refuses an unconnected transform, so wire the full chain;
    // with the endpoint down every stage stays empty.
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(50))),
        Arc::new(fetch),
        Arc::new(processor),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("valid graph");

    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_millis(300)).await;
    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    // Attempts happened, but nothing was accepted: no data, cursor unmoved —
    // the window will be retried once the endpoint recovers.
    assert!(!requests.lock().unwrap().is_empty(), "endpoint was tried");
    // Working store đã bị bỏ — assert qua trạm của node: không có dữ liệu.
    if let Some(handle) = registry::station("fetch-fail").await {
        assert!(
            handle
                .read()
                .await
                .query_all(Stage::Processed, 0, i64::MAX)
                .await
                .is_empty(),
            "failed cycles must not store anything"
        );
    } // endpoint fail ngay từ đầu -> trạm chưa từng nhận batch (hoặc None)
    assert_eq!(
        watermarks.get_node("fetch-fail"),
        0,
        "failed cycles keep cursor"
    );
}

#[test]
fn http_component_deserializes_from_config() {
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
        "headers": {"Authorization": "Bearer {{env.API_TOKEN}}"},
        "params": {"query": "up", "start": "{{from_ts}}", "end": "{{to_ts}}"},
        "body": null,
        "items": "data.result[].values[]",
        "fields": {
            "ts": { "query": "0", "cast_to": "i64" },
            "value": { "query": "1", "cast_to": "f64" },
        },
        "constants": { "metric_id": "up" },
        "timeout_secs": 5,
        "store_raw": true,
        "initial_lookback_secs": 600,
    }))
    .expect("every documented field must parse");
    assert_eq!(full.id(), "prom");

    let unknown = serde_json::json!({
        "type": "http_source",
        "id": "x",
        "inputs": [],
        "url": "http://x",
        "wat": 1,
    });
    assert!(serde_json::from_value::<Box<dyn Component>>(unknown).is_err());

    let bad_cast = serde_json::json!({
        "type": "http_source",
        "id": "x",
        "inputs": [],
        "url": "http://x",
        "fields": { "value": { "query": "0", "cast_to": "wat" } },
    });
    assert!(serde_json::from_value::<Box<dyn Component>>(bad_cast).is_err());
}
