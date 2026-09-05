//! The declarative jq HTTP story end-to-end: an `http_source` fetches a
//! Prometheus-shaped `/api/v1/query_range` response from a mocked endpoint and
//! maps it into observations purely through `items` + `fields` (`jq` paths) —
//! no script engine involved.
//!
//! This is the proof that dropping the dedicated Prometheus adapter lost
//! nothing: the generic node extracts arbitrary shapes declaratively.

use std::collections::BTreeMap;
use std::sync::Arc;

use opsense_components::vector::runtime::{Component, Message, Outbound};
use opsense_components::{OpsenseContext, new_station_registry, signal};
use opsense_core::Context;
use opsense_core::collector::Collector;
use opsense_core::registry;
use opsense_core::{Stage, Watermarks};

/// Serve a canned Prometheus matrix response and record request lines.
async fn spawn_mock(body: &'static str) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
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
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    addr
}

const PROM_BODY: &str = r#"{
    "status": "success",
    "data": {
        "resultType": "matrix",
        "result": [
            {
                "metric": {"__name__": "cpu_usage", "instance": "host-a", "job": "node"},
                "values": [[1700000000, "32.5"], [1700000060, "40.0"]]
            }
        ]
    }
}"#;

#[tokio::test]
async fn http_node_maps_prometheus_through_jq() {
    opsense_rhai::register();

    let addr = spawn_mock(PROM_BODY).await;

    let ctx = Arc::new(OpsenseContext::new(
        Arc::new(Collector::new(vec![])),
        Watermarks::new(),
        Arc::new(BTreeMap::from([(
            "prom_url".to_string(),
            format!("http://{addr}"),
        )])),
        new_station_registry(),
    ));
    let watermarks = ctx.watermarks().clone();

    // Built exactly as it would appear under `[pipeline.components]`. The matrix
    // response is flattened with `items` walking down to each sample, then
    // `fields` picks the timestamp/value out of the `[ts, "val"]` pair and
    // `constants` supplies the fixed metric id + labels.
    let component: Box<dyn Component> = serde_json::from_value(serde_json::json!({
        "type": "http_source",
        "id": "prom",
        "inputs": ["clock"],
        "url": "{{prom_url}}/api/v1/query_range",
        "items": "data.result[].values[]",
        "fields": {
            "ts": { "query": "0", "cast_to": "i64" },
            "value": { "query": "1", "cast_to": "f64" },
        },
        "constants": {
            "metric_id": "cpu_usage",
            "labels": { "instance": "host-a", "job": "node" },
        },
        "params": {
            "query": "rate(node_cpu_seconds_total[5m])",
            "start": "{{from_ts}}",
            "end": "{{to_ts}}",
            "step": "60",
        },
        "initial_lookback_secs": 600,
    }))
    .expect("jq http node must deserialize");

    // One tick at a fixed timestamp, then close the input.
    let now = 1_700_000_100i64;
    let (tx_in, mut rx) = tokio::sync::mpsc::channel::<Message>(4);
    tx_in.send(signal::tick(now)).await.expect("send tick");
    drop(tx_in);

    let (down_tx, mut down_rx) = tokio::sync::mpsc::channel(4);
    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::channel(4);
    // Drain runtime events so nothing blocks.
    tokio::spawn(async move { while ev_rx.recv().await.is_some() {} });

    component
        .run(
            0,
            &mut rx,
            Outbound {
                streams: vec![down_tx],
                broadcast: None,
                event: ev_tx,
                ctx: Some(ctx),
            },
        )
        .await
        .expect("cycle must succeed");

    // Downstream received data_ready(now).
    assert_eq!(down_rx.recv().await.and_then(|m| signal::ts(&m)), Some(now));
    // Cursor advanced exactly to the tick.
    assert_eq!(watermarks.get_node("prom"), now);

    // The script mapped the matrix into observations, labels preserved.
    // Model mới: batch nằm trong TRẠM RIÊNG của node (không còn working store).
    let prom_station = registry::station("prom")
        .await
        .expect("http_source must own its station");
    let obs = prom_station
        .read()
        .await
        .query(Stage::Processed, "cpu_usage", 0, i64::MAX)
        .await;
    assert_eq!(obs.len(), 2, "two samples expected");
    assert_eq!(obs[0].ts, 1_700_000_000);
    assert_eq!(obs[0].value, 32.5);
    assert_eq!(obs[1].value, 40.0);
    assert_eq!(
        obs[0].labels.get("instance").map(String::as_str),
        Some("host-a")
    );
    assert_eq!(obs[0].labels.get("job").map(String::as_str), Some("node"));
}

#[tokio::test]
async fn registered_runner_executes_inline_scripts() {
    opsense_rhai::register();

    let runner = opsense_core::script::script_runner().expect("runner must be registered");
    let out = runner
        .run(
            r#"fn process(body) { [ #{ ts: body.ts, metric_id: "m", kind: "metric", signal: "rate", value: body.v * 2.0 } ] }"#,
            "",
            serde_json::json!({"ts": 5, "v": 21.0}),
        )
        .await
        .expect("inline script runs");

    let obs: Vec<opsense_model::Observation> = out
        .into_iter()
        .map(|item| serde_json::from_value(item).expect("observation shape"))
        .collect();
    assert_eq!(obs.len(), 1);
    assert_eq!(obs[0].metric_id, "m");
    assert_eq!(obs[0].value, 42.0);
}
