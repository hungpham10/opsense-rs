//! `station_transform` đứng giữa pipeline:
//!
//! ```text
//! clock -> http(observations) -> station_transform -> processor -> persist
//! ```
//!
//! Asserts: trạm đăng ký handle theo id (`ts_query`/grid đọc được), snapshot
//! đúng stage cấu hình (raw vì đứng trước processor), VÀ downstream vẫn nhận
//! dữ liệu — pass-through không chặn luồng.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use opsense_components::new_station_registry;
use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_components::{
    ClockSource, CollectorSink, HttpSource, OpsenseContext, ProcessorTransform,
    TimeseriesStationTransform,
};
use opsense_core::collector::Collector;
use opsense_core::registry;
use opsense_core::{Stage, Watermarks};

const BODY: &str =
    r#"[{"ts":1700000001,"metric_id":"api_rps","kind":"metric","signal":"rate","value":7.5}]"#;

async fn spawn_mock(status_line: &'static str, body: &'static str) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let body = body;
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
                    "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn station_transform_snapshots_and_forwards() {
    let addr = spawn_mock("HTTP/1.1 200 OK", BODY).await;

    let ctx = Arc::new(OpsenseContext::new(
        Arc::new(Collector::new(vec![])),
        Watermarks::new(),
        Arc::new(BTreeMap::new()),
        new_station_registry(),
    ));

    let fetch = HttpSource::new("fetch", &["clock"], &format!("http://{addr}/metrics"));
    let mut mid_station = TimeseriesStationTransform::new("mid-station", &["fetch"]);
    mid_station.stage = "raw".to_string(); // đứng trước processor -> snapshot raw
    let mut processor = ProcessorTransform::new();
    processor.inputs = vec!["mid-station".to_string()];
    let mut drain = CollectorSink::new();
    drain.id = "drain".to_string();
    drain.inputs = vec!["processor".to_string()];

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(50))),
        Arc::new(fetch),
        Arc::new(mid_station),
        Arc::new(processor),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("valid graph");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_millis(350)).await;
    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    // 1) Trạm đăng ký theo id và chứa batch raw (đứng trước processor).
    let station = registry::station("mid-station")
        .await
        .expect("station_transform must publish its handle");
    let points = station
        .read()
        .await
        .query(Stage::Raw, "api_rps", 0, i64::MAX)
        .await;
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value, 7.5);

    // 2) Downstream không bị chặn: processor vẫn sinh chuỗi processed.
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
        "pass-through must keep the downstream chain flowing"
    );
}
