//! Từ refactor "station là cache/storage duy nhất": MỌI http_source đều tự có
//! trạm riêng theo node id — batch fetch luôn nằm trong registry, đọc được qua
//! `opsense_core::store::station(id)` (Rhai `ts_query` / grid dùng cùng đường này).
//! Flag `station` cũ chỉ còn là tuỳ chọn tương thích (không ảnh hưởng việc
//! đăng ký).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_components::{
    new_station_registry, ClockSource, CollectorSink, HttpSource, OpsenseContext,
};
use opsense_core::collector::Collector;
use opsense_core::registry;
use opsense_core::{Stage, Watermarks};

const BODY: &str =
    r#"[{"ts":1700000001,"metric_id":"api_rps","kind":"metric","signal":"rate","value":42.0}]"#;

/// Mock trả BODY cho mọi request và ghi lại từng request line.
async fn spawn_mock(
    status_line: &'static str,
    body: &'static str,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let reqs = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let reqs = reqs.clone();
            let body = body.to_string();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    let n = sock.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
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
                        buf.clear();
                    }
                }
            });
        }
    });
    (addr, requests)
}

fn context() -> Arc<OpsenseContext> {
    Arc::new(OpsenseContext::new(
        Arc::new(Collector::new(vec![])),
        Watermarks::new(),
        Arc::new(BTreeMap::new()),
        new_station_registry(),
    ))
}

async fn run_source(
    id: &str,
    station_flag: bool,
    addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
) -> usize {
    let ctx = context();
    let fetch = HttpSource::new(id, &["clock"], &format!("http://{addr}/metrics"));
    if station_flag {
        // Flag cũ: không còn quyết định việc đăng ký (auto-station luôn bật).
    }

    let mut drain = CollectorSink::new();
    drain.id = "drain".to_string();
    drain.inputs = vec![id.to_string()];

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(50))),
        Arc::new(fetch),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("valid graph");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_millis(400)).await;
    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    requests.lock().unwrap().len()
}

#[tokio::test]
async fn every_source_owns_a_station_and_data_flows() {
    let (addr, requests) = spawn_mock("HTTP/1.1 200 OK", BODY).await;

    run_source("no-station-src", false, addr, requests.clone()).await;
    assert!(
        !requests.lock().unwrap().is_empty(),
        "mock must have been called"
    );
    let plain = registry::station("no-station-src")
        .await
        .expect("auto-station exists");
    let points = plain
        .read()
        .await
        .query(Stage::Raw, "api_rps", 0, i64::MAX)
        .await;
    assert_eq!(points.len(), 1, "batch phải nằm trong trạm của node");
    assert_eq!(points[0].value, 42.0);

    // Chu kỳ kế tiếp tái dùng cùng trạm (block store giữ dữ liệu).
    run_source("src-station-a", true, addr, requests.clone()).await;
    let handle = registry::station("src-station-a").await.unwrap();
    let points = handle
        .read()
        .await
        .query(Stage::Raw, "api_rps", 0, i64::MAX)
        .await;
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value, 42.0);

    run_source("src-station-a", true, addr, requests.clone()).await;
    let handle = registry::station("src-station-a").await.unwrap();
    assert!(
        !handle
            .read()
            .await
            .query(Stage::Raw, "api_rps", 0, i64::MAX)
            .await
            .is_empty(),
        "second cycle must reuse the same station store"
    );
}
