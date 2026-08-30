//! Read-through fallback: khi một station được gắn origin (`http_source`),
//! truy vấn một cửa sổ CŨ hơn coverage hiện tại sẽ tự động đi xuống tầng đĩa,
//! rồi re-fetch đúng cửa sổ từ origin qua `fetch_window` (reuse làm fallback),
//! và trả kết quả đã nạp lại — minh bạch với caller.
//!
//! Luồng mới (validate-based): `Station::query_all` gọi `LruCache::get_with_load`
//! thay vì `get`. Cache hit mà không đủ coverage (validate fail) → coi miss →
//! đọc đĩa → không có → gọi `fallback` (origin). Mock trả observation nằm TRONG
//! cửa sổ yêu cầu nên kết quả fallback được phục vụ ngược lại cho caller.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_components::{new_station_registry, ClockSource, CollectorSink, HttpSource, OpsenseContext};
use opsense_core::collector::Collector;
use opsense_core::registry;
use opsense_core::{Stage, Watermarks};

fn body_at(ts: i64) -> String {
    format!(r#"[{{"ts":{ts},"metric_id":"api_rps","kind":"metric","signal":"rate","value":42.0}}]"#)
}

/// Parse `start`/`end` from the request line and return the window midpoint,
/// so the mock serves an observation INSIDE the requested window.
fn parse_window_ts(line: &str) -> i64 {
    let now = opsense_components::signal::now_secs();
    let start = extract_param(line, "start").unwrap_or(now);
    let end = extract_param(line, "end").unwrap_or(now);
    (start + end) / 2
}

fn extract_param(line: &str, key: &str) -> Option<i64> {
    let needle = format!("{key}=");
    let idx = line.find(&needle)?;
    let rest = &line[idx + needle.len()..];
    let val: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    val.parse::<i64>().ok()
}

async fn spawn_mock(
    status_line: &'static str,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let reqs = requests.clone();
    let status_line = status_line.to_string();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let reqs = reqs.clone();
            let status_line = status_line.clone();
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
                            let line = String::from_utf8_lossy(&buf[..line_end])
                                .trim_end()
                                .to_string();
                            reqs.lock().unwrap().push(line.clone());
                            let ts = parse_window_ts(&line);
                            let body = body_at(ts);
                            let resp = format!(
                                "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = sock.write_all(resp.as_bytes()).await;
                        }
                        buf.clear();
                    }
                }
            });
        }
    });
    (addr, requests)
}

#[tokio::test(flavor = "multi_thread")]
async fn station_query_falls_back_to_origin_for_old_windows() {
    let (addr, requests) = spawn_mock("HTTP/1.1 200 OK").await;

    let ctx = Arc::new(OpsenseContext::new(
        Arc::new(Collector::new(vec![])),
        Watermarks::new(),
        Arc::new(BTreeMap::new()),
        new_station_registry(),
    ));

    let mut fetch = HttpSource::new("fb-fetch", &["clock"], &format!("http://{addr}/metrics"));
    fetch.params = BTreeMap::from([
        ("start".to_string(), "{{from_ts}}".to_string()),
        ("end".to_string(), "{{to_ts}}".to_string()),
    ]);

    let mut drain = CollectorSink::new();
    drain.id = "drain".to_string();
    drain.inputs = vec!["fb-fetch".to_string()];

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(50))),
        Arc::new(fetch),
        Arc::new(drain),
    ];
    runtime.reload(components).expect("valid graph");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");
    tokio::time::sleep(Duration::from_millis(600)).await;

    eprintln!("DEBUG station_ids = {:?}", registry::station_ids().await);
    let station = registry::station("fb-fetch").await.expect("auto-station");

    // Query một cửa sổ CŨ hơn coverage hiện tại -> cache hit (recent point)
    // không qua validate -> miss -> đĩa rỗng -> fallback re-fetch đúng cửa sổ.
    let now = opsense_components::signal::now_secs();
    let old_from = now - 7_200;
    let old_to = now - 6_600;
    let before = requests.lock().unwrap().len();

    let hits = station
        .read()
        .await
        .query_all(Stage::Processed, old_from, old_to)
        .await;
    assert!(
        !hits.is_empty(),
        "fallback must refill the old window with an in-window observation"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Mock phải nhận thêm request render đúng cửa sổ cũ.
    let new_requests = requests.lock().unwrap().len();
    assert!(
        new_requests > before,
        "fallback must re-fetch the old window from origin"
    );
    let hit = {
        let seen = requests.lock().unwrap();
        seen.iter().any(|line| {
            line.contains(&format!("start={}", old_from + 1)) && line.contains(&format!("end={old_to}"))
        })
    };
    assert!(hit, "fallback must re-fetch the old window from origin");

    // Luồng live vẫn tiếp diễn sau fallback.
    let after_fallback = requests.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        requests.lock().unwrap().len() > after_fallback,
        "live cycles must continue after fallback"
    );

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");
}
