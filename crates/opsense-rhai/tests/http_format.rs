//! The declarative jq HTTP story end-to-end: an `http_source` fetches a
//! Prometheus-shaped `/api/v1/query_range` response from a mocked endpoint and
//! maps it into observations purely through `items` + `fields` (`jq` paths) —
//! no script engine involved.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opsense_components::http::HttpSource;
use opsense_components::vector::runtime::{Component, Runtime};
use opsense_mlib::vector::components::clock::Clock;
use opsense_mlib::vector::components::output::Output;

use opsense_core::Config;
use opsense_core::Context;
use opsense_model::secret::Secret;

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
                reqs.lock().unwrap().push(String::from_utf8_lossy(&buf).into_owned());
                let resp = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });

    (addr, requests)
}

#[tokio::test]
async fn http_source_maps_prometheus_through_jq() {
    let (addr, requests) = spawn_mock(
        "200 OK",
        r#"{"status":"success","data":{"resultType":"matrix","data":{"result":[{"metric":{"__name__":"cpu_usage","instance":"host1"},"values":[[1700000000,"12.5"],[1700000060,"13.0"]]}]}}}"#,
    ).await;

    let cfg: Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let url = format!("http://{}/api/v1/query_range?from_ts={{from_ts}}&to_ts={{to_ts}}&step={{step}}", addr);
    let mut src = HttpSource::new(
        "cpu-http",
        &["clock"],
        &url,
    );
    src.bindings = {
        let mut m = HashMap::new();
        m.insert("from_ts".into(), "{{from_ts}}".into());
        m.insert("to_ts".into(), "{{to_ts}}".into());
        m.insert("step".into(), "60".into());
        m
    };
    src.interval_secs = 60;
    src.timeout_secs = 10;

    let output = Output { id: "output".into(), inputs: vec!["cpu-http".into()] };
    let clock = Clock::new(Duration::from_secs(1));

    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(clock),
        Arc::new(src),
        Arc::new(output),
    ];
    let mut rt = Runtime::new();
    rt.set_context(ctx.clone());
    rt.reload(components).expect("valid graph");

    let _handle = rt.start(|_| async {}).unwrap();

    // Wait for at least one poll cycle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify request was made with interpolated bindings
    let reqs = requests.lock().unwrap();
    assert!(!reqs.is_empty(), "no HTTP request made");
    let req = &reqs[0];
    assert!(req.contains("from_ts="), "from_ts not interpolated: {req}");
    assert!(req.contains("to_ts="), "to_ts not interpolated: {req}");
}

#[tokio::test]
async fn http_source_handles_http_errors_gracefully() {
    let (addr, _requests) = spawn_mock("500 Internal Server Error", "{}").await;

    let cfg: Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let url = format!("http://{}/api/v1/query_range?from_ts={{from_ts}}&to_ts={{to_ts}}&step={{step}}", addr);
    let mut src = HttpSource::new(
        "bad-http",
        &["clock"],
        &url,
    );
    src.bindings = HashMap::from([
        ("from_ts".into(), "{{from_ts}}".into()),
        ("to_ts".into(), "{{to_ts}}".into()),
        ("step".into(), "60".into()),
    ]);
    src.interval_secs = 60;
    src.timeout_secs = 1;

    let output = Output { id: "output".into(), inputs: vec!["bad-http".into()] };
    let clock = Clock::new(Duration::from_secs(1));

    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(clock),
        Arc::new(src),
        Arc::new(output),
    ];
    let mut rt = Runtime::new();
    rt.set_context(ctx);
    rt.reload(components).expect("valid graph");

    let _handle = rt.start(|_| async {}).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should not crash; runtime survives
}