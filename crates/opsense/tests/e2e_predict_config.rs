//! End-to-end drive bằng chính config `strategies/predict/config.toml`.
//!
//! Hai tầng:
//!   1. `config_file_contract` — nhanh, không cần infra: load + validate config
//!      thật, khẳng định các knobs `[storage]` / `[storage.s3]` được áp dụng và
//!      graph pipeline 4 components (`clock`, `live-feed` http, `predict` rhai
//!      có `params`, `tsdb`) deserialize qua typetag registry.
//!   2. `full_pipeline_checks_parquet_s3` — full flow của config: chạy runtime
//!      thật, re-point http source duy nhất (`live-feed`) về mock deterministic
//!      trả history window (cpu_ramp 3→18 + cpu_steady const) VÀ live samples
//!      (cpu_live_ramp=999, cpu_live_steady=42) trong MỘT response.
//!      `predict` nhận message từ 2 input (clock + live-feed) và branch theo
//!      trigger (`payload.src`): clock → recompute bảng dự đoán qua
//!      `station_query(...)` (grid/transition, `labels.check="prediction"`);
//!      live-feed → check live sample vs prediction cũ trong own station
//!      (`labels.check="result"`, value 1.0/0.0). Khẳng định:
//!        - live-feed station nhận observations raw (history + live),
//!        - predict station có prediction + check true (1.0) + check false (0.0),
//!        - parquet local dưới `<data_dir>/tsdb-timeseries/ts/blk=*`,
//!        - mirror S3 (khi MinIO reachable): `opsense-lake/test-case-predict/tsdb/ts/**`.
//!
//! `data_dir` / `s3.endpoint` của strategy config là đường dẫn docker — test
//! override sang temp dir / env `OPSENSE_S3_ENDPOINT` (mặc định
//! `http://127.0.0.1:9000`).
//!
//! S3 follow repo convention (skip graceful):
//!   docker compose up -d minio minio-bucket
//!   cargo test -p opsense --test e2e_predict_config -- --nocapture
//!
//! Trong CI (`OPSENSE_INTEGRATION=true`), MinIO thiếu = panic để không green oan.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use object_store::{ObjectStore, aws::AmazonS3Builder};
use opsense::api::pipeline_from_config;
use opsense_components::http::HttpSource;
use opsense_components::signal;
use opsense_components::station::TimeseriesStationSink;
use opsense_components::vector::runtime::Runtime;
use opsense_core::{Config, Context, Observation, TimeseriesStation};
use opsense_mlib::vector::components::clock::Clock;
use opsense_model::secret::Secret;
use opsense_rhai::RhaiTransform;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

const CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../strategies/predict/config.toml"
);

const S3_PREFIX: &str = "test-case-predict";
const STATION_TSDB: &str = "tsdb";
const STATION_SOURCE: &str = "live-feed";
const STATION_PREDICT: &str = "predict";

fn s3_endpoint() -> String {
    std::env::var("OPSENSE_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into())
}

fn s3_user() -> String {
    std::env::var("OPSENSE_S3_ACCESS_KEY_ID").unwrap_or_else(|_| "opsense".into())
}

fn s3_pass() -> String {
    std::env::var("OPSENSE_S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "opsense123".into())
}

/// Body mock cho http source DUY NHẤT (`live-feed`) — history window + live
/// samples trong MỘT response:
///   - cpu_ramp 3.0 → 18.0, 21 điểm, ts cách 3s (ts ≡ now mod 3),
///   - cpu_steady hằng 42.0, 20 điểm (ts ≡ now+1 mod 3),
///   - cpu_live_ramp = 999 (đột biến, lệch xa prediction ~18) → check false,
///   - cpu_live_steady = 42 (khớp prediction steady) → check true.
/// Mọi ts dời về quá khứ (max ≤ now-60). Từng metric ts khác nhau vì
/// `update_range` dedup theo ts — và live sample phải khác ts prediction để
/// tsdb không dedup chúng về cùng hàng.
fn source_body(now: i64) -> String {
    let mut arr = Vec::new();
    for i in 0..=20 {
        arr.push(serde_json::json!({
            "ts": now - 120 + 3 * i,
            "metric_id": "cpu_ramp",
            "kind": "metric",
            "signal": "raw",
            "value": 3.0 + 0.75 * i as f64,
            "labels": {"phase": "ramp"}
        }));
    }
    for i in 0..=19 {
        arr.push(serde_json::json!({
            "ts": now - 119 + 3 * i,
            "metric_id": "cpu_steady",
            "kind": "metric",
            "signal": "raw",
            "value": 42.0,
            "labels": {"phase": "steady"}
        }));
    }
    // Live samples — metric riêng `*_live_*` (script chỉ fit grid history
    // không chứa "_live_", nên spike 999 không nhiễu forecast).
    arr.push(serde_json::json!({
        "ts": now - 30, "metric_id": "cpu_live_ramp", "kind": "metric",
        "signal": "raw", "value": 999.0, "labels": {"src": "live"}
    }));
    arr.push(serde_json::json!({
        "ts": now - 31, "metric_id": "cpu_live_steady", "kind": "metric",
        "signal": "raw", "value": 42.0, "labels": {"src": "live"}
    }));
    serde_json::json!(arr).to_string()
}

/// Mock HTTP server (deterministic, đúng pattern e2e_prometheus_config).
async fn spawn_mock(body: String) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));

    let reqs = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let reqs = reqs.clone();
            let body = body.clone();
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
                reqs.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf).into_owned());
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });

    (addr, requests)
}

async fn wait_minio(endpoint: &str) -> bool {
    let client = reqwest::Client::new();
    for _ in 0..30 {
        if client
            .get(format!("{endpoint}/minio/health/live"))
            .send()
            .await
            .is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

/// Same logic với parquet.rs — path-style cho MinIO.
fn s3_store(endpoint: &str) -> Arc<dyn ObjectStore> {
    let mut b = AmazonS3Builder::new().with_bucket_name("opsense-lake");
    b = b.with_access_key_id(s3_user());
    b = b.with_secret_access_key(s3_pass());
    b = b
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false);
    Arc::new(b.build().expect("build S3 client"))
}

async fn list_keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stream = store.list(Some(&object_store::path::Path::from(prefix)));
    while let Some(result) = stream.next().await {
        match result {
            Ok(obj) => out.push(obj.location.to_string()),
            Err(_) => break,
        }
    }
    out.sort();
    out
}

/// Đợi station được registry vào Context (components đăng ký tại start).
async fn wait_for_station(
    ctx: &Arc<Context>,
    id: &str,
    timeout_secs: u64,
) -> Arc<RwLock<TimeseriesStation>> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(st) = ctx.station::<Arc<RwLock<TimeseriesStation>>>(id).await {
            return st;
        }
        if Instant::now() >= deadline {
            panic!("station `{id}` chưa được registry sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Đợi station có dữ liệu (query lenient `query_recent` — không cần biết
/// coverage chính xác của window).
async fn wait_station_data(
    ctx: &Arc<Context>,
    id: &str,
    from: i64,
    to: i64,
    timeout_secs: u64,
) -> Vec<Observation> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(st) = ctx.station::<Arc<RwLock<TimeseriesStation>>>(id).await {
            let obs = st
                .write()
                .await
                .query_recent(from, to)
                .await
                .unwrap_or_default();
            if !obs.is_empty() {
                return obs;
            }
        }
        if Instant::now() >= deadline {
            panic!("station `{id}` chưa có observation sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Đợi predict station có đủ prediction + check true (1.0) + check false (0.0).
async fn wait_predict_checks(
    ctx: &Arc<Context>,
    from: i64,
    to: i64,
    timeout_secs: u64,
) -> Vec<Observation> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(st) = ctx
            .station::<Arc<RwLock<TimeseriesStation>>>(STATION_PREDICT)
            .await
        {
            let obs = st
                .write()
                .await
                .query_recent(from, to)
                .await
                .unwrap_or_default();
            let has_prediction = obs
                .iter()
                .any(|o| o.labels.get("check").map(String::as_str) == Some("prediction"));
            let has_true = obs.iter().any(|o| {
                o.labels.get("check").map(String::as_str) == Some("result") && o.value == 1.0
            });
            let has_false = obs.iter().any(|o| {
                o.labels.get("check").map(String::as_str) == Some("result") && o.value == 0.0
            });
            if has_prediction && has_true && has_false {
                return obs;
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "predict station chưa đủ prediction + check true + check false sau {timeout_secs}s"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Có file `.parquet` dưới `<root>/ts/**` chưa?
fn has_parquet(root: &Path) -> bool {
    fn walk(dir: &Path, found: &mut bool) {
        if *found {
            return;
        }
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, found);
                } else if p.extension().is_some_and(|x| x == "parquet") {
                    *found = true;
                }
            }
        }
    }
    let mut found = false;
    walk(&root.join("ts"), &mut found);
    found
}

fn dump_tree(root: &Path) -> String {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>, depth: usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            let rel = p
                .strip_prefix(root)
                .map(|r| r.display().to_string())
                .unwrap_or_else(|_| p.display().to_string());
            out.push(format!("{}{}", "  ".repeat(depth), rel));
            if p.is_dir() {
                walk(root, &p, out, depth + 1);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out, 0);
    out.truncate(40);
    out.join("\n")
}

/// ── Tầng 1: config file tự nó phải parse + tất cả setting thực sự được áp ──
#[test]
fn config_file_contract() {
    let cfg = Config::load(Path::new(CONFIG_PATH))
        .expect("strategies/predict/config.toml phải parse + validate");

    assert_eq!(cfg.engine.poll_interval_seconds, 10);
    assert_eq!(cfg.engine.cache_block_seconds, 5);
    assert_eq!(cfg.engine.cache_max_blocks, 12);
    assert_eq!(cfg.storage.backend, "parquet");
    assert_eq!(cfg.storage.block_secs, 5);
    assert_eq!(cfg.storage.retention_secs, 300);
    assert_eq!(
        cfg.storage.s3_flush_interval_secs, 15,
        "s3_flush_interval_secs nằm ở [storage] (không phải [storage.s3])"
    );
    assert_eq!(cfg.storage.s3_snapshot_interval_secs, 60);

    let s3 = cfg
        .storage
        .s3
        .as_ref()
        .expect("config phải khai [storage.s3]");
    assert_eq!(s3.bucket, "opsense-lake");
    assert_eq!(s3.prefix, S3_PREFIX);
    assert_eq!(s3.url_style.as_deref(), Some("path"));

    // Graph 4 components qua typetag registry.
    let graph = pipeline_from_config(&cfg).expect("components của config phải deserialize");
    assert_eq!(
        graph.len(),
        4,
        "config khai clock + live-feed (http source duy nhất) + rhai predict + timeseries_station_sink"
    );

    let clock = graph[0]
        .as_any()
        .downcast_ref::<Clock>()
        .expect("component 0 = clock");
    assert_eq!(clock.id, "clock");
    assert_eq!(clock.interval_secs, 10);

    // 1 http source duy nhất (vừa là nguồn history cho grid vừa nguồn live
    // sample) — predict query station của nó qua station_query.
    let live = graph[1]
        .as_any()
        .downcast_ref::<HttpSource>()
        .expect("component 1 = http_source live-feed");
    assert_eq!(live.id, STATION_SOURCE);
    assert_eq!(live.inputs, vec!["clock".to_string()]);
    assert!(
        live.station,
        "live-feed phải có station=true để station_query đọc"
    );
    assert!(
        live.url.contains("prometheus.demo.prometheus.io"),
        "{}",
        live.url
    );

    // `predict` nhận message từ 2 input và branch theo trigger
    // (`payload.src`, native `trigger()`): clock → recompute, live-feed → check.
    let predict = graph[2]
        .as_any()
        .downcast_ref::<RhaiTransform>()
        .expect("component 2 = rhai_transform predict");
    assert_eq!(predict.id, STATION_PREDICT);
    assert_eq!(predict.inputs, vec!["clock".to_string(), "live-feed".to_string()]);
    assert_eq!(predict.script_path, "strategies/predict/predict.rhai");
    assert_eq!(
        predict.params.get("window_source").and_then(|v| v.as_str()),
        Some("live-feed")
    );
    assert_eq!(
        predict.params.get("live_source").and_then(|v| v.as_str()),
        Some("live-feed")
    );
    assert_eq!(
        predict.params.get("own_station").and_then(|v| v.as_str()),
        Some(STATION_PREDICT)
    );
    assert_eq!(
        predict.params.get("tolerance").and_then(|v| v.as_f64()),
        Some(1.0)
    );
    assert!(
        Path::new(CONFIG_PATH)
            .parent()
            .expect("config parent")
            .join("predict.rhai")
            .exists(),
        "predict.rhai phải nằm cạnh config trong strategies/predict/"
    );

    let sink = graph[3]
        .as_any()
        .downcast_ref::<TimeseriesStationSink>()
        .expect("component 3 = timeseries_station_sink");
    assert_eq!(sink.id, STATION_TSDB);
    assert_eq!(sink.inputs, vec![STATION_PREDICT.to_string()]);
}

/// ── Tầng 2: full pipeline → predictions + checks → parquet local → S3 ──
#[tokio::test]
async fn full_pipeline_checks_parquet_s3() {
    let endpoint = s3_endpoint();
    if !wait_minio(&endpoint).await {
        if common::integration_mode() {
            panic!(
                "MinIO không reachable tại {endpoint} — CI yêu cầu `docker compose up -d minio minio-bucket`"
            );
        }
        eprintln!(
            "skipping: MinIO không reachable tại {endpoint} — `docker compose up -d minio minio-bucket`"
        );
        return;
    }

    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        // SAFETY: chỉ test này đọc các key này; set một lần đầu process.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_pass());
        }
    }

    // 1) Load config thật, override đường dẫn host-only.
    let mut cfg = Config::load(Path::new(CONFIG_PATH))
        .expect("strategies/predict/config.toml phải parse + validate");
    let td = tempfile::tempdir().expect("tempdir");
    cfg.storage.data_dir = td.path().to_string_lossy().into_owned();
    if let Some(s3) = &mut cfg.storage.s3 {
        s3.endpoint = Some(endpoint.clone());
    }

    let _sub = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let now = signal::now_secs();

    // 2) Mock deterministic duy nhất: history window + live samples.
    let (src_addr, src_reqs) = spawn_mock(source_body(now)).await;

    // 3) Runtime với graph của config, re-point url + script_path.
    let dir = Path::new(CONFIG_PATH)
        .parent()
        .expect("config parent")
        .to_path_buf();
    let script = dir.join("predict.rhai");
    let src_url = format!("http://{src_addr}/api/v1/query_range?query=cpu");
    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else {
                continue;
            };
            match obj.get("id").and_then(|v| v.as_str()) {
                Some(STATION_PREDICT) => {
                    obj.insert(
                        "script_path".into(),
                        serde_json::json!(script.to_string_lossy().as_ref()),
                    );
                }
                Some(STATION_SOURCE) => {
                    obj.insert("url".into(), serde_json::json!(src_url));
                }
                _ => {}
            }
        }
    }

    let secret = Secret::new().await.expect("secret init");
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let components = pipeline_from_config(&cfg).expect("config graph deserialize");
    let mut rt = Runtime::new();
    rt.set_context(ctx.clone());
    rt.reload(components).expect("valid component graph");
    let _runtime_handle = rt.start(|_| async {}).unwrap();

    // 4) Source nhận dữ liệu raw (http → station): history + live cùng station.
    let src_obs = wait_station_data(&ctx, STATION_SOURCE, now - 300, now, 40).await;
    assert!(
        src_obs.iter().any(|o| o.metric_id == "cpu_ramp"),
        "live-feed phải có cpu_ramp raw"
    );
    assert!(
        src_obs.iter().any(|o| o.metric_id == "cpu_steady"),
        "live-feed phải có cpu_steady raw"
    );
    assert!(
        src_obs
            .iter()
            .any(|o| o.metric_id == "cpu_live_ramp" && o.value == 999.0),
        "live-feed phải có cpu_live_ramp=999"
    );
    assert!(
        src_obs
            .iter()
            .any(|o| o.metric_id == "cpu_live_steady" && o.value == 42.0),
        "live-feed phải có cpu_live_steady=42"
    );
    assert!(
        !src_reqs.lock().unwrap().is_empty(),
        "http source phải thực sự poll mock"
    );

    // 5) predict: clock ping → recompute prediction từ station; live-feed
    //    message → check live sample vs prediction cũ. Cần ≥ 2 vòng (vòng đầu
    //    chưa có prediction cũ trong own station) — deadline 70s, clock 10s.
    let checks = wait_predict_checks(&ctx, now - 300, now, 70).await;
    let preds: Vec<&Observation> = checks
        .iter()
        .filter(|o| o.labels.get("check").map(String::as_str) == Some("prediction"))
        .collect();
    let results: Vec<&Observation> = checks
        .iter()
        .filter(|o| o.labels.get("check").map(String::as_str) == Some("result"))
        .collect();
    assert_eq!(preds.len(), 2, "2 metric phải có prediction: {preds:?}");
    let ramp_pred = preds.iter().find(|o| o.metric_id == "cpu_ramp").unwrap();
    assert!(
        ramp_pred.value > 18.0,
        "cpu_ramp dự báo phải vượt v_last=18.0, got {}",
        ramp_pred.value
    );
    let steady_pred = preds.iter().find(|o| o.metric_id == "cpu_steady").unwrap();
    assert_eq!(steady_pred.value, 42.0, "cpu_steady nhánh steady = 42");

    // check true (1.0, live steady khớp) + check false (0.0, live ramp lệch xa).
    let ok_result = results
        .iter()
        .find(|o| o.metric_id == "cpu_live_steady")
        .expect("result cpu_live_steady");
    assert_eq!(
        ok_result.value, 1.0,
        "steady predict 42 vs live 42 → match"
    );
    let bad_result = results
        .iter()
        .find(|o| o.metric_id == "cpu_live_ramp")
        .expect("result cpu_live_ramp");
    assert_eq!(
        bad_result.value, 0.0,
        "ramp predict ~18 vs live 999 → miss"
    );

    // 6) tsdb: predictions + checks chảy qua message → parquet local sau flush.
    let tsdb_station = wait_for_station(&ctx, STATION_TSDB, 10).await;
    let tsdb_dir = td.path().join(format!("{STATION_TSDB}-timeseries"));
    let local_deadline = Instant::now() + Duration::from_secs(60);
    let mut flushed = false;
    while !flushed && Instant::now() < local_deadline {
        flushed = has_parquet(&tsdb_dir);
        if !flushed {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    assert!(
        flushed,
        "parquet phải xuất hiện dưới {} sau flush theo lịch config (15s)\n{}",
        tsdb_dir.display(),
        dump_tree(&tsdb_dir)
    );

    // 7) Mirror S3: opsense-lake/test-case-predict/tsdb/ts/**.
    let store = s3_store(&endpoint);
    let s3_ts_prefix = format!("{S3_PREFIX}/{STATION_TSDB}/ts/");
    let s3_deadline = Instant::now() + Duration::from_secs(60);
    let mut keys = Vec::new();
    while Instant::now() < s3_deadline {
        keys = list_keys(&store, &s3_ts_prefix).await;
        if keys.iter().any(|k| k.contains("blk=")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
    assert!(
        keys.iter()
            .any(|k| k.contains("blk=") && k.ends_with(".parquet")),
        "phải có delta parquet theo block trên S3 ({s3_ts_prefix}): {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.ends_with("ts/manifest.json")),
        "phải có ts/manifest.json trên S3: {keys:?}"
    );

    // Cleanup best-effort: shutdown flush + checkpoint + abort bg task của tsdb.
    tsdb_station.read().await.shutdown().await;
}
