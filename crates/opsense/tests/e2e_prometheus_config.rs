//! End-to-end drive bằng chính config `strategies/prometheus/config.toml`.
//!
//! Hai tầng:
//!   1. `config_file_contract` — nhanh, không cần infra: load + validate config
//!      thật, khẳng định các knobs `[storage]` / `[storage.s3]` mà file khai
//!      báo ĐƯỢC áp dụng (regression guard: trước đây `s3_flush_interval_secs` /
//!      `s3_snapshot_interval_secs` bị đặt nhầm dưới `[storage.s3]` nên serde
//!      bỏ qua âm thầm → flush rơi về default 60s), và graph pipeline của file
//!      (`clock` + `timeseries_station_sink`) deserialize qua typetag registry.
//!   2. `full_pipeline_to_parquet_and_s3` — full flow của config: chạy runtime
//!      thật với graph tự đủ của config (clock → http_source prom-explore →
//!      rhai stats-summary → tsdb sink), test chỉ re-point `prom-explore.url`
//!      về mock HTTP deterministic (không bấm vào prometheus.demo.prometheus.io).
//!      Dữ liệu chảy thật qua message generic payload: http fetch → rhai chạy
//!      `script.rhai` (summary per metric) → sink tsdb → parquet local → mirror
//!      S3. Chờ lịch `s3_flush_interval_secs` của config (15s) rồi khẳng định:
//!        - prom-explore station nhận observations raw (http → station),
//!        - parquet rơi xuống local: `<data_dir>/tsdb-timeseries/ts/blk=*`
//!          (chỉ xuất hiện nếu summaries thực sự chảy qua message vào tsdb),
//!        - mirror S3 (khi RustFS reachable): `opsense-lake/test-case-prom/tsdb/ts/**`.
//!
//! `data_dir` (`/app/.opsense/parquet`) là đường dẫn docker của strategy config —
//! test override sang temp dir (mọi setting khác giữ nguyên). `s3.endpoint`
//! (`http://rustfs:9000`) là hostname trong compose network, không resolve được
//! từ host runner → override theo `OPSENSE_S3_ENDPOINT` (mặc định
//! `http://127.0.0.1:9000`).
//!
//! Phần S3 follow repo convention (skip graceful):
//!   docker compose up -d rustfs rustfs-bucket
//!   cargo test -p opsense --test e2e_prometheus_config -- --nocapture
//!
//! Trong CI (`OPSENSE_INTEGRATION=true`), RustFS thiếu = panic để không green oan.

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

const CONFIG_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../strategies/prometheus/config.toml");

const S3_PREFIX: &str = "test-case-prom";
const STATION_TSDB: &str = "tsdb";
const STATION_PROM: &str = "prom-explore";

fn s3_endpoint() -> String {
    std::env::var("OPSENSE_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into())
}

fn s3_user() -> String {
    std::env::var("OPSENSE_S3_ACCESS_KEY_ID").unwrap_or_else(|_| "opsense".into())
}

fn s3_pass() -> String {
    std::env::var("OPSENSE_S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "opsense123".into())
}

/// Body mock trả về observations theo đúng các metric config khai báo
/// (`up`, `node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`). `now`
/// tính lại mỗi lần chạy để ts nằm gần hiện tại: block vẫn được giữ bởi
/// retention 300s của config.
fn prom_body(now: i64) -> String {
    serde_json::json!([
        {"ts": now,        "metric_id": "up", "kind": "metric", "signal": "raw", "value": 1.0, "labels": {"job": "prometheus"}},
        {"ts": now,        "metric_id": "up", "kind": "metric", "signal": "raw", "value": 1.0, "labels": {"job": "node"}},
        {"ts": now - 6,    "metric_id": "node_cpu_seconds_total", "kind": "metric", "signal": "raw", "value": 12.5, "labels": {"mode": "idle"}},
        {"ts": now - 12,   "metric_id": "node_cpu_seconds_total", "kind": "metric", "signal": "raw", "value": 13.0, "labels": {"mode": "idle"}},
        {"ts": now - 6,    "metric_id": "node_memory_MemAvailable_bytes", "kind": "metric", "signal": "raw", "value": 4194304000.0, "labels": {}},
        {"ts": now - 12,   "metric_id": "node_memory_MemAvailable_bytes", "kind": "metric", "signal": "raw", "value": 4012345678.0, "labels": {}}
    ])
    .to_string()
}

/// Mock HTTP server (deterministic, đúng pattern e2e_disk_grid) — ghi lại
/// request đã nhận để chứng minh http source thực sự poll.
async fn spawn_mock(
    body: String,
) -> (std::net::SocketAddr, Arc<std::sync::Mutex<Vec<String>>>) {
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
                reqs.lock().unwrap().push(String::from_utf8_lossy(&buf).into_owned());
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

async fn wait_rustfs(endpoint: &str) -> bool {
    let client = reqwest::Client::new();
    for _ in 0..30 {
        if client
            .get(format!("{endpoint}/health"))
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

/// Same logic với parquet.rs — path-style cho RustFS.
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

/// Đợi station có ≥ 1 observation trong cửa sổ `from..=to`. Query range phải
/// khớp đúng cửa sổ được update_range cover (block rỗng giữa chừng trả `None`).
async fn wait_for_station_obs(
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
                .query_range(from, to)
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

/// Có file `.parquet` dưới `<root>/ts/**` chưa (delta parquet của lake)?
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

/// Dump cây thư mục (tối đa `limit` entry) để chẩn đoán khi flush chưa xuất file.
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
        .expect("strategies/prometheus/config.toml phải parse + validate");

    // Engine / storage mà file khai báo.
    assert_eq!(cfg.engine.poll_interval_seconds, 10);
    assert_eq!(cfg.engine.cache_block_seconds, 5);
    assert_eq!(cfg.engine.cache_max_blocks, 12);
    assert_eq!(cfg.storage.backend, "parquet");
    assert_eq!(cfg.storage.block_secs, 5);
    assert_eq!(cfg.storage.retention_secs, 300);
    // Regression guard: 2 key này phải nằm ở [storage] thì mới được áp dụng.
    assert_eq!(
        cfg.storage.s3_flush_interval_secs, 15,
        "s3_flush_interval_secs nằm ở [storage] (không phải [storage.s3]) để không bị serde bỏ qua"
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

    // Capacity của đúng các metric Prometheus demo mà config chú thích.
    assert!(cfg.capacity.contains_key("up"));
    assert!(cfg.capacity.contains_key("node_cpu_seconds_total"));
    assert!(cfg.capacity.contains_key("node_memory_MemAvailable_bytes"));

    // Graph pipeline của config deserialize qua typetag registry (nếu một
    // component chưa đăng ký sẽ lỗi `unknown variant …`). `rhai_transform`
    // deserialize được vì opsense-rhai là *regular* dependency của opsense
    // (cùng link vào `opsense validate`/`opsense serve` binary).
    let graph = pipeline_from_config(&cfg).expect("components của config phải deserialize");
    assert_eq!(
        graph.len(),
        4,
        "config khai clock + http_source + rhai_transform + timeseries_station_sink"
    );

    let clock = graph[0]
        .as_any()
        .downcast_ref::<Clock>()
        .expect("component 0 = clock");
    assert_eq!(clock.id, "clock");
    assert_eq!(clock.interval_secs, 10);

    // Component 1 = http_source `prom-explore`: phải đứng trong config (graph
    // tự đủ cho serve — không chỉ tồn tại trong test) với station=true.
    let http = graph[1]
        .as_any()
        .downcast_ref::<HttpSource>()
        .expect("component 1 = http_source");
    assert_eq!(http.id, STATION_PROM);
    assert_eq!(http.inputs, vec!["clock".to_string()]);
    assert!(http.station, "prom-explore phải có station=true để registry + verify");
    assert!(
        http.url.contains("prometheus.demo.prometheus.io"),
        "url demo Prometheus mà config khai: {}",
        http.url
    );

    let rhai = graph[2]
        .as_any()
        .downcast_ref::<RhaiTransform>()
        .expect("component 2 = rhai_transform");
    assert_eq!(rhai.id, "stats-summary");
    assert_eq!(rhai.inputs, vec![STATION_PROM.to_string()]);
    assert_eq!(rhai.script_path, "strategies/prometheus/script.rhai");
    assert!(
        Path::new(CONFIG_PATH)
            .parent()
            .expect("config parent")
            .join("script.rhai")
            .exists(),
        "script.rhai phải nằm cạnh config trong strategies/prometheus/"
    );

    let sink = graph[3]
        .as_any()
        .downcast_ref::<TimeseriesStationSink>()
        .expect("component 3 = timeseries_station_sink");
    assert_eq!(sink.id, STATION_TSDB);
    assert_eq!(sink.inputs, vec!["stats-summary".to_string()]);
}

/// ── Tầng 2: full pipeline → parquet local → S3 (RustFS) ──
#[tokio::test]
async fn full_pipeline_to_parquet_and_s3() {
    let endpoint = s3_endpoint();
    if !wait_rustfs(&endpoint).await {
        if common::integration_mode() {
            panic!("RustFS không reachable tại {endpoint} — CI yêu cầu `docker compose up -d rustfs rustfs-bucket`");
        }
        eprintln!("skipping: RustFS không reachable tại {endpoint} — `docker compose up -d rustfs rustfs-bucket`");
        return;
    }

    // Storage mirror dùng object_store fallback credential theo env `AWS_*` khi
    // `[storage.s3]` không khai access key (config.giữ file sạch) — cấp cùng
    // nguồn `OPSENSE_S3_*` với client check của test, khớp root của RustFS
    // (docker-compose: `MINIO_ROOT_USER/PASSWORD` mặc định `opsense`/`opsense123`).
    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        // SAFETY: chỉ test này đọc các key này; set một lần đầu process.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_pass());
        }
    }

    // 1) Load config thật, chỉ override 2 đường dẫn host-only.
    let mut cfg = Config::load(Path::new(CONFIG_PATH))
        .expect("strategies/prometheus/config.toml phải parse + validate");
    let td = tempfile::tempdir().expect("tempdir");
    cfg.storage.data_dir = td.path().to_string_lossy().into_owned();
    if let Some(s3) = &mut cfg.storage.s3 {
        s3.endpoint = Some(endpoint.clone());
    }

    // 2) Mock prometheus demo + observations đưa vào station.
    let _sub = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let now = signal::now_secs();
    let body = prom_body(now);
    let raw: Vec<Observation> = serde_json::from_str(&body).expect("mock body parse");
    let from = raw.iter().map(|o| o.ts).min().unwrap();
    let to = raw.iter().map(|o| o.ts).max().unwrap();
    let (addr, requests) = spawn_mock(body).await;

    // 3) Runtime: graph của config — đã tự đủ (clock → prom-explore(http)
    //    → stats-summary(rhai) → tsdb(sink)). Test chỉ re-point 2 đường dẫn
    //    host-only để deterministic: `url` của prom-explore về mock (không
    //    bấm demo) và `script_path` (trong config là tương đối repo root; test
    //    chạy từ crates/opsense nên anchor tuyệt đối trước khi deserialize).
    let dir = Path::new(CONFIG_PATH).parent().expect("config parent").to_path_buf();
    let script = dir.join("script.rhai");
    let mock_url = format!("http://{addr}/api/v1/query_range?query=up");
    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else { continue };
            match obj.get("id").and_then(|v| v.as_str()) {
                Some("stats-summary") => {
                    obj.insert(
                        "script_path".into(),
                        serde_json::json!(script.to_string_lossy().as_ref()),
                    );
                }
                Some(STATION_PROM) => {
                    obj.insert("url".into(), serde_json::json!(mock_url));
                }
                _ => {}
            }
        }
    }

    let secret = Secret::new().await.expect("secret init");
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    // Đúng graph của config — `reload` cũng chính là validation serve dùng.
    let components = pipeline_from_config(&cfg).expect("config graph deserialize");
    let mut rt = Runtime::new();
    rt.set_context(ctx.clone());
    rt.reload(components).expect("valid component graph");
    let _runtime_handle = rt.start(|_| async {}).unwrap();

    // 4) Network path thật: clock tick → http fetch → prom-explore station
    //    (mock trả observations raw — station=true của config giữ registry).
    let pe_obs = wait_for_station_obs(&ctx, STATION_PROM, from, to, 35).await;
    assert!(
        pe_obs.iter().any(|o| o.metric_id == "up"),
        "prom-explore phải có observations raw (metric `up`)"
    );
    assert!(
        !requests.lock().unwrap().is_empty(),
        "http source phải thực sự poll mock Prometheus"
    );

    // 5) Message path thật: http forward batch (body `data` generic payload) →
    //    rhai `script.rhai` sinh summaries (signal=summary per metric) →
    //    sink tsdb. KHÔNG còn viết thẳng vào station — parquet chỉ xuất hiện
    //    nếu summaries thực sự chảy qua message vào tsdb rồi flush.
    let tsdb_station = wait_for_station(&ctx, STATION_TSDB, 10).await;

    // 6) Chờ lịch flush của config (s3_flush_interval_secs = 15s) ghi parquet
    //    xuống local lake: <data_dir>/tsdb-timeseries/ts/blk=*/batch-*.parquet.
    let tsdb_dir = td.path().join(format!("{STATION_TSDB}-timeseries"));
    // Deadline 2 chu kỳ flush (15s) + margin cho tick thời gian/khởi động.
    let local_deadline = Instant::now() + Duration::from_secs(45);
    let started = Instant::now();
    let mut flushed = false;
    let mut req_history: Vec<(u64, usize)> = Vec::new();
    let mut tsdb_history: Vec<(u64, usize)> = Vec::new();
    let mut rhai_history: Vec<(u64, usize)> = Vec::new();
    while !flushed && Instant::now() < local_deadline {
        flushed = has_parquet(&tsdb_dir);
        let elapsed = started.elapsed().as_secs();
        req_history.push((elapsed, requests.lock().unwrap().len()));
        let tsdb_obs = tsdb_station
            .write()
            .await
            .query_range(now - 20, now + 5)
            .await
            .unwrap_or_default()
            .len();
        tsdb_history.push((elapsed, tsdb_obs));
        let rhai_obs = match ctx
            .station::<Arc<RwLock<TimeseriesStation>>>("stats-summary")
            .await
        {
            Ok(st) => st
                .write()
                .await
                .query_range(now - 20, now + 5)
                .await
                .unwrap_or_default()
                .len(),
            Err(_) => usize::MAX, // station chưa được registry → rhai run() chưa tới/đã chết
        };
        rhai_history.push((elapsed, rhai_obs));
        if !flushed {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    assert!(
        flushed,
        "parquet phải xuất hiện dưới {} sau flush theo lịch config (s3_flush_interval_secs=15)\n\
         requests-per-elapsed: {req_history:?}\n\
         rhai(summary)-obs-per-elapsed: {rhai_history:?}\n\
         tsdb-obs-per-elapsed: {tsdb_history:?}\n\
         tsdb_dir tree:\n{}",
        tsdb_dir.display(),
        dump_tree(&tsdb_dir)
    );

    // 7) Mirror S3: keys đúng prefix config khai — opsense-lake/test-case-prom/tsdb/ts/**.
    let store = s3_store(&endpoint);
    let s3_ts_prefix = format!("{S3_PREFIX}/{STATION_TSDB}/ts/");
    let s3_deadline = Instant::now() + Duration::from_secs(40);
    let mut keys = Vec::new();
    while Instant::now() < s3_deadline {
        keys = list_keys(&store, &s3_ts_prefix).await;
        if keys.iter().any(|k| k.contains("blk=")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
    assert!(
        keys.iter().any(|k| k.contains("blk=")),
        "phải có ts/blk=*/batch-*.parquet trên S3 ({s3_ts_prefix}): {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.contains("blk=") && k.ends_with(".parquet")),
        "phải có delta parquet theo block trên S3: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.ends_with("ts/manifest.json")),
        "phải có ts/manifest.json trên S3: {keys:?}"
    );

    // Cleanup best-effort: shutdown flush + checkpoint + abort bg task của tsdb.
    tsdb_station.read().await.shutdown().await;
}