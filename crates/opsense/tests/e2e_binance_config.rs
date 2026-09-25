//! E2E chạy thật `strategies/binance/config.toml`: kline history qua HTTP +
//! aggTrade realtime qua websocket, script `grid.rhai` dựng AnalysisGrid +
//! TransitionAnalysis, sink terminal nhận output.
//!
//! Hai tầng:
//!   1. `config_file_contract` — không cần infra: load + validate config thật,
//!      khẳng định graph 6 node deserialize qua typetag registry và các
//!      knobs (candle parse mode, jq string-or-array query, constants
//!      trigger/kind/signal/labels, params của script).
//!   2. `full_pipeline_ticks_and_snapshot` — runtime thật với mock HTTP klines
//!      + mock WebSocket aggTrade (axum `ws`): khẳng định history station có
//!      OHLCV từ klines, station `grid` có candle từ tick VÀ snapshot grid
//!      (`labels.kind="snapshot"`, `grid_step > 0`), sink terminal nhận data.
//!
//! **Không dùng object store** — `[storage] backend = "memory"`: v1 runtime-only.
//! `cargo test -p opsense --test e2e_binance_config -- --nocapture`

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use opsense::api::pipeline_from_config;
use opsense_components::http::HttpSource;
use opsense_components::signal;
use opsense_components::station::TimeseriesStationSink;
use opsense_components::vector::runtime::Runtime;
use opsense_core::{Config, Context, Observation, TimeseriesStation};
use opsense_model::events::Signal;
use opsense_mlib::cast::CastType;
use opsense_mlib::jq::JsonQuery;
use opsense_mlib::vector::components::clock::Clock;
use opsense_mlib::vector::components::{Json2Json, Websocket2Json};
use opsense_model::secret::Secret;
use opsense_rhai::RhaiTransform;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

const CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../strategies/binance/config.toml"
);

const SYMBOL: &str = "BTCUSDT";
const STATION_HISTORY: &str = "history";
const STATION_GRID: &str = "grid";
const STATION_SINK: &str = "binance-tsdb";
const NODE_TICK_FEED: &str = "tick-feed";
const NODE_TICK_MAP: &str = "tick-map";

/// ── Tầng 1: config contract ────────────────────────────────────────────────
#[test]
fn config_file_contract() {
    let cfg = Config::load(Path::new(CONFIG_PATH))
        .expect("strategies/binance/config.toml phải parse + validate");
    assert_eq!(
        cfg.storage.backend, "memory",
        "v1 runtime-only: không persist, không RustFS"
    );

    // Graph 6 node: clock → history (http candles) + tick-feed (ws) → tick-map
    // (json) → grid (rhai) → binance-tsdb (terminal sink).
    let graph = pipeline_from_config(&cfg).expect("components của config phải deserialize");
    assert_eq!(
        graph.len(),
        6,
        "config khai clock + history http + tick-feed ws + tick-map json + grid rhai + tsdb sink"
    );

    let clock = graph[0]
        .as_any()
        .downcast_ref::<Clock>()
        .expect("component 0 = clock");
    assert_eq!(clock.id, "clock");
    assert_eq!(clock.interval_secs, 10);

    // History: http source ở candle parse mode, terminal (station=true) nên
    // không cần consumer — script đọc candles qua `station_candles`.
    let history = graph[1]
        .as_any()
        .downcast_ref::<HttpSource>()
        .expect("component 1 = http_source history");
    assert_eq!(history.id, STATION_HISTORY);
    assert_eq!(history.inputs, vec!["clock".to_string()]);
    assert!(history.station, "history station=true → terminal, script đọc được");
    let candles = history.candles.as_ref().expect("http source phải bật candle parse mode");
    assert_eq!(candles.symbol, SYMBOL);
    assert_eq!(candles.resolution, "1m");
    assert_eq!(candles.unit_ms, 1, "Binance openTime là milliseconds");
    assert_eq!(
        candles.mapping,
        ["[].0", "[].1", "[].2", "[].3", "[].4", "[].5"],
        "mapping mặc định = layout Binance klines"
    );

    let feed = graph[2]
        .as_any()
        .downcast_ref::<Websocket2Json>()
        .expect("component 2 = websocket_2_json");
    assert_eq!(feed.id, NODE_TICK_FEED);
    assert!(
        feed.uri.starts_with("wss://"),
        "tick feed là Binance aggTrade stream: {}",
        feed.uri
    );

    // Tick map: jq path dạng chuỗi deserialize được, price string cast f64,
    // constants định danh người gửi (`trigger`) + convention OHLCV.
    let map = graph[3]
        .as_any()
        .downcast_ref::<Json2Json>()
        .expect("component 3 = json_2_json");
    assert_eq!(map.inputs, vec![NODE_TICK_FEED.to_string()]);
    assert_eq!(
        map.transforms.get("ts").expect("transform ts").query,
        JsonQuery::parse("E").expect("parse E").operators(),
        "E (event time) → ts"
    );
    assert_eq!(
        map.transforms.get("metric_id").expect("transform metric_id").query,
        JsonQuery::parse("s").expect("parse s").operators(),
        "s (symbol) → metric_id"
    );
    let price = map.transforms.get("value").expect("transform value");
    assert_eq!(
        price.query,
        JsonQuery::parse("p").expect("parse p").operators(),
        "p (price) → value"
    );
    assert_eq!(
        price.cast_to,
        Some(CastType::F64),
        "Binance price là string → phải cast f64"
    );
    let constants = map.constants.as_ref().expect("tick map phải khai constants");
    assert_eq!(constants["trigger"], "tick", "trigger định danh người gửi (ưu tiên payload.trigger)");
    assert_eq!(constants["kind"], "metric");
    assert_eq!(constants["signal"], "raw");
    assert_eq!(constants["labels"]["resolution"], "1m");

    let grid = graph[4]
        .as_any()
        .downcast_ref::<RhaiTransform>()
        .expect("component 4 = rhai_transform grid");
    assert_eq!(grid.id, STATION_GRID);
    assert_eq!(
        grid.inputs,
        vec!["clock".to_string(), STATION_HISTORY.to_string(), NODE_TICK_MAP.to_string()],
        "grid nhận clock + history + tick để branch theo trigger"
    );
    assert_eq!(grid.script_path, "strategies/binance/grid.rhai");
    assert_eq!(
        grid.params.get("history_source").and_then(Value::as_str),
        Some(STATION_HISTORY)
    );
    assert_eq!(
        grid.params.get("own_station").and_then(Value::as_str),
        Some(STATION_GRID)
    );
    assert_eq!(grid.params.get("symbol").and_then(Value::as_str), Some(SYMBOL));
    assert_eq!(grid.params.get("history_secs").and_then(Value::as_i64), Some(3600));

    // Strategy = script: `fn rebuild` trong grid.rhai dựng plan (opsense-rhai
    // `ScriptStrategy`), knob dưới đây là tham số `fn rebuild` đọc. Ghi rõ ở
    // đây để lệch config↔script (thêm/xoá knob) chết ngay ở contract test.
    assert_eq!(
        grid.params.get("strategy").and_then(Value::as_str),
        Some("rhai"),
        "strategy mặc định = script `fn rebuild` (không còn class Rust)"
    );
    for (key, want) in [
        ("grid_levels", 5.0),
        ("sl_pct", 0.008),
        ("grid_min_trades", 3.0),
        ("grid_weight_sharpness", 4.0),
        ("grid_max_bit", 20.0),
    ] {
        assert_eq!(
            grid.params.get(key).and_then(Value::as_f64),
            Some(want),
            "knob `{key}` của `fn rebuild` lệch với script"
        );
    }
    // Script phải thật sự chứa `fn rebuild` — nếu ai xoá nhầm thì strategy
    // chỉ chết lúc runtime, không phải lúc test.
    let script_src = std::fs::read_to_string(
        Path::new(CONFIG_PATH)
            .parent()
            .expect("config parent")
            .join("grid.rhai"),
    )
    .expect("đọc grid.rhai");
    assert!(
        script_src.contains("fn rebuild("),
        "grid.rhai phải định nghĩa `fn rebuild(candles, prev, params)`"
    );

    assert!(
        Path::new(CONFIG_PATH)
            .parent()
            .expect("config parent")
            .join("grid.rhai")
            .exists(),
        "grid.rhai phải nằm cạnh config trong strategies/binance/"
    );

    let sink = graph[5]
        .as_any()
        .downcast_ref::<TimeseriesStationSink>()
        .expect("component 5 = timeseries_station_sink");
    assert_eq!(sink.id, STATION_SINK);
    assert_eq!(
        sink.inputs,
        vec![STATION_GRID.to_string()],
        "rhai_transform non-sink bắt buộc có consumer"
    );
}

/// ── Tầng 2: full pipeline (mock HTTP klines + mock WS aggTrade) ────────────
#[tokio::test]
async fn full_pipeline_ticks_and_snapshot() {
    let _sub = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let now = signal::now_secs();

    // 1) Load config thật, override các đường dẫn host-only.
    let mut cfg = Config::load(Path::new(CONFIG_PATH))
        .expect("strategies/binance/config.toml phải parse + validate");
    let dir = Path::new(CONFIG_PATH)
        .parent()
        .expect("config parent")
        .to_path_buf();
    let script = dir.join("grid.rhai");

    // 2) Mock deterministic: 20 candle 1m ramp tăng + 4 aggTrade realtime.
    let (src_addr, src_reqs) = spawn_http_mock(klines_body(now)).await;
    let src_url = format!("http://{src_addr}/api/v3/klines?symbol={SYMBOL}&interval=1m");
    let ws_uri = spawn_ws_mock();

    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else {
                continue;
            };
            match obj.get("id").and_then(Value::as_str) {
                Some(STATION_HISTORY) => {
                    obj.insert("url".into(), json!(src_url));
                }
                Some(NODE_TICK_FEED) => {
                    obj.insert("uri".into(), json!(ws_uri));
                }
                Some(STATION_GRID) => {
                    obj.insert(
                        "script_path".into(),
                        json!(script.to_string_lossy().as_ref()),
                    );
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

    // 3) History station: HTTP klines → 5 obs OHLCV mỗi candle.
    let hist = wait_station_data(&ctx, STATION_HISTORY, now - 3600, now, 40).await;
    let hist_fields: Vec<&str> = hist
        .iter()
        .filter_map(|o| o.labels.get("field").map(String::as_str))
        .collect();
    assert!(
        hist.iter().all(|o| o.metric_id == SYMBOL),
        "history phải gắn metric_id = symbol: {hist:?}"
    );
    assert!(
        !hist_fields.is_empty(),
        "history phải có obs OHLCV (labels.field): {hist:?}"
    );
    assert!(
        hist.iter().any(|o| o.labels.get("field").map(String::as_str) == Some("c")),
        "history phải có field `c` (close): {hist:?}"
    );
    assert!(
        !src_reqs.lock().unwrap().is_empty(),
        "http source phải thực sự poll mock"
    );

    // 4) Grid station: candle từ tick (websocket→json→rhai) + snapshot grid.
    //    Clock 10s + history ~10s để có ≥ 2 candle → deadline 70s.
    let grid_obs = wait_grid_snapshot(&ctx, now - 3600, now + 60, 70).await;

    let tick_candles: Vec<&Observation> = grid_obs
        .iter()
        .filter(|o| o.labels.get("field").is_some())
        .collect();
    assert!(
        tick_candles.len() >= 5,
        "tick phải sinh candle 5 field trong own station: {grid_obs:?}"
    );
    assert!(
        tick_candles.iter().all(|o| o.metric_id == SYMBOL),
        "candle từ tick phải gắn symbol: {tick_candles:?}"
    );
    let v_sum: f64 = tick_candles
        .iter()
        .filter(|o| o.labels.get("field").map(String::as_str) == Some("v"))
        .map(|o| o.value)
        .sum();
    assert!(
        v_sum >= 1.0,
        "volume của candle = số tick đã gộp: {tick_candles:?}"
    );

    let snapshot = grid_obs
        .iter()
        .find(|o| o.labels.get("kind").map(String::as_str) == Some("snapshot"))
        .expect("grid station phải có snapshot");
    assert_eq!(snapshot.metric_id, SYMBOL);
    let step: f64 = snapshot
        .labels
        .get("grid_step")
        .expect("snapshot có grid_step")
        .parse()
        .expect("grid_step parse được");
    assert!(step > 0.0, "grid_step > 0: {snapshot:?}");
    let cells: i64 = snapshot
        .labels
        .get("cells")
        .expect("snapshot có cells")
        .parse()
        .expect("cells parse được");
    assert!(cells >= 1, "grid có ít nhất 1 cell: {snapshot:?}");
    assert_ne!(
        snapshot.labels.get("method").map(String::as_str),
        Some("steady"),
        "có candle đủ dữ liệu → phải chạy grid, không rơi nhánh steady: {snapshot:?}"
    );
    let up: f64 = snapshot.labels["up_prob"].parse().expect("up_prob");
    let down: f64 = snapshot.labels["down_prob"].parse().expect("down_prob");
    let stay: f64 = snapshot.labels["stay_prob"].parse().expect("stay_prob");
    assert!(
        (up + down + stay - 1.0).abs() < 1e-6,
        "xác suất transition cộng lại = 1: {snapshot:?}"
    );

    // 5) Terminal sink nhận output của grid (edge cuối hợp lệ).
    let sink_obs = wait_station_data(&ctx, STATION_SINK, now - 3600, now + 60, 20).await;
    assert!(
        sink_obs
            .iter()
            .any(|o| o.labels.get("kind").map(String::as_str) == Some("snapshot")),
        "sink terminal phải nhận snapshot từ grid: {sink_obs:?}"
    );
}

/// ── Tầng 3: `params.mode = "trading"` trên chính config thật ────────────────
///
/// Cùng graph + mock như trên, chỉ bật nhánh đặt lệnh: candle 1m từ klines +
/// candle realtime từ aggTrade → `portfolio_feed` chạy kernel →
/// observation `signal = "order"` trong station `grid`.
///
/// Mock klines dùng **giá nhấn sóng** + candle cuối biến động rộng để chắc
/// chắn giá chạm level grid (nến range hẹp chỉ trúng khi giá rơi đúng bậc).
#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline_trading_emits_orders() {
    let _sub = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let now = signal::now_secs();
    let mut cfg = Config::load(Path::new(CONFIG_PATH)).expect("config parse + validate");
    let dir = Path::new(CONFIG_PATH).parent().expect("config parent").to_path_buf();
    let script = dir.join("grid.rhai");

    let (src_addr, _reqs) = spawn_http_mock(swing_klines_body(now)).await;
    let src_url = format!("http://{src_addr}/api/v3/klines?symbol={SYMBOL}&interval=1m");
    let ws_uri = spawn_ws_mock();

    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else {
                continue;
            };
            match obj.get("id").and_then(Value::as_str) {
                Some(STATION_HISTORY) => {
                    obj.insert("url".into(), json!(src_url));
                }
                Some(NODE_TICK_FEED) => {
                    obj.insert("uri".into(), json!(ws_uri));
                }
                Some(STATION_GRID) => {
                    obj.insert("script_path".into(), json!(script.to_string_lossy().as_ref()));
                    let params = obj
                        .get_mut("params")
                        .and_then(Value::as_object_mut)
                        .expect("grid node có params");
                    params.insert("mode".into(), json!("trading"));
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

    let obs = wait_orders(&ctx, 90).await;
    let orders: Vec<&Observation> = obs
        .iter()
        .filter(|o| o.signal == Signal::Order)
        .collect();
    assert!(!orders.is_empty(), "trading mode phải sinh order: {obs:?}");
    for order in &orders {
        assert_eq!(order.metric_id, SYMBOL);
        assert_eq!(order.labels.get("status").map(String::as_str), Some("open"));
        assert!(order.value > 0.0, "entry price dương: {order:?}");
        for key in ["order_id", "dtype", "grid", "level", "size", "sl", "tp"] {
            assert!(order.labels.contains_key(key), "thiếu label `{key}`: {order:?}");
        }
    }

    // Cursor đánh dấu nến đã chạy trading step (T+N + idempotent sau restart).
    // Append-only: mỗi nến live đóng trong lúc test chờ sinh một cursor, nên
    // assert theo tính chất (có cursor, đều có `candle_seq`, không trùng nến)
    // chứ không khoá cứng vào con số — con số phụ thuộc tốc độ máy.
    let mut cursor: Vec<&Observation> = obs
        .iter()
        .filter(|o| o.labels.get("kind").map(String::as_str) == Some("trading_step"))
        .collect();
    assert!(!cursor.is_empty(), "phải có cursor trading_step: {obs:?}");
    for c in &cursor {
        assert!(
            c.labels
                .get("candle_seq")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|seq| seq >= 1),
            "cursor phải lưu candle_seq: {c:?}"
        );
        // Nến đã đóng = ts của cursor, luôn trùng bucket 60s của resolution 1m.
        assert_eq!(c.ts % 60, 0, "cursor ts phải theo bucket 60s: {c:?}");
        assert_eq!(c.value, c.ts as f64, "cursor value = candle_ts: {c:?}");
    }
    cursor.sort_by_key(|c| c.ts);
    let ts: Vec<i64> = cursor.iter().map(|c| c.ts).collect();
    let mut uniq = ts.clone();
    uniq.dedup();
    assert_eq!(ts, uniq, "mỗi nến chỉ có một cursor: {cursor:?}");
    // Nến đóng gần nhất phải đã được xử lý (cursor ts ≥ ts của mọi order).
    let last_order_ts = orders.iter().map(|o| o.ts).max().unwrap_or_default();
    assert!(
        ts.last().is_some_and(|last| *last >= last_order_ts),
        "cursor phải không cũ hơn order: {cursor:?}"
    );
}

/// Klines giá nhấn sóng; **candle mới nhất** có range rộng để chạm level grid.
///
/// Vì sao range rộng phải nằm ở candle mới nhất (i = 1) chứ không chỉ ở
/// candle cũ nhất: candle live từ websocket có thể rơi vào **cùng bucket** với
/// candle kline mới nhất (tick ts = now-5s, nên bucket phụ thuộc giây lúc test
/// khởi động). Merge quy tắc "live thắng cùng bucket" → candle rộng bị live che
/// mất, kernel nhận nến hẹp không cắt level nào → không đặt lệnh cho tới khi có
/// nến live đóng tiếp theo. Đặt range rộng ở cả đầu (i=1) và cuối (i=20) để nến
/// nào là nến "vừa đóng" cũng chạm level.
fn swing_klines_body(now: i64) -> String {
    let base = now / 60 * 60;
    let rows: Vec<Value> = (1..=20)
        .map(|i| {
            let i = i as f64;
            let close = 100.0 + (i % 10.0) - 5.0;
            let (high, low) = if i == 1.0 || i == 20.0 {
                (close + 20.0, close - 20.0)
            } else {
                (close + 0.5, close - 0.5)
            };
            json!([
                (base - 60 * i as i64) * 1000,
                close,
                high,
                low,
                close,
                12.5
            ])
        })
        .collect();
    json!(rows).to_string()
}

async fn wait_orders(ctx: &Arc<Context>, timeout_secs: u64) -> Vec<Observation> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(st) = ctx.station::<Arc<RwLock<TimeseriesStation>>>(STATION_GRID).await {
            let now = signal::now_secs();
            let obs = st
                .write()
                .await
                .query_recent(now - 3600, now)
                .await
                .unwrap_or_default();
            if obs.iter().any(|o| o.signal == Signal::Order) {
                return obs;
            }
        }
        if Instant::now() >= deadline {
            panic!("station `{STATION_GRID}` không có order sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Binance `klines` rows: `[openTimeMs, open, high, low, close, volume]`.
/// 20 candle 1m ramp tăng, mỗi candle cách 60s, mới nhất cách `now` 60s.
fn klines_body(now: i64) -> String {
    let base = now / 60 * 60;
    let rows: Vec<Value> = (1..=20)
        .map(|i| {
            let i = i as f64;
            let open = 100.0 + 0.2 * (i - 1.0);
            let close = 100.0 + 0.2 * i;
            json!([
                (base - 60 * i as i64) * 1000,
                open,
                close + 0.1,
                open - 0.1,
                close,
                12.5
            ])
        })
        .collect();
    json!(rows).to_string()
}

/// Mock HTTP server (raw TCP, cùng pattern e2e_predict_config).
async fn spawn_http_mock(body: String) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));

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
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    (addr, requests)
}

/// Mock WebSocket aggTrade (axum `ws` — server handshake tương thích client
/// `tokio-tungstenite` của `websocket_2_json`). Gửi 4 tick trong cùng phút
/// với giá tăng/giảm xen kẽ để `merge_price` cập nhật h/l/c, giữ socket mở.
fn spawn_ws_mock() -> String {
    let app = axum::Router::new().route("/ws", axum::routing::get(ws_handler));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("ws://{addr}/ws")
}

async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_ticks)
}

/// Phát tick **liên tục** suốt đời test, mỗi vòng 2 giây 2 tick ở hai đầu
/// dải giá (`95.0` / `112.0`) cách nhau 1s, timestamp `now-5s` / `now-4s`.
///
/// Vì sao phải liên tục chứ không bắn 4 tick rồi im: nến "vừa đóng" mà
/// `trade()` chọn là nến live, mà tick thì chỉ có 4 cái lúc connect ⇒ sau đó
/// data đứng yên. Kernel chỉ có **một** cơ hội đặt lệnh; nếu lần đó nến live
/// rơi vào bucket chỉ nhận 1 tick (range = 0, không cắt level nào) thì cursor
/// `trading_step` đánh dấu nến đó và mọi vòng clock sau bị idempotency bỏ qua
/// ⇒ test treo tới deadline. Phát liên tục ⇒ mỗi phút có một nến live đóng
/// với range thật (`95..112`) đủ chạm level, và nến đóng mới ⇒ cursor mới ⇒
/// kernel thử lại được.
async fn handle_ticks(mut socket: WebSocket) {
    let start_ms = signal::now_secs() * 1000;
    for round in 0..90i64 {
        for (i, price) in ["95.0", "112.0"].iter().enumerate() {
            let tick = json!({
                "e": "aggTrade",
                "E": start_ms - 5_000 + round * 2_000 + (i as i64 * 1_000),
                "s": SYMBOL,
                "p": price,
                "q": "0.01",
                "t": round * 2 + i as i64 + 1,
                "m": false
            });
            if socket
                .send(WsMessage::Text(tick.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(2_000)).await;
    }
    // Giữ connection mở tới khi client đóng (runtime không cần frame nữa).
    while socket.recv().await.is_some() {}
}

/// Đợi station có dữ liệu (query lenient `query_recent`).
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
            panic!("station `{id}` chưa có dữ liệu sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Đợi station `grid` có CẢ candle từ tick VÀ snapshot grid.
async fn wait_grid_snapshot(
    ctx: &Arc<Context>,
    from: i64,
    to: i64,
    timeout_secs: u64,
) -> Vec<Observation> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(st) = ctx.station::<Arc<RwLock<TimeseriesStation>>>(STATION_GRID).await {
            let obs = st
                .write()
                .await
                .query_recent(from, to)
                .await
                .unwrap_or_default();
            let has_candle = obs.iter().any(|o| o.labels.contains_key("field"));
            let has_snapshot = obs
                .iter()
                .any(|o| o.labels.get("kind").map(String::as_str) == Some("snapshot"));
            if has_candle && has_snapshot {
                return obs;
            }
        }
        if Instant::now() >= deadline {
            panic!("station `{STATION_GRID}` chưa có candle + snapshot sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
