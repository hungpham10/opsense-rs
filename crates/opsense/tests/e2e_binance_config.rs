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
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use opsense::api::pipeline_from_config;
use opsense::api::{AppState, repl};
use opsense_components::converters::Tick2Candle;
use opsense_components::signal;
use opsense_components::vector::runtime::Runtime;
use opsense_core::{Config, Context, Observation, TimeseriesStation};
use opsense_model::events::Signal;
use opsense_mlib::cast::CastType;
use opsense_mlib::jq::JsonQuery;
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
const STATION_GRID: &str = "grid";
/// Station của node `tick_2_candle` — nến gom từ tick.
const STATION_TICK_CANDLE: &str = "tick-candle";
const NODE_TICK_FEED: &str = "tick-feed";
const NODE_TICK_MAP: &str = "tick-map";
/// Số nến mock phát ra. ≥ 10 vì `AnalysisGrid` cần tối thiểu 10 nến.
const CANDLE_ROUNDS: i64 = 40;

/// ── Tầng 1: config contract ────────────────────────────────────────────────
#[test]
fn config_file_contract() {
    let cfg = Config::load(Path::new(CONFIG_PATH))
        .expect("strategies/binance/config.toml phải parse + validate");
    assert_eq!(
        cfg.storage.backend, "memory",
        "v1 runtime-only: không persist, không RustFS"
    );

    // Graph **4** node, chỉ từ websocket — không clock, không kline history,
    // không sink:
    //   tick-feed (ws) → tick-map (json) → tick-candle (gom nến) → grid (rhai)
    // Thứ tự graph theo đúng thứ tự `[[pipeline.components]]` trong config.
    let graph = pipeline_from_config(&cfg).expect("components của config phải deserialize");
    assert_eq!(
        graph.len(),
        4,
        "config chỉ khai tick-feed ws + tick-map json + tick_2_candle + grid rhai"
    );

    let feed = graph[0]
        .as_any()
        .downcast_ref::<Websocket2Json>()
        .expect("component 0 = websocket_2_json");
    assert_eq!(feed.id, NODE_TICK_FEED);
    assert!(
        feed.uri.starts_with("wss://"),
        "tick feed là Binance aggTrade stream: {}",
        feed.uri
    );

    // Tick map: jq path dạng chuỗi deserialize được, price string cast f64,
    // constants định danh người gửi (`trigger`) + convention OHLCV.
    let map = graph[1]
        .as_any()
        .downcast_ref::<Json2Json>()
        .expect("component 1 = json_2_json");
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

    // Gom nến: nằm giữa `tick-map` và `grid`, là node **duy nhất** sinh nến.
    let candle = graph[2]
        .as_any()
        .downcast_ref::<Tick2Candle>()
        .expect("component 2 = tick_2_candle");
    assert_eq!(candle.id, STATION_TICK_CANDLE);
    assert_eq!(candle.inputs, vec![NODE_TICK_MAP.to_string()]);
    assert_eq!(candle.resolution, "1m");
    assert_eq!(candle.symbol, SYMBOL);
    assert_eq!(candle.unit_ms, 1, "Binance aggTrade ts là mili-giây");
    assert!(candle.stale_secs > 0, "phải cảnh báo khi tick ngừng");
    assert!(candle.station, "nến phải vào station của chính node");

    let grid = graph[3]
        .as_any()
        .downcast_ref::<RhaiTransform>()
        .expect("component 3 = rhai_transform grid");
    assert_eq!(grid.id, STATION_GRID);
    assert_eq!(
        grid.inputs,
        vec![NODE_TICK_MAP.to_string(), STATION_TICK_CANDLE.to_string()],
        "grid nhận tick (để no-op) + tick-candle (nến đã gom)"
    );
    // Không có node sink nên `grid` phải terminal — nếu ai gỡ `station = true`
    // thì graph hỏng lúc `reload`, chết ngay ở contract test.
    assert!(grid.station, "grid phải terminal: không còn node sink");
    assert_eq!(grid.script_path, "strategies/binance/grid.rhai");
    // Không còn node `history`, nên cả cửa sổ phân tích lẫn nến live đều đọc
    // từ station của `tick-candle`.
    for key in ["history_source", "live_source"] {
        assert_eq!(
            grid.params.get(key).and_then(Value::as_str),
            Some(STATION_TICK_CANDLE),
            "`{key}` phải trỏ vào station nến gom từ tick"
        );
    }
    assert_eq!(
        grid.params.get("own_station").and_then(Value::as_str),
        Some(STATION_GRID)
    );
    assert_eq!(grid.params.get("symbol").and_then(Value::as_str), Some(SYMBOL));

    // Strategy = script: `fn rebuild` trong grid.rhai dựng plan (opsense-rhai
    // `ScriptStrategy`), knob dưới đây là tham số `fn rebuild` đọc. Ghi rõ ở
    // đây để lệch config↔script (thêm/xoá knob) chết ngay ở contract test.
    assert_eq!(
        grid.params.get("strategy").and_then(Value::as_str),
        Some("rhai"),
        "strategy mặc định = script `fn rebuild` (không còn class Rust)"
    );
    for (key, want) in [
        ("grid_levels", 4.0),
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

    // 2) Mock deterministic: aggTrade realtime qua websocket. Không còn node
    //    `history` (kline HTTP) nên không mock HTTP nữa.
    let ws_uri = spawn_ws_mock();

    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else {
                continue;
            };
            match obj.get("id").and_then(Value::as_str) {
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

    // 3) Station `tick-candle`: aggTrade → nến, mỗi nến 5 obs OHLCV.
    //
    //    Nến chỉ ghi khi **đã đóng** (đã thấy tick ở bucket kế tiếp), nên phải
    //    chờ đủ số phút. `handle_ticks` phát liên tục ~3 phút ⇒ vài nến đóng.
    let candles = wait_station_candles(&ctx, STATION_TICK_CANDLE, now - 3600, now + 300, 70).await;
    assert!(
        candles.iter().all(|o| o.metric_id == SYMBOL),
        "nến phải gắn metric_id = symbol: {candles:?}"
    );
    let fields: Vec<&str> = candles
        .iter()
        .filter_map(|o| o.labels.get("field").map(String::as_str))
        .collect();
    assert!(
        fields.len() >= 5,
        "nến phải có obs OHLCV (labels.field): {candles:?}"
    );
    assert!(
        candles.iter().any(|o| o.labels.get("field").map(String::as_str) == Some("c")),
        "nến phải có field `c` (close): {candles:?}"
    );

    // Biên nến phải là **giá thật** của các tick, không phải 1 cent. Đây là
    // bug đã gặp: nến biên ~0.01 thì không mốc lưới nào cắt được.
    let ranges = candle_ranges(&candles);
    assert!(
        ranges.iter().any(|(_, r)| *r > 0.01),
        "nến phải có biên > 1 cent (=0.01 thì không mốc lưới nào cắt được): {ranges:?}"
    );

    // `v` là **số tick đã gộp**, không phải volume thật — tick stream không
    // mang khối lượng. Ghi rõ ở đây để không ai dùng nhầm cho thanh khoản.
    let v_sum: f64 = candles
        .iter()
        .filter(|o| o.labels.get("field").map(String::as_str) == Some("v"))
        .map(|o| o.value)
        .sum();
    assert!(v_sum >= 1.0, "v = số tick đã gộp: {candles:?}");

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

    // Snapshot nằm ở station `grid`; nến thì ở station `tick-candle`. Trả về
    // của `wait_grid_snapshot` là **nến**, nên phải hỏi riêng station `grid`.
    let grid_station = ctx
        .station::<Arc<RwLock<TimeseriesStation>>>(STATION_GRID)
        .await
        .expect("station grid phải đăng ký");
    let snapshot_obs = grid_station
        .write()
        .await
        .query_recent(now - 3600, now + 60)
        .await
        .unwrap_or_default();
    let snapshot = snapshot_obs
        .iter()
        .find(|o| o.labels.get("kind").map(String::as_str) == Some("snapshot"))
        .expect("station grid phải có snapshot");
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

    // 5) Không còn node sink — station `grid` của chính node là nơi duy nhất
    //    đọc lại được. `config_file_contract` đã chốt `grid` phải terminal.
    assert!(
        ctx.station::<Arc<RwLock<TimeseriesStation>>>(STATION_GRID)
            .await
            .is_ok(),
        "station `grid` phải đăng ký — đó là nơi lưu snapshot + lệnh"
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

    let mut cfg = Config::load(Path::new(CONFIG_PATH)).expect("config parse + validate");
    let dir = Path::new(CONFIG_PATH).parent().expect("config parent").to_path_buf();
    let script = dir.join("grid.rhai");

    // Không còn node `history` (kline HTTP) nên chỉ mock websocket.
    let ws_uri = spawn_ws_mock();

    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else {
                continue;
            };
            match obj.get("id").and_then(Value::as_str) {
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
    let mut opened = 0usize;
    for order in &orders {
        assert_eq!(order.metric_id, SYMBOL);
        // Lệnh có thể **đã đóng**: máy nhanh (CI) thì tới lúc test đọc station,
        // các nến sau đã chạm SL/TP và sinh observation `status = "closed"`.
        // Đó là vòng đời bình thường, không phải hỏng — chỉ cần trạng thái hợp lệ
        // và đủ label cho từng nhánh.
        let status = order.labels.get("status").map(String::as_str);
        assert!(
            matches!(status, Some("open") | Some("closed")),
            "status phải open|closed: {order:?}"
        );
        // `value` = giá vào lệnh khi MỞ, PnL % khi ĐÓNG (`opsense-rhai/src/orders.rs:557`).
        // PnL âm là chuyện bình thường nên không thể assert dương cho lệnh đã đóng —
        // assertion cũ chỉ xanh vì tình cờ lần chạy đó chưa có lệnh đóng lỗ.
        if status == Some("open") {
            opened += 1;
            assert!(order.value > 0.0, "entry price dương: {order:?}");
        } else if let Some(pnl) = order
            .labels
            .get("pnl_pct")
            .and_then(|v| v.parse::<f64>().ok())
        {
            assert!(
                (order.value - pnl).abs() < 1e-9,
                "lệnh đóng: value phải bằng pnl_pct: {order:?}"
            );
        }
        for key in ["order_id", "dtype", "grid", "level", "size", "sl", "tp"] {
            assert!(order.labels.contains_key(key), "thiếu label `{key}`: {order:?}");
        }
    }
    assert!(
        opened > 0,
        "phải có ít nhất 1 lệnh đang mở (mọi lệnh đều đóng thì nghi ngờ đọc sai)"
    );

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
        // `candle_seq = 0` là **hợp lệ**: `orders.rs` ghi cursor sớm lúc warmup
        // (chưa đủ 10 nến cho `AnalysisGrid`) và khi đó seq chưa tăng. Vì vậy
        // chỉ đòi label phải đọc được, còn "đã tiến" thì kiểm trên giá trị lớn
        // nhất bên dưới.
        assert!(
            c.labels.get("candle_seq").and_then(|v| v.parse::<u64>().ok()).is_some(),
            "cursor phải lưu candle_seq đọc được: {c:?}"
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

/// ── Tầng 4: order đọc được qua `Query.queryTimeseries` ────────────────────
///
/// Tầng 3 chứng minh *kernel sinh ra* order, nhưng đọc station bằng
/// `ctx.station(...)` — tức không qua tầng API. Nên nó không bắt được bug ở
/// `Query.queryTimeseries`: reader strict (`query_range`) làm mọi cửa sổ kết
/// thúc ở `now` trả rỗng, và cửa sổ mặc định của `opsense orders` /
/// MCP `opsense_query_timeseries` **chính là** `to = now` ⇒ CLI/MCP báo "không
/// có lệnh" trong khi strategy vẫn chạy.
///
/// Tầng này chạy đúng state của server ([`AppState::new`] — dựng context +
/// runtime từ config thật) và đọc order bằng **schema GraphQL thật** của
/// `POST /api/repl/graphql`, filter `signal = "order"`, cửa sổ kết thúc ở `now`.
///
/// Không cần Docker: `Resolver` bỏ qua Redis khi `REDIS_DSN` rỗng
/// (`Secret::get` đọc env trước ⇒ dsn rỗng bị `continue`), và Postgres chỉ cần
/// DSN parse được — connect fail chỉ log, không làm hỏng (`resolver.rs:129`).
#[tokio::test(flavor = "multi_thread")]
async fn trading_orders_readable_via_graphql() {
    // SAFETY: env là process-global. Các test khác trong file này không đọc
    // `REDIS_DSN`/`DB_DSN`, nên không có tương tác đáng kể; test chạy
    // `--test-threads=1` cùng cả suite e2e.
    unsafe {
        std::env::set_var("REDIS_DSN", "");
        std::env::set_var("DB_DSN", "postgres://poc:poc@127.0.0.1:1/poc");
    }

    let mut cfg = Config::load(Path::new(CONFIG_PATH)).expect("config parse + validate");
    let dir = Path::new(CONFIG_PATH).parent().expect("config parent").to_path_buf();
    let script = dir.join("grid.rhai");

    // Không còn node `history` (kline HTTP) nên chỉ mock websocket.
    let ws_uri = spawn_ws_mock();

    if let Some(p) = &mut cfg.pipeline {
        for comp in &mut p.components {
            let Some(obj) = comp.as_object_mut() else {
                continue;
            };
            match obj.get("id").and_then(Value::as_str) {
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
                    // Phí gần như 0: test này kiểm tra **lệnh có đọc được qua
                    // API** hay không, không kiểm tra lợi nhuận sau phí. Để phí
                    // thật (0.001) thì việc giá có chạm bậc lưới phụ thuộc thời
                    // điểm khớp nến ⇒ test trở nên chập chờn theo thời gian. Kinh
                    // tế phí có test riêng ở `opsense-qlib` (`min_profitable_step`
                    // và cổng `2 × fee` trong `open_orders`).
                    params.insert("fee_rate".into(), json!(0.00001));
                }
                _ => {}
            }
        }
    }

    let state = AppState::new(&cfg)
        .await
        .expect("AppState::new — dựng context + runtime từ config thật");
    let schema = repl::schema();

    // Poll đúng query mà CLI/MCP gửi. `to = now` là cửa sổ mặc định — tức đúng
    // trường hợp từng trả rỗng dù station đầy order.
    const QUERY: &str = r#"
        query Orders($node: String!, $signal: String) {
            queryTimeseries(node: $node, signal: $signal, limit: 50) {
                observations { ts metricId kind signal value labels }
                truncated
                scanned
            }
        }
    "#;

    // Poll đến khi thấy order. Trả về `(orders, scanned, payload)` thay vì gán
    // biến ngoài vòng lặp — crate bật `warnings = "deny"`, và giá trị khởi tạo
    // trước loop luôn bị ghi đè nên là dead code.
    let deadline = Instant::now() + Duration::from_secs(150);
    let (orders, scanned, payload) = loop {
        let res = schema
            .execute(
                async_graphql::Request::new(QUERY)
                    .variables(async_graphql::Variables::from_json(serde_json::json!({
                        "node": STATION_GRID,
                        "signal": "order",
                    })))
                    .data(state.clone()),
            )
            .await;
        assert!(
            res.errors.is_empty(),
            "queryTimeseries trả GraphQL error: {:?}",
            res.errors
        );
        let payload = res.data.into_json().expect("data");
        let result = &payload["queryTimeseries"];
        let scanned = result["scanned"].as_u64().unwrap_or_default() as usize;
        let orders = result["observations"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if !orders.is_empty() || Instant::now() >= deadline {
            break (orders, scanned, payload);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    assert!(
        !orders.is_empty(),
        "không có order nào đọc được qua GraphQL sau 150s (scanned={scanned}, payload={payload}). \
         Kernel có thể đã sinh lệnh (xem `full_pipeline_trading_emits_orders`) \
         — nếu vậy thì đường đọc của API lại hỏng."
    );
    eprintln!("queryTimeseries: scanned={scanned} orders={}", orders.len());
    assert!(
        scanned > 0,
        "phải quét được observation trong cửa sổ `to = now`: {payload}"
    );

    let mut opened = 0usize;
    for order in &orders {
        let ts = order["ts"].as_i64().expect("ts phải là Int");
        let status = order["labels"]["status"].as_str();
        let value = order["value"].as_f64().expect("value phải là Float");
        assert_eq!(order["signal"], "order", "filter signal=order: {order}");
        assert_eq!(order["metricId"], SYMBOL, "symbol: {order}");
        assert!(
            matches!(status, Some("open") | Some("closed")),
            "status phải open|closed: {order}"
        );
        // `value` = giá vào lệnh khi MỞ, PnL % khi ĐÓNG
        // (`opsense-rhai/src/orders.rs:557`) — PnL âm là bình thường.
        match status {
            Some("open") => {
                opened += 1;
                assert!(value > 0.0, "entry price dương: {order}");
            }
            Some("closed") => {
                // Labels của station là `HashMap<String, String>` nên qua
                // GraphQL chúng là **chuỗi**, không phải số. Nhánh lệnh-đóng
                // trước đây hiếm chạy nên giả định `as_f64()` chưa lộ.
                let pnl = order["labels"]["pnl_pct"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or_else(|| panic!("lệnh đóng phải có label pnl_pct: {order}"));
                assert!((value - pnl).abs() < 1e-9, "value phải bằng pnl_pct: {order}");
            }
            _ => unreachable!("status đã assert ở trên"),
        }
        for key in ["order_id", "dtype", "grid", "level", "size", "sl", "tp"] {
            assert!(
                order["labels"].get(key).is_some(),
                "thiếu label `{key}`: {order}"
            );
        }
        // ts phải nằm trong cửa sổ đã xin (guard chéo cho lọc ts ở tầng API).
        assert!(ts <= signal::now_secs(), "order ts={ts} ở tương lai: {order}");
    }
    assert!(
        opened > 0,
        "phải có ít nhất 1 lệnh đang mở (mọi lệnh đều đóng thì nghi ngờ đọc sai): {orders:?}"
    );

    // `opsense orders` lọc theo `labels.kind`, nên `order_id` phải hiện được ở
    // tầng GraphQL (không bị lọc/strip trong `queryTimeseries`).
    assert!(
        orders
            .iter()
            .all(|o| o["labels"]["order_id"].as_str().is_some_and(|s| !s.is_empty())),
        "mọi order phải có order_id để `opsense orders` dùng được: {orders:?}"
    );

    state.stop().await.expect("dừng runtime");
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

/// Mock WebSocket aggTrade (axum `ws` — server handshake tương thích client
/// `tokio-tungstenite` của `websocket_2_json`). Nội dung frame do
/// [`handle_ticks`] định nghĩa: tick theo thời gian lùi để gom được nhiều nến
/// trong test mà không phải chờ thật.
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

/// Giá **đóng** của nến ở vòng `round` — xen kẽ 95/112.
///
/// `snapshot` dựng `v_min`/`v_max` từ giá đóng và bỏ qua nến khi
/// `v_max - v_min <= 0`. Đóng cố định ở một giá ⇒ biên 0 ⇒ không có obs nào.
fn close_of(round: i64) -> &'static str {
    if round % 2 == 0 { "95.0" } else { "112.0" }
}

/// Phát tick theo **thời gian lùi**: mỗi vòng 3 giá ở các mốc phút khác nhau,
/// nên `tick_2_candle` gom được `CANDLE_ROUNDS` nến trong vài chục mili-giây
/// thay vì phải chờ thật.
///
/// Vì sao lùi: nến chỉ ghi khi **đã đóng**, tức khi đã thấy tick ở bucket kế
/// tiếp. Nếu tick mang thời gian *hiện tại* thì mỗi nến tốn 1 phút thật ⇒ test
/// tối thiểu 10 phút mới đủ 10 nến cho `AnalysisGrid`. Gửi `E` lùi ~1 phút mỗi
/// vòng thì đồng hồ nến tiến nhanh trong RAM, không cần chờ.
///
/// Giá nhấn sóng 95 → 112 → 95 … để **mỗi nến có biên thật** (~17). Nến biên 0
/// thì không mốc lưới nào cắt được ⇒ kernel không vào lệnh — đúng triệu chứng
/// `low=83932.0 high=83932.01` đã gặp.
///
/// `CANDLE_ROUNDS + 1` vòng: vòng cuối rơi vào bucket kế tiếp để **đẩy nến
/// cuối đóng** (đúng luật: nến chỉ là dữ liệu cuối khi đã sang phút kế tiếp).
async fn handle_ticks(mut socket: WebSocket) {
    let start_ms = signal::now_secs() * 1000;
    // Lùi đủ để nằm trong `history_secs` (16h) của script.
    let base_ms = start_ms - CANDLE_ROUNDS * 60_000 - 60_000;
    for round in 0..=CANDLE_ROUNDS {
        // Giá cuối **phải xen kẽ**, không phải cố định.
        //
        // `snapshot` chạm `v_max - v_min <= 0.0 { return [] }` trên giá **đóng**
        // của các nến. Nếu mọi nến đóng ở cùng một giá thì về đúng về 0 ⇒ hàm
        // trả về rỗng ⇒ `grid` không có obs nào, và nhìn từ ngoài chỉ thấy
        // "không có snapshot" chứ không thấy lý do.
        for (i, price) in ["95.0", "112.0", close_of(round)].iter().enumerate() {
            let tick = json!({
                "e": "aggTrade",
                "E": base_ms + round * 60_000 + (i as i64 * 1_000),
                "s": SYMBOL,
                "p": price,
                "q": "0.01",
                "t": round * 3 + i as i64 + 1,
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
        // Nghỉ ngắn cho runtime kịp xử lý, nhưng **không** theo thời gian tick.
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    // Giữ connection mở tới khi client đóng.
    while socket.recv().await.is_some() {}
}

/// Biên (high − low) của từng nến, gom theo `ts` (bucket).
///
/// Nến = 5 obs cùng `ts` với `labels.field` ∈ o/h/l/c/v. Nến biên ~0.01 là
/// nến hỏng: không mốc lưới nào nằm trong `[l, h]` nên kernel không vào lệnh
/// — đúng triệu chứng đã gặp (`low=83932.0 high=83932.01`).
fn candle_ranges(obs: &[Observation]) -> Vec<(i64, f64)> {
    let mut acc: std::collections::BTreeMap<i64, (f64, f64)> =
        std::collections::BTreeMap::new();
    for o in obs {
        let Some(f) = o.labels.get("field").map(String::as_str) else {
            continue;
        };
        if f != "h" && f != "l" {
            continue;
        }
        let e = acc.entry(o.ts).or_insert((f64::MIN, f64::MAX));
        match f {
            "h" => e.0 = e.0.max(o.value),
            _ => e.1 = e.1.min(o.value),
        }
    }
    acc.into_iter()
        .map(|(ts, (h, l))| (ts, if h.is_finite() && l.is_finite() { h - l } else { 0.0 }))
        .collect()
}

/// Đợi station có **nến** (obs có `labels.field`), không phải bất kỳ obs nào.
async fn wait_station_candles(
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
            if obs.iter().any(|o| o.labels.contains_key("field")) {
                return obs;
            }
        }
        if Instant::now() >= deadline {
            panic!("station `{id}` chưa có nến sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Đợi **cả hai**: node `tick_2_candle` đã ghi nến VÀ node `grid` đã phát
/// snapshot.
///
/// Hai station tách bạch là điểm cần chứng minh của kiến trúc mới: nến do node
/// `tick_2_candle` gom rồi ghi vào station **của node đó**, còn `grid` chỉ đọc
/// để dựng lưới. Trước đây script tự gộp nến nên `grid` vừa gốc nguồn vừa sinh
/// ra nến — không kiểm được là nến đi qua bước gom hay không.
///
/// Nến chỉ được ghi khi **đã đóng**, nên phải chờ đủ một bucket đóng.
async fn wait_grid_snapshot(
    ctx: &Arc<Context>,
    from: i64,
    to: i64,
    timeout_secs: u64,
) -> Vec<Observation> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let candles = match ctx
            .station::<Arc<RwLock<TimeseriesStation>>>(STATION_TICK_CANDLE)
            .await
        {
            Ok(st) => st
                .write()
                .await
                .query_recent(from, to)
                .await
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let has_candle = candles.iter().any(|o| o.labels.contains_key("field"));

        let has_snapshot = match ctx.station::<Arc<RwLock<TimeseriesStation>>>(STATION_GRID).await
        {
            Ok(st) => st
                .write()
                .await
                .query_recent(from, to)
                .await
                .unwrap_or_default()
                .iter()
                .any(|o| o.labels.get("kind").map(String::as_str) == Some("snapshot")),
            Err(_) => false,
        };

        if has_candle && has_snapshot {
            return candles;
        }
        if Instant::now() >= deadline {
            let dump = match ctx.station::<Arc<RwLock<TimeseriesStation>>>(STATION_GRID).await {
                Ok(s) => {
                    let v = s
                        .read()
                        .await
                        .query_recent(from, to)
                        .await
                        .unwrap_or_default();
                    format!(
                        "{} obs: {:?}",
                        v.len(),
                        v.iter()
                            .take(4)
                            .map(|o| (
                                o.ts,
                                o.signal,
                                o.labels.get("kind").cloned(),
                                o.labels.get("field").cloned()
                            ))
                            .collect::<Vec<_>>()
                    )
                }
                Err(e) => format!("station chưa đăng ký: {e}"),
            };
            panic!(
                "`{STATION_TICK_CANDLE}` có candle={has_candle}, \
                 `{STATION_GRID}` có snapshot={has_snapshot} sau {timeout_secs}s — grid: {dump}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
