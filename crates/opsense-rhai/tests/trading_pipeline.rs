//! Integration: realtime grid trading chạy qua Runtime thật, không chạm
//! `QlibEngine` cũ (đã bỏ — logic nằm trong kernel `Portfolio::evaluate`).
//!
//! ```text
//! clock ────────────────────────────┐
//! Input("candles") → RhaiTransform("grid", script trading) ←┘
//!                                        └─→ Output("sink")
//! ```
//!
//! Script `strategies/binance/grid.rhai` ở `params.mode = "trading"`:
//!   - tick (`payload.trigger = "tick"`) → gộp giá thành OHLCV trong station `grid`,
//!   - clock ping (trigger "") → nến **đã đóng** chạy `portfolio_feed`: kernel dựng
//!     plan, đặt lệnh; lệnh + cursor ghi thành observation `signal = "order"`
//!     trong station `grid` — state sống ở station, không ở RAM node.
//!
//! Nến đóng gần nhất bơm 3 giá trong cùng bucket để range rộng → chắc chắn chạm
//! level grid (nến range hẹp chỉ trúng level khi giá rơi đúng bậc).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use opsense_components::vector::runtime::{Component, Message, Runtime};
use opsense_core::{Config, Context, Observation, TimeseriesStation};
use opsense_mlib::vector::components::clock::Clock;
use opsense_mlib::vector::components::input::Input;
use opsense_mlib::vector::components::output::Output;
use opsense_model::events::{Signal, TelemetryKind};
use opsense_model::secret::Secret;
use opsense_rhai::RhaiTransform;
use serde_json::{Value, json};
use tokio::sync::RwLock;

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../strategies/binance/grid.rhai"
);
const SYMBOL: &str = "BTCUSDT";
const GRID: &str = "grid";
const CLOSED_CANDLES: i64 = 40;

fn params() -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    m.insert("history_source".into(), Value::from("history"));
    m.insert("own_station".into(), Value::from(GRID));
    m.insert("symbol".into(), Value::from(SYMBOL));
    m.insert("resolution".into(), Value::from("1m"));
    m.insert("history_secs".into(), Value::from(3600));
    m.insert("live_window_secs".into(), Value::from(300));
    m.insert("mode".into(), Value::from("trading"));
    m.insert("calendar".into(), Value::from("crypto"));
    m.insert("strategy".into(), Value::from("grid"));
    m.insert("grid_levels".into(), Value::from(5));
    m.insert("sl_pct".into(), Value::from(0.008));
    m.insert("smoothing_k".into(), Value::from(10.0));
    m.insert("lookback_secs".into(), Value::from(172_800));
    m.insert("review_interval_secs".into(), Value::from(900));
    m.insert("trading_candle_secs".into(), Value::from(60));
    m.insert("fee_rate".into(), Value::from(0.0005));
    m.insert("kelly_fraction".into(), Value::from(0.25));
    m.insert("base_capital".into(), Value::from(100_000.0));
    m.insert("settlement_candles".into(), Value::from(0));
    m
}

/// Batch tick như `json_2_json` sinh: `trigger` ở **top level** để transform
/// branch đúng nhánh tick.
fn tick_payload(ts: i64, price: f64) -> Value {
    json!({
        "ts": ts * 1000,
        "metric_id": SYMBOL,
        "kind": "metric",
        "signal": "raw",
        "value": price,
        "trigger": "tick",
        "labels": { "resolution": "1m" }
    })
}

/// Giá nhấn sóng trong 1 phút; `wide = true` cho nến đóng gần nhất (bơm nhiều
/// giá để tạo range rộng → chắc chắn chạm level grid).
///
/// Candle bám theo **hiện tại** vì script đọc station bằng cửa sổ
/// `now - history_secs` — dữ liệu cũ hơn cửa sổ sẽ không được nhìn thấy.
fn feed_ticks(open_bucket: i64) -> Vec<(i64, f64, bool)> {
    (0..CLOSED_CANDLES)
        .map(|i| {
            let price = 100.0 + ((i % 30) as f64) - 15.0;
            let ts = open_bucket - (CLOSED_CANDLES - i) * 60;
            (ts, price, i == CLOSED_CANDLES - 1)
        })
        .collect()
}

/// Cửa sổ đọc: chỉ 1 ngày gần nhất. `query_recent` duyệt TỪNG block (block 5s),
/// nên đọc `(0, i64::MAX)` sẽ quét hàng trăm triệu block — test phải giới hạn
/// cửa sổ như ứng dụng thật.
const WINDOW_SECS: i64 = 86_400;

async fn wait_grid_orders(ctx: &Arc<Context>, timeout_secs: u64) -> Vec<Observation> {
    let now = opsense_components::signal::now_secs();
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(st) = ctx.station::<Arc<RwLock<TimeseriesStation>>>(GRID).await {
            let obs = st
                .write()
                .await
                .query_recent(now - WINDOW_SECS, now)
                .await
                .unwrap_or_default();
            if obs.iter().any(|o| o.signal == Signal::Order) {
                return obs;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("station `{GRID}` không có order sau {timeout_secs}s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn grid_trading_emits_order_events_through_runtime() {
    let _sub = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let cfg: Config = serde_json::from_str("{}").expect("default config");
    let secret = Secret::new().await.expect("secret");
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    // `SCRIPT` là path → dùng `new_file` (compile lại khi file đổi mtime).
    let mut transform = RhaiTransform::new_file(GRID, &["candles", "clock"], SCRIPT);
    transform.params = params();

    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(Input {
            id: "candles".into(),
        }),
        Arc::new(Clock::new(Duration::from_secs(1))),
        Arc::new(transform),
        Arc::new(Output {
            id: "sink".into(),
            inputs: vec![GRID.into()],
        }),
    ];
    let mut rt = Runtime::new();
    rt.set_context(ctx.clone());
    rt.reload(components).expect("valid graph");
    let _receiver = rt.broadcast("sink".into()).expect("output broadcast");
    let _handle = rt.start(|event| async move {
        if let opsense_components::vector::runtime::Event::Major((_, error)) = event {
            panic!("runtime error: {error}");
        }
    })
    .expect("runtime starts");

    // Nạp nến đã đóng qua Input (tick branch gộp OHLCV vào station `grid`).
    let open_bucket = opsense_components::signal::now_secs() / 60 * 60;
    for (ts, price, wide) in feed_ticks(open_bucket) {
        let prices: &[f64] = if wide {
            &[price - 15.0, price, price + 15.0]
        } else {
            &[price]
        };
        for p in prices {
            rt.inject(
                "candles".into(),
                Message {
                    payload: tick_payload(ts, *p),
                },
            )
            .await
            .expect("inject tick");
        }
    }
    // Clock ping sẽ kích nhánh trading; nến cuối đã đóng (`open_bucket - 60`).
    let obs = wait_grid_orders(&ctx, 60).await;

    let orders: Vec<&Observation> = obs.iter().filter(|o| o.signal == Signal::Order).collect();
    assert!(!orders.is_empty(), "phải có order observation: {obs:?}");

    let last_closed = open_bucket - 60;
    for order in &orders {
        assert_eq!(order.metric_id, SYMBOL);
        assert_eq!(order.kind, TelemetryKind::Metric);
        assert_eq!(order.labels.get("status").map(String::as_str), Some("open"));
        assert!(order.value > 0.0, "entry price dương: {order:?}");
        for key in ["order_id", "dtype", "grid", "level", "size", "sl", "tp"] {
            assert!(
                order.labels.contains_key(key),
                "order phải mang label `{key}`: {order:?}"
            );
        }
        assert_eq!(
            order.ts, last_closed,
            "lệnh gắn với nến đã đóng gần nhất: {order:?}"
        );
    }

    // State sống trong station: cursor đánh dấu nến đã chạy trading step.
    let cursor: Vec<&Observation> = obs
        .iter()
        .filter(|o| o.labels.get("kind").map(String::as_str) == Some("trading_step"))
        .collect();
    assert_eq!(cursor.len(), 1, "phải có đúng 1 cursor: {obs:?}");
    assert_eq!(cursor[0].ts, last_closed);
    assert!(
        cursor[0]
            .labels
            .get("candle_seq")
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|seq| seq >= 1),
        "cursor phải lưu candle_seq để T+N hoạt động sau restart: {cursor:?}"
    );
}
