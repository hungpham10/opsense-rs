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

use opsense_components::converters::Tick2Candle;
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
/// Station của node `tick_2_candle` — nến gom từ tick.
const CANDLE: &str = "tick-candle";
const CLOSED_CANDLES: i64 = 40;

fn params() -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    m.insert("history_source".into(), Value::from("history"));
    m.insert("own_station".into(), Value::from(GRID));
    // Nến live do node `tick_2_candle` gom, không phải script tự gộp.
    m.insert("live_source".into(), Value::from(CANDLE));
    m.insert("symbol".into(), Value::from(SYMBOL));
    m.insert("resolution".into(), Value::from("1m"));
    m.insert("history_secs".into(), Value::from(3600));
    m.insert("live_window_secs".into(), Value::from(300));
    m.insert("mode".into(), Value::from("trading"));
    m.insert("calendar".into(), Value::from("crypto"));
    m.insert("strategy".into(), Value::from("grid"));
    m.insert("grid_levels".into(), Value::from(5));
    m.insert("sl_pct".into(), Value::from(0.008));
    m.insert("grid_min_trades".into(), Value::from(3));
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
            // Chẩn đoán: nến đã vào station `tick-candle` chưa? Không có nó thì
            // `grid` không có gì để vào lệnh — nhưng nhìn log kernel thì thấy
            // "chưa đủ dữ liệu", rất dễ chẩn đoán nhầm thành lỗi chiến lược.
            let n_candle = match ctx
                .station::<Arc<RwLock<TimeseriesStation>>>(CANDLE)
                .await
            {
                Ok(s) => s
                    .read()
                    .await
                    .query_recent(now - WINDOW_SECS, now)
                    .await
                    .map_or(0, |v| v.len()),
                Err(_) => 0,
            };
            let grid_n = match ctx.station::<Arc<RwLock<TimeseriesStation>>>(GRID).await {
                Ok(s) => s
                    .read()
                    .await
                    .query_recent(now - WINDOW_SECS, now)
                    .await
                    .map_or(0, |v| v.len()),
                Err(_) => 0,
            };
            panic!(
                "station `{GRID}` không có order sau {timeout_secs}s \
                 (tick-candle: {n_candle} obs, grid: {grid_n} obs)"
            );
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
    //
    // Nhánh tick của script **không** tự gộp nến nữa — node `tick_2_candle` làm
    // việc đó (xem `strategies/binance/config.toml`: `tick-map → tick_2_candle →
    // grid`). Test dựng đúng chuỗi đó, nếu không thì tick không bao giờ thành
    // nến và kernel không có gì để vào lệnh.
    let mut transform = RhaiTransform::new_file(GRID, &["candles", "clock", CANDLE], SCRIPT);
    transform.params = params();

    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(Input {
            id: "candles".into(),
        }),
        Arc::new(Clock::new(Duration::from_secs(1))),
        Arc::new(Tick2Candle {
            id: CANDLE.into(),
            inputs: vec!["candles".into()],
            resolution: "1m".into(),
            symbol: SYMBOL.into(),
            unit_ms: 1,
            stale_secs: 0,
            station: true,
        }),
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

    // Nạp nến qua Input → node `tick_2_candle` gom rồi ghi vào station `CANDLE`.
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
    // **Một** tick ở bucket kế tiếp để đẩy nến cuối đóng.
    //
    // Đây là luật của nến chảy, không phải chi tiết của test: nến bucket B chỉ
    // là dữ liệu cuối khi đã thấy tick ở bucket B + step. Nến chưa đóng thì
    // **đúng là** chưa được ghi — đó là điều kiện để lỡ sót nến chưa hoàn tất.
    // Ở hệ thật thì tick kế tiếp tới 1 phút sau sẽ tự đẩy, không cần node nào
    // nhắc; ở test thì tick kế tiếp là tick cuối nên phải bơm thêm.
    rt.inject(
        "candles".into(),
        Message {
            payload: tick_payload(open_bucket, 100.0),
        },
    )
    .await
    .expect("inject tick mở bucket kế tiếp");
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
    //
    // Append-only nên **không** khoá cứng số cursor: pipeline chạy bao lâu trước
    // khi assert phụ thuộc tốc độ máy (CI chậm hơn local, chạy được 2 nến đóng
    // → 2 cursor; local ~0.5s chỉ kịp 1 nến). Assert theo tính chất: có
    // cursor, mỗi nến đúng 1 cursor, và nến cuối đã đóng nhất định có cursor.
    let mut cursor: Vec<&Observation> = obs
        .iter()
        .filter(|o| o.labels.get("kind").map(String::as_str) == Some("trading_step"))
        .collect();
    assert!(!cursor.is_empty(), "phải có cursor trading_step: {obs:?}");
    cursor.sort_by_key(|c| c.ts);
    let ts: Vec<i64> = cursor.iter().map(|c| c.ts).collect();
    let mut uniq = ts.clone();
    uniq.dedup();
    assert_eq!(ts, uniq, "mỗi nến chỉ có một cursor: {cursor:?}");
    assert!(
        ts.contains(&last_closed),
        "cursor phải có nến vừa đóng gần nhất ({last_closed}): {cursor:?}"
    );
    for c in &cursor {
        assert_eq!(c.ts % 60, 0, "cursor ts theo bucket 60s: {c:?}");
        assert!(
            c.labels
                .get("candle_seq")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|seq| seq >= 1),
            "cursor phải lưu candle_seq để T+N hoạt động sau restart: {cursor:?}"
        );
    }
}
