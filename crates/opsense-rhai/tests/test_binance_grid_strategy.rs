//! Unit tests cho `strategies/binance/grid.rhai` chạy qua `call_process_with`
//! + `Context` (station-as-DataLoader lookups).
//!
//! Script branch theo `trigger()`:
//!   - `"tick"` (aggTrade đã map bởi `json_2_json`: `ts` = **ms**, `value` =
//!     price) → gộp giá vào candle bucket hiện tại trong own station, trả về
//!     5 obs OHLCV của bucket đó.
//!   - trigger khác (clock ping `""` / klines `"history"`) → merge candles
//!     history + live → `grid_fit` + `transition_analysis` → 1 snapshot obs
//!     (`labels.kind = "snapshot"`).

use std::collections::BTreeMap;
use std::sync::Arc;

use opsense_core::{
    Context, Observation, Station, TimeseriesStation,
};
use opsense_model::events::{Signal, TelemetryKind};
use opsense_model::secret::Secret;
use opsense_rhai::{ScriptSource, call_process_with};
use serde_json::{Value, json};
use tokio::sync::RwLock;

const SCRIPT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../strategies/binance/grid.rhai"
));

const SYMBOL: &str = "BTCUSDT";
const RES: &str = "1m";

fn now() -> i64 {
    opsense_components::signal::now_secs()
}

fn params() -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    m.insert("history_source".into(), Value::from("history"));
    m.insert("own_station".into(), Value::from("grid"));
    m.insert("symbol".into(), Value::from(SYMBOL));
    m.insert("resolution".into(), Value::from(RES));
    m.insert("history_secs".into(), Value::from(3600));
    m.insert("live_window_secs".into(), Value::from(300));
    m
}

fn attrs() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// Một obs OHLCV đúng convention station (ts giây, `labels.field`).
fn candle_field_obs(ts: i64, field: &str, value: f64) -> Observation {
    Observation::new(ts, SYMBOL.into(), TelemetryKind::Metric, Signal::Raw, value)
        .with_label("field", field)
        .with_label("resolution", RES)
}

fn candle_rows(ts: i64, o: f64, h: f64, l: f64, c: f64, v: f64) -> Vec<Observation> {
    ["o", "h", "l", "c", "v"]
        .iter()
        .enumerate()
        .map(|(i, field)| candle_field_obs(ts, field, [o, h, l, c, v][i]))
        .collect()
}

fn to_observations(items: &[Value]) -> Vec<Observation> {
    items
        .iter()
        .map(|v| serde_json::from_value::<Observation>(v.clone()).expect("script output là Observation"))
        .collect()
}

/// Một tick đã qua `json_2_json` (payload map trực tiếp thành 1 observation).
fn tick_json(ts_ms: i64, price: f64) -> Value {
    json!([{
        "ts": ts_ms,
        "metric_id": SYMBOL,
        "kind": "metric",
        "signal": "raw",
        "value": price,
        "labels": { "resolution": RES }
    }])
}

async fn make_ctx() -> Arc<Context> {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));
    for id in ["history", "grid"] {
        let station = TimeseriesStation::from_storage(id, ctx.storage())
            .await
            .unwrap();
        ctx.registry(id, Station::Timeseries(Arc::new(RwLock::new(station))))
            .await
            .unwrap();
    }
    ctx
}

async fn write(ctx: &Arc<Context>, station_id: &str, obs: &[Observation], now: i64) {
    let from = obs.iter().map(|o| o.ts).min().unwrap();
    let to = obs.iter().map(|o| o.ts).max().unwrap();
    let st = ctx
        .station::<Arc<RwLock<TimeseriesStation>>>(station_id)
        .await
        .unwrap();
    st.write().await.update_range(obs, from, to, now);
}

async fn run(ctx: &Arc<Context>, trigger: Option<String>, input: Value) -> Vec<Value> {
    run_with(ctx, params(), trigger, input).await
}

async fn run_with(
    ctx: &Arc<Context>,
    params: BTreeMap<String, Value>,
    trigger: Option<String>,
    input: Value,
) -> Vec<Value> {
    call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        input,
        params,
        attrs(),
        trigger,
        Some(ctx.clone()),
        std::sync::Arc::new(Vec::new()),
    )
    .await
    .expect("script chạy")
}

/// Params bật nhánh trading (giống hệt `strategies/binance/config.toml`).
fn trading_params() -> BTreeMap<String, Value> {
    let mut m = params();
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

// ── gom tick thành nến KHÔNG còn ở script ──────────────────────────────────
//
// `tick_accumulates_candle_across_ticks` và `tick_skips_observation_without_ts`
// đã xoá: việc gom nến chuyển sang node `tick_2_candle`
// (`opsense-components/.../converters/tick2candle.rs`), nên script trả `[]` cho
// nhánh trigger `tick`.
//
// Vì sao chuyển (không phải vì thừa):
// - Script **stateless**: bản cũ phải `station_query` station của chính node rồi
//   gộp lại từ đầu mỗi batch, nên nến phụ thuộc thứ tự batch; batch thiếu thì
//   nến sai.
// - Nến **đã đóng** là dữ liệu cuối: bản cũ ghi cả nến đang mở và để kernel tự
//   lo bằng `last_closed` trong script. Nay chỉ ghi nến đóng, nên invariant
//   "chỉ nến đóng mới tới kernel" nằm ở đúng một chỗ.
// - Lỗi nến là lỗi dữ liệu, không phải lỗi chiến lược — nên log ở component.
//
// Phủ ở đâu: `Tick2Candle` có 9 unit test cho OHLC/biên/bucket/convention, và
// `e2e_binance_config::full_pipeline_ticks_and_snapshot` chạy pipeline thật
// (websocket → json_2_json → tick_2_candle) rồi đòi station `tick-candle` có
// nến **và** station `grid` có snapshot. Test dưới đây chỉ còn phần script:
// đọc nến đã gom sẵn từ `live_source`.
//
// Nhánh `tick` giờ phải trả rỗng — script không được ghi đè lên nến của node.
#[tokio::test]
async fn tick_branch_is_a_noop_now_that_a_component_builds_candles() {
    let ctx = make_ctx().await;
    let bucket = now() / 60 * 60;
    let out = run(&ctx, Some("tick".into()), tick_json(bucket * 1000, 42.0)).await;
    assert!(
        out.is_empty(),
        "script không tự gộp nến nữa — node `tick_2_candle` lo: {out:?}"
    );
}

#[tokio::test]
async fn snapshot_merges_history_and_live_candles() {
    let ctx = make_ctx().await;
    let now = now();
    let base = now / 60 * 60;

    // History: 12 candle 1m trong quá khứ, giá ramp tăng 100.5 → 106.0.
    let mut hist = Vec::new();
    for i in 1..=12 {
        let o = 100.0 + 0.5 * (i as f64 - 1.0);
        let c = 100.0 + 0.5 * i as f64;
        hist.extend(candle_rows(base - 60 * i, o, c + 0.2, o - 0.2, c, 10.0));
    }
    write(&ctx, "history", &hist, now).await;

    // Candle live của bucket đang mở (sinh từ tick) nằm trong own station.
    write(&ctx, "grid", &candle_rows(base, 106.0, 107.0, 105.5, 106.5, 4.0), now).await;

    // Clock ping (trigger None → "") → snapshot.
    let out = run(&ctx, None, Value::Array(vec![])).await;
    assert_eq!(out.len(), 1, "snapshot = 1 summary obs: {out:?}");
    let snap = &out[0];
    assert_eq!(snap["metric_id"], SYMBOL);
    assert_eq!(snap["signal"], "summary");
    assert_eq!(snap["labels"]["kind"], "snapshot");
    assert_eq!(snap["ts"], base, "ts = bucket candle cuối");
    assert_ne!(
        snap["labels"]["method"], "steady",
        "ramp tăng → transition dùng được: {snap:?}"
    );
    let step: f64 = snap["labels"]["grid_step"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(step > 0.0, "grid_step > 0: {snap:?}");
    let cells: i64 = snap["labels"]["cells"].as_str().unwrap().parse().unwrap();
    assert!(cells >= 1, "grid có ít nhất 1 cell: {snap:?}");
    let value = snap["value"].as_f64().unwrap();
    assert!(
        (value - 106.5).abs() <= step + 1e-9,
        "giá kỳ vọng lệch close cuối nhiều nhất 1 grid step: {snap:?}"
    );
    assert_eq!(
        snap["labels"]["live_candles"], "1",
        "candle live từ own station được merge vào fit: {snap:?}"
    );
    let up: f64 = snap["labels"]["up_prob"].as_str().unwrap().parse().unwrap();
    let down: f64 = snap["labels"]["down_prob"].as_str().unwrap().parse().unwrap();
    let stay: f64 = snap["labels"]["stay_prob"].as_str().unwrap().parse().unwrap();
    assert!(
        (up + down + stay - 1.0).abs() < 1e-6,
        "xác suất transition cộng lại = 1: {snap:?}"
    );
}

#[tokio::test]
async fn snapshot_also_runs_on_history_trigger() {
    let ctx = make_ctx().await;
    let now = now();
    let base = now / 60 * 60;

    let mut hist = Vec::new();
    for i in 1..=6 {
        let c = 100.0 + i as f64;
        hist.extend(candle_rows(base - 60 * i, c - 0.5, c + 0.1, c - 0.2, c, 3.0));
    }
    write(&ctx, "history", &hist, now).await;

    // Message klines mới từ http source mang `src = "history"` → vẫn snapshot.
    let out = run(&ctx, Some("history".into()), Value::Array(vec![])).await;
    assert_eq!(out.len(), 1, "trigger history → snapshot: {out:?}");
    assert_eq!(out[0]["labels"]["kind"], "snapshot");
}

#[tokio::test]
async fn snapshot_empty_without_data() {
    let ctx = make_ctx().await;
    // Station tồn tại nhưng chưa có candle nào (< 2 điểm để fit) → no output.
    let out = run(&ctx, None, Value::Array(vec![])).await;
    assert!(out.is_empty(), "không có candle → không output: {out:?}");
}

#[tokio::test]
async fn snapshot_empty_when_history_station_missing() {
    // Không đăng ký `history` → `station_candles` trả () → script thoát sớm.
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));
    let station = TimeseriesStation::from_storage("grid", ctx.storage())
        .await
        .unwrap();
    ctx.registry("grid", Station::Timeseries(Arc::new(RwLock::new(station))))
        .await
        .unwrap();

    let out = run(&ctx, None, Value::Array(vec![])).await;
    assert!(out.is_empty(), "thiếu station history → không output");
}

// ── Nhánh trading: kernel đặt lệnh qua `portfolio_feed` ─────────────────────

/// Nạp candle đã đóng vào station `grid` (như tick branch sẽ ghi) sao cho
/// nến CUỐI là nến đang mở (để nhánh trading bỏ qua, đúng hành vi thật).
///
/// Nến đóng gần nhất có range rộng (biến động) để chắc chắn chạm level grid —
/// nến range hẹp chỉ trúng level khi giá rơi đúng bậc, quá phụ thuộc dữ liệu.
async fn seed_candles(ctx: &Arc<Context>, closed: i64, open: i64) {
    let mut rows = Vec::new();
    for i in 0..closed {
        let price = 100.0 + ((i % 30) as f64) - 15.0;
        let ts = open - (closed - i) * 60;
        let (h, l) = if i == closed - 1 {
            (price + 15.0, price - 15.0) // nến biến động: phủ hết các cell
        } else {
            (price + 0.5, price - 0.5)
        };
        rows.extend(candle_rows(ts, price, h, l, price, 5.0));
    }
    // Nến đang mở (bucket = now) — chưa đóng nên không được giao.
    rows.extend(candle_rows(open, 100.0, 101.0, 99.0, 100.5, 1.0));
    write(ctx, "grid", &rows, now()).await;
}

#[tokio::test]
async fn trading_mode_places_orders_and_writes_cursor() {
    let ctx = make_ctx().await;
    let now = now();
    let open_bucket = now / 60 * 60;
    // 40 nến đã đóng + 1 nến đang mở.
    seed_candles(&ctx, 40, open_bucket).await;

    let out = run_with(
        &ctx,
        trading_params(),
        None,
        Value::Array(vec![]),
    )
    .await;
    let rows = to_observations(&out);

    let orders: Vec<&Observation> = rows.iter().filter(|o| o.signal == Signal::Order).collect();
    assert!(
        !orders.is_empty(),
        "giá nhấn sóng phải chạm grid level → có lệnh: {out:?}"
    );
    for o in &orders {
        assert_eq!(o.metric_id, SYMBOL);
        assert_eq!(o.labels.get("status").map(String::as_str), Some("open"));
        assert!(o.value > 0.0, "entry price dương: {o:?}");
        assert!(o.labels.contains_key("sl") && o.labels.contains_key("tp"));
        assert!(o.labels.contains_key("order_id"));
        assert_eq!(
            o.ts, open_bucket - 60,
            "lệnh gắn với nến ĐÃ ĐÓNG gần nhất, không phải nến đang mở"
        );
    }

    // Cursor: lần gọi sau không chạy lại nến đã xử lý.
    let cursor: Vec<&Observation> = rows
        .iter()
        .filter(|o| o.labels.get("kind").map(String::as_str) == Some("trading_step"))
        .collect();
    assert_eq!(cursor.len(), 1, "phải có 1 cursor: {out:?}");
    assert_eq!(cursor[0].ts, open_bucket - 60);
}

#[tokio::test]
async fn trading_mode_is_idempotent_across_calls() {
    let ctx = make_ctx().await;
    let open_bucket = now() / 60 * 60;
    seed_candles(&ctx, 40, open_bucket).await;

    let first = run_with(&ctx, trading_params(), None, Value::Array(vec![])).await;
    // Ghi output (lệnh + cursor) vào station như transform sẽ làm.
    let rows = to_observations(&first);
    assert!(!rows.is_empty(), "lần đầu phải có output: {first:?}");
    write(&ctx, "grid", &rows, now()).await;

    let second = run_with(&ctx, trading_params(), None, Value::Array(vec![])).await;
    let orders = second
        .iter()
        .filter(|v| v["signal"] == "order")
        .count();
    assert_eq!(orders, 0, "cursor chặn chạy lại nến đã xử lý: {second:?}");
}

#[tokio::test]
async fn trading_mode_skips_open_candle_only() {
    let ctx = make_ctx().await;
    let open_bucket = now() / 60 * 60;

    // Chỉ có nến đang mở (chưa đóng nến nào trong window) → không được giao.
    let rows = candle_rows(open_bucket, 100.0, 101.0, 99.0, 100.5, 1.0);
    write(&ctx, "grid", &rows, now()).await;

    let out = run_with(&ctx, trading_params(), None, Value::Array(vec![])).await;
    let orders = out.iter().filter(|v| v["signal"] == "order").count();
    assert_eq!(orders, 0, "nến chưa đóng thì không đặt lệnh: {out:?}");
}
