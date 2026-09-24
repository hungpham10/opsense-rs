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
    Context, Observation, Station, TimeseriesStation, candles_from_observations,
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

fn field_value(rows: &[Observation], field: &str) -> f64 {
    rows.iter()
        .find(|o| o.labels.get("field").map(String::as_str) == Some(field))
        .unwrap_or_else(|| panic!("output phải có field `{field}`"))
        .value
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
    call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        input,
        params(),
        attrs(),
        trigger,
        Some(ctx.clone()),
    )
    .await
    .expect("script chạy")
}

#[tokio::test]
async fn tick_accumulates_candle_across_ticks() {
    let ctx = make_ctx().await;
    let now = now();
    let bucket = now / 60 * 60;
    let bucket_ms = bucket * 1000;

    // Tick 1 (100.0) → candle mới: o=h=l=c=100, v=1.
    let out1 = run(&ctx, Some("tick".into()), tick_json(bucket_ms, 100.0)).await;
    assert_eq!(out1.len(), 5, "1 tick → 5 obs OHLCV: {out1:?}");
    let rows1 = to_observations(&out1);
    assert!(rows1.iter().all(|o| o.ts == bucket), "ts = bucket giây");
    assert!(rows1.iter().all(|o| o.metric_id == SYMBOL));
    assert!(
        rows1
            .iter()
            .all(|o| o.labels.get("resolution").map(String::as_str) == Some(RES)),
        "mọi obs mang labels.resolution"
    );
    assert_eq!(field_value(&rows1, "o"), 100.0);
    assert_eq!(field_value(&rows1, "h"), 100.0);
    assert_eq!(field_value(&rows1, "l"), 100.0);
    assert_eq!(field_value(&rows1, "c"), 100.0);
    assert_eq!(field_value(&rows1, "v"), 1.0);
    write(&ctx, "grid", &rows1, now).await;

    // Tick 2 (95.0) → đọc lại candle qua station: o giữ 100, l/c = 95, v=2.
    let rows2 = to_observations(
        &run(&ctx, Some("tick".into()), tick_json(bucket_ms, 95.0)).await,
    );
    write(&ctx, "grid", &rows2, now).await;
    assert_eq!(field_value(&rows2, "o"), 100.0, "open giữ giá đầu bucket");
    assert_eq!(field_value(&rows2, "h"), 100.0);
    assert_eq!(field_value(&rows2, "l"), 95.0);
    assert_eq!(field_value(&rows2, "c"), 95.0);
    assert_eq!(field_value(&rows2, "v"), 2.0, "volume đếm số tick đã gộp");

    // Tick 3 (105.0) → h=105, c=105, v=3 (state sống qua station).
    let rows3 = to_observations(
        &run(&ctx, Some("tick".into()), tick_json(bucket_ms, 105.0)).await,
    );
    assert_eq!(field_value(&rows3, "o"), 100.0);
    assert_eq!(field_value(&rows3, "h"), 105.0, "high cập nhật từ state station");
    assert_eq!(field_value(&rows3, "l"), 95.0);
    assert_eq!(field_value(&rows3, "c"), 105.0);
    assert_eq!(field_value(&rows3, "v"), 3.0);
    write(&ctx, "grid", &rows3, now).await;

    // Đọc lại station: 5 field cùng ts phải reconcile thành ĐÚNG 1 candle
    // (không bị dedup theo ts nuốt mất field).
    let mut all = rows1.clone();
    all.extend(rows2.iter().cloned());
    all.extend(rows3.iter().cloned());
    let candles = candles_from_observations(&all, RES);
    assert_eq!(candles.len(), 1, "3 tick cùng bucket → 1 candle: {candles:?}");
    assert_eq!(candles[0].t, bucket);
    assert_eq!((candles[0].o, candles[0].h, candles[0].l, candles[0].c), (100.0, 105.0, 95.0, 105.0));
    assert_eq!(candles[0].v, 3.0, "volume = số tick gộp, bản mới nhất thắng");
}

#[tokio::test]
async fn tick_skips_observation_without_ts() {
    let ctx = make_ctx().await;
    let bucket = now() / 60 * 60;

    // Batch có 1 obs thiếu `ts` (malformed) + 1 tick hợp lệ → chỉ tick sinh candle.
    let input = json!([
        { "metric_id": SYMBOL, "value": 1.0 },
        { "ts": bucket * 1000, "metric_id": SYMBOL, "value": 42.0 }
    ]);
    let out = run(&ctx, Some("tick".into()), input).await;
    assert_eq!(out.len(), 5, "chỉ 1 tick hợp lệ → 5 obs: {out:?}");
    let rows = to_observations(&out);
    assert_eq!(field_value(&rows, "c"), 42.0, "giá của tick hợp lệ");
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
