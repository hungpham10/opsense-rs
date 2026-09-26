//! Unit tests for `strategies/predict/predict.rhai` driven through the general
//! station lookup API (`call_process_with` with a `Context`).
//!
//! Script branch theo trigger (`trigger()` = `payload.src` của message):
//!   - clock ping (trigger None/""/"clock") → recompute: query `live-feed`
//!     station → grid/transition forecast (`labels.check = "prediction"`).
//!   - live source message (trigger "live-feed") → check: live sample
//!     (metric `*_live_*`) vs prediction cũ trong own station
//!     (`labels.check = "result"`, value 1.0/0.0).
//!
//! Single source "live-feed" vừa cung cấp history window vừa live samples
//! (1 http source duy nhất trong config).

use opsense_core::{Context, Observation, Station, TimeseriesStation};
use opsense_model::events::{Signal, TelemetryKind};
use opsense_model::secret::Secret;
use opsense_rhai::{ScriptSource, call_process_with};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const SCRIPT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../strategies/predict/predict.rhai"
));

fn obs(ts: i64, id: &str, value: f64) -> Observation {
    Observation::new(ts, id.into(), TelemetryKind::Metric, Signal::Raw, value)
}

fn params() -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    m.insert("window_source".into(), Value::from("live-feed"));
    m.insert("live_source".into(), Value::from("live-feed"));
    m.insert("own_station".into(), Value::from("predict"));
    m.insert("tolerance".into(), Value::from(1.0));
    m
}

fn attrs() -> BTreeMap<String, String> {
    BTreeMap::new()
}

async fn make_ctx() -> Arc<Context> {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));
    for id in ["live-feed", "predict"] {
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

/// History window (ts dời về quá khứ, max ≤ now-60): cpu_ramp 3.0 → 18.0
/// (ts ≡ now mod 3), cpu_steady hằng 42.0 (ts ≡ now+1 mod 3). Mỗi metric ts
/// khác nhau vì `update_range` dedup theo ts.
fn window_obs(now: i64) -> Vec<Observation> {
    let mut out = Vec::new();
    for i in 0..=20 {
        out.push(obs(now - 120 + 3 * i, "cpu_ramp", 3.0 + 0.75 * i as f64));
    }
    for i in 0..=19 {
        out.push(obs(now - 119 + 3 * i, "cpu_steady", 42.0));
    }
    out
}

/// Live samples (metric riêng `*_live_*` — tránh nhiễu grid, và ts khác
/// prediction để tsdb không dedup): cpu_live_ramp=999 (lệch xa prediction
/// ~18) → miss, cpu_live_steady=42.0 (khớp) → match.
fn live_obs(now: i64) -> Vec<Observation> {
    vec![
        obs(now - 30, "cpu_live_ramp", 999.0),
        obs(now - 31, "cpu_live_steady", 42.0),
    ]
}

fn live_obs_json(now: i64) -> Value {
    serde_json::to_value(live_obs(now)).unwrap()
}

fn find<'a>(items: &'a [Value], id: &str) -> &'a Value {
    items
        .iter()
        .find(|v| v["metric_id"] == id)
        .expect(&format!("output phải có metric {id}"))
}

/// Clock ping (trigger ""/None) → chỉ có predictions, không có check.
async fn tick_recompute(ctx: &Arc<Context>) -> Vec<Value> {
    call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        Value::Array(vec![]),
        params(),
        attrs(),
        None,
        Some(ctx.clone()),
        std::sync::Arc::new(Vec::new()),
    )
    .await
    .expect("script chạy (recompute)")
}

/// Live source message (trigger "live-feed") + batch = live samples.
async fn tick_check(ctx: &Arc<Context>, input: Value) -> Vec<Value> {
    call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        input,
        params(),
        attrs(),
        Some("live-feed".into()),
        Some(ctx.clone()),
        std::sync::Arc::new(Vec::new()),
    )
    .await
    .expect("script chạy (check)")
}

#[tokio::test]
async fn predict_window_emits_forecasts() {
    let ctx = make_ctx().await;
    let now = opsense_components::signal::now_secs();
    write(&ctx, "live-feed", &window_obs(now), now).await;

    // Clock ping → recompute: 2 prediction, chưa có check.
    let out = tick_recompute(&ctx).await;
    assert_eq!(
        out.len(),
        2,
        "recompute chỉ có 2 prediction (chưa có check): {out:?}"
    );

    for item in &out {
        assert_eq!(item["signal"], "summary");
        assert_eq!(item["labels"]["check"], "prediction");
    }

    // cpu_ramp: v_last = 18.0 → dự báo vượt 18.0 (up_prob > down_prob).
    let ramp = find(&out, "cpu_ramp");
    assert!(
        ramp["value"].as_f64().unwrap() > 18.0,
        "cpu_ramp prediction phải > v_last=18.0, got {}",
        ramp["value"]
    );
    assert_ne!(ramp["labels"]["method"], "steady");

    // cpu_steady: span = 0 → nhánh steady, prediction = 42.0.
    let steady = find(&out, "cpu_steady");
    assert_eq!(steady["labels"]["method"], "steady");
    assert_eq!(steady["value"].as_f64().unwrap(), 42.0);
}

#[tokio::test]
async fn predict_checks_live_against_previous_prediction() {
    let ctx = make_ctx().await;
    let now = opsense_components::signal::now_secs();
    write(&ctx, "live-feed", &window_obs(now), now).await;

    // Clock ping → predictions lưu vào own station (transform flush output).
    let tick1 = tick_recompute(&ctx).await;
    assert_eq!(tick1.len(), 2, "recompute = 2 predictions");
    let preds: Vec<Observation> = tick1
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect();
    write(&ctx, "predict", &preds, now).await;

    // Live source message → chỉ có check (no new predictions).
    let tick2 = tick_check(&ctx, live_obs_json(now)).await;
    assert_eq!(tick2.len(), 2, "live message = 2 check: {tick2:?}");
    for item in &tick2 {
        assert_eq!(item["labels"]["check"], "result");
    }

    // cpu_live_ramp: 999 vs prediction ~19 → miss (value 0.0).
    let bad = find(&tick2, "cpu_live_ramp");
    assert_eq!(bad["value"].as_f64().unwrap(), 0.0, "ramp lệch xa → false");
    assert!(
        bad["labels"]["delta"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            > 900.0
    );

    // cpu_live_steady: 42 vs prediction 42 → match (value 1.0).
    let good = find(&tick2, "cpu_live_steady");
    assert_eq!(good["value"].as_f64().unwrap(), 1.0, "steady khớp → true");
    assert_eq!(
        good["labels"]["delta"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap(),
        0.0,
        "steady delta = 0"
    );
}

#[tokio::test]
async fn predict_empty_when_window_station_missing() {
    // Không đăng ký live-feed → station_query trả () → script thoát sớm.
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));
    let station = TimeseriesStation::from_storage("predict", ctx.storage())
        .await
        .unwrap();
    ctx.registry(
        "predict",
        Station::Timeseries(Arc::new(RwLock::new(station))),
    )
    .await
    .unwrap();

    let out = tick_recompute(&ctx).await;
    assert!(out.is_empty(), "thiếu source station → không có output");
}

#[tokio::test]
async fn predict_skips_check_without_prediction() {
    // Live message nhưng own station chưa có prediction → không output.
    let ctx = make_ctx().await;
    let now = opsense_components::signal::now_secs();
    write(&ctx, "live-feed", &window_obs(now), now).await;

    let out = tick_check(&ctx, live_obs_json(now)).await;
    assert!(out.is_empty(), "chưa có prediction → check không sinh gì");
}