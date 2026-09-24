//! Unit tests for `strategies/predict/predict.rhai` driven through the general
//! station lookup API (`call_process_with` with a `Context`).
//!
//! The script receives a clock ping (empty payload) and pulls everything
//! itself via `station_query`: window history → grid/transition forecast
//! (`labels.check = "prediction"`), then live sample vs the previous
//! prediction ("result", value 1.0/0.0).
//!
//! Two ticks are simulated: the first call yields predictions only (own
//! station empty), the test mirrors the transform's own-station write, and the
//! second call emits the checks.

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
    m.insert("window_source".into(), Value::from("window-feed"));
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
    for id in ["window-feed", "live-feed", "predict"] {
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

/// Cửa sổ lịch sử (ts dời về quá khứ, max ≤ now-60): cpu_ramp 3.0 → 18.0
/// (ts ≡ now mod 3), cpu_steady hằng 42.0 (ts ≡ now+1 mod 3). Mỗi metric ts
/// khác nhau vì `TimeseriesStation::update_range` dedup theo ts — và phải khác
/// cả ts của live (pred/check không được trùng ts ở tsdb).
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

/// Realtime (sau cửa sổ, trước now): cpu_ramp đột biến 999 (so với prediction
/// ~18) → miss, cpu_steady 42.0 (khớp prediction) → match. Ts khác nhau giữa 2
/// metric và khác hẳn một số ts của prediction.
fn live_obs(now: i64) -> Vec<Observation> {
    vec![
        obs(now - 30, "cpu_ramp", 999.0),
        obs(now - 31, "cpu_steady", 42.0),
    ]
}

fn find<'a>(items: &'a [Value], id: &str) -> &'a Value {
    items
        .iter()
        .find(|v| v["metric_id"] == id)
        .expect(&format!("output phải có metric {id}"))
}

#[tokio::test]
async fn predict_window_emits_forecasts() {
    let ctx = make_ctx().await;
    let now = opsense_components::signal::now_secs();
    write(&ctx, "window-feed", &window_obs(now), now).await;
    write(&ctx, "live-feed", &live_obs(now), now).await;

    // Tick 1: own station rỗng → chỉ có prediction, chưa có check.
    let out = call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        Value::Array(vec![]),
        params(),
        attrs(),
        Some(ctx.clone()),
    )
    .await
    .expect("script chạy");
    assert_eq!(
        out.len(),
        2,
        "tick 1 chỉ có 2 prediction (chưa có check): {out:?}"
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
    write(&ctx, "window-feed", &window_obs(now), now).await;
    write(&ctx, "live-feed", &live_obs(now), now).await;

    // Tick 1 → predictions lưu vào station của chính predict (mô phỏng đúng
    // việc transform flush output vào own station sau script).
    let tick1 = call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        Value::Array(vec![]),
        params(),
        attrs(),
        Some(ctx.clone()),
    )
    .await
    .expect("tick 1 script chạy");
    assert_eq!(tick1.len(), 2, "tick 1 = 2 predictions");
    let preds: Vec<Observation> = tick1
        .iter()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .collect();
    write(&ctx, "predict", &preds, now).await;

    // Tick 2 → có prediction cũ trong own station → sinh check result.
    let tick2 = call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        Value::Array(vec![]),
        params(),
        attrs(),
        Some(ctx.clone()),
    )
    .await
    .expect("tick 2 script chạy");
    assert_eq!(tick2.len(), 4, "tick 2 = 2 prediction + 2 check");

    let checks: Vec<&Value> = tick2
        .iter()
        .filter(|v| v["labels"]["check"] == "result")
        .collect();
    assert_eq!(checks.len(), 2);

    // cpu_ramp: live 999 vs prediction ~19 → miss (value 0.0).
    let bad = checks
        .iter()
        .find(|c| c["metric_id"] == "cpu_ramp")
        .expect("check cpu_ramp");
    assert_eq!(bad["value"].as_f64().unwrap(), 0.0, "ramp lệch xa → false");
    assert!(
        bad["labels"]["delta"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            > 900.0
    );

    // cpu_steady: live 42 vs prediction 42 → match (value 1.0).
    let good = checks
        .iter()
        .find(|c| c["metric_id"] == "cpu_steady")
        .expect("check cpu_steady");
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
    // Không đăng ký window-feed → station_query trả () → script thoát sớm.
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));
    let station = TimeseriesStation::from_storage("live-feed", ctx.storage())
        .await
        .unwrap();
    ctx.registry(
        "live-feed",
        Station::Timeseries(Arc::new(RwLock::new(station))),
    )
    .await
    .unwrap();

    let out = call_process_with(
        ScriptSource::Inline(SCRIPT.into()),
        Value::Array(vec![]),
        params(),
        attrs(),
        Some(ctx.clone()),
    )
    .await
    .expect("script chạy (station_query trả ())");
    assert!(out.is_empty(), "thiếu window station → không có output");
}
