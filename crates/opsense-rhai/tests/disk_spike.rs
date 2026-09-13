//! Executes the real `scripts/disk_spike_check.rhai` through the Rhai
//! runtime: no station -> `no-baseline`; station present -> alerts computed
//! against the 1h baseline pulled by `ts_mean`.

use opsense_core::registry;
use opsense_core::{Stage, Station};
use opsense_model::{Observation, Signal, TelemetryKind};
use opsense_rhai::{ScriptSource, call_process};
use std::sync::Arc;
use tokio::sync::RwLock;

fn script() -> ScriptSource {
    ScriptSource::File(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/disk_spike_check.rhai"),
    )
}

fn input_point(ts: i64, value: f64) -> serde_json::Value {
    serde_json::json!({
        "ts": ts,
        "metric_id": "disk_usage_ratio",
        "kind": "metric",
        "signal": "utilization",
        "value": value,
        "labels": {"mountpoint": "/", "device": "/dev/sda1"},
    })
}

fn seed_obs(ts: i64, value: f64) -> Observation {
    Observation::new(
        ts,
        "disk_usage_ratio".into(),
        TelemetryKind::Metric,
        Signal::Utilization,
        value,
    )
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn disk_spike_script_alert_flow() {
    opsense_rhai::register();
    let now = now_secs();

    // 1) No station yet: every point must pass through flagged no-baseline.
    let out = call_process(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.5)]),
    )
    .await
    .expect("script runs without station");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["labels"]["alert"], serde_json::json!("no-baseline"));

    // 2) Register "tsdb" with ~0.31 baseline history in the last hour.
    let st = Arc::new(RwLock::new(Station::timeseries(1024)));
    st.write()
        .await
        .append(
            Stage::Processed,
            &[seed_obs(now - 1800, 0.30), seed_obs(now - 900, 0.32)],
        )
        .await;
    assert!(registry::register_station("tsdb", st).await);

    // Spike: 0.50 vs baseline 0.31 (+0.05 threshold).
    let out = call_process(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.5)]),
    )
    .await
    .expect("script runs with station");
    assert_eq!(out[0]["labels"]["alert"], serde_json::json!("spike"));
    let delta = out[0]["labels"]["delta"].as_f64().unwrap();
    assert!((delta - 0.19).abs() < 1e-6);

    // Normal point stays ok.
    let out = call_process(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.33)]),
    )
    .await
    .expect("ok case");
    assert_eq!(out[0]["labels"]["alert"], serde_json::json!("ok"));

    // Saturated overrides spike when value > 0.9.
    let out = call_process(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.95)]),
    )
    .await
    .expect("saturated case");
    assert_eq!(out[0]["labels"]["alert"], serde_json::json!("saturated"));

    // Node `params` override the script's built-in threshold defaults:
    // raising `saturated` to 0.97 makes the 0.95 point a spike instead.
    let mut params = std::collections::BTreeMap::new();
    params.insert("saturated".to_string(), serde_json::json!(0.97));
    params.insert("spike_delta".to_string(), serde_json::json!(0.05));
    let out = opsense_rhai::call_process_with(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.95)]),
        params,
        std::collections::BTreeMap::new(),
    )
    .await
    .expect("script runs with tuned params");
    assert_eq!(out[0]["labels"]["alert"], serde_json::json!("spike"));
}
