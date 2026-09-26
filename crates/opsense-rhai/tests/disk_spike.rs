//! Executes the real `disk_spike_check.rhai` through the Rhai
//! runtime: computes baseline from input window, supports param overrides.

use opsense_rhai::{ScriptSource, call_process, call_process_with};
use std::path::Path;

fn script() -> ScriptSource {
    ScriptSource::File(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/prometheus-demo/rhai/disk_spike_check.rhai"),
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

#[tokio::test]
async fn disk_spike_script_alert_flow() {
    let now = 1_700_000_000i64;

    // 1) Single point: no baseline can be computed (count=1, base=that value, but spike needs > base+delta)
    // Actually with 1 point, base = that value, so value > base + 0.05 is false → "ok"
    let out = call_process(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.5)]),
    )
    .await
    .expect("script runs without baseline override");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["labels"]["alert"], "ok");

    // 2) Multiple points forming a baseline ~0.5, spike to 0.6 with default spike_delta=0.05
    let baseline_pts: Vec<_> = (0..10).map(|i| input_point(now - i * 60, 0.5)).collect();
    let spike_pt = input_point(now, 0.6); // 0.6 > 0.5 + 0.05 → spike
    let mut input = baseline_pts;
    input.push(spike_pt);

    let out = call_process(script(), serde_json::Value::Array(input))
        .await
        .expect("script runs with baseline");
    assert_eq!(out.len(), 11);
    // Last point should be spike
    assert_eq!(out.last().unwrap()["labels"]["alert"], "spike");
    // Earlier points should be ok (0.5 not > 0.5+0.05)
    for o in &out[..10] {
        assert_eq!(o["labels"]["alert"], "ok");
    }

    // 3) Saturated threshold: value 0.95 > saturated(0.9) → saturated
    let out = call_process(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.95)]),
    )
    .await
    .expect("script runs saturated");
    assert_eq!(out[0]["labels"]["alert"], "saturated");

    // 4) Param override for baseline
    let params = {
        let mut m = std::collections::BTreeMap::new();
        m.insert("baseline".into(), serde_json::Value::from(0.5));
        m
    };
    let out = call_process_with(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.61)]),
        params,
        std::collections::BTreeMap::new(),
        None,
        None,
        std::sync::Arc::new(Vec::new()),
    )
    .await
    .expect("script runs with param_baseline");
    // 0.61 > 0.5 + 0.05 → spike
    assert_eq!(out[0]["labels"]["alert"], "spike");

    // 5) Param override for saturated
    let params = {
        let mut m = std::collections::BTreeMap::new();
        m.insert("saturated".into(), serde_json::Value::from(0.8));
        m
    };
    let out = call_process_with(
        script(),
        serde_json::Value::Array(vec![input_point(now, 0.85)]),
        params,
        std::collections::BTreeMap::new(),
        None,
        None,
        std::sync::Arc::new(Vec::new()),
    )
    .await
    .expect("script runs with param_saturated");
    // 0.85 > 0.8 → saturated
    assert_eq!(out[0]["labels"]["alert"], "saturated");
}
