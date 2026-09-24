//! Test for strategies/prometheus/script.rhai

use opsense_rhai::{ScriptSource, call_process};
use std::path::PathBuf;

fn script_path() -> String {
    // From crates/opsense-rhai/tests/ to workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../strategies/prometheus/script.rhai")
        .to_string_lossy()
        .to_string()
}

fn obs(ts: i64, metric_id: &str, value: f64) -> serde_json::Value {
    serde_json::json!({
        "ts": ts,
        "metric_id": metric_id,
        "kind": "metric",
        "signal": "utilization",
        "value": value,
        "labels": {"host": "test-host"}
    })
}

#[tokio::test]
async fn test_prometheus_script_computes_stats() {
    // Test data: 3 metrics with multiple observations each
    let input = vec![
        obs(1000, "cpu_usage", 10.0),
        obs(1060, "cpu_usage", 20.0),
        obs(1120, "cpu_usage", 30.0),
        obs(1000, "memory_usage", 100.0),
        obs(1060, "memory_usage", 200.0),
        obs(1120, "memory_usage", 150.0),
        obs(1000, "disk_io", 50.0),
        obs(1060, "disk_io", 60.0),
    ];

    let out = call_process(
        ScriptSource::File(PathBuf::from(script_path())),
        serde_json::Value::Array(input),
    )
    .await
    .expect("script must run");

    // Should have 3 output observations (one per metric_id)
    assert_eq!(out.len(), 3, "should have 3 output metrics");

    // Verify cpu_usage: values 10, 20, 30 -> mean=20, min=10, max=30, variance=100
    let cpu_out = out.iter().find(|o| o["metric_id"] == "cpu_usage").expect("cpu_usage output");
    assert_eq!(cpu_out["signal"], "summary");
    assert_eq!(cpu_out["labels"]["test_case"], "prometheus");
    assert!((cpu_out["value"].as_f64().unwrap() - 20.0).abs() < 0.001, "cpu mean");
    assert!((cpu_out["labels"]["mean"].as_str().unwrap().parse::<f64>().unwrap() - 20.0).abs() < 0.001);
    assert!((cpu_out["labels"]["min"].as_str().unwrap().parse::<f64>().unwrap() - 10.0).abs() < 0.001);
    assert!((cpu_out["labels"]["max"].as_str().unwrap().parse::<f64>().unwrap() - 30.0).abs() < 0.001);
    assert!((cpu_out["labels"]["variance"].as_str().unwrap().parse::<f64>().unwrap() - 100.0).abs() < 0.001);
    assert_eq!(cpu_out["labels"]["count"], "3");

    // Verify memory_usage: values 100, 200, 150 -> mean=150, min=100, max=200, variance=2500
    let mem_out = out.iter().find(|o| o["metric_id"] == "memory_usage").expect("memory_usage output");
    assert!((mem_out["value"].as_f64().unwrap() - 150.0).abs() < 0.001, "memory mean");
    assert!((mem_out["labels"]["min"].as_str().unwrap().parse::<f64>().unwrap() - 100.0).abs() < 0.001);
    assert!((mem_out["labels"]["max"].as_str().unwrap().parse::<f64>().unwrap() - 200.0).abs() < 0.001);
    assert!((mem_out["labels"]["variance"].as_str().unwrap().parse::<f64>().unwrap() - 2500.0).abs() < 0.001);

    // Verify disk_io: values 50, 60 -> mean=55, min=50, max=60, variance=50
    let disk_out = out.iter().find(|o| o["metric_id"] == "disk_io").expect("disk_io output");
    assert!((disk_out["value"].as_f64().unwrap() - 55.0).abs() < 0.001, "disk mean");
    assert!((disk_out["labels"]["min"].as_str().unwrap().parse::<f64>().unwrap() - 50.0).abs() < 0.001);
    assert!((disk_out["labels"]["max"].as_str().unwrap().parse::<f64>().unwrap() - 60.0).abs() < 0.001);
    assert!((disk_out["labels"]["variance"].as_str().unwrap().parse::<f64>().unwrap() - 50.0).abs() < 0.001);
}

#[tokio::test]
async fn test_prometheus_script_empty_input() {
    let out = call_process(
        ScriptSource::File(PathBuf::from(script_path())),
        serde_json::Value::Array(vec![]),
    )
    .await
    .expect("script must run");
    assert_eq!(out.len(), 0, "empty input should return empty output");
}

#[tokio::test]
async fn test_prometheus_script_single_metric() {
    let input = vec![
        obs(1000, "single_metric", 42.0),
        obs(1060, "single_metric", 58.0),
    ];

    let out = call_process(
        ScriptSource::File(PathBuf::from(script_path())),
        serde_json::Value::Array(input),
    )
    .await
    .expect("script must run");

    assert_eq!(out.len(), 1);
    let out = &out[0];
    assert_eq!(out["metric_id"], "single_metric");
    assert!((out["value"].as_f64().unwrap() - 50.0).abs() < 0.001);
    assert_eq!(out["labels"]["count"], "2");
}