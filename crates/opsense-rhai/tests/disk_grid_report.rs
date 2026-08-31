//! Executes `scripts/disk_grid_report.rhai` through the Rhai runtime.

use opsense_rhai::{call_process, ScriptSource};
use std::path::Path;

fn script() -> ScriptSource {
    ScriptSource::File(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/disk_grid_report.rhai"),
    )
}

fn point(ts: i64, mp: &str, value: f64) -> serde_json::Value {
    serde_json::json!({
        "ts": ts,
        "metric_id": format!("disk-{mp}"),
        "kind": "metric",
        "signal": "raw",
        "value": value,
        "labels": {"mountpoint": mp, "device": "/dev/sda1"},
    })
}

#[tokio::test]
async fn disk_grid_report_flow() {
    opsense_rhai::register();
    let now = 1_788_131_000i64;
    let input = serde_json::Value::Array(vec![
        point(now, "/", 35.7),
        point(now, "/boot/efi", 9.5),
        point(now, "/run", 0.14),
        point(now, "/run/lock", 0.0),
    ]);
    // Stress: window đầu tiên (cursor=0) có thể chứa ~7k điểm (24h lookback);
    // 12k điểm là biên trên — vượt ~13k sẽ chạm max_map_size toàn cục của Rhai
    // khi đọc labels từng observation (giới hạn động cơ, không phải script).
    let mut big: Vec<serde_json::Value> = Vec::new();
    let n: i64 = std::env::var("GRID_N").ok().and_then(|v| v.parse().ok()).unwrap_or(12_000);
    for i in 0..n {
        big.push(point(now - (n - i) * 60, "/", 35.0 + (i % 7) as f64));
    }
    let out_big = call_process(script(), serde_json::Value::Array(big))
        .await
        .expect("script handles 12k points");
    assert!(!out_big.is_empty());

    let out = call_process(script(), input).await.expect("script runs");
    eprintln!("out = {out:?}");
    assert_eq!(out.len(), 5); // 4 đĩa + 1 summary
    assert!(out[0]["metric_id"].as_str().unwrap().starts_with("disk_grid_band:"));
    assert!(out[0]["labels"]["band"].is_string());
    assert_eq!(out[4]["metric_id"], serde_json::json!("disk_grid_summary"));
}
