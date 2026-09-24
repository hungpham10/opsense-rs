//! Test for strategies/prometheus/script.rhai

use opsense_rhai::{ScriptSource, call_process};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

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

    // ── AnalysisGrid + TransitionAnalysis (native, qua `analysis_labels`) ──
    fn label_f64(summary: &serde_json::Value, key: &str) -> f64 {
        summary["labels"][key]
            .as_str()
            .unwrap_or_else(|| panic!("missing label `{key}` in {summary}"))
            .parse()
            .unwrap_or_else(|e| panic!("label `{key}` không phải số float trong {summary}: {e}"))
    }

    // cpu_usage: ts-sorted 10→20→30 (tăng đều) → grid + transitions có dữ
    // liệu: 3 bucket (interval = chu kỳ lấy mẫu 60s), probabilities hợp
    // chuẩn (tổng = 1) và không nghiêng về down.
    let cpu = out.iter().find(|o| o["metric_id"] == "cpu_usage").expect("cpu_usage output");
    let cpu_up = label_f64(cpu, "up_prob");
    let cpu_down = label_f64(cpu, "down_prob");
    let cpu_stay = label_f64(cpu, "stay_prob");
    assert!(
        (cpu_up + cpu_down + cpu_stay - 1.0).abs() < 1e-6,
        "cpu: up+down+stay phải khép kín = 1 (got {cpu_up}+{cpu_down}+{cpu_stay})"
    );
    assert!(
        cpu_up >= cpu_down,
        "chuỗi tăng đều (10→20→30) không được nghiêng down: up={cpu_up} down={cpu_down}"
    );
    assert!(label_f64(cpu, "grid_step") > 0.0, "grid_step phải > 0");
    assert!(label_f64(cpu, "grid_cells") >= 1.0, "grid phải có ≥1 cell");
    let cpu_buckets: i64 = cpu["labels"]["trans_buckets"]
        .as_str()
        .unwrap_or_else(|| panic!("cpu thiếu trans_buckets"))
        .parse()
        .expect("trans_buckets là số");
    assert!(cpu_buckets >= 3, "cpu có 3 bucket ts (interval=60): got {cpu_buckets}");

    // memory_usage 100→200→150 (lên rồi xuống): tổng xác suất vẫn khép kín.
    let mem = out.iter().find(|o| o["metric_id"] == "memory_usage").expect("memory_usage output");
    let mem_up = label_f64(mem, "up_prob");
    let mem_down = label_f64(mem, "down_prob");
    let mem_stay = label_f64(mem, "stay_prob");
    assert!(
        (mem_up + mem_down + mem_stay - 1.0).abs() < 1e-6,
        "mem: up+down+stay phải khép kín = 1 (got {mem_up}+{mem_down}+{mem_stay})"
    );
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

// ──────────────────────────────────────────────
// Debug logging: `print()`/`debug()` trong script phải route vào tracing
// (io writer tùy biến) chứ không rơi vào stdout — chính là cách xem log
// script khi chạy pipeline. Bật: RUST_LOG=opsense_rhai=info|debug.
// ──────────────────────────────────────────────

struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Khởi tạo một tracing subscriber ghi vào buffer chung (một lần/process).
fn log_capture_buffer() -> Arc<Mutex<Vec<u8>>> {
    static BUF: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    let buf = BUF.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = buf.clone();
        // `with_max_level(TRACE)` — không cần feature env-filter; chỉ test
        // binary này dùng subscriber, nên capture toàn bộ là an toàn.
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
            .with_ansi(false)
            .with_writer(move || SharedWriter(writer.clone()))
            .try_init();
        buf
    });
    buf.clone()
}

#[tokio::test]
async fn test_script_print_debug_routes_to_tracing() {
    let buf = log_capture_buffer();
    buf.lock().unwrap().clear();

    let src = r#"
fn process(obs) {
    print("hello-from-script-n=" + obs.len());
    debug("debug-marker");
    [ #{ ts: 1, metric_id: "t", kind: "metric", signal: "raw", value: 1.0 } ]
}
"#;
    call_process(
        ScriptSource::Inline(src.to_string()),
        serde_json::Value::Array(vec![obs(1, "t", 1.0)]),
    )
    .await
    .expect("script must run");

    let captured = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
    assert!(
        captured.contains("hello-from-script-n=1"),
        "print() phải route vào tracing (script log): {captured}"
    );
    assert!(
        captured.contains("debug-marker"),
        "debug() phải route vào tracing (script log): {captured}"
    );
}