//! Chạy `examples/prometheus-demo/rhai/disk_capacity_forecast.rhai` qua Rhai runtime.
//!
//! Đĩa là **một** cách cấu hình `capacity_forecast`, không phải phạm vi của nó —
//! nhưng script mẫu chính là cách cấu hình đó, nên đây là chỗ đúng để khẳng
//! định trên **giá trị**.
//!
//! Test ở đây bắt được cả hai lớp lỗi: accessor đăng ký sai (script không eval
//! được) và logic dự đoán sai (số ra sai). Vì vậy khẳng định trên **giá trị**,
//! không chỉ "không rỗng".

use opsense_rhai::{ScriptSource, call_process};
use std::path::Path;

fn script() -> ScriptSource {
    ScriptSource::File(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/prometheus-demo/rhai/disk_capacity_forecast.rhai"),
    )
}

const T0: i64 = 1_788_131_000;
const STEP: i64 = 900; // 15 phút
const N: i64 = 97; // 97 × 15 phút = đúng 24 giờ

/// `y = base + rate×(giờ) + amp×sóng tam giác(8h)` — cùng hình dạng với test Rust.
fn point(ts: i64, mp: &str, y: f64) -> serde_json::Value {
    serde_json::json!({
        "ts": ts,
        "metric_id": format!("disk-{mp}"),
        "kind": "metric",
        "signal": "raw",
        "value": y,
        "labels": {"mountpoint": mp, "device": "/dev/sda1"},
    })
}

fn wave(base: f64, rate_per_hour: f64, amp: f64) -> Vec<f64> {
    (0..N)
        .map(|i| {
            let hours = i as f64 * STEP as f64 / 3_600.0;
            let phase = (hours / 8.0).fract();
            let tri = if phase < 0.5 {
                1.0 - 4.0 * phase
            } else {
                4.0 * phase - 3.0
            };
            base + rate_per_hour * hours + amp * tri
        })
        .collect()
}

/// Ghép nhiều đĩa thành một window duy nhất.
fn windows(mounts: &[(&str, Vec<f64>)]) -> serde_json::Value {
    let all: Vec<serde_json::Value> = mounts
        .iter()
        .flat_map(|(mp, vals)| {
            vals.iter()
                .enumerate()
                .map(|(i, &y)| point(T0 + i as i64 * STEP, mp, y))
                .collect::<Vec<_>>()
        })
        .collect();
    serde_json::Value::Array(all)
}

/// Tìm observation theo `metric_id`.
fn find<'a>(out: &'a [serde_json::Value], metric_id: &str) -> &'a serde_json::Value {
    out.iter()
        .find(|o| o["metric_id"] == metric_id)
        .unwrap_or_else(|| panic!("không có observation {metric_id} trong {out:#?}"))
}

fn num(v: &serde_json::Value) -> f64 {
    v.as_f64().unwrap_or_else(|| panic!("value không phải số: {v}"))
}

#[tokio::test]
async fn every_disk_gets_three_observations() {
    // "/" đi lên + dao động, "/var" đi ngang + dao động, "/boot" đi xuống.
    let input = windows(&[
        ("/", wave(50.0, 0.5, 4.0)),
        ("/var", wave(30.0, 0.0, 4.0)),
        ("/boot", wave(20.0, -0.2, 0.5)),
    ]);

    let out = call_process(script(), input)
        .await
        .expect("script chạy được với 3 đĩa");
    assert_eq!(out.len(), 9, "3 đĩa × 3 observation:\n{out:#?}");

    for mp in ["/", "/var", "/boot"] {
        for prefix in ["disk_capacity_hours", "disk_capacity_trend", "disk_capacity_band"] {
            let id = format!("{prefix}:{mp}");
            let obs = find(&out, &id);
            assert!(num(&obs["value"]).is_finite(), "{id} có value không hữu hạn");
            assert_eq!(obs["labels"]["mountpoint"], mp, "{id} sai mountpoint");
            assert!(
                ["rising", "falling", "flat"].contains(&obs["labels"]["direction"].as_str().unwrap()),
                "{id} direction lạ: {}",
                obs["labels"]["direction"]
            );
        }
    }
}

#[tokio::test]
async fn rising_disk_gets_a_finite_eta_and_flat_disk_does_not() {
    let input = windows(&[("/", wave(50.0, 0.5, 4.0)), ("/var", wave(30.0, 0.0, 4.0))]);
    let out = call_process(script(), input).await.expect("script chạy được");

    // Đang đi lên ⇒ có mốc chạm trần ở tương lai, và cả hai mốc đều dương.
    let hours = num(&find(&out, "disk_capacity_hours:/")["value"]);
    let trend = num(&find(&out, "disk_capacity_trend:/")["value"]);
    assert!(hours > 0.0, "disk đi lên phải có ETA dương, got {hours}");
    assert!(trend > hours, "xu hướng ({trend}h) phải chậm hơn mép trên ({hours}h)");
    // Biên độ ~4% capacity, đi lên 12%/ngày ⇒ chênh lệch vài giờ, không phải vài tháng.
    assert!(
        trend - hours < 48.0,
        "chênh {}h là vô lý — biên độ hoặc slope bị sai",
        trend - hours
    );

    let dir = find(&out, "disk_capacity_hours:/")["labels"]["direction"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(dir, "rising", "phải nhận ra đi lên");
    assert_eq!(
        find(&out, "disk_capacity_hours:/")["labels"]["oscillating"],
        "yes",
        "dao động ±4% là dao động thật"
    );

    // Đi ngang ⇒ slope 0 ⇒ không có mốc chạm trần nào. Script ép -1 để series
    // không biến mất (thiếu series thì alert không bắt được).
    let flat = num(&find(&out, "disk_capacity_hours:/var")["value"]);
    assert!((flat + 1.0).abs() < 1e-9, "disk đi ngang phải trả -1, got {flat}");
    assert_eq!(
        find(&out, "disk_capacity_hours:/var")["labels"]["direction"],
        "flat"
    );
}

#[tokio::test]
async fn band_is_reported_in_grid_cells() {
    // Phải là chuỗi **có** xu hướng: sieve nhìn giá trị thô, và chuỗi đứng yên
    // quanh một mức thì ngưỡng dừng cho lưới mịn hơn hẳn (step 1.5625 / 64 dải
    // thay vì 12.5 / 8 dải) — biên cùng 4 đơn vị đó trải ra 2.5 dải thay vì 0.3.
    let input = windows(&[("/", wave(50.0, 0.5, 4.0))]);
    let out = call_process(script(), input).await.expect("script chạy được");

    let band = num(&find(&out, "disk_capacity_band:/")["value"]);
    // Dao động ±4% trên capacity 100, sieve chọn step ≈ 12.5 ⇒ biên ≈ 0.3 ô.
    // Chỉ kiểm tra dải rộng vì `grid.step` phụ thuộc thuật toán sieve.
    assert!(
        (0.05..1.0).contains(&band),
        "biên độ {band} ô lưới không hợp lý — sai phép chia hoặc sai grid.step"
    );
    assert_eq!(
        find(&out, "disk_capacity_band:/")["labels"]["oscillating"],
        "yes"
    );
}

#[tokio::test]
async fn disks_with_too_few_points_are_skipped() {
    // 4 điểm < min_samples (12) ⇒ không đĩa nào đủ dữ liệu dự đoán.
    let few: Vec<serde_json::Value> = (0..4)
        .map(|i| point(T0 + i * STEP, "/", 50.0 + i as f64))
        .collect();
    let out = call_process(script(), serde_json::Value::Array(few))
        .await
        .expect("script không lỗi khi thiếu dữ liệu");
    assert!(out.is_empty(), "phải bỏ qua đĩa thiếu dữ liệu, không đoán bừa: {out:#?}");

    // Cửa sổ rỗng sau khi cắt recent_secs.
    let out = call_process(script(), serde_json::Value::Array(vec![]))
        .await
        .expect("script không lỗi khi input rỗng");
    assert!(out.is_empty());
}

#[tokio::test]
async fn handles_large_first_window() {
    // Stress: window đầu tiên (cursor=0) có thể chứa cả lịch sử trong persistence.
    // Script cắt theo `recent_secs` nên phần còn lại vẫn nhỏ; nếu cắt hỏng,
    // group/fit sẽ vượt `max_operations` của Rhai.
    let n: i64 = std::env::var("CAPACITY_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12_000);
    let big: Vec<serde_json::Value> = (0..n)
        .map(|i| {
            let y = 35.0 + (i % 9) as f64 * 0.5;
            point(T0 + (i - n) * STEP, "/", y)
        })
        .collect();
    let out = call_process(script(), serde_json::Value::Array(big))
        .await
        .expect("script xử lý được 12k điểm");
    assert_eq!(out.len(), 3, "1 đĩa × 3 observation:\n{out:#?}");
}
