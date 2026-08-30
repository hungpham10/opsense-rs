// Cảnh báo đĩa theo baseline: mỗi điểm disk_usage_ratio mới được so với
// trung bình 1 giờ trước đọc trực tiếp từ trạm "tsdb".
//
// Yêu cầu: pipeline có station_sink id="tsdb" nhận processed của disk-usage
// (xem examples/prometheus-demo/config.toml). Nếu trạm chưa có, mọi điểm
// vẫn đi qua với alert="no-baseline".
//
// Output: metric_id "disk_usage_checked" mang thêm nhãn
//   baseline (f64 hoặc ()), delta, alert ∈ ok|spike|saturated|no-baseline.

fn process(observations) {
    let now = now_secs();
    let base = ts_mean("tsdb", "processed", "disk_usage_ratio", now - 3600, now);

    observations.map(|o| {
        let has_base = base != ();
        let alert = if !has_base {
            "no-baseline"
        } else if o.value > 0.9 {
            "saturated"                   // gần full đĩa (ưu tiên cao nhất)
        } else if o.value > base + 0.05 {
            "spike"                       // vượt baseline >5 điểm %
        } else {
            "ok"
        };

        #{
            ts: o.ts,
            metric_id: "disk_usage_checked",
            kind: "metric",
            signal: "utilization",
            value: o.value,
            labels: #{
                mountpoint: o.labels.mountpoint,
                baseline: base,
                delta: if has_base { o.value - base } else { () },
                alert: alert,
            },
        }
    })
}
