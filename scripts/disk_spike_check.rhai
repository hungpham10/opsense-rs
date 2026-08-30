// Cảnh báo đĩa theo baseline: mỗi điểm disk_usage_ratio mới được so với
// trung bình 1 giờ trước đọc trực tiếp từ trạm "tsdb".
//
// Yêu cầu: pipeline có station_sink id="tsdb" nhận processed của disk-usage
// (xem examples/prometheus-demo/config.toml). Nếu trạm chưa có, mọi điểm
// vẫn đi qua với alert="no-baseline".
//
// Ngưỡng cấu hình được qua `params` của node rhai_transform (fallback giá trị
// mặc định khi node không khai báo):
//
//   [[pipeline.components]]
//   type = "rhai_transform"
//   id   = "disk-alert"
//   inputs = ["disk-usage"]
//   script_path = "scripts/disk_spike_check.rhai"
//
//   [pipeline.components.params]
//   saturated   = 0.9    # gần full đĩa (ưu tiên cao nhất)
//   spike_delta = 0.05   # vượt baseline >5 điểm % tính spike
//
// Output: metric_id "disk_usage_checked" mang thêm nhãn
//   baseline (f64 hoặc ()), delta, alert ∈ ok|spike|saturated|no-baseline.

fn process(observations) {
    let now = now_secs();
    let base = ts_mean("tsdb", "processed", "disk_usage_ratio", now - 3600, now);

    // Ngưỡng đến từ params của node; khi không khai báo thì dùng mặc định.
    // (try/catch là statement trong rhai nên phải tách khỏi let.)
    let saturated = 0.9;
    try { saturated = param_saturated } catch { 0.9 };
    let spike_delta = 0.05;
    try { spike_delta = param_spike_delta } catch { 0.05 };

    observations.map(|o| {
        let has_base = base != ();
        let alert = if !has_base {
            "no-baseline"
        } else if o.value > saturated {
            "saturated"                   // gần full đĩa (ưu tiên cao nhất)
        } else if o.value > base + spike_delta {
            "spike"                       // vượt baseline quá ngưỡng spike
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
