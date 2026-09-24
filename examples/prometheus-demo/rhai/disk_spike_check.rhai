// Cảnh báo đĩa theo baseline: mỗi điểm disk_usage_ratio mới được so với
// baseline được tính từ cửa sổ hiện tại (trung bình) hoặc override qua param.
//
// Ngưỡng cấu hình được qua `params` của node rhai_transform (fallback giá trị
// mặc định khi node không khai báo):
//
//   [[pipeline.components]]
//   type = "rhai_transform"
//   id   = "disk-alert"
//   inputs = ["disk-usage"]
//   script_path = "examples/prometheus-demo/rhai/disk_spike_check.rhai"
//
//   [pipeline.components.params]
//   saturated     = 0.9    # gần full đĩa (ưu tiên cao nhất)
//   spike_delta   = 0.05   # vượt baseline >5 điểm % tính spike
//   baseline      = ()     // optional override: nếu set thì dùng giá trị này thay vì mean(observations)
//
// Output: metric_id "disk_usage_checked" mang thêm nhãn
//   baseline (f64 hoặc ()), delta, alert ∈ ok|spike|saturated|no-baseline.

fn process(observations) {
    // Compute baseline from current window (mean of values)
    let sum = 0.0;
    let count = 0;
    for o in observations {
        sum += o.value;
        count += 1;
    }
    let base = if count > 0 { sum / count } else { () };

    // Allow override via param
    let baseline = base;
    try { baseline = param_baseline } catch { };

    // Ngưỡng đến từ params của node; khi không khai báo thì dùng mặc định.
    let saturated = 0.9;
    try { saturated = param_saturated } catch { 0.9 };
    let spike_delta = 0.05;
    try { spike_delta = param_spike_delta } catch { 0.05 };

    observations.map(|o| {
        let has_base = baseline != ();
        let alert = if !has_base {
            "no-baseline"
        } else if o.value > saturated {
            "saturated"
        } else if o.value > baseline + spike_delta {
            "spike"
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
                baseline: baseline,
                delta: if has_base { o.value - baseline } else { () },
                alert: alert,
            },
        }
    })
}