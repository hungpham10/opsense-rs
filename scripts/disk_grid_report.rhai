/// disk_grid_report.rhai
/// Fit AnalysisGrid trên % disk usage (biên vật lý 0..100) rồi báo cáo
/// từng đĩa (label `mountpoint`) đang nằm ở band nào của grid.
///
/// Output: observation mỗi đĩa — metric `disk_grid_band:<mountpoint>`, value = % usage,
/// labels {mountpoint, band, band_low, band_high}; kèm 1 observation
/// `disk_grid_summary` (value = số band tối ưu, labels {step}).
///
/// LƯU Ý HIỆU NĂNG: window đầu tiên của node (cursor=0) chứa cả lịch sử
/// trong persistence (có thể hàng chục nghìn điểm, vượt max_operations của
/// Rhai nếu group trực tiếp). Script chỉ lấy dữ liệu `recent_secs` gần nhất
/// (mặc định 3600) để group và fit grid — 2 pass rẻ kiểu numeric.
///
/// Cấu hình qua params của node:
///
///   [pipeline.components.params]
///   cells = 10
///   recent_secs = 3600

fn process(observations) {
    let cells = 10;
    try { cells = param_cells } catch { 10 };
    let recent_secs = 3600;
    try { recent_secs = param_recent_secs } catch { 3600 };

    if observations.len() == 0 {
        return [];
    }

    // Pass 1 (rẻ): mốc thời gian lớn nhất của cửa sổ
    let tmax = 0;
    for x in observations {
        let t = x["ts"];
        if t > tmax {
            tmax = t;
        }
    }

    // Pass 2 (rẻ): chỉ giữ dữ liệu gần nhất — group/map đắt tiền chạy trên
    // vài trăm điểm này thay vì cả lịch sử
    let recent = [];
    let cutoff = tmax - recent_secs;
    for x in observations {
        if x["ts"] > cutoff {
            recent.push(x);
        }
    }
    if recent.len() == 0 {
        return [];
    }

    // Group điểm mới nhất của mỗi đĩa theo mountpoint
    let last_ts = #{};
    let last_use = #{};
    let values = [];
    for x in recent {
        let mp = x["labels"]["mountpoint"];
        if mp == () {
            mp = "?";
        }
        let ts = x["ts"];
        let val = x["value"];
        if last_ts[mp] == () || ts > last_ts[mp] {
            last_ts[mp] = ts;
            last_use[mp] = val;
        }
        values.push(val);
    }

    // Grid phân cấp trên mảng value: tìm số band tối ưu
    let g = grid_fit_values(values, 0.0, 100.0, cells);
    // so sánh `g == ()` khiến engine coerce grid thành map → nổ max_map_size
    if type_of(g) == "()" {
        return [];
    }
    let ranges = grid_ranges(g);
    let bands = num_cells(g);
    let step = grid_step(g);

    // Mỗi đĩa một observation: value = % usage, labels = band của grid
    let out = [];
    for mp in last_use.keys() {
        let usage = last_use[mp];
        let cell = grid_cell(g, usage);
        let low = 0.0;
        let high = 0.0;
        for r in ranges {
            if r["index"] == cell {
                low = r["low"];
                high = r["high"];
            }
        }
        out.push(#{
            ts: last_ts[mp],
            metric_id: "disk_grid_band:" + mp,
            kind: "metric",
            signal: "raw",
            value: usage,
            labels: #{
                mountpoint: mp,
                band: cell.to_string(),
                band_low: low.to_string(),
                band_high: high.to_string(),
                bands: bands.to_string(),
            },
        });
    }

    // Observation tóm tắt grid
    out.push(#{
        ts: now_secs(),
        metric_id: "disk_grid_summary",
        kind: "metric",
        signal: "raw",
        value: bands,
        labels: #{ step: step.to_string() },
    });

    out
}
