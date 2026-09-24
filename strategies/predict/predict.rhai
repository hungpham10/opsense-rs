// predict.rhai — prediction + live verification, branch theo trigger.
//
// Node nhận message từ HAI input edges và quyết định theo `trigger()` (= giá
// trị `payload.src` mà producer tự stamp bằng `signal::tagged`, xem
// crates/opsense-components/src/lib.rs; nếu có stage trung gian thêm
// `payload.trigger` thì ưu tiên field đó):
//
//   trigger() == live_source ("live-feed") → KIỂM TRA dữ liệu vừa đến từ
//     http source: live sample (metric `*_live_*` trong batch) so với
//     prediction gần nhất (`labels.check = "prediction"`) lưu trong station
//     của chính node → `labels.check = "result"` (value 1.0 = khớp trong
//     tolerance / 0.0 = lệch).
//   trigger() khác (clock ping, "") → TÍNH LẠI bảng dự đoán: query source
//     station qua `station_query(window_source, now-600, now)` → AnalysisGrid
//     + TransitionAnalysis trên history window → prediction per metric.
//
// Grid CHỈ fit history (ts <= now-60 và metric không chứa "_live_"): live
// samples (ts gần now) không nhiễu forecast dù nằm trong cùng station.
//
// Node params (all optional, sane defaults):
//   window_source = "live-feed", live_source = "live-feed",
//   own_station = "predict", tolerance = 1.0

fn process(observations) {
    let win_src = "live-feed";
    if is_def_var("param_window_source") && type_of(param_window_source) != "()" {
        win_src = param_window_source;
    }
    let live_src = "live-feed";
    if is_def_var("param_live_source") && type_of(param_live_source) != "()" {
        live_src = param_live_source;
    }
    let own = "predict";
    if is_def_var("param_own_station") && type_of(param_own_station) != "()" {
        own = param_own_station;
    }
    let tol = 1.0;
    if is_def_var("param_tolerance") && type_of(param_tolerance) != "()" {
        tol = param_tolerance.to_float();
    }

    if trigger() == live_src {
        // Dữ liệu từ http source vừa đến → kiểm tra lệch khỏi dự đoán.
        return check_live(observations, own, tol);
    }

    // Ngược lại (clock ping) → tính lại bảng dự đoán.
    let now = now_secs();
    let back = 600;

    let win = station_query(win_src, now - back, now);
    if type_of(win) == "()" { return []; }
    if win.len() == 0 { return []; }

    let preds = predict_window(win, now);
    print("predict: " + preds.len() + " prediction(s)");
    preds
}

/// Window → one prediction observation per metric.
///
/// Steady window (span <= 0): forecast = last value. Growing/shrinking
/// window: forecast = last value + (up_prob - down_prob) * grid_step, i.e. the
/// grid cell trend one step ahead.
fn predict_window(win, now) {
    let hist = history_window(win, now);
    let out = [];
    for id in metric_ids(hist) {
        let pts = metric_points(hist, id);
        if pts.len() == 0 { continue; }

        let points = sort_by_ts(pts);
        let vals = [];
        for p in points { vals.push(p["value"]); }

        let v_min = array_min(vals);
        let v_max = array_max(vals);
        let v_last = vals[vals.len() - 1].to_float();
        let span = v_max - v_min;

        let labels = #{
            check: "prediction",
            method: "",
            cells: "0",
            grid_step: "0",
            grid_cell: "0",
            up_prob: "0",
            down_prob: "0",
            stay_prob: "0"
        };

        let predicted = v_last;
        if span > 0.0 && vals.len() >= 2 {
            // Grid + transitions drive the next-step forecast.
            let g = grid_fit(points, v_min, v_max, 8);
            labels.grid_step = grid_step(g).to_string();
            labels.grid_cell = grid_cell(g, v_last).to_string();
            labels.cells = num_cells(g).to_string();

            let interval = interval_secs_of(points);
            if interval > 0 {
                let ta = transition_analysis(g, points, interval);
                if type_of(ta) != "()" {
                    labels.up_prob = up_probability(ta).to_string();
                    labels.down_prob = down_probability(ta).to_string();
                    labels.stay_prob = stay_probability(ta).to_string();
                    predicted = v_last + (up_probability(ta) - down_probability(ta)) * grid_step(g);
                    labels.method = "grid-transition";
                }
            }
            if labels.method == "" {
                labels.method = "grid-window";
            }
        } else {
            labels.method = "steady";
        }

        out.push(#{
            ts: points[points.len() - 1]["ts"],
            metric_id: id,
            kind: "metric",
            signal: "summary",
            value: predicted,
            labels: labels
        });
    }
    out
}

/// Lọc về history window: ts ≤ now-60 và không phải metric live (mock dùng
/// `*_live_*` làm realtime sample — không được vào grid).
fn history_window(win, now) {
    let out = [];
    for o in win {
        let id = o["metric_id"];
        if o["ts"] <= now - 60 && !id.contains("_live_") {
            out.push(o);
        }
    }
    out
}

/// Live sample (metric `*_live_*` trong batch source) vs prediction gần nhất
/// (`labels.check == "prediction"`) của metric gốc → 1.0 / 0.0.
///
/// Live id "cpu_live_ramp" → base "cpu_ramp" (bỏ đuôi "_live"); sample cũ hơn
/// prediction bị skip (prediction phải đứng trước actual nó giải thích).
fn check_live(live, own, tol) {
    let out = [];
    if type_of(live) == "()" { return out; }
    if live.len() == 0 { return out; }

    let now = now_secs();
    let back = 600;
    let hist = station_query(own, now - back, now);
    if type_of(hist) == "()" { return out; }

    for id in metric_ids(live) {
        // Chỉ live sample (metric chứa "_live_"); history trùng payload bị bỏ.
        if !id.contains("_live_") { continue; }
        let base = live_to_base(id);
        if base == id { continue; }
        let pts = metric_points(live, id);
        if pts.len() == 0 { continue; }

        let latest = sort_by_ts(pts);
        let actual_obs = latest[latest.len() - 1];
        let actual = actual_obs["value"].to_float();

        let prev = latest_prediction(hist, base);
        if type_of(prev) == "()" { continue; }
        if actual_obs["ts"] < prev["ts"] { continue; }

        let expected = prev["value"].to_float();
        let delta = (actual - expected).abs();
        let ok = delta <= tol;

        out.push(#{
            ts: actual_obs["ts"],
            metric_id: id,
            kind: "metric",
            signal: "summary",
            value: if ok { 1.0 } else { 0.0 },
            labels: #{
                check: "result",
                expected: expected.to_string(),
                actual: actual.to_string(),
                delta: delta.to_string(),
                tolerance: tol.to_string()
            }
        });
    }
    out
}

/// "cpu_live_ramp" → "cpu_ramp" (bỏ phần "_live"). `split` không mutate (trái
/// với `replace` — Rhai replace mutates in-place và trả unit). Trả nguyên id
/// nếu không khớp pattern `prefix_live_suffix`.
fn live_to_base(id) {
    let parts = id.split("_live_");
    if parts.len() == 2 { return parts[0] + "_" + parts[1]; }
    id
}

/// Latest stored prediction obs cho metric gốc `base` (max ts), hoặc () nếu
/// chưa có.
fn latest_prediction(hist, base) {
    let best = ();
    let best_ts = -1;
    for h in hist {
        if h["metric_id"] != base { continue; }
        let labels = h["labels"];
        if labels["check"] == "prediction" && h["ts"] > best_ts {
            best_ts = h["ts"];
            best = h;
        }
    }
    best
}

/// Những metric có mặt trong danh sách observation (giữ thứ tự xuất hiện).
fn metric_ids(obs_list) {
    let ids = [];
    for o in obs_list {
        let id = o["metric_id"];
        let found = false;
        for e in ids {
            if e == id { found = true; break; }
        }
        if !found { ids.push(id); }
    }
    ids
}

/// Observations thuộc một metric.
fn metric_points(obs_list, id) {
    let pts = [];
    for o in obs_list {
        if o["metric_id"] == id { pts.push(o); }
    }
    pts
}

/// Khoảng lấy mẫu trung bình (giây) giữa các điểm, 0 nếu không đủ 2 điểm.
fn interval_secs_of(points) {
    let n = points.len();
    if n < 2 { return 0; }
    let t0 = points[0]["ts"];
    let t1 = points[n - 1]["ts"];
    (t1 - t0) / (n - 1)
}

/// Sắp xếp observation maps theo `ts` tăng dần (insertion — dữ liệu nhỏ).
fn sort_by_ts(points) {
    let out = [];
    for p in points {
        let t = p["ts"];
        let i = 0;
        while i < out.len() && out[i]["ts"] <= t {
            i = i + 1;
        }
        if i < out.len() { out.insert(i, p); } else { out.push(p); }
    }
    out
}

fn array_min(vals) { if vals.len() == 0 { return 0.0; } let m = vals[0].to_float(); for x in vals { if x.to_float() < m { m = x.to_float(); } } m }
fn array_max(vals) { if vals.len() == 0 { return 0.0; } let m = vals[0].to_float(); for x in vals { if x.to_float() > m { m = x.to_float(); } } m }