// prometheus.rhai -- Prometheus demo test processor.
//
// Full end-to-end: clock -> parquet -> S3. Real data from Prometheus demo.
//
// Mỗi metric được tổng hợp thành một summary (mean/min/max/variance/count)
// kèm thêm phân tích AnalysisGrid (`grid_cell`/`crossings`/`grid_step`) và
// TransitionAnalysis (`up/down/stay_probability`, `trans_buckets`) qua các
// native functions opsense-rhai expose — xem crates/opsense-rhai/src/tools.rs.
//
// Debug: gọi `print(...)` (tracing INFO) hoặc `debug(...)` (tracing DEBUG),
// bật với `RUST_LOG=opsense_rhai=info|debug`.

fn process(observations) {
    if observations.len() == 0 { return []; }

    // Stamp summaries tại max ts của input batch (không phải now_secs) để
    // block key ổn định giữa các tick — update cùng key → persist_point fire
    // → parquet local + mirror S3 chắc chắn xuất hiện sau flush.
    let ts = 0;
    for obs in observations {
        let t = obs["ts"];
        if t > ts { ts = t; }
    }

    let metric_ids = [];
    for obs in observations {
        let id = obs["metric_id"];
        let found = false;
        for existing in metric_ids {
            if existing == id { found = true; break; }
        }
        if !found { metric_ids.push(id); }
    }

    print("process: " + observations.len() + " observations, " + metric_ids.len() + " metric(s), batch ts=" + ts);

    let out = [];
    for id in metric_ids {
        let pts = [];
        for obs in observations {
            if obs["metric_id"] == id { pts.push(obs); }
        }
        if pts.len() == 0 { continue; }

        // AnalysisGrid.occupancy giả định data ascending theo ts (t_start =
        // data[0]) — nếu giữ thứ tự raw (mock trả ts giảm dần) thì bucket
        // bị âm → panic/wrap. Sort trước khi đưa vào grid/transition.
        let points = sort_by_ts(pts);
        let vals = [];
        for p in points { vals.push(p["value"]); }

        let v_mean = mean(vals);
        let v_var = variance(vals);
        let v_min = array_min(vals);
        let v_max = array_max(vals);
        let v_count = vals.len();

        let labels = #{
            count: v_count.to_string(),
            min: v_min.to_string(),
            max: v_max.to_string(),
            variance: v_var.to_string(),
            mean: v_mean.to_string(),
            test_case: "prometheus",
            storage_backend: "parquet+s3",
            data_source: "prometheus_demo",
            s3_bucket: "opsense-lake",
            s3_prefix: "test-case-prom"
        };

        // Grid + Transition analysis (chỉ khi có biến thiên / đủ điểm).
        let extra = analysis_labels(points, vals);
        for key in extra.keys() {
            labels[key] = extra[key];
        }

        out.push(#{
            ts: ts,
            metric_id: id,
            kind: "metric",
            signal: "summary",
            value: v_mean,
            labels: labels
        });
    }

    out
}

/// Grid + Transition analysis qua AnalysisGrid / TransitionAnalysis (native).
/// Trả về map label string; rỗng khi dữ liệu không đủ (span=0, < 2 điểm).
fn analysis_labels(points, vals) {
    let out = #{};
    let n = vals.len();
    if n < 2 { return out; }

    let v_min = array_min(vals);
    let v_max = array_max(vals);
    let span = v_max - v_min;
    if span <= 0.0 {
        print("analysis[" + points[0]["metric_id"] + "]: constant series (span=0), skip grid");
        return out;
    }

    let g = grid_fit(points, v_min, v_max, 8);
    out.grid_cells = num_cells(g).to_string();
    out.grid_lines = num_lines(g).to_string();
    out.grid_step = grid_step(g).to_string();
    out.grid_cell = grid_cell(g, vals[n - 1]).to_string();
    out.crossings = grid_crossings(g, vals).to_string();

    // interval ≈ chu kỳ lấy mẫu trung bình → mỗi điểm một bucket.
    let t0 = points[0]["ts"];
    let t1 = points[n - 1]["ts"];
    let interval = (t1 - t0) / (n - 1);
    if interval <= 0 {
        print("analysis[" + points[0]["metric_id"] + "]: no ts spread, skip transitions");
        return out;
    }

    let ta = transition_analysis(g, points, interval);
    if type_of(ta) == "()" {
        print("analysis[" + points[0]["metric_id"] + "]: too few points for transitions");
        return out;
    }
    out.trans_buckets = num_buckets(ta).to_string();
    out.up_prob = up_probability(ta).to_string();
    out.down_prob = down_probability(ta).to_string();
    out.stay_prob = stay_probability(ta).to_string();

    debug("analysis[" + points[0]["metric_id"] + "]: cells=" + out.grid_cells + " step=" + out.grid_step + " up=" + out.up_prob + " down=" + out.down_prob + " stay=" + out.stay_prob);
    out
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

fn mean(vals) { if vals.len() == 0 { return 0.0; } let s = 0.0; for x in vals { s = s + x.to_float(); } s / vals.len().to_float() }
fn variance(vals) { if vals.len() < 2 { return 0.0; } let m = mean(vals); let s = 0.0; for x in vals { s = s + (x.to_float() - m) * (x.to_float() - m); } s / (vals.len() - 1).to_float() }
fn array_min(vals) { if vals.len() == 0 { return 0.0; } let m = vals[0].to_float(); for x in vals { if x.to_float() < m { m = x.to_float(); } } m }
fn array_max(vals) { if vals.len() == 0 { return 0.0; } let m = vals[0].to_float(); for x in vals { if x.to_float() > m { m = x.to_float(); } } m }