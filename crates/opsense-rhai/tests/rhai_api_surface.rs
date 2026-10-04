//! Canh bề mặt API mà script thấy: **mọi** accessor khai trong
//! `#[rhai_class]` (`opsense-mlib/src/grid.rs`, `transition.rs`, `trend.rs` và
//! `opsense-rhai/src/capacity.rs`) phải gọi được qua engine thật.
//!
//! Vì sao cần: `register_all` gọi `RhaiBindings::register` (khai báo ở mlib) thay
//! vì `eng.register_fn` tay. Nếu ai đó xoá một accessor trong khai báo mà quên
//! nó vẫn được script dùng, chỉ lộ ra lúc chạy script đó — test này chết ngay
//! lúc `cargo test`.

use opsense_rhai::{ScriptSource, call_process};

/// Script gọi hết accessor, trả về số lần gọi.
fn surface_script() -> &'static str {
    r#"
    fn process(points) {
        let values = [10.0, 20.0, 30.0, 25.0, 40.0, 55.0, 70.0, 65.0, 80.0, 95.0];
        let series = #{ ts: 0, value: 10.0 };
        let calls = [];

        // ── AnalysisGrid (grid.rs) ────────────────────────────────────
        let g = grid_fit(points, 0.0, 100.0, 8);
        let cells = num_cells(g);
        let lines = num_lines(g);
        let step = grid_step(g);
        let cell = grid_cell(g, 55.0);
        let crossings = grid_crossings(g, values);
        let ranges = grid_ranges(g).len();
        let occ_buckets = grid_occupancy(g, [series], 60).len();
        let gv = grid_fit_values(values, 0.0, 100.0, 8);
        let step_v = grid_step(gv);
        calls += [cells, lines, step.to_int(), cell, crossings, ranges, occ_buckets, step_v.to_int()];

        // ── TransitionAnalysis (transition.rs) ────────────────────────
        let t = transition_analysis(g, points, 60);
        let buckets = num_buckets(t);
        let t_cells = num_cells(t);
        let interval = interval_secs(t);
        let inner_cells = num_cells(grid(t));
        let down = down_probability(t);
        let up = up_probability(t);
        let stay = stay_probability(t);
        let down_arr = down_probabilities(t).len();
        let up_arr = up_probabilities(t).len();
        let stay_arr = stay_probabilities(t).len();
        let trans = transitions(t).len();
        let icells = interval_cells(t, 0).len();
        let dwells = dwell_times(t, 0).len();
        let mean = mean_dwell(t, 0);
        let mean_i = if type_of(mean) == "()" { -1 } else { mean.to_int() };
        let max_d = max_dwell(t, 0);
        let has_from = if has_transitions_from(t, 0) { 1 } else { 0 };
        let total = total_from(t, 0);
        calls += [buckets, t_cells, interval, inner_cells, down.to_int(), up.to_int(), stay.to_int()];
        calls += [down_arr, up_arr, stay_arr, trans, icells, dwells, mean_i, max_d, has_from, total];

        calls
    }
    "#
}

#[tokio::test]
async fn every_declared_accessor_is_callable() {
    let input: Vec<serde_json::Value> = (0..200)
        .map(|i| serde_json::json!({
            "ts": i,
            "metric_id": "disk_usage",
            "kind": "metric",
            "signal": "utilization",
            "value": i as f64 / 2.0,
        }))
        .collect();

    let out = call_process(
        ScriptSource::Inline(surface_script().into()),
        serde_json::Value::Array(input),
    )
    .await
    .expect("mọi accessor khai trong #[rhai_class] phải gọi được");

    // Script trả 25 lời gọi (8 grid + 7 transition scalar + 10 transition array);
    // thiếu accessor nào thì engine báo lỗi lúc eval.
    assert_eq!(
        out.len(),
        25,
        "số lời gọi phải khớp script (xem surface_script): {out:?}"
    );
}

/// Script gọi hết accessor của `TrendAnalysis` + `CapacityForecast`, trả về
/// mảng kết quả. Tách khỏi `surface_script` để một nhóm accessor hỏng không
/// làm mất chẩn đoán của nhóm kia.
fn trend_capacity_script() -> &'static str {
    r#"
    fn process(points) {
        let calls = [];

        // ── TrendAnalysis (mlib/src/trend.rs) ──────────────────────────────
        let t = trend_fit(points, 8, 0.95, 2.0, 0.0);
        calls += [
            trend_current(t),
            trend_slope_per_sec(t),
            trend_slope_per_hour(t),
            trend_slope_per_day(t),
            trend_r2(t),
            trend_residual_std(t),
            trend_amplitude(t),
            trend_amplitude_rel(t),
            trend_offset(t),
            trend_samples(t),
            trend_span_secs(t),
            trend_origin_ts(t),
            trend_anchor_ts(t),
            trend_value_at(t, 100),
            trend_direction(t),
        ];
        let proj = trend_project(t, 1.0);
        // `()` = không chắc (ví dụ đang đi xuống); ép -1 để vẫn assert được.
        let to_target = trend_hours_to(t, 100.0);
        let to_edge = trend_hours_to_upper_envelope(t, 100.0);
        let to_i = if type_of(to_target) == "()" { -1.0 } else { to_target };
        let edge_i = if type_of(to_edge) == "()" { -1.0 } else { to_edge };
        calls += [proj["trend"], proj["low"], proj["high"], to_i, edge_i];

        // ── CapacityForecast (opsense-rhai/src/capacity.rs) ───────────────
        let f = capacity_forecast(points, 100.0, 0, 12, 8);
        calls += [
            capacity(f),
            capacity_current(f),
            capacity_headroom(f),
            capacity_headroom_rel(f),
            capacity_direction(f),
            capacity_oscillating(f),
            capacity_amplitude(f),
            capacity_amplitude_rel(f),
            capacity_drift(f),
            capacity_samples(f),
            capacity_span_secs(f),
            capacity_interval_secs(f),
            capacity_envelope_cells(f),
            capacity_current_cell(f),
            capacity_top_cell(f),
        ];
        // Ba tầng dưới phải trao được cho nhau qua script, không chỉ trong Rust.
        calls += [
            num_cells(capacity_grid(f)),
            num_buckets(capacity_transition(f)),
            trend_direction(capacity_trend(f)),
        ];
        let cproj = capacity_project(f, 1.0);
        let to_full = capacity_hours_to_full(f);
        let to_trend = capacity_hours_to_trend_full(f);
        let to_top = capacity_hours_to_top_cell(f);
        let full_i = if type_of(to_full) == "()" { -1.0 } else { to_full };
        let trend_i = if type_of(to_trend) == "()" { -1.0 } else { to_trend };
        let top_i = if type_of(to_top) == "()" { -1.0 } else { to_top };
        calls += [cproj["trend"], cproj["low"], cproj["high"], full_i, trend_i, top_i];

        calls
    }
    "#
}

#[tokio::test]
async fn trend_and_capacity_accessors_are_callable() {
    // Dốc thẳng 0 → 99.5 trên capacity 100: đủ dữ liệu cho cả trend lẫn
    // capacity, và hai chuỗi "giờ tới mốc" đều xác định (không rơi vào `()`).
    let input: Vec<serde_json::Value> = (0..200)
        .map(|i| serde_json::json!({
            "ts": i,
            "metric_id": "disk_usage",
            "kind": "metric",
            "signal": "utilization",
            "value": i as f64 / 2.0,
        }))
        .collect();

    let out = call_process(
        ScriptSource::Inline(trend_capacity_script().into()),
        serde_json::Value::Array(input),
    )
    .await
    .expect("mọi accessor của TrendAnalysis/CapacityForecast phải gọi được");

    assert_eq!(
        out.len(),
        44,
        "số lời gọi phải khớp trend_capacity_script (20 trend + 24 capacity): {out:?}"
    );
}
