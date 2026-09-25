//! Canh bề mặt API mà script thấy: **mọi** accessor khai trong
//! `#[rhai_class]` (`opsense-mlib/src/grid.rs`, `transition.rs`) phải gọi được
//! qua engine thật.
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
