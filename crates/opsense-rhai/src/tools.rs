//! Native tool library registered onto every sandboxed Rhai engine.
//!
//! One entrypoint — [`register_all`] — installs every script-facing function,
//! grouped by concern:
//!
//! - [`time_fns`] – `now_secs()`
//! - [`ts_ops`] – time-series operators (`ts_rate`, `ts_moving_avg`,
//!   `ts_resample`, `ts_quantile`, `ts_p95`, `ts_p99`, `ts_delta`,
//!   `ts_pct_change`)
//! - [`attributes`] – per-call config lookups: `attr(name)` / `attrs()`
//! - AnalysisGrid & TransitionAnalysis bindings (manual registration for reliability)

use opsense_mlib::grid::AnalysisGrid;
use opsense_mlib::transition::TransitionAnalysis;

/// Install every script-facing native function.
pub fn register_all(eng: &mut rhai::Engine, attributes: std::collections::BTreeMap<String, String>) {
    // Register custom types first (required for Rhai to extract them from Dynamic)
    eng.register_type::<AnalysisGrid>();
    eng.register_type::<TransitionAnalysis>();

    // AnalysisGrid: constructor + accessors (receiver = &mut Self)
    eng.register_fn("grid_fit", AnalysisGrid::grid_fit);
    eng.register_fn("grid_fit_values", |values: rhai::Array, min: f64, max: f64, max_bit: i64| -> rhai::Dynamic {
        let pts: Vec<f64> = values.iter().filter_map(|v| v.clone().try_cast::<f64>()).collect();
        if pts.is_empty() { return rhai::Dynamic::UNIT; }
        let grid = AnalysisGrid::new(&pts, min, max, max_bit as usize);
        rhai::Dynamic::from(grid)
    });
    eng.register_fn("num_cells", |g: &mut AnalysisGrid| -> i64 { g.num_cells() as i64 });
    eng.register_fn("num_lines", |g: &mut AnalysisGrid| -> i64 { g.num_lines() as i64 });
    eng.register_fn("grid_step", |g: &mut AnalysisGrid| -> f64 { g.step });
    eng.register_fn("grid_cell", |g: &mut AnalysisGrid, y: f64| -> i64 { g.cell(y) as i64 });
    eng.register_fn("grid_crossings", |g: &mut AnalysisGrid, values: rhai::Array| -> i64 {
        let pts: Vec<f64> = values.iter().filter_map(|v| v.clone().try_cast::<f64>()).collect();
        g.crossings(&pts) as i64
    });
    eng.register_fn("grid_occupancy", |g: &mut AnalysisGrid, data: rhai::Array, interval_secs: i64| -> rhai::Dynamic {
        let pts = opsense_mlib::script::parse_points(&data).unwrap_or_default();
        let occ = g.occupancy(&pts, interval_secs);
        let mut arr = rhai::Array::new();
        for bucket in occ {
            let mut sub = rhai::Array::new();
            for cnt in bucket {
                sub.push(rhai::Dynamic::from(cnt as i64));
            }
            arr.push(rhai::Dynamic::from(sub));
        }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("grid_ranges", |g: &mut AnalysisGrid| -> rhai::Dynamic {
        let ranges = g.cell_ranges();
        let mut arr = rhai::Array::new();
        for (idx, lo, hi) in ranges {
            let mut m = rhai::Map::new();
            m.insert("index".into(), rhai::Dynamic::from(idx as i64));
            m.insert("low".into(), rhai::Dynamic::from(lo));
            m.insert("high".into(), rhai::Dynamic::from(hi));
            arr.push(rhai::Dynamic::from(m));
        }
        rhai::Dynamic::from(arr)
    });

    // TransitionAnalysis: constructor + accessors (receiver = &mut Self)
    eng.register_fn("transition_analysis", TransitionAnalysis::transition_analysis);
    eng.register_fn("num_buckets", |t: &mut TransitionAnalysis| -> i64 { t.num_buckets() as i64 });
    eng.register_fn("num_cells", |t: &mut TransitionAnalysis| -> i64 { t.num_cells() as i64 });
    eng.register_fn("interval_secs", |t: &mut TransitionAnalysis| -> i64 { t.interval_secs() });
    eng.register_fn("grid", |t: &mut TransitionAnalysis| -> AnalysisGrid { t.grid().clone() });
    eng.register_fn("down_probability", |t: &mut TransitionAnalysis| -> f64 { t.down_probability() });
    eng.register_fn("up_probability", |t: &mut TransitionAnalysis| -> f64 { t.up_probability() });
    eng.register_fn("stay_probability", |t: &mut TransitionAnalysis| -> f64 { t.stay_probability() });
    eng.register_fn("down_probabilities", |t: &mut TransitionAnalysis| -> rhai::Dynamic {
        let v = t.down_probabilities();
        let mut arr = rhai::Array::new();
        for x in v { arr.push(rhai::Dynamic::from(x)); }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("up_probabilities", |t: &mut TransitionAnalysis| -> rhai::Dynamic {
        let v = t.up_probabilities();
        let mut arr = rhai::Array::new();
        for x in v { arr.push(rhai::Dynamic::from(x)); }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("stay_probabilities", |t: &mut TransitionAnalysis| -> rhai::Dynamic {
        let v = t.stay_probabilities();
        let mut arr = rhai::Array::new();
        for x in v { arr.push(rhai::Dynamic::from(x)); }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("transitions", |t: &mut TransitionAnalysis| -> rhai::Dynamic {
        let trans = t.transitions();
        let mut arr = rhai::Array::new();
        for from_list in trans {
            let mut sub = rhai::Array::new();
            for (to, cnt) in from_list {
                let mut m = rhai::Map::new();
                m.insert("from".into(), rhai::Dynamic::from(*to as i64));
                m.insert("to".into(), rhai::Dynamic::from(*cnt as i64));
                sub.push(rhai::Dynamic::from(m));
            }
            arr.push(rhai::Dynamic::from(sub));
        }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("interval_cells", |t: &mut TransitionAnalysis, i: i64| -> rhai::Dynamic {
        let idx = i as usize;
        let cells = t.interval_cells(idx);
        let mut arr = rhai::Array::new();
        for (cell, cnt) in cells {
            let mut m = rhai::Map::new();
            m.insert("cell".into(), rhai::Dynamic::from(*cell as i64));
            m.insert("count".into(), rhai::Dynamic::from(*cnt as i64));
            arr.push(rhai::Dynamic::from(m));
        }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("dwell_times", |t: &mut TransitionAnalysis, cell: i64| -> rhai::Dynamic {
        let idx = cell as usize;
        let times = t.dwell_times(idx);
        let mut arr = rhai::Array::new();
        for d in times { arr.push(rhai::Dynamic::from(*d as i64)); }
        rhai::Dynamic::from(arr)
    });
    eng.register_fn("mean_dwell", |t: &mut TransitionAnalysis, cell: i64| -> rhai::Dynamic {
        let idx = cell as usize;
        match t.mean_dwell(idx) {
            Some(v) => rhai::Dynamic::from(v),
            None => rhai::Dynamic::UNIT,
        }
    });
    eng.register_fn("max_dwell", |t: &mut TransitionAnalysis, cell: i64| -> i64 {
        let idx = cell as usize;
        t.max_dwell(idx) as i64
    });
    eng.register_fn("has_transitions_from", |t: &mut TransitionAnalysis, cell: i64| -> bool {
        let idx = cell as usize;
        t.has_transitions_from(idx)
    });
    eng.register_fn("total_from", |t: &mut TransitionAnalysis, cell: i64| -> i64 {
        let idx = cell as usize;
        t.total_from(idx) as i64
    });

    // Free functions via macro-generated register
    crate::time_fns::register(eng);
    crate::ts_ops::register(eng);
    crate::attributes::register(eng, attributes);
}