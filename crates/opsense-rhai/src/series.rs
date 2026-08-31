//! Zero-copy series handle for large-batch scripts.
//!
//! Passing a million points as a Rhai array of observation maps is infeasible:
//! every map lands in the engine's global size accounting (`max_map_size`,
//! `max_array_size` are engine-wide caps, not per-value) and each per-point
//! loop burns the `max_operations` budget. [`Series`] sidesteps all of it —
//! the points stay inside one `Arc<Vec<(i64, f64)>>` in Rust, the script only
//! ever sees an opaque handle, and the heavy lifting (`grid_fit_series`,
//! future `ts_*` variants) happens natively where engine limits don't apply.
//!
//! Contract: a transform hands the batch to `process_series(series, meta)`
//! when the script defines it and the batch has at least
//! [`SERIES_THRESHOLD`] points; `meta` is `#{count, tmin, tmax}`. Smaller
//! batches (or scripts without `process_series`) keep the classic
//! `process(observations)` path, so existing scripts are unaffected.

use std::sync::Arc;

/// Shared (ts, value) points — one allocation, cloned by reference count.
#[derive(Clone)]
pub struct Series(pub(crate) Arc<Vec<(i64, f64)>>);

/// Batches at or above this many points go to `process_series` (when the
/// script defines it) instead of `process(observations)`.
pub const SERIES_THRESHOLD: usize = 4096;

impl Series {
    /// Wrap already-parsed points — no copy, the caller's `Vec` is moved in.
    #[must_use]
    pub fn from_points(points: Vec<(i64, f64)>) -> Self {
        Self(Arc::new(points))
    }
}

/// Register the `Series` handle type and its native operators.
pub fn register(eng: &mut rhai::Engine) {
    eng.register_type_with_name::<Series>("Series");

    eng.register_fn("len", |s: &mut Series| -> i64 { s.0.len() as i64 });
    eng.register_fn("is_empty", |s: &mut Series| -> bool { s.0.is_empty() });
    eng.register_fn("tmin", |s: &mut Series| -> i64 {
        s.0.first().map_or(0, |&(t, _)| t)
    });
    eng.register_fn("tmax", |s: &mut Series| -> i64 {
        s.0.last().map_or(0, |&(t, _)| t)
    });

    // Grid fit straight over the shared points — no per-point Rhai work.
    // Values only (the grid algorithm ignores timestamps), matching
    // `grid_fit_values` semantics.
    eng.register_fn(
        "grid_fit_series",
        |s: &mut Series, min: f64, max: f64, max_bit: i64| -> rhai::Dynamic {
            let values: Vec<f64> = s.0.iter().map(|&(_, v)| v).collect();
            let grid = opsense_libs::grid::AnalysisGrid::new(
                &values,
                min,
                max,
                max_bit.clamp(1, 31) as usize,
            );
            rhai::Dynamic::from(grid)
        },
    );

    // Escape hatch: materialize the (bounded!) points as `#{ts, value}` maps
    // so small series can flow back into the classic map-based helpers.
    // A million-point series materialized through here trips the engine's
    // array/map caps by design — chunk first if you need that.
    eng.register_fn("to_points", |s: &mut Series| -> rhai::Array {
        s.0.iter()
            .map(|&(ts, value)| {
                let mut map = rhai::Map::new();
                map.insert("ts".into(), rhai::Dynamic::from(ts));
                map.insert("value".into(), rhai::Dynamic::from(value));
                rhai::Dynamic::from(map)
            })
            .collect()
    });
}

/// Parse a batch (array of observation JSON objects) into raw points, skipping
/// items without numeric `ts`/`value`. Runs natively — no `Dynamic` involved.
#[must_use]
pub fn points_from_batch(input: &serde_json::Value) -> Vec<(i64, f64)> {
    let Some(items) = input.as_array() else {
        return Vec::new();
    };
    let mut points = Vec::with_capacity(items.len());
    for item in items {
        let (Some(ts), Some(value)) = (
            item.get("ts").and_then(serde_json::Value::as_i64),
            item.get("value").and_then(serde_json::Value::as_f64),
        ) else {
            continue;
        };
        points.push((ts, value));
    }
    points
}

/// `#{count, tmin, tmax}` metadata map for `process_series`.
#[must_use]
pub fn meta_for(series: &Series) -> rhai::Map {
    let mut meta = rhai::Map::new();
    meta.insert("count".into(), rhai::Dynamic::from(series.0.len() as i64));
    meta.insert(
        "tmin".into(),
        rhai::Dynamic::from(series.0.first().map_or(0, |&(t, _)| t)),
    );
    meta.insert(
        "tmax".into(),
        rhai::Dynamic::from(series.0.last().map_or(0, |&(t, _)| t)),
    );
    meta
}
