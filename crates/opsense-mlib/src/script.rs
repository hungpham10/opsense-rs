//! Rhai scripting support for opsense-mlib types.
//!
//! This module is only compiled when the `rhai` feature is enabled.
//! It defines the `RhaiBindings` trait that types implement to register
//! themselves with a Rhai engine, plus shared helper functions used by
//! the generated bindings.

use rhai::{Array, Dynamic, Engine};

/// Trait for types that can register themselves with a Rhai engine.
///
/// This is implemented by the `#[rhai]` proc-macro on structs that
/// want to expose their constructor and accessors to Rhai scripts.
pub trait RhaiBindings {
    /// Register this type and its functions with the given engine.
    fn register(engine: &mut Engine);
}

/// Parse a Rhai `Array` of observation maps into a vector of `(ts, value)` pairs.
///
/// Each map must contain `ts` (i64) and `value` (f64) keys.
/// Invalid entries are skipped.
pub fn parse_points(points: &Array) -> Result<Vec<(i64, f64)>, String> {
    let mut out = Vec::with_capacity(points.len());
    for item in points.iter() {
        if let Ok(obj) = item.as_map_ref() {
            let ts = obj.get("ts").and_then(|v| v.clone().try_cast::<i64>());
            let val = obj.get("value").and_then(|v| v.clone().try_cast::<f64>());
            if let (Some(ts), Some(val)) = (ts, val) {
                out.push((ts, val));
            }
        }
    }
    Ok(out)
}

/// Convert a `Dynamic` to `f64`, returning 0.0 on failure.
pub fn dyn_f64(v: &Dynamic) -> f64 {
    v.clone().try_cast::<f64>().unwrap_or(0.0)
}

/// Convert a `Dynamic` array to `Vec<f64>`, skipping non-float elements.
pub fn dyn_f64_array(v: &Dynamic) -> Vec<f64> {
    if let Ok(arr) = v.as_array_ref() {
        arr.iter().filter_map(|x| x.clone().try_cast::<f64>()).collect()
    } else {
        Vec::new()
    }
}

/// Clamp `max_bit` to a sensible range for `AnalysisGrid`.
pub fn clamp_max_bit(max_bit: i64) -> usize {
    max_bit.clamp(1, 16) as usize
}

/// Build a standard Rhai map for a single observation point.
pub fn point_map(ts: i64, value: f64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("ts".into(), Dynamic::from(ts));
    map.insert("value".into(), Dynamic::from(value));
    Dynamic::from(map)
}

/// Aggregate `values` by `agg` name: "avg" | "min" | "max" | "sum" | "count".
/// Unknown names fall back to mean.
pub fn aggregate(values: &[f64], agg: &str) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    match agg {
        "min" => values.iter().copied().fold(f64::INFINITY, f64::min),
        "max" => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        "sum" => values.iter().sum(),
        "count" => values.len() as f64,
        _ => values.iter().sum::<f64>() / values.len() as f64,
    }
}