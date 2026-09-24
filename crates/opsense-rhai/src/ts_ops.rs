//! Time-series operator library for transform scripts.
//!
//! All operators take an array of observation maps (the same shape produced
//! by `process` input), and return either a scalar or a new array of
//! `{ts, value}` pairs. Empty / degenerate input returns `()` (unit) for
//! scalars and an empty array for series, so scripts can `if x == ()` safely.
//!
//! ```rhai
//! let pts = [{ts: 100, value: 1.0}, {ts: 160, value: 2.0}, ...];
//! let rate = ts_rate(pts);                  // value per second
//! let avg5 = ts_moving_avg(pts, 300);       // 5-min trailing mean
//! let per_min = ts_resample(pts, 60, "avg");// 1-min buckets, averaged
//! let p95 = ts_p95(pts);
//! let d = ts_delta(pts);                    // point-to-point change
//! ```

use rhai::{Array, Dynamic, Map};

/// Parse `Array` of observation maps into `Vec<(ts, value)>`.
fn parse_points(points: &Array) -> Result<Vec<(i64, f64)>, String> {
    let mut out = Vec::with_capacity(points.len());
    for item in points.iter() {
        if let Ok(obj) = item.as_map_ref() {
            let ts = obj.get("ts").and_then(|v| v.clone().try_cast::<i64>());
            let val = obj.get("value").and_then(|v| {
                v.clone().try_cast::<f64>()
                    .or_else(|| v.clone().try_cast::<i64>().map(|x| x as f64))
                    .or_else(|| v.clone().try_cast::<u64>().map(|x| x as f64))
            });
            if let (Some(ts), Some(val)) = (ts, val) {
                out.push((ts, val));
            }
        }
    }
    Ok(out)
}

/// Extract just the values from an array of observation maps.
fn extract_values(points: &Array) -> Vec<f64> {
    let result = parse_points(points);
    eprintln!("DEBUG extract_values parse_points result: {:?}", result);
    result
        .map(|pairs| pairs.into_iter().map(|(_, v)| v).collect())
        .unwrap_or_default()
}

fn point_map(ts: i64, value: f64) -> Dynamic {
    let mut map = Map::new();
    map.insert("ts".into(), Dynamic::from(ts));
    map.insert("value".into(), Dynamic::from(value));
    Dynamic::from(map)
}

/// Aggregate `values` by `agg` name: "avg" | "min" | "max" | "sum" | "count".
fn aggregate(values: &[f64], agg: &str) -> f64 {
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

fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    // Special case: median uses linear interpolation; others use nearest-rank (floor)
    let pos = q * (sorted.len() - 1) as f64;
    if (q - 0.5).abs() < f64::EPSILON {
        // Median: linear interpolation
        let idx = pos.floor() as usize;
        let frac = pos - idx as f64;
        if idx + 1 < sorted.len() {
            sorted[idx] + frac * (sorted[idx + 1] - sorted[idx])
        } else {
            sorted[idx]
        }
    } else {
        // Others: nearest-rank (floor)
        let idx = pos.floor() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

/// Register all time-series operator functions with the given engine.
pub fn register(engine: &mut rhai::Engine) {
    // ts_rate
    engine.register_fn("ts_rate", |points: Array| -> Dynamic {
        let Ok(p) = parse_points(&points) else { return Dynamic::UNIT; };
        if p.len() < 2 { return Dynamic::UNIT; }
        let dt = (p.last().unwrap().0 - p.first().unwrap().0) as f64;
        if dt <= 0.0 { return Dynamic::UNIT; }
        let rate = (p.last().unwrap().1 - p.first().unwrap().1) / dt;
        Dynamic::from(rate)
    });

    // ts_moving_avg - returns array (series operator)
    engine.register_fn("ts_moving_avg", |points: Array, window_secs: i64| -> Dynamic {
        let Ok(p) = parse_points(&points) else { return Dynamic::from(Array::new()); };
        if p.is_empty() || window_secs <= 0 { return Dynamic::from(Array::new()); }
        let ws = window_secs as i64;
        let mut out = Array::new();
        for i in 0..p.len() {
            let cutoff = p[i].0 - ws;
            let window_vals: Vec<f64> = p[..=i]
                .iter()
                .filter(|(ts, _)| *ts >= cutoff)
                .map(|(_, v)| *v)
                .collect();
            let avg = if window_vals.is_empty() { 0.0 } else { window_vals.iter().sum::<f64>() / window_vals.len() as f64 };
            out.push(point_map(p[i].0, avg));
        }
        Dynamic::from(out)
    });

    // ts_resample - returns array (series operator)
    engine.register_fn("ts_resample", |points: Array, bucket_secs: i64, agg: &str| -> Dynamic {
        let Ok(p) = parse_points(&points) else { return Dynamic::from(Array::new()); };
        if p.is_empty() || bucket_secs <= 0 { return Dynamic::from(Array::new()); }
        let bs = bucket_secs as i64;
        let t0 = p[0].0;
        let t1 = p.last().unwrap().0;
        let num_buckets = ((t1 - t0) / bs + 1) as usize;
        let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); num_buckets];
        for (ts, v) in p {
            let idx = ((ts - t0) / bs) as usize;
            buckets[idx].push(v);
        }
        let mut out = Array::new();
        for (i, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() { continue; }
            let val = aggregate(&bucket, agg);
            let ts = t0 + i as i64 * bs;
            out.push(point_map(ts, val));
        }
        Dynamic::from(out)
    });

    // ts_quantile - extract values from observation maps
    engine.register_fn("ts_quantile", |points: Array, q: f64| -> Dynamic {
        let mut vals = extract_values(&points);
        eprintln!("DEBUG ts_quantile extracted values: {:?}", vals);
        if vals.is_empty() { return Dynamic::UNIT; }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Dynamic::from(quantile_sorted(&vals, q.clamp(0.0, 1.0)))
    });

    // ts_p95
    engine.register_fn("ts_p95", |points: Array| -> Dynamic {
        let mut vals = extract_values(&points);
        eprintln!("DEBUG ts_p95 extracted values: {:?}", vals);
        if vals.is_empty() { return Dynamic::UNIT; }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Dynamic::from(quantile_sorted(&vals, 0.95))
    });

    // ts_p99
    engine.register_fn("ts_p99", |points: Array| -> Dynamic {
        let mut vals = extract_values(&points);
        eprintln!("DEBUG ts_p99 extracted values: {:?}", vals);
        if vals.is_empty() { return Dynamic::UNIT; }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Dynamic::from(quantile_sorted(&vals, 0.99))
    });

    // ts_delta - returns array (series operator)
    engine.register_fn("ts_delta", |points: Array| -> Dynamic {
        let Ok(p) = parse_points(&points) else { return Dynamic::from(Array::new()); };
        if p.len() < 2 { return Dynamic::from(Array::new()); }
        let mut out = Array::new();
        for i in 1..p.len() {
            let d = p[i].1 - p[i - 1].1;
            out.push(point_map(p[i].0, d));
        }
        Dynamic::from(out)
    });

    // ts_pct_change - returns array (series operator)
    engine.register_fn("ts_pct_change", |points: Array| -> Dynamic {
        let Ok(p) = parse_points(&points) else { return Dynamic::from(Array::new()); };
        if p.len() < 2 { return Dynamic::from(Array::new()); }
        let mut out = Array::new();
        for i in 1..p.len() {
            let prev = p[i - 1].1;
            let curr = p[i].1;
            let pct = if prev.abs() < f64::EPSILON { 0.0 } else { (curr - prev) / prev.abs() };
            out.push(point_map(p[i].0, pct));
        }
        Dynamic::from(out)
    });
}

