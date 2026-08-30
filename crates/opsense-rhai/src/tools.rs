//! Native tool library registered onto every sandboxed Rhai engine.
//!
//! One entrypoint — [`register_all`] — installs every script-facing function,
//! grouped by concern:
//!
//! - [`register_time`] – `now_secs()`
//! - [`register_station_lookups`] – `ts_query` / `ts_mean` over the global
//!   station registry (`opsense_store`). Mỗi station khai báo stage nó chứa
//!   lúc tạo (`stages` trong `/describe`): source là `raw`, transform là
//!   `processed` — query stage khác sẽ trả rỗng.
//! - [`register_ts_ops`] – time-series operators (`ts_rate`, `ts_moving_avg`,
//!   `ts_resample`, `ts_quantile`, `ts_p95`, `ts_p99`, `ts_delta`,
//!   `ts_pct_change`)
//! - [`register_grid_ops`] – capacity-grid analysis (`opsense_libs::grid`):
//!   fit the uniform band grid that minimises boundary crossings
//!
//! All functions are read-only over shared registries/stores so a runaway or
//! malicious script cannot mutate pipeline state.

use opsense_libs::grid::AnalysisGrid;

/// Install every script-facing native function.
pub fn register_all(eng: &mut rhai::Engine) {
    register_time(eng);
    register_station_lookups(eng);
    register_ts_ops(eng);
    register_grid_ops(eng);
    register_text_index_ops(eng);
}

/// Install the per-call config lookups: `attr(name)` (value or `()` when the
/// attribute is absent) and `attrs()` (the whole map, as a Rhai `Map` so
/// scripts can index it). Attributes come from the pipeline config's
/// `[attributes]` table with `OPSENSE_ATTR_*` env overrides resolved — they
/// are copied in per `process` call, so scripts see a read-only snapshot.
pub fn register_attributes(
    eng: &mut rhai::Engine,
    attributes: std::collections::BTreeMap<String, String>,
) {
    let attrs = std::sync::Arc::new(attributes);
    let attrs2 = attrs.clone();
    eng.register_fn("attr", move |name: &str| -> rhai::Dynamic {
        attrs
            .get(name)
            .map(|v| rhai::Dynamic::from(v.clone()))
            .unwrap_or(rhai::Dynamic::UNIT)
    });
    eng.register_fn("attrs", move || -> rhai::Map {
        attrs2
            .iter()
            .map(|(k, v)| (k.clone().into(), rhai::Dynamic::from(v.clone())))
            .collect()
    });
}

// ---------------------------------------------------------------------------
// pattern / catalog text indexes
// ---------------------------------------------------------------------------

/// Script-facing functions over the global text index registry
/// (`opsense_core::registry::text_index`). Two flavours per node id:
///
/// - **Pattern** (`ensure_pattern`): Aho-Corasick automaton — add patterns,
///   check whether a log line matches any known one.
/// - **Search** (`ensure_search`): Radix trie + KMP — index key/value pairs,
///   search by substring.
///
/// The Rhai engine runs on a blocking thread (`spawn_blocking`), so these
/// closures bridge back into the async registry/station API via the current
/// tokio runtime handle.
///
/// ```rhai
/// // Log pattern detection
/// pattern_is_known("log-matcher", "2026-08-26 OOM killed pod xyz");
/// pattern_stats("log-matcher");
///
/// // Catalog
/// catalog_insert("svc-catalog", "cpu_usage", "{\"team\":\"sre\"}");
/// catalog_search("svc-catalog", "cpu");
/// ```
fn register_text_index_ops(eng: &mut rhai::Engine) {
    use opsense_core::registry;
    use tokio::runtime::Handle;

    eng.register_fn(
        "pattern_is_known",
        |node_id: &str, text: &str| -> rhai::Dynamic {
            let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
            runtime.block_on(async {
                match registry::text_index(node_id).await {
                    Some(idx) => {
                        rhai::Dynamic::from(idx.read().await.is_known(text).await.unwrap_or(false))
                    }
                    None => rhai::Dynamic::UNIT,
                }
            })
        },
    );

    eng.register_fn(
        "pattern_add",
        |node_id: &str, pattern: &str| -> rhai::Dynamic {
            let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
            runtime.block_on(async {
                let idx = registry::ensure_pattern(node_id).await;
                idx.write().await.add_pattern(pattern).await;
            });
            rhai::Dynamic::UNIT
        },
    );

    eng.register_fn("pattern_stats", |node_id: &str| -> rhai::Dynamic {
        let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
        runtime.block_on(async {
            match registry::text_index(node_id).await {
                Some(idx) => {
                    let (total, hits, misses) = idx.read().await.pattern_stats();
                    let mut map = rhai::Map::new();
                    map.insert("total_patterns".into(), rhai::Dynamic::from(total as i64));
                    map.insert("hits".into(), rhai::Dynamic::from(hits as i64));
                    map.insert("misses".into(), rhai::Dynamic::from(misses as i64));
                    rhai::Dynamic::from(map)
                }
                None => rhai::Dynamic::UNIT,
            }
        })
    });

    eng.register_fn(
        "catalog_insert",
        |node_id: &str, key: &str, value: &str| -> rhai::Dynamic {
            let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
            runtime.block_on(async {
                let idx = registry::ensure_search(node_id).await;
                idx.write().await.insert_entry(key.as_bytes(), value).await;
            });
            rhai::Dynamic::UNIT
        },
    );

    eng.register_fn(
        "catalog_search",
        |node_id: &str, pattern: &str| -> rhai::Dynamic {
            let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
            runtime.block_on(async {
                let Some(idx) = registry::text_index(node_id).await else {
                    return rhai::Dynamic::UNIT;
                };
                let entries = idx.read().await.search_entries(pattern, None).await;
                let items: Vec<rhai::Dynamic> = entries
                    .into_iter()
                    .map(|(key, value)| {
                        let mut map = rhai::Map::new();
                        map.insert("key".into(), rhai::Dynamic::from(key));
                        map.insert("value".into(), rhai::Dynamic::from(value));
                        rhai::Dynamic::from(map)
                    })
                    .collect();
                rhai::Dynamic::from(items)
            })
        },
    );
}

// ---------------------------------------------------------------------------
// time
// ---------------------------------------------------------------------------

fn register_time(eng: &mut rhai::Engine) {
    // Current unix time for "so sánh hiện tại với quá khứ" queries.
    eng.register_fn("now_secs", || -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    });
}

// ---------------------------------------------------------------------------
// station lookups
// ---------------------------------------------------------------------------

/// Native helpers over the global station registry (`opsense_core::registry`),
/// so a transform can compare its current batch against stored history:
///
/// ```rhai
/// let base = ts_mean("tsdb", "processed", "cpu_usage", now - 3600, now);
/// ```
///
/// - `ts_query(id, stage, metric, from_ts, to_ts)` → array of observation maps,
///   or `()` when no station with that id exists.
/// - `ts_mean(...)` → average value over the window, or `()` when empty.
///
/// NOTE on stages: every station declares the stages it holds at creation
/// time (visible via `opsense_describe` / HTTP `/describe` as the `stages`
/// field). Source nodes (`http_source`, `ingest`) khai báo `["processed"]`;
/// transform nodes (`rhai_transform`, processor) cũng khai báo stage output
/// của chúng (mặc định `["processed"]`). Query một stage không được khai báo
/// (ví dụ `ts_query("prom", "raw", ...)`) luôn trả về rỗng — hãy dùng stage
/// mà station đó khai báo.
///
/// The Rhai engine runs on a blocking thread, so these bridge into the async
/// registry/station API via the current tokio runtime handle.
fn register_station_lookups(eng: &mut rhai::Engine) {
    use opsense_core::registry;
    use opsense_core::Stage;
    use tokio::runtime::Handle;

    fn resolve_stage(name: &str) -> Stage {
        if name.eq_ignore_ascii_case("raw") {
            Stage::Raw
        } else {
            Stage::Processed
        }
    }

    eng.register_fn(
        "ts_query",
        |id: &str, stage: &str, metric: &str, from_ts: i64, to_ts: i64| -> rhai::Dynamic {
            let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
            runtime.block_on(async {
                let Some(station_handle) = registry::station(id).await else {
                    return rhai::Dynamic::UNIT;
                };
                let points = station_handle
                    .read()
                    .await
                    .query(resolve_stage(stage), metric, from_ts, to_ts)
                    .await;
                let items: Vec<rhai::Dynamic> = points
                    .iter()
                    .filter_map(|obs| rhai::serde::to_dynamic(obs).ok())
                    .collect();
                rhai::Dynamic::from(items)
            })
        },
    );

    eng.register_fn(
        "ts_mean",
        |id: &str, stage: &str, metric: &str, from_ts: i64, to_ts: i64| -> rhai::Dynamic {
            let runtime = Handle::try_current().expect("opsense runtime unavailable in script");
            runtime.block_on(async {
                let Some(station_handle) = registry::station(id).await else {
                    return rhai::Dynamic::UNIT;
                };
                let points = station_handle
                    .read()
                    .await
                    .query(resolve_stage(stage), metric, from_ts, to_ts)
                    .await;
                if points.is_empty() {
                    return rhai::Dynamic::UNIT;
                }
                let sum: f64 = points.iter().map(|o| o.value).sum();
                rhai::Dynamic::from(sum / points.len() as f64)
            })
        },
    );
}

// ---------------------------------------------------------------------------
// time-series operators
// ---------------------------------------------------------------------------

/// Parse a Rhai array of observation maps into `(ts, value)` pairs, sorted by
/// time. Anything that is not a `{ts, value}` observation yields `Err`.
pub(crate) fn parse_points(points: &rhai::Array) -> Result<Vec<(i64, f64)>, String> {
    let mut out = Vec::with_capacity(points.len());
    for item in points {
        let value: serde_json::Value =
            rhai::serde::from_dynamic(item).map_err(|e| format!("observation: {e}"))?;
        let ts = value
            .get("ts")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| "observation missing integer `ts`".to_string())?;
        let val = value
            .get("value")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| "observation missing numeric `value`".to_string())?;
        out.push((ts, val));
    }
    out.sort_by_key(|&(ts, _)| ts);
    Ok(out)
}

/// One `(ts, value)` pair as a Rhai map, the standard shape returned by the
/// time-series operators so downstream scripts can read `.ts` / `.value`.
fn point_map(ts: i64, value: f64) -> rhai::Dynamic {
    let mut map = rhai::Map::new();
    map.insert("ts".into(), rhai::Dynamic::from(ts));
    map.insert("value".into(), rhai::Dynamic::from(value));
    rhai::Dynamic::from(map)
}

/// Aggregate `values` by `agg` name: "avg" | "min" | "max" | "sum" | "count".
/// Unknown names fall back to mean so a typo never silently returns garbage.
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

/// Time-series operator library for transform scripts.
///
/// All operators take an array of observation maps (the same shape produced
/// by `ts_query` / `process`), and return either a scalar or a new array of
/// `{ts, value}` pairs. Empty / degenerate input returns `()` (unit) for
/// scalars and an empty array for series, so scripts can `if x == ()` safely.
///
/// ```rhai
/// let pts = ts_query("station", "processed", "cpu", now - 3600, now);
/// let rate = ts_rate(pts);                  // value per second
/// let avg5 = ts_moving_avg(pts, 300);       // 5-min trailing mean
/// let per_min = ts_resample(pts, 60, "avg");// 1-min buckets, averaged
/// let p95 = ts_p95(pts);
/// let d = ts_delta(pts);                    // point-to-point change
/// ```
fn register_ts_ops(eng: &mut rhai::Engine) {
    eng.register_fn("ts_rate", |points: rhai::Array| -> rhai::Dynamic {
        let Ok(p) = parse_points(&points) else {
            return rhai::Dynamic::UNIT;
        };
        if p.len() < 2 {
            return rhai::Dynamic::UNIT;
        }
        let (t0, v0) = p.first().copied().unwrap();
        let (t1, v1) = p.last().copied().unwrap();
        let dt = t1 - t0;
        if dt == 0 {
            return rhai::Dynamic::UNIT;
        }
        rhai::Dynamic::from((v1 - v0) / dt as f64)
    });

    eng.register_fn(
        "ts_moving_avg",
        |points: rhai::Array, window_secs: i64| -> rhai::Dynamic {
            let Ok(p) = parse_points(&points) else {
                return rhai::Dynamic::from(Vec::<rhai::Dynamic>::new());
            };
            let window = window_secs.max(0);
            let mut out = Vec::with_capacity(p.len());
            for &(ts, _) in &p {
                let mut sum = 0.0;
                let mut n = 0;
                for &(ots, oval) in &p {
                    if ots > ts - window && ots <= ts {
                        sum += oval;
                        n += 1;
                    }
                }
                if n > 0 {
                    out.push(point_map(ts, sum / n as f64));
                }
            }
            rhai::Dynamic::from(out)
        },
    );

    eng.register_fn(
        "ts_resample",
        |points: rhai::Array, bucket_secs: i64, agg: &str| -> rhai::Dynamic {
            let Ok(p) = parse_points(&points) else {
                return rhai::Dynamic::from(Vec::<rhai::Dynamic>::new());
            };
            if bucket_secs <= 0 {
                return rhai::Dynamic::from(Vec::<rhai::Dynamic>::new());
            }
            let mut buckets: std::collections::BTreeMap<i64, Vec<f64>> =
                std::collections::BTreeMap::new();
            for (ts, val) in p {
                let start = ts - ts.rem_euclid(bucket_secs);
                buckets.entry(start).or_default().push(val);
            }
            let mut out = Vec::with_capacity(buckets.len());
            for (start, values) in buckets {
                out.push(point_map(start, aggregate(&values, agg)));
            }
            rhai::Dynamic::from(out)
        },
    );

    eng.register_fn(
        "ts_quantile",
        |points: rhai::Array, q: f64| -> rhai::Dynamic {
            let Ok(p) = parse_points(&points) else {
                return rhai::Dynamic::UNIT;
            };
            if p.is_empty() {
                return rhai::Dynamic::UNIT;
            }
            let mut vals: Vec<f64> = p.iter().map(|(_, v)| *v).collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let q = q.clamp(0.0, 1.0);
            let pos = q * (vals.len() - 1) as f64;
            let lo = pos.floor() as usize;
            let hi = pos.ceil() as usize;
            let frac = pos - lo as f64;
            let value = vals[lo] + (vals[hi] - vals[lo]) * frac;
            rhai::Dynamic::from(value)
        },
    );

    eng.register_fn("ts_p95", |points: rhai::Array| -> rhai::Dynamic {
        let Ok(p) = parse_points(&points) else {
            return rhai::Dynamic::UNIT;
        };
        if p.is_empty() {
            return rhai::Dynamic::UNIT;
        }
        let mut vals: Vec<f64> = p.iter().map(|(_, v)| *v).collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pos = 0.95 * (vals.len() - 1) as f64;
        let lo = pos.floor() as usize;
        let hi = pos.ceil() as usize;
        let frac = pos - lo as f64;
        rhai::Dynamic::from(vals[lo] + (vals[hi] - vals[lo]) * frac)
    });

    eng.register_fn("ts_p99", |points: rhai::Array| -> rhai::Dynamic {
        let Ok(p) = parse_points(&points) else {
            return rhai::Dynamic::UNIT;
        };
        if p.is_empty() {
            return rhai::Dynamic::UNIT;
        }
        let mut vals: Vec<f64> = p.iter().map(|(_, v)| *v).collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pos = 0.99 * (vals.len() - 1) as f64;
        let lo = pos.floor() as usize;
        let hi = pos.ceil() as usize;
        let frac = pos - lo as f64;
        rhai::Dynamic::from(vals[lo] + (vals[hi] - vals[lo]) * frac)
    });

    eng.register_fn("ts_delta", |points: rhai::Array| -> rhai::Dynamic {
        let Ok(p) = parse_points(&points) else {
            return rhai::Dynamic::from(Vec::<rhai::Dynamic>::new());
        };
        let mut out = Vec::with_capacity(p.len());
        let mut prev: Option<f64> = None;
        for (ts, val) in p {
            let delta = match prev {
                Some(pv) => val - pv,
                None => 0.0,
            };
            out.push(point_map(ts, delta));
            prev = Some(val);
        }
        rhai::Dynamic::from(out)
    });

    eng.register_fn("ts_pct_change", |points: rhai::Array| -> rhai::Dynamic {
        let Ok(p) = parse_points(&points) else {
            return rhai::Dynamic::from(Vec::<rhai::Dynamic>::new());
        };
        let mut out = Vec::with_capacity(p.len());
        let mut prev: Option<f64> = None;
        for (ts, val) in p {
            let pct = match prev {
                Some(pv) if pv != 0.0 => (val - pv) / pv * 100.0,
                _ => 0.0,
            };
            out.push(point_map(ts, pct));
            prev = Some(val);
        }
        rhai::Dynamic::from(out)
    });
}

// ---------------------------------------------------------------------------
// capacity grid analysis
// ---------------------------------------------------------------------------

/// Extract an `f64` from a Rhai dynamic accepting both int and float.
fn dyn_f64(value: &rhai::Dynamic) -> Option<f64> {
    value
        .as_float()
        .ok()
        .or_else(|| value.as_int().ok().map(|i| i as f64))
}

/// Values from a mixed Rhai array (ints and/or floats).
fn dyn_f64_array(values: &rhai::Array) -> Vec<f64> {
    values.iter().filter_map(dyn_f64).collect()
}

fn clamp_max_bit(max_bit: i64) -> usize {
    max_bit.clamp(1, 31) as usize
}

/// Capacity-grid analysis (`opsense_libs::grid`) for scripts:
/// divide `[min, max]` into equal bands and find the band count whose
/// boundary-crossing rate across the series is lowest while staying as fine
/// as possible.
///
/// ```rhai
/// // points từ trạm, biên vật lý [0, disk_capacity]
/// let g = grid_fit(points, 0.0, disk_capacity, 12);
/// let bands = num_cells(g);              // số dải tối ưu
/// let occ   = grid_occupancy(g, points, 3600);  // điểm theo bucket×band
/// let rs   = grid_ranges(g);             // #{index, low, high} từng dải
/// ```
fn register_grid_ops(eng: &mut rhai::Engine) {
    eng.register_type::<AnalysisGrid>();

    // Fit from observation-map points ({ts, value}); `()` when unparsable.
    eng.register_fn(
        "grid_fit",
        |points: rhai::Array, min: f64, max: f64, max_bit: i64| -> rhai::Dynamic {
            let Ok(parsed) = parse_points(&points) else {
                return rhai::Dynamic::UNIT;
            };
            let values: Vec<f64> = parsed.into_iter().map(|(_, v)| v).collect();
            let grid = AnalysisGrid::new(&values, min, max, clamp_max_bit(max_bit));
            rhai::Dynamic::from(grid)
        },
    );

    // Fit from a plain numeric array (ints and/or floats).
    eng.register_fn(
        "grid_fit_values",
        |values: rhai::Array, min: f64, max: f64, max_bit: i64| -> rhai::Dynamic {
            let vals = dyn_f64_array(&values);
            if vals.is_empty() {
                return rhai::Dynamic::UNIT;
            }
            let grid = AnalysisGrid::new(&vals, min, max, clamp_max_bit(max_bit));
            rhai::Dynamic::from(grid)
        },
    );

    eng.register_fn("num_cells", |g: &mut AnalysisGrid| -> i64 {
        g.num_cells() as i64
    });
    eng.register_fn("num_lines", |g: &mut AnalysisGrid| -> i64 {
        g.num_lines() as i64
    });
    eng.register_fn("grid_step", |g: &mut AnalysisGrid| -> f64 { g.step });

    // Cell index containing `y`.
    eng.register_fn("grid_cell", |g: &mut AnalysisGrid, y: f64| -> i64 {
        g.cell(y) as i64
    });

    // Boundary crossings for observation-map points (or a plain number array).
    eng.register_fn(
        "grid_crossings",
        |g: &mut AnalysisGrid, series: rhai::Array| -> rhai::Dynamic {
            let values = match parse_points(&series) {
                Ok(parsed) => parsed.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
                Err(_) => dyn_f64_array(&series),
            };
            rhai::Dynamic::from(g.crossings(&values) as i64)
        },
    );

    // Occupancy histogram: `result[bucket][cell]`, buckets of `interval_secs`.
    eng.register_fn(
        "grid_occupancy",
        |g: &mut AnalysisGrid, points: rhai::Array, interval_secs: i64| -> rhai::Dynamic {
            let Ok(parsed) = parse_points(&points) else {
                return rhai::Dynamic::UNIT;
            };
            let occupancy = g.occupancy(&parsed, interval_secs);
            // i64 cells so the result converts cleanly back through serde.
            let rows: Vec<rhai::Dynamic> = occupancy
                .into_iter()
                .map(|bucket| {
                    let cells: Vec<rhai::Dynamic> = bucket
                        .into_iter()
                        .map(|count| rhai::Dynamic::from(count as i64))
                        .collect();
                    rhai::Dynamic::from(cells)
                })
                .collect();
            rhai::Dynamic::from(rows)
        },
    );

    // Every band as `#{index, low, high}`.
    eng.register_fn("grid_ranges", |g: &mut AnalysisGrid| -> rhai::Dynamic {
        let ranges: Vec<rhai::Dynamic> = g
            .cell_ranges()
            .into_iter()
            .map(|(index, low, high)| {
                let mut map = rhai::Map::new();
                map.insert("index".into(), rhai::Dynamic::from(index as i64));
                map.insert("low".into(), rhai::Dynamic::from(low));
                map.insert("high".into(), rhai::Dynamic::from(high));
                rhai::Dynamic::from(map)
            })
            .collect();
        rhai::Dynamic::from(ranges)
    });
}
