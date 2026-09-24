//! Candle access over station data.
//!
//! A [`TimeseriesStation`] filled with OHLCV observations (convention below)
//! can be read back as [`CandleStick`]s through [`TimeseriesStationHandle`],
//! which implements the qlib [`DataLoader`] trait. This makes "fetch and load"
//! natural: collectors (`http`, `csv`) persist observations into a station,
//! and any consumer — including scripts — pulls real candles through the same
//! [`DataLoader::range`] surface the rest of the trading domain uses.
//!
//! ## OHLCV observation convention
//!
//! Per candle five observations are stored:
//!
//! - `ts` = candle open time (seconds)
//! - `metric_id` = symbol (e.g. `"BTCUSDT"`)
//! - `kind` = `"metric"`, `signal` = `"raw"`
//! - `value` = price / volume
//! - `labels.field` ∈ {`"o"`, `"h"`, `"l"`, `"c"`, `"v"`}
//! - `labels.resolution` (optional) = e.g. `"1m"`, `"1D"` — filters when one
//!   station holds several resolutions

use std::collections::BTreeMap;
use std::io::Error;
use std::sync::Arc;

use async_trait::async_trait;
use opsense_model::events::Observation;
use opsense_qlib::{CandleStick, DataLoader};
use tokio::sync::RwLock;

use crate::station::TimeseriesStation;

/// OHLCV fields stored per candle in `labels.field`.
pub const OHLCV_FIELDS: [&str; 5] = ["o", "h", "l", "c", "v"];

/// Handle wrapping a registered [`TimeseriesStation`] so it can be queried as
/// a candle [`DataLoader`]. Clonable; holds no lock until `range` is called.
#[derive(Clone)]
pub struct TimeseriesStationHandle {
    station: Arc<RwLock<TimeseriesStation>>,
}

impl TimeseriesStationHandle {
    /// Wrap a station reference obtained from the pipeline registry.
    #[must_use]
    pub fn new(station: Arc<RwLock<TimeseriesStation>>) -> Self {
        Self { station }
    }
}

#[async_trait]
impl DataLoader for TimeseriesStationHandle {
    async fn range(&self, from: u64, to: u64, resolution: &str) -> Result<Vec<CandleStick>, Error> {
        let observations = self
            .station
            .read()
            .await
            .query_recent(from as i64, to as i64)
            .await
            .unwrap_or_default();
        Ok(candles_from_observations(&observations, resolution))
    }
}

/// Assemble [`CandleStick`]s from OHLCV observations (see module docs).
#[must_use]
pub fn candles_from_observations(
    observations: &[Observation],
    resolution: &str,
) -> Vec<CandleStick> {
    // Group per candle timestamp; values arrive field-by-field.
    let mut rows: BTreeMap<i64, [f64; 5]> = BTreeMap::new();
    for obs in observations {
        let Some(field) = obs.labels.get("field").map(String::as_str) else {
            continue;
        };
        if !OHLCV_FIELDS.contains(&field) {
            continue;
        }
        // Optional resolution filter: skip rows tagged with a different
        // resolution; rows without the label are kept (single-resolution use).
        if let Some(res) = obs.labels.get("resolution")
            && !res.is_empty()
            && res.as_str() != resolution
        {
            continue;
        }
        let idx = OHLCV_FIELDS
            .iter()
            .position(|f| *f == field)
            .expect("field in OHLCV_FIELDS");
        rows.entry(obs.ts).or_default()[idx] = obs.value;
    }

    rows.into_iter()
        .map(|(t, [o, h, l, c, v])| {
            let (o, h, l, c, v) = reconcile(o, h, l, c, v);
            CandleStick { t, o, h, l, c, v }
        })
        .collect()
}

/// Fill missing OHLCV cells from the best known price so partial writes still
/// produce a usable candle: missing close/open fall back to the last known
/// price; missing high/low derive from the known extremes.
fn reconcile(o: f64, h: f64, l: f64, c: f64, v: f64) -> (f64, f64, f64, f64, f64) {
    let has = |x: f64| x > 0.0;
    let base = if has(c) { c } else if has(o) { o } else { h.max(l) };
    let o = if has(o) { o } else { base };
    let c = if has(c) { c } else { base };
    let h = if has(h) { h } else { o.max(c) };
    let l = if has(l) { l } else { o.min(c) };
    (o, h, l, c, v.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::{OHLCV_FIELDS, candles_from_observations};
    use opsense_model::events::{Observation, Signal, TelemetryKind};

    fn obs(ts: i64, field: &str, value: f64, resolution: &str) -> Observation {
        Observation::new(
            ts,
            "BTCUSDT".into(),
            TelemetryKind::Metric,
            Signal::Raw,
            value,
        )
        .with_label("field", field)
        .with_label("resolution", resolution)
    }

    #[test]
    fn assembles_candles_in_ts_order() {
        let (ts_a, ts_b) = (1_700_000_100, 1_700_000_200);
        let mut obs_rows: Vec<Observation> = Vec::new();
        for (i, ts) in [ts_a, ts_b].into_iter().enumerate() {
            let base = 100.0 + i as f64;
            for field in OHLCV_FIELDS {
                let value = match field {
                    "o" => base,
                    "h" => base + 1.0,
                    "l" => base - 1.0,
                    "c" => base + 0.5,
                    _ => 10.0,
                };
                obs_rows.push(obs(ts, field, value, "1m"));
            }
        }

        let candles = candles_from_observations(&obs_rows, "1m");
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].t, ts_a);
        assert_eq!(candles[0].o, 100.0);
        assert_eq!(candles[1].t, ts_b);
        assert_eq!(candles[1].c, 101.5);
        assert_eq!(candles[1].v, 10.0);
    }

    #[test]
    fn filters_by_resolution() {
        let mut rows = vec![obs(1, "o", 10.0, "1m"), obs(1, "c", 10.5, "1D")];
        let mut extra = obs(1, "h", 11.0, "1m");
        extra.labels.remove("resolution");
        rows.push(extra);

        let candles = candles_from_observations(&rows, "1m");
        assert_eq!(candles.len(), 1);
        // Untagged observation kept; the `1D` close filtered out (≠ 10.5).
        assert_eq!(candles[0].o, 10.0);
        assert_eq!(candles[0].h, 11.0);
        // Missing close (filtered) falls back to the open; low derives from
        // the known extremes.
        assert_eq!(candles[0].c, 10.0);
        assert_eq!(candles[0].l, 10.0);
    }

    #[test]
    fn empty_and_unknown_fields_yield_empty() {
        let rows = vec![obs(1, "open", 10.0, "1m")];
        assert!(candles_from_observations(&rows, "1m").is_empty());
        assert!(candles_from_observations(&[], "1m").is_empty());
    }
}