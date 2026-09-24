//! Shared OHLCV row → observation conversion.
//!
//! Both the HTTP candle mode ([`crate::http::CandleParse`]) and the CSV node
//! ([`crate::csv::CsvSource`]) expand a parsed candle row
//! `[open time, open, high, low, close, volume]` into five observations
//! following the shared station convention (see
//! `opsense_core` → [`candles`]): `ts` = open time in seconds,
//! `metric_id` = symbol, `kind` = `"metric"`, `signal` = `"raw"`,
//! `labels.field` ∈ {`o`, `h`, `l`, `c`, `v`}, `labels.resolution` =
//! configured resolution. A station fed by either source is thus fully
//! interchangeable for `DataLoader` consumers.
//!
//! [`candles`]: opsense_core::candles

use opsense_core::{Observation, Signal, TelemetryKind};

/// Candle fields, in order, as stored in `labels.field`.
pub use opsense_core::OHLCV_FIELDS;

/// Normalize an open-time raw tick to whole seconds.
///
/// `unit_ms` = milliseconds per open-time unit (`1` for millisecond
/// timestamps — Binance klines; `1000` for second-based timestamps).
#[must_use]
pub fn open_time_to_secs(raw: f64, unit_ms: u64) -> i64 {
    (raw * unit_ms.max(1) as f64 / 1000.0) as i64
}

/// Expand one candle row into five OHLCV observations (see module docs).
#[must_use]
pub fn row_to_observations(
    ts: i64,
    cells: &[f64; 6],
    symbol: &str,
    resolution: &str,
) -> Vec<Observation> {
    OHLCV_FIELDS
        .iter()
        .zip(cells[1..].iter())
        .map(|(field, val)| {
            Observation::new(ts, symbol.to_string(), TelemetryKind::Metric, Signal::Raw, *val)
                .with_label("field", *field)
                .with_label("resolution", resolution)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{open_time_to_secs, row_to_observations};

    #[test]
    fn normalizes_millis_and_seconds() {
        assert_eq!(open_time_to_secs(1_700_000_000_000.0, 1), 1_700_000_000);
        assert_eq!(open_time_to_secs(1_700_000_000.0, 1000), 1_700_000_000);
        assert_eq!(open_time_to_secs(0.0, 1000), 0);
    }

    #[test]
    fn row_expands_to_five_observations() {
        let cells = [1_700_000_000.0, 1.0, 2.0, 0.5, 1.5, 10.0];
        let out = row_to_observations(1_700_000_000, &cells, "BTCUSDT", "1m");
        assert_eq!(out.len(), 5);
        assert_eq!(out[0].metric_id, "BTCUSDT");
        assert_eq!(out[0].labels.get("field").map(String::as_str), Some("o"));
        assert_eq!(out[4].labels.get("field").map(String::as_str), Some("v"));
        assert_eq!(out[4].value, 10.0);
        assert_eq!(out[0].labels.get("resolution").map(String::as_str), Some("1m"));
    }
}