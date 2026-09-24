//! CSV source node: read a CSV file on every tick, parse OHLCV rows
//! (`open time, open, high, low, close, volume`), write observations to the
//! node's own `Timeseries` station, then forward `data_ready(ts)`.
//!
//! Same role as the HTTP candle mode ([`crate::http::CandleParse`]) but reading
//! delimiter-separated text from disk instead of JSON over the wire. Both
//! expand rows into the shared OHLCV station convention
//! ([`crate::ohlcv`]), so a station fed by either source is interchangeable
//! for `DataLoader` consumers.
//!
//! The path is template-driven via `{{name}}` placeholders resolved per cycle
//! from the incoming payload fields and the pipeline `Context` attributes
//! (same lookup `HttpSource` uses for its URL).

use std::collections::BTreeMap;
use std::io::Error;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, RwLock};

use opsense_core::{Observation, Station, TimeseriesStation};
use opsense_macros::transform;

use crate::station::downcast_ctx;
use crate::vector::runtime::{Component, Identify, Message, Outbound};
use crate::{render, signal};

/// `station = true` makes the node terminal: its own station is queryable, so
/// the node does not need a downstream consumer to be useful.
#[transform(terminal_field = "station")]
pub struct CsvSource {
    pub id: String,
    pub inputs: Vec<String>,

    /// Path to the CSV file. `{{name}}` placeholders are resolved per cycle.
    pub path: String,

    /// First row is a header and is skipped. Defaults to `true`.
    #[serde(default = "default_has_header")]
    pub has_header: bool,

    /// Column indexes selecting `[open time, open, high, low, close, volume]`
    /// from each row. Defaults to the leading six columns `[0,1,2,3,4,5]`.
    #[serde(default = "default_columns")]
    pub columns: [usize; 6],

    /// Milliseconds represented by one open-time unit (`1` for millisecond
    /// timestamps — Binance klines; `1000` for second-based timestamps).
    #[serde(default = "default_unit_ms")]
    pub unit_ms: u64,

    /// Symbol written as `metric_id` on each observation (e.g. `"BTCUSDT"`).
    pub symbol: String,

    /// Resolution stored under `labels.resolution` (e.g. `"1m"`).
    pub resolution: String,

    /// Register a `TimeseriesStation` under this node's id so reads can go
    /// through the registry (REPL/MCP/HTTP).
    #[serde(default = "default_station")]
    pub station: bool,
}

const fn default_has_header() -> bool {
    true
}

const fn default_columns() -> [usize; 6] {
    [0, 1, 2, 3, 4, 5]
}

const fn default_unit_ms() -> u64 {
    1
}

const fn default_station() -> bool {
    true
}

impl CsvSource {
    #[must_use]
    pub fn new(
        id: &str,
        inputs: &[&str],
        path: &str,
        symbol: impl Into<String>,
        resolution: impl Into<String>,
    ) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            path: path.to_string(),
            has_header: default_has_header(),
            columns: default_columns(),
            unit_ms: default_unit_ms(),
            symbol: symbol.into(),
            resolution: resolution.into(),
            station: default_station(),
        }
    }
}

/// Parse a CSV body of OHLCV rows into observations. Malformed rows (missing a
/// column or non-numeric cell) are skipped and logged.
fn parse_csv(body: &str, cfg: &CsvSource) -> Result<Vec<Observation>, String> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(cfg.has_header)
        .flexible(true)
        .from_reader(body.as_bytes());

    let mut out = Vec::new();
    for (idx, record) in reader.records().enumerate() {
        let record = record.map_err(|e| format!("row {idx}: {e}"))?;
        let mut cells = [0.0f64; 6];
        let mut complete = true;
        for (col, column) in cfg.columns.iter().enumerate() {
            match record
                .get(*column)
                .and_then(|s| s.trim().parse::<f64>().ok())
            {
                Some(v) => cells[col] = v,
                None => {
                    tracing::warn!("csv row {idx}: non-numeric column {column}, skipped");
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }
        let ts = crate::ohlcv::open_time_to_secs(cells[0], cfg.unit_ms);
        out.extend(crate::ohlcv::row_to_observations(
            ts,
            &cells,
            &cfg.symbol,
            &cfg.resolution,
        ));
    }
    Ok(out)
}

impl_csv_source!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;

        // Register the station eagerly so reads before the first cycle still
        // resolve to an empty timeseries rather than a `NotFound` error.
        if self.station {
            let station = TimeseriesStation::from_storage(&self.id, ctx.storage()).await?;
            ctx.registry(
                &self.id,
                Station::Timeseries(Arc::new(RwLock::new(station))),
            )
            .await
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
        }
        let me_handle = if self.station {
            Some(
                ctx.station::<Arc<RwLock<TimeseriesStation>>>(&self.id).await?,
            )
        } else {
            None
        };

        while let Some(msg) = rx.recv().await {
            // Only ticks and `data_ready`/`processed` carry a usable ts.
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            // 1. resolve `{{name}}` placeholders (payload fields skip the
            //    bindings layer; context attributes/env provide the rest).
            let vars = crate::http::build_vars(ctx, BTreeMap::new(), &msg.payload).await;
            let path = match render(&self.path, &vars) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("csv {} path render: {e}", self.id);
                    continue;
                }
            };

            // 2. read + parse.
            let body = match tokio::fs::read_to_string(&path).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("csv {} read {path}: {e}", self.id);
                    continue;
                }
            };
            let batch = match parse_csv(&body, self) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("csv {} parse: {e}", self.id);
                    continue;
                }
            };

            // 3. write to station.
            if let Some(me) = &me_handle
                && !batch.is_empty()
            {
                let from = batch.iter().map(|o| o.ts).min().unwrap_or(ts);
                let to = batch.iter().map(|o| o.ts).max().unwrap_or(ts);
                me.write().await.update_range(&batch, from, to, to);
            }

            // 4. forward so downstream nodes see this cycle.
            let out = if batch.is_empty() {
                signal::data_ready(ts)
            } else {
                signal::data_ready_with(ts, serde_json::to_value(&batch).unwrap_or(Value::Null))
            };
            let ready = signal::tagged(out, &self.id);
            for s in &tx.streams {
                let _ = s.send(ready.clone()).await;
            }
        }
        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::{CsvSource, parse_csv};

    fn cfg(path: &str) -> CsvSource {
        CsvSource::new("csv", &["clock"], path, "BTCUSDT", "1m")
    }

    #[test]
    fn parses_header_rows() {
        let body = "open_time,open,high,low,close,volume\n\
                    1700000000000,100,101,99,100.5,10\n\
                    1700000060000,100.5,102,100,101,20\n";
        let obs = parse_csv(body, &cfg("x.csv")).unwrap();
        assert_eq!(obs.len(), 10); // 2 candles × 5 fields
        let ts: Vec<_> = obs.iter().map(|o| o.ts).collect();
        assert!(ts.contains(&1_700_000_000));
        assert!(ts.contains(&1_700_000_060));
        let close: f64 = obs
            .iter()
            .filter(|o| o.ts == 1_700_000_000)
            .find(|o| o.labels.get("field").map(String::as_str) == Some("c"))
            .unwrap()
            .value;
        assert_eq!(close, 100.5);
    }

    #[test]
    fn parses_headerless_with_custom_columns_and_unit() {
        let mut source = cfg("x.csv");
        source.has_header = false;
        source.unit_ms = 1000;
        // Reorder: ts at col 1, o/h/l/c at 2..6, v at 0.
        source.columns = [1, 2, 3, 4, 5, 0];
        let body = "10,1700000000,1.1,1.2,1.0,1.15\n41,1700003600,1.15,1.3,1.1,1.25\n";
        let obs = parse_csv(body, &source).unwrap();
        assert_eq!(obs.len(), 10);
        assert_eq!(obs[0].ts, 1_700_000_000);
        assert_eq!(obs[5].ts, 1_700_003_600);
        assert_eq!(obs[0].value, 1.1); // open
        assert_eq!(obs[4].value, 10.0); // volume (:0)
    }

    #[test]
    fn skips_malformed_rows() {
        let mut source = cfg("x.csv");
        source.has_header = false;
        let body = "1700000000000,100,101,99,100.5,10\n\
                    bad row\n\
                    1700000060000,100.5\n";
        let obs = parse_csv(body, &source).unwrap();
        assert_eq!(obs.len(), 5); // only the complete row survived
        assert!(obs.iter().all(|o| o.ts == 1_700_000_000));
    }
}