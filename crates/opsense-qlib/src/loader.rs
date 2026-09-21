use std::io::Error;
use std::sync::RwLock;

use crate::candle::CandleStick;
use crate::ohcl::QueryCandleSticks;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::DataLoader;

#[derive(Deserialize, Serialize, Default)]
pub struct FromQueryCandleSticks {
    /// Broker/provider name (e.g. "simplefx", "dnse").
    pub broker: String,

    /// Symbol name (e.g. "BTCUSD", "VN30").
    pub symbol: String,

    #[serde(skip)]
    engine: RwLock<Option<QueryCandleSticks>>,

    #[serde(skip)]
    client: RwLock<Option<reqwest::Client>>,
}

impl FromQueryCandleSticks {
    pub fn new(
        broker: impl Into<String>,
        symbol: impl Into<String>,
        client: Option<reqwest::Client>,
    ) -> Self {
        Self {
            broker: broker.into(),
            symbol: symbol.into(),
            engine: RwLock::new(None),
            client: RwLock::new(client),
        }
    }
}

#[typetag::serde(name = "from_query_candlesticks")]
#[async_trait]
impl DataLoader for FromQueryCandleSticks {
    async fn range(&self, from: u64, to: u64, resolution: &str) -> Result<Vec<CandleStick>, Error> {
        let engine = {
            let mut client = self
                .client
                .write()
                .map_err(|_| Error::other("Lock poison"))?;
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Error::other("Lock poison"))?;

            if client.is_none() {
                // Initialize the default reqwest client if none was provided.
                let http_client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .map_err(|e| Error::other(format!("Failed to setup http client: {e}")))?;

                *client = Some(http_client.clone());
                *engine = Some(QueryCandleSticks::new(http_client, 32)?);
            } else if engine.is_none() {
                let http_client = client
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| Error::other("Fail to setup http client"))?;
                *engine = Some(QueryCandleSticks::new(http_client, 32)?);
            }

            engine.clone()
        };

        if let Some(engine) = engine {
            engine
                .get_candlesticks(
                    &self.broker,
                    &self.symbol,
                    resolution,
                    from.try_into().map_err(|error| {
                        Error::other(format!("`from` converting failed: {error}"))
                    })?,
                    to.try_into().map_err(|error| {
                        Error::other(format!("`to` converting failed: {error}"))
                    })?,
                    0,
                )
                .await
        } else {
            Ok(Vec::new())
        }
    }
}

/// CSV DataLoader — đọc dữ liệu OHLCV từ file CSV.
#[derive(Deserialize, Serialize, Default)]
pub struct FromCsv {
    /// Đường dẫn đến file CSV.
    pub path: String,

    /// Resolution mặc định (VD: "1H", "1D").
    pub resolution: String,

    /// CSV có header row không? (mặc định: true)
    #[serde(default = "default_true")]
    pub has_header: bool,

    #[serde(skip)]
    candles: RwLock<Option<Vec<CandleStick>>>,
}

impl FromCsv {
    /// Tạo DataLoader đọc dữ liệu từ file CSV.
    pub fn new(path: impl Into<String>, resolution: impl Into<String>, has_header: bool) -> Self {
        Self {
            path: path.into(),
            resolution: resolution.into(),
            has_header,
            candles: RwLock::new(None),
        }
    }
}

fn default_true() -> bool {
    true
}

#[typetag::serde(name = "from_csv")]
#[async_trait]
impl DataLoader for FromCsv {
    async fn range(
        &self,
        from: u64,
        to: u64,
        _resolution: &str,
    ) -> Result<Vec<CandleStick>, Error> {
        let mut cache = self
            .candles
            .write()
            .map_err(|e| Error::other(format!("lock poison: {e}")))?;

        if cache.is_none() {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(self.has_header)
                .from_path(&self.path)
                .map_err(|e| Error::other(format!("cannot open CSV '{}': {e}", self.path)))?;

            let candles = reader
                .deserialize()
                .filter_map(|r| match r {
                    Ok(c) => Some(c),
                    Err(e) => {
                        eprintln!("  ⚠ CSV row skipped: {e}");
                        None
                    }
                })
                .collect::<Vec<CandleStick>>();

            if candles.is_empty() {
                return Err(Error::other(format!(
                    "CSV '{}' contains no valid candles",
                    self.path
                )));
            }

            eprintln!(
                "  ✓ Loaded {} candles from CSV '{}'",
                candles.len(),
                self.path
            );
            *cache = Some(candles);
        }

        let candles = cache.as_ref().unwrap();
        let start = candles.partition_point(|c| (c.t as u64) < from);
        let end = candles.partition_point(|c| (c.t as u64) < to);

        Ok(candles[start..end].to_vec())
    }
}
