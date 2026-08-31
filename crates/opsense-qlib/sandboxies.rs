//! # Tick2Signals — Forward-testing Sink Component
//!
//! Nhận `Tick` từ upstream source, aggregate thành nến OHLC, định kỳ gọi
//! `Portfolio::forward()` để rebuild grid và xử lý giao dịch.
//! Gửi Telegram signal, lưu state vào Redis.
//!
//! Hỗ trợ nhiều `(broker, symbol)` trong cùng một config — mỗi cặp có
//! Portfolio riêng, state riêng, được route bằng `tick.broker` / `tick.symbol`.

use std::collections::HashMap;
use std::fmt;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use reqwest::Client as HttpClient;
use reqwest_middleware::ClientBuilder;
use reqwest_tracing::TracingMiddleware;
use schemas::{CandleStick, Tick};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use vector_config_macro::transform;
use vector_runtime::{Component, Identify, Message};

use super::{Calendar, Fee, FromQueryCandleSticks, Order, OrderEvent, Portfolio, Score, Strategy};

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════
fn default_analysis_resolution() -> String {
    "1H".into()
}

fn default_trading_resolution() -> String {
    "5m".into()
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct Sandbox {
    pub broker: String,
    pub symbol: String,
    pub orders: Vec<Order>,
    pub history: Vec<Order>,

    #[serde(default = "default_analysis_resolution")]
    pub analysis_resolution: String,

    #[serde(default = "default_trading_resolution")]
    pub trading_resolution: String,

    #[serde(skip)]
    pub portfolio: Option<Arc<Portfolio>>,

    #[serde(skip)]
    pub interval: u64,
}

#[transform(exclude(Debug, PartialEq))]
struct Tick2Signals {
    pub id: String,
    pub inputs: Vec<String>,
    pub sandboxes: Vec<Sandbox>,

    #[serde(default)]
    pub strategy: Option<Arc<dyn Strategy>>,

    #[serde(default)]
    pub fee: Option<Arc<dyn Fee>>,

    #[serde(default)]
    pub score: Option<Arc<dyn Score>>,

    #[serde(default)]
    pub calendar: Option<Arc<dyn Calendar>>,
}

impl fmt::Debug for Tick2Signals {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Tick2Signals")
            .field("id", &self.id)
            .field("strategy", &self.strategy.as_ref().map(|_| "Some"))
            .field("fee", &self.fee.as_ref().map(|_| "Some"))
            .field("score", &self.score.as_ref().map(|_| "Some"))
            .field("calendar", &self.calendar.as_ref().map(|_| "Some"))
            .field("sandboxes", &self.sandboxes.len())
            .finish()
    }
}

impl PartialEq for Tick2Signals {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && serde_json::to_string(&self.strategy).ok()
                == serde_json::to_string(&other.strategy).ok()
            && serde_json::to_string(&self.fee).ok() == serde_json::to_string(&other.fee).ok()
            && serde_json::to_string(&self.score).ok() == serde_json::to_string(&other.score).ok()
            && serde_json::to_string(&self.calendar).ok()
                == serde_json::to_string(&other.calendar).ok()
            && serde_json::to_string(&self.sandboxes).ok()
                == serde_json::to_string(&other.sandboxes).ok()
    }
}

impl_tick_2_signals!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: vector_runtime::Outbound,
    ) -> Result<(), std::io::Error> {
        let tx_streams = tx.streams.clone();

        let mut candles = HashMap::<String, HashMap<String, CandleStick>>::new();

        let http_client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let strategy = self
            .strategy
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "`strategy` is required".to_string()))?
            .clone();
        let fee = self
            .fee
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "`fee` is required".to_string()))?
            .clone();
        let score = self
            .score
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "`score` is required".to_string()))?
            .clone();
        let calendar = self
            .calendar
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "`calendar` is required".to_string()))?
            .clone();

        let mut entries = self.sandboxes.iter().try_fold(
            HashMap::new(),
            |mut acc,
             sandbox|
             -> Result<HashMap<String, HashMap<String, Sandbox>>, std::io::Error> {
                let loader = Arc::new(FromQueryCandleSticks::new(
                    sandbox.broker.clone(),
                    sandbox.symbol.clone(),
                    Some(http_client.clone()),
                ));

                let entry = Sandbox {
                    broker: sandbox.broker.clone(),
                    symbol: sandbox.symbol.clone(),
                    orders: sandbox.orders.clone(),
                    history: sandbox.orders.clone(),
                    analysis_resolution: sandbox.analysis_resolution.clone(),
                    trading_resolution: sandbox.trading_resolution.clone(),
                    interval: resolution_to_seconds(&sandbox.trading_resolution) * 1000,
                    portfolio: Some(Arc::new(Portfolio::new(
                        loader,
                        strategy.clone(),
                        fee.clone(),
                        score.clone(),
                        calendar.clone(),
                        sandbox.analysis_resolution.clone(),
                        sandbox.trading_resolution.clone(),
                        crate::qlib::DEFAULT_SETTLEMENT_CANDLES,
                    )?)),
                };

                acc.entry(sandbox.broker.clone())
                    .or_default()
                    .insert(sandbox.symbol.clone(), entry);

                Ok(acc)
            },
        )?;

        loop {
            if let Some(msg) = rx.recv().await {
                let tick: Tick = match serde_json::from_value(msg.payload) {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                if let Some(broker) = entries.get_mut(&tick.broker)
                    && let Some(sandbox) = broker.get_mut(&tick.symbol)
                    && let Some(candle) = candles
                        .entry(tick.broker.clone())
                        .or_default()
                        .entry(tick.symbol.clone())
                        .or_default()
                        .update(&tick, sandbox.interval)
                    && let Some(portfolio) = &sandbox.portfolio
                {
                    let broker_name = sandbox.broker.clone();
                    let symbol_name = sandbox.symbol.clone();
                    let streams = tx_streams.clone();

                    portfolio
                        .hands_on(
                            &mut sandbox.orders,
                            &mut sandbox.history,
                            0, // lookback — single-candle, không cần prefetch
                            candle.t as u64,
                            candle.t as u64 + 1,
                            &mut move |from, to| {
                                let candle = candle;

                                Box::pin(async move {
                                    if candle.t as u64 >= from && (candle.t as u64) < to {
                                        Ok(vec![candle])
                                    } else {
                                        Ok(vec![])
                                    }
                                })
                            },
                            &mut move |event: OrderEvent| {
                                // Telegram signal chỉ quan tâm lệnh đã đóng
                                let OrderEvent::Closed { order, .. } = event else {
                                    return Box::pin(async move { Ok(()) });
                                };

                                let streams = streams.clone();
                                let broker = broker_name.clone();
                                let symbol = symbol_name.clone();

                                Box::pin(async move {
                                    let text = format!(
                                        "📊 *Order Closed*\n\
		                                     {}:{} ({})\n\
		                                     Entry: {:.2} → Exit: {:.2}\n\
		                                     PnL: {:+.2}%\n\
		                                     Size: {:.4}\n\
		                                     Grid: [{}][{}]",
                                        broker,
                                        symbol,
                                        order.dtype,
                                        order.entry_price,
                                        order.exit_price.unwrap_or(0.0),
                                        order.pnl_pct.unwrap_or(0.0) * 100.0,
                                        order.size,
                                        order.grid_index,
                                        order.level_index,
                                    );

                                    let payload = serde_json::json!({
                                        "text": text,
                                        "parse_mode": "Markdown",
                                    });
                                    for stream in &streams {
                                        stream
                                            .send(Message {
                                                payload: payload.clone(),
                                            })
                                            .await
                                            .map_err(|e| {
                                                Error::other(format!("notify send: {e}"))
                                            })?;
                                    }
                                    Ok(())
                                })
                            },
                        )
                        .await?;
                }
            }
        }
    }
);

fn resolution_to_seconds(res: &str) -> u64 {
    let digits = res
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>();
    let num = digits.parse().unwrap_or(1);
    let suffix = &res[digits.len()..];

    match suffix {
        "s" => num,
        "" | "m" => num * 60,
        "H" | "h" => num * 3600,
        "D" | "d" => num * 86400,
        "W" | "w" => num * 604800,
        _ => num * 60, // default: treat as minutes
    }
}
