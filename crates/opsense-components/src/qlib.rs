use std::io::{Error, ErrorKind};
use std::sync::Arc;

use opsense_macros::transform;
use opsense_qlib::{CandleStick, OrderEvent, StreamingPortfolio};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::vector::runtime::{Component, Identify, Message, Outbound};

/// Transform nhận candle JSON (`{"t","o","h","l","c","v"}`) từ upstream
/// (vd json_2_json), feed vào [`StreamingPortfolio`] giữ state giữa các
/// message, rồi emit từng [`OrderEvent`] thành JSON trade event:
/// `{"event_id", "ts", "broker", "symbol", "event"}` — tương thích trực tiếp
/// với `trade_event_station_transform` (persist trước khi forward).
#[transform]
pub struct QlibEngine {
    pub id: String,
    pub inputs: Vec<String>,
    pub portfolio: StreamingPortfolio,
}

impl QlibEngine {
    #[must_use]
    pub fn new(id: String, inputs: Vec<String>, portfolio: StreamingPortfolio) -> Self {
        Self {
            id,
            inputs,
            portfolio,
        }
    }

    fn candle_from_payload(payload: &Value) -> Result<CandleStick, Error> {
        let candle: CandleStick = serde_json::from_value(payload.clone())
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("candle payload: {e}")))?;
        if !candle.t.is_positive() || !candle.c.is_finite() || candle.c <= 0.0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "candle must have positive ts and finite close",
            ));
        }
        Ok(candle)
    }

    fn events_to_payload(ts: u64, events: &[OrderEvent]) -> Vec<Value> {
        events
            .iter()
            .enumerate()
            .map(|(index, event)| {
                Value::Object({
                    let mut map = serde_json::Map::new();
                    map.insert(
                        "event_id".into(),
                        Value::String(format!("qlib-{ts}-{index}")),
                    );
                    map.insert(
                        "ts".into(),
                        Value::from(i64::try_from(ts).unwrap_or(i64::MAX)),
                    );
                    map.insert("broker".into(), Value::String("paper".into()));
                    map.insert("symbol".into(), Value::String("BTC/USDT".into()));
                    map.insert(
                        "event".into(),
                        serde_json::to_value(event).unwrap_or(Value::Null),
                    );
                    map
                })
            })
            .collect()
    }
}

impl_qlib_engine!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let engine = Arc::new(Mutex::new(self.portfolio.clone()));
        while let Some(msg) = rx.recv().await {
            let candle = QlibEngine::candle_from_payload(&msg.payload)?;
            let events = {
                let mut engine = engine.lock().await;
                engine.on_candle(&candle).await?
            };
            if events.is_empty() {
                continue;
            }
            let payloads = QlibEngine::events_to_payload(candle.t.max(0) as u64, &events);
            for stream in &tx.streams {
                for payload in &payloads {
                    stream
                        .send(Message {
                            payload: payload.clone(),
                        })
                        .await
                        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
                }
            }
        }
        Ok(())
    }
);
