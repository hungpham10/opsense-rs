use crate::CandleStick;

#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct Tick {
    pub broker: String,
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    pub timestamp: i64,
    /// Unix timestamp tính bằng mili-giây (milliseconds) tại thời điểm khớp lệnh
    pub candlestick: Option<CandleStick>,
}
