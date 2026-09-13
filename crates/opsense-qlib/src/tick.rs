use crate::CandleStick;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, Default)]
pub struct Tick {
    #[schema(example = "binance")]
    pub broker: String,

    #[schema(example = "BTCUSDT")]
    pub symbol: String,

    #[schema(example = 60150.5)]
    pub price: f64,

    #[schema(example = 1.25)]
    pub quantity: f64,

    /// Unix timestamp tính bằng mili-giây (milliseconds) tại thời điểm khớp lệnh
    #[schema(example = 1715855400000i64)] // Ví dụ một timestamp thực tế
    pub timestamp: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub candlestick: Option<CandleStick>,
}
