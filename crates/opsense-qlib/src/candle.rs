//! # CandleStick — canonical OHLCV type cho toàn bộ hệ thống
//!
//! Field names ngắn (`t`, `o`, `h`, `l`, `c`, `v`) để tương thích với
//! API bên ngoài (Investing.com, SimpleFX).

use crate::Tick;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Một cây nến OHLCV — canonical type dùng chung cho toàn bộ hệ thống.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, Default)]
pub struct CandleStick {
    /// Unix timestamp (seconds).
    pub t: i64,
    /// Open price.
    pub o: f64,
    /// High price.
    pub h: f64,
    /// Low price.
    pub l: f64,
    /// Close price.
    pub c: f64,
    /// Volume.
    pub v: f64,
}

impl CandleStick {
    /// Tạo nến từ dữ liệu thô.
    pub fn new(t: i64, o: f64, h: f64, l: f64, c: f64, v: f64) -> Self {
        Self { t, o, h, l, c, v }
    }

    pub fn max(&self) -> f64 {
        self.o.max(self.c)
    }

    pub fn min(&self) -> f64 {
        self.o.min(self.c)
    }

    pub fn update(&mut self, tick: &Tick, interval_ms: u64) -> Option<CandleStick> {
        let interval_ms = interval_ms as i64;
        if interval_ms == 0 {
            return None;
        }

        let tick_candle_start = (tick.timestamp / interval_ms) * interval_ms;

        if self.t == 0 {
            // Lần đầu khởi tạo
            self.t = tick_candle_start;
            self.o = tick.price;
            self.h = tick.price;
            self.l = tick.price;
            self.c = tick.price;
            self.v = tick.quantity;
            None
        } else if tick_candle_start > self.t {
            // Đã sang nến mới -> Lưu cây nến cũ ra để trả về
            let closed_candle = *self;

            // Reset self thành nến mới
            self.t = tick_candle_start;
            self.o = tick.price;
            self.h = tick.price;
            self.l = tick.price;
            self.c = tick.price;
            self.v = tick.quantity;

            Some(closed_candle)
        } else {
            self.h = self.h.max(tick.price);
            self.l = self.l.min(tick.price);
            self.c = tick.price;
            self.v += tick.quantity;
            None
        }
    }
}
