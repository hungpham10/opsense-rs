//! # Session — run-state tách khỏi `Portfolio`
//!
//! [`crate::portfolio::Portfolio`] là **model**: loader, strategy, fee, score,
//! calendar và cấu hình chạy — bất biến trong một lần chạy. Mọi thứ *thay đổi
//! theo thời gian* thì nằm ở `Session`:
//!
//! - `orders` / `history` — lệnh đang mở / đã đóng
//! - `plan` — các [`TradingGrid`] từ lần rebuild gần nhất
//! - `review_at` — mốc rebuild kế tiếp
//! - `candle_seq` — thứ tự nến toàn cục (T+N settlement, không reset khi rebuild)
//! - `candle_id` — chỉ số nến trong review window (cột của weight matrix)
//! - `candle_ts` — nến cuối đã xử lý; chặn xử lý lại nến cũ (idempotent)
//!
//! Trước khi có `Session`, các biến này là **local của `forward`** nên hết
//! lời gọi là mất: không dừng giữa chừng rồi chạy tiếp được, và mỗi wrapper
//! (streaming realtime) phải copy lại cả vòng lặp. Với `Session`,
//! [`Portfolio::evaluate`](crate::portfolio::Portfolio::evaluate) chỉ cần
//! nhận session + hai [`FetchFn`](crate::FetchFn) là chạy tiếp được từ nến cuối.

use crate::grid::TradingGrid;
use crate::portfolio::Order;

/// State của một phiên chạy (backtest hoặc realtime). Không chứa cấu hình:
/// đổi `Portfolio` (strategy/fee/calendar) thì giữ nguyên `Session` và chạy tiếp.
#[derive(Clone, Debug, Default)]
pub struct Session {
    /// Lệnh đang mở.
    pub orders: Vec<Order>,
    /// Lệnh đã đóng (nguồn sự thật cho [`Report`](crate::portfolio::Report)).
    pub history: Vec<Order>,
    /// Plan grid hiện hành.
    pub plan: Vec<TradingGrid>,
    /// Timestamp rebuild kế tiếp. 0 = chưa rebuild lần nào.
    pub review_at: u64,
    /// Thứ tự nến toàn cục, tăng đơn điệu qua cả backtest lẫn realtime.
    pub candle_seq: u64,
    /// Chỉ số nến trong review window hiện tại (reset khi rebuild) — dùng làm
    /// cột weight matrix của `TradingGrid`.
    pub candle_id: usize,
    /// Timestamp (giây) của nến cuối đã xử lý.
    pub candle_ts: i64,
}

impl Session {
    /// Session trống — backtest mới, hoặc realtime lúc khởi động.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Đã xử lý candle nào chưa?
    #[must_use]
    pub fn processed_upto(&self) -> i64 {
        self.candle_ts
    }

    /// Còn lệnh nào đang mở không?
    #[must_use]
    pub fn has_open_orders(&self) -> bool {
        !self.orders.is_empty()
    }
}
