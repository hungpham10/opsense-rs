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
//! - `next_ts` — con trỏ: nến kế tiếp `forward` sẽ lấy
//! - `kelly_fraction` / `base_capital` / `settlement` — hằng số đã resolve từ
//!   `params`, để `forward` không phải gọi lại `ParamFn` cho mỗi nến
//!
//! Trước khi có `Session`, các biến này là **local của `forward`** nên hết
//! lời gọi là mất: không dừng giữa chừng rồi chạy tiếp được, và mỗi wrapper
//! (streaming realtime) phải copy lại cả vòng lặp. Với `Session`,
//! [`Portfolio::evaluate`](crate::portfolio::Portfolio::evaluate) chỉ cần
//! nhận session + hai [`FetchFn`](crate::FetchFn) là chạy tiếp được từ nến cuối.
//!
//! ## Vì sao `Session` mang cả trạng thái của `forward`
//!
//! [`Portfolio::forward`](crate::portfolio::Portfolio::forward) tiến **đúng
//! một nến** mỗi lời gọi, nên nó không giữ vòng lặp. Muốn gọi được lần thứ
//! hai thì phải biết "đang ở đâu" — đó là [`next_ts`](Session::next_ts) và
//! [`candle_ts`](Session::candle_ts). Nhờ vậy kernel không cần biết dữ liệu
//! đến từ loader, station hay array trong RAM: caller chỉ cần đóng `fetch`.
//!
//! Hằng số `kelly_fraction` / `base_capital` / `settlement` nằm ở đây vì cùng
//! lý do: chúng không đổi theo từng nến, mà `forward` cần chúng mỗi nến.

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
    /// Mốc thời gian của nến kế tiếp `forward` sẽ lấy. **0 = chưa khởi tạo** —
    /// lần `forward` đầu tiên sẽ lấy `from` do caller truyền.
    ///
    /// Đây là con trỏ duy nhất quyết định `forward` đi đâu; `candle_ts` chỉ
    /// để chặn xử lý lại nến đã thấy.
    pub next_ts: u64,
    /// Phân bổ vốn theo Kelly (param 0), resolve một lần.
    pub kelly_fraction: f64,
    /// Vốn gốc (param 1), resolve một lần.
    pub base_capital: f64,
    /// T+N: số nến phải chờ trước khi được đóng lệnh. `0` = theo thị trường.
    pub settlement: u64,
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

    /// Nạp hằng số tính toán từ `params` + calendar vào `Session`.
    ///
    /// Gọi một lần trước khi chạy, thay vì `forward` tự tra `ParamFn` mỗi nến.
    /// `settlement` ưu tiên `config.settlement_candles` khi khác 0, còn lại thuộc
    /// thị trường (`CryptoCalendar` → T+0, `StockCalendar` → T+3).
    pub fn prime(&mut self, config: &crate::portfolio::PortfolioConfig, calendar: &dyn crate::Calendar, params: crate::ParamFn<'_>) {
        const KELLY_FRACTION: usize = 0;
        const BASE_CAPITAL: usize = 1;

        self.kelly_fraction = params(KELLY_FRACTION);
        self.base_capital = params(BASE_CAPITAL);
        self.settlement = if config.settlement_candles > 0 {
            config.settlement_candles
        } else {
            calendar.settlement_candles()
        };
    }
}
