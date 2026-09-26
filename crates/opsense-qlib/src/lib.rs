mod calendar;
mod candle;
mod extractors;
mod fee;
mod grid;
mod portfolio;
mod score;
mod session;
mod tick;

pub use calendar::{CryptoCalendar, ForexCalendar, StockCalendar};
pub use candle::CandleStick;
pub use tick::Tick;

/// Genotype DAG: `ops` = DNA alphabet, `nodes` = wiring. Compile sang ONNX
/// reusable làm Genotype cho ML/neuroevolution.
pub use extractors::OhlcvExtractor;
pub use fee::{
    DerivativeFee, MbsDerivativeFee, SimpleFixedFee, SsiDerivativeFee, VpsDerivativeFee,
};
pub use grid::TradingGrid;

/// Plan dạng dữ liệu cho strategy viết bằng script (Rhai) — xem [`plan`].
///
/// Bật bằng feature `rhai` (đường script) hoặc `json` (đường DAG/typetag).
#[cfg(any(feature = "json", feature = "rhai"))]
pub mod plan;

#[cfg(feature = "graph")]
mod graph;

pub use portfolio::{
    DEFAULT_SETTLEMENT_CANDLES, Order, OrderType, Portfolio, PortfolioConfig, Report,
};
pub use score::{NetPnlScore, SharpeScore};
pub use session::Session;

#[cfg(feature = "graph")]
pub use graph::{Graph, In, Node, ops::*};

#[cfg(feature = "json")]
pub use opsense_mlib::grid::{AnalysisGrid, SieveConfig};

#[cfg(feature = "json")]
pub use opsense_mlib::transition::TransitionAnalysis;

/// Re-export runtime dưới `crate::vector::runtime` để macro `#[source]`/`#[sink]`/`#[transform]`
/// của opsense-macros mở rộng dùng URL như trong opsense-components.
#[cfg(feature = "json")]
pub mod vector {
    pub use opsense_mlib::vector::runtime;
}

use std::fmt::Debug;
use std::io::Error;
use std::pin::Pin;

use async_trait::async_trait;

#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

/// Fetch candles cho một range bất kỳ, bất kỳ lúc nào kernel cần (rebuild,
/// vòng lặp trade, bước realtime…).
///
/// Future trả về là `'static`, nên cùng một `FetchFn` **reborrow** được cho
/// lifetime ngắn hơn — nhờ vậy truyền vào hàm lồng nhau
/// (`evaluate` → `rebuild_strategy` → `Strategy::rebuild`) không đụng nhau.
/// Đổi lại closure phải tự sở hữu dữ liệu nó capture (clone `Arc`, `String`)
/// thay vì mượn từ scope bên ngoài.
pub type FetchFn<'a> = &'a mut (dyn FnMut(
    u64,
    u64,
) -> Pin<Box<dyn Future<Output = Result<Vec<CandleStick>, Error>> + Send + 'static>>
                + Send
                + Sync
        + 'a);

/// Mọi biến cố lệnh trong vòng forward — bắn qua `NotifyFn`, consumer tự lọc
/// loại mình quan tâm (vd TelegramSink chỉ xử lý `Closed`).
/// Snapshot slim của một grid tại thời điểm rebuild — chỉ giữ dải giá levels.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct GridSnapshot {
    pub levels: Vec<f64>,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub enum OrderEvent {
    /// Lệnh vừa được đặt (đã nằm trong `orders` mở)
    Placed { ts: u64, order: Order },
    /// Lệnh vừa bị đóng (đã nằm trong `history`)
    Closed { ts: u64, order: Order },
    /// Strategy vừa rebuild plan — snapshot dải giá grid mới (grid history)
    Rebuilt { ts: u64, grids: Vec<GridSnapshot> },
    /// Lệnh bị lọc, không bao giờ được đặt (vd lợi nhuận kỳ vọng <= phí)
    Rejected {
        ts: u64,
        grid: usize,
        level: usize,
        reason: String,
    },
}

/// Báo biến cố ra ngoài trong lúc kernel chạy. Future `'static` (như
/// [`FetchFn`]) nên cùng một `NotifyFn` reborrow được cho lifetime ngắn hơn —
/// kernel gọi nó ở nhiều chỗ, kể cả bên trong vòng lặp và trong hàm lồng nhau.
pub type NotifyFn<'a> = &'a mut (
            dyn FnMut(OrderEvent) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>
                + Send
                + Sync
        + 'a);

pub type ParamFn<'a> = &'a (dyn Fn(usize) -> f64 + Send + Sync);

#[async_trait]
pub trait DataLoader: Sync + Send {
    async fn range(&self, from: u64, to: u64, resolution: &str) -> Result<Vec<CandleStick>, Error>;
}

/// Transforms candle data into one or more feature vectors.
#[cfg_attr(feature = "json", typetag::serde(tag = "type"))]
pub trait Extractor: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn extract(&self, candles: &[CandleStick]) -> Result<Vec<Vec<f64>>, Error>;
}

#[cfg_attr(feature = "json", typetag::serde(tag = "type"))]
#[async_trait]
pub trait Strategy: Sync + Send {
    fn init(&self) -> Vec<f64>;
    async fn next(&self, current: u64) -> u64;
    async fn rebuild(
        &self,
        current_ts: u64,
        grids: &[TradingGrid],
        fetch: FetchFn<'_>,
        param: ParamFn<'_>,
    ) -> Result<Vec<TradingGrid>, Error>;
}

#[cfg_attr(feature = "json", typetag::serde(tag = "type"))]
pub trait Fee: Send + Sync {
    /// Phí **một chiều** (một lần khớp lệnh), dạng fraction của giá trị.
    fn rate(&self) -> f64;

    /// Phí **trọn vòng**: mở + đóng. Một lượt giao dịch là HAI lệnh (vào, ra)
    /// nên phí trả hai lần.
    ///
    /// Mặc định `2 × rate()`. Impl có phí khác nhau theo chiều (ví dụ vào là
    /// maker, ra là taker) thì override — đó là lý do nó ở trait chứ không phải
    /// một hằng số `2.0` rải rác trong code.
    ///
    /// **Đây là nguồn duy nhất** cho ngưỡng lãi: cổng vào lệnh và sổ sách khi
    /// đóng đều phải dùng nó, không hardcode `2.0 * fee`. Trước đây cổng dùng
    /// `rate()` (một phí) còn PnL dùng `2.0 * rate()` ⇒ kernel mở lệnh rồi tự
    /// ghi sổ là lỗ.
    fn round_trip_rate(&self) -> f64 {
        self.rate() * 2.0
    }

    /// Lợi nhuận ròng sau phí trọn vòng, từ biên độ giá thô.
    fn net_pnl_pct(&self, gross_pnl_pct: f64) -> f64 {
        gross_pnl_pct - self.round_trip_rate()
    }
}

#[cfg_attr(feature = "json", typetag::serde(tag = "type"))]
pub trait Score: Send + Sync {
    fn score(&self, report: &Report) -> f64;
}

#[cfg_attr(feature = "json", typetag::serde(tag = "type"))]
pub trait Calendar: Send + Sync {
    fn next(&self, current_ts: u64, resolution: &str) -> u64;
    fn settlement_candles(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod fee_trait_tests {
    use super::Fee;
    use crate::fee::SimpleFixedFee;

    /// Ngưỡng lọc lệnh vào và sổ sách khi đóng phải dùng **cùng một** con số.
    ///
    /// Trước đây cổng vào so với `rate()` (một phí) còn PnL trừ `2.0 × rate()`
    /// hardcode ⇒ kernel mở lệnh rồi tự ghi sổ là lỗ. Test này khoá lại: cùng
    /// một `gross`, `net_pnl_pct` phải cho kết quả **âm** khi biên độ nằm giữa
    /// một phí và hai phí.
    #[test]
    fn entry_gate_and_ledger_agree_on_round_trip_fee() {
        let fee = SimpleFixedFee::new(0.001);
        let one_side = 0.001;
        let round_trip = fee.round_trip_rate();

        assert!((round_trip - 2.0 * one_side).abs() < 1e-12);

        // Biên độ 0.15%: lọt qua cổng cũ (một phí) nhưng lỗ thật (hai phí).
        let gross = 0.0015;
        assert!(gross > one_side, "cổng cũ sẽ cho qua");
        assert!(
            fee.net_pnl_pct(gross) < 0.0,
            "sổ sách phải ghi lỗ: net={}",
            fee.net_pnl_pct(gross)
        );

        // Biên độ 0.25%: qua cổng mới và lãi.
        let good = 0.0025;
        assert!(good > round_trip, "cổng mới phải cho qua");
        assert!(fee.net_pnl_pct(good) > 0.0);
    }
}
