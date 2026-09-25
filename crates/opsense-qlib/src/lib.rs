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
#[cfg(feature = "json")]
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
    fn rate(&self) -> f64;
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
