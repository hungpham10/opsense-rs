mod candle;
mod calendar;
mod data_loader;
mod extractors;
mod fee;
mod graph;
mod ohcl;


mod portfolio;
mod grid;
mod strategies;
mod tick;

pub use candle::CandleStick;
pub use calendar::{CryptoCalendar, ForexCalendar, StockCalendar};
pub use data_loader::{FromCsv, FromQueryCandleSticks};
pub use grid::TradingGrid;
pub use opsense_libs::grid::{AnalysisGrid, SieveConfig};
pub use opsense_libs::transition::TransitionAnalysis;
pub use ohcl::QueryCandleSticks;
pub use tick::Tick;
/// Genotype DAG: `ops` = DNA alphabet, `nodes` = wiring. Compile sang ONNX
/// reusable làm Genotype cho ML/neuroevolution.
pub use extractors::OhlcvExtractor;
pub use fee::{
    DerivativeFee, MbsDerivativeFee, SimpleFixedFee, SsiDerivativeFee, VpsDerivativeFee,
};
pub use graph::{Graph, In, Node};
pub use graph::ops::*;


pub use portfolio::{DEFAULT_SETTLEMENT_CANDLES, Order, OrderType, Portfolio, Report};
pub use strategies::{GridStrategy, VolatilityAdaptiveGridStrategy};

/// Re-export runtime dưới `crate::vector::runtime` để macro `#[source]`/`#[sink]`/`#[transform]`
/// của opsense-macros mở rộng dùng URL như trong opsense-components.
pub mod vector {
    pub use opsense_libs::vector::runtime;
}

use std::fmt::Debug;
use std::io::Error;
use std::pin::Pin;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub type FetchFn<'a> = &'a mut (
            dyn FnMut(
    u64,
    u64,
) -> Pin<Box<dyn Future<Output = Result<Vec<CandleStick>, Error>> + Send + 'a>>
                + Send
                + Sync
        );

/// Mọi biến cố lệnh trong vòng forward — bắn qua `NotifyFn`, consumer tự lọc
/// loại mình quan tâm (vd TelegramSink chỉ xử lý `Closed`).
/// Snapshot slim của một grid tại thời điểm rebuild — chỉ giữ dải giá levels.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GridSnapshot {
    pub levels: Vec<f64>,
}

#[derive(Clone, Debug)]
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

pub type NotifyFn<'a> = &'a mut (
            dyn FnMut(OrderEvent) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>
                + Send
                + Sync
        );

pub type ParamFn<'a> = &'a (dyn Fn(usize) -> f64 + Send + Sync);

#[typetag::serde(tag = "loader")]
#[async_trait]
pub trait DataLoader: Sync + Send {
    async fn range(&self, from: u64, to: u64, resolution: &str) -> Result<Vec<CandleStick>, Error>;
}

/// Transforms candle data into one or more feature vectors.
#[typetag::serde(tag = "type")]
pub trait Extractor: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn extract(&self, candles: &[CandleStick]) -> Result<Vec<Vec<f64>>, Error>;
}

#[typetag::serde(tag = "type")]
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

#[typetag::serde(tag = "type")]
pub trait Fee: Send + Sync {
    fn rate(&self) -> f64;
}

#[typetag::serde(tag = "type")]
pub trait Score: Send + Sync {
    fn score(&self, report: &Report) -> f64;
}

#[typetag::serde(tag = "type")]
pub trait Calendar: Send + Sync {
    fn next(&self, current_ts: u64, resolution: &str) -> u64;
    fn settlement_candles(&self) -> u64 {
        0
    }
}
