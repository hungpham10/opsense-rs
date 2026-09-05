mod calendar;
mod data_loader;
mod extractors;
mod fee;
mod graph;
mod jobs;
mod macros;
mod models;
mod opt_cache;
mod portfolio;
mod redis_sink;
mod redis_source;
mod sandboxies;
mod strategies;
mod telegram;

pub use calendar::{CryptoCalendar, ForexCalendar, StockCalendar};
pub use data_loader::{FromCsv, FromQueryCandleSticks};
/// Genotype DAG: `ops` = DNA alphabet, `nodes` = wiring. Compile sang ONNX
/// reusable làm Genotype cho ML/neuroevolution.
pub use extractors::OhlcvExtractor;
pub use fee::{
    DerivativeFee, MbsDerivativeFee, SimpleFixedFee, SsiDerivativeFee, VpsDerivativeFee,
};
pub use graph::{Graph, Graph as GraphV2, In, Node, Op};
pub use jobs::{
    BacktestExecutor, BacktestJobSpec, JOB_STREAM_KEY, JobEvent, JobEventKind, JobStatus,
    job_cancel_key, job_events_key, job_status_key, read_events, read_status, request_cancel,
    user_guard_key, write_status,
};
pub use models::{
    NUM_GRID_PARAMS, build_mean_reversion_onnx_bytes, build_mean_reversion_onnx_default,
    build_momentum_breakout_onnx_bytes, build_momentum_breakout_onnx_default,
    build_trend_follower_onnx_bytes, build_trend_follower_onnx_default,
};
pub use opt_cache::{OptCache, OptResult, default_opt_cache_path};
pub use portfolio::{DEFAULT_SETTLEMENT_CANDLES, Order, OrderType, Portfolio, Report};
pub use redis_sink::{RedisSink, RedisSinkMode};
pub use redis_source::{RedisSource, RedisSourceMode};
pub use strategies::{GridStrategy, VolatilityAdaptiveGridStrategy};
pub use telegram::{TelegramMessage, TelegramSink};

use std::fmt::Debug;
use std::io::Error;
use std::pin::Pin;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use analysis::TradingGrid;
use schemas::CandleStick;

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
///
/// Each extractor produces one or more `Vec<f64>` arrays from the same candle
/// slice.  Models compose a list of extractors and feed their concatenated
/// outputs (plus any learned weights) to the ONNX predictor.
///
/// # Padding
///
/// Extractors are responsible for producing fixed-size arrays when the model
/// requires them (e.g. [`OhlcvExtractor`] pads to its `window`).  Downstream
/// code simply collects whatever each extractor returns.
#[typetag::serde(tag = "type")]
pub trait Extractor: Debug + Send + Sync {
    /// Human-readable name (for debugging / logging).
    fn name(&self) -> &str;

    /// Extract feature vectors from a candle slice.
    ///
    /// Returns one or more `Vec<f64>` arrays.  The caller appends them to the
    /// full feature vector in order.
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

    /// Số cây nến phải chờ trước khi được phép đóng lệnh (T+N) theo loại thị trường.
    /// Mặc định 0 (T+0, không khóa) — chỉ thị trường chứng khoán (StockCalendar) trả 3 (T+3).
    fn settlement_candles(&self) -> u64 {
        0
    }
}
