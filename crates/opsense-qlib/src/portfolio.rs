use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};

use crate::candle::CandleStick;
use crate::grid::TradingGrid;
use opsense_mlib::sgd::SGDOptimizer;

#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

use super::calendar::to_timestamp_secs;
use super::session::Session;
use super::{
    Calendar, DataLoader, Fee, FetchFn, GridSnapshot, NotifyFn, OrderEvent, ParamFn, Score,
    Strategy,
};

// ═══════════════════════════════════════════════════════════════════════════
// Caching — weekly-block LRU (dùng opsense_mlib::lru::LruCache)
//
// Pattern từ ohcl.rs: chia time thành weekly blocks, mỗi block track
// `covered_first`/`covered_last` là actual candle extents. Cache HIT khi
// requested range nằm gọn trong covered range của các block liên quan.
//
// Tránh được false HIT khi API trả về data bị lag: block cuối cùng chỉ
// claim coverage cho khoảng data thực tế nhận được, không mở rộng theo
// request boundary.
// ═══════════════════════════════════════════════════════════════════════════

const BLOCK_SECS: i64 = 7 * 24 * 60 * 60; // 1 tuần

/// Một weekly block chứa candles đã fetch, với actual covered range.
#[derive(Clone)]
struct BlockCache {
    candles: Vec<CandleStick>,
    /// Timestamp nến đầu tiên trong block này (actual)
    covered_first: u64,
    /// Timestamp nến cuối cùng + 1 trong block này (actual, exclusive)
    covered_last: u64,
}

impl BlockCache {
    /// Extract candles trong [from, to) từ block này.
    fn subrange(&self, from: u64, to: u64) -> Vec<CandleStick> {
        let start = self.candles.partition_point(|c| (c.t as u64) < from);
        let end = self.candles.partition_point(|c| (c.t as u64) < to);
        self.candles[start..end].to_vec()
    }
}

/// LRU cache cho một cache key (ví dụ "1H:analysis"), keyed by block_id.
type BlockLru = opsense_mlib::lru::LruCache<i64, BlockCache, 32>;

/// Future một fetch closure trả về (`'static` — closure tự sở hữu capture).
type LoaderFuture = Pin<Box<dyn Future<Output = Result<Vec<CandleStick>, Error>> + Send + 'static>>;

// Helper functions.
// Đây là free functions để tránh borrow-checker issues với self.
// Logic tương tự ohcl.rs::fetch_from_cache.

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(i32)]
#[derive(Default)]
pub enum OrderType {
    #[default]
    Unknown,
    Long,
    Short,
}

impl From<i32> for OrderType {
    fn from(value: i32) -> Self {
        match value {
            1 => OrderType::Long,
            2 => OrderType::Short,
            _ => OrderType::Unknown,
        }
    }
}

impl From<String> for OrderType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "long" => OrderType::Long,
            "short" => OrderType::Short,
            _ => OrderType::Unknown,
        }
    }
}

impl Display for OrderType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            OrderType::Long => write!(f, "long"),
            OrderType::Short => write!(f, "short"),
            OrderType::Unknown => write!(f, "unknown"),
        }
    }
}

#[cfg(feature = "json")]
impl<'de> Deserialize<'de> for OrderType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(OrderType::from(s))
    }
}

#[cfg(feature = "json")]
impl Serialize for OrderType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct Order {
    // @NOTE: setup
    pub dtype: OrderType,
    pub entry_price: f64,
    pub size: f64,
    pub sl_price: f64,
    pub tp_price: f64,
    pub grid_index: usize,
    pub level_index: usize,

    // @NOTE: outcome
    pub pnl_pct: Option<f64>,
    pub exit_price: Option<f64>,

    /// T+N: chỉ cho phép đóng lệnh khi `candle_seq >= unlock_seq`.
    /// Lưu số thứ tự nến (toàn cục) được phép đóng. 0 = không giới hạn.
    pub unlock_seq: u64,
}

#[derive(Default, Clone, Copy, Debug)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct Report {
    // Tổng quan
    pub total_trades: usize,
    pub win_rate: f64,     // %
    pub net_pnl_pct: f64,  // %
    pub total_return: f64, // %

    // Risk-Adjusted Metrics
    pub profit_factor: f64,
    pub expectancy: f64, // % PnL trung bình / lệnh
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,

    // Drawdown & Rủi ro
    pub max_drawdown_pct: f64, // %
    pub max_consecutive_losses: usize,

    // Chi tiết Lời/Lỗ
    pub avg_win_pct: f64,    // %
    pub avg_loss_pct: f64,   // %
    pub win_loss_ratio: f64, // Avg Win / Avg Loss

    // Phân tách Long/Short (Đánh giá Model Bias)
    pub long_trades: usize,
    pub long_win_rate: f64, // %
    pub short_trades: usize,
    pub short_win_rate: f64, // %

    /// PnL tuyệt đối (size-weighted, đơn vị tiền). `size` phụ thuộc head
    /// weights (w/b qua win probabilities) nên field này làm reward phân biệt
    /// được w/b — SGD oracle fit w/b đúng nghĩa. `#[serde(default)]` để report
    /// cũ (web service) deserialize không vỡ.
    pub net_pnl_abs: f64,
}

impl Display for Report {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(f, "  ── Report ─────────────────────────────────────────")?;
        writeln!(f, "  Trades        : {}", self.total_trades)?;
        writeln!(f, "  Win Rate      : {:.2}%", self.win_rate)?;
        writeln!(f, "  Net PnL       : {:.2}%", self.net_pnl_pct)?;
        writeln!(f, "  Net PnL (abs) : {:.2}", self.net_pnl_abs)?;
        writeln!(f, "  Profit Factor : {:.2}", self.profit_factor)?;
        writeln!(f, "  Expectancy    : {:.2}%", self.expectancy)?;
        writeln!(f, "  Sharpe        : {:.4}", self.sharpe_ratio)?;
        writeln!(f, "  Sortino       : {:.4}", self.sortino_ratio)?;
        writeln!(f, "  Calmar        : {:.4}", self.calmar_ratio)?;
        writeln!(f, "  Max DD        : {:.2}%", self.max_drawdown_pct)?;
        writeln!(f, "  Max Cons Loss : {}", self.max_consecutive_losses)?;
        write!(
            f,
            "  Long/Short    : {}/{} ({:.1}% / {:.1}%)",
            self.long_trades, self.short_trades, self.long_win_rate, self.short_win_rate,
        )
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct Portfolio {
    /// define environment
    #[cfg_attr(feature = "json", serde(skip, default = "default_loader"))]
    loader: Arc<dyn DataLoader + Sync + Send>,
    strategy: Arc<dyn Strategy + Sync + Send>,
    score: Arc<dyn Score + Sync + Send>,
    fee: Arc<dyn Fee + Sync + Send>,
    calendar: Arc<dyn Calendar + Sync + Send>,

    /// configure
    try_random_search: usize,
    config: PortfolioConfig,

    /// Per-resolution LRU block cache. Skip serialize.
    #[cfg_attr(feature = "json", serde(skip, default = "default_block_cache"))]
    cache: Arc<RwLock<HashMap<String, BlockLru>>>,
}

/// Cấu hình chạy của [`Portfolio`] — phần bất biến, không phụ thuộc runtime
/// (`loader`/`cache` là runtime nên không khai ở đây).
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct PortfolioConfig {
    /// Resolution của nến giao dịch (stream nến đưa vào kernel).
    pub resolution_for_test: String,
    /// Resolution dùng cho phân tích/rebuild (thường rộng hơn `resolution_for_test`).
    pub resolution_for_rebuild: String,

    /// T+N: số cây nến phải chờ trước khi được phép đóng lệnh.
    /// 0 = theo thị trường (`Calendar::settlement_candles`).
    pub settlement_candles: u64,

    /// Dùng weekly-block LRU cache nội bộ không.
    ///
    /// `true` (mặc định) — backtest: cache cắt hẳn áp lực `DataLoader::range`
    /// vì rebuild cần đọc lại cửa sổ lookback mỗi chu kỳ review.
    /// `false` — mọi range đi thẳng ra `DataLoader` (bỏ cả prefetch lẫn
    /// cache read/write); hợp khi loader rẻ và stateless (station, file local)
    /// hoặc khi chạy một lần trên dữ liệu đã nằm sẵn trong RAM.
    pub cache_enabled: bool,
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            resolution_for_test: "1m".to_string(),
            resolution_for_rebuild: "1D".to_string(),
            settlement_candles: DEFAULT_SETTLEMENT_CANDLES,
            cache_enabled: true,
        }
    }
}

/// Mặc định T+0 (không khóa). Khi truyền `0` vào `Portfolio::new`, engine dùng
/// T+N theo thị trường (StockCalendar → T+3, còn lại → T+0). Truyền giá trị >0 để ép T+N.
pub const DEFAULT_SETTLEMENT_CANDLES: u64 = 0;

#[cfg(feature = "json")]
fn default_block_cache() -> Arc<RwLock<HashMap<String, BlockLru>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

/// DataLoader rỗng — default của field `loader` khi `Portfolio` được
/// deserialize (loader là runtime, chỉ `Portfolio::new` gán loader thật).
#[cfg(feature = "json")]
#[derive(Default)]
struct EmptyDataLoader;

#[cfg(feature = "json")]
#[async_trait::async_trait]
impl DataLoader for EmptyDataLoader {
    async fn range(
        &self,
        _from: u64,
        _to: u64,
        _resolution: &str,
    ) -> Result<Vec<CandleStick>, Error> {
        Ok(Vec::new())
    }
}

#[cfg(feature = "json")]
fn default_loader() -> Arc<dyn DataLoader + Sync + Send> {
    Arc::new(EmptyDataLoader)
}

impl Portfolio {
    pub fn new(
        loader: Arc<dyn DataLoader + Sync + Send>,
        strategy: Arc<dyn Strategy + Sync + Send>,
        fee: Arc<dyn Fee + Sync + Send>,
        score: Arc<dyn Score + Sync + Send>,
        calendar: Arc<dyn Calendar + Sync + Send>,
        config: PortfolioConfig,
    ) -> Result<Self, Error> {
        Ok(Self {
            loader,
            strategy,
            fee,
            score,
            calendar,
            try_random_search: 30,
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn optimize(
        &self,
        optimizer: &SGDOptimizer,
        lookback: u64,
        windows: &[(u64, u64)],
    ) -> Result<(Vec<f64>, Vec<f64>), Error> {
        assert!(!windows.is_empty(), "optimize: need at least one window");

        // ── Phase 1: Random search (30 trials) — song song ────────────
        let bounds = optimizer.bounds().to_vec();
        let is_integer = optimizer.is_integer().to_vec();
        let n_params = bounds.len();

        // Pre-generate tất cả trial params
        let mut rng = rand::thread_rng();
        let trials = (0..self.try_random_search)
            .map(|_| {
                (0..n_params)
                    .map(|i| {
                        let (lo, hi) = bounds[i];
                        let mut v = if lo.is_finite() && hi.is_finite() {
                            rng.gen_range(lo..=hi)
                        } else {
                            lo
                        };
                        if is_integer[i] {
                            v = v.round();
                        }
                        v
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        drop(rng);

        // Warm cache covering ALL windows
        self.prefetch_cache(
            bounds
                .last()
                .map(|&(_, hi)| (hi as u64).max(lookback))
                .unwrap_or(lookback),
            windows.iter().map(|&(f, _)| f).min().unwrap(),
            windows.iter().map(|&(_, t)| t).max().unwrap(),
        )
        .await?;

        // Random search trên window đầu tiên
        let mut handles = Vec::with_capacity(self.try_random_search);
        for trial in &trials {
            let pf = self.clone();
            let trial = trial.clone();
            let (from, to) = windows[0];

            handles.push(tokio::spawn(async move {
                let mut session = Session::new();
                match pf
                    .backtest(
                        &mut session,
                        lookback,
                        from,
                        to,
                        &|id| trial[id],
                        &mut |_| Box::pin(async move { Ok(()) }),
                    )
                    .await
                {
                    Ok((_, report)) => report,
                    Err(_) => Report::default(),
                }
            }));
        }

        let mut best_params = self.strategy.init();
        let mut best_score = f64::NEG_INFINITY;

        for (handle, trial) in handles.into_iter().zip(trials) {
            // Trial panic (NaN từ graph, ...) → bỏ qua trial, không làm chết
            // cả optimize (trước đây `.unwrap()` lan panic ra ngoài).
            let report = handle.await.unwrap_or_default();
            let score = self.score.score(&report);
            if score > best_score {
                best_score = score;
                best_params = trial;
            }
        }

        // ── Phase 2: SGD từ best params — multi-window objective ─────
        // Cần 'static closure cho tokio::spawn bên trong sgd.rs
        let sgd_self = Arc::new(self.clone());
        let win_list: Vec<(u64, u64)> = windows.to_vec();
        Ok(optimizer
            .optimize(
                {
                    let sgd_self = sgd_self.clone();
                    let win_list = win_list.clone();
                    move |params: &[f64]| {
                        let sgd_self = sgd_self.clone();
                        let params = params.to_vec();
                        let win = win_list.clone();
                        async move {
                            let n = win.len();
                            let mut total = 0.0f64;

                            for &(eval_from, eval_to) in &win {
                                let mut session = Session::new();

                                if let Ok((score, _)) = sgd_self
                                    .backtest(
                                        &mut session,
                                        lookback,
                                        eval_from,
                                        eval_to,
                                        &|id| params[id],
                                        &mut |_| Box::pin(async move { Ok(()) }),
                                    )
                                    .await
                                {
                                    total += score;
                                }
                            }

                            total / n as f64
                        }
                    }
                },
                &best_params,
            )
            .await)
    }

    /// Backtest entry: warm cache (nếu bật) rồi chạy kernel
    /// [`Self::evaluate`] với fetch đóng từ `self.loader` / block cache.
    ///
    /// Đây là đường backtest: có prefetch + LRU + calendar alignment, và mỗi
    /// trial SGD chạy trên `Session` riêng. Realtime thì gọi thẳng
    /// [`Self::evaluate`] với fetch đóng từ station.
    #[allow(clippy::too_many_arguments)]
    pub async fn backtest(
        &self,
        session: &mut Session,
        lookback: u64,
        from: u64,
        to: u64,
        params: ParamFn<'_>,
        notify: NotifyFn<'_>,
    ) -> Result<(f64, Report), Error> {
        if self.config.cache_enabled {
            self.prefetch_cache(lookback, from, to).await?;
        }

        let mut trade = self.loader_fetch(&self.config.resolution_for_test, "trade");
        let mut analysis = self.loader_fetch(&self.config.resolution_for_rebuild, "analysis");

        self.evaluate(
            session,
            from,
            to,
            params,
            &mut *trade,
            &mut *analysis,
            notify,
        )
        .await
    }

    /// Kernel: đưa nến vào `session`, mỗi nến một bước (check exit → rebuild
    /// nếu tới hạn → evaluate entry). Không tự I/O: nến đến từ `fetch`, dữ
    /// liệu phân tích đến từ `analysis_fetch` — kernel không biết dữ liệu nằm
    /// ở loader/cache hay station.
    ///
    /// Có thể gọi nhiều lần trên cùng `Session` để **chạy tiếp**: `session.review_at`
    /// / `candle_seq` / `orders` / `plan` đều mang qua lời gọi, và
    /// `session.candle_ts` chặn xử lý lại nến đã thấy.
    ///
    /// `fetch` và `analysis_fetch` tách riêng vì backtest dùng 2 resolution
    /// (trade = `resolution_for_test`, rebuild = `resolution_for_rebuild`).
    #[allow(clippy::too_many_arguments)]
    pub async fn evaluate(
        &self,
        session: &mut Session,
        from: u64,
        to: u64,
        params: ParamFn<'_>,
        fetch: FetchFn<'_>,
        analysis_fetch: FetchFn<'_>,
        notify: NotifyFn<'_>,
    ) -> Result<(f64, Report), Error> {
        const KELLY_FRACTION: usize = 0;
        const BASE_CAPITAL: usize = 1;

        let resolution = self.config.resolution_for_test.clone();
        let kelly_fraction = params(KELLY_FRACTION);
        let base_capital = params(BASE_CAPITAL);
        let fee_rate = self.fee.rate();

        // T+N: `settlement_candles == 0` → theo thị trường (StockCalendar → T+3,
        // Crypto/Forex → T+0); giá trị >0 → ép T+N cố định.
        let settlement = if self.config.settlement_candles > 0 {
            self.config.settlement_candles
        } else {
            self.calendar.settlement_candles()
        };

        // Advance to first valid trading time (tránh rebuild ở ngoài giờ giao dịch,
        // khiến strategy fetch data không có nến → ATR/analysis fail).
        let mut current = {
            let step_ts = to_timestamp_secs(&resolution);
            let first_valid = self
                .calendar
                .next(from.saturating_sub(step_ts), &resolution);
            if first_valid > from && first_valid < to {
                first_valid
            } else {
                from
            }
        };

        while current < to {
            if session.review_at <= current {
                #[cfg(debug_assertions)]
                let t_rebuild = std::time::Instant::now();

                match self
                    .rebuild_strategy(
                        current,
                        session.plan.as_slice(),
                        &mut *analysis_fetch,
                        params,
                    )
                    .await
                {
                    Ok((review, plan)) => {
                        session.review_at = review;
                        session.plan = plan;

                        notify(OrderEvent::Rebuilt {
                            ts: current,
                            grids: session
                                .plan
                                .iter()
                                .map(|g| GridSnapshot {
                                    levels: g.levels().to_vec(),
                                })
                                .collect(),
                        })
                        .await?;

                        #[cfg(debug_assertions)]
                        println!(
                            "  [debug] evaluate: rebuild at {}  next review={}  took {:.0}ms",
                            current,
                            session.review_at,
                            t_rebuild.elapsed().as_secs_f64() * 1000.0,
                        );
                    }
                    Err(error) => {
                        // Rebuild fail (chưa đủ data, market closed, …) → giữ plan
                        // cũ và **tiến `review_at`**, nếu không realtime sẽ thử
                        // lại ở mọi nến. Session vẫn nhất quán: chỉ mốc thời gian
                        // đổi, không mất lệnh/plan.
                        session.review_at = self.strategy.next(current).await;
                        return Err(error);
                    }
                }

                session.candle_id = 0;
            }

            let next = std::cmp::min(session.review_at, to);
            #[cfg(debug_assertions)]
            let t_fetch = std::time::Instant::now();
            let candles = fetch(current, next).await?;
            #[cfg(debug_assertions)]
            if t_fetch.elapsed().as_secs_f64() * 1000.0 > 50.0 {
                println!(
                    "  [debug] evaluate: fetch [{}, {})  got {} candles  took {:.0}ms",
                    current,
                    next,
                    candles.len(),
                    t_fetch.elapsed().as_secs_f64() * 1000.0,
                );
            }

            for candle in &candles {
                if candle.t <= session.candle_ts {
                    continue;
                }
                session.candle_seq += 1;
                let current_seq = session.candle_seq;

                let prev_hist_len = session.history.len();

                // `plan`/`orders` đưa ra local rồi ghi lại cuối nến — giữ nguyên
                // cách `forward` cũ làm, tránh giữ borrow `session` qua `.await`.
                let mut plan = std::mem::take(&mut session.plan);
                let mut orders = std::mem::take(&mut session.orders);

                orders.retain_mut(|order| {
                    if let Some((exit_price, pnl_pct)) =
                        Self::check_order_exit(order, candle, fee_rate, current_seq)
                    {
                        order.exit_price = Some(exit_price);
                        order.pnl_pct = Some(pnl_pct);

                        if let Some(grid) = plan.get_mut(order.grid_index) {
                            grid.record_trade_outcome(
                                order.level_index,
                                order.dtype == OrderType::Long,
                                pnl_pct,
                            );
                        }

                        session.history.push(*order);
                        false
                    } else {
                        true
                    }
                });

                // Notify về các lệnh vừa đóng trong nến này
                for order in &session.history[prev_hist_len..] {
                    notify(OrderEvent::Closed {
                        ts: candle.t.max(0) as u64,
                        order: *order,
                    })
                    .await?;
                }

                // Ngưỡng mở khóa = thứ tự nến hiện tại + N (T+N)
                let unlock_seq = current_seq + settlement;
                let events = Self::evaluate_grid_entries(
                    session.candle_id,
                    candle,
                    &plan,
                    &mut orders,
                    fee_rate,
                    kelly_fraction,
                    base_capital,
                    unlock_seq,
                );

                // Notify về lệnh vừa đặt / bị từ chối
                for event in events {
                    notify(event).await?;
                }

                session.plan = plan;
                session.orders = orders;
                session.candle_id += 1;
            }

            // ── Advance simulation time ───────────────────────────────
            current = if !candles.is_empty() {
                next
            } else {
                self.calendar.next(current, &resolution)
            };

            if !candles.is_empty() {
                session.candle_ts = candles.last().map_or(session.candle_ts, |c| c.t);
            }
        }

        let report = Self::convert_order_history_into_report(&session.history);
        Ok((self.score.score(&report), report))
    }

    /// Fetch closure đóng quanh `self.loader` + block cache + calendar: kernel
    /// gọi bất kỳ lúc nào với range bất kỳ, không cần biết nguồn dữ liệu.
    ///
    /// Closure clone sẵn `Arc`/`String` nên future `'static` — thỏa [`FetchFn`],
    /// tức kernel có thể reborrow nó cho `rebuild_strategy`.
    fn loader_fetch(
        &self,
        resolution: &str,
        kind: &'static str,
    ) -> Box<dyn FnMut(u64, u64) -> LoaderFuture + Send + Sync + '_> {
        let loader = self.loader.clone();
        let cache = self.cache.clone();
        let calendar = self.calendar.clone();
        let resolution = resolution.to_string();
        let cache_key = format!("{resolution}:{kind}");
        let cache_enabled = self.config.cache_enabled;

        Box::new(move |from: u64, to: u64| {
            let loader = loader.clone();
            let cache = cache.clone();
            let calendar = calendar.clone();
            let resolution = resolution.clone();
            let cache_key = cache_key.clone();
            Box::pin(async move {
                Self::fetch_candles_from_loader(
                    &loader,
                    &cache,
                    &calendar,
                    from,
                    to,
                    &resolution,
                    &cache_key,
                    cache_enabled,
                )
                .await
            })
        })
    }


    /// Pre-fetch trade + analysis data into cache before simulation loop runs.
    /// Avoids cache MISS/EXTEND during rebuild since data is already warmed.
    ///
    /// Uses `calendar` to only fetch trading hours (skip weekends/after-hours for StockCalendar, etc.)
    /// Fetches full weekly blocks aligned to BLOCK_SECS boundaries for optimal cache coverage.
    #[inline]
    async fn prefetch_cache(&self, lookback: u64, from: u64, to: u64) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::other(e.to_string()))?
            .as_secs();
        let safe_to = if to <= now { to } else { now };

        // Helper: adjust 'from' to first valid trading timestamp using calendar.
        // Returns None if the entire range is outside trading hours.
        let adjust_from = |from: u64, resolution: &str| -> Option<u64> {
            let step = to_timestamp_secs(resolution);
            let valid = self.calendar.next(from.saturating_sub(step), resolution);
            if valid < safe_to { Some(valid) } else { None }
        };

        // ── Trade data ─────────────────────────────────────────────────
        let cache_key_trade = format!("{}:trade", self.config.resolution_for_test);
        if let Some(adj_from) = adjust_from(from, &self.config.resolution_for_test) {
            #[cfg(debug_assertions)]
            if adj_from != from {
                println!(
                    "  [debug] calendar adjusted trade fetch: {} → {}  (trading range)",
                    from, adj_from
                );
            }
            Self::fetch_candles_from_loader(
                &self.loader,
                &self.cache,
                &self.calendar,
                adj_from,
                safe_to,
                &self.config.resolution_for_test,
                &cache_key_trade,
                self.config.cache_enabled,
            )
            .await?;
        } else {
            #[cfg(debug_assertions)]
            println!(
                "  [debug] calendar skipped trade fetch [{}, {}) — outside trading hours",
                from, safe_to
            );
        }

        // ── Analysis / rebuild data ────────────────────────────────────
        let cache_key_rebuild = format!("{}:analysis", self.config.resolution_for_rebuild);
        let analysis_from = from.saturating_sub(lookback);
        if analysis_from < safe_to {
            if let Some(adj_from) = adjust_from(analysis_from, &self.config.resolution_for_rebuild) {
                #[cfg(debug_assertions)]
                if adj_from != analysis_from {
                    println!(
                        "  [debug] calendar adjusted analysis fetch: {} → {}  (trading range)",
                        analysis_from, adj_from
                    );
                }
                Self::fetch_candles_from_loader(
                    &self.loader,
                    &self.cache,
                    &self.calendar,
                    adj_from,
                    safe_to,
                    &self.config.resolution_for_rebuild,
                    &cache_key_rebuild,
                    self.config.cache_enabled,
                )
                .await?;
            } else {
                #[cfg(debug_assertions)]
                println!(
                    "  [debug] calendar skipped analysis fetch [{}, {}) — outside trading hours",
                    analysis_from, safe_to
                );
            }
        }

        Ok(())
    }

    /// Rebuild strategy plan: gọi `Strategy::next` + `Strategy::rebuild` với
    /// `fetch` do kernel truyền vào — strategy tự fetch range lookback nó cần,
    /// không cần biết nguồn dữ liệu.
    #[inline]
    pub(crate) async fn rebuild_strategy(
        &self,
        current: u64,
        plan: &[TradingGrid],
        fetch: FetchFn<'_>,
        params: ParamFn<'_>,
    ) -> Result<(u64, Vec<TradingGrid>), Error> {
        #[cfg(debug_assertions)]
        let t = std::time::Instant::now();
        let review = self.strategy.next(current).await;

        let plan = self.strategy.rebuild(current, plan, fetch, params).await?;

        #[cfg(debug_assertions)]
        if t.elapsed().as_secs_f64() * 1000.0 > 1000.0 {
            println!(
                "  [debug] rebuild at {}: {} grids, {:.0}ms",
                current,
                plan.len(),
                t.elapsed().as_secs_f64() * 1000.0
            );
        }

        Ok((review, plan))
    }

    /// Duyệt các grid level để tìm cơ hội entry mới từ một candle.
    /// Trả về các biến cố (Placed/Rejected) để vòng forward notify ra ngoài.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_grid_entries(
        id: usize,
        candle: &CandleStick,
        plan: &[TradingGrid],
        orders: &mut Vec<Order>,
        fee_rate: f64,
        kelly_fraction: f64,
        base_capital: f64,
        unlock_seq: u64,
    ) -> Vec<OrderEvent> {
        let ts = candle.t.max(0) as u64;
        let mut events = Vec::new();

        for (ig, grid) in plan.iter().enumerate() {
            if candle.h < grid.min() || candle.l > grid.max() {
                continue;
            }

            for il in 0..grid.num_levels() {
                let entry_price = grid.level_price(il);

                if entry_price >= candle.l && entry_price <= candle.h {
                    if orders
                        .iter()
                        .any(|o| o.grid_index == ig && o.level_index == il)
                    {
                        continue;
                    }

                    let (dtype, win_p, sl_price, tp_price) =
                        if entry_price <= (grid.min() + grid.max()) / 2.0 {
                            (
                                OrderType::Long,
                                grid.long_win_pct(il) * grid.weight(il, id),
                                grid.sl_long(il),
                                grid.tp_above(il),
                            )
                        } else {
                            (
                                OrderType::Short,
                                grid.short_win_pct(il) * grid.weight(il, id),
                                grid.sl_short(il),
                                grid.tp_below(il),
                            )
                        };

                    let expected_profit_pct = if dtype == OrderType::Long {
                        (tp_price - entry_price) / entry_price
                    } else {
                        (entry_price - tp_price) / entry_price
                    };

                    if expected_profit_pct <= fee_rate {
                        events.push(OrderEvent::Rejected {
                            ts,
                            grid: ig,
                            level: il,
                            reason: format!(
                                "expected_profit_pct {expected_profit_pct:.6} <= fee {fee_rate:.6}"
                            ),
                        });
                        continue;
                    }

                    let order = Order {
                        size: Self::calculate_order_size(
                            win_p,
                            grid.stoploss_pct(),
                            kelly_fraction,
                            base_capital,
                        ),
                        grid_index: ig,
                        level_index: il,
                        dtype,
                        entry_price,
                        sl_price,
                        tp_price,
                        unlock_seq,
                        ..Default::default()
                    };

                    orders.push(order);
                    events.push(OrderEvent::Placed { ts, order });
                }
            }
        }

        events
    }

    #[inline]
    pub fn check_order_exit(
        order: &Order,
        candle: &CandleStick,
        fee_rate: f64,
        current_seq: u64,
    ) -> Option<(f64, f64)> {
        // T+N: chưa đủ N nến thì không được đóng lệnh (giữ nguyên trạng thái mở)
        if current_seq < order.unlock_seq {
            return None;
        }

        let should_exit = match order.dtype {
            OrderType::Long => candle.l <= order.sl_price || candle.h >= order.tp_price,
            OrderType::Short => candle.h >= order.sl_price || candle.l <= order.tp_price,
            OrderType::Unknown => false,
        };

        if !should_exit {
            return None;
        }

        let exit_price = match order.dtype {
            OrderType::Long => {
                let hit_sl = candle.l <= order.sl_price;
                let hit_tp = candle.h >= order.tp_price;

                if hit_sl {
                    if candle.o < order.sl_price {
                        candle.o
                    } else {
                        order.sl_price
                    }
                } else if hit_tp {
                    if candle.o > order.tp_price {
                        candle.o
                    } else {
                        order.tp_price
                    }
                } else {
                    order.sl_price
                }
            }
            OrderType::Short => {
                let hit_sl = candle.h >= order.sl_price;
                let hit_tp = candle.l <= order.tp_price;

                if hit_sl {
                    if candle.o > order.sl_price {
                        candle.o
                    } else {
                        order.sl_price
                    }
                } else if hit_tp {
                    if candle.o < order.tp_price {
                        candle.o
                    } else {
                        order.tp_price
                    }
                } else {
                    order.sl_price
                }
            }
            OrderType::Unknown => 0.0,
        };

        let pnl_pct = match order.dtype {
            OrderType::Short => (order.entry_price - exit_price) / order.entry_price,
            OrderType::Long => (exit_price - order.entry_price) / order.entry_price,
            OrderType::Unknown => 0.0,
        };

        // Phí khứ hồi (entry + exit) — trừ vào PnL thực hiện để report và reward
        // (SizeAwareSharpe) là NET of fees, không chỉ dùng fee làm hurdle.
        Some((exit_price, pnl_pct - 2.0 * fee_rate))
    }

    #[inline]
    fn convert_order_history_into_report(orders: &[Order]) -> Report {
        if orders.is_empty() {
            return Report::default();
        }

        let mut peak_equity = 0.0f64;
        let mut max_drawdown = 0.0f64;
        let mut current_equity = 0.0f64;

        let mut consecutive_losses = 0usize;
        let mut max_consecutive_losses = 0usize;

        let mut pnl_list = Vec::with_capacity(orders.len());
        let (mut long_trades, mut long_wins) = (0usize, 0usize);
        let (mut short_trades, mut short_wins) = (0usize, 0usize);
        let (mut gross_profit, mut gross_loss) = (0.0, 0.0);
        let (mut wins, mut total_pnl) = (0usize, 0.0);
        // PnL tuyệt đối (size-weighted): `size` phụ thuộc head weights (w/b qua
        // win probabilities → Kelly) nên field này làm reward phân biệt được w/b.
        // Các metric còn lại chỉ nhìn `pnl_pct` (tỷ lệ, không phụ thuộc size).
        let mut net_pnl_abs = 0.0f64;

        for order in orders {
            if let Some(pnl) = order.pnl_pct {
                pnl_list.push(pnl);
                total_pnl += pnl;
                net_pnl_abs += order.size * pnl;

                current_equity += pnl;

                if current_equity > peak_equity {
                    peak_equity = current_equity;
                } else {
                    max_drawdown = max_drawdown.max(peak_equity - current_equity);
                }

                match order.dtype {
                    OrderType::Long => {
                        long_trades += 1;
                        if pnl > 0.0 {
                            long_wins += 1;
                        }
                    }
                    OrderType::Short => {
                        short_trades += 1;
                        if pnl > 0.0 {
                            short_wins += 1;
                        }
                    }
                    _ => {}
                }

                if pnl > 0.0 {
                    wins += 1;
                    gross_profit += pnl;
                    consecutive_losses = 0;
                } else {
                    gross_loss += pnl.abs();
                    consecutive_losses += 1;
                    max_consecutive_losses = max_consecutive_losses.max(consecutive_losses);
                }
            }
        }

        let total_trades = orders.len();
        let losses = total_trades - wins;
        let win_rate = wins as f64 / total_trades as f64;

        let avg_win = if wins > 0 {
            gross_profit / wins as f64
        } else {
            0.0
        };
        let avg_loss = if losses > 0 {
            gross_loss / losses as f64
        } else {
            0.0
        };

        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else {
            f64::INFINITY
        };
        let win_loss_ratio = if avg_loss > 0.0 {
            avg_win / avg_loss
        } else {
            f64::INFINITY
        };
        let expectancy = (win_rate * avg_win) - ((1.0 - win_rate) * avg_loss);

        // Ratios
        let mean_pnl = total_pnl / total_trades as f64;
        let std_dev = (pnl_list.iter().map(|p| (p - mean_pnl).powi(2)).sum::<f64>()
            / total_trades as f64)
            .sqrt();
        let downside_std_dev = (pnl_list
            .iter()
            .filter(|&&p| p < 0.0)
            .map(|p| p.powi(2))
            .sum::<f64>()
            / total_trades as f64)
            .sqrt();

        let sqrt_trades = (total_trades as f64).sqrt();
        let sharpe_ratio = if std_dev > 0.0 {
            (mean_pnl / std_dev) * sqrt_trades
        } else {
            0.0
        };
        let sortino_ratio = if downside_std_dev > 0.0 {
            (mean_pnl / downside_std_dev) * sqrt_trades
        } else {
            0.0
        };
        let calmar_ratio = if max_drawdown > 0.0 {
            total_pnl / max_drawdown
        } else {
            0.0
        };

        Report {
            total_trades,
            win_rate: win_rate * 100.0,
            net_pnl_pct: total_pnl * 100.0,
            total_return: total_pnl * 100.0,
            profit_factor,
            expectancy: expectancy * 100.0,
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            max_drawdown_pct: max_drawdown * 100.0,
            max_consecutive_losses,
            avg_win_pct: avg_win * 100.0,
            avg_loss_pct: avg_loss * 100.0,
            win_loss_ratio,
            long_trades,
            long_win_rate: if long_trades > 0 {
                (long_wins as f64 / long_trades as f64) * 100.0
            } else {
                0.0
            },
            short_trades,
            short_win_rate: if short_trades > 0 {
                (short_wins as f64 / short_trades as f64) * 100.0
            } else {
                0.0
            },
            net_pnl_abs,
        }
    }

    #[inline]
    fn calculate_order_size(win_p: f64, sl_pct: f64, fraction: f64, base_capital: f64) -> f64 {
        if win_p <= 0.0 || win_p >= 1.0 || sl_pct <= 0.0 {
            return 0.0;
        }

        // b = odds = reward/risk
        let b = 1.0 / sl_pct; // Ví dụ: SL 5% → b = 20
        let q = 1.0 - win_p;

        let kelly = (win_p * b - q) / b; // Kelly fraction
        let safe_kelly = (kelly * fraction).max(0.0);

        base_capital * safe_kelly
    }

    /// Tính block_id cho timestamp.
    #[inline]
    fn block_id(ts: u64) -> i64 {
        (ts as i64) / BLOCK_SECS
    }

    /// Kiểm tra xem tất cả blocks từ `start_bid` đến `end_bid` có coverage
    /// đầy đủ cho [from, to) không. Nếu có, trả về Some(candles).
    #[inline]
    fn try_read_blocks(lru: &BlockLru, from: u64, to: u64) -> Option<Vec<CandleStick>> {
        let mut result = Vec::new();
        let start_bid = Self::block_id(from);
        let end_bid = Self::block_id(to.saturating_sub(1));

        for bid in start_bid..=end_bid {
            let block_start = (bid * BLOCK_SECS) as u64;
            let block_end = ((bid + 1) * BLOCK_SECS) as u64;
            let needed_start = from.max(block_start);
            let needed_end = to.min(block_end);
            let block = lru.get(&bid)?;

            if needed_start < block.covered_first || needed_end > block.covered_last {
                #[cfg(debug_assertions)]
                println!(
                    "  [debug] try_read_blocks: block {}  need [{}, {})  covered [{}, {})  MISS  key=<see caller>",
                    bid, needed_start, needed_end, block.covered_first, block.covered_last,
                );
                return None;
            }

            result.extend(block.subrange(needed_start, needed_end));
        }

        if result.is_empty() && from < to {
            return Some(vec![]);
        }

        result.sort_by_key(|c| c.t);
        result.dedup_by_key(|c| c.t);
        Some(result)
    }

    /// Merge candles vào LRU blocks, cập nhật covered range.
    ///
    /// Với dữ liệu historical (`now >= original_to`), `covered_last` được extend
    /// đến `effective_to` (full query boundary) vì dữ liệu đã hoàn chỉnh.
    /// Với dữ liệu live (`now < original_to`), `covered_last` được cap tại
    /// `last_t + step` để tránh false HIT do API data lag.
    fn update_blocks(
        lru: &BlockLru,
        candles: &[CandleStick],
        query_from: u64,
        query_to: u64,
        original_to: u64,
        now: u64,
    ) {
        // Nhóm candles theo block
        let mut groups: std::collections::HashMap<i64, Vec<CandleStick>> =
            std::collections::HashMap::new();
        for c in candles {
            let bid = Self::block_id(c.t as u64);
            groups.entry(bid).or_default().push(*c);
        }

        let start_bid = Self::block_id(query_from);
        let end_bid = Self::block_id(query_to.saturating_sub(1));

        for bid in start_bid..=end_bid {
            let mut block = lru.get(&bid).unwrap_or(BlockCache {
                candles: vec![],
                covered_first: u64::MAX,
                covered_last: u64::MIN,
            });

            // Merge candles mới (nếu có)
            if let Some(new_cands) = groups.get_mut(&bid) {
                block.candles.append(new_cands);
                block.candles.sort_by_key(|c| c.t);
                block.candles.dedup_by_key(|c| c.t);
            }

            // Cập nhật coverage cho block này dùng query boundary.
            // Pattern giống ohcl.rs: coverage = query range ∩ block range.
            //
            // `covered_first`: mở rộng TRÁI đến `effective_from` (query boundary).
            //   Nếu candles bắt đầu sau `from` (do alignment), chúng ta vẫn claim
            //   coverage từ `from` → tránh false MISS ở lần query tiếp theo.
            //
            // `covered_last`:  mở rộng PHẢI đến candle thực tế (`last_t + step`),
            //   KHÔNG dùng `effective_to` — tránh false HIT khi API chưa có
            //   nến cuối (live data lag). Nếu block rỗng (không candle), dùng
            //   `effective_to` để claim coverage cho khoảng empty đã query.
            let block_start = (bid * BLOCK_SECS) as u64;
            let block_end = ((bid + 1) * BLOCK_SECS) as u64;
            let effective_from = query_from.max(block_start);
            let effective_to = query_to.min(block_end);

            // covered_first: luôn extend về phía query_from
            #[cfg(debug_assertions)]
            let old_first = block.covered_first;
            block.covered_first = block.covered_first.min(effective_from);

            // covered_last:
            //   - Historical (now >= original_to): extend đến effective_to
            //     vì dữ liệu đã hoàn chỉnh, tránh persistent MISS cho block
            //     cuối cùng do non-trading gap (weekend, after-hours).
            //   - Live (now < original_to): cap tại last_t + step để tránh
            //     false HIT khi API chưa có nến cuối (data lag).
            #[cfg(debug_assertions)]
            let old_last = block.covered_last;

            if let (Some(_), Some(last)) = (block.candles.first(), block.candles.last()) {
                if now >= original_to {
                    // Historical — dữ liệu hoàn chỉnh, extend toàn bộ query range
                    block.covered_last = block.covered_last.max(effective_to);
                } else {
                    // Live — cap tại candle extent để tránh false HIT
                    let step = block
                        .candles
                        .windows(2)
                        .map(|w| (w[1].t - w[0].t).unsigned_abs())
                        .filter(|&d| d > 0)
                        .min()
                        .unwrap_or(300); // fallback 5 phút
                    let extent = (last.t as u64).saturating_add(step);
                    block.covered_last = block.covered_last.max(extent).min(effective_to);
                }
            } else {
                // Block rỗng — claim coverage từ query range
                if block.covered_first == u64::MAX {
                    block.covered_first = effective_from;
                    block.covered_last = effective_to;
                } else {
                    block.covered_last = block.covered_last.max(effective_to);
                }
            }

            lru.put(bid, block.clone());

            #[cfg(debug_assertions)]
            if old_first != block.covered_first || old_last != block.covered_last {
                println!(
                    "  [debug] update_blocks: block {}  coverage [{}, {}) → [{}, {})  {} candles",
                    bid,
                    old_first,
                    old_last,
                    block.covered_first,
                    block.covered_last,
                    block.candles.len(),
                );
            }
        }
    }

    /// Direct fetch từ DataLoader, với retry nếu `to > now` (live data lag).
    #[inline]
    async fn fetch_direct(
        loader: &Arc<dyn DataLoader + Sync + Send>,
        resolution: &str,
        from: u64,
        to: u64,
        now: u64,
    ) -> Result<Vec<CandleStick>, Error> {
        match loader.range(from, to, resolution).await {
            Ok(candles) if !candles.is_empty() => Ok(candles),
            Ok(_) if to > now => {
                let mut retry_count = 0u32;
                const MAX_RETRIES: u32 = 3;
                loop {
                    sleep(Duration::from_secs(1)).await;
                    match loader.range(from, to, resolution).await {
                        Ok(candles) if !candles.is_empty() => return Ok(candles),
                        Ok(_) => {
                            retry_count += 1;
                            if retry_count >= MAX_RETRIES {
                                return Err(Error::new(
                                    ErrorKind::TimedOut,
                                    format!("live timeout after retry {}", MAX_RETRIES),
                                ));
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            Ok(_) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// Fetch candles từ block cache hoặc loader.
    /// Trả về candles trong [from, to).
    ///
    /// Dùng `calendar` để bỏ qua khoảng thời gian không có giao dịch
    /// (ví dụ cuối tuần StockCalendar, ngoài giờ VN), tránh query API
    /// tốn thời gian chỉ để nhận về empty candles.
    ///
    /// `cache_enabled = false` bỏ qua LRU hoàn toàn: mỗi range gọi thẳng
    /// `loader.range` (vẫn giữ calendar alignment + retry live-lag). Hợp khi
    /// loader rẻ và stateless — không tốn RAM cache và không phải quản lý
    /// coverage cho dữ liệu không cần giữ.
    #[inline]
    async fn fetch_candles_from_loader(
        loader: &Arc<dyn DataLoader + Sync + Send>,
        cache: &Arc<RwLock<HashMap<String, BlockLru>>>,
        calendar: &Arc<dyn Calendar + Sync + Send>,
        from: u64,
        to: u64,
        resolution: &str,
        cache_key: &str,
        cache_enabled: bool,
    ) -> Result<Vec<CandleStick>, Error> {
        if from >= to {
            return Err(Error::new(
                ErrorKind::BrokenPipe,
                format!("`from` ({from}) should be smaller than `to` ({to})"),
            ));
        }

        // --- 1. Thử block cache trước ---
        if cache_enabled {
            let guard = cache.read().await;
            if let Some(lru) = guard.get(cache_key)
                && let Some(candles) = Self::try_read_blocks(lru, from, to)
            {
                return Ok(candles);
            }
        }

        // --- 1b. Calendar adjustment — nếu from/to rơi vào ngoài giờ giao dịch,
        //          đẩy cả hai đầu đến vùng có nến thực tế.
        //          Dùng cùng pattern: `calendar.next(ts - step)` để cả from và to
        //          đều được align vào khung giao dịch, tránh trượt khung (tránh bị
        //          fetch_to > to khiến block_to tràn ra ngoài trading session).
        let step = to_timestamp_secs(resolution);
        let fetch_from = calendar.next(from.saturating_sub(step), resolution);
        let fetch_to = calendar.next(to.saturating_sub(step), resolution);

        #[cfg(debug_assertions)]
        if fetch_from != from || fetch_to != to {
            println!(
                "  [debug] fetch_candles: calendar adjust [{}, {}) → [{}, {})  step={}  key={}",
                from, to, fetch_from, fetch_to, step, cache_key
            );
        }

        if fetch_from >= fetch_to {
            #[cfg(debug_assertions)]
            println!(
                "  [debug] fetch_candles: skip [{}, {}) — outside trading hours  key={}",
                from, to, cache_key
            );
            return Ok(vec![]);
        }

        // --- 2. Cache miss → gọi loader ---
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::other(e.to_string()))?
            .as_secs();

        // Cache bật: mở rộng ra full weekly blocks để coverage LRU khớp ranh
        // giới block (đọc lại cửa sổ lookback ở lần sau không phải fetch lại).
        // Cache tắt: chỉ hỏi đúng range cần, không mở rộng — mở rộng chỉ có
        // nghĩa khi kết quả được giữ lại.
        let (block_from, block_to) = if cache_enabled {
            let start_bid = Self::block_id(fetch_from);
            let end_bid = Self::block_id(fetch_to.saturating_sub(1));
            ((start_bid * BLOCK_SECS) as u64, ((end_bid + 1) * BLOCK_SECS) as u64)
        } else {
            (fetch_from, fetch_to)
        };

        #[cfg(debug_assertions)]
        let t_fetch = std::time::Instant::now();

        let candles_full =
            Self::fetch_direct(loader, resolution, block_from, block_to, now).await?;

        #[cfg(debug_assertions)]
        println!(
            "  [debug] fetch_candles: MISS [{}, {}) → blocks [{}, {})  got {} candles  {:.0}ms  key={}  cache={}",
            from,
            to,
            block_from,
            block_to,
            candles_full.len(),
            t_fetch.elapsed().as_secs_f64() * 1000.0,
            cache_key,
            cache_enabled,
        );

        // --- 3. Lưu vào LRU blocks ---
        if cache_enabled {
            let mut guard = cache.write().await;
            let lru = guard
                .entry(cache_key.to_string())
                .or_insert_with(|| opsense_mlib::lru::LruCache::new(1024)); // 256 blocks ~ 5 years
            Self::update_blocks(lru, &candles_full, block_from, block_to, to, now);
        }

        // Return trunc theo [from, to) gốc
        let start = candles_full.partition_point(|c| (c.t as u64) < from);
        let end = candles_full.partition_point(|c| (c.t as u64) < to);
        Ok(candles_full[start..end].to_vec())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Historical backtest tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_order_size_basic() {
        let size = Portfolio::calculate_order_size(0.6, 0.05, 0.25, 100_000.0);
        assert!(size > 0.0 && size < 100_000.0);
    }

    #[test]
    fn test_calculate_order_size_edge() {
        let size = Portfolio::calculate_order_size(0.0, 0.05, 0.25, 100_000.0);
        assert_eq!(size, 0.0);
        let size = Portfolio::calculate_order_size(1.0, 0.05, 0.25, 100_000.0);
        assert_eq!(size, 0.0);
        let size = Portfolio::calculate_order_size(0.6, 0.0, 0.25, 100_000.0);
        assert_eq!(size, 0.0);
    }

    #[test]
    fn test_check_order_exit_long_hit_sl() {
        let o = Order {
            dtype: OrderType::Long,
            entry_price: 100.0,
            sl_price: 95.0,
            tp_price: 110.0,
            size: 1.0,
            ..Default::default()
        };
        let c = CandleStick {
            t: 1000,
            o: 98.0,
            h: 99.0,
            l: 94.0,
            c: 95.0,
            v: 1000.0,
        };
        let result = Portfolio::check_order_exit(&o, &c, 0.0005, 0);
        assert!(result.is_some());
        let (exit, pnl) = result.unwrap();
        assert!(exit <= o.sl_price);
        assert!(pnl < 0.0);
    }

    #[test]
    fn test_check_order_exit_short_hit_tp() {
        let o = Order {
            dtype: OrderType::Short,
            entry_price: 100.0,
            sl_price: 105.0,
            tp_price: 95.0,
            size: 1.0,
            ..Default::default()
        };
        let c = CandleStick {
            t: 1000,
            o: 99.0,
            h: 100.0,
            l: 94.0,
            c: 95.0,
            v: 1000.0,
        };
        let result = Portfolio::check_order_exit(&o, &c, 0.0005, 0);
        assert!(result.is_some());
        let (exit, pnl) = result.unwrap();
        assert!(exit >= o.tp_price);
        assert!(pnl > 0.0);
    }

    #[test]
    fn test_check_order_exit_no_exit() {
        let o = Order {
            dtype: OrderType::Long,
            entry_price: 100.0,
            sl_price: 95.0,
            tp_price: 110.0,
            size: 1.0,
            ..Default::default()
        };
        let c = CandleStick {
            t: 1000,
            o: 98.0,
            h: 103.0,
            l: 97.0,
            c: 102.0,
            v: 1000.0,
        };
        assert!(Portfolio::check_order_exit(&o, &c, 0.0005, 0).is_none());
    }

    #[test]
    fn test_check_order_exit_t_plus_n_blocks_before_unlock() {
        // Lệnh Long chạm SL, nhưng current_seq < unlock_seq (T+N chưa đủ) → không được đóng.
        let o = Order {
            dtype: OrderType::Long,
            entry_price: 100.0,
            sl_price: 95.0,
            tp_price: 110.0,
            size: 1.0,
            unlock_seq: 5,
            ..Default::default()
        };
        let c = CandleStick {
            t: 1000,
            o: 98.0,
            h: 99.0,
            l: 94.0, // chạm SL
            c: 95.0,
            v: 1000.0,
        };
        assert!(Portfolio::check_order_exit(&o, &c, 0.0005, 4).is_none());
        // Đủ T+N (current_seq == unlock_seq) → đóng bình thường.
        assert!(Portfolio::check_order_exit(&o, &c, 0.0005, 5).is_some());
    }

    #[test]
    fn test_check_order_exit_no_unlock_is_unrestricted() {
        // unlock_seq = 0 (mặc định) → luôn được phép đóng khi chạm SL/TP.
        let o = Order {
            dtype: OrderType::Long,
            entry_price: 100.0,
            sl_price: 95.0,
            tp_price: 110.0,
            size: 1.0,
            ..Default::default()
        };
        let c = CandleStick {
            t: 1000,
            o: 98.0,
            h: 99.0,
            l: 94.0,
            c: 95.0,
            v: 1000.0,
        };
        assert!(Portfolio::check_order_exit(&o, &c, 0.0005, 0).is_some());
    }

    #[test]
    fn test_empty_report() {
        let report = Portfolio::convert_order_history_into_report(&[]);
        assert_eq!(report.total_trades, 0);
    }

    // ── Kernel `evaluate` + `Session` ───────────────────────────────────────

    use crate::fee::SimpleFixedFee;
    use crate::score::SharpeScore;
    // `Future`/`Pin`/`LoaderFuture` đã có ở scope cha (qua `use super::*`).

    /// Price series 1m (openTime giây, cách nhau 60s), dùng cho cả kernel lẫn
    /// dữ liệu analysis.
    fn candles(n: i64, from_ts: i64, f: impl Fn(i64) -> f64) -> Vec<CandleStick> {
        (0..n)
            .map(|i| {
                let price = f(i);
                CandleStick::new(
                    from_ts + i * 60,
                    price,
                    price + 0.5,
                    price - 0.5,
                    price,
                    10.0,
                )
            })
            .collect()
    }

    /// Loader rỗng — kernel không hỏi loader (fetch do caller đóng), chỉ cần
    /// một giá trị hợp lệ cho `Portfolio::new`.
    struct NoLoader;
    #[async_trait::async_trait]
    impl DataLoader for NoLoader {
        async fn range(&self, _from: u64, _to: u64, _res: &str) -> Result<Vec<CandleStick>, Error> {
            Ok(Vec::new())
        }
    }

    /// Strategy tối giản cho test kernel: một lưới phẳng bộc kín khoảng
    /// `[min low, max high]` của dữ liệu fetch được.
    ///
    /// Test kernel cần *một* strategy thật (để plan có hiệu lực, có lệnh để
    /// kiểm tra settlement/idempotency), không cần logic sieve của
    /// `AnalysisGrid` — nên ở lại đây thay vì kéo cả grid strategy vào crate
    /// này. Strategy viết bằng script (Rhai) là đường production: xem
    /// `opsense-rhai::ScriptStrategy`.
    // `typetag` + serde chỉ có khi bật feature `json`; trait `Strategy` cũng chỉ
    // mang supertrait typetag khi đó → helper test tự ẩn theo feature.
    #[cfg_attr(feature = "json", derive(serde::Serialize, serde::Deserialize))]
    struct FlatStrategy {
        levels: usize,
        sl_pct: f64,
        review_interval_secs: u64,
    }

    #[cfg_attr(feature = "json", typetag::serde(name = "test_flat"))]
    #[async_trait::async_trait]
    impl Strategy for FlatStrategy {
        fn init(&self) -> Vec<f64> {
            vec![0.25, 100_000.0, self.levels as f64, self.sl_pct, 2.0 * 24.0 * 3600.0]
        }

        async fn next(&self, current: u64) -> u64 {
            current + self.review_interval_secs
        }

        async fn rebuild(
            &self,
            current_ts: u64,
            _grids: &[TradingGrid],
            fetch: FetchFn<'_>,
            param: ParamFn<'_>,
        ) -> Result<Vec<TradingGrid>, Error> {
            let lookback = param(4) as u64;
            let data = fetch(current_ts.saturating_sub(lookback), current_ts).await?;
            if data.len() < 10 {
                return Err(Error::other("not enough analysis data"));
            }
            let min = data.iter().map(|c| c.l).fold(f64::INFINITY, f64::min);
            let max = data.iter().map(|c| c.h).fold(f64::NEG_INFINITY, f64::max);
            let Some(g) = TradingGrid::new(self.levels, min, max) else {
                return Err(Error::other("price range hợp lệ?"));
            };
            Ok(vec![g.with_sl_pct(param(3))])
        }
    }

    fn flat_strategy() -> Arc<dyn Strategy + Sync + Send> {
        Arc::new(FlatStrategy {
            levels: 5,
            sl_pct: 0.008,
            review_interval_secs: 900,
        })
    }

    fn portfolio(config: PortfolioConfig) -> Portfolio {
        Portfolio::new(
            Arc::new(NoLoader),
            flat_strategy(),
            Arc::new(SimpleFixedFee::new(0.0005)),
            Arc::new(SharpeScore),
            Arc::new(crate::calendar::CryptoCalendar),
            config,
        )
        .unwrap()
    }

    fn grid_config(settlement: u64) -> PortfolioConfig {
        PortfolioConfig {
            resolution_for_test: "1m".to_string(),
            resolution_for_rebuild: "1m".to_string(),
            settlement_candles: settlement,
            cache_enabled: false,
        }
    }

    /// Fetch đóng trên 1 slice cố định: kernel gọi bất kỳ range nào cũng được,
    /// không cần biết dữ liệu nằm ở đâu — đúng abstraction của `FetchFn`.
    fn slice_fetch(
        data: Vec<CandleStick>,
    ) -> Box<
        dyn FnMut(u64, u64) -> LoaderFuture + Send + Sync,
    > {
        Box::new(move |from: u64, to: u64| {
            let out: Vec<CandleStick> = data
                .iter()
                .copied()
                .filter(|c| c.t >= 0 && (c.t as u64) >= from && (c.t as u64) < to)
                .collect();
            Box::pin(async move { Ok(out) })
        })
    }

    /// Notify thu event vào vector (boxed để coerce được sang `NotifyFn`).
    fn notify_box<'a>(
        sink: &'a mut Vec<OrderEvent>,
    ) -> Box<
        dyn FnMut(OrderEvent) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>
            + Send
            + Sync
            + 'a,
    > {
        Box::new(move |e: OrderEvent| {
            sink.push(e);
            Box::pin(async move { Ok(()) })
        })
    }

    /// Mốc bắt đầu series + số nến warm-up đủ cho `AnalysisGrid` (≥10).
    /// Kernel rebuild tại `from` bằng cửa sổ `[from - lookback, from)`, nên
    /// series phải có dữ liệu **trước** `from` — đúng như backtest thật.
    const BASE: i64 = 1_700_000_000;
    const WARMUP: i64 = 60;

    fn from_ts() -> u64 {
        (BASE + WARMUP * 60) as u64
    }

    fn grid_params() -> Vec<f64> {
        vec![0.25, 1_000.0, 5.0, 0.008, (2 * 24 * 3600) as f64]
    }

    #[tokio::test]
    async fn kernel_advances_session_one_candle_at_a_time() {
        // Giá nhấn sóng để giá chạm nhiều grid level → có lệnh đặt/đóng.
        let data = candles(WARMUP + 40, BASE, |i| 100.0 + (i % 40) as f64 - 20.0);
        let pf = portfolio(grid_config(0));
        let params = grid_params();
        let mut session = Session::new();
        let mut events = Vec::new();
        let from = from_ts();

        // 10 lời gọi × 4 nến: mỗi lời gọi nhận cửa sổ MỚI và chỉ tiến session
        // (đúng cách realtime gọi từng nến) — không reset plan/seq/lệnh.
        for i in 0..10u64 {
            let mut trade = slice_fetch(data.clone());
            let mut analysis = slice_fetch(data.clone());
            let mut notify = notify_box(&mut events);
            pf.evaluate(
                &mut session,
                from + i * 4 * 60,
                from + (i + 1) * 4 * 60,
                &|id| params[id],
                &mut *trade,
                &mut *analysis,
                &mut *notify,
            )
            .await
            .expect("kernel chạy");
        }

        assert_eq!(session.candle_seq, 40, "mỗi nến đếm đúng 1 lần");
        assert_eq!(session.candle_ts, from as i64 + 39 * 60);
        assert!(
            !session.plan.is_empty(),
            "rebuild phải dựng được plan từ candle: {session:?}"
        );
        assert!(
            session.orders.len() + session.history.len() > 0,
            "phải có lệnh: {session:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, OrderEvent::Placed { .. })),
            "notify phải nhận Placed: {events:?}"
        );
    }

    #[tokio::test]
    async fn kernel_is_idempotent_for_already_processed_candles() {
        let data = candles(WARMUP + 20, BASE, |i| 100.0 + (i % 10) as f64);
        let pf = portfolio(grid_config(0));
        let params = grid_params();
        let mut session = Session::new();
        let from = from_ts();

        for _ in 0..2 {
            let mut trade = slice_fetch(data.clone());
            let mut analysis = slice_fetch(data.clone());
            let mut events = Vec::new();
            let mut notify = notify_box(&mut events);
            pf.evaluate(
                &mut session,
                from,
                from + 20 * 60,
                &|id| params[id],
                &mut *trade,
                &mut *analysis,
                &mut *notify,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            session.candle_seq, 20,
            "nến đã xử lý không được đếm lại (session.candle_ts chặn)"
        );
    }

    #[tokio::test]
    async fn settlement_blocks_exit_until_n_more_candles() {
        // T+3: lệnh mở ở nến 100 phải còn mở qua các nến 101..103.
        let pf = portfolio(grid_config(3));
        let params = grid_params();
        let from = from_ts();

        // Nạp đủ history để plan có level quanh 100, rồi đẩy giá xuống sâu để
        // chạm SL — nhưng T+3 chặn nên lệnh phải còn mở.
        let mut data: Vec<CandleStick> = candles(WARMUP + 30, BASE, |i| 100.0 - i as f64 * 0.1);
        let mut session = Session::new();
        let mut events = Vec::new();

        {
            let mut trade = slice_fetch(data.clone());
            let mut analysis = slice_fetch(data.clone());
            let mut notify = notify_box(&mut events);
            pf.evaluate(
                &mut session,
                from,
                from + 30 * 60,
                &|id| params[id],
                &mut *trade,
                &mut *analysis,
                &mut *notify,
            )
            .await
            .unwrap();
        }

        assert!(!session.plan.is_empty(), "sau 30 nến plan phải có");
        let open = session.orders.len();
        if open > 0 {
            // Nến giảm mạnh chạm SL/TP mọi level.
            let crash_ts = u64::try_from(session.candle_ts + 60).unwrap();
            data.push(CandleStick::new(
                i64::try_from(crash_ts).unwrap(),
                1.0,
                101.0,
                0.5,
                1.0,
                10.0,
            ));
            let mut trade = slice_fetch(data.clone());
            let mut analysis = slice_fetch(data.clone());
            {
                let mut notify = notify_box(&mut events);
                pf.evaluate(
                    &mut session,
                    crash_ts,
                    crash_ts + 60,
                    &|id| params[id],
                    &mut *trade,
                    &mut *analysis,
                    &mut *notify,
                )
                .await
                .unwrap();
            }
            assert_eq!(
                session.orders.len(),
                open,
                "T+3: lệnh chưa đủ nến chờ nên không đóng được"
            );
        }
    }

    #[tokio::test]
    async fn rebuild_failure_advances_review_and_keeps_plan() {
        // Chưa đủ 10 nến analysis → rebuild fail; session phải vẫn tiến
        // `review_at` (không thử lại mỗi nến) và giữ plan rỗng.
        let data = candles(3, BASE, |_| 100.0);
        let pf = portfolio(grid_config(0));
        let params = grid_params();
        let mut session = Session::new();
        let mut events = Vec::new();
        let mut notify = notify_box(&mut events);

        let mut trade = slice_fetch(data.clone());
        let mut analysis = slice_fetch(data.clone());
        let result = pf
            .evaluate(
                &mut session,
                BASE as u64,
                BASE as u64 + 3 * 60,
                &|id| params[id],
                &mut *trade,
                &mut *analysis,
                &mut notify,
            )
            .await;

        assert!(result.is_err(), "thiếu data phải báo lỗi rebuild");
        assert!(
            session.review_at > 0,
            "review_at phải tiến dù rebuild fail, nếu không sẽ retry mỗi nến"
        );
        assert!(session.plan.is_empty(), "plan cũ (rỗng) được giữ nguyên");
    }

    #[tokio::test]
    async fn cache_disabled_skips_lru_and_asks_loader_exact_range() {
        // Loader đếm số lần được hỏi + range nhận được: tắt cache phải hỏi
        // đúng range cần (không mở rộng full weekly block) và không lưu LRU.
        struct CountingLoader {
            calls: Arc<std::sync::atomic::AtomicUsize>,
            last: Arc<std::sync::Mutex<Option<(u64, u64)>>>,
            data: Vec<CandleStick>,
        }
        #[async_trait::async_trait]
        impl DataLoader for CountingLoader {
            async fn range(
                &self,
                from: u64,
                to: u64,
                _resolution: &str,
            ) -> Result<Vec<CandleStick>, Error> {
                self.calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                *self.last.lock().unwrap() = Some((from, to));
                Ok(self
                    .data
                    .iter()
                    .copied()
                    .filter(|c| c.t >= 0 && (c.t as u64) >= from && (c.t as u64) < to)
                    .collect())
            }
        }

        let data = candles(WARMUP + 200, BASE, |i| 100.0 + (i % 10) as f64);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let last = Arc::new(std::sync::Mutex::new(None));
        let loader = Arc::new(CountingLoader {
            calls: calls.clone(),
            last: last.clone(),
            data,
        });

        let strategy = flat_strategy();
        let config = PortfolioConfig {
            cache_enabled: false,
            ..grid_config(0)
        };
        let pf = Portfolio::new(
            loader,
            strategy,
            Arc::new(SimpleFixedFee::new(0.0005)),
            Arc::new(SharpeScore),
            Arc::new(crate::calendar::CryptoCalendar),
            config,
        )
        .unwrap();

        let params = grid_params();
        let mut session = Session::new();
        let mut events = Vec::new();
        let mut notify = notify_box(&mut events);
        pf.backtest(
            &mut session,
            3600,
            from_ts(),
            from_ts() + 200 * 60,
            &|id| params[id],
            &mut notify,
        )
        .await
        .unwrap();

        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "loader phải được hỏi"
        );
        let (from, to) = last.lock().unwrap().expect("loader phải được hỏi ít nhất 1 lần");
        assert!(
            to - from < 7 * 24 * 3600,
            "cache tắt: không mở rộng full weekly block, hỏi đúng range ({from}, {to})"
        );
        assert_eq!(session.candle_seq, 200, "vẫn chạy đủ nến qua loader");
    }
}
