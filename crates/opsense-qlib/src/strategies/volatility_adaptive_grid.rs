//! # VolatilityAdaptiveGridStrategy — Grid với bước lưới điều chỉnh theo volatility
//!
//! Strategy này mở rộng GridStrategy bằng cách:
//! - Tính ATR (Average True Range) trong lookback period
//! - Điều chỉnh grid step = ATR * multiplier
//! - Khi volatility cao → grid thưa hơn, volatility thấp → grid dày hơn
//! - SL được tính dựa trên ATR (SL = entry ± ATR * sl_multiplier)
//!

use std::io::Error;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use analysis::{AnalysisGrid, TradingGrid, TransitionAnalysis};

use crate::qlib::{FetchFn, ParamFn, Strategy};

const INITIAL_CAPITAL: f64 = 100_000.0;

#[derive(Debug, Serialize, Deserialize)]
pub struct VolatilityAdaptiveGridStrategy {
    /// Số grid levels tối đa
    max_grid_levels: usize,

    /// ATR multiplier để tính grid step
    /// grid_step = ATR * atr_multiplier
    atr_multiplier: f64,

    /// SL multiplier dựa trên ATR
    /// SL = entry ± (ATR * sl_atr_multiplier)
    sl_atr_multiplier: f64,

    /// Lookback để tính ATR (giây)
    atr_lookback_secs: u64,

    /// Lookback để phân tích transition (giây)
    analysis_lookback_secs: u64,

    review_interval_secs: u64,
    smoothing_k: f64,
    trading_candle_secs: u64,

    #[serde(skip)]
    last_review: AtomicU64,
}

impl Clone for VolatilityAdaptiveGridStrategy {
    fn clone(&self) -> Self {
        Self {
            max_grid_levels: self.max_grid_levels,
            atr_multiplier: self.atr_multiplier,
            sl_atr_multiplier: self.sl_atr_multiplier,
            atr_lookback_secs: self.atr_lookback_secs,
            analysis_lookback_secs: self.analysis_lookback_secs,
            review_interval_secs: self.review_interval_secs,
            smoothing_k: self.smoothing_k,
            trading_candle_secs: self.trading_candle_secs,
            last_review: AtomicU64::new(self.last_review.load(Ordering::Relaxed)),
        }
    }
}

impl VolatilityAdaptiveGridStrategy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_grid_levels: usize,
        atr_multiplier: f64,
        sl_atr_multiplier: f64,
        atr_lookback_secs: u64,
        analysis_lookback_secs: u64,
        review_interval_secs: u64,
        smoothing_k: f64,
        trading_candle_secs: u64,
    ) -> Self {
        Self {
            max_grid_levels,
            atr_multiplier,
            sl_atr_multiplier,
            atr_lookback_secs,
            analysis_lookback_secs,
            review_interval_secs,
            smoothing_k,
            trading_candle_secs,
            last_review: AtomicU64::new(0),
        }
    }

    /// Tính ATR từ dữ liệu nến
    fn calculate_atr(candles: &[CandleStick], period: usize) -> f64 {
        if candles.len() < 2 || period == 0 {
            return 0.0;
        }

        let true_ranges: Vec<f64> = candles
            .windows(2)
            .map(|w| {
                let prev_close = w[0].c;
                let high = w[1].h;
                let low = w[1].l;
                let _close = w[1].c;

                let tr1 = high - low;
                let tr2 = (high - prev_close).abs();
                let tr3 = (low - prev_close).abs();

                tr1.max(tr2).max(tr3)
            })
            .collect();

        // Simple moving average của true ranges
        if true_ranges.len() < period {
            return true_ranges.iter().sum::<f64>() / true_ranges.len() as f64;
        }

        true_ranges[true_ranges.len() - period..]
            .iter()
            .sum::<f64>()
            / period as f64
    }

    /// Tính số grid levels tối ưu dựa trên ATR và price range
    fn calculate_optimal_levels(
        atr: f64,
        min_price: f64,
        max_price: f64,
        atr_multiplier: f64,
        max_levels: usize,
    ) -> usize {
        if atr <= 0.0 || min_price >= max_price {
            return 2;
        }

        let grid_step = atr * atr_multiplier;
        let price_range = max_price - min_price;

        // Số levels = price_range / grid_step + 1
        let calculated_levels = (price_range / grid_step).round() as usize + 1;

        // Clamp vào [2, max_levels]
        calculated_levels.clamp(2, max_levels)
    }

    fn cell_range(grid: &AnalysisGrid, cell: usize) -> (f64, f64) {
        let low = grid.min + cell as f64 * grid.step;
        (low, low + grid.step)
    }

    fn cell_probs(transition: &TransitionAnalysis, cell: usize) -> (f64, f64, f64) {
        let up = transition
            .up_probabilities()
            .get(cell)
            .copied()
            .unwrap_or(0.0);
        let down = transition
            .down_probabilities()
            .get(cell)
            .copied()
            .unwrap_or(0.0);
        let stay = transition
            .stay_probabilities()
            .get(cell)
            .copied()
            .unwrap_or(0.0);
        (up, down, stay)
    }

    fn update_win_probabilities(grids: &mut [TradingGrid], min_trades: usize, smoothing_k: f64) {
        for grid in grids.iter_mut() {
            let n_levels = grid.num_levels();
            let mut new_long_win_p = Vec::with_capacity(n_levels);
            let mut new_short_win_p = Vec::with_capacity(n_levels);

            for j in 0..n_levels {
                let long_wins = grid.long_win_count(j);
                let long_losses = grid.long_lost_count(j);
                let long_total = long_wins + long_losses;
                let model_long_p = grid.long_win_pct(j);

                let updated_long_p = if long_total >= min_trades {
                    let empirical_rate = long_wins as f64 / long_total as f64;
                    let weight = long_total as f64 / (long_total as f64 + smoothing_k);
                    (weight * empirical_rate + (1.0 - weight) * model_long_p).clamp(0.25, 0.75)
                } else {
                    model_long_p
                };

                let short_wins = grid.short_win_count(j);
                let short_losses = grid.short_lost_count(j);
                let short_total = short_wins + short_losses;
                let model_short_p = grid.short_win_pct(j);

                let updated_short_p = if short_total >= min_trades {
                    let empirical_rate = short_wins as f64 / short_total as f64;
                    let weight = short_total as f64 / (short_total as f64 + smoothing_k);
                    (weight * empirical_rate + (1.0 - weight) * model_short_p).clamp(0.25, 0.75)
                } else {
                    model_short_p
                };

                new_long_win_p.push(updated_long_p);
                new_short_win_p.push(updated_short_p);
            }

            *grid = std::mem::replace(grid, grid.clone())
                .with_win_probabilities(new_long_win_p, new_short_win_p);
        }
    }
}

use schemas::CandleStick;

#[typetag::serde(name = "volatility_adaptive_grid")]
#[async_trait]
impl Strategy for VolatilityAdaptiveGridStrategy {
    fn init(&self) -> Vec<f64> {
        vec![
            0.25,                               // 0: kelly_fraction
            INITIAL_CAPITAL,                    // 1: base_capital
            self.atr_multiplier,                // 2: atr_multiplier
            self.sl_atr_multiplier,             // 3: sl_atr_multiplier
            self.atr_lookback_secs as f64,      // 4: atr_lookback_secs
            self.analysis_lookback_secs as f64, // 5: analysis_lookback_secs
        ]
    }

    async fn next(&self, current: u64) -> u64 {
        self.last_review.store(current, Ordering::Relaxed);
        current + self.review_interval_secs
    }

    async fn rebuild(
        &self,
        current_ts: u64,
        grids: &[TradingGrid],
        fetch: FetchFn<'_>,
        param: ParamFn<'_>,
    ) -> Result<Vec<TradingGrid>, Error> {
        let _t = std::time::Instant::now();
        let atr_multiplier = param(2);
        let sl_atr_multiplier = param(3);
        let atr_lookback_secs = param(4) as u64;
        let analysis_lookback_secs = param(5) as u64;

        // Cập nhật win probabilities
        let mut updated_grids = grids.to_vec();
        Self::update_win_probabilities(&mut updated_grids, 3, self.smoothing_k);

        // Fetch data cho ATR calculation (ngắn hạn)
        let atr_from = current_ts.saturating_sub(atr_lookback_secs);
        let atr_data = fetch(atr_from, current_ts).await?;

        // Fetch data cho transition analysis (dài hạn hơn)
        let analysis_from = current_ts.saturating_sub(analysis_lookback_secs);
        let analysis_data = fetch(analysis_from, current_ts).await?;
        #[cfg(debug_assertions)]
        println!(
            "  [debug] VAG: fetch {}+{} candles in {:.0}ms",
            atr_data.len(),
            analysis_data.len(),
            _t.elapsed().as_secs_f64() * 1000.0
        );

        if atr_data.len() < 5 {
            return Err(Error::other("not enough data for ATR calculation"));
        }

        if analysis_data.len() < 10 {
            return Err(Error::other("not enough analysis data"));
        }

        // Tính ATR
        let atr_period = (atr_lookback_secs / 300).max(14) as usize; // Mặc định 14 nến nếu 5m candle
        let atr = Self::calculate_atr(&atr_data, atr_period);

        if atr <= 0.0 {
            return Err(Error::other("ATR calculation failed"));
        }

        // Tính price range từ analysis data
        let min_price = analysis_data
            .iter()
            .map(|c| c.l)
            .fold(f64::INFINITY, f64::min);
        let max_price = analysis_data
            .iter()
            .map(|c| c.h)
            .fold(f64::NEG_INFINITY, f64::max);
        let current_price = analysis_data.last().map(|c| c.c).unwrap_or(min_price);

        // Tính số levels tối ưu
        let optimal_levels = Self::calculate_optimal_levels(
            atr,
            min_price,
            max_price,
            atr_multiplier,
            self.max_grid_levels,
        );

        // Tính grid step từ ATR
        let grid_step = atr * atr_multiplier;

        // Tạo AnalysisGrid với số cells tối ưu
        let analysis_grid = AnalysisGrid::new(
            &analysis_data.iter().map(|c| c.c).collect::<Vec<_>>(),
            min_price,
            max_price,
            20,
        );

        // Tính interval_secs từ median gap
        let interval_secs = {
            let mut gaps: Vec<i64> = analysis_data.windows(2).map(|w| w[1].t - w[0].t).collect();
            gaps.sort();
            gaps.get(gaps.len() / 2).copied().unwrap_or(3600).max(1)
        };

        let transition = TransitionAnalysis::new(
            analysis_grid,
            &analysis_data.iter().map(|c| (c.t, c.c)).collect::<Vec<_>>(),
            interval_secs,
        );

        // Tạo TradingGrid centered tại current price với step từ ATR
        #[cfg(debug_assertions)]
        let _t2 = std::time::Instant::now();
        let n_cells = analysis_grid.num_cells();
        let mut result = Vec::with_capacity(n_cells);

        for cell in 0..n_cells {
            let (low, high) = Self::cell_range(&analysis_grid, cell);

            // Tạo grid với step từ ATR, centered tại midpoint của cell
            let cell_mid = (low + high) / 2.0;

            let Some(tg) = TradingGrid::centered(optimal_levels, cell_mid, grid_step) else {
                continue;
            };

            // Win probabilities từ transition
            let (up, down, _stay) = Self::cell_probs(&transition, cell);
            let total = up + down;
            let drift = if total > 0.0 {
                (up - down) / total
            } else {
                0.0
            };

            let k = tg.num_levels();
            let mut long_win = Vec::with_capacity(k);
            let mut short_win = Vec::with_capacity(k);

            for j in 0..k {
                let t = j as f64 / (k.max(2) - 1) as f64;

                let (model_long_p, model_short_p) = (
                    (0.5 + drift + (1.0 - t) * 0.10).clamp(0.25, 0.75),
                    (0.5 - drift + t * 0.10).clamp(0.25, 0.75),
                );

                let (final_long_p, final_short_p) = if cell < updated_grids.len() {
                    let ug = &updated_grids[cell];
                    let long_wins = ug.long_win_count(j);
                    let long_losses = ug.long_lost_count(j);
                    let short_wins = ug.short_win_count(j);
                    let short_losses = ug.short_lost_count(j);

                    let emp_long = if long_wins + long_losses >= 3 {
                        let rate = long_wins as f64 / (long_wins + long_losses) as f64;
                        rate.clamp(0.25, 0.75)
                    } else {
                        model_long_p
                    };

                    let emp_short = if short_wins + short_losses >= 3 {
                        let rate = short_wins as f64 / (short_wins + short_losses) as f64;
                        rate.clamp(0.25, 0.75)
                    } else {
                        model_short_p
                    };

                    (emp_long, emp_short)
                } else {
                    (model_long_p, model_short_p)
                };

                long_win.push(final_long_p);
                short_win.push(final_short_p);
            }

            // SL dựa trên ATR
            let sl_pct = (atr * sl_atr_multiplier / current_price).clamp(0.001, 0.1);

            let max_candles =
                (self.review_interval_secs / self.trading_candle_secs.max(1)) as usize;

            result.push(
                tg.with_sl_pct(sl_pct)
                    .with_weights_normal(4.0)
                    .with_max_candles(max_candles)
                    .with_win_probabilities(long_win, short_win),
            );
        }

        #[cfg(debug_assertions)]
        println!(
            "  [debug] VAG: {} grids ({} cells), {} levels, build {:.0}ms, total {:.0}ms",
            result.len(),
            n_cells,
            optimal_levels,
            _t2.elapsed().as_secs_f64() * 1000.0,
            _t.elapsed().as_secs_f64() * 1000.0
        );

        Ok(result)
    }
}
