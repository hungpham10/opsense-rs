//! # GridStrategy — Implementation of [`Strategy`] trait
//!
//! Phân tích grid từ dữ liệu nến, sử dụng TransitionAnalysis và Bayesian-style
//! win-probability updates dựa trên kết quả giao dịch thực tế.

use std::io::Error;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use analysis::{AnalysisGrid, TradingGrid, TransitionAnalysis};

use crate::qlib::{FetchFn, ParamFn, Strategy};

const INITIAL_CAPITAL: f64 = 100_000.0;

#[derive(Debug, Serialize, Deserialize)]
pub struct GridStrategy {
    grid_levels: usize,
    sl_pct: f64,
    lookback_secs: u64,
    review_interval_secs: u64,
    smoothing_k: f64,

    /// Số giây mỗi nến trading (vd: "5m" → 300).
    /// Dùng để tính max_candles trong mỗi review window.
    trading_candle_secs: u64,

    /// Timestamp của lần review gần nhất (0 = chưa review).
    #[serde(skip)]
    last_review: AtomicU64,
}

impl Clone for GridStrategy {
    fn clone(&self) -> Self {
        Self {
            smoothing_k: self.smoothing_k,
            grid_levels: self.grid_levels,
            sl_pct: self.sl_pct,
            lookback_secs: self.lookback_secs,
            review_interval_secs: self.review_interval_secs,
            trading_candle_secs: self.trading_candle_secs,
            last_review: AtomicU64::new(self.last_review.load(Ordering::Relaxed)),
        }
    }
}

impl GridStrategy {
    pub fn new(
        grid_levels: usize,
        sl_pct: f64,
        smoothing_k: f64,
        lookback_secs: u64,
        review_interval_secs: u64,
        trading_candle_secs: u64,
    ) -> Self {
        Self {
            grid_levels,
            sl_pct,
            smoothing_k,
            lookback_secs,
            review_interval_secs,
            trading_candle_secs,
            last_review: AtomicU64::new(0),
        }
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

    /// Cập nhật win probabilities dựa trên kết quả thực tế từ trading.
    ///
    /// Dùng Bayesian-style blending:
    /// - `empirical_rate = wins / (wins + losses)`
    /// - `weight = n_trades / (n_trades + k)` với k = smoothing constant (10)
    /// - `updated_p = weight * empirical_rate + (1 - weight) * model_p`
    ///
    /// Khi có ít trades → tin vào model nhiều hơn
    /// Khi có nhiều trades → tin vào kết quả thực tế nhiều hơn
    fn update_win_probabilities(grids: &mut [TradingGrid], min_trades: usize, smoothing_k: f64) {
        for grid in grids.iter_mut() {
            let n_levels = grid.num_levels();
            let mut new_long_win_p = Vec::with_capacity(n_levels);
            let mut new_short_win_p = Vec::with_capacity(n_levels);

            for j in 0..n_levels {
                // Long side
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

                // Short side
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

            // Apply updated probabilities
            *grid = std::mem::replace(grid, grid.clone())
                .with_win_probabilities(new_long_win_p, new_short_win_p);
        }
    }
}

#[typetag::serde(name = "grid")]
#[async_trait]
impl Strategy for GridStrategy {
    /// Layout params:
    /// - 0: kelly_fraction (cho forward loop)
    /// - 1: base_capital   (cho forward loop)
    /// - 2: grid_levels    (cho strategy)
    /// - 3: sl_pct         (cho strategy)
    /// - 4: lookback_secs  (cho strategy)
    fn init(&self) -> Vec<f64> {
        vec![
            0.25,                      // 0: kelly_fraction
            INITIAL_CAPITAL,           // 1: base_capital (fixed, ko optimize)
            self.grid_levels as f64,   // 2: grid_levels
            self.sl_pct,               // 3: sl_pct
            self.lookback_secs as f64, // 4: lookback_secs
        ]
    }

    /// Trả về timestamp rebuild tiếp theo.
    /// Luôn ghi nhận `current` là lần review gần nhất và schedule lần tiếp theo.
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
        let grid_levels = param(2).round() as usize;
        let sl_pct = param(3);
        let lookback_secs = param(4) as u64;

        // Cập nhật win probabilities dựa trên kết quả thực tế từ các trades đã đóng
        let mut updated_grids = grids.to_vec();
        Self::update_win_probabilities(&mut updated_grids, 3, self.smoothing_k);

        // Fetch analysis data trong [current - lookback, current)
        let from = current_ts.saturating_sub(lookback_secs);
        let analysis_data = fetch(from, current_ts).await?;

        if analysis_data.len() < 10 {
            return Err(Error::other("not enough analysis data"));
        }

        // Tính interval_secs từ median gap giữa các nến
        let interval_secs = {
            let mut gaps: Vec<i64> = analysis_data.windows(2).map(|w| w[1].t - w[0].t).collect();
            gaps.sort();
            gaps.get(gaps.len() / 2).copied().unwrap_or(3600).max(1)
        };

        // AnalysisGrid: sieve → grid
        let grid = AnalysisGrid::new(
            &analysis_data.iter().map(|c| c.c).collect::<Vec<_>>(),
            analysis_data
                .iter()
                .map(|c| c.l)
                .fold(f64::INFINITY, f64::min),
            analysis_data
                .iter()
                .map(|c| c.h)
                .fold(f64::NEG_INFINITY, f64::max),
            20,
        );
        let transition = TransitionAnalysis::new(
            grid,
            &analysis_data.iter().map(|c| (c.t, c.c)).collect::<Vec<_>>(),
            interval_secs,
        );

        // Tạo TradingGrid cho mỗi cell
        let n_cells = grid.num_cells();
        let mut result = Vec::with_capacity(n_cells);

        for cell in 0..n_cells {
            let (low, high) = Self::cell_range(&grid, cell);
            let Some(tg) = TradingGrid::new(grid_levels, low, high) else {
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

                // Nếu có updated grid cho cell này, dùng empirical probability thay vì model
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

                    // Dùng empirical rate nếu có đủ dữ liệu, ngược lại dùng model
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

            // Số nến tối đa grid có hiệu lực = review_interval / trading_candle_secs
            let max_candles =
                (self.review_interval_secs / self.trading_candle_secs.max(1)) as usize;

            result.push(
                tg.with_sl_pct(sl_pct)
                    .with_weights_normal(4.0)
                    .with_max_candles(max_candles)
                    .with_win_probabilities(long_win, short_win),
            );
        }

        Ok(result)
    }
}
