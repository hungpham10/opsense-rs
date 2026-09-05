// ── Trading Grid ─────────────────────────────────────────────────────────
///
/// Lưới giao dịch cố định, do người dùng cấu hình, dùng để đặt limit orders
/// theo tỉ lệ phân bổ vốn trên các bậc lưới.
/// Lưới giao dịch cố định — chia một khoảng giá thành `K` bậc đều nhau.
///
/// Khác với [`AnalysisGrid`] (tự động tìm số ô tối ưu từ dữ liệu),
/// trading grid do người dùng cấu hình: số bậc, khoảng giá, chiến lược phân bổ.
///
/// Hỗ trợ các chiến lược trọng số:
/// - [`Self::weights_normal`] — phân bổ chuẩn, tập trung ở giữa
/// - [`Self::weights_uniform`] — đều nhau
/// - [`Self::weights_linear`] — tuyến tính (tăng hoặc giảm dần)
///
/// # Ví dụ
/// ```
/// # use analysis::TradingGrid;
/// let g = TradingGrid::new(5, 76000.0, 77000.0).unwrap();
/// assert_eq!(g.num_levels(), 5);
/// assert_eq!(g.level_price(0), 76000.0);
/// assert_eq!(g.level_price(4), 77000.0);
/// assert!((g.step() - 250.0).abs() < 1e-9);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingGrid {
    /// Level prices, sorted ascending.
    levels: Vec<f64>,

    /// SL = entry * (1 ± sl_pct)  — dùng chung cho cả long/short.
    sl_pct: f64,

    /// Số nến tối đa grid này có hiệu lực. 0 = không giới hạn.
    max_candles: usize,

    /// Trọng số phân bổ vốn. Matrix [level × time] — strategy có thể tự build.
    weights: Vec<Vec<f64>>,

    /// Win probability cho long ở mỗi level.
    long_win_p: Vec<f64>,

    /// Win probability cho short ở mỗi level.
    short_win_p: Vec<f64>,

    /// Statistic: số lần long thắng của từng level.
    order_long_win_cnt: Vec<usize>,
    /// Statistic: số lần long thua của từng level.
    order_long_lost_cnt: Vec<usize>,
    /// Statistic: số lần short thắng của từng level.
    order_short_win_cnt: Vec<usize>,
    /// Statistic: số lần short thua của từng level.
    order_short_lost_cnt: Vec<usize>,
}

/// Defaults của 1 trading grid — (sl_pct, weights matrix, long/short win_p,
/// long_win_cnt, long_lost_cnt, short_win_cnt, short_lost_cnt).
type GridDefaults = (
    f64,
    Vec<Vec<f64>>,
    Vec<f64>,
    Vec<f64>,
    Vec<usize>,
    Vec<usize>,
    Vec<usize>,
    Vec<usize>,
);

impl TradingGrid {
    fn fill_defaults(levels: &[f64]) -> GridDefaults {
        let k = levels.len();
        let w = 1.0 / k as f64;
        let weights: Vec<Vec<f64>> = (0..k).map(|_| vec![w]).collect();
        (
            0.05,         // sl_pct
            weights,      // matrix [level × time], 1 column
            vec![0.5; k], // long_win_p default
            vec![0.5; k], // short_win_p default
            vec![0; k],   // order_long_win_cnt
            vec![0; k],   // order_long_lost_cnt
            vec![0; k],   // order_short_win_cnt
            vec![0; k],   // order_short_lost_cnt
        )
    }

    /// Tạo trading grid với `K` bậc đều nhau trong `[min, max]`.
    ///
    /// K levels → K-1 intervals, step = (max - min) / (K - 1).
    /// Trả về `None` nếu `K < 2` hoặc `max <= min`.
    pub fn new(levels: usize, min: f64, max: f64) -> Option<Self> {
        // NaN-safe: `max <= min` là false khi NaN — phải chặn cả NaN để không
        // tạo grid toàn NaN rồi lan ra report.
        if levels < 2 || max.partial_cmp(&min) != Some(std::cmp::Ordering::Greater) {
            return None;
        }
        let step = (max - min) / (levels - 1) as f64;
        let prices: Vec<f64> = (0..levels).map(|j| min + j as f64 * step).collect();
        let (sl, w, lw, sw, lwc, llc, swc, slc) = Self::fill_defaults(&prices);
        Some(Self {
            levels: prices,
            sl_pct: sl,
            weights: w,
            long_win_p: lw,
            short_win_p: sw,
            order_long_win_cnt: lwc,
            order_long_lost_cnt: llc,
            order_short_win_cnt: swc,
            order_short_lost_cnt: slc,
            max_candles: 0,
        })
    }

    /// Tạo trading grid với `K` bậc từ `start`, mỗi bậc cách `step`.
    ///
    /// Công thức: level[j] = start + j * step.
    /// `step > 0` → grid tăng dần, `step < 0` → grid giảm dần.
    /// Trả về `None` nếu `K < 2` hoặc `step == 0.0`.
    pub fn from_step(levels: usize, start: f64, step: f64) -> Option<Self> {
        if levels < 2 || step == 0.0 {
            return None;
        }
        let prices: Vec<f64> = (0..levels).map(|j| start + j as f64 * step).collect();
        let (sl, w, lw, sw, lwc, llc, swc, slc) = Self::fill_defaults(&prices);
        Some(Self {
            levels: prices,
            sl_pct: sl,
            weights: w,
            long_win_p: lw,
            short_win_p: sw,
            order_long_win_cnt: lwc,
            order_long_lost_cnt: llc,
            order_short_win_cnt: swc,
            order_short_lost_cnt: slc,
            max_candles: 0,
        })
    }

    /// Tạo trading grid `K` bậc, centered tại `center` với `step` cho trước.
    ///
    ///  Lưới đối xứng quanh `center`:
    ///  - min = center - (K-1)/2 * step
    ///  - max = center + (K-1)/2 * step
    pub fn centered(levels: usize, center: f64, step: f64) -> Option<Self> {
        if levels < 2 || step <= 0.0 {
            return None;
        }
        let half = (levels - 1) as f64 / 2.0;
        let start = center - half * step;
        Self::from_step(levels, start, step)
    }

    /// Tạo trading grid từ mảng giá các level (sẽ được sort).
    pub fn from_levels(mut levels: Vec<f64>) -> Option<Self> {
        if levels.len() < 2 {
            return None;
        }
        levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let (sl, w, lw, sw, lwc, llc, swc, slc) = Self::fill_defaults(&levels);
        Some(Self {
            levels,
            sl_pct: sl,
            weights: w,
            long_win_p: lw,
            short_win_p: sw,
            order_long_win_cnt: lwc,
            order_long_lost_cnt: llc,
            order_short_win_cnt: swc,
            order_short_lost_cnt: slc,
            max_candles: 0,
        })
    }

    /// Số bậc lưới (K).
    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }

    /// Giá của bậc thứ `j` (0-based, 0 = min, K-1 = max).
    pub fn level_price(&self, j: usize) -> f64 {
        self.levels[j]
    }

    /// Khoảng cách giữa các bậc liền kề (step).
    ///
    /// Giả định các bậc cách đều nhau, lấy step từ 2 bậc đầu tiên.
    pub fn step(&self) -> f64 {
        self.levels[1] - self.levels[0]
    }

    /// Trọng số phân bổ khối lượng tại level `j` ở nến thứ `t`.
    ///
    /// Tra trực tiếp vào matrix `weights[level][time]`.
    /// Nếu `t` vượt quá số cột, dùng cột cuối cùng (hết vòng đời).
    pub fn weight(&self, j: usize, t: usize) -> f64 {
        let col = t.min(self.weights[j].len().saturating_sub(1));
        self.weights[j][col]
    }

    /// Số cột (bước thời gian) của weight matrix.
    /// Khi `max_candles > 0` thì dùng `max_candles`, ngược lại là 1 (constant).
    fn weight_cols(&self) -> usize {
        if self.max_candles > 0 {
            self.max_candles
        } else {
            1
        }
    }

    pub fn long_win_pct(&self, j: usize) -> f64 {
        if j >= self.long_win_p.len() {
            0.0
        } else {
            self.long_win_p[j]
        }
    }

    pub fn short_win_pct(&self, j: usize) -> f64 {
        if j >= self.short_win_p.len() {
            0.0
        } else {
            self.short_win_p[j]
        }
    }

    /// Số lần long thắng của level `j`.
    pub fn long_win_count(&self, j: usize) -> usize {
        if j >= self.order_long_win_cnt.len() {
            0
        } else {
            self.order_long_win_cnt[j]
        }
    }

    /// Số lần long thua của level `j`.
    pub fn long_lost_count(&self, j: usize) -> usize {
        if j >= self.order_long_lost_cnt.len() {
            0
        } else {
            self.order_long_lost_cnt[j]
        }
    }

    /// Số lần short thắng của level `j`.
    pub fn short_win_count(&self, j: usize) -> usize {
        if j >= self.order_short_win_cnt.len() {
            0
        } else {
            self.order_short_win_cnt[j]
        }
    }

    /// Số lần short thua của level `j`.
    pub fn short_lost_count(&self, j: usize) -> usize {
        if j >= self.order_short_lost_cnt.len() {
            0
        } else {
            self.order_short_lost_cnt[j]
        }
    }

    /// Ghi nhận kết quả 1 trade tại level.
    /// `is_long` = true nếu là long, false nếu short.
    /// pnl > 0 → win, ngược lại → loss.
    pub fn record_trade_outcome(&mut self, level: usize, is_long: bool, pnl_pct: f64) {
        if pnl_pct > 0.0 {
            if is_long {
                if let Some(c) = self.order_long_win_cnt.get_mut(level) {
                    *c += 1;
                }
            } else if let Some(c) = self.order_short_win_cnt.get_mut(level) {
                *c += 1;
            }
        } else if is_long {
            if let Some(c) = self.order_long_lost_cnt.get_mut(level) {
                *c += 1;
            }
        } else if let Some(c) = self.order_short_lost_cnt.get_mut(level) {
            *c += 1;
        }
    }

    pub fn stoploss_pct(&self) -> f64 {
        self.sl_pct
    }

    /// Stop-loss price cho long ở level `j`: SL = entry * (1 - sl_pct)
    pub fn sl_long(&self, j: usize) -> f64 {
        self.levels[j] * (1.0 - self.sl_pct)
    }

    /// Stop-loss price cho short ở level `j`: SL = entry * (1 + sl_pct)
    pub fn sl_short(&self, j: usize) -> f64 {
        self.levels[j] * (1.0 + self.sl_pct)
    }

    // ── Builder methods ────────────────────────────────────────────────

    /// Set stop-loss percentage.
    pub fn with_sl_pct(mut self, sl_pct: f64) -> Self {
        self.sl_pct = sl_pct;
        self
    }

    /// Set weight vector (phân bổ vốn). Nhận `Vec<f64>` (base weight per level),
    /// tự động expand thành matrix với `max_candles` cột. Nếu độ dài không khớp, giữ nguyên.
    pub fn with_weights(mut self, base: Vec<f64>) -> Self {
        if base.len() == self.weights.len() {
            let cols = self.weight_cols();
            self.weights = base.into_iter().map(|w| vec![w; cols]).collect();
        }
        self
    }

    /// Set weight matrix trực tiếp `[level][time]`. Nếu số level không khớp, giữ nguyên.
    /// Cập nhật `max_candles` theo số cột của matrix.
    pub fn with_weight_matrix(mut self, matrix: Vec<Vec<f64>>) -> Self {
        if matrix.len() == self.weights.len() {
            self.max_candles = if matrix[0].len() > 1 {
                matrix[0].len()
            } else {
                0
            };
            self.weights = matrix;
        }
        self
    }

    /// Set weight vector từ chiến lược phân bổ chuẩn (Gaussian).
    pub fn with_weights_normal(mut self, sharpness: f64) -> Self {
        let base = self.weights_normal(sharpness);
        let cols = self.weight_cols();
        self.weights = base.into_iter().map(|w| vec![w; cols]).collect();
        self
    }

    /// Set weight vector từ chiến lược phân bổ đều.
    pub fn with_weights_uniform(mut self) -> Self {
        let base = self.weights_uniform();
        let cols = self.weight_cols();
        self.weights = base.into_iter().map(|w| vec![w; cols]).collect();
        self
    }

    /// Set weight vector từ chiến lược phân bổ tuyến tính.
    pub fn with_weights_linear(mut self, ascending: bool) -> Self {
        let base = self.weights_linear(ascending);
        let cols = self.weight_cols();
        self.weights = base.into_iter().map(|w| vec![w; cols]).collect();
        self
    }

    /// Set weight vector từ chiến lược **theo xu hướng** (trend-following).
    ///
    /// Tập trung khối lượng lớn về phía trend để tối đa lợi nhuận khi thị
    /// trường đi một chiều:
    /// - `direction > 0` (bullish) → nặng ở bậc thấp (phía LONG, dưới center).
    /// - `direction < 0` (bearish) → nặng ở bậc cao (phía SHORT, trên center).
    /// - `direction == 0` → uniform (không xu hướng).
    ///
    /// `strength ∈ [0,1]` quy đổi thành exponent `p ∈ [1, 5]` của phân bố luỹ
    /// thừa (0 = gần linear, 1 = cực đoan); kết quả chuẩn hoá (sum = 1.0).
    pub fn with_weights_trend(mut self, direction: f64, strength: f64) -> Self {
        let base = self.weights_trend(direction, strength);
        let cols = self.weight_cols();
        self.weights = base.into_iter().map(|w| vec![w; cols]).collect();
        self
    }

    /// Set long win probability vector.
    /// Nếu độ dài không khớp, giữ nguyên.
    pub fn with_long_win_p(mut self, long_win_p: Vec<f64>) -> Self {
        if long_win_p.len() == self.long_win_p.len() {
            self.long_win_p = long_win_p;
        }
        self
    }

    /// Set short win probability vector.
    /// Nếu độ dài không khớp, giữ nguyên.
    pub fn with_short_win_p(mut self, short_win_p: Vec<f64>) -> Self {
        if short_win_p.len() == self.short_win_p.len() {
            self.short_win_p = short_win_p;
        }
        self
    }

    /// Set số nến tối đa grid có hiệu lực. 0 = không giới hạn.
    ///
    /// Khi `max_candles` tăng, expand mỗi row của matrix bằng cách
    /// repeat giá trị cuối cùng để đủ số cột mới.
    pub fn with_max_candles(mut self, max_candles: usize) -> Self {
        if max_candles > self.weight_cols() {
            for row in &mut self.weights {
                let last = *row.last().unwrap_or(&0.0);
                row.resize(max_candles, last);
            }
        }
        self.max_candles = max_candles;
        self
    }

    /// Set cả long và short win probability vectors cùng lúc.
    pub fn with_win_probabilities(mut self, long_win_p: Vec<f64>, short_win_p: Vec<f64>) -> Self {
        if long_win_p.len() == self.long_win_p.len() && short_win_p.len() == self.short_win_p.len()
        {
            self.long_win_p = long_win_p;
            self.short_win_p = short_win_p;
        }
        self
    }

    /// Take-profit price cho long (level cao hơn kế).
    /// Nếu ko có (j là level cuối), trả về level hiện tại.
    pub fn tp_above(&self, j: usize) -> f64 {
        self.levels.get(j + 1).copied().unwrap_or(self.levels[j])
    }

    /// Take-profit price cho short (level thấp hơn kế).
    /// Nếu ko có (j là level đầu), trả về level hiện tại.
    pub fn tp_below(&self, j: usize) -> f64 {
        j.checked_sub(1)
            .and_then(|i| self.levels.get(i))
            .copied()
            .unwrap_or(self.levels[j])
    }

    /// Giá thấp nhất (bậc 0).
    pub fn min(&self) -> f64 {
        self.levels[0]
    }

    /// Giá cao nhất (bậc cuối).
    pub fn max(&self) -> f64 {
        self.levels[self.levels.len() - 1]
    }

    /// Tham chiếu tới mảng giá các bậc.
    pub fn levels(&self) -> &[f64] {
        &self.levels
    }

    // ── Chiến lược phân bổ vốn ──────────────────────────────────────────

    /// Trọng số **phân bổ chuẩn** (Gaussian), peak tại center, giảm dần về 2 đầu.
    ///
    /// `std_dev` = `num_levels / sharpness`. `sharpness` càng lớn → phân bổ
    /// càng tập trung ở center. Mặc định `sharpness = 4.0` (std_dev = K/4)
    /// phủ hết lưới với trọng số giảm dần đều về biên.
    ///
    /// Kết quả được chuẩn hoá (sum = 1.0).
    pub fn weights_normal(&self, sharpness: f64) -> Vec<f64> {
        let k = self.levels.len() as f64;
        let center = (k - 1.0) / 2.0;
        let std_dev = k / sharpness.max(1.0);
        let mut w: Vec<f64> = (0..self.levels.len())
            .map(|j| {
                let z = (j as f64 - center) / std_dev;
                (-0.5 * z * z).exp()
            })
            .collect();
        let sum: f64 = w.iter().sum();
        if sum > 0.0 {
            for v in &mut w {
                *v /= sum;
            }
        }
        w
    }

    /// Trọng số **đều**: mọi bậc nhận cùng tỉ lệ (1/K).
    pub fn weights_uniform(&self) -> Vec<f64> {
        let w = 1.0 / self.levels.len() as f64;
        vec![w; self.levels.len()]
    }

    /// Trọng số **tuyến tính**: tăng dần (`ascending = true`) hoặc giảm dần.
    ///
    /// - `ascending = true`: bậc thấp → cao, trọng số tăng dần
    /// - `ascending = false`: bậc cao → thấp, trọng số giảm dần
    ///
    /// Kết quả được chuẩn hoá (sum = 1.0).
    pub fn weights_linear(&self, ascending: bool) -> Vec<f64> {
        let n = self.levels.len() as f64;
        let raw: Vec<f64> = (0..self.levels.len())
            .map(|j| {
                if ascending {
                    (j + 1) as f64
                } else {
                    n - j as f64
                }
            })
            .collect();
        let sum: f64 = raw.iter().sum();
        raw.into_iter().map(|v| v / sum).collect()
    }

    /// Trọng số **theo xu hướng** — đặt khối lượng lớn đúng hướng trend.
    ///
    /// Entry dưới center là LONG, trên center là SHORT (xem
    /// `portfolio::evaluate_grid_entries`), nên:
    /// - bullish: LONG thắng khi giá lên → nặng bậc thấp: `w_j ∝ (K − j)^p`
    /// - bearish: SHORT thắng khi giá xuống → nặng bậc cao: `w_j ∝ (j + 1)^p`
    ///
    /// `strength ∈ [0,1]` → `p = 1 + 4·strength` (1 = linear, 5 = cực đoan).
    /// Kết quả chuẩn hoá sum = 1.0 (khớp `weights_normal`/`weights_linear`).
    pub fn weights_trend(&self, direction: f64, strength: f64) -> Vec<f64> {
        let k = self.levels.len() as f64;
        let p = 1.0 + strength.clamp(0.0, 1.0) * 4.0;
        let raw: Vec<f64> = (0..self.levels.len())
            .map(|j| {
                let j = j as f64;
                if direction > 0.0 {
                    (k - j).powf(p)
                } else if direction < 0.0 {
                    (j + 1.0).powf(p)
                } else {
                    1.0
                }
            })
            .collect();
        let sum: f64 = raw.iter().sum();
        raw.into_iter().map(|v| v / sum).collect()
    }

    // ── Fee-aware helpers ──────────────────────────────────────────────

    /// Minimum step cần để 1 trade có lời sau phí (roundtrip).
    ///
    /// Với LONG: gross_profit = step, fee_cost ≈ 2 × taker_fee_rate × entry.
    /// Cần step > 2 × fee_rate × entry để net_profit > 0.
    pub fn min_profitable_step(fee_rate: f64, at_price: f64) -> f64 {
        2.0 * fee_rate * at_price
    }

    /// Kiểm tra step hiện tại có đủ lớn để có lời sau phí không.
    pub fn is_step_profitable(&self, fee_rate: f64, at_price: f64) -> bool {
        self.step() > Self::min_profitable_step(fee_rate, at_price)
    }

    /// Số levels tối đa trong `[min, max]` sao cho step vẫn profitable.
    ///
    /// Tự động giảm K nếu khoảng giá quá hẹp so với fee.
    /// Trả về 2 nếu không đủ rộng (kích thước tối thiểu).
    pub fn max_levels_for_profit(min: f64, max: f64, fee_rate: f64) -> usize {
        let width = max - min;
        let min_step = Self::min_profitable_step(fee_rate, min);
        if min_step <= 0.0 || width <= min_step * 1.001 {
            return 2;
        }
        let k = (width / min_step).floor() as usize + 1;
        k.max(2)
    }
}

impl fmt::Display for TradingGrid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let k = self.num_levels();
        let step = self.step();
        let (min, max) = (self.min(), self.max());
        writeln!(
            f,
            "TradingGrid(K={}, min={:.2}, max={:.2}, step={:.4}, SL={:.2}%, max_candles={})",
            k,
            min,
            max,
            step,
            self.sl_pct * 100.0,
            self.max_candles,
        )?;

        if k <= 10 {
            for j in 0..k {
                let price = self.level_price(j);
                let w = self.weight(j, 0);
                let lw = self.long_win_pct(j);
                let sw = self.short_win_pct(j);

                let lwins = self.long_win_count(j);
                let lloss = self.long_lost_count(j);
                let ltotal = lwins + lloss;
                let lactual = if ltotal > 0 {
                    lwins as f64 / ltotal as f64 * 100.0
                } else {
                    f64::NAN
                };

                let swins = self.short_win_count(j);
                let sloss = self.short_lost_count(j);
                let stotal = swins + sloss;
                let sactual = if stotal > 0 {
                    swins as f64 / stotal as f64 * 100.0
                } else {
                    f64::NAN
                };

                write!(f, "  #{j}  {price:.2}  w={w:.3}")?;
                write!(f, "  L={lw:.2}")?;
                if ltotal > 0 {
                    write!(f, "/{lactual:.1}%")?;
                }
                write!(f, "  S={sw:.2}")?;
                if stotal > 0 {
                    write!(f, "/{sactual:.1}%")?;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

// ── Analysis Grid ─────────────────────────────────────────────────────────

use serde::{Deserialize, Serialize};
use std::fmt;

/// Cấu hình cho thuật toán sàng phân cấp ([`AnalysisGrid::with_config`]).
///
/// Điều khiển điều kiện dừng: vòng lặp sieve sẽ dừng khi mức tăng
/// crossings đột biến vượt quá ngưỡng, báo hiệu overfitting.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct SieveConfig {
    /// Hệ số nhân: dừng khi `delta > prev_delta * delta_multiplier`.
    /// Mặc định `1.3` (tăng ≥30% so với level trước).
    pub delta_multiplier: f64,
    /// Delta tuyệt đối tối thiểu để kích hoạt điều kiện dừng.
    /// Mặc định `0.02` (tỉ lệ crossings mới ≥2% tổng segments).
    pub min_abs_delta: f64,
}

impl Default for SieveConfig {
    fn default() -> Self {
        Self {
            delta_multiplier: 1.3,
            min_abs_delta: 0.02,
        }
    }
}

/// Lưới phân tích 1D — thuật toán sàng phân cấp để tìm số ô tối ưu
/// từ dữ liệu (tỉ lệ crossing thấp nhất với số ô nhiều nhất và kích thước ô nhỏ nhất).
///
/// Dùng cho phân tích occupancy, transition, crossings.
/// **Không phải** trading grid — trading grid là lưới lệnh cố định K bậc trong strategy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AnalysisGrid {
    pub step: f64,
    pub offset: f64,
    pub min: f64,
    pub max: f64,
    pub max_bit: usize,
}

impl AnalysisGrid {
    /// Cell index chứa value `y` (0-based).
    pub fn cell(&self, y: f64) -> usize {
        if y <= self.min {
            return 0;
        }
        if y >= self.max {
            return self.num_cells().saturating_sub(1);
        }
        ((y - self.offset) / self.step).floor() as usize
    }

    /// Số lượng ô.
    pub fn num_cells(&self) -> usize {
        let last = ((self.max - self.offset) / self.step).ceil() as usize;
        last.max(1)
    }

    /// Số đường lưới (= num_cells + 1).
    pub fn num_lines(&self) -> usize {
        self.num_cells() + 1
    }

    /// Đếm số crossings giữa các value liên tiếp.
    pub fn crossings(&self, values: &[f64]) -> usize {
        let mut count = 0;
        for pair in values.windows(2) {
            let a = self.cell(pair[0]);
            let b = self.cell(pair[1]);
            count += b.abs_diff(a);
        }
        count
    }

    /// Đếm số điểm trong mỗi cell, theo từng khoảng thời gian.
    ///
    /// `data` — các `(timestamp_unix_secs, value)` đã sắp xếp.
    /// `interval_secs` — độ rộng mỗi bucket thời gian (giây).
    ///
    /// Trả về `Vec<Vec<usize>>` với `result[bucket][cell]` = số điểm
    /// thuộc bucket thời gian đó và cell đó.
    pub fn occupancy(&self, data: &[(i64, f64)], interval_secs: i64) -> Vec<Vec<usize>> {
        if data.is_empty() || interval_secs <= 0 {
            return Vec::new();
        }

        let t_start = data[0].0;
        let t_end = data[data.len() - 1].0;
        let num_buckets = ((t_end - t_start) / interval_secs) as usize + 1;
        let num_cells = self.num_cells();

        let mut result = vec![vec![0usize; num_cells]; num_buckets];

        for &(ts, val) in data {
            let bucket = ((ts - t_start) / interval_secs) as usize;
            let cell = self.cell(val);
            debug_assert!(bucket < num_buckets, "bucket out of range");
            result[bucket][cell] += 1;
        }

        result
    }

    /// Tìm lưới tối ưu từ dữ liệu, dùng thuật toán sàng phân cấp.
    /// `min` / `max` là biên vật lý (thường là 0 và disk_capacity).
    ///
    /// Algorithm: chuẩn hoá dữ liệu về thang `[0, 2^max_bit)`, duyệt
    /// step = `capacity / 2^level` và đếm crossings.
    /// Dừng khi delta crossings tăng đột biến theo [`SieveConfig::default`].
    /// `offset` được đặt bằng `min` để `cell()` tính đúng index.
    pub fn new(values: &[f64], min: f64, max: f64, max_bit: usize) -> Self {
        Self::with_config(values, min, max, max_bit, &SieveConfig::default())
    }

    /// Như [`new`](Self::new) nhưng cho phép tuỳ chỉnh điều kiện dừng
    /// của thuật toán sàng qua [`SieveConfig`].
    pub fn with_config(
        values: &[f64],
        min: f64,
        max: f64,
        max_bit: usize,
        config: &SieveConfig,
    ) -> Self {
        let capacity = max - min;
        let n = values.len();

        if capacity <= 0.0 || n == 0 {
            return Self {
                step: 1.0,
                offset: min,
                min,
                max,
                max_bit,
            };
        }

        // ── Build hierarchical sieve ──
        let scale = (1u64 << max_bit) as f64;

        let cell = values
            .iter()
            .map(|&y| {
                if y <= min {
                    0
                } else if y >= max {
                    (1u64 << max_bit) - 1
                } else {
                    ((y - min) / capacity * scale) as u64
                }
            })
            .collect::<Vec<_>>();

        let total_segments = n.saturating_sub(1);
        let mut best = Self {
            step: capacity,
            offset: min,
            min,
            max,
            max_bit,
        };
        let mut prev_cross = 0;
        let mut prev_delta = 0.0;

        for level in 0..=max_bit {
            let step = capacity / (1u64 << level) as f64;
            if step <= 0.0 || !step.is_finite() {
                continue;
            }

            let shift = max_bit - level;
            let crossings = {
                let mut c = 0;

                for i in 0..total_segments {
                    let a = cell[i] >> shift;
                    let b = cell[i + 1] >> shift;
                    c += if b > a {
                        (b - a) as usize
                    } else {
                        (a - b) as usize
                    };
                }
                c
            };

            let delta = if level == 0 {
                0.0
            } else {
                (crossings - prev_cross) as f64 / total_segments.max(1) as f64
            };

            if level >= 2
                && delta > prev_delta * config.delta_multiplier
                && delta >= config.min_abs_delta
                && prev_delta > 0.0
            {
                break;
            }

            best = Self {
                step,
                min,
                max,
                max_bit,
                offset: min,
            };
            prev_cross = crossings;
            prev_delta = delta;
        }

        best
    }

    // ── Cell rendering ──

    /// Khoảng `[low, high)` của cell `cell`.
    /// Trả về `None` nếu `cell >= num_cells`.
    pub fn cell_range(&self, cell: usize) -> Option<(f64, f64)> {
        if cell >= self.num_cells() {
            return None;
        }
        let low = self.min + cell as f64 * self.step;
        Some((low, low + self.step))
    }

    /// Danh sách tất cả các cell: `(index, low, high)`.
    pub fn cell_ranges(&self) -> Vec<(usize, f64, f64)> {
        (0..self.num_cells())
            .filter_map(|i| self.cell_range(i).map(|(lo, hi)| (i, lo, hi)))
            .collect()
    }
}

impl fmt::Display for AnalysisGrid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "AnalysisGrid — {} cells", self.num_cells())?;
        writeln!(f, "  Range  : [{:.4}, {:.4}]", self.min, self.max)?;
        writeln!(f, "  Step   : {:.6}", self.step)?;
        writeln!(f, "  Offset : {:.4}", self.offset)?;
        writeln!(f, "  MaxBit : {}", self.max_bit)?;
        for (i, lo, hi) in &self.cell_ranges() {
            writeln!(f, "  [{:>3}] {:>10.4} – {:>10.4}", i, lo, hi)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod trend_weights_tests {
    use super::*;

    fn assert_close(a: f64, b: f64, tol: f64) {
        assert!((a - b).abs() < tol, "expected {a} ≈ {b}");
    }

    #[test]
    fn bullish_weights_heavy_on_low_levels() {
        let g = TradingGrid::new(5, 100.0, 110.0).unwrap();
        let w = g.weights_trend(1.0, 1.0);
        assert_close(w.iter().sum(), 1.0, 1e-9);
        // Heavy ở bậc thấp (phía LONG), giảm dần về bậc cao.
        assert!(w[0] > w[1] && w[1] > w[2] && w[2] > w[3] && w[3] > w[4]);
    }

    #[test]
    fn bearish_weights_heavy_on_high_levels() {
        let g = TradingGrid::new(5, 100.0, 110.0).unwrap();
        let w = g.weights_trend(-1.0, 1.0);
        assert_close(w.iter().sum(), 1.0, 1e-9);
        // Heavy ở bậc cao (phía SHORT), tăng dần theo bậc.
        assert!(w[0] < w[1] && w[1] < w[2] && w[2] < w[3] && w[3] < w[4]);
    }

    #[test]
    fn neutral_weights_are_uniform() {
        let g = TradingGrid::new(5, 100.0, 110.0).unwrap();
        let w = g.weights_trend(0.0, 1.0);
        let u = 1.0 / 5.0;
        for v in w {
            assert_close(v, u, 1e-9);
        }
    }

    #[test]
    fn zero_strength_reduces_to_linear() {
        let g = TradingGrid::new(5, 100.0, 110.0).unwrap();
        let w = g.weights_trend(1.0, 0.0);
        let raw: Vec<f64> = (0..5).map(|j| (5 - j) as f64).collect();
        let sum: f64 = raw.iter().sum();
        for (got, e) in w.iter().zip(raw.iter().map(|e| e / sum)) {
            assert_close(*got, e, 1e-9);
        }
    }

    #[test]
    fn with_weights_trend_expands_to_matrix() {
        let g = TradingGrid::new(5, 100.0, 110.0)
            .unwrap()
            .with_max_candles(10)
            .with_weights_trend(-1.0, 0.5);
        assert_eq!(g.weight_cols(), 10);
        for j in 0..5 {
            let row = g.weights[j].clone();
            assert_eq!(row.len(), 10);
            for (t, item) in row.iter().enumerate().take(10) {
                assert_close(*item, g.weight(j, t), 1e-12);
            }
        }
    }
}
