use std::fmt;

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
/// # use opsense_qlib::TradingGrid;
/// let g = TradingGrid::new(5, 76000.0, 77000.0).unwrap();
/// assert_eq!(g.num_levels(), 5);
/// assert_eq!(g.level_price(0), 76000.0);
/// assert_eq!(g.level_price(4), 77000.0);
/// assert!((g.step() - 250.0).abs() < 1e-9);
/// ```
#[derive(Debug, Clone)]
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

    /// Ma trận trọng số đầy đủ — `matrix[level][time]`.
    ///
    /// Cần để **serialize** lưới: `weight(j, t)` trả giá trị đã clamp theo cột
    /// cuối, nên không đọc ngược được ra hình dạng gốc. Không có hàm này thì
    /// `TradingGrid` **không round-trip được** từ ngoài crate ⇒ không lưu
    /// được plan vào station, và mọi thứ phải rebuild mỗi lần gọi.
    pub fn weight_matrix(&self) -> &[Vec<f64>] {
        &self.weights
    }

    /// Số cột (bước thời gian) của weight matrix.
    /// Khi `max_candles > 0` thì dùng `max_candles`, ngược lại là 1 (constant).
    pub fn weight_cols(&self) -> usize {
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

    /// Nạp thống kê lệnh đã đóng cho từng level (long/short win/lost).
    ///
    /// Cần cho plan do **script** dựng lại: script chỉ quyết định cấu trúc
    /// (levels, sl, weights, win-prob), còn bộ đếm win/lost là trí nhớ của kernel
    /// — nếu script trả plan mới mà quên chép bộ đếm thì win-prob "học" từ lịch
    /// sử giao dịch sẽ bị reset mỗi lần rebuild. Vì vậy [`crate::plan::GridPlan`]
    /// copy lại đúng bộ đếm của plan cũ theo vị trí (cell, level).
    pub fn with_outcome_counts(
        mut self,
        long_win: Vec<usize>,
        long_lost: Vec<usize>,
        short_win: Vec<usize>,
        short_lost: Vec<usize>,
    ) -> Self {
        let n = self.levels.len();
        let take = |v: Vec<usize>| -> Vec<usize> {
            if v.len() == n {
                v
            } else {
                vec![0; n]
            }
        };
        self.order_long_win_cnt = take(long_win);
        self.order_long_lost_cnt = take(long_lost);
        self.order_short_win_cnt = take(short_win);
        self.order_short_lost_cnt = take(short_lost);
        self
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

    /// Số nến tối đa lưới này còn hiệu lực. `0` = không giới hạn.
    ///
    /// Cần để serialize: `weight_cols()` suy ra từ nó, mà `weight_matrix()`
    /// đã đã lưu cả số cột thật — đọc `max_candles` là cách duy nhất dựng lại
    /// hành vi "hết vòng đời" sau khi khôi phục từ station.
    pub fn max_candles(&self) -> usize {
        self.max_candles
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

    /// Take-profit chọn theo **tỉ lệ RR mục tiêu** thay vì bậc kề.
    ///
    /// Quét từ `j` ra ngoài và lấy **bậc đầu tiên** mà
    /// `reward / risk >= target_rr`. `risk` = `entry × sl_pct` (đối xứng hai
    /// chiều: `sl_long(j)` = `entry × (1 - sl_pct)` nên khoảng cách là
    /// `entry × sl_pct`). Bậc nào xa hơn thì reward chỉ lớn hơn, nên bậc đầu
    /// tiên thoả là bậc **rẻ nhất** còn đạt ⇒ không kéo TP đi xa vô ích.
    ///
    /// `None` = **không bậc nào trong lưới đạt** ⇒ caller phải từ chối đặt lệnh.
    /// Quan trọng: `None` KHÔ được quy về `tp_above`/`tp_below`, vì bậc kề
    /// có thể RR < yêu cầu ⇒ đặt lệnh thì lỗ ngay từ bậc đầu tiên chạm bậc
    /// kề đó. "Không có bậc nào đạt" và "có bậc kề nhưng RR quá thấp" là hai
    /// chuyện khác nhau, và chỉ cái thứ nhất mới là lý do bỏ lệnh.
    pub fn tp_for_rr(&self, j: usize, target_rr: f64, long: bool) -> Option<f64> {
        let entry = self.levels.get(j)?;
        let risk = entry * self.sl_pct;
        if !risk.is_finite() || risk <= 0.0 || !target_rr.is_finite() || target_rr <= 0.0 {
            return None;
        }
        if long {
            self.levels
                .iter()
                .skip(j + 1)
                .find(|&&tp| (tp - entry) / risk >= target_rr)
                .copied()
        } else {
            self.levels
                .iter()
                .take(j)
                .rev()
                .find(|&&tp| (entry - tp) / risk >= target_rr)
                .copied()
        }
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
    /// `portfolio::open_orders`), nên:
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

    /// **Tỉ lệ thắng tối thiểu để một lệnh hòa vốn** với TP/SL cho trước.
    ///
    /// `reward` = lợi nhuận thô khi chạm TP (dương, dạng tỉ lệ)
    /// `risk`   = lỗ thô khi chạm SL (dương, dạng tỉ lệ)
    /// `fee_rate` = phí **mỗi phía**; một lệnh khứ hồi trả 2 lần.
    ///
    /// Công thức: `win_p × reward = (1 − win_p) × risk + 2 × fee`
    /// ⇒ `win_p = (risk + 2 × fee) / (reward + risk)`.
    ///
    /// # Vì sao cần hàm này
    ///
    /// `grid.rhai` từng chặn trần `win_p` ở **0,75** — con số đó là **ràng buộc
    /// độ tin cậy** (đừng tin mô hình/thống kê quá đà), nhưng lại được dùng như
    /// **ràng buộc kinh tế**. Với TP = 1 bước lưới (0,271%) và SL = 0,8%, ngưỡng
    /// hòa vốn là **78,5%**, tức cao hơn trần ⇒ hệ thống **luôn âm** dù dữ liệu
    /// thật có tốt. Đo: `P(chạm TP trước) = risk/(reward+risk) = 74,77%`, khớp
    /// prior 0,75 — tức thiết kế hòa vốn ở mức random walk **cộng phí**, nên
    /// thua chắc.
    ///
    /// Trần độ-tin-cậy phải tách khỏi ngưỡng này (xem `grid.rhai`).
    pub fn breakeven_win_p(reward: f64, risk: f64, fee_rate: f64) -> f64 {
        if reward <= 0.0 || risk <= 0.0 {
            return f64::NAN;
        }
        (risk + 2.0 * fee_rate) / (reward + risk)
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

#[cfg(test)]
mod breakeven_tests {
    use super::TradingGrid;

    /// Con số đo được trên chính `strategies/binance/grid.rhai`: TP = 1 bước
    /// lưới, SL = 0,8%, phí 0,02%/phía. Trần `clamp01` cũ là 0,75 ⇒ hệ thống
    /// âm ở **mọi** tham số hợp lệ. Test này chốt lại để lỗi đó không quay lại.
    #[test]
    fn breakeven_above_old_clamp_ceiling() {
        let be = TradingGrid::breakeven_win_p(0.00271, 0.008, 0.0002);
        assert!(
            be > 0.75,
            "ngưỡng hòa vốn {be} phải cao hơn trần clamp01 cũ 0.75, nếu không \
             thì trần độ-tin-cậy lại thành trần kinh tế và chiến lúc luôn âm"
        );
    }

    /// Ở đúng ngưỡng hòa vốn, kỳ vọng mỗi lệnh bằng 0 — phí đã nằm trong công
    /// thức (nhân 2 vì khứ hồi vào/ra). Nếu test này lệch thì công thức sai.
    #[test]
    fn breakeven_gives_zero_expectancy() {
        for (reward, risk, fee) in [
            (0.00271, 0.008, 0.0002),
            (0.005, 0.005, 0.0002),
            (0.001, 0.002, 0.001),
        ] {
            let be = TradingGrid::breakeven_win_p(reward, risk, fee);
            let ev = be * reward - (1.0 - be) * risk - 2.0 * fee;
            assert!(
                ev.abs() < 1e-12,
                "({reward}, {risk}, {fee}): E = {ev}, phải bằng 0 tại ngưỡng hòa vốn"
            );
        }
    }

    /// Thắng nhiều hơn ngưỡng hòa vốn thì E dương, thua thì E âm — đảo chiều
    /// đúng quanh điểm hòa vốn.
    #[test]
    fn expectancy_signs_around_breakeven() {
        let (reward, risk, fee) = (0.00271, 0.008, 0.0002);
        let be = TradingGrid::breakeven_win_p(reward, risk, fee);
        let ev = |w: f64| w * reward - (1.0 - w) * risk - 2.0 * fee;
        assert!(ev(be + 0.02) > 0.0, "trên ngưỡng phải lãi");
        assert!(ev(be - 0.02) < 0.0, "dưới ngưỡng phải lỗ");
    }

    /// `reward = 0` (mốc cuối cùng không có TP phía trên ⇒ `tp_above` trả về
    /// chính nó) thì **không tỉ lệ thắng nào** hòa vốn. Trả `NaN` để script
    /// nhận ra và không đặt trần sai — trả 1.0 sẽ giấu mất sự thật là mốc đó
    /// lỗ chắc.
    #[test]
    fn zero_reward_is_undefined_not_one() {
        assert!(TradingGrid::breakeven_win_p(0.0, 0.008, 0.0002).is_nan());
        assert!(TradingGrid::breakeven_win_p(0.00271, 0.0, 0.0002).is_nan());
        assert!(TradingGrid::breakeven_win_p(-1.0, 0.008, 0.0002).is_nan());
    }

    // ── `tp_for_rr`: TP chọn theo RR mục tiêu ────────────────────────────
    //
    // Grid thử nghiệm: 5 mốc, mỗi mốc cách nhau 0.4% giá, `sl_pct = 0.008`
    // ⇒ risk ≈ 0.8% giá ⇒ RR mỗi bậc kề ≈ 0.5. Đúng vùng RR của strategy thật
    // (bậc kề 0.42, hai bậc 0.85), nên test bắt được cả ca "chọn bậc kề" lẫn
    // ca "phải nhảy bậc".
    fn rr_grid() -> TradingGrid {
        TradingGrid::new(5, 100.0, 101.6)
            .expect("grid hợp lệ")
            .with_sl_pct(0.008)
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn rr_picks_first_level_that_clears_target() {
        let g = rr_grid();
        // entry = mốc 1 (100.4), risk = 0.8032.
        //   → mốc 2: reward 0.4 ⇒ RR 0.4980
        //   → mốc 3: reward 0.8 ⇒ RR 0.9960
        // min_rr = 0.4 ⇒ mốc 2 (bậc kề, RR đã đủ).
        assert!(close(
            g.tp_for_rr(1, 0.4, true).expect("mốc 2 đạt RR"),
            100.8
        ));
        // min_rr = 0.6 ⇒ phải NHẢY lên mốc 3, không dừng ở mốc 2.
        assert!(close(
            g.tp_for_rr(1, 0.6, true).expect("mốc 3 đạt RR"),
            101.2
        ));
    }

    #[test]
    fn rr_none_when_no_level_clears_target() {
        let g = rr_grid();
        // Mọi bậc trên đều < RR 2.0 ⇒ phải từ chối, KHÔNG rơi về bậc kề.
        assert!(g.tp_for_rr(1, 2.0, true).is_none());
    }

    #[test]
    fn rr_short_mirrors_long() {
        let g = rr_grid();
        // entry = mốc 3 (101.2), risk = 0.8096.
        //   → mốc 2: RR 0.4941   → mốc 1: RR 0.9881
        assert!(close(
            g.tp_for_rr(3, 0.4, false).expect("mốc 2 đạt RR"),
            100.8
        ));
        assert!(close(
            g.tp_for_rr(3, 0.6, false).expect("mốc 1 đạt RR"),
            100.4
        ));
        assert!(g.tp_for_rr(3, 2.0, false).is_none());
    }

    #[test]
    fn rr_edge_levels_have_no_room_in_that_direction() {
        let g = rr_grid();
        // Mốc cuối không có mốc nào cao hơn ⇒ TP phía trên không tồn tại.
        assert!(g.tp_for_rr(4, 0.1, true).is_none());
        // Mốc đầu không có mốc nào thấp hơn ⇒ TP phía dưới không tồn tại.
        assert!(g.tp_for_rr(0, 0.1, false).is_none());
        // Ngược chiều thì vẫn tìm thấy (mốc đầu có mốc cao hơn).
        assert!(g.tp_for_rr(0, 0.1, true).is_some());
    }

    #[test]
    fn rr_rejects_nonpositive_target() {
        let g = rr_grid();
        // `min_rr = 0` là "tắt RR" ở kernel (rơi về `tp_above`), nên hàm này
        // trả `None` thay vì âm thầm trả bậc đầu tiên — hai ngữ nghĩa khác nhau.
        assert!(g.tp_for_rr(1, 0.0, true).is_none());
        assert!(g.tp_for_rr(1, -1.0, true).is_none());
        // `j` ngoài dải cũng phải an toàn, không panic.
        assert!(g.tp_for_rr(99, 0.5, true).is_none());
    }
}
