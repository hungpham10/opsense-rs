//! # Phân tích xu hướng một chuỗi timeseries
//!
//! Ba câu hỏi, ba hàm:
//!
//! | Câu hỏi | Hàm |
//! |---|---|
//! | Nó **đi đâu**, nhanh cỡ nào? | [`TrendAnalysis::direction`], [`TrendAnalysis::slope_per_day`] |
//! | Nó **dao động** quanh xu hướng cỡ nào? | [`TrendAnalysis::amplitude`] |
//! | Bao lâu nữa nó **chạm** mốc `target`? | [`TrendAnalysis::hours_to`] |
//!
//! Không phụ thuộc crate nào khác — đây là toán thuần trên `[(ts, value)]`.
//! Lớp trên ([`crate::grid`], [`crate::transition`]) ghép thêm ngữ cảnh; xem
//! `opsense-rhai/src/capacity.rs` để thấy một ứng dụng cụ thể.
//!
//! ## Biên đường chéo
//!
//! Dữ liệu thật không đi thẳng — nó **dao động quanh một xu hướng**. Vẽ `t`
//! ngang, `value` dọc thì dữ liệu là một dải dốc, không phải một đường. Dải đó
//! có hai mép, và cả hai đều dốc:
//!
//! ```text
//!   value
//!     │               ╱ ← mép trên = trend(h) + amplitude
//!     │            ╱╱╱
//!     │         ╱╱╱   ← trend(h)   (đường hồi quy)
//!     │      ╱╱╱╱╱╱╱  ← mép dưới = trend(h) − amplitude
//!     │   ╱╱╱╱╱
//!     └────────────────────────────► t
//! ```
//!
//! Vì hai mép cùng dốc, chúng **cắt** một đường ngang (biên vật lý) tại hai thời
//! điểm khác nhau — chênh nhau đúng `2 × amplitude / slope`. Số nhỏ hơn là cái
//! dùng để cảnh báo; số lớn hơn là cái dùng để lập kế hoạch. Chênh lệch đó mới
//! là thứ đáng quan tâm, và chỉ nhìn đường hồi quy thì không thấy.

/// Hướng đi của chuỗi.
///
/// Hướng và biên độ là **hai trục độc lập** (xem [`TrendAnalysis::amplitude`]):
/// `Rising` + biên độ lớn nghĩa là "dao động quanh một xu hướng đi lên" — tình
/// huống phổ biến nhất, và cũng là tình huống dễ đoán sai nhất nếu chỉ nhìn
/// đường hồi quy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Đi lên.
    Rising,
    /// Đi xuống.
    Falling,
    /// Đứng yên — dao động quanh một mức.
    Flat,
}

impl Direction {
    /// Tên ổn định để đưa vào label / log.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rising => "rising",
            Self::Falling => "falling",
            Self::Flat => "flat",
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Cấu hình [`TrendAnalysis`].
///
/// Mọi ngưỡng ở đây là **tương đối** (so với biên độ đo được) hoặc **đơn vị
/// của chính value** — không có hằng số tuyệt đối nào. Nhờ vậy cùng một cấu
/// hình chạy được cho phần trăm disk, byte, hay độ dài queue.
#[derive(Debug, Clone, Copy)]
pub struct TrendConfig {
    /// Ít nhất bao nhiêu điểm thì mới dựng được. Dưới ngưỡng này [`TrendAnalysis::new`]
    /// trả `None` — vài điểm rác không đủ để nói "đang đi lên" hay không.
    pub min_samples: usize,

    /// Phân vị của |phần dư| dùng làm biên độ. `0.95` = mép trên vượt 95% số
    /// quan sát — nhịp dao động thường là **đỉnh**, không phải trung bình.
    pub amplitude_quantile: f64,

    /// Sàn cho biên độ, **cùng đơn vị với value**.
    ///
    /// Mặc định `0.0`: chuỗi thật sự phẳng thì biên hẹp là **đúng** — đừng bịa
    /// ra một dải chỉ để có cái gì đó vẽ. Đặt sàn khi nghi ngờ phần dư nhỏ là
    /// nhiễu đo chứ không phải dao động thật (ví dụ giá trị làm tròn số nguyên).
    pub min_amplitude: f64,

    /// Hệ số để coi là "có hướng". Xem [`TrendAnalysis::direction`].
    pub significance: f64,
}

impl Default for TrendConfig {
    fn default() -> Self {
        Self {
            min_samples: 8,
            amplitude_quantile: 0.95,
            min_amplitude: 0.0,
            significance: 2.0,
        }
    }
}

/// Một mốc dự đoán: giá trị xu hướng và hai mép của biên đường chéo.
#[derive(Debug, Clone, Copy)]
pub struct Projection {
    /// Mốc thời gian dự đoán (unix giây).
    pub ts: i64,
    /// Số giờ tính từ mốc quan sát cuối (có thể âm — nhìn về quá khứ).
    pub hours: f64,
    /// Giá trị xu hướng tại `ts`.
    pub trend: f64,
    /// Mép dưới = `trend − amplitude`.
    pub low: f64,
    /// Mép trên = `trend + amplitude`.
    pub high: f64,
}

/// Kết quả phân tích xu hướng của một chuỗi.
///
/// Dựng bằng [`TrendAnalysis::new`]. Dùng `Projection` ([`TrendAnalysis::project`])
/// cho một mốc, [`TrendAnalysis::hours_to`] cho một mốc ngang.
#[cfg_attr(
    feature = "rhai",
    opsense_macros::rhai_class(
        constructor = "trend_fit",
        accessors(
            "trend_current" -> |t: &mut Self| -> f64 { t.current() },
            "trend_direction" -> |t: &mut Self| -> String { t.direction().as_str().into() },
            "trend_samples" -> |t: &mut Self| -> i64 { t.samples() as i64 },
            "trend_span_secs" -> |t: &mut Self| -> i64 { t.span_secs() },
            "trend_origin_ts" -> |t: &mut Self| -> i64 { t.origin_ts() },
            "trend_anchor_ts" -> |t: &mut Self| -> i64 { t.anchor_ts() },
            "trend_slope_per_sec" -> |t: &mut Self| -> f64 { t.slope_per_sec() },
            "trend_slope_per_hour" -> |t: &mut Self| -> f64 { t.slope_per_hour() },
            "trend_slope_per_day" -> |t: &mut Self| -> f64 { t.slope_per_day() },
            "trend_r2" -> |t: &mut Self| -> f64 { t.r2() },
            "trend_residual_std" -> |t: &mut Self| -> f64 { t.residual_std() },
            "trend_amplitude" -> |t: &mut Self| -> f64 { t.amplitude() },
            "trend_amplitude_rel" -> |t: &mut Self| -> f64 { t.amplitude_rel() },
            "trend_offset" -> |t: &mut Self| -> f64 { t.trend_offset() },
            "trend_value_at" -> |t: &mut Self, ts: i64| -> f64 { t.value_at(ts) },
            "trend_hours_to" -> |t: &mut Self, target: f64| -> rhai::Dynamic {
                match t.hours_to(target) {
                    Some(h) => rhai::Dynamic::from(h),
                    None => rhai::Dynamic::UNIT,
                }
            },
            "trend_hours_to_upper_envelope" -> |t: &mut Self, target: f64| -> rhai::Dynamic {
                match t.hours_to_upper_envelope(target) {
                    Some(h) => rhai::Dynamic::from(h),
                    None => rhai::Dynamic::UNIT,
                }
            },
            "trend_project" -> |t: &mut Self, hours: f64| -> rhai::Dynamic {
                let p = t.project(hours);
                let mut m = rhai::Map::new();
                m.insert("hours".into(), rhai::Dynamic::from(p.hours));
                m.insert("ts".into(), rhai::Dynamic::from(p.ts));
                m.insert("trend".into(), rhai::Dynamic::from(p.trend));
                m.insert("low".into(), rhai::Dynamic::from(p.low));
                m.insert("high".into(), rhai::Dynamic::from(p.high));
                rhai::Dynamic::from(m)
            }
        )
    )
)]
#[derive(Debug, Clone)]
pub struct TrendAnalysis {
    /// Mốc gốc của đường hồi quy (mốc quan sát hữu hạn đầu tiên).
    origin_ts: i64,
    /// value / giây.
    slope_per_sec: f64,
    /// Giá trị tại `origin_ts`.
    intercept: f64,
    r2: f64,
    residual_std: f64,
    direction: Direction,
    /// Nửa bề rộng biên đường chéo, cùng đơn vị với value.
    amplitude: f64,
    /// `max − min` của dữ liệu — thang để chuẩn hoá [`Self::amplitude_rel`].
    value_range: f64,
    /// Mốc thời gian quan sát cuối — gốc của mọi phép chiếu.
    anchor_ts: i64,
    /// Giá trị quan sát cuối.
    anchor_value: f64,
    span_secs: i64,
    samples: usize,
}

impl TrendAnalysis {
    /// Phân tích xu hướng của `points = [(ts_unix_secs, value)]`.
    ///
    /// Bỏ qua điểm có value không hữu hạn (NaN/Inf do script đưa vào). Trả `None`
    /// khi không nói được bất cứ điều gì: dưới `config.min_samples` điểm, mọi
    /// điểm trùng một mốc thời gian (slope là 0/0), hoặc không còn điểm hữu
    /// hạn nào. Lớp trên nhận `None` và bỏ qua node — đúng hơn là báo cáo dự
    /// đoán bịa ra.
    #[must_use]
    pub fn new(points: &[(i64, f64)], config: &TrendConfig) -> Option<Self> {
        // Mốc đầu tiên **hữu hạn** — không phải `points[0]`, vì điểm đầu có thể
        // là NaN và bị bỏ ở bước dưới.
        let origin_ts = points.iter().find(|(_, v)| v.is_finite()).map(|(t, _)| *t)?;
        // Ngay từ đầu đã dời mốc: `x` là **giây kể từ origin**. Giữ timestamp
        // thô (~1.8e9) thì `x − mean_x` là phép trừ hai số gần nhau trong f64 —
        // 7 chữ số trước dấu thập phân ăn mất độ chính xác, và trên cửa sổ vài
        // chục giây sai số đó đủ nghiêng hẳn cả slope.
        let pts: Vec<(f64, f64)> = points
            .iter()
            .filter(|(_, v)| v.is_finite())
            .map(|(t, v)| (t.saturating_sub(origin_ts) as f64, *v))
            .collect();
        if pts.len() < config.min_samples.max(2) {
            return None;
        }

        // `pts[].0` là giây **kể từ origin**, nên điểm đầu có x = 0 và bề rộng
        // cửa sổ chính là x của điểm cuối.
        let last = *pts.last()?;
        let span_secs = last.0 as i64;
        if span_secs <= 0 {
            return None;
        }
        let anchor_ts = origin_ts.saturating_add(span_secs);
        let anchor_value = last.1;
        let samples = pts.len();

        let n = pts.len() as f64;
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        for &(x, y) in &pts {
            sum_x += x;
            sum_y += y;
        }
        let mean_x = sum_x / n;
        let mean_y = sum_y / n;

        let mut sxx = 0.0;
        let mut sxy = 0.0;
        for &(x, y) in &pts {
            let dx = x - mean_x;
            sxx += dx * dx;
            sxy += dx * (y - mean_y);
        }
        // Mọi điểm trùng một mốc thời gian → Sxx = 0, slope là 0/0.
        if sxx <= f64::EPSILON {
            return None;
        }

        let slope_per_sec = sxy / sxx;
        // `intercept` = giá trị tại `origin_ts`, vì x đã tương đối origin.
        let intercept = mean_y - slope_per_sec * mean_x;

        let mut ss_tot = 0.0;
        let mut ss_res = 0.0;
        let mut value_min = f64::INFINITY;
        let mut value_max = f64::NEG_INFINITY;
        let mut abs_res = Vec::with_capacity(pts.len());
        for &(x, y) in &pts {
            let err = y - (intercept + slope_per_sec * x);
            ss_tot += (y - mean_y) * (y - mean_y);
            ss_res += err * err;
            abs_res.push(err.abs());
            value_min = value_min.min(y);
            value_max = value_max.max(y);
        }
        // `ss_tot == 0`: mọi điểm bằng nhau → đường nằm đúng qua chúng, nhưng
        // r² không xác định (0/0). Trả 0 thay vì NaN để script không mang NaN
        // vào label.
        let r2 = if ss_tot > f64::EPSILON {
            1.0 - ss_res / ss_tot
        } else {
            0.0
        };
        let dof = (pts.len() as f64 - 2.0).max(1.0);
        let residual_std = (ss_res / dof).max(0.0).sqrt();

        // ── Biên độ: phân vị |phần dư|, không nhỏ hơn sàn cấu hình ───────
        abs_res.sort_by(f64::total_cmp);
        let amplitude = quantile(&abs_res, config.amplitude_quantile)
            .max(config.min_amplitude.max(0.0));

        // ── Hướng: tổng độ dốc cả cửa sổ phải vượt ngưỡng tính theo biên ──
        // So *độ dốc tích luỹ* với biên độ, không so slope thô với hằng số:
        // slope phụ thuộc đơn vị của value (byte hay %) và độ dài cửa sổ, còn
        // biên độ thì cùng đơn vị với chính cái độ dốc đó — nên so hai thứ
        // cùng đơn vị mới nói được chuyện "xu hướng có mạnh hơn dao động không".
        let drift_total = slope_per_sec * span_secs as f64;
        let threshold = config.significance.max(0.0) * amplitude;
        let direction = if drift_total > threshold {
            Direction::Rising
        } else if drift_total < -threshold {
            Direction::Falling
        } else {
            Direction::Flat
        };

        Some(Self {
            origin_ts,
            slope_per_sec,
            intercept,
            r2,
            residual_std,
            direction,
            amplitude,
            value_range: (value_max - value_min).max(0.0),
            anchor_ts,
            anchor_value,
            span_secs,
            samples,
        })
    }

    // ──────────────────────────────────────────────
    // Trạng thái chuỗi
    // ──────────────────────────────────────────────

    /// Số điểm hữu hạn đã dùng.
    #[must_use]
    pub fn samples(&self) -> usize {
        self.samples
    }

    /// Bề rộng cửa sổ quan sát (giây).
    #[must_use]
    pub fn span_secs(&self) -> i64 {
        self.span_secs
    }

    /// Mốc thời gian quan sát cuối (unix giây) — gốc mọi phép chiếu.
    #[must_use]
    pub fn anchor_ts(&self) -> i64 {
        self.anchor_ts
    }

    /// Giá trị quan sát cuối cùng.
    #[must_use]
    pub fn current(&self) -> f64 {
        self.anchor_value
    }

    // ──────────────────────────────────────────────
    // Hướng
    // ──────────────────────────────────────────────

    /// Hướng đi (trục thứ nhất).
    ///
    /// Chỉ là `Rising`/`Falling` khi **tổng độ dốc cả cửa sổ** vượt
    /// `significance × amplitude`. Nghĩa là: xu hướng phải mạnh hơn sự dao
    /// động theo bội số đó mới được gọi là xu hướng. Chuỗi dao động quanh một
    /// mức thì slope trung bình rất nhỏ, nên kết quả là [`Direction::Flat`] chứ
    /// không phải `Rising` — đúng như trực giác về một đường đi ngang.
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Độ dốc, đơn vị value / giây.
    #[must_use]
    pub fn slope_per_sec(&self) -> f64 {
        self.slope_per_sec
    }

    /// Độ dốc chuẩn hoá theo giờ — con số đọc được khi báo cáo.
    #[must_use]
    pub fn slope_per_hour(&self) -> f64 {
        self.slope_per_sec * 3_600.0
    }

    /// Độ dốc chuẩn hoá theo ngày.
    #[must_use]
    pub fn slope_per_day(&self) -> f64 {
        self.slope_per_sec * 86_400.0
    }

    /// Hệ số quyết định: bao nhiêu phần biến động được đường giải thích.
    #[must_use]
    pub fn r2(&self) -> f64 {
        self.r2
    }

    /// Độ lệch chuẩn của phần dư — cùng đơn vị với value.
    #[must_use]
    pub fn residual_std(&self) -> f64 {
        self.residual_std
    }

    // ──────────────────────────────────────────────
    // Biên đường chéo
    // ──────────────────────────────────────────────

    /// Nửa bề rộng biên đường chéo, cùng đơn vị với value (trục thứ hai).
    ///
    /// Lấy từ phân vị |phần dư| — không phải độ lệch chuẩn, vì dao động thật
    /// thường **nhọn** (đỉnh rồi đáy), và phân vị bám đỉnh còn std thì không.
    #[must_use]
    pub fn amplitude(&self) -> f64 {
        self.amplitude
    }

    /// Biên độ chuẩn hoá theo `max − min` của chính dữ liệu.
    ///
    /// Mẫu số là **toàn bộ biên độ động của chuỗi** — gồm cả xu hướng lẫn dao
    /// động, không chỉ dao động. Nên đây là "dao động chiếm bao nhiêu phần cái
    /// nhảy tổng", dùng để so hai chuỗi khác thang đo, không phải để đo riêng
    /// dao động (muốn thế thì so với biên vật lý ở lớp trên).
    ///
    /// Chuỗi phẳng (`value_range == 0`) trả `0.0` thay vì chia 0.
    #[must_use]
    pub fn amplitude_rel(&self) -> f64 {
        if self.value_range > 0.0 {
            self.amplitude / self.value_range
        } else {
            0.0
        }
    }

    /// Mốc gốc của đường hồi quy (mốc quan sát hữu hạn đầu tiên).
    #[must_use]
    pub fn origin_ts(&self) -> i64 {
        self.origin_ts
    }

    /// Giá trị **đường hồi quy** tại `ts` (khác [`Self::project`], xem
    /// [`Self::trend_offset`]).
    #[must_use]
    pub fn value_at(&self, ts: i64) -> f64 {
        self.intercept + self.slope_per_sec * ts.saturating_sub(self.origin_ts) as f64
    }

    /// Lệch giữa quan sát cuối và đường hồi quy tại cùng mốc đó.
    ///
    /// Phép chiếu neo ở **quan sát cuối**, không neo ở đường hồi quy: đường fit
    /// trên cửa sổ dài có xu hướng nằm lệch khỏi mốc cuối, nên "còn bao lâu
    /// nữa" tính từ đường fit sẽ lệch ngay tại thời điểm báo cáo. Lệch này là
    /// chẩn đoán xem mức hiện tại có đang lệch khỏi xu hướng dài hạn không.
    #[must_use]
    pub fn trend_offset(&self) -> f64 {
        self.anchor_value - self.value_at(self.anchor_ts)
    }

    // ──────────────────────────────────────────────
    // Phép chiếu
    // ──────────────────────────────────────────────

    /// Chiếu `hours` giờ từ mốc quan sát cuối: xu hướng + hai mép biên.
    ///
    /// `hours` âm (nhìn về quá khứ) cũng hợp lệ — dùng để vẽ lại dải lịch sử.
    #[must_use]
    pub fn project(&self, hours: f64) -> Projection {
        // Không clamp 0 (xem doc). Chỉ chặn NaN/Inf vì chúng làm hỏng cả `ts`
        // lẫn `trend`.
        let hours = if hours.is_finite() { hours } else { 0.0 };
        let trend = self.anchor_value + self.slope_per_hour() * hours;
        Projection {
            // Ép f64 → i64 **bão hoà** (Rust ≥1.45), nên `hours` khổng lồ không
            // gây UB — chỉ bão hoà về `i64::MAX`.
            ts: self.anchor_ts.saturating_add((hours * 3_600.0) as i64),
            hours,
            trend,
            low: trend - self.amplitude,
            high: trend + self.amplitude,
        }
    }

    /// Bao lâu nữa **đường xu hướng** chạm `target`.
    ///
    /// `None` khi đang đi xuống / đứng yên, hoặc chưa đủ dữ liệu để nói.
    /// `Some(0.0)` = đã chạm rồi.
    fn hours_until(&self, target: f64) -> Option<f64> {
        let slope_hour = self.slope_per_hour();
        // `!(x > 0)` thay vì `x <= 0` để NaN cũng rơi vào nhánh này.
        if !(slope_hour > 0.0) {
            return None;
        }
        let need = target - self.anchor_value;
        if !(need > 0.0) {
            return Some(0.0);
        }
        Some(need / slope_hour)
    }

    /// Bao lâu nữa **đường xu hướng** chạm `target` — lạc quan, bỏ qua dao động.
    #[must_use]
    pub fn hours_to(&self, target: f64) -> Option<f64> {
        self.hours_until(target)
    }

    /// Bao lâu nữa **mép trên** của biên đường chéo chạm `target`.
    ///
    /// Luôn ≤ [`Self::hours_to`], và chênh lệch đúng bằng `amplitude / slope`:
    /// đó là "dao động cắt ngang bao nhiêu phần thời gian còn lại".
    #[must_use]
    pub fn hours_to_upper_envelope(&self, target: f64) -> Option<f64> {
        self.hours_until(target - self.amplitude)
    }
}

impl std::fmt::Display for TrendAnalysis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TrendAnalysis {{ direction={}  slope={:+.4}/day  r²={:.3}  \
             amplitude=±{:.4} ({:.1}% range)  offset={:+.4}  n={} × {}s }}",
            self.direction,
            self.slope_per_day(),
            self.r2,
            self.amplitude,
            self.amplitude_rel() * 100.0,
            self.trend_offset(),
            self.samples,
            self.span_secs
        )
    }
}

/// Phân vị nội sup của mảng **đã sắp xếp tăng dần**.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let pos = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let frac = pos - pos.floor();
    // `pos` ∈ [0, len-1] nên `floor`/`ceil` luôn nằm trong index hợp lệ.
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    sorted[lo] + (sorted[hi] - sorted[lo]) * frac
}

#[cfg(feature = "rhai")]
impl TrendAnalysis {
    /// Rhai constructor:
    /// `trend_fit(points, min_samples, amplitude_quantile, significance, min_amplitude)`.
    ///
    /// Trả `()` khi dữ liệu không đủ — script kiểm bằng `type_of(t) == "()"`
    /// trước khi đọc accessor (như `grid_fit`).
    pub fn trend_fit(
        points: rhai::Array,
        min_samples: i64,
        amplitude_quantile: f64,
        significance: f64,
        min_amplitude: f64,
    ) -> rhai::Dynamic {
        let pts = crate::script::parse_points(&points).unwrap_or_default();
        let config = TrendConfig {
            min_samples: if min_samples < 2 { 2 } else { min_samples as usize },
            amplitude_quantile,
            significance,
            min_amplitude,
        };
        match TrendAnalysis::new(&pts, &config) {
            Some(analysis) => rhai::Dynamic::from(analysis),
            None => rhai::Dynamic::UNIT,
        }
    }
}

// ──────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// 97 điểm cách nhau 15 phút ⇒ cửa sổ **đúng 24 giờ**.
    ///
    /// Không tùy ý: 24h chia hết cho chu kỳ 8h (3 chu kỳ đúng), nên trung bình
    /// sóng tam giác bằng 0 và slope hồi quy ra đúng `rate_per_hour` — test kiểm
    /// được biên độ mà không bị xung lệch của sóng.
    const N: i64 = 97;
    /// Điểm cách nhau 15 phút (giây).
    const STEP_SECS: i64 = 900;
    /// Độ dốc dùng chung: +0.5 đơn vị/giờ ⇒ +12/ngày.
    const RATE: f64 = 0.5;
    /// Biên độ dao động dùng chung.
    const AMP: f64 = 4.0;
    /// Chu kỳ dao động (giờ).
    const PERIOD: f64 = 8.0;

    /// Sinh chuỗi `y = base + rate×(giờ) + amp×sóng tam giác(chu kỳ)`.
    fn series(base: f64, rate_per_hour: f64, amp: f64, period_h: f64) -> Vec<(i64, f64)> {
        (0..N)
            .map(|i| {
                let ts = i * STEP_SECS;
                let hours = ts as f64 / 3_600.0;
                // Tam giác hai cực trong [-1, 1], trung bình 0 trên mỗi chu kỳ.
                let phase = (hours / period_h).fract();
                let tri = if phase < 0.5 {
                    1.0 - 4.0 * phase
                } else {
                    4.0 * phase - 3.0
                };
                (ts, base + rate_per_hour * hours + amp * tri)
            })
            .collect()
    }

    fn cfg() -> TrendConfig {
        TrendConfig::default()
    }

    // ── Hồi quy ──────────────────────────────────────────────────────

    #[test]
    fn recovers_exact_line() {
        // y = 2t + 5, t tính bằng giây.
        let pts: Vec<(i64, f64)> = (0..50)
            .map(|i| (i * 60, 2.0 * (i * 60) as f64 + 5.0))
            .collect();
        let t = TrendAnalysis::new(&pts, &cfg()).expect("đủ điểm");
        assert!((t.slope_per_sec() - 2.0).abs() < 1e-9, "slope {}", t.slope_per_sec());
        assert!((t.r2() - 1.0).abs() < 1e-9, "r2 {}", t.r2());
        // Khớp cả hai đầu: đường đúng, không chỉ slope đúng.
        assert!((t.value_at(0) - 5.0).abs() < 1e-6);
        assert!((t.value_at(49 * 60) - (2.0 * (49 * 60) as f64 + 5.0)).abs() < 1e-6);
    }

    #[test]
    fn slope_units_agree() {
        let t = TrendAnalysis::new(&series(0.0, RATE, AMP, PERIOD), &cfg()).expect("đủ điểm");
        assert!((t.slope_per_hour() - RATE).abs() < 1e-6, "{}/h", t.slope_per_hour());
        assert!((t.slope_per_day() - RATE * 24.0).abs() < 1e-6, "{}/ngày", t.slope_per_day());
        assert!((t.slope_per_hour() - t.slope_per_sec() * 3_600.0).abs() < 1e-9);
    }

    #[test]
    fn survives_huge_unix_timestamps() {
        // Timestamp thật (~1.8e9). Nếu hồi quy trừ trên unix timestamp thay vì
        // trên delta thì mất chữ số có nghĩa — đây là test chặn đúng lỗi đó.
        let t0 = 1_788_131_000i64;
        let pts: Vec<(i64, f64)> = (0..200)
            .map(|i| (t0 + i * 300, 40.0 + 0.001 * i as f64))
            .collect();
        let t = TrendAnalysis::new(&pts, &cfg()).expect("đủ điểm");

        // 0.001 mỗi 300s ⇒ 0.001/300 × 86400 = 0.288 mỗi ngày.
        assert!((t.slope_per_day() - 0.288).abs() < 1e-3, "slope/ngày = {}", t.slope_per_day());
        assert!((t.value_at(t0) - 40.0).abs() < 1e-6, "intercept lệch");
        assert_eq!(t.origin_ts(), t0);
        assert_eq!(t.anchor_ts(), t0 + 199 * 300);

        // Bất biến quan trọng: dời cửa sổ về sát 0 phải cho **cùng** slope. Nếu
        // hồi quy lẩn sang con số gần-nhau ở thang 1.8e9 thì hai bản lệch nhau.
        let rebased: Vec<(i64, f64)> = pts.iter().map(|&(t, y)| (t - t0, y)).collect();
        let r = TrendAnalysis::new(&rebased, &cfg()).expect("đủ điểm");
        assert!(
            (t.slope_per_sec() - r.slope_per_sec()).abs() < 1e-18,
            "slope phụ thuộc mốc thời gian: {} vs {}",
            t.slope_per_sec(),
            r.slope_per_sec()
        );
    }

    #[test]
    fn origin_is_first_finite_point() {
        // Dùng chuỗi **thẳng** (không dao động): đường hồi quy đi qua đúng mọi
        // điểm nên `value_at` khớp tuyệt đối — đó mới là cách kiểm origin.
        // Với chuỗi dao động, đường fit cố tình lệch khỏi từng điểm.
        let mut pts = series(50.0, RATE, 0.0, PERIOD);
        pts[0].1 = f64::NAN;
        let t = TrendAnalysis::new(&pts, &cfg()).expect("đủ điểm");
        assert_eq!(t.origin_ts(), STEP_SECS);
        assert_eq!(t.anchor_ts(), (N - 1) * STEP_SECS);
        let first = pts[1].1;
        let last = pts[(N - 1) as usize].1;
        assert!((t.value_at(STEP_SECS) - first).abs() < 1e-6, "origin lệch ở đầu");
        assert!((t.value_at((N - 1) * STEP_SECS) - last).abs() < 1e-6, "lệch ở cuối");
    }

    #[test]
    fn skips_non_finite_values() {
        let mut pts = series(30.0, RATE, 0.0, PERIOD);
        pts[3].1 = f64::NAN;
        pts[7].1 = f64::INFINITY;
        let t = TrendAnalysis::new(&pts, &cfg()).expect("vẫn đủ điểm hữu hạn");
        assert_eq!(t.samples(), N as usize - 2);
        assert!(t.r2() > 0.999, "NaN phá fit thì r2 = {}", t.r2());
    }

    #[test]
    fn constant_series_has_zero_slope_and_no_nan() {
        let pts: Vec<(i64, f64)> = (0..20).map(|i| (i * 60, 50.0)).collect();
        let t = TrendAnalysis::new(&pts, &cfg()).expect("đủ điểm");
        assert!(t.slope_per_sec().abs() < 1e-12);
        // ss_tot = 0 → r² là 0/0. Trả 0 chứ không NaN, vì NaN lọt vào label.
        assert_eq!(t.r2(), 0.0);
        assert_eq!(t.direction(), Direction::Flat);
        // value_range = 0 ⇒ amplitude_rel phải là 0, không phải NaN.
        assert_eq!(t.amplitude_rel(), 0.0);
    }

    // ── Hướng ────────────────────────────────────────────────────────

    #[test]
    fn detects_rising_and_falling() {
        let up = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        assert_eq!(up.direction(), Direction::Rising, "{up}");
        let down = TrendAnalysis::new(&series(50.0, -RATE, 0.0, PERIOD), &cfg()).expect("đủ");
        assert_eq!(down.direction(), Direction::Falling, "{down}");
    }

    #[test]
    fn oscillation_alone_is_not_a_direction() {
        // Dao động biên 4% nhưng đi ngang: tổng dốc cả cửa sổ (0) không vượt
        // `significance × amplitude` ⇒ Flat, **không phải** Rising.
        let t = TrendAnalysis::new(&series(50.0, 0.0, AMP, PERIOD), &cfg()).expect("đủ");
        assert_eq!(t.direction(), Direction::Flat, "{t}");
        assert!(t.amplitude() > 0.0, "vẫn phải đo được dao động");
    }

    #[test]
    fn slow_rise_below_threshold_is_flat() {
        // Đi lên nhưng tổng dốc cả 24h (0.1×24 = 2.4) nhỏ hơn ngưỡng
        // (2 × ~3.6) ⇒ xu hướng không đủ mạnh. Đúng tinh thần: dao động bao
        // trùm xu hướng thì đường hồi quy là ảo.
        let t = TrendAnalysis::new(&series(50.0, 0.1, AMP, PERIOD), &cfg()).expect("đủ");
        assert_eq!(t.direction(), Direction::Flat, "{t}");
    }

    #[test]
    fn significance_is_configurable() {
        // Nới ngưỡng xuống 0.1 ⇒ cùng chuỗi "chậm" đó thành Rising.
        let loose = TrendConfig { significance: 0.1, ..cfg() };
        let t = TrendAnalysis::new(&series(50.0, 0.1, AMP, PERIOD), &loose).expect("đủ");
        assert_eq!(t.direction(), Direction::Rising, "{t}");
    }

    // ── Biên độ ──────────────────────────────────────────────────────

    #[test]
    fn amplitude_tracks_the_wave() {
        let t = TrendAnalysis::new(&series(50.0, 0.0, AMP, PERIOD), &cfg()).expect("đủ");
        // Phân vị 95% của |sóng| < biên cực đại (đỉnh bị lấy mẫu thưa).
        assert!(t.amplitude() > 0.8 * AMP, "amplitude = {}", t.amplitude());
        assert!(t.amplitude() < AMP, "amplitude = {}", t.amplitude());
        // Chuẩn hoá theo `max − min` của **cả dữ liệu**. Ở đây rate = 0 nên
        // dữ liệu là sóng thuần: `max − min = 2×AMP = 8`, còn `amplitude` là
        // phân vị 95% của |sóng| ≈ 3.9588 ⇒ `rel ≈ 0.4948`, tức gần **nửa**
        // peak-to-peak — đúng nghĩa của "nửa bề rộng biên đường chéo".
        //
        // Trước đây ngưỡng là `(0.15, 0.30)`: đó là con số của chuỗi **có** xu
        // hướng (`rate = RATE` ⇒ `max − min = 18`), nhưng test gọi
        // `series(50.0, 0.0, …)` — rate 0 — nên rel = 0.4948 và test **không
        // bao giờ xanh**, dù thuật toán đúng.
        assert!(
            (t.amplitude_rel() - 0.5).abs() < 0.01,
            "sóng thuần phải cho rel ≈ 0.5 (nửa peak-to-peak), thực tế {}",
            t.amplitude_rel()
        );
    }

    #[test]
    fn min_amplitude_raises_the_floor() {
        // Chuỗi phẳng ⇒ phần dư = 0 ⇒ biên độ lấy từ sàn cấu hình.
        let cfg = TrendConfig { min_amplitude: 1.25, ..cfg() };
        let t = TrendAnalysis::new(&series(50.0, 0.0, 0.0, PERIOD), &cfg).expect("đủ");
        assert!((t.amplitude() - 1.25).abs() < 1e-9, "amplitude = {}", t.amplitude());
    }

    #[test]
    fn negative_min_amplitude_cannot_widen_envelope() {
        let cfg = TrendConfig { min_amplitude: -5.0, ..cfg() };
        let t = TrendAnalysis::new(&series(50.0, 0.0, AMP, PERIOD), &cfg).expect("đủ");
        assert!(t.amplitude() > 0.0, "biên độ âm sẽ là mép đảo chiều");
    }

    #[test]
    fn quantile_helper_is_well_behaved() {
        let v = [1.0, 2.0, 3.0, 4.0];
        assert!((quantile(&v, 0.0) - 1.0).abs() < 1e-12);
        assert!((quantile(&v, 1.0) - 4.0).abs() < 1e-12);
        assert!((quantile(&v, 0.5) - 2.5).abs() < 1e-12);
        assert!((quantile(&v, 0.4) - 2.2).abs() < 1e-12, "nội sup");
        assert_eq!(quantile(&[], 0.5), 0.0);
        assert_eq!(quantile(&[7.0], 0.9), 7.0);
        // q ngoài [0,1] phải bị chặn chứ không panic.
        assert!((quantile(&v, -5.0) - 1.0).abs() < 1e-12);
        assert!((quantile(&v, 5.0) - 4.0).abs() < 1e-12);
    }

    // ── Biên đường chéo ──────────────────────────────────────────────

    #[test]
    fn envelope_is_two_sided_around_the_trend() {
        let t = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        let p = t.project(12.0);
        assert!((p.trend - (t.current() + t.slope_per_day() / 2.0)).abs() < 1e-6, "{t}");
        assert!((p.high - p.trend - t.amplitude()).abs() < 1e-9);
        assert!((p.trend - p.low - t.amplitude()).abs() < 1e-9);
        assert!(p.high > p.low);
    }

    #[test]
    fn projection_anchors_on_last_observation() {
        let t = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        let p = t.project(0.0);
        assert!((p.trend - t.current()).abs() < 1e-9, "phải neo đúng quan sát cuối");
        assert_eq!(p.ts, t.anchor_ts());
        // `trend_offset` là chênh lệch giữa neo thật và đường fit.
        let expect_offset = t.current() - t.value_at(t.anchor_ts());
        assert!((t.trend_offset() - expect_offset).abs() < 1e-12);
    }

    #[test]
    fn negative_hours_project_backwards() {
        let t = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        let past = t.project(-24.0);
        assert!(past.trend < t.current(), "nhìn về quá khứ phải thấp hơn");
        assert!(past.ts < t.anchor_ts());
    }

    #[test]
    fn non_finite_horizon_cannot_corrupt_projection() {
        let t = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let p = t.project(bad);
            assert!(p.trend.is_finite(), "trend NaN với {bad}");
            // `ts` là **i64** nên không có `.is_finite()` — và kiểm tra "hữu
            // hạn" cũng vô nghĩa cho nó. `project` gộp horizon không hữu hạn
            // về 0.0 (`src/trend.rs:456`) nên `ts` phải **bằng** anchor chứ
            // không phải "gần" anchor: đây là assert chặt hơn, và bắt được cả
            // trường hợp `(f64::INFINITY * 3600.0) as i64` bão hoà về `i64::MAX`
            // rồi `saturating_add` làm lệch đi một chút.
            assert_eq!(p.ts, t.anchor_ts(), "ts lệch anchor với {bad}");
            assert!((p.hours - 0.0).abs() < 1e-12);
        }
    }

    #[test]
    fn envelope_crosses_target_before_the_trend_line() {
        let t = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        let edge = t.hours_to_upper_envelope(100.0).expect("đang đi lên");
        let trend = t.hours_to(100.0).expect("đang đi lên");
        assert!(edge < trend, "mép trên ({edge}h) phải chạm trước xu hướng ({trend}h)");
        // Hiệu số đúng bằng biên độ chia độ dốc — không phải con số tùy ý.
        let expect_gap = t.amplitude() / t.slope_per_hour();
        assert!((trend - edge - expect_gap).abs() < 1e-6, "hiệu số = {}", trend - edge);
    }

    #[test]
    fn falling_or_flat_series_has_no_eta() {
        for rate in [-RATE, 0.0] {
            let t = TrendAnalysis::new(&series(50.0, rate, AMP, PERIOD), &cfg()).expect("đủ");
            assert_eq!(t.hours_to(100.0), None, "rate = {rate}");
            assert_eq!(t.hours_to_upper_envelope(100.0), None, "rate = {rate}");
        }
    }

    #[test]
    fn already_past_target_is_zero_hours() {
        let t = TrendAnalysis::new(&series(99.0, RATE, 5.0, PERIOD), &cfg()).expect("đủ");
        assert!(t.current() > 100.0, "current = {}", t.current());
        assert_eq!(t.hours_to(100.0), Some(0.0));
        assert_eq!(t.hours_to_upper_envelope(100.0), Some(0.0));
    }

    // ── Guard: không đủ dữ liệu thì không đoán bừa ───────────────────

    #[test]
    fn refuses_insufficient_data() {
        let cfg = cfg();
        let short = series(50.0, RATE, AMP, PERIOD).into_iter().take(3).collect::<Vec<_>>();
        assert!(TrendAnalysis::new(&short, &cfg).is_none());
        assert!(TrendAnalysis::new(&[], &cfg).is_none());
        assert!(TrendAnalysis::new(&[(1, 1.0)], &cfg).is_none());
    }

    #[test]
    fn refuses_zero_width_window() {
        // Mọi điểm trùng mốc thời gian → Sxx = 0, slope là 0/0.
        let same: Vec<(i64, f64)> = (0..20).map(|i| (500, 10.0 + i as f64)).collect();
        assert!(TrendAnalysis::new(&same, &cfg()).is_none());
    }

    #[test]
    fn refuses_all_nan_series() {
        let pts: Vec<(i64, f64)> = (0..40).map(|i| (i * 60, f64::NAN)).collect();
        assert!(TrendAnalysis::new(&pts, &cfg()).is_none());
    }

    #[test]
    fn accepts_values_outside_any_nominal_range() {
        // Không có "biên vật lý" nào ở đây — đó là việc của lớp trên.
        let t = TrendAnalysis::new(&series(1e12, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        assert!(t.current() > 1e12, "current = {}", t.current());
        assert_eq!(t.direction(), Direction::Rising, "{t}");
    }

    #[test]
    fn scale_invariance_of_relative_amplitude() {
        // Cùng hình dạng ở hai thang đo khác nhau: biên tương đối phải bằng nhau.
        let a = TrendAnalysis::new(&series(40.0, 0.0, AMP, PERIOD), &cfg()).expect("đủ");
        let b = TrendAnalysis::new(&series(40e9, 0.0, AMP * 1e9, PERIOD), &cfg()).expect("đủ");
        assert!((a.amplitude_rel() - b.amplitude_rel()).abs() < 1e-9);
    }

    #[test]
    fn display_is_readable() {
        let t = TrendAnalysis::new(&series(50.0, RATE, AMP, PERIOD), &cfg()).expect("đủ");
        let s = t.to_string();
        for needle in ["direction=rising", "slope=", "r²=", "amplitude="] {
            assert!(s.contains(needle), "thiếu {needle} trong:\n{s}");
        }
    }
}
