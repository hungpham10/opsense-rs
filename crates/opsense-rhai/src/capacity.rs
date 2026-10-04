//! # Dự đoán capacity — hướng đi và biên đường chéo
//!
//! Lớp ghép: lấy [xu hướng + biên độ] từ
//! [`TrendAnalysis`](opsense_mlib::trend), rồi bổ sung ba thứ mà phần xu hướng
//! không có:
//!
//! 1. **Đo biên độ bằng đơn vị ô của [`AnalysisGrid`]**
//!    ([`CapacityForecast::envelope_cells`]). Biên độ đến từ phần dư của hồi quy
//!    và cùng đơn vị `capacity`, nên "4%" là câu trả lời trừu tượng; chia cho
//!    `grid.step` thành "0.4 ô" là câu trả lời đọc được — mà độ rộng ô do sieve
//!    chọn, tức đúng bằng những mức mà dữ liệu thật sự phân biệt được.
//! 2. **Chân trời đặt theo ô, không theo đường tròn tròn**.
//!    [`CapacityForecast::hours_to_full`] kéo tới đúng `capacity`, tức một con
//!    số không dữ liệu nào chạm tới; [`CapacityForecast::hours_to_top_cell`] kéo
//!    tới **biên dưới của dải trên cùng** — mốc đầu tiên về mặt cấu trúc mà dữ
//!    liệu thật sự đi qua, và là mốc duy nhất transition quan sát được.
//! 3. **Đường chân trời có xác suất**. `up_probability − down_probability`
//!    ([`CapacityForecast::drift`]) là hướng đi theo **xác suất bước**, bổ sung
//!    cho hướng theo **hồi quy**.
//!
//! Vì sao tách khỏi `TrendAnalysis`: phần xu hướng là toán thuần, dùng được cho
//! bất cứ chuỗi nào; còn "bao nhiêu thì đáng lo so với capacity" là chính sách
//! vận hành, chỉ có nghĩa với biên vật lý. Xem module doc của
//! [`opsense_mlib::trend`] cho phần "biên đường chéo".
//!
//! ## Hai con số "còn bao lâu nữa"
//!
//! - [`CapacityForecast::hours_to_trend_full`] — **đường xu hướng** chạm trần.
//! - [`CapacityForecast::hours_to_full`] — **mép trên** chạm trần, sớm hơn.
//!
//! Chênh lệch giữa hai con số chính là "dao động cắt ngang bao nhiêu phần thời
//! gian còn lại" (`amplitude / slope`). Dùng số đầu để cảnh báo, số sau để lập
//! kế hoạch.

use opsense_mlib::grid::{AnalysisGrid, SieveConfig};
use opsense_mlib::script::parse_points;
use opsense_mlib::trend::{Direction, TrendAnalysis, TrendConfig};
use opsense_mlib::transition::TransitionAnalysis;

/// Cấu hình dựng [`CapacityForecast`].
///
/// Mặc định bám đúng bài toán disk **theo phần trăm** (`capacity = 100`) và cửa
/// sổ vài chục giờ; đổi `capacity` sang byte thì chỉ sửa một ô.
#[derive(Debug, Clone, Copy)]
pub struct ForecastConfig {
    /// Ngưỡng của phần xu hướng, truyền thẳng xuống
    /// [`TrendAnalysis::new`]. Lớp này không ghi đè gì — biên độ thuần túy do
    /// phần dư quyết định, còn grid chỉ **lượng hoá** nó ra
    /// ([`CapacityForecast::envelope_cells`]).
    pub trend: TrendConfig,

    /// Độ rộng bucket cho [`TransitionAnalysis`]. `0` = tự chọn theo
    /// [`Self::target_buckets`]. Đặt tường minh khi muốn mọi node cùng nhịp
    /// bucket để so sánh được với nhau.
    pub interval_secs: i64,

    /// Số bucket mong muốn khi [`Self::interval_secs`] = `0`. Thực tế độ rộng
    /// bucket là `span / target_buckets`, tối thiểu 1 giây — nên không cần biết
    /// trước cửa sổ dài bao nhiêu.
    pub target_buckets: usize,

    /// Số ô tối đa của sieve (`2^max_bit`), bị chặn trong `[1, 16]`.
    pub max_bit: usize,

    /// Biên độ tương đối (`amplitude / capacity`) trên đó mới gọi là **dao
    /// động**. `0.02` = lệch 2% capacity là dao động thật, dưới ngưỡng là nhiễu.
    ///
    /// Ngưỡng này so với **capacity**, không so với range của dữ liệu — cần
    /// biên vật lý để trả lời "việc này có đáng lo không", còn
    /// [`TrendAnalysis::amplitude_rel`] chỉ trả lời "dao động nặng bao nhiêu so
    /// với chính nó".
    pub oscillation_rel: f64,

    /// Sàn kích thước ô cho sieve ([`SieveConfig::min_step_frac`]).
    /// `None` = lưới mịn nhất dữ liệu chịu được — đúng cho phân tích.
    pub min_step_frac: Option<f64>,
}

impl Default for ForecastConfig {
    fn default() -> Self {
        Self {
            trend: TrendConfig::default(),
            interval_secs: 0,
            target_buckets: 64,
            max_bit: 12,
            oscillation_rel: 0.02,
            min_step_frac: None,
        }
    }
}

/// Dự đoán capacity: xu hướng + biên đường chéo + xác suất bước kế tiếp.
///
/// Dựng bằng [`CapacityForecast::new`]. Xem module doc để hiểu vì sao ghép ba
/// tầng.
#[opsense_macros::rhai_class(
    constructor = "capacity_forecast",
    accessors(
        "capacity" -> |f: &mut Self| -> f64 { f.capacity() },
        "capacity_current" -> |f: &mut Self| -> f64 { f.current() },
        "capacity_headroom" -> |f: &mut Self| -> f64 { f.headroom() },
        "capacity_headroom_rel" -> |f: &mut Self| -> f64 { f.headroom_rel() },
        "capacity_direction" -> |f: &mut Self| -> String { f.direction().as_str().into() },
        "capacity_oscillating" -> |f: &mut Self| -> bool { f.oscillating() },
        "capacity_amplitude" -> |f: &mut Self| -> f64 { f.amplitude() },
        "capacity_amplitude_rel" -> |f: &mut Self| -> f64 { f.amplitude_rel() },
        "capacity_drift" -> |f: &mut Self| -> f64 { f.drift() },
        "capacity_envelope_cells" -> |f: &mut Self| -> f64 { f.envelope_cells() },
        "capacity_current_cell" -> |f: &mut Self| -> i64 { f.current_cell() as i64 },
        "capacity_top_cell" -> |f: &mut Self| -> i64 { f.top_cell() as i64 },
        "capacity_samples" -> |f: &mut Self| -> i64 { f.samples() as i64 },
        "capacity_span_secs" -> |f: &mut Self| -> i64 { f.span_secs() },
        "capacity_interval_secs" -> |f: &mut Self| -> i64 { f.interval_secs() },
        "capacity_anchor_ts" -> |f: &mut Self| -> i64 { f.anchor_ts() },
        "capacity_grid" -> |f: &mut Self| -> AnalysisGrid { *f.grid() },
        "capacity_transition" -> |f: &mut Self| -> TransitionAnalysis {
            f.transition().clone()
        },
        "capacity_trend" -> |f: &mut Self| -> TrendAnalysis { f.trend().clone() },
        "capacity_hours_to_full" -> |f: &mut Self| -> rhai::Dynamic {
            match f.hours_to_full() {
                Some(h) => rhai::Dynamic::from(h),
                None => rhai::Dynamic::UNIT,
            }
        },
        "capacity_hours_to_trend_full" -> |f: &mut Self| -> rhai::Dynamic {
            match f.hours_to_trend_full() {
                Some(h) => rhai::Dynamic::from(h),
                None => rhai::Dynamic::UNIT,
            }
        },
        "capacity_hours_to_top_cell" -> |f: &mut Self| -> rhai::Dynamic {
            match f.hours_to_top_cell() {
                Some(h) => rhai::Dynamic::from(h),
                None => rhai::Dynamic::UNIT,
            }
        },
        "capacity_project" -> |f: &mut Self, hours: f64| -> rhai::Dynamic {
            let p = f.project(hours);
            let mut m = rhai::Map::new();
            m.insert("hours".into(), rhai::Dynamic::from(p.hours));
            m.insert("ts".into(), rhai::Dynamic::from(p.ts));
            m.insert("trend".into(), rhai::Dynamic::from(p.trend));
            m.insert("low".into(), rhai::Dynamic::from(p.low));
            m.insert("high".into(), rhai::Dynamic::from(p.high));
            rhai::Dynamic::from(m)
        },
    )
)]
#[derive(Debug, Clone)]
pub struct CapacityForecast {
    grid: AnalysisGrid,
    transition: TransitionAnalysis,
    trend: TrendAnalysis,
    oscillating: bool,
    capacity: f64,
}

impl CapacityForecast {
    /// Dựng dự đoán từ cửa sổ quan sát `points = [(ts_unix_secs, value)]`.
    ///
    /// * `capacity` — biên vật lý cùng đơn vị với `value`: `100.0` cho phần
    ///   trăm, `52591026176.0` cho byte.
    /// * `config` — ngưỡng sieve / biên độ / dao động.
    ///
    /// Trả `None` khi dữ liệu không đủ để nói bất cứ điều gì: ít điểm hữu hạn
    /// hơn `config.trend.min_samples`, cửa sổ không dài, `capacity` không hợp
    /// lệ, hoặc hồi quy không xác định. Script nhận `()` và bỏ qua node — đúng
    /// hơn là báo cáo dự đoán bịa ra.
    #[must_use]
    pub fn new(points: &[(i64, f64)], capacity: f64, config: &ForecastConfig) -> Option<Self> {
        if !capacity.is_finite() || capacity <= 0.0 {
            return None;
        }
        let pts: Vec<(i64, f64)> = points
            .iter()
            .copied()
            .filter(|(_, v)| v.is_finite())
            .collect();
        if pts.len() < config.trend.min_samples.max(2) {
            return None;
        }

        let first_ts = pts.first()?.0;
        let anchor_ts = pts.last()?.0;
        let span_secs = anchor_ts.saturating_sub(first_ts);
        if span_secs <= 0 {
            return None;
        }

        let values: Vec<f64> = pts.iter().map(|(_, v)| *v).collect();

        // ── 1. Grid trước: cần `grid.step` để đặt sàn biên độ cho trend ────
        let sieve = SieveConfig {
            delta_multiplier: SieveConfig::default().delta_multiplier,
            min_abs_delta: SieveConfig::default().min_abs_delta,
            min_step_frac: config.min_step_frac,
        };
        let grid = AnalysisGrid::with_config(
            &values,
            0.0,
            capacity,
            config.max_bit.clamp(1, 16),
            &sieve,
        );

        // ── 2. Xu hướng + biên độ (thuần phần dư, grid không can thiệp) ───
        let trend = TrendAnalysis::new(&pts, &config.trend)?;

        // ── 3. Xác suất bước kế tiếp trên chính grid đó ────────────────────
        let interval_secs = if config.interval_secs > 0 {
            config.interval_secs
        } else {
            let target = config.target_buckets.clamp(1, 100_000);
            (span_secs / target as i64).max(1)
        };
        let transition = TransitionAnalysis::new(grid, &pts, interval_secs);

        let oscillating = trend.amplitude() > config.oscillation_rel.max(0.0) * capacity;

        Some(Self {
            grid,
            transition,
            trend,
            oscillating,
            capacity,
        })
    }

    // ──────────────────────────────────────────────
    // Biên vật lý
    // ──────────────────────────────────────────────

    /// Biên vật lý.
    #[must_use]
    pub fn capacity(&self) -> f64 {
        self.capacity
    }

    /// Giá trị quan sát cuối cùng.
    #[must_use]
    pub fn current(&self) -> f64 {
        self.trend.current()
    }

    /// Dung lượng còn trống (`capacity − current`), có thể âm khi đã vượt trần.
    #[must_use]
    pub fn headroom(&self) -> f64 {
        self.capacity - self.current()
    }

    /// Dung lượng còn trống theo tỉ lệ (`0.2` = còn 20%).
    #[must_use]
    pub fn headroom_rel(&self) -> f64 {
        self.headroom() / self.capacity
    }

    /// Số điểm hữu hạn đã dùng.
    #[must_use]
    pub fn samples(&self) -> usize {
        self.trend.samples()
    }

    /// Bề rộng cửa sổ quan sát (giây).
    #[must_use]
    pub fn span_secs(&self) -> i64 {
        self.trend.span_secs()
    }

    /// Mốc thời gian quan sát cuối — gốc của mọi phép chiếu.
    #[must_use]
    pub fn anchor_ts(&self) -> i64 {
        self.trend.anchor_ts()
    }

    // ──────────────────────────────────────────────
    // Hướng và biên đường chéo
    // ──────────────────────────────────────────────

    /// Hướng đi (trục thứ nhất).
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.trend.direction()
    }

    /// Có dao động đáng kể **so với capacity** không (trục thứ hai).
    ///
    /// `Direction::Rising` + `oscillating()` = "dao động có tính hướng lên".
    #[must_use]
    pub fn oscillating(&self) -> bool {
        self.oscillating
    }

    /// Biên độ dao động quanh xu hướng, cùng đơn vị `capacity`.
    #[must_use]
    pub fn amplitude(&self) -> f64 {
        self.trend.amplitude()
    }

    /// Biên độ theo tỉ lệ `capacity`.
    ///
    /// Khác [`TrendAnalysis::amplitude_rel`] (so với `max − min` của dữ liệu):
    /// đây là so với **biên vật lý**, nên "2%" nghĩa là 2% capacity thật.
    #[must_use]
    pub fn amplitude_rel(&self) -> f64 {
        self.amplitude() / self.capacity
    }

    /// Độ dốc chuẩn hoá theo ngày (phụ lục của [`Self::trend`]).
    #[must_use]
    pub fn slope_per_day(&self) -> f64 {
        self.trend.slope_per_day()
    }

    /// Hệ số quyết định của đường hồi quy (phụ lục của [`Self::trend`]).
    #[must_use]
    pub fn r2(&self) -> f64 {
        self.trend.r2()
    }

    /// Lệch giữa quan sát cuối và đường hồi quy tại cùng mốc đó (chẩn đoán).
    #[must_use]
    pub fn trend_offset(&self) -> f64 {
        self.trend.trend_offset()
    }

    /// **Biên độ tính bằng ô lưới** — `amplitude / grid.step`.
    ///
    /// Đây là chỗ [`AnalysisGrid`] thật sự góp phần: biên độ thuần là "4% dung
    /// lượng", còn `grid.step` là độ rộng dải mà sieve chọn từ chính dữ liệu
    /// này — tức đúng những mức dữ liệu phân biệt được. `0.4` nghĩa là mép trên
    /// với dưới lệch nhau chưa tới một dải: disk đang đi trong **một dải**, và
    /// [`Self::hours_to_full`] dựa trên biên đó là dự đoán trên dữ liệu nhiễu.
    #[must_use]
    pub fn envelope_cells(&self) -> f64 {
        if self.grid.step > 0.0 {
            self.amplitude() / self.grid.step
        } else {
            0.0
        }
    }

    /// Chỉ số dải của grid đang chứa giá trị hiện tại.
    #[must_use]
    pub fn current_cell(&self) -> usize {
        self.grid.cell(self.current())
    }

    /// Chỉ số dải trên cùng — dải "sắp đầy".
    #[must_use]
    pub fn top_cell(&self) -> usize {
        self.grid.num_cells().saturating_sub(1)
    }

    /// Biên dưới của dải trên cùng: mốc cấu trúc gần `capacity` nhất mà dữ liệu
    /// thật sự đi qua.
    #[must_use]
    pub fn top_cell_edge(&self) -> f64 {
        self.capacity - self.grid.step
    }

    /// Chênh lệch xác suất bước: `P(lên) − P(xuống)`, trong `[-1, 1]`.
    ///
    /// Hướng theo **xác suất bước** (transition), bổ sung cho hướng theo **hồi
    /// quy** ([`Self::direction`]). Hai cái lệch nhau là tín hiệu đáng tin:
    /// mỗi bước đi lên nhưng trung bình cả cửa sổ lại đi xuống nghĩa là dữ liệu
    /// đang hồi phục sau một cú sụt, và dự đoán theo hồi quy sẽ quá bi.
    #[must_use]
    pub fn drift(&self) -> f64 {
        self.transition.up_probability() - self.transition.down_probability()
    }

    // ──────────────────────────────────────────────
    // Phụ lục: các tầng bên dưới
    // ──────────────────────────────────────────────

    /// Phần xu hướng thuần (không biết gì về capacity).
    #[must_use]
    pub fn trend(&self) -> &TrendAnalysis {
        &self.trend
    }

    /// Grid dùng làm độ phân giải ô (và là nguồn của sàn biên độ).
    #[must_use]
    pub fn grid(&self) -> &AnalysisGrid {
        &self.grid
    }

    /// Phân tích chuyển trạng thái trên grid.
    #[must_use]
    pub fn transition(&self) -> &TransitionAnalysis {
        &self.transition
    }

    /// Độ rộng bucket thực tế đã dùng cho transition.
    #[must_use]
    pub fn interval_secs(&self) -> i64 {
        self.transition.interval_secs()
    }

    // ──────────────────────────────────────────────
    // Phép chiếu
    // ──────────────────────────────────────────────

    /// Chiếu `hours` giờ từ mốc quan sát cuối.
    ///
    /// Neo ở quan sát cuối rồi cộng `slope × hours`, rồi bọc biên độ quanh đó.
    /// `hours` âm (nhìn về quá khứ) cũng hợp lệ — dùng để vẽ lại dải lịch sử.
    #[must_use]
    pub fn project(&self, hours: f64) -> opsense_mlib::trend::Projection {
        self.trend.project(hours)
    }

    /// Bao lâu nữa **đường xu hướng** chạm đầy — lạc quan, bỏ qua dao động.
    ///
    /// Cùng gốc với [`Self::hours_to_full`] và cùng một đường neo; khác nhau
    /// **đúng bằng biên độ**. Nên khi [`Self::oscillating`] thì luôn
    /// `hours_to_full() ≤ hours_to_trend_full()`.
    #[must_use]
    pub fn hours_to_trend_full(&self) -> Option<f64> {
        self.trend.hours_to(self.capacity)
    }

    /// Bao lâu nữa **mép trên** chạm tới **biên dưới của dải trên cùng**.
    ///
    /// Lạc quan hơn [`Self::hours_to_full`] (mốc ở trong dải trên cùng, chưa tới
    /// `capacity`) nhưng **vi mô hơn** [`Self::hours_to_trend_full`] (mép trên,
    /// không phải đường xu hướng). Đây là câu hỏi mà transition trả lời được:
    /// "mấy giờ nữa dữ liệu vào dải sắp đầy", và là câu hỏi đáng hỏi nhất khi
    /// muốn cảnh báo theo mức độ **dải** thay vì theo phần trăm tuyệt đối.
    #[must_use]
    pub fn hours_to_top_cell(&self) -> Option<f64> {
        self.trend.hours_to_upper_envelope(self.top_cell_edge())
    }

    /// Bao lâu nữa **mép trên của biên đường chéo** chạm đầy — dùng để cảnh báo.
    ///
    /// Chênh lệch so với [`Self::hours_to_trend_full`] chính là [`Self::amplitude`]:
    /// chuỗi dao động quanh trần chạm trần sớm hơn nhiều so với khi xu hướng tới
    /// nơi, và đó mới là con số nên trong alert.
    #[must_use]
    pub fn hours_to_full(&self) -> Option<f64> {
        self.trend.hours_to_upper_envelope(self.capacity)
    }
}

impl std::fmt::Display for CapacityForecast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CapacityForecast {{")?;
        writeln!(
            f,
            "  current  : {:.4} / {:.4}  (headroom {:.2}%)",
            self.current(),
            self.capacity,
            self.headroom_rel() * 100.0
        )?;
        writeln!(f, "  window   : {} pts × {}s", self.samples(), self.span_secs())?;
        writeln!(
            f,
            "  direction: {}{}",
            self.direction(),
            if self.oscillating() { " (oscillating)" } else { "" }
        )?;
        writeln!(
            f,
            "  slope    : {:+.4}/day   r² {:.3}   offset {:+.4}",
            self.slope_per_day(),
            self.r2(),
            self.trend_offset()
        )?;
        writeln!(
            f,
            "  envelope : ±{:.4} ({:.2}% capacity)  drift {:+.3}",
            self.amplitude(),
            self.amplitude_rel() * 100.0,
            self.drift()
        )?;
        writeln!(
            f,
            "  grid     : {} cells × {:.4}  bucket {}s  (hiện tại ở dải {})",
            self.grid().num_cells(),
            self.grid().step,
            self.interval_secs(),
            self.current_cell()
        )?;
        writeln!(
            f,
            "  band     : ±{:.4} = {:.2} ô lưới",
            self.amplitude(),
            self.envelope_cells()
        )?;
        match self.hours_to_top_cell() {
            Some(h) => writeln!(f, "  →top cell : {h:.1} h ({:.1} d)", h / 24.0)?,
            None => writeln!(f, "  →top cell : —")?,
        }
        match self.hours_to_trend_full() {
            Some(h) => writeln!(f, "  trend→full  : {h:.1} h ({:.1} d)", h / 24.0)?,
            None => writeln!(f, "  trend→full  : —")?,
        }
        match self.hours_to_full() {
            Some(h) => writeln!(f, "  edge→full   : {h:.1} h ({:.1} d)", h / 24.0)?,
            None => writeln!(f, "  edge→full   : —")?,
        }
        writeln!(f, "}}")?;
        Ok(())
    }
}

impl CapacityForecast {
    /// Rhai constructor:
    /// `capacity_forecast(points, capacity, interval_secs, max_bit, min_samples)`.
    ///
    /// Trả `()` khi dữ liệu không đủ dựng dự đoán — script kiểm bằng
    /// `type_of(f) == "()"` trước khi đọc accessor (như `grid_fit`).
    pub fn capacity_forecast(
        points: rhai::Array,
        capacity: f64,
        interval_secs: i64,
        max_bit: i64,
        min_samples: i64,
    ) -> rhai::Dynamic {
        let pts = parse_points(&points).unwrap_or_default();
        let config = ForecastConfig {
            trend: TrendConfig {
                min_samples: if min_samples < 2 {
                    2
                } else {
                    min_samples as usize
                },
                ..TrendConfig::default()
            },
            interval_secs,
            max_bit: max_bit.clamp(1, 16) as usize,
            ..ForecastConfig::default()
        };
        match CapacityForecast::new(&pts, capacity, &config) {
            Some(forecast) => rhai::Dynamic::from(forecast),
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

    /// 97 điểm cách nhau 15 phút ⇒ cửa sổ **đúng 24 giờ** (= 3 chu kỳ 8h, nên
    /// trung bình sóng tam giác bằng 0 và slope hồi quy ra đúng `rate`).
    const N: i64 = 97;
    const STEP_SECS: i64 = 900;
    const RATE: f64 = 0.5;
    const AMP: f64 = 4.0;
    const PERIOD: f64 = 8.0;

    fn series(base: f64, rate_per_hour: f64, amp: f64, period_h: f64) -> Vec<(i64, f64)> {
        (0..N)
            .map(|i| {
                let ts = i * STEP_SECS;
                let hours = ts as f64 / 3_600.0;
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

    fn rising_oscillating() -> Vec<(i64, f64)> {
        series(50.0, RATE, AMP, PERIOD)
    }

    fn cfg() -> ForecastConfig {
        ForecastConfig::default()
    }

    // ── Ghép ba tầng ─────────────────────────────────────────────────

    #[test]
    fn rising_oscillating_is_detected() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        assert_eq!(f.direction(), Direction::Rising, "{f}");
        assert!(f.oscillating(), "biên độ {} phải đáng kể", f.amplitude());
        assert!(f.amplitude_rel() > 0.02, "amplitude_rel = {}", f.amplitude_rel());
        assert!((f.slope_per_day() - 12.0).abs() < 1e-6, "slope/ngày = {}", f.slope_per_day());
    }

    #[test]
    fn grid_is_fitted_over_the_whole_capacity_range() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        assert!(f.grid().num_cells() > 1, "grid phải có nhiều ô");
        assert!(f.grid().step > 0.0 && f.grid().step < 100.0);
        // Grid phải bọc trọn biên vật lý, không phải chỉ vùng có dữ liệu.
        assert!((f.grid().max - 100.0).abs() < 1e-9);
        assert!((f.grid().min - 0.0).abs() < 1e-9);
    }

    #[test]
    fn transition_step_probabilities_are_a_distribution() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        let up = f.transition().up_probability();
        let down = f.transition().down_probability();
        let stay = f.transition().stay_probability();
        assert!((up + down + stay - 1.0).abs() < 1e-9, "{up}+{down}+{stay}");
        // `drift` là hướng theo xác suất bước, tách khỏi hướng theo hồi quy.
        assert!((f.drift() - (up - down)).abs() < 1e-12);
        assert!(f.drift().abs() <= 1.0);
        assert_eq!(f.interval_secs(), f.transition().interval_secs());
        assert!(f.interval_secs() > 0);
    }

    // ── Biên độ đo bằng ô lưới ───────────────────────────────────────

    #[test]
    fn flat_series_has_zero_width_band() {
        // Chuỗi phẳng tuyệt đối ⇒ phần dư = 0 ⇒ biên hẹp. Đây là câu trả lời
        // **đúng**, không phải thiếu dữ liệu: không có dao động thì không dựng
        // dải giả.
        let f = CapacityForecast::new(&series(50.0, 0.0, 0.0, PERIOD), 100.0, &cfg())
            .expect("đủ dữ liệu");
        assert!(f.grid().step > 0.0, "grid phải mịn hơn capacity");
        assert_eq!(f.amplitude(), 0.0, "{f}");
        assert_eq!(f.envelope_cells(), 0.0);
        assert!(!f.oscillating());
        assert_eq!(f.direction(), Direction::Flat);
        // Mép trên bằng mép dưới, nên hai mốc "còn bao lâu" trùng nhau.
        assert_eq!(f.hours_to_full(), f.hours_to_trend_full());
    }

    #[test]
    fn band_is_measured_in_grid_cells() {
        // Biên độ ~3.96, sieve chọn step = 12.5 (8 dải trên capacity 100) ⇒
        // mép trên với dưới lệch ~0.32 dải: disk đang đi **trong một dải**, nên
        // `hours_to_full` dựa trên biên đó là dự đoán trên dữ liệu nhiễu.
        let f = CapacityForecast::new(&series(50.0, 0.0, AMP, PERIOD), 100.0, &cfg())
            .expect("đủ dữ liệu");
        assert!((f.grid().step - 12.5).abs() < 1e-9, "step = {}", f.grid().step);
        assert!(f.amplitude() > 0.8 * AMP && f.amplitude() < AMP, "biên độ = {}", f.amplitude());
        assert!(f.envelope_cells() < 0.5, "biên chưa tới nửa dải: {}", f.envelope_cells());
        assert!(
            (f.envelope_cells() - f.amplitude() / f.grid().step).abs() < 1e-12,
            "envelope_cells phải là biên độ tính bằng ô"
        );
    }

    #[test]
    fn current_and_top_cell_locate_the_value_in_the_grid() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        assert_eq!(f.top_cell(), f.grid().num_cells() - 1);
        // current = 66, step = 12.5 ⇒ dải 5 trên 0..7.
        assert_eq!(f.current_cell(), 5, "current = {}", f.current());
        // Giá trị nằm đúng trong dải mà `cell()` trả về.
        let (lo, hi) = f.grid().cell_range(f.current_cell()).expect("dải hợp lệ");
        assert!(lo <= f.current() && f.current() < hi, "{lo} ≤ {} < {hi}", f.current());
        // Biên dưới dải trên cùng nằm dưới `capacity` đúng một ô.
        assert!((f.top_cell_edge() - (100.0 - 12.5)).abs() < 1e-9);
        assert!(f.top_cell_edge() < f.capacity());
    }

    #[test]
    fn hours_to_top_cell_is_between_the_two_eta_numbers() {
        // dải trên cùng bắt đầu ở 75, nên "vào dải" phải sớm hơn "đầy trống"
        // nhưng chậm hơn "đường xu hướng tới 75".
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        let to_top = f.hours_to_top_cell().expect("đang đi lên");
        let to_full = f.hours_to_full().expect("đang đi lên");
        assert!(to_top < to_full, "vào dải ({to_top}h) phải trước khi đầy ({to_full}h)");
        // Mép trên phải băng qua biên dưới dải trên cùng trước khi tới capacity,
        // chênh đúng một ô chia độ dốc.
        let expect_gap = f.grid().step / (f.slope_per_day() / 24.0);
        assert!((to_full - to_top - expect_gap).abs() < 1e-6, "chênh = {}", to_full - to_top);
    }

    #[test]
    fn top_cell_eta_is_none_when_not_rising() {
        let f = CapacityForecast::new(&series(50.0, -RATE, AMP, PERIOD), 100.0, &cfg())
            .expect("đủ dữ liệu");
        assert_eq!(f.hours_to_top_cell(), None);
    }

    #[test]
    fn oscillation_threshold_is_relative_to_capacity_not_to_the_data() {
        // Cùng dao động 4 đơn vị, nhưng capacity 100 vs capacity 400:
        // với capacity 100 thì 4% là dao động, với 400 thì 1% là nhiễu.
        let pts = series(50.0, 0.0, AMP, PERIOD);
        let small = CapacityForecast::new(&pts, 100.0, &cfg()).expect("đủ");
        let big = CapacityForecast::new(&pts, 400.0, &cfg()).expect("đủ");
        assert!((small.amplitude() - big.amplitude()).abs() < 1e-9, "biên độ phải bằng nhau");
        assert!(small.oscillating(), "4% capacity là dao động thật");
        assert!(!big.oscillating(), "1% capacity là nhiễu");
        // Ngưỡng dùng biên vật lý, không dùng range của dữ liệu.
        assert!(small.amplitude_rel() > big.amplitude_rel());
    }

    // ── Hai con số "còn bao lâu nữa" ─────────────────────────────────

    #[test]
    fn envelope_high_crosses_full_earlier_than_the_trend() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        let edge = f.hours_to_full().expect("đang đi lên");
        let trend = f.hours_to_trend_full().expect("đang đi lên");
        assert!(edge < trend, "mép trên ({edge}h) phải chạm trước xu hướng ({trend}h)");
        // Hiệu số đúng bằng biên độ chia độ dốc — không phải con số tùy ý.
        let expect_gap = f.amplitude() / f.slope_per_day() * 24.0;
        assert!((trend - edge - expect_gap).abs() < 1e-6, "hiệu số = {}", trend - edge);
        // Chênh lệch vài giờ, không phải vài tháng.
        assert!(trend - edge < 24.0, "chênh lệch {}h là vô lý", trend - edge);
    }

    #[test]
    fn falling_series_has_no_eta() {
        let f = CapacityForecast::new(&series(50.0, -RATE, AMP, PERIOD), 100.0, &cfg())
            .expect("đủ dữ liệu");
        assert_eq!(f.direction(), Direction::Falling, "{f}");
        assert_eq!(f.hours_to_full(), None);
        assert_eq!(f.hours_to_trend_full(), None);
    }

    #[test]
    fn already_full_reports_zero_hours() {
        let f = CapacityForecast::new(&series(99.0, RATE, 5.0, PERIOD), 100.0, &cfg())
            .expect("đủ dữ liệu");
        assert!(f.current() > f.capacity(), "current = {}", f.current());
        assert_eq!(f.hours_to_full(), Some(0.0));
        assert!(f.headroom() < 0.0, "headroom = {}", f.headroom());
        assert!(f.headroom_rel() < 0.0);
    }

    #[test]
    fn headroom_is_proportional() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        assert!((f.headroom() - (100.0 - f.current())).abs() < 1e-9);
        assert!((f.headroom_rel() * 100.0 - f.headroom()).abs() < 1e-9);
    }

    #[test]
    fn projection_matches_the_inner_trend() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        let p = f.project(12.0);
        let inner = f.trend().project(12.0);
        assert!((p.trend - inner.trend).abs() < 1e-12);
        assert!((p.high - inner.high).abs() < 1e-12);
        assert!((p.low - inner.low).abs() < 1e-12);
        // Anchor là quan sát cuối, không phải đường fit.
        assert!((f.project(0.0).trend - f.current()).abs() < 1e-9);
        // Nhìn về quá khứ phải thấp hơn.
        assert!(f.project(-24.0).trend < f.current());
    }

    // ── Guard ────────────────────────────────────────────────────────

    #[test]
    fn refuses_invalid_capacity() {
        let cfg = cfg();
        let pts = rising_oscillating();
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(
                CapacityForecast::new(&pts, bad, &cfg).is_none(),
                "capacity = {bad} phải bị từ chối"
            );
        }
    }

    #[test]
    fn refuses_insufficient_data() {
        let cfg = cfg();
        let short = rising_oscillating().into_iter().take(3).collect::<Vec<_>>();
        assert!(CapacityForecast::new(&short, 100.0, &cfg).is_none());
        assert!(CapacityForecast::new(&[], 100.0, &cfg).is_none());
        // Cửa sổ không dài: mọi điểm trùng mốc thời gian.
        let same: Vec<(i64, f64)> = (0..20).map(|i| (500, 10.0 + i as f64)).collect();
        assert!(CapacityForecast::new(&same, 100.0, &cfg).is_none());
    }

    #[test]
    fn all_nan_series_is_refused() {
        let cfg = cfg();
        let pts: Vec<(i64, f64)> = (0..40).map(|i| (i * 60, f64::NAN)).collect();
        assert!(CapacityForecast::new(&pts, 100.0, &cfg).is_none());
    }

    #[test]
    fn interval_secs_override_is_respected() {
        let cfg = ForecastConfig { interval_secs: 600, ..cfg() };
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg).expect("đủ dữ liệu");
        assert_eq!(f.interval_secs(), 600);
    }

    #[test]
    fn auto_interval_scales_with_window() {
        // Cửa sổ 24h, target_buckets = 24 ⇒ bucket ~1h.
        let cfg = ForecastConfig { target_buckets: 24, ..cfg() };
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg).expect("đủ dữ liệu");
        assert!((f.interval_secs() - 3_600).abs() <= STEP_SECS, "bucket = {}", f.interval_secs());
    }

    #[test]
    fn byte_scale_behaves_like_percent_scale() {
        // Cùng hình dạng, chỉ khác đơn vị — lý do mọi ngưỡng phải là tương đối.
        let gb = 52_591_026_176.0_f64;
        let pct_pts = series(40.0, RATE, AMP, PERIOD);
        let byte_pts: Vec<(i64, f64)> = pct_pts.iter().map(|&(t, p)| (t, p / 100.0 * gb)).collect();
        let pct = CapacityForecast::new(&pct_pts, 100.0, &cfg()).expect("đủ dữ liệu");
        let byte = CapacityForecast::new(&byte_pts, gb, &cfg()).expect("đủ dữ liệu");

        assert_eq!(byte.direction(), pct.direction());
        assert_eq!(byte.oscillating(), pct.oscillating());
        assert!((byte.amplitude_rel() - pct.amplitude_rel()).abs() < 1e-9);
        assert!((byte.slope_per_day() - pct.slope_per_day() * gb / 100.0).abs() < 1.0);
        // Giờ tới đầy là con số vô đơn vị — nếu lệch thì đang có hằng số tuyệt
        // đối lọt vào đâu đó.
        assert!(
            (byte.hours_to_full().expect("lên") - pct.hours_to_full().expect("lên")).abs() < 1.0,
            "byte {} vs pct {}",
            byte.hours_to_full().expect("lên"),
            pct.hours_to_full().expect("lên")
        );
    }

    #[test]
    fn display_is_readable() {
        let f = CapacityForecast::new(&rising_oscillating(), 100.0, &cfg()).expect("đủ dữ liệu");
        let s = f.to_string();
        for needle in ["rising", "oscillating", "edge→full", "trend→full", "→top cell", "ô lưới"] {
            assert!(s.contains(needle), "thiếu {needle} trong:\n{s}");
        }
    }
}
