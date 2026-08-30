use serde::{Deserialize, Serialize};

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
    #[must_use]
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
    #[must_use]
    pub fn num_cells(&self) -> usize {
        let last = ((self.max - self.offset) / self.step).ceil() as usize;
        last.max(1)
    }

    /// Số đường lưới (= num_cells + 1).
    #[must_use]
    pub fn num_lines(&self) -> usize {
        self.num_cells() + 1
    }

    /// Đếm số crossings giữa các value liên tiếp.
    #[must_use]
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
    /// thuộc bucket thời gian đó và cell đó. Timestamp lệch khỏi
    /// `[t_start, t_end]` được clamp vào bucket biên thay vì panic.
    #[must_use]
    pub fn occupancy(&self, data: &[(i64, f64)], interval_secs: i64) -> Vec<Vec<usize>> {
        if data.is_empty() || interval_secs <= 0 {
            return Vec::new();
        }

        let t_start = data[0].0;
        let t_end = data[data.len() - 1].0;
        let num_buckets = (((t_end - t_start) / interval_secs) as usize).saturating_add(1);
        let num_cells = self.num_cells();

        let mut result = vec![vec![0usize; num_cells]; num_buckets];

        for &(ts, val) in data {
            let bucket = (((ts - t_start) / interval_secs) as usize).min(num_buckets - 1);
            let cell = self.cell(val).min(num_cells - 1);
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
    #[must_use]
    pub fn new(values: &[f64], min: f64, max: f64, max_bit: usize) -> Self {
        Self::with_config(values, min, max, max_bit, &SieveConfig::default())
    }

    /// Như [`new`](Self::new) nhưng cho phép tuỳ chỉnh điều kiện dừng
    /// của thuật toán sàng qua [`SieveConfig`].
    #[must_use]
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
    #[must_use]
    pub fn cell_range(&self, cell: usize) -> Option<(f64, f64)> {
        if cell >= self.num_cells() {
            return None;
        }
        let low = self.min + cell as f64 * self.step;
        Some((low, low + self.step))
    }

    /// Danh sách tất cả các cell: `(index, low, high)`.
    #[must_use]
    pub fn cell_ranges(&self) -> Vec<(usize, f64, f64)> {
        (0..self.num_cells())
            .filter_map(|i| self.cell_range(i).map(|(lo, hi)| (i, lo, hi)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ramp(n: usize, lo: f64, hi: f64) -> Vec<f64> {
        (0..n)
            .map(|i| lo + (hi - lo) * i as f64 / (n - 1).max(1) as f64)
            .collect()
    }

    #[test]
    fn constant_series_picks_finest_grid_zero_crossings_everywhere() {
        // Crossings = 0 ở MỌI level → không bao giờ có delta spike →
        // tie-break chọn lưới mịn nhất (nhiều ô nhất), đúng chủ đích.
        let values = vec![5.0; 50];
        let grid = AnalysisGrid::new(&values, 0.0, 100.0, 10);
        assert_eq!(grid.num_cells(), 1024);
        assert_eq!(grid.crossings(&values), 0);
    }

    #[test]
    fn two_plateaus_crossings_saturate_at_transitions() {
        // Xen kẽ 10/90: mọi segment đều là transition → crossings = segments
        // tại mọi level đủ tinh để tách hai plateau.
        let values: Vec<f64> = (0..20).flat_map(|_| vec![10.0, 90.0]).collect();
        let grid = AnalysisGrid::new(&values, 0.0, 100.0, 8);
        assert_eq!(grid.crossings(&values), values.len() - 1);
        // Level 1 (step=50) đã cho đúng 1 crossing mỗi transition — tối ưu;
        // refine sâu hơn chỉ NHÂN crossings (delta spike) → sieve dừng tại đó.
        assert!(
            (grid.step - 50.0).abs() < f64::EPSILON,
            "sieve dừng ở step=50, got {}",
            grid.step
        );
    }

    #[test]
    fn ramp_series_stops_before_overfitting() {
        // Ramp tuyến tính: crossings tăng đều ~mức nửa số segment khi refine;
        // sieve nên dừng ở level thô hơn max_bit (không dùng hết 4096 ô).
        let values = ramp(200, 0.0, 100.0);
        let coarse = AnalysisGrid::with_config(
            &values,
            0.0,
            100.0,
            12,
            &SieveConfig {
                delta_multiplier: 1.05,
                min_abs_delta: 0.001,
            },
        );
        assert!(
            coarse.num_cells() < 4096,
            "dừng sớm: {} cells",
            coarse.num_cells()
        );
        assert!(coarse.step > 100.0 / 4096.0);
    }

    #[test]
    fn degenerate_inputs_fall_back_safely() {
        let empty = AnalysisGrid::new(&[], 0.0, 100.0, 8);
        assert_eq!(empty.step, 1.0);
        assert_eq!(empty.num_cells(), 100);

        let flat_range = AnalysisGrid::new(&[1.0, 2.0], 5.0, 5.0, 8); // capacity = 0
        assert_eq!(flat_range.step, 1.0);

        assert_eq!(AnalysisGrid::new(&[42.0], 0.0, 100.0, 8).num_cells(), 256);
    }

    #[test]
    fn occupancy_buckets_by_time_and_cell() {
        let grid = AnalysisGrid {
            step: 10.0,
            offset: 0.0,
            min: 0.0,
            max: 100.0,
            max_bit: 4,
        };
        // 3 buckets × 60s: bucket0 có 2 điểm cell 0, bucket2 có 1 điểm cell 9.
        let data = [(0, 5.0), (30, 9.999), (150, 95.0)];
        let occ = grid.occupancy(&data, 60);
        assert_eq!(occ.len(), 3);
        assert_eq!(occ[0], vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(occ[1], vec![0; 10]);
        assert_eq!(occ[2][9], 1);

        // Timestamp lệch phạm vi bị clamp vào bucket biên (không panic,
        // không mất điểm) — số bucket theo first/last của dữ liệu vào.
        let wild = [(0, 5.0), (-500, 5.0), (10_000, 95.0)];
        let occ = grid.occupancy(&wild, 60);
        let total: usize = occ.iter().map(|bucket| bucket.iter().sum::<usize>()).sum();
        assert_eq!(total, 3);

        assert!(grid.occupancy(&data, 0).is_empty());
    }

    #[test]
    fn cell_and_ranges_respect_boundaries() {
        let grid = AnalysisGrid {
            step: 10.0,
            offset: 0.0,
            min: 0.0,
            max: 100.0,
            max_bit: 4,
        };
        assert_eq!(grid.cell(-1.0), 0);
        assert_eq!(grid.cell(0.0), 0);
        assert_eq!(grid.cell(15.0), 1);
        assert_eq!(grid.cell(99.999), 9);
        assert_eq!(grid.cell(100.0), 9);

        let ranges = grid.cell_ranges();
        assert_eq!(ranges.len(), 10);
        assert_eq!(ranges[0], (0, 0.0, 10.0));
        assert_eq!(ranges[9], (9, 90.0, 100.0));
        assert_eq!(grid.cell_range(10), None);
        assert_eq!(grid.num_lines(), 11);
    }
}
