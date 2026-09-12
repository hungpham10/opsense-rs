use serde::{Deserialize, Serialize};

use crate::grid::AnalysisGrid;

/// Phân tích chuyển trạng thái của timeseries trên lưới [`AnalysisGrid`].
///
/// Lưu trữ hoàn toàn **sparse**: interval activity và transition matrix
/// đều chỉ giữ các phần tử ≠ 0.
///
/// Hỗ trợ tính xác suất sau `n` interval (dùng iterative vector-matrix
/// multiplication với ma trận chuyển dạng sparse).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionAnalysis {
    grid: AnalysisGrid,
    interval_secs: i64,
    /// intervals[i] = [(cell, count), ...] — các cell có dữ liệu trong interval i
    intervals: Vec<Vec<(usize, usize)>>,
    /// transitions[from] = [(to, count), ...] — sparse transition matrix
    transitions: Vec<Vec<(usize, usize)>>,
    /// cells_from[c]    = tổng số lần chuyển từ cell c
    cells_from: Vec<usize>,
    /// cells_down[c]    = số lần chuyển xuống từ cell c
    cells_down: Vec<usize>,
    /// cells_up[c]      = số lần chuyển lên từ cell c
    cells_up: Vec<usize>,
    /// cells_stay[c]    = số lần ở nguyên cell c
    cells_stay: Vec<usize>,
    /// dwell[c] = [d1, d2, ...] — độ dài mỗi lần ở liên tục trong cell c (số interval)
    dwell: Vec<Vec<usize>>,
}

impl TransitionAnalysis {
    /// Xây dựng phân tích từ grid và dữ liệu timeseries.
    ///
    /// * `grid` — lưới chia biên độ.
    /// * `data` — các `(timestamp_unix_secs, value)` đã sắp xếp.
    /// * `interval_secs` — độ rộng mỗi bucket (giây), VD `3600` = 1 giờ.
    pub fn new(grid: AnalysisGrid, data: &[(i64, f64)], interval_secs: i64) -> Self {
        let occ = grid.occupancy(data, interval_secs);
        let num_cells = grid.num_cells();

        // ── Sparse intervals ──
        let intervals: Vec<Vec<(usize, usize)>> = occ
            .iter()
            .map(|bucket| {
                bucket
                    .iter()
                    .enumerate()
                    .filter(|&(_, &count)| count > 0)
                    .map(|(cell, &count)| (cell, count))
                    .collect()
            })
            .collect();

        // ── Dominant cell per bucket ──
        let dominant: Vec<usize> = occ
            .iter()
            .map(|bucket| {
                bucket
                    .iter()
                    .enumerate()
                    .max_by_key(|&(_, count)| *count)
                    .map(|(cell, _)| cell)
                    .unwrap_or(0)
            })
            .collect();

        // ── Sparse transition matrix ──
        let mut transitions = vec![Vec::<(usize, usize)>::new(); num_cells];
        let mut cells_from = vec![0usize; num_cells];
        let mut cells_down = vec![0usize; num_cells];
        let mut cells_up = vec![0usize; num_cells];
        let mut cells_stay = vec![0usize; num_cells];

        for pair in dominant.windows(2) {
            let from = pair[0];
            let to = pair[1];
            if from >= num_cells || to >= num_cells {
                continue;
            }
            cells_from[from] += 1;
            if to < from {
                cells_down[from] += 1;
            } else if to > from {
                cells_up[from] += 1;
            } else {
                cells_stay[from] += 1;
            }

            // Thêm vào sparse transition
            let found = transitions[from].iter_mut().find(|(t, _)| *t == to);
            if let Some((_, count)) = found {
                *count += 1;
            } else {
                transitions[from].push((to, 1));
            }
        }

        // ── Dwell time: độ dài các lần ở liên tục trong mỗi cell ──
        let mut dwell = vec![Vec::<usize>::new(); num_cells];
        {
            let mut run_len = 1usize;
            for i in 1..dominant.len() {
                if dominant[i] == dominant[i - 1] {
                    run_len += 1;
                } else {
                    let cell = dominant[i - 1];
                    if cell < num_cells {
                        dwell[cell].push(run_len);
                    }
                    run_len = 1;
                }
            }
            if let Some(&last) = dominant.last()
                && last < num_cells
            {
                dwell[last].push(run_len);
            }
        }

        Self {
            grid,
            interval_secs,
            intervals,
            transitions,
            cells_from,
            cells_down,
            cells_up,
            cells_stay,
            dwell,
        }
    }

    // ──────────────────────────────────────────────
    // Accessors
    // ──────────────────────────────────────────────

    pub fn grid(&self) -> &AnalysisGrid {
        &self.grid
    }

    pub fn interval_secs(&self) -> i64 {
        self.interval_secs
    }

    /// Số bucket thời gian.
    pub fn num_buckets(&self) -> usize {
        self.intervals.len()
    }

    /// Số cell của grid.
    pub fn num_cells(&self) -> usize {
        self.grid.num_cells()
    }

    /// Dữ liệu sparse theo interval: `(cell, count)`.
    pub fn intervals(&self) -> &[Vec<(usize, usize)>] {
        &self.intervals
    }

    /// Cell & số điểm trong interval thứ `i`.
    pub fn interval_cells(&self, i: usize) -> &[(usize, usize)] {
        self.intervals.get(i).map_or(&[], |v| v.as_slice())
    }

    /// Dwell time: các lần ở liên tục trong cell `c` (số interval mỗi lần).
    pub fn dwell_times(&self, cell: usize) -> &[usize] {
        self.dwell.get(cell).map_or(&[], |v| v.as_slice())
    }

    /// Dwell time trung bình cho cell `c` (số interval), None nếu không có dữ liệu.
    pub fn mean_dwell(&self, cell: usize) -> Option<f64> {
        let runs = self.dwell.get(cell)?;
        if runs.is_empty() {
            return None;
        }
        Some(runs.iter().sum::<usize>() as f64 / runs.len() as f64)
    }

    /// Dwell time tối đa cho cell `c` (số interval).
    pub fn max_dwell(&self, cell: usize) -> usize {
        self.dwell
            .get(cell)
            .map_or(0, |r| *r.iter().max().unwrap_or(&0))
    }

    /// Ma trận chuyển dạng sparse: `transitions[from] = [(to, count), ...]`.
    pub fn transitions(&self) -> &[Vec<(usize, usize)>] {
        &self.transitions
    }

    /// Kiểm tra cell nguồn có dữ liệu chuyển trạng thái không.
    pub fn has_transitions_from(&self, cell: usize) -> bool {
        self.cells_from.get(cell).is_some_and(|&c| c > 0)
    }

    /// Tổng số lần chuyển từ cell.
    pub fn total_from(&self, cell: usize) -> usize {
        self.cells_from.get(cell).copied().unwrap_or(0)
    }

    // ──────────────────────────────────────────────
    // Xác suất 1 bước
    // ──────────────────────────────────────────────

    /// Xác suất tổng thể đi **xuống**.
    pub fn down_probability(&self) -> f64 {
        let total: usize = self.cells_from.iter().sum();
        if total == 0 {
            return 0.0;
        }
        self.cells_down.iter().sum::<usize>() as f64 / total as f64
    }

    /// Xác suất tổng thể đi **lên**.
    pub fn up_probability(&self) -> f64 {
        let total: usize = self.cells_from.iter().sum();
        if total == 0 {
            return 0.0;
        }
        self.cells_up.iter().sum::<usize>() as f64 / total as f64
    }

    /// Xác suất tổng thể **ở nguyên**.
    pub fn stay_probability(&self) -> f64 {
        let total: usize = self.cells_from.iter().sum();
        if total == 0 {
            return 0.0;
        }
        self.cells_stay.iter().sum::<usize>() as f64 / total as f64
    }

    /// Xác suất đi **xuống** theo từng cell nguồn.
    pub fn down_probabilities(&self) -> Vec<f64> {
        let n = self.num_cells();
        let mut r = vec![0.0f64; n];
        for (i, item) in r.iter_mut().enumerate().take(n) {
            let t = self.cells_from[i];
            if t > 0 {
                *item = self.cells_down[i] as f64 / t as f64;
            }
        }
        r
    }

    /// Xác suất đi **lên** theo từng cell nguồn.
    pub fn up_probabilities(&self) -> Vec<f64> {
        let n = self.num_cells();
        let mut r = vec![0.0f64; n];
        for (i, item) in r.iter_mut().enumerate().take(n) {
            let t = self.cells_from[i];
            if t > 0 {
                *item = self.cells_up[i] as f64 / t as f64;
            }
        }
        r
    }

    /// Xác suất **ở nguyên** theo từng cell nguồn.
    pub fn stay_probabilities(&self) -> Vec<f64> {
        let n = self.num_cells();
        let mut r = vec![0.0f64; n];
        for (i, item) in r.iter_mut().enumerate().take(n) {
            let t = self.cells_from[i];
            if t > 0 {
                *item = self.cells_stay[i] as f64 / t as f64;
            }
        }
        r
    }
}

use std::fmt;

impl fmt::Display for TransitionAnalysis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "TransitionAnalysis {{")?;
        writeln!(f, "  interval : {}h", self.interval_secs / 3600)?;
        writeln!(f, "  buckets  : {}", self.num_buckets())?;
        writeln!(f, "  cells    : {}", self.num_cells())?;
        writeln!(
            f,
            "  global   : ↓ {:.1}%  ↑ {:.1}%  = {:.1}%",
            self.down_probability() * 100.0,
            self.up_probability() * 100.0,
            self.stay_probability() * 100.0,
        )?;

        let up = self.up_probabilities();
        let down = self.down_probabilities();
        let stay = self.stay_probabilities();
        let n = self.num_cells();

        writeln!(f, "  per-cell :")?;
        for cell in 0..n {
            let (lo, hi) = self.grid.cell_range(cell).unwrap_or((0.0, 0.0));
            let total = self.total_from(cell);
            let mean_d = self
                .mean_dwell(cell)
                .map(|m| format!("{:.1}", m))
                .unwrap_or("-".into());
            let max_d = self.max_dwell(cell);
            let label = if total == 0 {
                "  — no data —".into()
            } else {
                format!(
                    "↓ {:>5.1}%  ↑ {:>5.1}%  = {:>5.1}%  dwell={}i/max={}i  (n={})",
                    down[cell] * 100.0,
                    up[cell] * 100.0,
                    stay[cell] * 100.0,
                    mean_d,
                    max_d,
                    total,
                )
            };
            writeln!(
                f,
                "    [{:>3}] {:>10.4} – {:>10.4}   {}",
                cell, lo, hi, label
            )?;
        }
        writeln!(f, "}}")?;
        Ok(())
    }
}

// ──────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let grid = AnalysisGrid {
            step: 1.0,
            offset: 0.0,
            min: 0.0,
            max: 4.0,
            max_bit: 20,
        };
        // 2 buckets: cell 0 → cell 2 (up)
        let data = [(0i64, 0.5), (30, 0.5), (60, 2.5), (90, 2.5)];
        let a = TransitionAnalysis::new(grid, &data, 60);
        assert_eq!(a.num_buckets(), 2);
        assert_eq!(a.interval_cells(0), &[(0, 2)]);
        assert_eq!(a.interval_cells(1), &[(2, 2)]);
        assert_eq!(a.up_probability(), 1.0);
        assert_eq!(a.down_probability(), 0.0);
    }

    #[test]
    fn test_sparse_intervals() {
        let grid = AnalysisGrid {
            step: 0.5,
            offset: 0.0,
            min: 0.0,
            max: 10.0,
            max_bit: 20,
        };
        assert_eq!(grid.num_cells(), 20);
        let data = [
            (0i64, 2.3),
            (10, 2.7),
            (20, 2.3),
            (3600, 3.2),
            (3610, 2.8),
            (7200, 2.3),
            (7210, 2.3),
        ];
        let a = TransitionAnalysis::new(grid, &data, 3600);
        assert!(a.interval_cells(0).contains(&(4, 2)));
        assert!(a.interval_cells(0).contains(&(5, 1)));
        assert!(a.interval_cells(1).contains(&(6, 1)));
        assert!(a.interval_cells(1).contains(&(5, 1)));
        assert_eq!(a.interval_cells(2), &[(4, 2)]);
    }
}
