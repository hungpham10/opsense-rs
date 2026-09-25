//! # GridPlan — hợp đồng plan giữa strategy script và kernel
//!
//! `Strategy::rebuild` trả `Vec<TradingGrid>`, mà `TradingGrid` là struct Rust
//! có field private. Strategy viết bằng Rhai (`opsense-rhai::ScriptStrategy`)
//! không tạo được struct đó — nên script trả **plan dạng dữ liệu** ([`GridPlan`],
//! serde), còn kernel dựng lại `TradingGrid` từ plan.
//!
//! Script **không** tự mang thống kê lệnh: bộ đếm win/lost thuộc kernel (nó
//! ghi qua `TradingGrid::record_trade_outcome` mỗi khi lệnh đóng), nên khi dựng
//! lại, [`GridPlan::to_grids`] copy bộ đếm từ plan cũ theo vị trí (cell, level).
//!
//! ```json
//! [
//!   { "levels": [99.0, 100.0, 101.0],
//!     "sl_pct": 0.008,
//!     "max_candles": 15,
//!     "weight_sharpness": 4.0,
//!     "long_win":  [0.5, 0.52, 0.55],
//!     "short_win": [0.5, 0.48, 0.45] },
//!   ...
//! ]
//! ```
//!
//! Mọi trường ngoài `levels` đều optional: thiếu → lấy mặc định của
//! `TradingGrid` (sl 5%, win-prob 0.5, weight đều).

use serde::{Deserialize, Serialize};

use crate::grid::TradingGrid;

/// Một cell của plan: lưới lệnh trong khoảng giá của cell đó.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridPlan {
    /// Giá từng bậc lưới, tăng dần. Kernel dùng `TradingGrid::from_levels`.
    pub levels: Vec<f64>,

    /// Stop-loss mỗi lệnh (fraction). Mặc định: `TradingGrid` (5%).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sl_pct: Option<f64>,

    /// Số nến tối đa lưới này còn hiệu lực. `0` = không giới hạn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_candles: Option<usize>,

    /// Trọng số phân bổ vốn theo bậc (đều số cột thời gian).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weights: Option<Vec<f64>>,

    /// Phân bổ chuẩn quanh giữa với độ nhọn này (dùng khi `weights` thiếu).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight_sharpness: Option<f64>,

    /// Win-prob long theo bậc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_win: Option<Vec<f64>>,

    /// Win-prob short theo bậc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_win: Option<Vec<f64>>,
}

/// Thống kê lệnh đã đóng của **một** plan cũ, đưa vào `rebuild` cho script.
///
/// Script dùng để blend win-prob model với thực tế (ví dụ đủ 3 lệnh thì tin
/// tỉ lệ thắng thực tế) — đây là chỗ "học từ lịch sử" của chiến lược, viết
/// được bằng Rhai thay vì hardcode trong Rust.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CellStats {
    pub long_win: Vec<usize>,
    pub long_lost: Vec<usize>,
    pub short_win: Vec<usize>,
    pub short_lost: Vec<usize>,
}

impl CellStats {
    /// Đọc thống kê từ một plan đã có (vị trí = chỉ số level).
    pub fn of(grid: &TradingGrid) -> Self {
        let n = grid.num_levels();
        let pick = |f: fn(&TradingGrid, usize) -> usize| {
            (0..n).map(|j| f(grid, j)).collect::<Vec<usize>>()
        };
        Self {
            long_win: pick(TradingGrid::long_win_count),
            long_lost: pick(TradingGrid::long_lost_count),
            short_win: pick(TradingGrid::short_win_count),
            short_lost: pick(TradingGrid::short_lost_count),
        }
    }
}

impl GridPlan {
    /// Dựng `TradingGrid` từ plan, chép lại bộ đếm win/lost của `prev` (nếu có).
    ///
    /// Cell nào `levels` rỗng/≤ 1 giá hoặc không hợp lệ thì bị bỏ qua — script
    /// có thể trả ô rác mà kernel vẫn dựng được plan dùng được.
    pub fn to_grid(&self, prev: Option<&TradingGrid>) -> Option<TradingGrid> {
        let levels: Vec<f64> = self
            .levels
            .iter()
            .copied()
            .filter(|v| v.is_finite() && *v > 0.0)
            .collect();
        let mut grid = TradingGrid::from_levels(levels)?;
        if let Some(sl) = self.sl_pct {
            grid = grid.with_sl_pct(sl);
        }
        if let Some(mc) = self.max_candles {
            grid = grid.with_max_candles(mc);
        }
        match &self.weights {
            Some(w) if w.len() == grid.num_levels() => {
                grid = grid.with_weights(w.clone());
            }
            Some(_) => {}
            None => {
                if let Some(sharp) = self.weight_sharpness {
                    grid = grid.with_weights_normal(sharp);
                }
            }
        }
        let k = grid.num_levels();
        let clamp_probs = |v: &Option<Vec<f64>>| -> Option<Vec<f64>> {
            v.as_ref()
                .filter(|p| p.len() == k)
                .map(|p| p.iter().map(|x| x.clamp(0.0, 1.0)).collect())
        };
        if let (Some(lw), Some(sw)) = (clamp_probs(&self.long_win), clamp_probs(&self.short_win)) {
            grid = grid.with_win_probabilities(lw, sw);
        }
        if let Some(p) = prev {
            let stats = CellStats::of(p);
            grid = grid.with_outcome_counts(
                stats.long_win,
                stats.long_lost,
                stats.short_win,
                stats.short_lost,
            );
        }
        Some(grid)
    }

    /// Dựng cả plan; `prev` = plan cũ (cùng thứ tự cell) để giữ thống kê.
    pub fn to_grids(plans: &[GridPlan], prev: &[TradingGrid]) -> Vec<TradingGrid> {
        plans
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.to_grid(prev.get(i)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_round_trip_and_stats_carried_over() {
        let plan = GridPlan {
            levels: vec![99.0, 100.0, 101.0],
            sl_pct: Some(0.008),
            max_candles: Some(15),
            weights: None,
            weight_sharpness: Some(4.0),
            long_win: Some(vec![0.5, 0.52, 0.55]),
            short_win: Some(vec![0.5, 0.48, 0.45]),
        };
        // Script trả JSON → kernel parse (đúng đường Rhai → serde).
        let json = serde_json::to_string(&[plan]).unwrap();
        let parsed: Vec<GridPlan> = serde_json::from_str(&json).unwrap();

        // Plan cũ có bộ đếm từ lịch sử giao dịch.
        let mut prev = parsed[0].to_grid(None).unwrap();
        prev.record_trade_outcome(0, true, 0.01);
        prev.record_trade_outcome(0, true, 0.01);
        prev.record_trade_outcome(1, true, -0.01);

        let grids = GridPlan::to_grids(&parsed, std::slice::from_ref(&prev));
        assert_eq!(grids.len(), 1);
        let g = &grids[0];
        assert_eq!(g.num_levels(), 3);
        assert!((g.stoploss_pct() - 0.008).abs() < 1e-12);
        assert_eq!(g.long_win_count(0), 2);
        assert_eq!(g.long_lost_count(1), 1);
        assert_eq!(g.long_win_count(2), 0, "bộ đếm phải theo level");
        assert!((g.long_win_pct(2) - 0.55).abs() < 1e-12);
    }

    #[test]
    fn plan_optional_fields_use_defaults() {
        let plan = GridPlan {
            levels: vec![10.0, 11.0],
            sl_pct: None,
            max_candles: None,
            weights: None,
            weight_sharpness: None,
            long_win: None,
            short_win: None,
        };
        let g = plan.to_grid(None).expect("2 levels là hợp lệ");
        assert!((g.stoploss_pct() - 0.05).abs() < 1e-12, "mặc định TradingGrid");
        assert!((g.long_win_pct(0) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn plan_drops_invalid_cells_but_keeps_valid_ones() {
        let plans = vec![
            GridPlan {
                levels: vec![10.0],
                sl_pct: None,
                max_candles: None,
                weights: None,
                weight_sharpness: None,
                long_win: None,
                short_win: None,
            },
            GridPlan {
                levels: vec![f64::NAN, -1.0, 10.0, 11.0],
                sl_pct: None,
                max_candles: None,
                weights: None,
                weight_sharpness: None,
                long_win: None,
                short_win: None,
            },
        ];
        let grids = GridPlan::to_grids(&plans, &[]);
        assert_eq!(grids.len(), 1, "cell 1 level bị bỏ, giá ≤ 0 bị lọc");
        assert_eq!(grids[0].num_levels(), 2);
    }
}
