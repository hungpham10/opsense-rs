//! Score objective dùng cho SGD optimization — bản rút gọn của
//! `services/src/playground.rs` (repo `algorithm`): chỉ giữ `SharpeScore`,
//! phần runner/entry point vẫn ở repo gốc.

use serde::{Deserialize, Serialize};

use crate::{Report, Score};

/// Score = Sharpe ratio (đơn giản, hiệu quả).
///
/// Nếu không có trade (sharpe = 0) → score ~0 để SGD không chọn.
/// Nếu sharpe âm → score âm, SGD sẽ tránh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharpeScore;

#[typetag::serde(name = "sharpe")]
impl Score for SharpeScore {
    fn score(&self, report: &Report) -> f64 {
        // Cần ít nhất 3 trades để có ý nghĩa thống kê
        if report.total_trades < 3 {
            return 0.0;
        }
        report.sharpe_ratio.max(0.0)
    }
}
