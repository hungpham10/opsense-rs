//! # Score — chấm điểm một [`Report`](crate::Report)
//!
//! `Portfolio` dùng score để so sánh các trial trong `optimize` (random search
//! rồi SGD) và trả về giá trị cuối cùng cho caller. Đây là điểm neo tối thiểu:
//! risk-adjusted return theo Sharpe, lấy trực tiếp từ `Report::sharpe_ratio`.
//!
//! Score không tham gia vào `Portfolio::evaluate` kernel — kernel chỉ gọi nó
//! một lần ở cuối, nên thêm/swap cách chấm không ảnh hưởng logic giao dịch.

#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

use crate::{Report, Score};

/// Sharpe ratio (đã nhân √n_trades trong `Report`) — mặc định khi chưa chọn
/// cách chấm khác. Lỗi: không có lệnh nào hoặc std = 0 → 0.0.
#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct SharpeScore;

#[cfg_attr(feature = "json", typetag::serde(name = "sharpe"))]
impl Score for SharpeScore {
    fn score(&self, report: &Report) -> f64 {
        if report.total_trades == 0 {
            0.0
        } else {
            report.sharpe_ratio
        }
    }
}

/// Tổng PnL tuyệt đối (size-weighted) — biến thể thiên về lợi nhuận tuyệt đối
/// thay vì risk-adjusted. Hữu ích khi muốn ưu tiên quy mô lệnh hơn biến động.
#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct NetPnlScore;

#[cfg_attr(feature = "json", typetag::serde(name = "net_pnl"))]
impl Score for NetPnlScore {
    fn score(&self, report: &Report) -> f64 {
        report.net_pnl_abs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sharpe_score_is_zero_without_trades() {
        assert_eq!(SharpeScore.score(&Report::default()), 0.0);
    }

    #[test]
    fn sharpe_score_reads_report() {
        let report = Report {
            total_trades: 10,
            sharpe_ratio: 1.5,
            net_pnl_abs: 42.0,
            ..Report::default()
        };
        assert_eq!(SharpeScore.score(&report), 1.5);
        assert_eq!(NetPnlScore.score(&report), 42.0);
    }
}
