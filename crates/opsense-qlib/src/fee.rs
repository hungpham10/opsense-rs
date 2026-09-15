//! Fee implementations for derivative securities (chứng khoán phái sinh).
//!
//! ## Cấu trúc phí phái sinh tại Việt Nam
//!
//! Mỗi giao dịch phái sinh (VD: hợp đồng tương lai VN30) chịu ba loại phí:
//!
//! | Loại phí | Bản chất | Ví dụ |
//! |---|---|---|
//! | **Phí môi giới** (commission) | % giá trị HĐ, có min | 0.05%, tối thiểu 10,000đ |
//! | **Phí VSD** | Cố định (đồng/HĐ/chiều) | 3,300đ/HĐ |
//! | **Thuế TNCN** | % giá trị HĐ | 0.1% (từ 01/2026) |
//!
//! Công thức tính thuế TNCN mới (01/2026):
//! `Thuế TNCN = Giá chuyển nhượng × 0.1%`
//! với `Giá chuyển nhượng = Giá thanh toán × Hệ số nhân × Số lượng HĐ × Tỷ lệ ký quỹ / 2`
//!
//! ## Cách `rate()` hoạt động
//!
//! `Fee::rate()` trả về tổng phí một chiều dưới dạng fraction của giá trị hợp đồng.
//! Giá trị này được dùng làm **hurdle rate** trong `Portfolio::forward()`:
//! nếu lợi nhuận kỳ vọng ≤ `rate()` → bỏ qua lệnh (phí > lợi nhuận).
//!
//! Công thức:
//! ```text
//! contract_value = assumed_price × contract_multiplier
//! commission = max(commission_rate × contract_value, min_commission)
//! fee_per_side = commission + vsd_fee + tax_rate × contract_value
//! rate = fee_per_side / contract_value
//! ```

use super::Fee;
use serde::{Deserialize, Serialize};

// ── DerivativeFee ────────────────────────────────────────────────────────────

/// Cấu hình phí giao dịch phái sinh tổng quát.
///
/// Cho phép mô phỏng chính xác biểu phí của bất kỳ broker nào với đầy đủ
/// ba thành phần: hoa hồng, phí VSD, thuế TNCN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeFee {
    /// Tỷ lệ hoa hồng môi giới (VD: `0.0005` = 0.05%).
    pub commission_rate: f64,
    /// Hoa hồng tối thiểu mỗi hợp đồng (VND). `0` nếu không có min.
    pub min_commission: f64,
    /// Phí VSD mỗi hợp đồng mỗi chiều (VND). Thường `3_300`.
    pub vsd_fee: f64,
    /// Thuế suất TNCN (VD: `0.001` = 0.1%).
    pub tax_rate: f64,
    /// Hệ số nhân hợp đồng (VD: `100_000` cho VN30 futures).
    pub contract_multiplier: f64,
    /// Giá cơ sở giả định (điểm) để quy đổi phí cố định → tỷ lệ.
    /// VD: VN30 đang ở mức 1,300 điểm.
    pub assumed_price: f64,
}

#[allow(dead_code)]
impl DerivativeFee {
    /// Giá trị một hợp đồng = `assumed_price × contract_multiplier`.
    pub fn contract_value(&self) -> f64 {
        self.assumed_price * self.contract_multiplier
    }

    /// Tổng phí một chiều (mở *hoặc* đóng) cho một hợp đồng (VND).
    pub fn fee_per_side(&self) -> f64 {
        let cv = self.contract_value();
        let commission = (self.commission_rate * cv).max(self.min_commission);
        commission + self.vsd_fee + self.tax_rate * cv
    }

    /// Tổng phí khứ hồi (mở + đóng) cho một hợp đồng (VND).
    pub fn fee_round_trip(&self) -> f64 {
        self.fee_per_side() * 2.0
    }
}

#[typetag::serde(name = "derivative")]
impl Fee for DerivativeFee {
    /// Trả về tỷ lệ phí một chiều dưới dạng fraction của giá trị hợp đồng.
    ///
    /// ```text
    /// rate = (commission + VSD + tax) / contract_value
    /// ```
    ///
    /// Nếu `assumed_price ≤ 0`, fallback về `commission_rate + tax_rate`
    /// (bỏ qua phí VSD và min commission vì không thể quy đổi).
    fn rate(&self) -> f64 {
        let cv = self.contract_value();
        if cv <= 0.0 {
            return self.commission_rate + self.tax_rate;
        }
        let commission = (self.commission_rate * cv).max(self.min_commission);
        (commission + self.vsd_fee + self.tax_rate * cv) / cv
    }
}

// ── VPS ──────────────────────────────────────────────────────────────────────

/// Phí giao dịch phái sinh tại **VPS**.
///
/// | Thành phần | Mức phí |
/// |---|---|
/// | Phí môi giới | 0.05% (tối thiểu 10,000đ/HĐ) |
/// | Nội bộ | 0.03% |
/// | Phí VSD | 3,300đ/HĐ/một chiều |
/// | Thuế TNCN | 0.1% |
///
/// Tham khảo: <https://chungkhoanvps.org/phi-giao-dich/phi-giao-dich-phai-sinh-vps/>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpsDerivativeFee(pub DerivativeFee);

#[allow(dead_code)]
impl VpsDerivativeFee {
    /// Tạo fee VPS với mức phí chuẩn (0.05% commission).
    ///
    /// `assumed_price` — giá cơ sở giả định (điểm), VD: 1_300.
    pub fn standard(assumed_price: f64) -> Self {
        Self(DerivativeFee {
            commission_rate: 0.0005,  // 0.05%
            min_commission: 10_000.0, // tối thiểu 10,000đ/HĐ
            vsd_fee: 3_300.0,
            tax_rate: 0.001, // 0.1%
            contract_multiplier: 100_000.0,
            assumed_price,
        })
    }

    /// Tạo fee VPS với mức phí nội bộ (0.03% commission).
    pub fn internal(assumed_price: f64) -> Self {
        Self(DerivativeFee {
            commission_rate: 0.0003, // 0.03%
            min_commission: 10_000.0,
            vsd_fee: 3_300.0,
            tax_rate: 0.001,
            contract_multiplier: 100_000.0,
            assumed_price,
        })
    }
}

#[typetag::serde(name = "vps")]
impl Fee for VpsDerivativeFee {
    fn rate(&self) -> f64 {
        self.0.rate()
    }
}

// ── MBS ──────────────────────────────────────────────────────────────────────

/// Phí giao dịch phái sinh tại **MBS**.
///
/// | Thành phần | Mức phí |
/// |---|---|
/// | Phí môi giới | 0.045% |
/// | Prepaid | từ 0.03% |
/// | Phí VSD | 3,300đ/HĐ |
/// | Thuế TNCN | 0.1% |
///
/// Tham khảo: <https://www.mbs.com.vn/bieu-phi-giao-dich-chung-khoan-phai-sinh/>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MbsDerivativeFee(pub DerivativeFee);

#[allow(dead_code)]
impl MbsDerivativeFee {
    /// Tạo fee MBS với mức phí chuẩn (0.045% commission).
    pub fn standard(assumed_price: f64) -> Self {
        Self(DerivativeFee {
            commission_rate: 0.00045, // 0.045%
            min_commission: 0.0,      // MBS không có min commission
            vsd_fee: 3_300.0,
            tax_rate: 0.001,
            contract_multiplier: 100_000.0,
            assumed_price,
        })
    }

    /// Tạo fee MBS với mức phí trả trước (0.03% commission).
    pub fn prepaid(assumed_price: f64) -> Self {
        Self(DerivativeFee {
            commission_rate: 0.0003, // 0.03%
            min_commission: 0.0,
            vsd_fee: 3_300.0,
            tax_rate: 0.001,
            contract_multiplier: 100_000.0,
            assumed_price,
        })
    }
}

#[typetag::serde(name = "mbs")]
impl Fee for MbsDerivativeFee {
    fn rate(&self) -> f64 {
        self.0.rate()
    }
}

// ── SSI ──────────────────────────────────────────────────────────────────────

/// Phí giao dịch phái sinh tại **SSI**.
///
/// | Thành phần | Mức phí |
/// |---|---|
/// | Phí môi giới | 0.05% (tối thiểu 8,000đ/HĐ) |
/// | Phí VSD | 3,300đ/HĐ |
/// | Thuế TNCN | 0.1% |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsiDerivativeFee(pub DerivativeFee);

#[allow(dead_code)]
impl SsiDerivativeFee {
    pub fn standard(assumed_price: f64) -> Self {
        Self(DerivativeFee {
            commission_rate: 0.0005,
            min_commission: 8_000.0,
            vsd_fee: 3_300.0,
            tax_rate: 0.001,
            contract_multiplier: 100_000.0,
            assumed_price,
        })
    }
}

#[typetag::serde(name = "ssi")]
impl Fee for SsiDerivativeFee {
    fn rate(&self) -> f64 {
        self.0.rate()
    }
}

// ── SimpleFixedFee ───────────────────────────────────────────────────────────

/// Phí đơn giản — chỉ một tỷ lệ phần trăm cố định.
///
/// Hữu ích cho backtesting nhanh khi không cần mô phỏng chi tiết phí phái sinh.
/// VD: `SimpleFixedFee::new(0.001)` = 0.1% phí một chiều.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleFixedFee {
    /// Tỷ lệ phí (VD: `0.001` = 0.1%).
    pub rate_value: f64,
}

#[allow(dead_code)]
impl SimpleFixedFee {
    pub fn new(rate_value: f64) -> Self {
        Self { rate_value }
    }
}

#[typetag::serde(name = "fixed")]
impl Fee for SimpleFixedFee {
    fn rate(&self) -> f64 {
        self.rate_value
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Với VN30 ở 1,300 điểm, contract value = 130,000,000 VND.
    /// Commission 0.05% = 65,000 (≥ min 10,000).
    /// Phí VSD = 3,300.
    /// Thuế 0.1% = 130,000.
    /// Tổng một chiều = 65,000 + 3,300 + 130,000 = 198,300.
    /// rate ≈ 198,300 / 130,000,000 ≈ 0.001525 (≈0.1525%).
    #[test]
    fn test_vps_standard_rate() {
        let fee = VpsDerivativeFee::standard(1_300.0);
        let cv = fee.0.contract_value();
        assert_eq!(cv, 130_000_000.0);

        let expected_commission = (0.0005 * cv).max(10_000.0);
        let expected_vsd = 3_300.0;
        let expected_tax = 0.001 * cv;
        let expected_per_side = expected_commission + expected_vsd + expected_tax;
        let expected_rate = expected_per_side / cv;

        let rate = fee.rate();
        assert!(
            (rate - expected_rate).abs() < 1e-12,
            "rate={} expected={}",
            rate,
            expected_rate
        );
        assert!(
            rate > 0.0015 && rate < 0.0016,
            "rate={} out of expected range",
            rate
        );
    }

    /// Với VN30 ở 1,000 điểm, contract value = 100,000,000 VND.
    /// Commission 0.05% = 50,000 ≥ 10,000 ✓.
    #[test]
    fn test_vps_internal_rate() {
        let fee = VpsDerivativeFee::internal(1_000.0);
        let rate = fee.rate();
        // 0.03% + 0.1% tax + VSD
        assert!(rate > 0.0 && rate < 0.01, "rate={} seems unrealistic", rate);
    }

    #[test]
    fn test_mbs_standard_rate() {
        let fee = MbsDerivativeFee::standard(1_300.0);
        let rate = fee.rate();
        // 0.045% commission + 0.1% tax + VSD
        assert!(rate > 0.0014 && rate < 0.0016, "rate={} out of range", rate);
    }

    #[test]
    fn test_ssi_standard_rate() {
        let fee = SsiDerivativeFee::standard(1_300.0);
        let rate = fee.rate();
        assert!(rate > 0.0);
        // SSI: 0.05% + 0.1% tax + VSD, min commission 8,000 < 65,000 → không ảnh hưởng
    }

    #[test]
    fn test_simple_fixed_fee() {
        let fee = SimpleFixedFee::new(0.001);
        assert!((fee.rate() - 0.001).abs() < 1e-12);
    }

    #[test]
    fn test_min_commission_dominates() {
        // Với giá rất thấp (VD: index = 10), commission 0.05% × 10 × 100k = 500
        // sẽ bị min 10,000 át.
        let fee = VpsDerivativeFee::standard(10.0);
        let rate = fee.rate();
        // fee_per_side = max(500, 10k) + 3.3k + 0.1% × 1tr = 10k + 3.3k + 1k = 14.3k
        // cv = 1,000,000
        // rate = 14,300 / 1,000,000 = 0.0143 (1.43%)
        assert!(
            rate > 0.01,
            "min commission should dominate at low price: rate={}",
            rate
        );
    }

    #[test]
    fn test_zero_assumed_price_fallback() {
        let fee = DerivativeFee {
            commission_rate: 0.0005,
            min_commission: 10_000.0,
            vsd_fee: 3_300.0,
            tax_rate: 0.001,
            contract_multiplier: 100_000.0,
            assumed_price: 0.0,
        };
        // Fallback: commission_rate + tax_rate = 0.0015
        assert!(
            (fee.rate() - 0.0015).abs() < 1e-12,
            "fallback rate={}",
            fee.rate()
        );
    }
}
