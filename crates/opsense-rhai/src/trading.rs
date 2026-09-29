//! Hai phép tính phí của `TradingGrid` cho script Rhai.
//!
//! Script dựng lưới lệnh (`fn rebuild`) cần biết **một bước giữa hai mốc tối
//! thiểu bao nhiêu để còn lãi sau phí sàn**. Công thức đó đã có sẵn và đã có
//! test trong `opsense-qlib` ([`TradingGrid::min_profitable_step`],
//! [`TradingGrid::max_levels_for_profit`]) — expose ra đây để script **không
//! tự viết lại**.
//!
//! Vì sao quan trọng: lặp lại phép tính trong script dễ lệch với kernel, và
//! lệch theo hướng nguy hiểm — lưới mốc quá dày thì mọi lệnh đều bị kernel lo
//! (`placed=0`) mà nhìn từ ngoài chỉ thấy "chiến lược không vào lệnh". Đo thật
//! trên Binance: mốc cách nhau $6.28 trong khi phí đòi ≥ $84.

use opsense_qlib::TradingGrid;
use rhai::Dynamic;

/// `min_profitable_step(fee_rate, at_price) -> f64`
///
/// Bước giữa hai mốc tối thiểu để một lệnh còn lãi sau phí: `2 × fee × giá`
/// (phí vào + phí ra).
pub fn min_profitable_step(fee_rate: Dynamic, at_price: Dynamic) -> Dynamic {
    let (Some(fee), Some(price)) = (as_f64(&fee_rate), as_f64(&at_price)) else {
        return Dynamic::UNIT;
    };
    Dynamic::from(TradingGrid::min_profitable_step(fee, price))
}

/// `max_levels_for_profit(min, max, fee_rate) -> i64`
///
/// Số mốc tối đa trong `[min, max]` mà bước giữa hai mốc vẫn lãi sau phí
/// (tối thiểu 2). Dùng để đặt mốc trong ô của `AnalysisGrid`.
pub fn max_levels_for_profit(min: Dynamic, max: Dynamic, fee_rate: Dynamic) -> Dynamic {
    let (Some(lo), Some(hi), Some(fee)) = (
        as_f64(&min),
        as_f64(&max),
        as_f64(&fee_rate),
    ) else {
        return Dynamic::UNIT;
    };
    Dynamic::from(TradingGrid::max_levels_for_profit(lo, hi, fee) as i64)
}

fn as_f64(v: &Dynamic) -> Option<f64> {
    v.clone().try_cast::<f64>()
}

/// `breakeven_win_p(reward, risk, fee_rate) -> f64`
///
/// Tỉ lệ thắng tối thiểu để một lệnh hòa vốn: `(risk + 2·fee) / (reward + risk)`.
/// Xem [`TradingGrid::breakeven_win_p`] — hàm đó giải thích vì sao trần `0,75`
/// cũ trong `grid.rhai` khiến chiến lược luôn âm.
///
/// `reward` = TP thô (tỉ lệ), `risk` = SL thô (tỉ lệ), `fee_rate` = phí **mỗi
/// phía** (khứ hồi trả 2 lần, hàm tự nhân 2).
///
/// Trả `()` cho input thiếu, `reward`/`risk` ≤ 0, hoặc kết quả không hữu hạn.
/// Gặp `reward = 0` (mốc cuối không có TP phía trên — `tp_above` trả chính nó)
/// thì **không tỉ lệ thắng nào** hòa vốn; trả `()` để script biết là không xác
/// định, chứ không trả 1.0 để giấu mất việc mốc đó lỗ chắc.
pub fn breakeven_win_p(reward: Dynamic, risk: Dynamic, fee_rate: Dynamic) -> Dynamic {
    let (Some(rwd), (Some(rsk), Some(fee))) = (
        as_f64_loose(&reward),
        (as_f64_loose(&risk), as_f64_loose(&fee_rate)),
    ) else {
        return Dynamic::UNIT;
    };
    let be = TradingGrid::breakeven_win_p(rwd, rsk, fee);
    if be.is_finite() {
        Dynamic::from(be)
    } else {
        Dynamic::UNIT
    }
}

/// Như [`as_f64`] nhưng nhận cả số nguyên. `try_cast::<f64>()` fail với `0`
/// (Rhai đọc literal nguyên thành `i64`) ⇒ script truyền `sl_pct: 1` sẽ ra
/// `()` một cách khó hiểu.
fn as_f64_loose(v: &Dynamic) -> Option<f64> {
    v.clone()
        .try_cast::<f64>()
        .or_else(|| v.clone().try_cast::<i64>().map(|i| i as f64))
}

/// Đăng ký các hàm trên lên engine.
pub fn register(eng: &mut rhai::Engine) {
    eng.register_fn("min_profitable_step", min_profitable_step);
    eng.register_fn("max_levels_for_profit", max_levels_for_profit);
    eng.register_fn("breakeven_win_p", breakeven_win_p);
}

#[cfg(test)]
mod breakeven_script_tests {
    use super::*;

    /// `breakeven_win_p` phải chạy được **trong script** với đúng engine thật
    /// (`set_max_expr_depths(256, 256)` — mặc định của Rhai là 64 và sẽ báo
    /// sai "exceeds maximum complexity" cho script hợp lệ).
    #[test]
    fn callable_from_script() {
        let mut eng = crate::runtime::new_sandbox_engine(true);
        super::register(&mut eng);
        // Con số đo được trên Binance: TP 1 bước lưới, SL 0,8%, phí 0,02%.
        let v: f64 = eng
            .eval("breakeven_win_p(0.00271, 0.008, 0.0002)")
            .expect("gọi được từ script");
        assert!(
            v > 0.75 && v < 1.0,
            "ngưỡng hòa vốn {v} phải nằm giữa trần clamp cũ 0,75 và 1,0"
        );
    }

    /// `reward = 0` (mốc cuối không có TP phía trên) ⇒ không tỉ lệ thắng nào
    /// hòa vốn. Phải trả `()` để script biết, **không** phải 1.0 — 1.0 sẽ làm
    /// `clamp_win` đặt trần 1.0 và ngụ ý mốc đó có thể lãi.
    #[test]
    fn zero_reward_returns_unit() {
        let mut eng = crate::runtime::new_sandbox_engine(true);
        super::register(&mut eng);
        let v: Dynamic = eng.eval("breakeven_win_p(0.0, 0.008, 0.0002)").unwrap();
        assert!(v.is_unit(), "reward=0 phải trả UNIT, nhận {v:?}");
    }

    /// Rhai đọc literal nguyên thành `i64`; `try_cast::<f64>()` fail với `0`.
    /// Không có nhánh này thì script truyền `sl_pct: 1` nhận `()` — khó hiểu.
    #[test]
    fn accepts_integer_arguments() {
        assert_eq!(as_f64_loose(&Dynamic::from(1_i64)), Some(1.0));
        assert_eq!(as_f64_loose(&Dynamic::from(0_i64)), Some(0.0));
        assert_eq!(as_f64_loose(&Dynamic::from(0.5_f64)), Some(0.5));
        assert_eq!(as_f64_loose(&Dynamic::UNIT), None);
    }
}
