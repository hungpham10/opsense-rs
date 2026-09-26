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

/// Đăng ký hai hàm trên lên engine.
pub fn register(eng: &mut rhai::Engine) {
    eng.register_fn("min_profitable_step", min_profitable_step);
    eng.register_fn("max_levels_for_profit", max_levels_for_profit);
}
