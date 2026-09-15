//! # Calendar — Market calendar implementations
//!
//! Xác định thời điểm nến tiếp theo dựa trên loại thị trường:
//! - `CryptoCalendar`: 24/7 — không có ngày nghỉ
//! - `ForexCalendar`: 24/5 — cuối tuần nghỉ (Thứ 7, CN)
//! - `StockCalendar`: 9h-15h, Thứ 2 → Thứ 6 (assuming giờ VN)

use super::Calendar;

/// Convert resolution string → seconds.
pub(crate) fn to_timestamp_secs(resolution: &str) -> u64 {
    match resolution {
        "1" | "1m" => 60,
        "5" | "5m" => 300,
        "15" | "15m" => 900,
        "30" | "30m" => 1800,
        "1H" | "60" => 3600,
        "4H" => 14400,
        "1D" => 86400,
        _ => {
            // fallback: parse number suffix
            let s = resolution.trim_end_matches(|c: char| !c.is_ascii_digit());
            s.parse::<u64>().unwrap_or(60)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CryptoCalendar — 24/7
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoCalendar;

#[typetag::serde]
impl Calendar for CryptoCalendar {
    fn next(&self, current_ts: u64, resolution: &str) -> u64 {
        current_ts + to_timestamp_secs(resolution)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ForexCalendar — 24/5 (nghỉ Thứ 7, Chủ Nhật)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForexCalendar;

#[typetag::serde]
impl Calendar for ForexCalendar {
    fn next(&self, current_ts: u64, resolution: &str) -> u64 {
        let mut ts = current_ts + to_timestamp_secs(resolution);
        // Nếu rơi vào Thứ 7 (6) hoặc Chủ Nhật (0), nhảy đến Thứ 2 00:00
        while is_weekend(ts) {
            ts = next_monday_00(ts);
        }
        ts
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// StockCalendar — 9h → 15h, Thứ 2 → Thứ 6 (giờ VN UTC+7)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockCalendar;

#[typetag::serde]
impl Calendar for StockCalendar {
    fn next(&self, current_ts: u64, resolution: &str) -> u64 {
        let step = to_timestamp_secs(resolution);
        let mut ts = current_ts + step;

        // Giới hạn khung giờ: 9h → 15h (tính theo UTC+7)
        loop {
            if is_weekend(ts) {
                ts = next_monday_09(ts);
                continue;
            }

            let hour = hour_vn(ts);
            if hour < 9 {
                // Trước 9h sáng → nhảy đến 9h cùng ngày
                ts = ts - (ts % 86400) + 9 * 3600;
            } else if hour >= 15 {
                // Sau 15h → nhảy đến 9h hôm sau
                ts = ts - (ts % 86400) + 86400 + 9 * 3600;
            } else {
                break; // đang trong khung giờ
            }
        }

        ts
    }

    fn settlement_candles(&self) -> u64 {
        // Chứng khoán cơ sở VN: chỉ được phép bán sau T+3.
        3
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

use serde::{Deserialize, Serialize};

/// Unix → giờ Việt Nam (UTC+7).
fn hour_vn(ts: u64) -> u32 {
    let utc_hour = (ts % 86400) / 3600;
    ((utc_hour + 7) % 24) as u32
}

/// Kiểm tra xem `ts` có rơi vào Thứ 7 hoặc Chủ Nhật (UTC) không.
fn is_weekend(ts: u64) -> bool {
    // Unix epoch (1970-01-01) là Thứ 5.
    // days since epoch:
    let days = ts / 86400;
    // 1970-01-01 = Thursday = 4 (ISO: Monday=1, Sunday=7)
    // (days + 4) % 7 = 0 → CN, 6 → Thứ 7
    let dow = (days + 4) % 7;
    dow == 0 || dow == 6
}

/// Nhảy đến Thứ 2 00:00 UTC.
fn next_monday_00(ts: u64) -> u64 {
    let days = ts / 86400;
    let dow = (days + 4) % 7;
    let add = match dow {
        0 => 1, // CN → +1 ngày
        6 => 2, // Thứ 7 → +2 ngày
        _ => 0,
    };
    (days + add) * 86400
}

/// Nhảy đến Thứ 2 09:00 VN (= 02:00 UTC).
fn next_monday_09(ts: u64) -> u64 {
    // 09:00 VN = 02:00 UTC
    next_monday_00(ts) + 2 * 3600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settlement_candles_market_defaults() {
        // Chứng khoán → T+3; Crypto/Forex → T+0 (mặc định).
        assert_eq!(StockCalendar.settlement_candles(), 3);
        assert_eq!(CryptoCalendar.settlement_candles(), 0);
        assert_eq!(ForexCalendar.settlement_candles(), 0);
    }
}
