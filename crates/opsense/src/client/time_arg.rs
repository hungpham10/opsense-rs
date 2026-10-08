//! Mốc thời gian truy vấn: unix giây tuyệt đối **hoặc** biểu thức tương đối.
//!
//! Một chỗ parse duy nhất cho cả ba adapter (CLI / REPL / MCP) là điều kiện để
//! ba mặt của cùng một API không lệch nhau: `--from 2h` trong REPL phải cho ra
//! đúng cửa sổ mà MCP nhận với `from_ts: "2h"`.
//!
//! Server vẫn nhận `Int` thuần (`fromTs`/`toTs`) — neo "now" ở **client** để đổi
//! kiểu tham số GraphQL không phá client cũ đang khai `$fromTs: Int!`.

use anyhow::anyhow;

/// Mốc thời gian truy vấn: unix giây tuyệt đối, hoặc biểu thức tương đối.
///
/// `untagged` là bắt buộc: MCP nhận JSON, agent có thể gửi `1757000000`
/// (số) lẫn `"2h"` (chuỗi) — cùng một field, không hai.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum TimeArg {
    Unix(i64),
    Expr(String),
}

/// Một chuỗi lỗi duy nhất cho **cả** `parse` lẫn `resolve`: cùng một câu chữ,
/// không lệch tuỳ chỗ gọi — và luôn kèm ví dụ để agent tự sửa được.
fn invalid(input: &str) -> anyhow::Error {
    anyhow!(
        "thời điểm không hợp lệ '{input}'; nhận unix giây (vd 1757000000), \"now\", \
         hoặc khoảng tương đối 90s/15m/2h/7d/1w (vd \"now-2h\")"
    )
}

impl TimeArg {
    /// Tách chuỗi thô thành unix giây hoặc biểu thức. Không validate vội: để
    /// `resolve` báo lỗi có ngữ cảnh (`now` lúc gọi), thay vì parse lỗi ở chỗ
    /// không có "bây giờ" để neo.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        let s = raw.trim();
        Ok(match s.parse::<i64>() {
            Ok(v) => TimeArg::Unix(v),
            Err(_) => TimeArg::Expr(s.to_string()),
        })
    }

    /// Chốt thành unix giây, neo vào đồng hồ của caller.
    ///
    /// Nhân/trừ bằng `checked_*`: `-2h` phải là lỗi chứ không phải "2 giờ trong
    /// tương lai", và số khổng lồ không được làm tràn âm thầm rồi trả về một mốc
    /// thời gian hoàn toàn khác.
    pub fn resolve(&self, now: i64) -> anyhow::Result<i64> {
        let expr = match self {
            TimeArg::Unix(v) => return Ok(*v),
            TimeArg::Expr(s) => s,
        };
        let bad = || invalid(expr);
        let lower = expr.trim().to_ascii_lowercase();
        let body = lower.strip_prefix("now-").unwrap_or(&lower);
        if body == "now" {
            return Ok(now);
        }
        let unit = body.chars().last().ok_or_else(bad)?;
        let digits = &body[..body.len() - unit.len_utf8()];
        let secs = match unit {
            's' => 1,
            'm' => 60,
            'h' => 3_600,
            'd' => 86_400,
            'w' => 604_800,
            _ => return Err(bad()),
        };
        let n: i64 = digits.parse().map_err(|_| bad())?;
        // `0` (cửa sổ rỗng) và số âm (`-2h` ⇒ thành mốc ở tương lai) đều vô nghĩa.
        if n <= 0 {
            return Err(bad());
        }
        let delta = n.checked_mul(secs).ok_or_else(bad)?;
        now.checked_sub(delta).ok_or_else(bad)
    }
}

/// Schema thủ công: `untagged` không nói cho agent biết **cả hai** dạng được
/// chấp nhận, nên agent chỉ thử số rồi báo lỗi schema khi muốn hỏi "2 giờ gần
/// nhất". Ở đây `anyOf` là thứ duy nhất giữ được thông tin đó.
impl schemars::JsonSchema for TimeArg {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TimeArg".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "Unix seconds (integer) or relative window: \"now\", \"2h\", \"now-1d\", \"90m\", \"7d\", \"1w\". Relative values resolve against the client's current clock.",
            "anyOf": [ { "type": "integer" }, { "type": "string" } ]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(raw: &str, now: i64) -> anyhow::Result<i64> {
        TimeArg::parse(raw)?.resolve(now)
    }

    #[test]
    fn unix_seconds_pass_through_untouched() {
        assert_eq!(TimeArg::parse("1757000000").unwrap(), TimeArg::Unix(1757000000));
        // Trim trước khi parse: shell hay dán kèm dấu cách.
        assert_eq!(res(" 1757000000 ", 1).unwrap(), 1757000000);
        // `i64::MIN` không đi qua phép trừ nào — giữ nguyên, không panic.
        assert_eq!(TimeArg::Unix(i64::MIN).resolve(0).unwrap(), i64::MIN);
    }

    #[test]
    fn now_and_relative_windows() {
        assert_eq!(res("now", 1000).unwrap(), 1000);
        assert_eq!(res("2h", 1000).unwrap(), 1000 - 7200);
        assert_eq!(res("now-2h", 1000).unwrap(), 1000 - 7200);
        assert_eq!(res("90s", 1000).unwrap(), 910);
        assert_eq!(res("15m", 1000).unwrap(), 100);
        assert_eq!(res("7d", 1000).unwrap(), 1000 - 604_800);
        // 1 tuần = 604_800s (không phải 6_048_000 — đó là 10 tuần).
        assert_eq!(res("1w", 1000).unwrap(), 1000 - 604_800);
        // Case-insensitive + trim.
        assert_eq!(res(" 2H ", 1000).unwrap(), 1000 - 7200);
    }

    #[test]
    fn garbage_is_rejected_with_one_shared_message() {
        for bad in ["", "abc", "2y", "h", "0h", "-2h", "99999999999999999999h"] {
            let err = res(bad, 1000).expect_err(&format!("`{bad}` phải bị từ chối"));
            let msg = err.to_string();
            assert!(msg.contains(&format!("'{bad}'")), "thiếu input trong lỗi: {msg}");
            assert!(msg.contains("1757000000"), "lỗi phải kèm ví dụ unix giây: {msg}");
            assert!(msg.contains("now-2h"), "lỗi phải kèm ví dụ tương đối: {msg}");
        }
    }
}
