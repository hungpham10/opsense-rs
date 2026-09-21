use std::env;

use chrono::{DateTime, NaiveDateTime, Utc};

use crate::entities::admin::errors::AdminError;
use crate::resolver::DbKind;

/// Sinh key cho `sys_token_map` từ `user_id`.
/// Service name format: `"user:<user_id>"`.
pub fn user_token_service(user_id: &str) -> String {
    format!("user:{user_id}")
}

/// SHA-256 hex encoding (lowercase).
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Constant-time byte slice comparison.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;

    a.ct_eq(b).into()
}

/// Parse datetime từ DB — sqlx `Any` driver không implement `Decode<Any>` cho
/// chrono type, nên ta lấy `String` rồi parse bằng chrono (RFC 3339,
/// Postgres `timestamptz` text, hoặc MySQL/SQLite `TIMESTAMP` không offset).
pub fn parse_dt(s: Option<String>) -> Result<Option<DateTime<Utc>>, AdminError> {
    let Some(s) = s else { return Ok(None) };
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }
    parse_datetime(s)
        .map(Some)
        .ok_or_else(|| AdminError::Other(format!("Invalid datetime `{s}`")))
}

/// Lấy master key từ `MASTER_KEY` env var. Sau này thay bằng KMS SDK call.
pub async fn get_master_key() -> Result<Vec<u8>, AdminError> {
    // TODO: Sau này thay thế đoạn này bằng gọi KMS SDK
    env::var("MASTER_KEY")
        .map(|s| s.into_bytes())
        .map_err(|_| AdminError::Other("Missing MASTER_KEY".into()))
}

// =========================================================================
// TIMESTAMPTZ binding helpers
// =========================================================================
//
// `sqlx::Any` không implement `Encode<Any>`/`Decode<Any>` cho chrono type —
// nó chỉ có per-backend impl. Khi bind `String` (RFC 3339) qua `Any`, sqlx
// convert thành `&str` cho backend thật, và Postgres strict type-checker từ
// chối implicit cast từ `text` → `timestamptz`.
//
// Giải pháp: format thành "YYYY-MM-DD HH:MM:SS+00:00" (common ground cả 3
// backend đều parse được), rồi dùng `$N::timestamptz` trong SQL cho Postgres.
// MySQL/SQLite: bind string literal trực tiếp, driver parse thành TIMESTAMP.

/// Format `DateTime<Utc>` thành string mà cả Postgres `TIMESTAMPTZ`, MySQL
/// `TIMESTAMP`, SQLite `TIMESTAMP` đều parse được.
pub fn format_dt_for_db(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S+00:00").to_string()
}

/// Sinh placeholder thích hợp cho từng dialect khi bind datetime.
/// - Postgres: `$N::timestamptz` ép kiểu text → timestamptz
/// - MySQL/SQLite: `$N` bind thẳng, driver parse string → TIMESTAMP
pub fn tz_placeholder(kind: DbKind, n: usize) -> String {
    match kind {
        DbKind::Postgres => format!("${n}::timestamptz"),
        DbKind::MySql | DbKind::Sqlite | DbKind::Unknown => format!("${n}"),
    }
}

/// Parse các định dạng datetime mà cả 3 backend trả về thành `DateTime<Utc>`:
/// - RFC 3339: `YYYY-MM-DDTHH:MM:SS[.fraction](Z|±HH[:MM])`
/// - Postgres text: `YYYY-MM-DD HH:MM:SS[.fraction]±HH` (offset không phút)
/// - MySQL/SQLite text: `YYYY-MM-DD HH:MM:SS` (không TZ — coi như UTC)
fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    // RFC 3339 chặt (`T` + offset đầy đủ) — đường đi nhanh.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Offset linh hoạt: có/không dấu hai chấm, `T` hoặc space phân cách.
    const OFFSET_FORMATS: [&str; 2] = ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.f%#z"];
    for fmt in OFFSET_FORMATS {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }

    // Không offset (MySQL/SQLite) → coi như UTC.
    const NAIVE_FORMATS: [&str; 2] = ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"];
    for fmt in NAIVE_FORMATS {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(naive.and_utc());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hex() {
        let input = b"hello world";
        let result = sha256_hex(input);
        assert_eq!(
            result,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }

    #[test]
    fn test_parse_dt() {
        // RFC 3339.
        let r = parse_dt(Some("2026-01-02T03:04:05Z".into())).unwrap();
        assert!(r.is_some());

        // MySQL/SQLite (không TZ).
        let r = parse_dt(Some("2026-01-02 03:04:05".into())).unwrap();
        assert!(r.is_some());

        // Cùng một instant dù format khác nhau.
        let via_z = parse_dt(Some("2026-01-02T03:04:05Z".into()))
            .unwrap()
            .unwrap();
        let via_space = parse_dt(Some("2026-01-02 03:04:05+00".into()))
            .unwrap()
            .unwrap();
        assert_eq!(via_z, via_space);

        // Postgres timestamptz text representation (offset without minutes).
        let r = parse_dt(Some("2026-01-02 03:04:05+00".into())).unwrap();
        assert_eq!(
            r.map(format_dt_for_db),
            Some("2026-01-02 03:04:05+00:00".into())
        );
        let r = parse_dt(Some("2026-09-07 23:20:47+00".into())).unwrap();
        assert_eq!(
            r.map(format_dt_for_db),
            Some("2026-09-07 23:20:47+00:00".into())
        );

        // Fractional seconds — khớp với RFC 3339 ở độ phân giải nano giây.
        let frac = parse_dt(Some("2026-09-06 23:09:22.416527+00".into()))
            .unwrap()
            .unwrap();
        let frac_z = parse_dt(Some("2026-09-06T23:09:22.416527Z".into()))
            .unwrap()
            .unwrap();
        assert_eq!(frac, frac_z);

        // Offset khác 0 được trừ về UTC.
        let with_offset = parse_dt(Some("2026-01-02 05:04:05+02:00".into()))
            .unwrap()
            .unwrap();
        assert_eq!(with_offset, via_z);

        assert!(parse_dt(None).unwrap().is_none());
        assert!(parse_dt(Some(String::new())).unwrap().is_none());
        assert!(parse_dt(Some("not-a-date".into())).is_err());
    }

    #[test]
    fn test_format_dt_for_db() {
        // 2026-01-02 03:04:05 UTC.
        let t = DateTime::from_timestamp(1_767_323_045, 0).unwrap();
        assert_eq!(format_dt_for_db(t), "2026-01-02 03:04:05+00:00");

        // Round-trip: parse → format ra đúng chuẩn lưu DB.
        let dt = parse_dt(Some("2026-09-07 23:20:47.123456+00".into()))
            .unwrap()
            .unwrap();
        assert_eq!(format_dt_for_db(dt), "2026-09-07 23:20:47+00:00");
    }

    #[test]
    fn test_user_token_service() {
        assert_eq!(user_token_service("alice"), "user:alice");
    }
}
