use std::env;

use chrono::{DateTime, Utc};

use crate::entities::admin::errors::AdminError;

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

/// Parse datetime từ DB — sqlx `Any` driver chưa implement
/// `Decode<Type, Any>` cho `chrono::DateTime<Utc>`, nên ta lấy `String` rồi
/// parse thủ công (Postgres trả `2026-...`, MySQL trả `2026-...`, SQLite
/// trả `2026-...`).
pub fn parse_dt(s: Option<String>) -> Result<Option<DateTime<Utc>>, AdminError> {
    let Some(s) = s else { return Ok(None) };
    if s.is_empty() {
        return Ok(None);
    }
    DateTime::parse_from_rfc3339(&s)
        .map(|dt| Some(dt.with_timezone(&Utc)))
        .or_else(|_| {
            // Fallback: thử "YYYY-MM-DD HH:MM:SS" (MySQL/SQLite không có TZ)
            chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| Some(ndt.and_utc()))
        })
        .map_err(|e| AdminError::Other(format!("Invalid datetime `{s}`: {e}")))
}

/// Lấy master key từ `MASTER_KEY` env var. Sau này thay bằng KMS SDK call.
pub async fn get_master_key() -> Result<Vec<u8>, AdminError> {
    // TODO: Sau này thay thế đoạn này bằng gọi KMS SDK
    env::var("MASTER_KEY")
        .map(|s| s.into_bytes())
        .map_err(|_| AdminError::Other("Missing MASTER_KEY".into()))
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
        let r = parse_dt(Some("2026-01-02T03:04:05Z".into())).unwrap();
        assert!(r.is_some());
        let r = parse_dt(Some("2026-01-02 03:04:05".into())).unwrap();
        assert!(r.is_some());
        assert!(parse_dt(None).unwrap().is_none());
        assert!(parse_dt(Some(String::new())).unwrap().is_none());
        assert!(parse_dt(Some("not-a-date".into())).is_err());
    }

    #[test]
    fn test_user_token_service() {
        assert_eq!(user_token_service("alice"), "user:alice");
    }
}
