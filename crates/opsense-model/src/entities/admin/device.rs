//! Device Authorization Grant service (RFC 8628).
//!
//! Quản lý `sys_device_code` table — lưu trữ tạm thời device flow state
//! khi console/CLI authenticate với host. Sau khi user duyệt trên browser,
//! device_code chuyển sang `approved` và tokens được phát hành.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use sqlx::Row;

use crate::entities::admin::errors::AdminError;
use crate::entities::admin::helpers::{parse_dt, sha256_hex};
use crate::entities::admin::Admin;

/// Thông tin device code trả về cho CLI sau khi gọi `/device/code`.
#[derive(Debug, Clone)]
pub struct DeviceCodeInfo {
    pub device_code: String,
    pub user_code:   String,
    pub interval_secs: i32,
    pub expires_in_secs: i64,
    pub verification_uri: String,
}

/// Thông tin token trả về sau khi poll `/device/token`.
#[derive(Debug, Clone)]
pub struct DeviceTokenInfo {
    pub access_token:  String,
    pub refresh_token: String,
    pub session_id:    Option<String>,
}

/// Sinh random bytes dưới dạng base64url (no padding).
fn rand_base64(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(&bytes)
}

impl Admin {
    /// Sinh device_code + user_code mới, lưu vào `sys_device_code`.
    /// Trả về `DeviceCodeInfo` để CLI poll và user nhập trên browser.
    ///
    /// - `device_code`: 64 bytes random, base64url
    /// - `user_code`: 8 bytes random, base64url (dễ nhập hơn)
    /// - TTL: 10 phút
    /// - Poll interval: 5 giây
    pub async fn issue_device_code(
        &self,
        tenant_id: i64,
        verification_uri: &str,
    ) -> Result<DeviceCodeInfo, AdminError> {
        let device_code = rand_base64(64);
        let user_code   = rand_base64(8);
        let interval_secs = 5;
        let expires_in_secs = 600i64; // 10 phút

        let expires_at = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::seconds(expires_in_secs))
            .ok_or_else(|| AdminError::Other("Timestamp overflow".into()))?;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        sqlx::query(
            "INSERT INTO sys_device_code \
             (tenant_id, device_code, user_code, interval_secs, expires_at, status) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending')",
        )
        .bind(tenant_id)
        .bind(&device_code)
        .bind(&user_code)
        .bind(interval_secs)
        .bind(expires_at.to_rfc3339())
        .execute(&mut *conn)
        .await?;

        Ok(DeviceCodeInfo {
            device_code,
            user_code,
            interval_secs,
            expires_in_secs,
            verification_uri: verification_uri.to_string(),
        })
    }

    /// Xử lý khi user nhập `user_code` trên browser và duyệt device flow.
    ///
    /// Tìm `sys_device_code` theo `user_code`, kiểm tra:
    /// - Còn hiệu lực (chưa expired, status = 'pending')
    ///
    /// Sau đó phát hành `access_token` + `refresh_token` (lưu vào
    /// `sys_user` / `sys_token_map`) và cập nhật `sys_device_code` status
    /// = 'approved'.
    ///
    /// Trả về user_id nếu thành công.
    pub async fn approve_device_code(
        &self,
        tenant_id: i64,
        user_id: &str,
        user_code: &str,
    ) -> Result<String, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        // 1. Lookup device code
        let row = sqlx::query(
            "SELECT id, status, CAST(expires_at AS TEXT) AS expires_at \
             FROM sys_device_code WHERE tenant_id = ?1 AND user_code = ?2",
        )
        .bind(tenant_id)
        .bind(user_code)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| AdminError::Other("Device code not found".into()))?;

        let expires_at = parse_dt(Some(row.try_get::<String, _>(2)?))?;
        if expires_at.is_none() || expires_at.unwrap() < chrono::Utc::now() {
            return Err(AdminError::Other("Device code expired".into()));
        }

        let status: String = row.try_get(1)?;
        if status != "pending" {
            return Err(AdminError::Other(format!("Device code already {status}")));
        }

        let record_id: i64 = row.try_get(0)?;

        // 2. Sinh tokens
        let access_token  = rand_base64(32);
        let refresh_token = rand_base64(48);
        // token_hash dùng cho sys_short_sessions (chưa dùng ở đây; access_token
        // được lưu encrypted trong sys_token_map).
        let _token_hash = sha256_hex(access_token.as_bytes());

        // 3. Lưu access_token vào sys_token_map
        let master_key = crate::entities::admin::helpers::get_master_key().await?;
        let encrypted_token = opsense_libs::sops::encrypt(&master_key, &access_token.to_string())
            .map_err(|e| AdminError::Other(format!("Encrypt token failed: {e}")))?;

        let expires_at_ts = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::hours(8))
            .ok_or_else(|| AdminError::Other("Timestamp overflow".into()))?;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        let row2 = sqlx::query(
            "INSERT INTO sys_token_map (tenant_id, service, token) VALUES (?1, ?2, ?3)",
        )
        .bind(tenant_id)
        .bind(format!("user:{user_id}"))
        .bind(&encrypted_token)
        .execute(&mut *conn)
        .await?;

        let token_id = row2.last_insert_id();

        // 4. Lưu refresh_token hash vào sys_user (hoặc cập nhật nếu đã có)
        sqlx::query(
            "INSERT INTO sys_user \
             (tenant_id, user_id, token_hash, token_id, expires_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (tenant_id, user_id) DO UPDATE SET \
               token_hash = EXCLUDED.token_hash, \
               token_id   = EXCLUDED.token_id, \
               expires_at = EXCLUDED.expires_at, \
               revoked_at = NULL",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(sha256_hex(refresh_token.as_bytes()))
        .bind(token_id)
        .bind(expires_at_ts.to_rfc3339())
        .execute(&mut *conn)
        .await?;

        // 5. Cập nhật device_code → approved + lưu tokens
        sqlx::query(
            "UPDATE sys_device_code \
             SET status = 'approved', user_id = ?1, approved_at = CURRENT_TIMESTAMP, \
                 access_token = ?2, refresh_token = ?3 \
             WHERE id = ?4",
        )
        .bind(user_id)
        .bind(&access_token)
        .bind(&refresh_token)
        .bind(record_id)
        .execute(&mut *conn)
        .await?;

        Ok(user_id.to_string())
    }

    /// Poll endpoint — CLI gọi để kiểm tra xem user đã duyệt chưa.
    ///
    /// Trả về `DeviceTokenInfo` nếu approved, hoặc lỗi `authorization_pending`
    /// / `slow_down` / `expired`.
    pub async fn poll_device_token(
        &self,
        tenant_id: i64,
        device_code: &str,
    ) -> Result<DeviceTokenInfo, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        let row = sqlx::query(
            "SELECT status, user_id, CAST(expires_at AS TEXT) AS expires_at, \
                    access_token, refresh_token, interval_secs \
             FROM sys_device_code WHERE tenant_id = ?1 AND device_code = ?2",
        )
        .bind(tenant_id)
        .bind(device_code)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| AdminError::Other("Device code not found".into()))?;

        let status: String = row.try_get(0)?;
        let expires_at = parse_dt(Some(row.try_get::<String, _>(2)?))?;

        if expires_at.is_none() || expires_at.unwrap() < chrono::Utc::now() {
            return Err(AdminError::Other("authorization_expired".into()));
        }

        match status.as_str() {
            "pending" => Err(AdminError::Other("authorization_pending".into())),
            "approved" => {
                let access_token:  String = row.try_get(3)?;
                let refresh_token: String = row.try_get(4)?;
                Ok(DeviceTokenInfo {
                    access_token,
                    refresh_token,
                    session_id: None,
                })
            }
            "denied" => Err(AdminError::Other("access_denied".into())),
            other => Err(AdminError::Other(format!("Unknown device code status: {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `rand_base64` sinh ra string unique mỗi lần gọi, base64url no-padding.
    #[test]
    fn test_rand_base64_uniqueness() {
        let a = rand_base64(32);
        let b = rand_base64(32);
        assert_ne!(a, b, "rand_base64 must produce different output each call");
        assert!(!a.contains('+'), "must use base64url (no +)");
        assert!(!a.contains('/'), "must use base64url (no /)");
        assert!(!a.contains('='), "no padding");
    }

    /// `rand_base64(N)` cho ra độ dài base64url đúng.
    #[test]
    fn test_rand_base64_length() {
        // 16 bytes → 22 base64 chars (4 * ceil(16/3) = 22)
        assert_eq!(rand_base64(16).len(), 22);
        // 32 bytes → 43 chars
        assert_eq!(rand_base64(32).len(), 43);
    }

    /// `DeviceCodeInfo` derive Debug + Clone đúng.
    #[test]
    fn test_device_code_info_clone() {
        let info = DeviceCodeInfo {
            device_code: "abc".into(),
            user_code: "xyz".into(),
            interval_secs: 5,
            expires_in_secs: 600,
            verification_uri: "/device".into(),
        };
        let cloned = info.clone();
        assert_eq!(cloned.device_code, "abc");
        assert_eq!(cloned.user_code, "xyz");
    }
}
