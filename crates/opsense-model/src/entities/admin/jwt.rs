use sqlx::Row;

use crate::entities::admin::errors::AdminError;
use crate::entities::admin::helpers::{constant_time_eq, parse_dt, sha256_hex};
use crate::entities::admin::Admin;

/// JWT-style verification (sha256-hashed base token).
///
/// Hiện tại chỉ hỗ trợ `abt_*` base token (do admin cấp), lookup theo
/// `sha256(plaintext)`. Sau này có thể mở rộng cho OIDC/JWT validation.
#[async_trait::async_trait]
pub trait Jwt: Send + Sync {
    /// Verify base token: lookup theo hash, chặn revoked/expired, so khớp
    /// plaintext constant-time (chống hash collision). Trả `user_id` nếu hợp lệ.
    async fn verify_user_token(
        &self,
        tenant_id: i64,
        token: &str,
    ) -> Result<Option<String>, AdminError>;
}

#[async_trait::async_trait]
impl Jwt for Admin {
    async fn verify_user_token(
        &self,
        tenant_id: i64,
        token: &str,
    ) -> Result<Option<String>, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let row = sqlx::query(
            "SELECT id, user_id, token_id, \
             CAST(expires_at AS TEXT) AS expires_at, \
             CAST(revoked_at AS TEXT) AS revoked_at \
             FROM sys_user \
             WHERE tenant_id = ?1 AND token_hash = ?2",
        )
        .bind(tenant_id)
        .bind(sha256_hex(token.as_bytes()))
        .fetch_optional(&mut *conn)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let revoked_at = parse_dt(row.try_get(3)?)?;
        if revoked_at.is_some() {
            return Ok(None);
        }
        let expires_at = parse_dt(row.try_get(2)?)?;
        if let Some(expires_at) = expires_at
            && expires_at < chrono::Utc::now()
        {
            return Ok(None);
        }

        let record_id: i64 = row.try_get(0)?;
        let user_id: String = row.try_get(1)?;
        let token_id: i64 = row.try_get(2)?;

        let stored = self.get_unencrypted_token_by_id(tenant_id, token_id).await?;
        if !constant_time_eq(stored.as_bytes(), token.as_bytes()) {
            return Ok(None);
        }

        // Best-effort cập nhật last_used_at
        let _ = sqlx::query("UPDATE sys_user SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?1")
            .bind(record_id)
            .execute(&mut *conn)
            .await;

        Ok(Some(user_id))
    }
}
