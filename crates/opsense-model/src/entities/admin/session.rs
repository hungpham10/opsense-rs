//! Session management service.
//!
//! Quản lý 2 bảng:
//! - `sys_long_sessions`: Ed25519 keypair cho REPL/MCP signing (8h TTL,
//!   lazy cleanup inline khi issue/resolve mới).
//! - `sys_short_sessions`: OAuth2 access_token storage (5min TTL,
//!   cleanup qua DB partition drop).

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use rand::RngCore;
use sqlx::Row;

use crate::entities::admin::errors::AdminError;
use crate::entities::admin::helpers::{parse_dt, sha256_hex};
use crate::entities::admin::Admin;

/// Thông tin long session trả về khi gọi `/session/issue`.
#[derive(Debug, Clone)]
pub struct LongSessionInfo {
    pub session_id:    String, // base64(public_key)
    pub private_key:   String, // base64(private_key_bytes_32)
    pub expires_in_secs: i64,
}

/// Thông tin short session — chỉ hash lưu DB, plaintext là access_token.
#[derive(Debug, Clone)]
pub struct ShortSessionInfo {
    pub session_id:    String,
    pub access_token:  String,
    pub expires_in_secs: i64,
}

/// Thông tin long session cho list endpoint.
#[derive(Debug, Clone)]
pub struct LongSessionSummary {
    pub session_id:   String,
    pub status:       String,
    pub expires_at:   chrono::DateTime<chrono::Utc>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at:   chrono::DateTime<chrono::Utc>,
}

impl Admin {
    // ----------------------------------------------------------------
    // Long sessions — Ed25519 keypair
    // ----------------------------------------------------------------

    /// Sinh Ed25519 keypair, lưu `private_key` mã hóa AES-256-GCM, trả về
    /// cho client `(session_id, private_key)`.
    pub async fn issue_long_session(
        &self,
        tenant_id: i64,
        user_id: &str,
    ) -> Result<LongSessionInfo, AdminError> {
        // 1. Sinh keypair
        let mut csprng = OsRng;
        let mut secret_bytes = [0u8; 32];
        csprng.fill_bytes(&mut secret_bytes);
        let signing_key = SigningKey::from_bytes(&secret_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();

        let session_id = URL_SAFE_NO_PAD.encode(verifying_key.to_bytes());
        let private_key_b64 = URL_SAFE_NO_PAD.encode(secret_bytes);

        // 2. Mã hóa private_key
        let master_key = crate::entities::admin::helpers::get_master_key().await?;
        let encrypted = opsense_libs::sops::encrypt(&master_key, &private_key_b64)
            .map_err(|e| AdminError::Other(format!("Encrypt private_key failed: {e}")))?;

        // 3. Lazy cleanup: xóa expired sessions của user trước
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let _ = sqlx::query(
            "DELETE FROM sys_long_sessions \
             WHERE tenant_id = ?1 AND user_id = ?2 AND expires_at < CURRENT_TIMESTAMP",
        )
        .bind(tenant_id)
        .bind(user_id)
        .execute(&mut *conn)
        .await;

        // 4. Insert session
        let expires_in_secs = 8 * 3600i64; // 8h
        let expires_at = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::seconds(expires_in_secs))
            .ok_or_else(|| AdminError::Other("Timestamp overflow".into()))?;

        sqlx::query(
            "INSERT INTO sys_long_sessions \
             (tenant_id, user_id, session_id, private_key_enc, status, expires_at) \
             VALUES (?1, ?2, ?3, ?4, 'active', ?5)",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(&session_id)
        .bind(&encrypted)
        .bind(expires_at.to_rfc3339())
        .execute(&mut *conn)
        .await?;

        Ok(LongSessionInfo {
            session_id,
            private_key: private_key_b64,
            expires_in_secs,
        })
    }

    /// Revoke long session theo `session_id`.
    pub async fn revoke_long_session(
        &self,
        tenant_id: i64,
        user_id: &str,
        session_id: &str,
    ) -> Result<(), AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        let affected = sqlx::query(
            "UPDATE sys_long_sessions \
             SET status = 'revoked' \
             WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3 AND status = 'active'",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(session_id)
        .execute(&mut *conn)
        .await?
        .rows_affected();

        if affected == 0 {
            return Err(AdminError::Other("Session not found or already revoked".into()));
        }
        Ok(())
    }

    /// List long sessions của user (kèm lazy cleanup).
    pub async fn list_long_sessions(
        &self,
        tenant_id: i64,
        user_id: &str,
    ) -> Result<Vec<LongSessionSummary>, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        // Lazy cleanup expired trước
        let _ = sqlx::query(
            "DELETE FROM sys_long_sessions \
             WHERE tenant_id = ?1 AND user_id = ?2 AND expires_at < CURRENT_TIMESTAMP",
        )
        .bind(tenant_id)
        .bind(user_id)
        .execute(&mut *conn)
        .await;

        // Select
        let rows = sqlx::query(
            "SELECT session_id, status, CAST(expires_at AS TEXT) AS expires_at, \
                    CAST(last_used_at AS TEXT) AS last_used_at, \
                    CAST(created_at AS TEXT) AS created_at \
             FROM sys_long_sessions \
             WHERE tenant_id = ?1 AND user_id = ?2 \
             ORDER BY created_at DESC",
        )
        .bind(tenant_id)
        .bind(user_id)
        .fetch_all(&mut *conn)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let session_id: String    = row.try_get(0)?;
            let status:     String    = row.try_get(1)?;
            let expires_at  = parse_dt(Some(row.try_get::<String, _>(2)?))?
                .ok_or_else(|| AdminError::Other("Missing expires_at".into()))?;
            let last_used_at = parse_dt(Some(row.try_get::<String, _>(3)?))?;
            let created_at  = parse_dt(Some(row.try_get::<String, _>(4)?))?
                .ok_or_else(|| AdminError::Other("Missing created_at".into()))?;

            out.push(LongSessionSummary {
                session_id,
                status,
                expires_at,
                last_used_at,
                created_at,
            });
        }
        Ok(out)
    }

    /// Verify long session (lookup + lazy cleanup).
    /// Trả về `Some((session_id, private_key))` nếu hợp lệ.
    pub async fn resolve_long_session(
        &self,
        tenant_id: i64,
        user_id: &str,
        session_id: &str,
    ) -> Result<Option<String>, AdminError> {
        use opsense_libs::sops::decrypt;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        let row = sqlx::query(
            "SELECT private_key_enc, status, CAST(expires_at AS TEXT) AS expires_at \
             FROM sys_long_sessions \
             WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(session_id)
        .fetch_optional(&mut *conn)
        .await?;

        let Some(row) = row else { return Ok(None); };

        let status: String = row.try_get(1)?;
        let expires_at = parse_dt(Some(row.try_get::<String, _>(2)?))?;

        if status != "active" {
            return Ok(None);
        }
        if expires_at.is_none() || expires_at.unwrap() < chrono::Utc::now() {
            // Lazy cleanup
            let _ = sqlx::query(
                "DELETE FROM sys_long_sessions \
                 WHERE tenant_id = ?1 AND session_id = ?2",
            )
            .bind(tenant_id)
            .bind(session_id)
            .execute(&mut *conn)
            .await;
            return Ok(None);
        }

        let encrypted: Vec<u8> = row.try_get(0)?;
        let master_key = crate::entities::admin::helpers::get_master_key().await?;
        let private_key = decrypt(&master_key, &encrypted)
            .map_err(|e| AdminError::Other(format!("Decrypt private_key failed: {e}")))?;

        // Best-effort cập nhật last_used_at
        let _ = sqlx::query(
            "UPDATE sys_long_sessions SET last_used_at = CURRENT_TIMESTAMP \
             WHERE tenant_id = ?1 AND session_id = ?2",
        )
        .bind(tenant_id)
        .bind(session_id)
        .execute(&mut *conn)
        .await;

        Ok(Some(private_key))
    }

    // ----------------------------------------------------------------
    // Short sessions — OAuth2 access_token
    // ----------------------------------------------------------------

    /// Sinh short session (OAuth2 access_token 5 phút).
    pub async fn insert_short_session(
        &self,
        tenant_id: i64,
        user_id: &str,
    ) -> Result<ShortSessionInfo, AdminError> {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let access_token = URL_SAFE_NO_PAD.encode(bytes);
        let session_id   = rand::random::<u64>().to_string();
        let token_hash   = sha256_hex(access_token.as_bytes());

        let expires_in_secs = 300i64; // 5 phút
        let expires_at = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::seconds(expires_in_secs))
            .ok_or_else(|| AdminError::Other("Timestamp overflow".into()))?;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        sqlx::query(
            "INSERT INTO sys_short_sessions \
             (tenant_id, user_id, session_id, token_hash, expires_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(&session_id)
        .bind(&token_hash)
        .bind(expires_at.to_rfc3339())
        .execute(&mut *conn)
        .await?;

        Ok(ShortSessionInfo {
            session_id,
            access_token,
            expires_in_secs,
        })
    }

    /// Lookup short session theo access_token.
    /// Trả về `(user_id, session_id)` nếu hợp lệ, None nếu expired/unknown.
    pub async fn lookup_short_session(
        &self,
        tenant_id: i64,
        access_token: &str,
    ) -> Result<Option<(String, String)>, AdminError> {
        let token_hash = sha256_hex(access_token.as_bytes());

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        let row = sqlx::query(
            "SELECT user_id, session_id, CAST(expires_at AS TEXT) AS expires_at \
             FROM sys_short_sessions \
             WHERE tenant_id = ?1 AND token_hash = ?2",
        )
        .bind(tenant_id)
        .bind(&token_hash)
        .fetch_optional(&mut *conn)
        .await?;

        let Some(row) = row else { return Ok(None); };

        let expires_at = parse_dt(Some(row.try_get::<String, _>(2)?))?;
        if expires_at.is_none() || expires_at.unwrap() < chrono::Utc::now() {
            return Ok(None);
        }

        let user_id:    String = row.try_get(0)?;
        let session_id: String = row.try_get(1)?;
        Ok(Some((user_id, session_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `LongSessionInfo` Debug + Clone work.
    #[test]
    fn test_long_session_info_clone() {
        let info = LongSessionInfo {
            session_id: "session-1".into(),
            private_key: "priv".into(),
            expires_in_secs: 3600,
        };
        let cloned = info.clone();
        assert_eq!(cloned.session_id, "session-1");
        assert_eq!(cloned.expires_in_secs, 3600);
    }

    /// `ShortSessionInfo` Debug + Clone work.
    #[test]
    fn test_short_session_info_clone() {
        let info = ShortSessionInfo {
            session_id: "s".into(),
            access_token: "tok".into(),
            expires_in_secs: 300,
        };
        let cloned = info.clone();
        assert_eq!(cloned.access_token, "tok");
        assert_eq!(cloned.expires_in_secs, 300);
    }

    /// `LongSessionSummary` Debug + Clone work.
    #[test]
    fn test_long_session_summary_clone() {
        let now = chrono::Utc::now();
        let s = LongSessionSummary {
            session_id: "s1".into(),
            status: "active".into(),
            expires_at: now,
            last_used_at: None,
            created_at: now,
        };
        let cloned = s.clone();
        assert_eq!(cloned.status, "active");
        assert!(cloned.last_used_at.is_none());
    }
}
