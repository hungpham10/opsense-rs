use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use opsense_libs::sops::encrypt;

use crate::entities::admin::errors::AdminError;
use crate::entities::admin::helpers::{get_master_key, sha256_hex, user_token_service};
use crate::entities::admin::Admin;

/// Thông tin base token của một user (không bao giờ chứa plaintext đầy đủ)
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserTokenInfo {
    pub user_id: String,

    /// 4 ký tự cuối của plaintext token để nhận diện khi review
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_hint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,

    pub created_at: DateTime<Utc>,
}

/// Generic token vault (`sys_token_map`) + user base token (`sys_user`).
///
/// Tất cả token plaintext đều được mã hoá AES-256-GCM qua `sops` với
/// `MASTER_KEY` (env var), chỉ giữ ciphertext trong DB.
#[async_trait::async_trait]
pub trait Token: Send + Sync {
    /// Tra token plaintext từ `sys_token_map` theo `(tenant_id, service)`.
    async fn get_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &str,
    ) -> Result<String, AdminError>;

    /// Upsert token vào `sys_token_map` (encrypt trước khi ghi).
    async fn put_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &str,
        token_plain: &str,
    ) -> Result<(), AdminError>;

    /// Cấp/rotate `abt_*` base token cho user. Plaintext chỉ trả 1 lần.
    async fn issue_user_token(
        &self,
        tenant_id: i64,
        user_id: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<String, AdminError>;

    /// Admin lấy lại plaintext token của user.
    async fn reveal_user_token(&self, tenant_id: i64, user_id: &str) -> Result<String, AdminError>;

    /// Revoke base token (set `sys_user.revoked_at`).
    async fn revoke_user_token(&self, tenant_id: i64, user_id: &str) -> Result<(), AdminError>;

    /// List tất cả base token của tenant.
    async fn list_user_tokens(&self, tenant_id: i64) -> Result<Vec<UserTokenInfo>, AdminError>;
}

#[async_trait::async_trait]
impl Token for Admin {
    async fn get_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &str,
    ) -> Result<String, AdminError> {
        self.get_unencrypted_token_by_services(tenant_id, service_name)
            .await
    }

    async fn put_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &str,
        token_plain: &str,
    ) -> Result<(), AdminError> {
        let key = get_master_key().await?;
        let encrypted = encrypt(&key, &token_plain.to_string())
            .map_err(|e| AdminError::Other(format!("Encrypt failed: {e}")))?;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        // Cú pháp upsert tùy dialect.
        let sql = if self.kind(tenant_id).is_mysql() {
            "INSERT INTO sys_token_map (tenant_id, service, token) \
             VALUES ($1, $2, $3) \
             ON DUPLICATE KEY UPDATE token = VALUES(token), updated_at = CURRENT_TIMESTAMP"
        } else {
            "INSERT INTO sys_token_map (tenant_id, service, token) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, service) DO UPDATE SET \
             token = EXCLUDED.token, updated_at = CURRENT_TIMESTAMP"
        };

        sqlx::query(sql)
            .bind(tenant_id)
            .bind(service_name)
            .bind(encrypted)
            .execute(&mut *conn)
            .await?;

        self.cache_unencrypted_tokens_by_services
            .put((tenant_id, service_name.to_string()), Some(token_plain.to_string()));
        Ok(())
    }

    async fn issue_user_token(
        &self,
        tenant_id: i64,
        user_id: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<String, AdminError> {
        use rand::RngCore;

        let mut raw = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut raw);
        let token_plain = format!(
            "abt_{}",
            raw.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let service = user_token_service(user_id);

        self.put_unencrypted_token(tenant_id, &service, &token_plain)
            .await?;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let row = sqlx::query("SELECT id FROM sys_token_map WHERE tenant_id = $1 AND service = $2")
            .bind(tenant_id)
            .bind(&service)
            .fetch_optional(&mut *conn)
            .await?;
        let Some(row) = row else {
            return Err(AdminError::Other(format!(
                "Not found token map entry for service {service}, tenant {tenant_id}"
            )));
        };
        let token_id: i64 = row.try_get(0)?;

        let upsert_sql = if self.kind(tenant_id).is_mysql() {
            "INSERT INTO sys_user (tenant_id, user_id, token_hash, token_id, expires_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, NULL) \
             ON DUPLICATE KEY UPDATE \
             token_hash = VALUES(token_hash), \
             token_id = VALUES(token_id), \
             expires_at = VALUES(expires_at), \
             revoked_at = NULL, \
             updated_at = CURRENT_TIMESTAMP"
        } else {
            "INSERT INTO sys_user (tenant_id, user_id, token_hash, token_id, expires_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, NULL) \
             ON CONFLICT (tenant_id, user_id) DO UPDATE SET \
             token_hash = EXCLUDED.token_hash, \
             token_id = EXCLUDED.token_id, \
             expires_at = EXCLUDED.expires_at, \
             revoked_at = NULL, \
             updated_at = CURRENT_TIMESTAMP"
        };

        sqlx::query(upsert_sql)
            .bind(tenant_id)
            .bind(user_id)
            .bind(sha256_hex(token_plain.as_bytes()))
            .bind(token_id)
            .bind(expires_at.map(|dt| dt.to_rfc3339()))
            .execute(&mut *conn)
            .await?;

        Ok(token_plain)
    }

    async fn reveal_user_token(&self, tenant_id: i64, user_id: &str) -> Result<String, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let row = sqlx::query(
            "SELECT token_id FROM sys_user WHERE tenant_id = $1 AND user_id = $2",
        )
        .bind(tenant_id)
        .bind(user_id)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(row) = row else {
            return Err(AdminError::Other(format!(
                "Not found user {user_id}, tenant {tenant_id}"
            )));
        };
        let token_id: i64 = row.try_get(0)?;
        self.get_unencrypted_token_by_id(tenant_id, token_id).await
    }

    async fn revoke_user_token(&self, tenant_id: i64, user_id: &str) -> Result<(), AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let result = sqlx::query(
            "UPDATE sys_user SET revoked_at = CURRENT_TIMESTAMP \
             WHERE tenant_id = $1 AND user_id = $2",
        )
        .bind(tenant_id)
        .bind(user_id)
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(AdminError::Other(format!(
                "Not found user {user_id}, tenant {tenant_id}"
            )));
        }
        Ok(())
    }

    async fn list_user_tokens(&self, tenant_id: i64) -> Result<Vec<UserTokenInfo>, AdminError> {
        use crate::entities::admin::helpers::parse_dt;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let rows = sqlx::query(
            "SELECT user_id, token_id, \
             CAST(expires_at AS TEXT) AS expires_at, \
             CAST(revoked_at AS TEXT) AS revoked_at, \
             CAST(last_used_at AS TEXT) AS last_used_at, \
             CAST(created_at AS TEXT) AS created_at \
             FROM sys_user WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(&mut *conn)
        .await?;

        let mut infos = Vec::with_capacity(rows.len());
        for row in rows {
            let user_id: String = row.try_get(0)?;
            let token_id: i64 = row.try_get(1)?;
            let expires_at = parse_dt(row.try_get(2)?)?;
            let revoked_at = parse_dt(row.try_get(3)?)?;
            let last_used_at = parse_dt(row.try_get(4)?)?;
            let created_at = parse_dt(row.try_get(5)?)?
                .ok_or_else(|| AdminError::Other("created_at must be present".into()))?;

            let token_hint = self
                .get_unencrypted_token_by_id(tenant_id, token_id)
                .await
                .ok()
                .map(|token| token[token.len().saturating_sub(4)..].to_string());

            infos.push(UserTokenInfo {
                user_id,
                token_hint,
                expires_at,
                revoked_at,
                last_used_at,
                created_at,
            });
        }
        Ok(infos)
    }
}
