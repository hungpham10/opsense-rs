use std::env;
use std::io::Error as IoError;
use std::sync::Arc;

use opsense_libs::lru::LruCache;
use opsense_libs::sops::{decrypt, encrypt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::AnyPool;
use sqlx::Row;

use crate::resolver::{DbKind, Resolver};

/// Lỗi nội bộ của admin — đóng gói cả sqlx lẫn DbErr-shaped messages để
/// upstream HTTP layer chỉ cần một kiểu duy nhất.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("io error: {0}")]
    Io(#[from] IoError),
    #[error("{0}")]
    Other(String),
}

pub struct Admin {
    resolver: Arc<Resolver>,

    // @NOTE: caching
    cache_unencrypted_tokens_by_services: Arc<LruCache<(i64, String), Option<String>, 32>>,
    cache_unencrypted_tokens_by_ids: Arc<LruCache<i64, Option<String>, 32>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AuthConfig {
    id: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    jwt_mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    jwt_secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    session_secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    oidc_issuer: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    oidc_client_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    oidc_client_secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    oidc_jwks_url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    oidc_expected_alg: Option<String>,
}

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

fn user_token_service(user_id: &str) -> String {
    format!("user:{user_id}")
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;

    a.ct_eq(b).into()
}

/// Parse datetime từ DB — sqlx `Any` driver chưa implement
/// `Decode<Type, Any>` cho `chrono::DateTime<Utc>`, nên ta lấy `String` rồi
/// parse thủ công (Postgres trả `2026-...`, MySQL trả `2026-...`, SQLite
/// trả `2026-...`).
fn parse_dt(s: Option<String>) -> Result<Option<DateTime<Utc>>, AdminError> {
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

impl Admin {
    pub fn new(resolver: &Arc<Resolver>) -> Self {
        Self {
            resolver: resolver.clone(),
            cache_unencrypted_tokens_by_services: Arc::new(LruCache::new(10 * 32)),
            cache_unencrypted_tokens_by_ids: Arc::new(LruCache::new(10 * 32)),
        }
    }

    fn dbt(&self, tenant_id: i64) -> &AnyPool {
        self.resolver.database(tenant_id)
    }

    fn kind(&self, tenant_id: i64) -> DbKind {
        self.resolver.database_kind(tenant_id)
    }

    async fn get_master_key(&self) -> Result<Vec<u8>, AdminError> {
        // TODO: Sau này thay thế đoạn này bằng gọi KMS SDK
        env::var("MASTER_KEY")
            .map(|s| s.into_bytes())
            .map_err(|_| AdminError::Other("Missing MASTER_KEY".into()))
    }

    // @TODO: refresh cache

    // --------------------------------------------------------------
    pub async fn get_tenant_id(&self, host: &String) -> Result<i64, AdminError> {
        let pool = self.dbt(0);
        let mut conn = pool.acquire().await?;
        let row = sqlx::query("SELECT id FROM sys_tenant WHERE host = ?1")
            .bind(host)
            .fetch_optional(&mut *conn)
            .await?;
        match row {
            Some(row) => {
                let id: i64 = row.try_get(0)?;
                Ok(id)
            }
            None => Err(AdminError::Other(format!("Not found host {host}"))),
        }
    }

    pub async fn get_tenant_auth_config(
        &self,
        host: &String,
        oidc_name: &str,
    ) -> Result<AuthConfig, AdminError> {
        let tenant_id = self.get_tenant_id(host).await?;
        let pool = self.dbt(0);
        let mut conn = pool.acquire().await?;

        let row = sqlx::query(
            "SELECT id, jwt_mode, jwt_secret, session_secret, oidc_issuer, oidc_jwks_url, \
             oidc_client_id, oidc_client_secret, oidc_expected_alg \
             FROM sys_oidc WHERE tenant_id = ?1 AND name = ?2",
        )
        .bind(tenant_id)
        .bind(oidc_name)
        .fetch_optional(&mut *conn)
        .await?;

        let Some(row) = row else {
            return Err(AdminError::Other(format!(
                "Not found host {host} for oidc {oidc_name}"
            )));
        };

        let id: i64 = row.try_get(0)?;
        let jwt_mode: Option<String> = row.try_get(1)?;
        let jwt_secret_id: Option<i64> = row.try_get(2)?;
        let session_secret_id: Option<i64> = row.try_get(3)?;
        let oidc_issuer: Option<String> = row.try_get(4)?;
        let oidc_jwks_url: Option<String> = row.try_get(5)?;
        let oidc_client_id: Option<String> = row.try_get(6)?;
        let oidc_client_secret_id: Option<i64> = row.try_get(7)?;
        let oidc_expected_alg: Option<String> = row.try_get(8)?;

        let jwt_secret = match jwt_secret_id {
            Some(token_id) => Some(self.get_unencrypted_token_by_id(tenant_id, token_id).await?),
            None => None,
        };
        let session_secret = match session_secret_id {
            Some(token_id) => Some(self.get_unencrypted_token_by_id(tenant_id, token_id).await?),
            None => None,
        };
        let oidc_client_secret = match oidc_client_secret_id {
            Some(token_id) => Some(self.get_unencrypted_token_by_id(tenant_id, token_id).await?),
            None => None,
        };

        Ok(AuthConfig {
            id,
            jwt_mode,
            jwt_secret,
            session_secret,
            oidc_issuer,
            oidc_client_id,
            oidc_client_secret,
            oidc_jwks_url,
            oidc_expected_alg,
        })
    }

    // --------------------------------------------------------------
    pub async fn get_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &String,
    ) -> Result<String, AdminError> {
        self.get_unencrypted_token_by_services(tenant_id, service_name)
            .await
    }

    async fn get_unencrypted_token_by_services(
        &self,
        tenant_id: i64,
        service_name: &String,
    ) -> Result<String, AdminError> {
        let cache_key = (tenant_id, service_name.clone());

        match self.cache_unencrypted_tokens_by_services.get(&cache_key) {
            Some(Some(token)) => Ok(token),
            Some(None) => Err(AdminError::Other(format!(
                "Not found service {service_name}, tenant {tenant_id}"
            ))),
            None => {
                let cache_key_after_done = cache_key.clone();
                self.cache_unencrypted_tokens_by_services
                    .put(cache_key, None);

                let pool = self.dbt(tenant_id);
                let mut conn = pool.acquire().await?;
                let row = sqlx::query(
                    "SELECT token FROM sys_token_map WHERE tenant_id = ?1 AND service = ?2",
                )
                .bind(tenant_id)
                .bind(service_name)
                .fetch_optional(&mut *conn)
                .await?;
                let Some(row) = row else {
                    return Err(AdminError::Other(format!(
                        "Not found service {service_name}, tenant {tenant_id}"
                    )));
                };
                let encrypted_bytes: Vec<u8> = row.try_get(0)?;

                let key = self.get_master_key().await?;
                let token = decrypt(&key, &encrypted_bytes)
                    .map_err(|e| AdminError::Other(format!("Decrypt failed: {e}")))?;

                self.cache_unencrypted_tokens_by_services
                    .put(cache_key_after_done, Some(token.clone()));
                Ok(token)
            }
        }
    }

    async fn get_unencrypted_token_by_id(
        &self,
        tenant_id: i64,
        token_id: i64,
    ) -> Result<String, AdminError> {
        let cache_key = token_id;

        match self.cache_unencrypted_tokens_by_ids.get(&cache_key) {
            Some(Some(token)) => Ok(token),
            Some(None) => Err(AdminError::Other(format!(
                "Not found token {token_id}, tenant {tenant_id}"
            ))),
            None => {
                let pool = self.dbt(tenant_id);
                let mut conn = pool.acquire().await?;
                let row = sqlx::query(
                    "SELECT token FROM sys_token_map WHERE tenant_id = ?1 AND id = ?2",
                )
                .bind(tenant_id)
                .bind(token_id)
                .fetch_optional(&mut *conn)
                .await?;
                let Some(row) = row else {
                    return Err(AdminError::Other(format!(
                        "Not found token_id {token_id} for tenant {tenant_id}"
                    )));
                };
                let encrypted_bytes: Vec<u8> = row.try_get(0)?;

                let key = self.get_master_key().await?;
                let token = decrypt(&key, &encrypted_bytes)
                    .map_err(|e| AdminError::Other(format!("Decrypt failed: {e}")))?;

                self.cache_unencrypted_tokens_by_ids
                    .put(cache_key, Some(token.clone()));
                Ok(token)
            }
        }
    }

    pub async fn put_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &String,
        token_plain: &String,
    ) -> Result<(), AdminError> {
        let key = self.get_master_key().await?;
        let encrypted = encrypt(&key, token_plain)
            .map_err(|e| AdminError::Other(format!("Encrypt failed: {e}")))?;

        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;

        // Cú pháp upsert tùy dialect.
        let sql = if self.kind(tenant_id).is_mysql() {
            "INSERT INTO sys_token_map (tenant_id, service, token) \
             VALUES (?1, ?2, ?3) \
             ON DUPLICATE KEY UPDATE token = VALUES(token), updated_at = CURRENT_TIMESTAMP"
        } else {
            "INSERT INTO sys_token_map (tenant_id, service, token) \
             VALUES (?1, ?2, ?3) \
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
            .put((tenant_id, service_name.clone()), Some(token_plain.clone()));
        Ok(())
    }

    // --------------------------------------------------------------
    /// Cấp mới (hoặc rotate) base token cho user. Plaintext chỉ trả về một
    /// lần duy nhất; bản mã hoá nằm trong sys_token_map, sys_user chỉ giữ
    /// hash + id tham chiếu. Rotate ghi đè cùng hàng sys_token_map nên
    /// token_id giữ nguyên.
    pub async fn issue_user_token(
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
        let row = sqlx::query("SELECT id FROM sys_token_map WHERE tenant_id = ?1 AND service = ?2")
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
             VALUES (?1, ?2, ?3, ?4, ?5, NULL) \
             ON DUPLICATE KEY UPDATE \
             token_hash = VALUES(token_hash), \
             token_id = VALUES(token_id), \
             expires_at = VALUES(expires_at), \
             revoked_at = NULL, \
             updated_at = CURRENT_TIMESTAMP"
        } else {
            "INSERT INTO sys_user (tenant_id, user_id, token_hash, token_id, expires_at, revoked_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, NULL) \
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

    /// Kiểm tra base token: lookup theo hash, chặn revoked/expired, sau đó
    /// giải mã và so khớp plaintext constant-time (chống hash collision).
    /// Trả về user_id nếu hợp lệ.
    pub async fn verify_user_token(
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

    /// Admin lấy lại plaintext token của user (token do admin quản lý toàn bộ)
    pub async fn reveal_user_token(&self, tenant_id: i64, user_id: &str) -> Result<String, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let row = sqlx::query(
            "SELECT token_id FROM sys_user WHERE tenant_id = ?1 AND user_id = ?2",
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

    pub async fn revoke_user_token(&self, tenant_id: i64, user_id: &str) -> Result<(), AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let result = sqlx::query(
            "UPDATE sys_user SET revoked_at = CURRENT_TIMESTAMP \
             WHERE tenant_id = ?1 AND user_id = ?2",
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

    /// Liệt kê base token của tenant, kèm 4 ký tự cuối để nhận diện
    pub async fn list_user_tokens(&self, tenant_id: i64) -> Result<Vec<UserTokenInfo>, AdminError> {
        let pool = self.dbt(tenant_id);
        let mut conn = pool.acquire().await?;
        let rows = sqlx::query(
            "SELECT user_id, token_id, \
             CAST(expires_at AS TEXT) AS expires_at, \
             CAST(revoked_at AS TEXT) AS revoked_at, \
             CAST(last_used_at AS TEXT) AS last_used_at, \
             CAST(created_at AS TEXT) AS created_at \
             FROM sys_user WHERE tenant_id = ?1",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn has_env() -> bool {
        ["DB_DSN", "REDIS_DSN", "MASTER_KEY"]
            .iter()
            .all(|k| std::env::var(k).is_ok())
    }

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

    #[tokio::test]
    #[ignore]
    async fn test_get_tenant_id_smoke() {
        if !has_env() {
            return;
        }
        // Smoke test cần DB thật — chỉ chạy khi môi trường dev có envs.
    }
}
