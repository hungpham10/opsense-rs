//! Admin entity — centralized storage for tenants, tokens, sessions.
//!
//! Refactored từ monolithic `admin.rs` (617 dòng) thành 7 file:
//! - `errors.rs`   — `AdminError` enum
//! - `helpers.rs`  — sha256_hex, parse_dt, get_master_key, user_token_service
//! - `tenant.rs`   — `Tenant` trait (get_tenant_id, get_tenant_auth_config)
//! - `token.rs`    — `Token` trait (get/put/issue/reveal/revoke/list user token)
//! - `jwt.rs`      — `Jwt` trait (verify_user_token)
//! - `device.rs`   — Device flow methods (issue/approve/poll_device_code)
//! - `session.rs`  — Session methods (long/short issue/revoke/lookup)
//!
//! Public API giữ nguyên — tất cả callers hiện tại (`admin/v1.rs`,
//! `api/mod.rs`) tiếp tục work mà không cần thay đổi.

mod device;
mod errors;
mod helpers;
mod jwt;
mod session;
mod tenant;
mod token;

use std::sync::Arc;

use opsense_libs::lru::LruCache;

pub use errors::AdminError;
pub use helpers::sha256_hex;
pub use jwt::Jwt;
pub use tenant::{AuthConfig, Tenant};
pub use token::{Token, UserTokenInfo};

use crate::resolver::Resolver;

pub struct Admin {
    resolver: Arc<Resolver>,

    // @NOTE: caching
    cache_unencrypted_tokens_by_services: Arc<LruCache<(i64, String), Option<String>, 32>>,
    cache_unencrypted_tokens_by_ids: Arc<LruCache<i64, Option<String>, 32>>,
}

impl Admin {
    pub fn new(resolver: &Arc<Resolver>) -> Self {
        Self {
            resolver: resolver.clone(),
            cache_unencrypted_tokens_by_services: Arc::new(LruCache::new(10 * 32)),
            cache_unencrypted_tokens_by_ids: Arc::new(LruCache::new(10 * 32)),
        }
    }

    /// Helper lấy DB pool theo tenant_id (internal use, gọi từ traits).
    pub(crate) fn dbt(&self, tenant_id: i64) -> &sqlx::AnyPool {
        self.resolver.database(tenant_id)
    }

    /// Helper lấy DB dialect theo tenant_id.
    pub(crate) fn kind(&self, tenant_id: i64) -> crate::resolver::DbKind {
        self.resolver.database_kind(tenant_id)
    }

    // @TODO: refresh cache

    // --------------------------------------------------------------
    // Private cache wrappers (không public, không nằm trong trait)
    // --------------------------------------------------------------

    pub(crate) async fn get_unencrypted_token_by_services(
        &self,
        tenant_id: i64,
        service_name: &str,
    ) -> Result<String, AdminError> {
        use opsense_libs::sops::decrypt;
        use sqlx::Row;

        let cache_key = (tenant_id, service_name.to_string());

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

                let key = helpers::get_master_key().await?;
                let token = decrypt(&key, &encrypted_bytes)
                    .map_err(|e| AdminError::Other(format!("Decrypt failed: {e}")))?;

                self.cache_unencrypted_tokens_by_services
                    .put(cache_key_after_done, Some(token.clone()));
                Ok(token)
            }
        }
    }

    pub(crate) async fn get_unencrypted_token_by_id(
        &self,
        tenant_id: i64,
        token_id: i64,
    ) -> Result<String, AdminError> {
        use opsense_libs::sops::decrypt;
        use sqlx::Row;

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

                let key = helpers::get_master_key().await?;
                let token = decrypt(&key, &encrypted_bytes)
                    .map_err(|e| AdminError::Other(format!("Decrypt failed: {e}")))?;

                self.cache_unencrypted_tokens_by_ids
                    .put(cache_key, Some(token.clone()));
                Ok(token)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Smoke tests cần DB thật — gated theo `#[ignore]`, chạy qua
    // `cargo test -- --ignored` khi có envs (DB_DSN, REDIS_DSN, MASTER_KEY).
}
