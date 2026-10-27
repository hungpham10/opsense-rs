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
mod sitemap;
mod tenant;
mod token;

use std::sync::Arc;

use opsense_mlib::lru::LruCache;

pub use errors::AdminError;
pub use helpers::sha256_hex;
pub use jwt::Jwt;
pub use sitemap::{FileMap, Site};
pub use tenant::{AuthConfig, Tenant};
pub use token::{Token, UserTokenInfo};

use crate::resolver::{DbKind, Resolver};

pub struct Admin {
    resolver: Arc<Resolver>,

    // @NOTE: caching
    //
    // CHỈ cache theo **service** (`user:<user_id>`), tuyệt đối không cache theo
    // `token_id`. `token_id` là khoá danh tính của hàng `sys_token_map`, nó
    // **không đổi** khi token được phát lại; còn giá trị token thì đổi. Cache
    // khoá theo nó sẽ giữ plaintext của token *cũ* vĩnh viễn (không TTL, chỉ
    // LRU theo sức chứa) ⇒ mọi lần phát token từ lần thứ hai trở đi đều bị
    // từ chối 401 vì `verify_user_token` so constant-time với plaintext cũ.
    //
    // Bù lại, tầng introspect của Nginx đã cache 60s (`conf/nginx/vhost/
    // 04-api.conf`), nên bỏ cache ở đây chỉ tốn thêm ~1 lần giải mã AES mỗi
    // 60s mỗi token — không đáng để đổi lấy việc phải nhớ write-through ở
    // mọi nơi phát token. Đó chính là bẫy đã gây ra bug này.
    cache_unencrypted_tokens_by_services: Arc<LruCache<(i64, String), Option<String>, 32>>,
}

impl Admin {
    pub fn new(resolver: &Arc<Resolver>) -> Self {
        Self {
            resolver: resolver.clone(),
            cache_unencrypted_tokens_by_services: Arc::new(LruCache::new(10 * 32)),
        }
    }

    /// Helper lấy DB pool theo tenant_id (internal use, gọi từ traits).
    /// `Err` khi chưa cấu hình `DB_DSN` — trước đây là `panic!`.
    pub(crate) fn dbt(&self, tenant_id: i64) -> Result<&sqlx::AnyPool, AdminError> {
        self.resolver
            .database(tenant_id)
            .ok_or_else(|| AdminError::Other(format!("missing DB pool for tenant {tenant_id}")))
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
        use opsense_mlib::sops::decrypt;
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

                let pool = self.dbt(tenant_id)?;
                let mut conn = pool.acquire().await?;
                let row = sqlx::query(
                    "SELECT token FROM sys_token_map WHERE tenant_id = $1 AND service = $2",
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

    /// Đọc plaintext của token từ `sys_token_map` theo `token_id`.
    ///
    /// **Cố ý không cache.** `token_id` là id của hàng `sys_token_map`, không đổi
    /// khi token được phát lại, nên cache theo nó sẽ trả về plaintext cũ. Xem
    /// ghi chú ở trường `cache_unencrypted_tokens_by_services` trên struct
    /// `Admin`. Gọi thẳng DB: introspect đã được Nginx cache 60s nên chi phí
    /// thực tế không đáng kể.
    pub(crate) async fn get_unencrypted_token_by_id(
        &self,
        tenant_id: i64,
        token_id: i64,
    ) -> Result<String, AdminError> {
        use opsense_mlib::sops::decrypt;
        use sqlx::Row;

        let pool = self.dbt(tenant_id)?;
        let mut conn = pool.acquire().await?;
        let row = sqlx::query("SELECT token FROM sys_token_map WHERE tenant_id = $1 AND id = $2")
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
        decrypt(&key, &encrypted_bytes)
            .map_err(|e| AdminError::Other(format!("Decrypt failed: {e}")))
    }

    // --------------------------------------------------------------
    // Sitemap & file-mapping methods (admin entity)
    // --------------------------------------------------------------

    /// Upsert nhiều dòng sitemap (sys_sitemap) cho tenant.
    pub async fn insert_or_update_sites(
        &self,
        tenant_id: i64,
        sites: Vec<Site>,
    ) -> Result<(), AdminError> {
        let pool = self.dbt(tenant_id)?;
        let mut conn = pool.acquire().await?;

        for site in sites {
            let created_at = helpers::format_dt_for_db(site.last_mod);
            let updated_at = created_at.clone();

            let sql = match self.kind(tenant_id) {
                DbKind::Postgres => {
                    concat!(
                        "INSERT INTO sys_sitemap (tenant_id, created_at, updated_at, loc, freq, priority) VALUES (",
                        "$1::timestamptz, $2::timestamptz, $3::timestamptz, $4, $5, $6)",
                        " ON CONFLICT (tenant_id, loc)",
                        " DO UPDATE SET updated_at = EXCLUDED.updated_at, freq = EXCLUDED.freq, priority = EXCLUDED.priority",
                    )
                }
                DbKind::MySql => {
                    concat!(
                        "INSERT INTO sys_sitemap (tenant_id, created_at, updated_at, loc, freq, priority) VALUES (",
                        "?, ?, ?, ?, ?, ?)",
                        " ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at), freq = VALUES(freq), priority = VALUES(priority)",
                    )
                }
                DbKind::Sqlite | DbKind::Unknown => {
                    concat!(
                        "INSERT INTO sys_sitemap (tenant_id, created_at, updated_at, loc, freq, priority) VALUES (",
                        "?, ?, ?, ?, ?, ?)",
                        " ON CONFLICT (tenant_id, loc) DO UPDATE SET",
                        "   updated_at = excluded.updated_at,",
                        "   freq = excluded.freq,",
                        "   priority = excluded.priority",
                    )
                }
            };

            sqlx::query(sql)
                .bind(tenant_id)
                .bind(created_at)
                .bind(updated_at)
                .bind(&site.loc)
                .bind(&site.freq)
                .bind(site.priority)
                .execute(&mut *conn)
                .await?;
        }
        Ok(())
    }

    /// Lấy danh sách sitemap hiện tại của tenant.
    pub async fn list_sites(&self, tenant_id: i64) -> Result<Vec<Site>, AdminError> {
        use sqlx::Row;
        let pool = self.dbt(tenant_id)?;
        let mut conn = pool.acquire().await?;

        let rows = sqlx::query(
            "SELECT loc, freq, priority, created_at FROM sys_sitemap WHERE tenant_id = ? ORDER BY loc",
        )
        .bind(tenant_id)
        .fetch_all(&mut *conn)
        .await?;

        let mut sites = Vec::with_capacity(rows.len());
        for row in rows {
            let loc: String = row.try_get::<String, _>(0)?;
            let freq: String = row.try_get::<String, _>(1)?;
            let priority: f64 = row.try_get::<f64, _>(2)?;
            let created_at: Option<String> = row.try_get::<Option<String>, _>(3)?;
            let last_mod = helpers::parse_dt(created_at)?.unwrap_or_else(|| chrono::Utc::now());

            sites.push(Site { loc, freq, priority, last_mod });
        }
        Ok(sites)
    }

    /// Resolve file path via sys_filemap (src -> dest). Trả về original path nếu không có ánh xạ.
    pub async fn get_full_path(
        &self,
        tenant_id: i64,
        path: &str,
    ) -> Result<String, AdminError> {
        use sqlx::Row;
        let pool = self.dbt(tenant_id)?;
        let mut conn = pool.acquire().await?;

        let dest: Option<String> = sqlx::query(
            "SELECT dest FROM sys_filemap WHERE tenant_id = ? AND src = ?",
        )
        .bind(tenant_id)
        .bind(path)
        .fetch_optional(&mut *conn)
        .await?
        .map(|row| row.try_get::<String, _>("dest"))
        .transpose()?;

        dest.ok_or_else(|| {
            AdminError::Other(format!("Not found path {path} for tenant {tenant_id}"))
        })
    }
}

#[cfg(test)]
mod tests {
    // Smoke tests cần DB thật — gated theo `#[ignore]`, chạy qua
    // `cargo test -- --ignored` khi có envs (DB_DSN, REDIS_DSN, MASTER_KEY).
}
