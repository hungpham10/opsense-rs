use std::fmt;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use aws_config::{
    BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig,
};
use aws_sdk_s3::Client as S3Client;

use redis::Client as CacheClient;
use redis::aio::MultiplexedConnection;

use sqlx::AnyPool;
use sqlx::ConnectOptions;
use sqlx::any::{AnyConnectOptions, AnyPoolOptions};

use crate::secret::Secret;

/// Database client abstraction - currently sqlx::AnyPool (supports MySQL & PostgreSQL)
pub type DbClient = AnyPool;

#[derive(Clone)]
pub struct Resolver {
    caches: Vec<MultiplexedConnection>,
    dbs: Vec<DbClient>,
    db_kinds: Vec<DbKind>,
    s3_client: Arc<S3Client>,
}

/// Dialect tag of một DB pool — dùng để chọn cú pháp SQL (upsert,
/// identifier quote) theo từng backend mà không cần phụ thuộc feature
/// `any` của sqlx (vốn deprecated cho `AnyKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbKind {
    Postgres,
    MySql,
    Sqlite,
    /// Không xác định được — mặc định dùng cú pháp Postgres/SQLite.
    Unknown,
}

impl DbKind {
    pub fn from_dsn(dsn: &str) -> Self {
        let prefix = dsn
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("");
        match prefix {
            "postgres" | "postgresql" => DbKind::Postgres,
            "mysql" | "mariadb" => DbKind::MySql,
            "sqlite" | "file" => DbKind::Sqlite,
            _ => DbKind::Unknown,
        }
    }

    pub fn is_mysql(self) -> bool {
        matches!(self, DbKind::MySql)
    }
}

impl fmt::Debug for Resolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolver")
            .field("caches_count", &self.caches.len())
            .field("dbs_count", &self.dbs.len())
            .finish()
    }
}

impl Resolver {
    pub async fn new(secret: Arc<Secret>) -> Result<Self, Error> {
        let mut caches = Vec::new();
        let mut dbs = Vec::new();

        // Build DB DSN from secrets or environment fallback
        let mysql_host = std::env::var("MYSQL_HOST").unwrap_or_else(|_| "".to_string());
        let mysql_port = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "".to_string());
        let mysql_password = std::env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "".to_string());
        let mysql_user = std::env::var("MYSQL_USER").unwrap_or_else(|_| "".to_string());
        let mysql_db = std::env::var("MYSQL_DATABASE").unwrap_or_else(|_| "".to_string());

        // Build DB DSN from secrets or environment fallback
        let postgres_host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "".to_string());
        let postgres_port = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "".to_string());
        let postgres_password = std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "".to_string());
        let postgres_user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "".to_string());
        let postgres_db = std::env::var("POSTGRES_DATABASE").unwrap_or_else(|_| "".to_string());

        let db_dsn = if postgres_host.len() > 0 {
            secret.get("DB_DSN", "/").await.unwrap_or(format!(
                "postgres://{}:{}@{}:{}/{}",
                postgres_user, postgres_password, postgres_host, postgres_port, postgres_db,
            )) 
        } else {
            secret.get("DB_DSN", "/").await.unwrap_or(format!(
                "mysql://{}:{}@{}:{}/{}",
                mysql_user, mysql_password, mysql_host, mysql_port, mysql_db,
            )) 
        };

        // Process each comma-separated DSN
        let mut db_kinds: Vec<DbKind> = Vec::new();
        for dsn in db_dsn.split(",") {
            let dsn = dsn.trim();
            if dsn.is_empty() {
                continue;
            }

            // Parse DSN into AnyConnectOptions (works for mysql://, postgres://, etc.)
            let url = reqwest::Url::parse(dsn).map_err(|e| {
                Error::new(ErrorKind::InvalidInput, format!("bad DB_DSN {dsn}: {e}"))
            })?;
            let opts = AnyConnectOptions::from_url(&url).map_err(|e| {
                Error::new(ErrorKind::InvalidInput, format!("bad DB_DSN {dsn}: {e}"))
            })?;

            // Apply logging options
            let opts = opts
                .log_statements(log::LevelFilter::Info)
                .log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(1));

            // Ghi nhớ dialect để Admin/admin sqlx-emit chọn cú pháp đúng
            // (upsert, identifier quote…) mà không phụ thuộc feature `any`.
            db_kinds.push(DbKind::from_dsn(dsn));

            // Create connection pool
            match AnyPoolOptions::new()
                .max_connections(10)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(30))
                .idle_timeout(Some(Duration::from_secs(30)))
                .max_lifetime(Some(Duration::from_secs(120)))
                .connect_with(opts)
                .await
            {
                Ok(pool) => dbs.push(pool),
                Err(e) => eprintln!("Error during connect to database: {e}"),
            }
        }

        // S3 client setup
        let s3_endpoint = secret.get("S3_ENDPOINT", "/").await.unwrap_or_default();
        let s3_region = secret.get("S3_REGION", "/").await.unwrap_or_default();
        let s3_client = Arc::new(S3Client::new(
            &(aws_config::defaults(BehaviorVersion::latest())
                .timeout_config(
                    TimeoutConfig::builder()
                        .operation_timeout(Duration::from_secs(30))
                        .operation_attempt_timeout(Duration::from_millis(10000))
                        .build(),
                )
                .region(
                    RegionProviderChain::first_try(Region::new(s3_region.clone()))
                        .or_default_provider(),
                )
                .endpoint_url(s3_endpoint.clone())
                .load()
                .await),
        ));
        let _ = secret.get("S3_BUCKET", "/").await.unwrap_or_default();

        // Redis setup
        let redis_host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "".to_string());
        let redis_port = std::env::var("REDIS_PORT").unwrap_or_else(|_| "".to_string());
        let redis_password = std::env::var("REDIS_PASSWORD").unwrap_or_else(|_| "".to_string());
        let redis_username = std::env::var("REDIS_USERNAME").unwrap_or_else(|_| "".to_string());

        let redis_dsn = secret.get("REDIS_DSN", "/").await.unwrap_or(format!(
            "redis://{redis_username}:{redis_password}@{redis_host}:{redis_port}",
        ));

        for dsn in redis_dsn.split(",") {
            let dsn = dsn.trim();
            if dsn.is_empty() {
                continue;
            }

            let client = CacheClient::open(dsn).map_err(|error| {
                Error::other(format!("New redis client to {dsn} failed: {error}"))
            })?;

            let conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|error| Error::other(format!("Connect to {dsn} failed: {error}")))?;

            caches.push(conn);
        }

        Ok(Self {
            caches,
            dbs,
            db_kinds,
            s3_client,
        })
    }

    pub fn cache(&self, tenant_id: i64) -> MultiplexedConnection {
        self.caches
            .get((tenant_id % (self.caches.len() as i64)) as usize)
            .expect("Failed to get cache connection")
            .clone()
    }

    pub fn database(&self, tenant_id: i64) -> &DbClient {
        self.dbs
            .get((tenant_id % (self.dbs.len() as i64)) as usize)
            .unwrap_or_else(|| panic!("Failed to get database client for tenant_id: {}", tenant_id))
    }

    pub fn database_kind(&self, tenant_id: i64) -> DbKind {
        self.db_kinds
            .get((tenant_id % (self.db_kinds.len() as i64)) as usize)
            .copied()
            .unwrap_or(DbKind::Unknown)
    }

    pub fn databases(&self) -> &Vec<DbClient> {
        &self.dbs
    }

    pub fn s3(&self) -> Arc<S3Client> {
        self.s3_client.clone()
    }
}
