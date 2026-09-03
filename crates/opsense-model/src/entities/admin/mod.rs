mod api_map;
mod article_map;
mod database_map;
mod file_map;
mod link_streams_to_sinks;
mod oidc;
mod sinks;
mod sitemap;
mod streams;
mod table_map;
mod tenant;
mod token_map;
mod user;

pub use api_map::Entity as ApiMap;
pub use article_map::Entity as ArticleMap;
pub use database_map::Entity as DatabaseMap;
pub use file_map::Entity as FileMap;
pub use link_streams_to_sinks::Entity as LinkStreamsToSinks;
pub use oidc::Entity as Oidc;
pub use sinks::Entity as Sinks;
pub use sitemap::Entity as Sitemap;
pub use streams::Entity as Streams;
pub use table_map::Entity as TableMap;
pub use tenant::Entity as Tenant;
pub use token_map::Entity as TokenMap;
pub use user::Entity as User;

use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sea_orm::sea_query::{Alias, Condition, Expr, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection,
    DatabaseTransaction, DbErr, EntityTrait, ExprTrait, JoinType, NotSet, QueryFilter, QuerySelect,
    RuntimeErr, Set, TransactionTrait, Value as OrmValue,
};

use algorithm::{LruCache, Operator, decrypt, encrypt};
use chrono::{DateTime, Utc};
use integration::Api as ApiEngine;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use utoipa::{IntoParams, ToSchema};
use vector_runtime::Component;

use crate::resolver::Resolver;

static API_PLACEHOLDE_REGEX: OnceLock<Regex> = OnceLock::new();

pub struct Admin {
    // @NOTE: controller
    resolver: Arc<Resolver>,
    api: Arc<ApiEngine>,

    // @NOTE: caching
    cache_unencrypted_tokens_by_services: Arc<LruCache<(i64, String), Option<String>, 32>>,
    cache_unencrypted_tokens_by_ids: Arc<LruCache<i64, Option<String>, 32>>,
    cache_api_info_by_name: Arc<LruCache<String, Option<Api>, 32>>,
    cache_api_info_by_id: Arc<LruCache<i64, Option<Api>, 32>>,
    cache_connections: Arc<LruCache<(i64, String), DatabaseConnection, 32>>,
}

#[derive(Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct Article {
    pub title: String,
    pub loc: String,
    pub name: String,
    pub language: String,
    pub keywords: Option<String>,

    #[schema(value_type = String, format = DateTime)]
    pub published_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Default, ToSchema)]
pub struct Site {
    pub loc: String,
    pub freq: String,
    pub priority: f64,

    #[schema(value_type = String, format = DateTime)]
    pub last_mod: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, ToSchema)]
#[repr(i32)]
pub enum ApiType {
    Unknown,
    Create,
    Delete,
    Update,
    Read,
}

impl From<i32> for ApiType {
    fn from(value: i32) -> Self {
        match value {
            1 => ApiType::Create,
            2 => ApiType::Delete,
            3 => ApiType::Update,
            4 => ApiType::Read,
            _ => ApiType::Unknown,
        }
    }
}

impl From<String> for ApiType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "create" => ApiType::Create,
            "read" => ApiType::Read,
            "update" => ApiType::Update,
            "delete" => ApiType::Delete,
            _ => ApiType::Unknown,
        }
    }
}

impl Display for ApiType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ApiType::Unknown => write!(f, "unknown"),
            ApiType::Create => write!(f, "create"),
            ApiType::Read => write!(f, "read"),
            ApiType::Update => write!(f, "update"),
            ApiType::Delete => write!(f, "delete"),
        }
    }
}

impl<'de> Deserialize<'de> for ApiType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ApiType::from(s))
    }
}

impl serde::Serialize for ApiType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, ToSchema, IntoParams)]
pub struct Api {
    pub id: Option<i64>,
    pub name: Option<String>,
    pub mode: Option<ApiType>,
    pub url: Option<String>,
    pub parser: Option<Vec<Operator>>,
    pub ttl: Option<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, ToSchema)]
#[repr(i32)]
pub enum ColumnType {
    Unknown,
    Int32,
    Int64,
    Real,
    Text,
}

impl From<i32> for ColumnType {
    fn from(value: i32) -> Self {
        match value {
            1 => ColumnType::Int32,
            2 => ColumnType::Int64,
            3 => ColumnType::Text,
            4 => ColumnType::Real,
            _ => ColumnType::Unknown,
        }
    }
}

impl From<String> for ColumnType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "i32" => ColumnType::Int32,
            "i64" => ColumnType::Int64,
            "string" | "text" => ColumnType::Text,
            "real" => ColumnType::Real,
            _ => ColumnType::Unknown,
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ColumnType::Unknown => write!(f, "unknown"),
            ColumnType::Int32 => write!(f, "i32"),
            ColumnType::Int64 => write!(f, "i64"),
            ColumnType::Real => write!(f, "real"),
            ColumnType::Text => write!(f, "string"),
        }
    }
}

impl<'de> Deserialize<'de> for ColumnType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ColumnType::from(s))
    }
}

impl serde::Serialize for ColumnType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, ToSchema, IntoParams)]
pub struct ColumnDescription {
    pub name: Option<String>,
    pub kind: Option<ColumnType>,
}

#[derive(Clone, Copy, Debug, PartialEq, ToSchema)]
#[repr(i32)]
pub enum BackendType {
    Unknown,
    Rdbms,
    Duckdb,
}

impl From<i32> for BackendType {
    fn from(value: i32) -> Self {
        match value {
            1 => BackendType::Rdbms,
            2 => BackendType::Duckdb,
            _ => BackendType::Unknown,
        }
    }
}

impl From<String> for BackendType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "postgres" => BackendType::Rdbms,
            "duckdb" => BackendType::Duckdb,
            _ => BackendType::Unknown,
        }
    }
}

impl Display for BackendType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            BackendType::Unknown => write!(f, "unknown"),
            BackendType::Rdbms => write!(f, "rdbms"),
            BackendType::Duckdb => write!(f, "duckdb"),
        }
    }
}

impl<'de> Deserialize<'de> for BackendType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(BackendType::from(s))
    }
}

impl serde::Serialize for BackendType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, ToSchema, IntoParams)]
pub struct Table {
    pub id: Option<i64>,
    pub table: Option<String>,
    pub backend: Option<BackendType>,
    pub columns: Option<Vec<ColumnDescription>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, ToSchema)]
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
#[derive(Serialize, Deserialize, Clone, Debug, Default, ToSchema)]
pub struct UserTokenInfo {
    pub user_id: String,

    /// 4 ký tự cuối của plaintext token để nhận diện khi review
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_hint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, format = DateTime)]
    pub revoked_at: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, format = DateTime)]
    pub last_used_at: Option<DateTime<Utc>>,

    #[schema(value_type = String, format = DateTime)]
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

impl Admin {
    pub fn new(resolver: &Arc<Resolver>) -> Self {
        // @TODO: có cách nào lấy dữ liêụ từ resolver về capacity của cache_unencrypted_tokens_by_services và api

        Self {
            resolver: resolver.clone(),
            api: Arc::new(ApiEngine::new(10 * 32)),
            cache_unencrypted_tokens_by_services: Arc::new(LruCache::new(10 * 32)),
            cache_unencrypted_tokens_by_ids: Arc::new(LruCache::new(10 * 32)),
            cache_api_info_by_name: Arc::new(LruCache::new(10 * 32)),
            cache_api_info_by_id: Arc::new(LruCache::new(10 * 32)),
            cache_connections: Arc::new(LruCache::new(10 * 32)),
        }
    }

    fn dbt(&self, tenant_id: i64) -> &DatabaseConnection {
        self.resolver.database(tenant_id)
    }

    async fn get_master_key(&self) -> Result<Vec<u8>, DbErr> {
        // TODO: Sau này thay thế đoạn này bằng gọi KMS SDK
        env::var("MASTER_KEY")
            .map(|s| s.into_bytes())
            .map_err(|_| DbErr::Custom("Missing MASTER_KEY".into()))
    }

    // @TODO: refresh cache

    // --------------------------------------------------------------
    pub async fn get_tenant_id(&self, host: &String) -> Result<i64, DbErr> {
        match Tenant::find()
            .filter(tenant::Column::Host.eq(host))
            .select_only()
            .column(tenant::Column::Id)
            .into_tuple::<i64>()
            .one(self.dbt(0))
            .await?
        {
            Some(id) => Ok(id),
            None => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found host {}",
                host,
            )))),
        }
    }

    pub async fn get_tenant_auth_config(
        &self,
        host: &String,
        oidc_name: &str,
    ) -> Result<AuthConfig, DbErr> {
        // Bước 1: Lấy tenant_id từ sys_tenant
        let tenant_id = self.get_tenant_id(host).await?;

        let oidc_info = Oidc::find()
            .filter(oidc::Column::TenantId.eq(tenant_id))
            .filter(oidc::Column::Name.eq(oidc_name))
            .select_only()
            .column(oidc::Column::TenantId)
            .column(oidc::Column::JwtMode)
            .column(oidc::Column::JwtSecret)
            .column(oidc::Column::SessionSecret)
            .column(oidc::Column::OidcIssuer)
            .column(oidc::Column::OidcJwksUrl)
            .column(oidc::Column::OidcClientId)
            .column(oidc::Column::OidcClientSecret)
            .column(oidc::Column::OidcExpectedAlg)
            .into_tuple::<(
                i64,
                Option<String>,
                Option<i64>,
                Option<i64>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
                Option<String>,
            )>()
            .one(self.dbt(0))
            .await
            .map_err(|error| {
                DbErr::Query(RuntimeErr::Internal(format!(
                    "Failed when fetch oidc auth_config: {error}",
                )))
            })?;

        match oidc_info {
            Some((
                id,
                jwt_mode,
                jwt_secret_id,
                session_secret_id,
                oidc_issuer,
                oidc_jwks_url,
                oidc_client_id,
                oidc_client_secret_id,
                oidc_expected_alg,
            )) => {
                let jwt_secret = match jwt_secret_id {
                    Some(token_id) => Some(
                        self.get_unencrypted_token_by_id(tenant_id, token_id)
                            .await?,
                    ),
                    None => None,
                };

                let session_secret = match session_secret_id {
                    Some(token_id) => Some(
                        self.get_unencrypted_token_by_id(tenant_id, token_id)
                            .await?,
                    ),
                    None => None,
                };

                let oidc_client_secret = match oidc_client_secret_id {
                    Some(token_id) => Some(
                        self.get_unencrypted_token_by_id(tenant_id, token_id)
                            .await?,
                    ),
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
            None => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found host {} for oidc {}",
                host, oidc_name,
            )))),
        }
    }

    pub async fn get_full_path(&self, tenant_id: i64, path: &String) -> Result<String, DbErr> {
        match FileMap::find()
            .filter(file_map::Column::TenantId.eq(tenant_id))
            .filter(file_map::Column::Src.eq(path))
            .select_only()
            .column(file_map::Column::Dest)
            .into_tuple::<String>()
            .one(self.dbt(tenant_id))
            .await?
        {
            Some(dest) => Ok(dest),
            None => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found path {}",
                path,
            )))),
        }
    }

    // --------------------------------------------------------------
    pub async fn insert_or_update_sites(
        &self,
        tenant_id: i64,
        sites: Vec<Site>,
    ) -> Result<(), DbErr> {
        sitemap::Entity::insert_many(
            sites
                .iter()
                .map(|site| sitemap::ActiveModel {
                    tenant_id: Set(tenant_id),
                    loc: Set(site.loc.clone()),
                    freq: Set(site.freq.clone()),
                    priority: Set(site.priority),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        )
        .on_conflict(
            OnConflict::columns([sitemap::Column::TenantId, sitemap::Column::Loc])
                .update_columns([
                    sitemap::Column::Freq,
                    sitemap::Column::Priority,
                    sitemap::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(self.dbt(tenant_id))
        .await?;

        Ok(())
    }

    pub async fn list_sites(&self, tenant_id: i64) -> Result<Vec<Site>, DbErr> {
        Ok(Sitemap::find()
            .filter(sitemap::Column::TenantId.eq(tenant_id))
            .select_only()
            .column(sitemap::Column::Loc)
            .column(sitemap::Column::Freq)
            .column(sitemap::Column::Priority)
            .column(sitemap::Column::CreatedAt)
            .into_tuple::<(String, String, f64, DateTime<Utc>)>()
            .all(self.dbt(tenant_id))
            .await?
            .iter()
            .map(|(loc, freq, priority, lastmod)| Site {
                loc: loc.clone(),
                freq: freq.clone(),
                priority: *priority,
                last_mod: *lastmod,
            })
            .collect::<Vec<_>>())
    }

    pub async fn insert_or_update_acticles(
        &self,
        tenant_id: i64,
        articles: Vec<Article>,
    ) -> Result<(), DbErr> {
        article_map::Entity::insert_many(
            articles
                .into_iter()
                .map(|article| article_map::ActiveModel {
                    tenant_id: Set(tenant_id),
                    loc: Set(article.loc),
                    title: Set(article.title),
                    name: Set(article.name),
                    language: Set(article.language),
                    keywords: Set(article.keywords),
                    ..Default::default() // id tự sinh
                })
                .collect::<Vec<_>>(),
        )
        .on_conflict(
            OnConflict::column(article_map::Column::Loc)
                .update_columns([
                    article_map::Column::Title,
                    article_map::Column::Name,
                    article_map::Column::Language,
                    article_map::Column::Keywords,
                    article_map::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(self.dbt(tenant_id))
        .await?;
        Ok(())
    }

    pub async fn list_articles(&self, tenant_id: i64) -> Result<Vec<Article>, DbErr> {
        Ok(ArticleMap::find()
            .filter(article_map::Column::TenantId.eq(tenant_id))
            .select_only()
            .column(article_map::Column::Loc)
            .column(article_map::Column::Name)
            .column(article_map::Column::Title)
            .column(article_map::Column::Language)
            .column(article_map::Column::Keywords)
            .column(article_map::Column::CreatedAt)
            .into_tuple::<(
                String,
                String,
                String,
                String,
                Option<String>,
                DateTime<Utc>,
            )>()
            .all(self.dbt(tenant_id))
            .await?
            .iter()
            .map(
                |(loc, name, title, language, keywords, published_at)| Article {
                    loc: loc.clone(),
                    name: name.clone(),
                    title: title.clone(),
                    language: language.clone(),
                    keywords: keywords.clone(),
                    published_at: *published_at,
                },
            )
            .collect::<Vec<_>>())
    }

    // --------------------------------------------------------------
    pub async fn get_unencrypted_token(
        &self,
        tenant_id: i64,
        service_name: &String,
    ) -> Result<String, DbErr> {
        self.get_unencrypted_token_by_services(tenant_id, service_name)
            .await
    }

    async fn get_unencrypted_token_by_services(
        &self,
        tenant_id: i64,
        service_name: &String,
    ) -> Result<String, DbErr> {
        let cache_key = (tenant_id, service_name.clone());

        match self.cache_unencrypted_tokens_by_services.get(&cache_key) {
            Some(Some(token)) => Ok(token),
            Some(None) => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found service {}, tenant {}",
                service_name, tenant_id,
            )))),
            None => {
                let cache_key_after_done = cache_key.clone();

                self.cache_unencrypted_tokens_by_services
                    .put(cache_key, None);

                let encrypted_bytes = TokenMap::find()
                    .select_only()
                    .filter(token_map::Column::TenantId.eq(tenant_id))
                    .filter(token_map::Column::Service.eq(service_name))
                    .column(token_map::Column::Token)
                    .into_tuple::<Vec<u8>>()
                    .one(self.dbt(tenant_id))
                    .await
                    .map_err(|error| {
                        DbErr::Query(RuntimeErr::Internal(format!(
                            "Failed when querying to fetch token id: {error}",
                        )))
                    })?
                    .ok_or_else(|| {
                        DbErr::Query(RuntimeErr::Internal(format!(
                            "Not found service {}, tenant {}",
                            service_name, tenant_id,
                        )))
                    })?;

                let token = decrypt(
                    self.get_master_key()
                        .await
                        .map_err(|error| {
                            DbErr::Query(RuntimeErr::Internal(format!("Decrypt failed: {error}")))
                        })?
                        .as_slice(),
                    encrypted_bytes.as_slice(),
                )
                .map_err(|error| {
                    DbErr::Query(RuntimeErr::Internal(format!("Decrypt failed: {error}")))
                })?;

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
    ) -> Result<String, DbErr> {
        let cache_key = token_id;

        match self.cache_unencrypted_tokens_by_ids.get(&cache_key) {
            Some(Some(token)) => Ok(token),
            Some(None) => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found token {token_id}, tenant {tenant_id}",
            )))),
            None => {
                let encrypted_bytes = TokenMap::find()
                    .select_only()
                    .filter(token_map::Column::TenantId.eq(tenant_id))
                    .filter(token_map::Column::Id.eq(token_id))
                    .column(token_map::Column::Token)
                    .into_tuple::<Vec<u8>>()
                    .one(self.dbt(tenant_id))
                    .await
                    .map_err(|error| {
                        DbErr::Query(RuntimeErr::Internal(format!(
                            "Failed when querying to fetch token data: {error}",
                        )))
                    })?
                    .ok_or_else(|| {
                        DbErr::Query(RuntimeErr::Internal(format!(
                            "Not found token_id {} for tenant {}",
                            token_id, tenant_id,
                        )))
                    })?;

                let token = decrypt(
                    self.get_master_key()
                        .await
                        .map_err(|error| {
                            DbErr::Query(RuntimeErr::Internal(format!("Decrypt failed: {error}")))
                        })?
                        .as_slice(),
                    encrypted_bytes.as_slice(),
                )
                .map_err(|error| {
                    DbErr::Query(RuntimeErr::Internal(format!("Decrypt failed: {error}")))
                })?;

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
    ) -> Result<(), DbErr> {
        let txn = self.dbt(tenant_id).begin().await?;
        self.put_unencrypted_token_txn(&txn, tenant_id, service_name, token_plain)
            .await?;
        txn.commit().await?;

        self.cache_unencrypted_tokens_by_services
            .put((tenant_id, service_name.clone()), Some(token_plain.clone()));
        Ok(())
    }

    async fn put_unencrypted_token_txn(
        &self,
        txn: &DatabaseTransaction,
        tenant_id: i64,
        service_name: &String,
        token_plain: &String,
    ) -> Result<(), DbErr> {
        token_map::Entity::insert(token_map::ActiveModel {
            tenant_id: Set(tenant_id),
            service: Set(service_name.to_owned()),
            token: Set(encrypt(
                self.get_master_key()
                    .await
                    .map_err(|error| {
                        DbErr::Query(RuntimeErr::Internal(format!("Encrypt failed: {error}")))
                    })?
                    .as_slice(),
                token_plain,
            )
            .map_err(|error| {
                DbErr::Query(RuntimeErr::Internal(format!("Encrypt failed: {error}")))
            })?),
            ..Default::default()
        })
        .on_conflict(
            sea_query::OnConflict::columns([
                token_map::Column::TenantId,
                token_map::Column::Service,
            ])
            .update_column(token_map::Column::Token)
            .update_column(token_map::Column::UpdatedAt)
            .to_owned(),
        )
        .exec(txn)
        .await?;
        Ok(())
    }

    pub async fn list_supported_services(&self, tenant_id: i64) -> Result<Vec<String>, DbErr> {
        TokenMap::find()
            .filter(token_map::Column::TenantId.eq(tenant_id))
            .select_only()
            .column(token_map::Column::Service)
            .into_tuple::<String>()
            .all(self.dbt(tenant_id))
            .await
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
    ) -> Result<String, DbErr> {
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

        // Upsert theo (tenant_id, service) không trả về id nên phải select lại
        let token_id = TokenMap::find()
            .filter(token_map::Column::TenantId.eq(tenant_id))
            .filter(token_map::Column::Service.eq(&service))
            .select_only()
            .column(token_map::Column::Id)
            .into_tuple::<i64>()
            .one(self.dbt(tenant_id))
            .await?
            .ok_or_else(|| {
                DbErr::Query(RuntimeErr::Internal(format!(
                    "Not found token map entry for service {service}, tenant {tenant_id}"
                )))
            })?;

        user::Entity::insert(user::ActiveModel {
            tenant_id: Set(tenant_id),
            user_id: Set(user_id.to_owned()),
            token_hash: Set(sha256_hex(token_plain.as_bytes())),
            token_id: Set(token_id),
            expires_at: Set(expires_at),
            revoked_at: Set(None),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::columns([user::Column::TenantId, user::Column::UserId])
                .update_columns([
                    user::Column::TokenHash,
                    user::Column::TokenId,
                    user::Column::ExpiresAt,
                    user::Column::RevokedAt,
                ])
                .to_owned(),
        )
        .exec(self.dbt(tenant_id))
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
    ) -> Result<Option<String>, DbErr> {
        let record = User::find()
            .filter(user::Column::TenantId.eq(tenant_id))
            .filter(user::Column::TokenHash.eq(sha256_hex(token.as_bytes())))
            .one(self.dbt(tenant_id))
            .await?;

        let Some(record) = record else {
            return Ok(None);
        };

        if record.revoked_at.is_some() {
            return Ok(None);
        }

        if let Some(expires_at) = record.expires_at
            && expires_at < chrono::Utc::now()
        {
            return Ok(None);
        }

        let stored = self
            .get_unencrypted_token_by_id(tenant_id, record.token_id)
            .await?;
        if !constant_time_eq(stored.as_bytes(), token.as_bytes()) {
            return Ok(None);
        }

        // Best-effort cập nhật last_used_at
        let _ = user::Entity::update_many()
            .col_expr(user::Column::LastUsedAt, Expr::value(chrono::Utc::now()))
            .filter(user::Column::Id.eq(record.id))
            .exec(self.dbt(tenant_id))
            .await;

        Ok(Some(record.user_id))
    }

    /// Admin lấy lại plaintext token của user (token do admin quản lý toàn bộ)
    pub async fn reveal_user_token(&self, tenant_id: i64, user_id: &str) -> Result<String, DbErr> {
        let token_id = User::find()
            .filter(user::Column::TenantId.eq(tenant_id))
            .filter(user::Column::UserId.eq(user_id))
            .select_only()
            .column(user::Column::TokenId)
            .into_tuple::<i64>()
            .one(self.dbt(tenant_id))
            .await?
            .ok_or_else(|| {
                DbErr::Query(RuntimeErr::Internal(format!(
                    "Not found user {user_id}, tenant {tenant_id}"
                )))
            })?;

        self.get_unencrypted_token_by_id(tenant_id, token_id).await
    }

    pub async fn revoke_user_token(&self, tenant_id: i64, user_id: &str) -> Result<(), DbErr> {
        let result = user::Entity::update_many()
            .col_expr(user::Column::RevokedAt, Expr::value(chrono::Utc::now()))
            .filter(user::Column::TenantId.eq(tenant_id))
            .filter(user::Column::UserId.eq(user_id))
            .exec(self.dbt(tenant_id))
            .await?;

        if result.rows_affected == 0 {
            return Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found user {user_id}, tenant {tenant_id}"
            ))));
        }

        Ok(())
    }

    /// Liệt kê base token của tenant, kèm 4 ký tự cuối để nhận diện
    pub async fn list_user_tokens(&self, tenant_id: i64) -> Result<Vec<UserTokenInfo>, DbErr> {
        let records = User::find()
            .filter(user::Column::TenantId.eq(tenant_id))
            .all(self.dbt(tenant_id))
            .await?;

        let mut infos = Vec::with_capacity(records.len());
        for record in records {
            // Hint chỉ lấy được qua decrypt; fail thì bỏ qua hint
            let token_hint = self
                .get_unencrypted_token_by_id(tenant_id, record.token_id)
                .await
                .ok()
                .map(|token| token[token.len().saturating_sub(4)..].to_string());

            infos.push(UserTokenInfo {
                user_id: record.user_id,
                token_hint,
                expires_at: record.expires_at,
                revoked_at: record.revoked_at,
                last_used_at: record.last_used_at,
                created_at: record.created_at,
            });
        }

        Ok(infos)
    }

    // --------------------------------------------------------------
    pub async fn list_paginated_api_schema(
        &self,
        tenant_id: i64,
        after: i64,
        limit: u64,
    ) -> Result<Vec<Api>, DbErr> {
        Ok(ApiMap::find()
            .filter(api_map::Column::TenantId.eq(tenant_id))
            .filter(api_map::Column::Id.gt(after))
            .select_only()
            .column(api_map::Column::Id)
            .column(api_map::Column::Mode)
            .column(api_map::Column::Name)
            .column(api_map::Column::Url)
            .column(api_map::Column::Parser)
            .column(api_map::Column::Ttl)
            .limit(limit)
            .into_tuple::<(i64, i32, String, String, api_map::Parser, Option<i32>)>()
            .all(self.dbt(tenant_id))
            .await?
            .into_iter()
            .map(|(id, mode, name, url, parser, ttl)| Api {
                id: Some(id),
                mode: Some(ApiType::from(mode)),
                name: Some(name),
                url: Some(url),
                parser: Some(parser.0.clone()),
                ttl,
            })
            .collect())
    }

    pub async fn get_api_schema_by_name(
        &self,
        tenant_id: i64,
        name: &String,
        method: ApiType,
    ) -> Result<Api, DbErr> {
        let cache_key = format!("{name}:{method}");

        match self.cache_api_info_by_name.get(&cache_key) {
            Some(Some(api_info)) => Ok(api_info),
            Some(None) => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found api {} for tenant_id {} ",
                name, tenant_id,
            )))),
            None => {
                let cache_key_after_done = cache_key.clone();

                self.cache_api_info_by_name.put(cache_key, None);

                match ApiMap::find()
                    .select_only()
                    .column(api_map::Column::Id)
                    .column(api_map::Column::Mode)
                    .column(api_map::Column::Name)
                    .column(api_map::Column::Url)
                    .column(api_map::Column::Parser)
                    .column(api_map::Column::Ttl)
                    .filter(api_map::Column::TenantId.eq(tenant_id))
                    .filter(api_map::Column::Name.eq(name))
                    .filter(api_map::Column::Mode.eq(method as i32))
                    .into_tuple::<(i64, i32, String, String, api_map::Parser, Option<i32>)>()
                    .one(self.dbt(tenant_id))
                    .await?
                {
                    Some((id, mode, name, url, parser, ttl)) => {
                        let api_info = Api {
                            id: Some(id),
                            mode: Some(ApiType::from(mode)),
                            name: Some(name),
                            url: Some(url),
                            parser: Some(parser.0.clone()),
                            ttl,
                        };

                        self.cache_api_info_by_name
                            .put(cache_key_after_done, Some(api_info.clone()));
                        Ok(api_info)
                    }
                    None => Err(DbErr::Query(RuntimeErr::Internal(format!(
                        "Not found api {} for tenant_id {} ",
                        name, tenant_id,
                    )))),
                }
            }
        }
    }

    pub async fn get_api_schema_by_id(&self, tenant_id: i64, id: i64) -> Result<Api, DbErr> {
        match self.cache_api_info_by_id.get(&id) {
            Some(Some(api_info)) => Ok(api_info),
            Some(None) => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found api schema for tenant_id {} and id {}",
                tenant_id, id,
            )))),
            None => {
                match ApiMap::find()
                    .select_only()
                    .column(api_map::Column::Id)
                    .column(api_map::Column::Mode)
                    .column(api_map::Column::Name)
                    .column(api_map::Column::Url)
                    .column(api_map::Column::Parser)
                    .column(api_map::Column::Ttl)
                    .filter(api_map::Column::TenantId.eq(tenant_id))
                    .filter(api_map::Column::Id.eq(id))
                    .into_tuple::<(i64, i32, String, String, api_map::Parser, Option<i32>)>()
                    .one(self.dbt(tenant_id))
                    .await
                    .map_err(|error| {
                        DbErr::Query(RuntimeErr::Internal(format!(
                            "Failed to perform SQL of tenant_id {tenant_id} and id {id}: {error}",
                        )))
                    })?
                    .map(|(id, mode, name, url, parser, ttl)| Api {
                        id: Some(id),
                        mode: Some(ApiType::from(mode)),
                        name: Some(name),
                        url: Some(url),
                        parser: Some(parser.0.clone()),
                        ttl,
                    }) {
                    Some(api_info) => {
                        self.cache_api_info_by_id.put(id, Some(api_info.clone()));
                        Ok(api_info)
                    }
                    None => {
                        self.cache_api_info_by_id.put(id, None);
                        Err(DbErr::Query(RuntimeErr::Internal(format!(
                            "Not found api schema for tenant_id {} and id {}",
                            tenant_id, id,
                        ))))
                    }
                }
            }
        }
    }

    pub async fn create_api_schemas(
        &self,
        tenant_id: i64,
        schemas: Vec<Api>,
    ) -> Result<Vec<Api>, DbErr> {
        let mut active_models = Vec::new();
        let re = API_PLACEHOLDE_REGEX.get_or_init(|| Regex::new(r"(?P<key>[^?=&]+)=\{\}").unwrap());

        for (i, schema) in schemas.iter().enumerate() {
            let url = schema
                .url
                .clone()
                .ok_or_else(|| DbErr::Custom(format!("`url` is missing in schema {}", i,)))?;

            active_models.push(api_map::ActiveModel {
                tenant_id: Set(tenant_id),
                name: Set(schema
                    .name
                    .clone()
                    .ok_or_else(|| DbErr::Custom(format!("`name` is missing in schema {}", i)))?),
                url: Set(if let Some((base, query)) = url.split_once('?') {
                    let mut keys = re
                        .captures_iter(query)
                        .map(|cap| cap["key"].to_string())
                        .collect::<Vec<_>>();

                    keys.sort();

                    format!(
                        "{base}?{}",
                        keys.iter()
                            .map(|key| format!("{key}={{}}"))
                            .collect::<Vec<_>>()
                            .join("&")
                    )
                } else {
                    url
                }),
                mode: Set(schema
                    .mode
                    .ok_or_else(|| DbErr::Custom(format!("`mode` is missing in schema {}", i)))?
                    as i32),
                parser: Set(api_map::Parser(schema.parser.clone().ok_or_else(|| {
                    DbErr::Custom(format!("`parser` is missing in schema {}", i))
                })?)),
                ..Default::default()
            });
        }

        ApiMap::insert_many(active_models)
            .exec(self.dbt(tenant_id))
            .await?;

        Ok(ApiMap::find()
            .filter(api_map::Column::TenantId.eq(tenant_id))
            .filter(
                api_map::Column::Name
                    .is_in(schemas.iter().map(|s| s.name.clone()).collect::<Vec<_>>()),
            )
            .all(self.dbt(tenant_id))
            .await?
            .into_iter()
            .map(|m| Api {
                id: Some(m.id),
                name: Some(m.name),
                mode: Some(ApiType::from(m.mode)),
                url: Some(m.url),
                parser: Some(m.parser.0),
                ttl: m.ttl,
            })
            .collect::<Vec<_>>())
    }

    pub async fn perform_api_by_api_id(
        &self,
        tenant_id: i64,
        query_id: i64,
        paths: Vec<String>,
        args: Vec<String>,
        headers: HashMap<String, String>,
        body: Option<JsonValue>,
    ) -> Result<(Vec<JsonValue>, Option<i32>), DbErr> {
        self.perform_api_by_api_info(
            &self.get_api_schema_by_id(tenant_id, query_id).await?,
            paths,
            args,
            headers,
            body,
        )
        .await
    }

    pub async fn perform_api_by_api_name(
        &self,
        tenant_id: i64,
        name: &String,
        mode: ApiType,
        args: Vec<String>,
        headers: HashMap<String, String>,
        body: Option<JsonValue>,
    ) -> Result<(Vec<JsonValue>, Option<i32>), DbErr> {
        self.perform_api_by_api_info(
            &self.get_api_schema_by_name(tenant_id, name, mode).await?,
            vec![],
            args,
            headers,
            body,
        )
        .await
    }

    async fn perform_api_by_api_info(
        &self,
        api_info: &Api,
        paths: Vec<String>,
        args: Vec<String>,
        headers: HashMap<String, String>,
        body: Option<JsonValue>,
    ) -> Result<(Vec<JsonValue>, Option<i32>), DbErr> {
        let mut url = api_info.url.clone().ok_or_else(|| {
            DbErr::Query(RuntimeErr::Internal("Api is broken, missing `url`".into()))
        })?;

        let api_type = api_info.mode.ok_or_else(|| {
            DbErr::Query(RuntimeErr::Internal("Api is broken, missing `mode`".into()))
        })?;

        let parser = api_info.parser.as_ref().ok_or_else(|| {
            DbErr::Query(RuntimeErr::Internal(
                "Api is broken, missing `template`".into(),
            ))
        })?;

        for (i, path) in paths.iter().enumerate() {
            url = url.replacen(format!(":{i}").as_str(), path, 1);
        }

        for arg in args {
            url = url.replacen("{}", &arg, 1);
        }

        if body.is_none() && matches!(api_type, ApiType::Create | ApiType::Update) {
            return Err(DbErr::Query(RuntimeErr::Internal(format!(
                "No body provided for create or update API type: {api_type}",
            ))));
        }

        let query_parser = Arc::new(algorithm::JsonQuery::new(parser.clone()));

        match api_type {
            ApiType::Create => Ok((
                self.api
                    .create(url.as_str(), &query_parser, &headers, body.unwrap())
                    .await
                    .map_err(|e| {
                        DbErr::Query(RuntimeErr::Internal(format!("Error creating: {e}")))
                    })?,
                api_info.ttl,
            )),

            ApiType::Read => Ok((
                self.api
                    .read(url.as_str(), &query_parser, &headers)
                    .await
                    .map_err(|e| {
                        DbErr::Query(RuntimeErr::Internal(format!("Error reading: {e}")))
                    })?,
                api_info.ttl,
            )),

            ApiType::Update => Ok((
                self.api
                    .update(url.as_str(), &query_parser, &headers, body.unwrap())
                    .await
                    .map_err(|e| {
                        DbErr::Query(RuntimeErr::Internal(format!("Error updating: {e}")))
                    })?,
                api_info.ttl,
            )),

            ApiType::Delete => Ok((
                self.api
                    .delete(url.as_str(), &query_parser, &headers)
                    .await
                    .map_err(|e| {
                        DbErr::Query(RuntimeErr::Internal(format!("Error deleting: {e}")))
                    })?,
                api_info.ttl,
            )),

            _ => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Unknown API type: {api_type}"
            )))),
        }
    }

    // --------------------------------------------------------------
    pub async fn list_paginated_table_schema(
        &self,
        tenant_id: i64,
        after: i64,
        limit: u64,
    ) -> Result<Vec<Table>, DbErr> {
        TableMap::find()
            .filter(table_map::Column::TenantId.eq(tenant_id))
            .filter(table_map::Column::Id.gt(after))
            .select_only()
            .column(table_map::Column::Id)
            .column(table_map::Column::Name)
            .column(table_map::Column::Backend)
            .column(table_map::Column::Schema)
            .limit(limit)
            .into_tuple::<(i64, String, Option<i32>, table_map::Schema)>()
            .all(self.dbt(tenant_id))
            .await?
            .into_iter()
            .map(|(id, table, backend, schema)| {
                let backend = backend
                    .ok_or_else(|| DbErr::Custom(format!("Backend is NULL for table '{table}'")))?;
                Ok(Table {
                    id: Some(id),
                    backend: Some(BackendType::from(backend)),
                    table: Some(table.clone()),
                    columns: Some(schema.0.columns.clone()),
                })
            })
            .collect::<Result<Vec<_>, DbErr>>()
    }

    pub async fn is_database_connection_setup(&self, tenant_id: i64) -> Result<bool, DbErr> {
        let result = DatabaseMap::find()
            .filter(database_map::Column::TenantId.eq(tenant_id))
            .select_only()
            .column(database_map::Column::Token)
            .one(self.dbt(tenant_id))
            .await?;
        Ok(result.is_some())
    }

    pub async fn setup_database_connection(
        &self,
        tenant_id: i64,
        token: String,
        dsn: String,
    ) -> Result<(), DbErr> {
        self.setup_database_connection_for_table(tenant_id, token, dsn, None)
            .await
    }

    pub async fn setup_database_connection_for_table(
        &self,
        tenant_id: i64,
        token: String,
        dsn: String,
        table_id: Option<i64>,
    ) -> Result<(), DbErr> {
        let txn = self.dbt(tenant_id).begin().await?;

        self.put_unencrypted_token_txn(&txn, tenant_id, &dsn, &token)
            .await?;

        DatabaseMap::insert(database_map::ActiveModel {
            tenant_id: Set(tenant_id),
            token: Set(token.clone()),
            table_id: match table_id {
                Some(id) => Set(id),
                None => NotSet,
            },
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;
        self.cache_unencrypted_tokens_by_services
            .put((tenant_id, token.clone()), Some(dsn.clone()));
        Ok(())
    }

    pub async fn create_table_schemas(
        &self,
        tenant_id: i64,
        tables: Vec<Table>,
    ) -> Result<Vec<Table>, DbErr> {
        let mut active_models = Vec::new();

        for table in tables.iter() {
            if let Some(columns) = &table.columns {
                for (i, column) in columns.iter().enumerate() {
                    if i == 0 && column.kind != Some(ColumnType::Int64) {}
                }
            }

            active_models.push(table_map::ActiveModel {
                tenant_id: Set(tenant_id),
                name: Set(table.table.clone().unwrap()),
                backend: Set(table.backend.unwrap_or(BackendType::Unknown) as i32),
                schema: Set(table_map::Schema(table_map::SchemaDesciption {
                    columns: table.columns.clone().unwrap(),
                })),
                ..Default::default()
            });
        }

        TableMap::insert_many(active_models)
            .exec(self.dbt(tenant_id))
            .await?;

        Ok(TableMap::find()
            .filter(table_map::Column::TenantId.eq(tenant_id))
            .filter(
                table_map::Column::Name
                    .is_in(tables.iter().map(|t| t.table.clone()).collect::<Vec<_>>()),
            )
            .all(self.dbt(tenant_id))
            .await?
            .into_iter()
            .map(|m| Table {
                id: Some(m.id),
                backend: Some(BackendType::from(m.backend)),
                table: Some(m.name.clone()),
                columns: Some(m.schema.0.columns.clone()),
            })
            .collect::<Vec<_>>())
    }

    pub async fn get_connection_by_id(
        &self,
        tenant_id: i64,
        table_id: i64,
    ) -> Result<DatabaseConnection, DbErr> {
        match DatabaseMap::find()
            .select_only()
            .column(database_map::Column::Token)
            .filter(database_map::Column::TenantId.eq(tenant_id))
            .filter(database_map::Column::TableId.eq(table_id))
            .into_tuple::<String>()
            .one(self.dbt(tenant_id))
            .await?
        {
            Some(token) => {
                // Cache by (tenant_id, token_name) — tables cùng token share 1 pool
                if let Some(conn) = self.cache_connections.get(&(tenant_id, token.clone())) {
                    return Ok(conn);
                }

                let dsn = self.get_unencrypted_token(tenant_id, &token).await?;
                let mut opt = ConnectOptions::new(dsn.to_string());

                opt.max_connections(100)
                    .min_connections(5)
                    .connect_timeout(Duration::from_secs(8))
                    .idle_timeout(Duration::from_secs(8))
                    .max_lifetime(Duration::from_secs(8))
                    .sqlx_logging(true);

                let conn = Database::connect(opt).await?;

                self.cache_connections.put((tenant_id, token), conn.clone());
                Ok(conn)
            }
            None => Err(DbErr::Custom(format!(
                "No token found for tenant {}",
                tenant_id,
            ))),
        }
    }

    pub async fn get_table_info_by_id(
        &self,
        tenant_id: i64,
        table_id: i64,
    ) -> Result<Table, DbErr> {
        match TableMap::find()
            .select_only()
            .column(table_map::Column::Id)
            .column(table_map::Column::Name)
            .column(table_map::Column::Backend)
            .column(table_map::Column::Schema)
            .filter(table_map::Column::TenantId.eq(tenant_id))
            .filter(table_map::Column::Id.eq(table_id))
            .into_tuple::<(i64, String, Option<i32>, table_map::Schema)>()
            .one(self.dbt(tenant_id))
            .await?
            .map(|(id, table, backend, schema)| -> Result<Table, DbErr> {
                let backend = backend
                    .ok_or_else(|| DbErr::Custom(format!("Backend is NULL for table '{table}'")))?;
                Ok(Table {
                    id: Some(id),
                    backend: Some(BackendType::from(backend)),
                    table: Some(table.clone()),
                    columns: Some(schema.0.columns.clone()),
                })
            })
            .transpose()?
        {
            Some(table) => Ok(table),
            None => Err(DbErr::Query(RuntimeErr::Internal(format!(
                "Not found table for tenant_id {} and id {}",
                tenant_id, table_id,
            )))),
        }
    }

    pub async fn read_from_table_by_id(
        &self,
        tenant_id: i64,
        table_id: i64,
        after: i64,
        limit: u64,
        filters: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<HashMap<String, Vec<JsonValue>>, DbErr> {
        let table_info = self.get_table_info_by_id(tenant_id, table_id).await?;

        match table_info.backend {
            Some(BackendType::Rdbms) => {
                self.read_from_rdbms_table(tenant_id, &table_info, after, limit, filters)
                    .await
            }
            Some(backend) => Err(DbErr::Custom(format!("Not support `{backend}`"))),
            None => Err(DbErr::Custom("field `backend` is required".to_string())),
        }
    }

    pub async fn write_to_table_by_id(
        &self,
        tenant_id: i64,
        table_id: i64,
        body: Option<JsonValue>,
        upsert_columns: Option<Vec<String>>,
    ) -> Result<usize, DbErr> {
        let table_info = self.get_table_info_by_id(tenant_id, table_id).await?;

        match table_info.backend {
            Some(BackendType::Rdbms) => {
                self.write_to_rdbms_table(tenant_id, &table_info, body, upsert_columns)
                    .await
            }
            Some(backend) => Err(DbErr::Custom(format!("Not support `{backend}`"))),
            None => Err(DbErr::Custom("field `backend` is required".to_string())),
        }
    }

    pub async fn write_to_rdbms_table(
        &self,
        tenant_id: i64,
        table_info: &Table,
        body: Option<JsonValue>,
        upsert_columns: Option<Vec<String>>,
    ) -> Result<usize, DbErr> {
        let mut stmt = Query::insert();
        let mut columns_to_insert = Vec::new();
        let mut values_to_insert = Vec::new();
        let mut col_names: Vec<String> = Vec::new();

        let body = match body {
            Some(JsonValue::Object(map)) => map,
            _ => return Err(DbErr::Custom("Body must be a JSON object".into())),
        };
        let table_name = table_info
            .table
            .as_deref()
            .ok_or_else(|| DbErr::Custom("Table name is required".into()))?;
        let table_id = table_info
            .id
            .ok_or_else(|| DbErr::Custom("Table id is required".into()))?;
        let columns_schema = table_info
            .columns
            .as_ref()
            .ok_or_else(|| DbErr::Custom("Table schema not found".into()))?;

        stmt.into_table(Alias::new(table_name));

        for col in columns_schema {
            let col_name = match &col.name {
                Some(name) => name,
                None => continue,
            };

            if let Some(value) = body.get(col_name) {
                match (col.kind.unwrap_or(ColumnType::Unknown), value) {
                    (ColumnType::Int32 | ColumnType::Int64, JsonValue::Number(n)) if n.is_i64() => {
                    }
                    (ColumnType::Real, JsonValue::Number(_)) => {}
                    (ColumnType::Text, JsonValue::String(_)) => {}

                    (kind, value) => {
                        let type_str = match value {
                            JsonValue::Null => "null",
                            JsonValue::Bool(_) => "bool",
                            JsonValue::Number(_) => "number",
                            JsonValue::String(_) => "string",
                            JsonValue::Array(_) => "array",
                            JsonValue::Object(_) => "object",
                        };
                        return Err(DbErr::Custom(format!(
                            "Column '{}' (type {:?}) received `{type_str}`, expected matching type",
                            col_name, kind,
                        )));
                    }
                }

                col_names.push(col_name.clone());
                columns_to_insert.push(Alias::new(col_name));
                values_to_insert.push(
                    match value {
                        JsonValue::String(s) => OrmValue::String(Some(s.clone())),
                        JsonValue::Number(n) => {
                            let col_kind = col.kind.unwrap_or(ColumnType::Unknown);
                            match col_kind {
                                ColumnType::Real => {
                                    if let Some(f) = n.as_f64() {
                                        OrmValue::Double(Some(f))
                                    } else {
                                        OrmValue::Double(None)
                                    }
                                }
                                _ => {
                                    if let Some(i) = n.as_i64() {
                                        OrmValue::BigInt(Some(i))
                                    } else if let Some(f) = n.as_f64() {
                                        OrmValue::Double(Some(f))
                                    } else {
                                        OrmValue::BigInt(None)
                                    }
                                }
                            }
                        }
                        _ => OrmValue::String(Some(value.to_string())),
                    }
                    .into(),
                );
            }
        }

        if columns_to_insert.is_empty() {
            return Err(DbErr::Custom("No valid columns to insert".into()));
        }

        // ─── ON CONFLICT (upsert) ──────────────────────────────────────────
        if let Some(ref conflict_cols) = upsert_columns
            && !conflict_cols.is_empty()
        {
            let conflict_alias: Vec<Alias> = conflict_cols
                .iter()
                .map(|c| Alias::new(c.as_str()))
                .collect();

            // Update columns = all inserted columns MINUS conflict columns
            let update_cols: Vec<Alias> = col_names
                .iter()
                .filter(|n| !conflict_cols.contains(n))
                .map(|n| Alias::new(n.as_str()))
                .collect();

            if !update_cols.is_empty() {
                stmt.on_conflict(
                    OnConflict::columns(conflict_alias)
                        .update_columns(update_cols)
                        .to_owned(),
                );
            }
        }

        stmt.columns(columns_to_insert)
            .values_panic(values_to_insert);

        let result = self
            .get_connection_by_id(tenant_id, table_id)
            .await?
            .execute(&stmt)
            .await?;

        Ok(result.rows_affected() as usize)
    }

    pub async fn read_from_rdbms_table(
        &self,
        tenant_id: i64,
        table_info: &Table,
        after: i64,
        limit: u64,
        filters: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<HashMap<String, Vec<JsonValue>>, DbErr> {
        let mut result = HashMap::new();
        let mut stmt = Query::select();

        let table_name = table_info
            .table
            .as_deref()
            .ok_or_else(|| DbErr::Custom("Table name is required".into()))?;
        let table_id = table_info
            .id
            .ok_or_else(|| DbErr::Custom("Table id is required".into()))?;
        let columns = table_info
            .columns
            .as_ref()
            .ok_or_else(|| DbErr::Custom("Table schema not found".into()))?;
        let pkey = columns
            .first()
            .and_then(|c| c.name.as_deref())
            .unwrap_or("id");

        // Chỉ thêm WHERE pkey > after nếu pkey là kiểu số (Int32/Int64)
        // Tránh lỗi "operator does not exist: character varying > bigint"
        // cho các bảng có PK dạng text như ohcl_bank_interest_rate(bank)
        let pkey_is_numeric = columns
            .first()
            .and_then(|c| c.kind)
            .is_some_and(|t| matches!(t, ColumnType::Int32 | ColumnType::Int64));

        for col in columns.iter().filter_map(|c| c.name.as_deref()) {
            stmt.column(Alias::new(col));
        }

        stmt.from(Alias::new(table_name));

        // ─── WHERE: pagination ───────────────────────────────────────────
        if pkey_is_numeric {
            stmt.cond_where(Condition::all().add(Expr::col(Alias::new(pkey)).gt(Expr::val(after))));
        }

        // ─── WHERE: filters ──────────────────────────────────────────────
        if let Some(ref filter_map) = filters {
            for (col_name, value) in filter_map {
                let orm_val: OrmValue = match value {
                    JsonValue::String(s) => OrmValue::String(Some(s.clone())),
                    JsonValue::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            OrmValue::BigInt(Some(i))
                        } else if let Some(f) = n.as_f64() {
                            OrmValue::Double(Some(f))
                        } else {
                            OrmValue::BigInt(None)
                        }
                    }
                    JsonValue::Bool(b) => OrmValue::Bool(Some(*b)),
                    _ => OrmValue::String(Some(value.to_string())),
                };
                stmt.cond_where(
                    Condition::all().add(Expr::col(Alias::new(col_name.as_str())).eq(orm_val)),
                );
            }
        }

        stmt.order_by(Alias::new(pkey), sea_orm::Order::Asc)
            .limit(limit);

        let rows = self
            .get_connection_by_id(tenant_id, table_id)
            .await?
            .query_all(&stmt)
            .await?;

        let col_names = columns
            .iter()
            .filter_map(|c| c.name.clone())
            .collect::<Vec<_>>();

        for col in &col_names {
            result.insert(col.clone(), Vec::with_capacity(rows.len()));
        }

        // Build column name -> ColumnType lookup
        let col_type_map: HashMap<&str, &ColumnType> = columns
            .iter()
            .filter_map(|c| {
                c.name
                    .as_deref()
                    .map(|n| (n, c.kind.as_ref().unwrap_or(&ColumnType::Text)))
            })
            .collect();

        for row in rows {
            for col_name in &col_names {
                let col_type = col_type_map
                    .get(col_name.as_str())
                    .copied()
                    .unwrap_or(&ColumnType::Text);

                let val: JsonValue = match col_type {
                    ColumnType::Int32 => row
                        .try_get::<Option<i32>>("", col_name.as_str())
                        .ok()
                        .flatten()
                        .map(|v| serde_json::json!(v))
                        .unwrap_or(JsonValue::Null),
                    ColumnType::Int64 => row
                        .try_get::<Option<i64>>("", col_name.as_str())
                        .ok()
                        .flatten()
                        .map(|v| serde_json::json!(v))
                        .unwrap_or(JsonValue::Null),
                    ColumnType::Real => row
                        .try_get::<Option<f32>>("", col_name.as_str())
                        .ok()
                        .flatten()
                        .map(|v| serde_json::json!(v))
                        .unwrap_or(JsonValue::Null),
                    _ => {
                        // Text / default
                        row.try_get::<Option<String>>("", col_name.as_str())
                            .ok()
                            .flatten()
                            .map(JsonValue::String)
                            .unwrap_or(JsonValue::Null)
                    }
                };

                if let Some(vec) = result.get_mut(col_name) {
                    vec.push(val);
                }
            }
        }

        Ok(result)
    }
    // --------------------------------------------------------------

    pub async fn into_components(&self, tenant_id: i64) -> Result<Vec<Arc<dyn Component>>, DbErr> {
        let mut unique_streams = HashSet::new();
        let mut unique_sinks = HashSet::new();
        let mut query = LinkStreamsToSinks::find()
            .select_only()
            .column(link_streams_to_sinks::Column::SinkId)
            .column(link_streams_to_sinks::Column::StreamId)
            .column(sinks::Column::Handler)
            .column(streams::Column::Context)
            .join_rev(
                JoinType::InnerJoin,
                sinks::Entity::belongs_to(LinkStreamsToSinks)
                    .from(sinks::Column::Id)
                    .to(link_streams_to_sinks::Column::SinkId)
                    .into(),
            )
            .join_rev(
                JoinType::InnerJoin,
                streams::Entity::belongs_to(LinkStreamsToSinks)
                    .from(streams::Column::Id)
                    .to(link_streams_to_sinks::Column::StreamId)
                    .into(),
            )
            .filter(link_streams_to_sinks::Column::Enabled.eq(true));

        if tenant_id > 0 {
            query = query.filter(link_streams_to_sinks::Column::TenantId.eq(tenant_id));
        }

        Ok(query
            .into_tuple::<(i64, i64, sinks::Handler, streams::Context)>()
            .all(self.dbt(tenant_id))
            .await?
            .into_iter()
            .flat_map(|(sink_id, stream_id, sink, ctx)| {
                let mut pair = Vec::with_capacity(2);

                if unique_streams.insert(stream_id) {
                    pair.extend(ctx.0);
                }
                if unique_sinks.insert(sink_id) {
                    pair.push(sink.0);
                }

                pair
            })
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::ConnectionTrait;
    use std::sync::OnceLock;

    static TEST_ENV: OnceLock<()> = OnceLock::new();

    /// Must be exactly 32 bytes for AES-256-GCM
    const TEST_TABLE_NAME: &str = "test_unit_table";

    fn setup_env() {
        TEST_ENV.get_or_init(|| {
            let _db = std::env::var("DB_DSN").expect(
                "DB_DSN must be set (e.g. postgres://postgres:rootroot@127.0.0.1:5432/test)",
            );
            let _redis = std::env::var("REDIS_DSN")
                .expect("REDIS_DSN must be set (e.g. redis://127.0.0.1:6379)");
            let _master_key = std::env::var("MASTER_KEY").expect(
                "MASTER_KEY must be set (32 bytes for AES-256-GCM, e.g. 'test-master-key-32-bytes-for-aes')",
            );
        });
    }

    async fn create_admin() -> Admin {
        let secret = Arc::new(crate::secret::Secret::new().await.unwrap());
        let resolver = Arc::new(crate::resolver::Resolver::new(secret).await.unwrap());
        Admin::new(&resolver)
    }

    fn admin_db(admin: &Admin) -> &DatabaseConnection {
        admin.resolver.databases().first().unwrap()
    }

    /// Insert a tenant using the private entity (accessible because tests are inside the module)
    async fn setup_tenant(db: &DatabaseConnection, tenant_id: i64) {
        Tenant::insert(tenant::ActiveModel {
            host: Set(format!("test.unit.local.{}", tenant_id)),
            id: Set(tenant_id),
            ..Default::default()
        })
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(tenant::Column::Host)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .await
        .unwrap();
    }

    async fn create_real_table(db: &DatabaseConnection) {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (\
             id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, \
             name VARCHAR(255), \
             value BIGINT)",
            TEST_TABLE_NAME
        );
        db.execute_unprepared(&sql).await.unwrap();
    }

    async fn drop_real_table(db: &DatabaseConnection) {
        let sql = format!("DROP TABLE IF EXISTS \"{}\"", TEST_TABLE_NAME);
        let _ = db.execute_unprepared(&sql).await;
    }

    async fn cleanup_tenant_data(db: &DatabaseConnection, tenant_id: i64) {
        let _ = db
            .execute_unprepared(&format!(
                "DELETE FROM sys_user WHERE tenant_id = {}",
                tenant_id
            ))
            .await;
        let _ = db
            .execute_unprepared(&format!(
                "DELETE FROM sys_token_map WHERE tenant_id = {}",
                tenant_id
            ))
            .await;
        let _ = db
            .execute_unprepared(&format!(
                "DELETE FROM sys_database_map WHERE tenant_id = {}",
                tenant_id
            ))
            .await;
        let _ = db
            .execute_unprepared(&format!(
                "DELETE FROM sys_table_map WHERE tenant_id = {}",
                tenant_id
            ))
            .await;
        let _ = db
            .execute_unprepared(&format!("DELETE FROM sys_tenant WHERE id = {}", tenant_id))
            .await;
    }

    #[tokio::test]
    async fn test_create_table_schema_and_get_info() {
        setup_env();
        let admin = create_admin().await;
        let db = admin_db(&admin);
        let tenant_id: i64 = 100001;

        cleanup_tenant_data(db, tenant_id).await;
        setup_tenant(db, tenant_id).await;

        let columns = vec![
            ColumnDescription {
                name: Some("id".into()),
                kind: Some(ColumnType::Int64),
            },
            ColumnDescription {
                name: Some("name".into()),
                kind: Some(ColumnType::Text),
            },
            ColumnDescription {
                name: Some("value".into()),
                kind: Some(ColumnType::Int64),
            },
        ];

        let tables = vec![Table {
            id: None,
            table: Some(TEST_TABLE_NAME.to_string()),
            backend: Some(BackendType::Rdbms),
            columns: Some(columns.clone()),
        }];

        let result = admin
            .create_table_schemas(tenant_id, tables)
            .await
            .expect("create_table_schemas should succeed");

        assert_eq!(result.len(), 1, "Should create 1 table schema");
        let created = &result[0];
        assert_eq!(created.table.as_deref(), Some(TEST_TABLE_NAME));
        assert_eq!(created.backend, Some(BackendType::Rdbms));

        // Now get info by id
        let table_id = created.id.expect("Table should have an id");
        let info = admin
            .get_table_info_by_id(tenant_id, table_id)
            .await
            .expect("get_table_info_by_id should succeed");

        assert_eq!(info.table.as_deref(), Some(TEST_TABLE_NAME));
        assert_eq!(info.columns.as_ref().map(|c| c.len()), Some(3));

        // List paginated
        let list = admin
            .list_paginated_table_schema(tenant_id, 0, 10)
            .await
            .expect("list should succeed");
        assert!(!list.is_empty(), "Should list at least 1 schema");

        cleanup_tenant_data(db, tenant_id).await;
    }

    #[tokio::test]
    async fn test_write_and_read_rdbms_table() {
        setup_env();
        let admin = create_admin().await;
        let db = admin_db(&admin);
        let tenant_id: i64 = 100002;
        let service = "test_unit_dsn";

        cleanup_tenant_data(db, tenant_id).await;
        drop_real_table(db).await;

        setup_tenant(db, tenant_id).await;
        let db_url = std::env::var("DB_DSN").expect("DB_DSN must be set");

        // Create table schema first to get table_id
        let columns = vec![
            ColumnDescription {
                name: Some("id".into()),
                kind: Some(ColumnType::Int64),
            },
            ColumnDescription {
                name: Some("name".into()),
                kind: Some(ColumnType::Text),
            },
            ColumnDescription {
                name: Some("value".into()),
                kind: Some(ColumnType::Int64),
            },
        ];
        let schemas = admin
            .create_table_schemas(
                tenant_id,
                vec![Table {
                    id: None,
                    table: Some(TEST_TABLE_NAME.to_string()),
                    backend: Some(BackendType::Rdbms),
                    columns: Some(columns.clone()),
                }],
            )
            .await
            .expect("create_table_schemas");
        let table_id = schemas[0].id.expect("table id");

        // Set up DB connection with table_id so get_connection_by_id can find it
        admin
            .setup_database_connection_for_table(
                tenant_id,
                service.to_string(),
                db_url.clone(),
                Some(table_id),
            )
            .await
            .expect("setup_database_connection should succeed");

        create_real_table(db).await;

        // Write data
        let body = serde_json::json!({"name": "test_row", "value": 42});
        let rows = admin
            .write_to_table_by_id(tenant_id, table_id, Some(body), None)
            .await
            .expect("write should succeed");
        assert_eq!(rows, 1, "Should insert 1 row");

        // Read data back
        let data = admin
            .read_from_table_by_id(tenant_id, table_id, 0, 10, None)
            .await
            .expect("read should succeed");

        let names = data.get("name").expect("Should have 'name' column");
        assert_eq!(names.len(), 1, "Should have 1 row");
        assert_eq!(names[0], serde_json::json!("test_row"));

        let values = data.get("value").expect("Should have 'value' column");
        assert_eq!(values[0], serde_json::json!(42));

        // Read with pagination: after=9999 should return nothing
        let data_empty = admin
            .read_from_table_by_id(tenant_id, table_id, 9999, 5, None)
            .await
            .expect("read with high after should succeed");
        if let Some(col) = data_empty.get("name") {
            assert!(col.is_empty(), "Should have no rows with high 'after'");
        }

        drop_real_table(db).await;
        cleanup_tenant_data(db, tenant_id).await;
    }

    #[tokio::test]
    async fn test_write_to_table_invalid_body() {
        setup_env();
        let admin = create_admin().await;
        let db = admin_db(&admin);
        let tenant_id: i64 = 100003;

        cleanup_tenant_data(db, tenant_id).await;
        setup_tenant(db, tenant_id).await;

        // Non-existent table ID should fail
        let result = admin
            .write_to_table_by_id(
                tenant_id,
                99999999,
                Some(serde_json::json!(["not", "an", "object"])),
                None,
            )
            .await;
        assert!(result.is_err(), "Should fail for non-existent table");

        cleanup_tenant_data(db, tenant_id).await;
    }

    #[tokio::test]
    async fn test_write_to_table_wrong_type() {
        setup_env();
        let admin = create_admin().await;
        let db = admin_db(&admin);
        let tenant_id: i64 = 100004;
        let service = "test_type_dsn";

        cleanup_tenant_data(db, tenant_id).await;
        setup_tenant(db, tenant_id).await;

        // Create table schema first to get table_id
        let columns = vec![
            ColumnDescription {
                name: Some("id".into()),
                kind: Some(ColumnType::Int64),
            },
            ColumnDescription {
                name: Some("label".into()),
                kind: Some(ColumnType::Text),
            },
        ];
        let schemas = admin
            .create_table_schemas(
                tenant_id,
                vec![Table {
                    id: None,
                    table: Some("test_type_table".into()),
                    backend: Some(BackendType::Rdbms),
                    columns: Some(columns.clone()),
                }],
            )
            .await
            .expect("create table schema");
        let table_id = schemas[0].id.expect("table id");

        let db_url = std::env::var("DB_DSN").unwrap();
        admin
            .setup_database_connection_for_table(
                tenant_id,
                service.to_string(),
                db_url,
                Some(table_id),
            )
            .await
            .expect("setup_database_connection");

        // Create the actual table
        let sql = "CREATE TABLE IF NOT EXISTS \"test_type_table\" (\
                    id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, \
                    label VARCHAR(255))";
        db.execute_unprepared(sql).await.unwrap();

        // Write with valid text value
        let body = serde_json::json!({"label": "hello"});
        let rows = admin
            .write_to_table_by_id(tenant_id, table_id, Some(body), None)
            .await
            .expect("write with valid string should succeed");
        assert_eq!(rows, 1);

        // Cleanup
        let _ = db
            .execute_unprepared("DROP TABLE IF EXISTS \"test_type_table\"")
            .await;
        cleanup_tenant_data(db, tenant_id).await;
    }

    #[tokio::test]
    async fn test_write_and_read_rdbms_table_with_real_type() {
        setup_env();
        let admin = create_admin().await;
        let db = admin_db(&admin);
        let tenant_id: i64 = 100005;
        let service = "test_real_dsn";
        let table_name = "test_real_table";

        cleanup_tenant_data(db, tenant_id).await;
        let _ = db
            .execute_unprepared(&format!("DROP TABLE IF EXISTS \"{table_name}\""))
            .await;
        setup_tenant(db, tenant_id).await;
        let db_url = std::env::var("DB_DSN").expect("DB_DSN must be set");

        // Schema: id (Int64), label (Text), price (Real)
        let columns = vec![
            ColumnDescription {
                name: Some("id".into()),
                kind: Some(ColumnType::Int64),
            },
            ColumnDescription {
                name: Some("label".into()),
                kind: Some(ColumnType::Text),
            },
            ColumnDescription {
                name: Some("price".into()),
                kind: Some(ColumnType::Real),
            },
        ];

        let schemas = admin
            .create_table_schemas(
                tenant_id,
                vec![Table {
                    id: None,
                    table: Some(table_name.to_string()),
                    backend: Some(BackendType::Rdbms),
                    columns: Some(columns.clone()),
                }],
            )
            .await
            .expect("create_table_schemas");
        let table_id = schemas[0].id.expect("table id");

        admin
            .setup_database_connection_for_table(
                tenant_id,
                service.to_string(),
                db_url.clone(),
                Some(table_id),
            )
            .await
            .expect("setup_database_connection should succeed");

        // Create physical table with REAL column (như production)
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" (\
             id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, \
             label VARCHAR(255), \
             price REAL)"
        );
        db.execute_unprepared(&sql).await.unwrap();

        // --- Write a float value ---
        let body = serde_json::json!({"label": "product_a", "price": 19.75});
        let rows = admin
            .write_to_table_by_id(tenant_id, table_id, Some(body), None)
            .await
            .expect("write with Real value should succeed");
        assert_eq!(rows, 1, "Should insert 1 row");

        // --- Write an integer value into Real column ---
        let body2 = serde_json::json!({"label": "product_b", "price": 42});
        let rows2 = admin
            .write_to_table_by_id(tenant_id, table_id, Some(body2), None)
            .await
            .expect("write integer into Real column should succeed");
        assert_eq!(rows2, 1, "Should insert 1 row");

        // --- Read data back ---
        let data = admin
            .read_from_table_by_id(tenant_id, table_id, 0, 10, None)
            .await
            .expect("read should succeed");

        let prices = data.get("price").expect("Should have 'price' column");
        assert_eq!(prices.len(), 2, "Should have 2 rows");

        // Verify float value is read back as a JSON number
        assert_eq!(
            prices[0],
            serde_json::json!(19.75),
            "Float value should be read back as JSON number"
        );
        // Verify integer stored in Real column is read back as float
        assert_eq!(
            prices[1],
            serde_json::json!(42.0),
            "Integer in Real column should be read back as JSON float"
        );

        // --- Test type validation: string value should fail for Real column ---
        let body_invalid = serde_json::json!({"label": "bad", "price": "not_a_number"});
        let result = admin
            .write_to_table_by_id(tenant_id, table_id, Some(body_invalid), None)
            .await;
        assert!(
            result.is_err(),
            "Should reject string value for Real column"
        );

        // Cleanup
        let _ = db
            .execute_unprepared(&format!("DROP TABLE IF EXISTS \"{table_name}\""))
            .await;
        cleanup_tenant_data(db, tenant_id).await;
    }

    #[tokio::test]
    async fn test_issue_verify_rotate_revoke_user_token() {
        setup_env();
        let admin = create_admin().await;
        let db = admin_db(&admin);
        let tenant_id: i64 = 100099;
        let user_id = "unit-test-user".to_string();

        cleanup_tenant_data(db, tenant_id).await;
        setup_tenant(db, tenant_id).await;

        // 1. Issue lần đầu
        let token_v1 = admin
            .issue_user_token(tenant_id, &user_id, None)
            .await
            .unwrap();
        assert!(token_v1.starts_with("abt_"), "token phải có prefix abt_");

        // 2. Verify hợp lệ
        let verified = admin.verify_user_token(tenant_id, &token_v1).await.unwrap();
        assert_eq!(verified, Some(user_id.clone()));

        // 3. Token sai
        let invalid = admin
            .verify_user_token(tenant_id, "abt_deadbeef")
            .await
            .unwrap();
        assert_eq!(invalid, None);

        // 4. Rotate: token cũ chết, token mới sống, reveal khớp
        let token_v2 = admin
            .issue_user_token(tenant_id, &user_id, None)
            .await
            .unwrap();
        assert_ne!(token_v1, token_v2);
        let old = admin.verify_user_token(tenant_id, &token_v1).await.unwrap();
        assert_eq!(old, None, "token cũ phải bị vô hiệu sau rotate");
        let new = admin.verify_user_token(tenant_id, &token_v2).await.unwrap();
        assert_eq!(new, Some(user_id.clone()));
        let revealed = admin.reveal_user_token(tenant_id, &user_id).await.unwrap();
        assert_eq!(revealed, token_v2);

        // 5. List có hint là 4 ký tự cuối
        let users = admin.list_user_tokens(tenant_id).await.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].user_id, user_id);
        let hint = &token_v2[token_v2.len() - 4..];
        assert_eq!(users[0].token_hint.as_deref(), Some(hint));

        // 6. Revoke → verify trả None
        admin.revoke_user_token(tenant_id, &user_id).await.unwrap();
        let revoked = admin.verify_user_token(tenant_id, &token_v2).await.unwrap();
        assert_eq!(revoked, None);

        // 7. Issue lại sau revoke thì token sống lại
        let token_v3 = admin
            .issue_user_token(tenant_id, &user_id, None)
            .await
            .unwrap();
        let revived = admin.verify_user_token(tenant_id, &token_v3).await.unwrap();
        assert_eq!(revived, Some(user_id.clone()));

        cleanup_tenant_data(db, tenant_id).await;
    }

    #[test]
    fn test_sha256_hex_and_constant_time_eq() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
