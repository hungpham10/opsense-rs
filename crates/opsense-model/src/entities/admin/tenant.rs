use serde::{Deserialize, Serialize};

use crate::entities::admin::errors::AdminError;
use crate::entities::admin::Admin;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AuthConfig {
    pub id: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc_issuer: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc_client_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc_client_secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc_jwks_url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc_expected_alg: Option<String>,
}

/// Tenant resolution + OIDC config lookup.
#[async_trait::async_trait]
pub trait Tenant: Send + Sync {
    /// Tra `sys_tenant.host` → `tenant_id` (BIGINT).
    async fn get_tenant_id(&self, host: &str) -> Result<i64, AdminError>;

    /// Tra full OIDC/JWT config cho 1 host + oidc_name (multi-IdP per tenant).
    /// Decrypt jwt_secret, session_secret, oidc_client_secret từ `sys_token_map`.
    async fn get_tenant_auth_config(
        &self,
        host: &str,
        oidc_name: &str,
    ) -> Result<AuthConfig, AdminError>;
}

#[async_trait::async_trait]
impl Tenant for Admin {
    async fn get_tenant_id(&self, host: &str) -> Result<i64, AdminError> {
        use sqlx::Row;

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

    async fn get_tenant_auth_config(
        &self,
        host: &str,
        oidc_name: &str,
    ) -> Result<AuthConfig, AdminError> {
        use sqlx::Row;

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
}
