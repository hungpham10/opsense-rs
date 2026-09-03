use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "sys_oidc")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub jwt_mode: Option<String>,
    pub jwt_secret: Option<i64>,
    pub session_secret: Option<i64>,
    pub oidc_issuer: Option<String>,
    pub oidc_jwks_url: Option<String>,
    pub oidc_client_id: Option<String>,
    pub oidc_client_secret: Option<i64>,
    pub oidc_expected_alg: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
