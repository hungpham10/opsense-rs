use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use super::ColumnDescription;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct Schema(pub SchemaDesciption);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaDesciption {
    pub columns: Vec<ColumnDescription>,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "sys_table_map")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub tenant_id: i64,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
    pub backend: i32,
    pub name: String,
    pub schema: Schema,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
