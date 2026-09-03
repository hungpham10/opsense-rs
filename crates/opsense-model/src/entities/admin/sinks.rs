use std::sync::Arc;

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use vector_runtime::Component;

#[derive(Debug, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct Handler(#[serde(bound = "")] pub Arc<dyn Component>);

impl Clone for Handler {
    fn clone(&self) -> Self {
        Handler(self.0.clone_arc())
    }
}

impl PartialEq for Handler {
    fn eq(&self, other: &Self) -> bool {
        self.0.compare(other.0.as_ref())
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "sys_sinks")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub tenant_id: i64,
    pub handler: Handler,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
