use std::sync::Arc;

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use vector_runtime::Component;

#[derive(Debug, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct Context(#[serde(bound = "")] pub Vec<Arc<dyn Component>>);

impl Clone for Context {
    fn clone(&self) -> Self {
        let cloned_vec = self.0.iter().map(|comp| comp.clone_arc()).collect();
        Context(cloned_vec)
    }
}

impl PartialEq for Context {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(a, b)| a.compare(b.as_ref()))
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "sys_streams")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub tenant_id: i64,
    pub context: Context,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
