//! GraphQL endpoint `/graphql` — Tầng 1 (pipeline/stations).
//!
//! Surface 2 nhóm tính năng:
//! 1. Xem pipeline   — `Query.status`
//! 2. Attribute edit — `Query.attributes`, `Mutation.{set,remove}Attribute`
//! 3. Truy vấn timeseries — `Query.queryTimeseries`
//!
//! Mọi thay đổi pipeline đi qua `Mutation.reload(components)` — REPL client
//! tính full component list locally rồi push lên.

use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, InputObject, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::Extension;
use opsense_core::TimeseriesStation;
use opsense_core::Observation;
use opsense_libs::vector::runtime::Component;
use tokio::sync::RwLock;

use super::ReplHeaders;
use crate::api::{AppState, NodeSummary, Status};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(SimpleObject, Clone, Debug)]
pub struct EditResult {
    pub reloaded: bool,
    pub nodes: Vec<NodeSummary>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct SetAttributeResult {
    pub ok: bool,
    /// True when `OPSENSE_ATTR_<NAME>` is also set (env wins on next lookup).
    pub env_override_active: bool,
}

/// `ComponentInput` → `Arc<dyn Component>` qua typetag serde.
/// Nếu `type` không hợp lệ → GraphQL error (validate miễn phí).
#[derive(InputObject, Clone, Debug)]
pub struct ComponentInput {
    #[graphql(name = "type")]
    pub kind: String,
    pub id: String,
    pub config: Option<serde_json::Value>,
    pub inputs: Option<Vec<String>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Lấy `AppState` từ GraphQL context. Inject bởi [`graphql`] handler.
fn state<'a>(ctx: &'a Context<'_>) -> &'a AppState {
    ctx.data::<AppState>().expect("AppState not injected")
}

/// `ComponentInput` → `Arc<dyn Component>`. Build a JSON object with the
/// typetag-required `type` field, merge `config`, and deserialize via typetag.
fn parse_component(input: &ComponentInput) -> async_graphql::Result<Arc<dyn Component>> {
    let mut json = serde_json::Map::new();
    json.insert("type".into(), serde_json::Value::String(input.kind.clone()));
    json.insert("id".into(), serde_json::Value::String(input.id.clone()));
    if let Some(cfg) = &input.config {
        if let Some(cfg_obj) = cfg.as_object() {
            for (k, v) in cfg_obj {
                json.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(inputs) = &input.inputs {
        json.insert(
            "inputs".into(),
            serde_json::to_value(inputs).unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::from_value::<Box<dyn Component>>(serde_json::Value::Object(json))
        .map(Arc::from)
        .map_err(|e| async_graphql::Error::new(format!("component '{}': {}", input.id, e)))
}

/// True when `OPSENSE_ATTR_<NAME>` is set and non-empty.
fn env_attr_override(name: &str) -> bool {
    std::env::var(format!("OPSENSE_ATTR_{}", name.to_uppercase()))
        .ok()
        .filter(|v| !v.is_empty())
        .is_some()
}

// ─────────────────────────────────────────────────────────────────────────────
// Query root
// ─────────────────────────────────────────────────────────────────────────────

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Snapshot toàn bộ pipeline + station registry + config path.
    async fn status(&self, ctx: &Context<'_>) -> Status {
        state(ctx).status().await
    }

    /// Toàn bộ attributes trong memory.
    async fn attributes(&self, ctx: &Context<'_>) -> std::collections::BTreeMap<String, String> {
        state(ctx).attributes().await
    }

    /// Truy vấn 1 time series trong khoảng thời gian.
    async fn query_timeseries(
        &self,
        ctx: &Context<'_>,
        node: String,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
    ) -> async_graphql::Result<Vec<Observation>> {
        let s = state(ctx);
        let station = s
            .context
            .station::<Arc<RwLock<TimeseriesStation>>>(&node)
            .await
            .map_err(|e| async_graphql::Error::new(format!("station '{node}' is not a timeseries: {e}")))?;

        let from = from_ts.unwrap_or(i64::MIN);
        let to = to_ts.unwrap_or(i64::MAX);

        let mut station = station.write().await;
        Ok(station.query_range(from, to).unwrap_or_else(|| {
            tracing::warn!(node = %node, "timeseries cache miss");
            Vec::new()
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutation root
// ─────────────────────────────────────────────────────────────────────────────

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Push full component list — add/update/remove đều qua đây.
    /// REPL client tính new list locally rồi call 1 lần.
    async fn reload(
        &self,
        ctx: &Context<'_>,
        components: Vec<ComponentInput>,
    ) -> async_graphql::Result<EditResult> {
        let s = state(ctx);
        let parsed: Vec<Arc<dyn Component>> =
            components.iter().map(parse_component).collect::<async_graphql::Result<Vec<_>>>()?;

        let runtime = s.runtime.write().await;
        runtime
            .reload(parsed)
            .map_err(|e| async_graphql::Error::new(format!("runtime.reload: {e}")))?;

        drop(runtime);
        let nodes = s.status().await.nodes;
        Ok(EditResult { reloaded: true, nodes })
    }

    async fn set_attribute(
        &self,
        ctx: &Context<'_>,
        name: String,
        value: String,
    ) -> async_graphql::Result<SetAttributeResult> {
        let s = state(ctx);
        s.set_attribute(name.clone(), value).await;
        Ok(SetAttributeResult { ok: true, env_override_active: env_attr_override(&name) })
    }

    async fn remove_attribute(&self, ctx: &Context<'_>, name: String) -> async_graphql::Result<bool> {
        Ok(state(ctx).remove_attribute(&name).await)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GraphQL handler
// ─────────────────────────────────────────────────────────────────────────────

pub async fn graphql(
    State(app_state): State<AppState>,
    ReplHeaders { tenant_id, .. }: ReplHeaders,
    Extension(schema): Extension<Arc<Schema<QueryRoot, MutationRoot, EmptySubscription>>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut req = req.into_inner();
    req = req.data(app_state);
    req = req.data(Into::<i64>::into(tenant_id));
    schema.execute(req).await.into()
}
