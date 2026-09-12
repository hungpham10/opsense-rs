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
use opsense_proto::pb::SessionParams;
use tokio::sync::RwLock;

use super::ReplHeaders;
use crate::api::{AppState, KernelSessionEntry, NodeSummary, Status};
use crate::client::grpc::RunnerClient;

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
    if let Some(cfg) = &input.config
        && let Some(cfg_obj) = cfg.as_object() {
            for (k, v) in cfg_obj {
                json.insert(k.clone(), v.clone());
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
// Kernel (Tầng 2) types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(SimpleObject, Clone, Debug)]
pub struct KernelSession {
    /// Runner-assigned session id (= Ed25519 public key, base64).
    pub id: String,
    /// Backend requested at `kernelStart` (`python` / `julia` / `echo`).
    pub backend: String,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct KernelResult {
    /// True when execution completed without error or timeout.
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
    /// The final `result_value` as text, when the kernel produced one.
    pub text: Option<String>,
    /// The final `result_value` as number, when the kernel produced one.
    pub number: Option<f64>,
    /// Kernel-reported error message, if any.
    pub error: Option<String>,
    /// True when the runner timed out the execution.
    pub timed_out: bool,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct KernelHealth {
    pub ok: bool,
    pub kernel_name: String,
}

/// Resolve the runner gRPC endpoint for a backend name.
///
/// Precedence: `OPSENSE_RUNNER_<BACKEND>` (e.g. `OPSENSE_RUNNER_PYTHON`) →
/// `OPSENSE_RUNNER_GRPC` → compose DNS `opsense-runner-<backend>:50051`.
fn runner_endpoint(backend: &str) -> String {
    std::env::var(format!("OPSENSE_RUNNER_{}", backend.to_uppercase()))
        .or_else(|_| std::env::var("OPSENSE_RUNNER_GRPC"))
        .unwrap_or_else(|_| format!("opsense-runner-{backend}:50051"))
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

    /// Danh sách kernel session đang sống trong host (Tầng 2).
    async fn kernel_list_sessions(&self, ctx: &Context<'_>) -> Vec<KernelSession> {
        state(ctx)
            .kernel()
            .ids()
            .await
            .into_iter()
            .map(|(id, backend)| KernelSession { id, backend })
            .collect()
    }

    /// Health của runner (Tầng 2). Cần ít nhất 1 kernel session đang sống.
    async fn kernel_health(&self, ctx: &Context<'_>) -> async_graphql::Result<KernelHealth> {
        let kernel = state(ctx).kernel();
        let mut sessions = kernel.lock().await;
        if sessions.is_empty() {
            return Err(async_graphql::Error::new(
                "no active kernel session — call kernelStart first",
            ));
        }

        for entry in sessions.values_mut() {
            match entry.client.health().await {
                Ok(h) => {
                    return Ok(KernelHealth { ok: h.ok, kernel_name: h.kernel_name });
                }
                Err(e) => tracing::warn!(backend = %entry.backend, "kernel health: {e}"),
            }
        }
        Err(async_graphql::Error::new("no kernel session reachable"))
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

    /// Mở kernel session tới runner của `backend` (`python` / `julia` / `echo`).
    /// Endpoint resolve theo `runner_endpoint` (env override hoặc compose DNS).
    async fn kernel_start(
        &self,
        ctx: &Context<'_>,
        backend: String,
    ) -> async_graphql::Result<KernelSession> {
        let endpoint = runner_endpoint(&backend);
        let client = RunnerClient::connect(
            &endpoint,
            SessionParams {
                session_id: format!("graphql-{backend}"),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| async_graphql::Error::new(format!("kernelStart '{backend}' at {endpoint}: {e}")))?;

        let session = KernelSession { id: client.session_id().to_string(), backend: backend.clone() };
        state(ctx)
            .kernel()
            .insert(session.id.clone(), KernelSessionEntry { client, backend })
            .await;
        Ok(session)
    }

    /// Chạy `code` trong kernel session `id` (multi-line được gửi nguyên xi —
    /// kernel tự xử lý qua `exec(compile)` / `Meta.parseall()`).
    async fn kernel_execute(
        &self,
        ctx: &Context<'_>,
        id: String,
        code: String,
    ) -> async_graphql::Result<KernelResult> {
        let outcome = {
            let mut sessions = state(ctx).kernel().lock().await;
            let entry = sessions
                .get_mut(&id)
                .ok_or_else(|| async_graphql::Error::new(format!("unknown kernel session '{id}'")))?;
            entry.client.execute(&code).await
        }
        .map_err(|e| async_graphql::Error::new(format!("kernelExecute: {e}")))?;

        Ok(KernelResult {
            ok: outcome.ok(),
            stdout: outcome.stdout(),
            stderr: outcome.stderr(),
            text: outcome.text().map(str::to_string),
            number: outcome.number(),
            error: outcome.error.map(|e| e.message),
            timed_out: outcome.timed_out,
        })
    }

    /// Huỷ execution đang chạy trong kernel session `id`.
    async fn kernel_interrupt(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> async_graphql::Result<bool> {
        let mut sessions = state(ctx).kernel().lock().await;
        let entry = sessions
            .get_mut(&id)
            .ok_or_else(|| async_graphql::Error::new(format!("unknown kernel session '{id}'")))?;
        entry
            .client
            .interrupt()
            .await
            .map_err(|e| async_graphql::Error::new(format!("kernelInterrupt: {e}")))?;
        Ok(true)
    }

    /// Đóng kernel session `id` và xoá khỏi registry.
    async fn kernel_close(&self, ctx: &Context<'_>, id: String) -> async_graphql::Result<bool> {
        let entry = state(ctx)
            .kernel()
            .remove(&id)
            .await
            .ok_or_else(|| async_graphql::Error::new(format!("unknown kernel session '{id}'")))?;
        let mut client = entry.client;
        client
            .close()
            .await
            .map_err(|e| async_graphql::Error::new(format!("kernelClose: {e}")))?;
        Ok(true)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema<QueryRoot, MutationRoot, EmptySubscription> {
        Schema::build(QueryRoot, MutationRoot, EmptySubscription).finish()
    }

    /// Phase 2 GraphQL bridge: 6 kernel operations phải có trong SDL
    /// (2 queries + 4 mutations) kèm đúng field của result types.
    #[tokio::test]
    async fn kernel_bridge_schema_surface() {
        let sdl = schema().sdl();
        for op in [
            "kernelListSessions",
            "kernelHealth",
            "kernelStart",
            "kernelExecute",
            "kernelInterrupt",
            "kernelClose",
            "type KernelSession",
            "type KernelResult",
            "type KernelHealth",
        ] {
            assert!(sdl.contains(op), "schema missing `{op}`\n--- SDL ---\n{sdl}");
        }
    }
}
