//! GraphQL endpoint `/graphql` — Tầng 1 (pipeline/stations).
//!
//! Surface 3 nhóm tính năng:
//! 1. Xem pipeline   — `Query.status`, `Query.components`
//! 2. Attribute edit — `Query.attributes`, `Mutation.{set,remove}Attribute`
//! 3. Truy vấn timeseries — `Query.queryTimeseries`
//!
//! Mọi thay đổi pipeline đi qua `Mutation.reload(components)`. Nhưng reload nhận
//! **danh sách đầy đủ**, nên client phải đọc cấu hình hiện tại trước
//! (`Query.components`) — đọc rồi sửa thì không đoán. `Mutation.patchComponent`
//! là cách sửa một phần (thêm ở G2).

use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, InputObject, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::Extension;
use axum::extract::State;
use opsense_core::Observation;
use opsense_core::TimeseriesStation;
use opsense_mlib::vector::runtime::Component;
use tokio::sync::RwLock;

use super::ReplHeaders;
use crate::api::{AppState, Node, Status};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(SimpleObject, Clone, Debug)]
pub struct EditResult {
    pub reloaded: bool,
    pub nodes: Vec<Node>,
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

/// Cấu hình **đang chạy** của một component (`Query.components`).
///
/// `config` là JSON typetag của component — cùng shape với `ComponentInput` nên
/// đọc xong có thể patch rồi `reload`, hoặc patch từng path (xem
/// `Mutation.patchComponent`) mà không phải dựng lại cả pipeline.
#[derive(SimpleObject, Clone, Debug)]
pub struct ComponentConfig {
    pub id: String,

    #[graphql(name = "type")]
    pub kind: String,

    pub inputs: Vec<String>,

    /// Toàn bộ field của component dạng JSON (`script_path`, `params`, …).
    pub config: serde_json::Value,
}

/// Bọc JSON thô của `Runtime::components()` thành `ComponentConfig`.
fn component_config(value: serde_json::Value) -> Option<ComponentConfig> {
    let obj = value.as_object()?;
    let id = obj.get("id")?.as_str()?.to_string();
    // `type` do typetag sinh khi serialize; `id` do runtime nhét thêm.
    let kind = obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let inputs = obj
        .get("inputs")
        .and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(ComponentConfig {
        id,
        kind,
        inputs,
        config: value,
    })
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
        && let Some(cfg_obj) = cfg.as_object()
    {
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

/// JSON trả về từ `Runtime::components()` (typetag phẳng + `id`) → component.
///
/// Validate miễn phí: type lạ / field thiếu / sai kiểu đều lỗi ở đây, **trước**
/// khi chạm vào runtime.
fn parse_component_json(value: serde_json::Value) -> async_graphql::Result<Arc<dyn Component>> {
    let id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<no id>")
        .to_string();
    serde_json::from_value::<Box<dyn Component>>(value)
        .map(Arc::from)
        .map_err(|e| async_graphql::Error::new(format!("component '{id}': {e}")))
}

/// Ghi `new_value` vào `path` (JSON pointer, vd `params.sl_pct`).
///
/// - Đường dẫn phải bắt đầu bằng `/` và có ít nhất 1 segment.
/// - Segment **cuối** được tạo mới nếu chưa có (thêm knob mới vào `params` là
///   việc hợp lệ); nhưng không tạo được cha — `params.a.b` khi `params.a` chưa có
///   thì lỗi, thay vì âm thầm tạo object rỗng.
fn patch_json_pointer(
    mut root: serde_json::Value,
    path: &str,
    new_value: serde_json::Value,
) -> async_graphql::Result<serde_json::Value> {
    if !path.starts_with('/') || path == "/" {
        return Err(async_graphql::Error::new(format!(
            "path phải là JSON pointer bắt đầu bằng '/', vd \"params.sl_pct\" (nhận `{path}`)"
        )));
    }
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect();
    let (last, parents) = segments.split_last().expect("path != \"/\" đã kiểm tra");

    let mut cursor = &mut root;
    for seg in parents {
        let next = cursor
            .as_object_mut()
            .and_then(|o| o.get_mut(seg))
            .ok_or_else(|| {
                async_graphql::Error::new(format!("path `{path}`: không có `{seg}` để đi tiếp"))
            })?;
        cursor = next;
    }
    let obj = cursor.as_object_mut().ok_or_else(|| {
        async_graphql::Error::new(format!("path `{path}`: cha không phải object"))
    })?;
    if let Some(existing) = obj.get_mut(last) {
        *existing = new_value;
    } else {
        obj.insert(last.clone(), new_value);
    }
    Ok(root)
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

    /// Cấu hình đang chạy của component. Bỏ `id` → tất cả.
    ///
    /// Đường đọc chuẩn trước khi sửa: không có nó thì client buộc phải nhớ cấu
    /// hình cũ rồi gửi lại cả danh sách qua `Mutation.reload` — một node sót là
    /// mất node.
    async fn components(
        &self,
        ctx: &Context<'_>,
        id: Option<String>,
    ) -> async_graphql::Result<Vec<ComponentConfig>> {
        let s = state(ctx);
        Ok(s
            .components(id.as_deref())
            .await
            .into_iter()
            .filter_map(component_config)
            .collect())
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
            .map_err(|e| {
                async_graphql::Error::new(format!("station '{node}' is not a timeseries: {e}"))
            })?;

        let from = from_ts.unwrap_or(i64::MIN);
        let to = to_ts.unwrap_or(i64::MAX);

        // `query_range` takes `&self` (the LRU cache is interior-mutable), so a
        // read lock is enough — no writer contention against collectors.
        let station = station.read().await;
        Ok(station.query_range(from, to).await.unwrap_or_else(|| {
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
        let parsed: Vec<Arc<dyn Component>> = components
            .iter()
            .map(parse_component)
            .collect::<async_graphql::Result<Vec<_>>>()?;

        let runtime = s.runtime.write().await;
        runtime
            .reload(parsed)
            .map_err(|e| async_graphql::Error::new(format!("runtime.reload: {e}")))?;

        drop(runtime);
        let nodes = s.status().await.nodes;
        Ok(EditResult {
            reloaded: true,
            nodes,
        })
    }

    /// Sửa **một** thành phần của **một** node, không cần gửi lại cả pipeline.
    ///
    /// `path` là JSON pointer vào config đang chạy (vd `params.sl_pct`,
    /// `script_path`, `params.grid_min_trades`). Server đọc cấu hình hiện tại
    /// (nên không cần client tự dựng lại), patch, deserialize **toàn bộ** danh
    /// sách qua typetag, rồi mới reload — hỏng ở bước deserialize thì runtime
    /// giữ nguyên, không để lại pipeline nửa vời.
    ///
    /// `value` là JSON literal (`0.02`, `"rhai"`, `true`, `[1,2]`). Thay cả
    /// pipeline thì dùng `reload`.
    async fn patch_component(
        &self,
        ctx: &Context<'_>,
        id: String,
        path: String,
        value: String,
    ) -> async_graphql::Result<EditResult> {
        let s = state(ctx);
        let current = s.components(Some(&id)).await;
        let Some(target) = current.into_iter().next() else {
            return Err(async_graphql::Error::new(format!(
                "không có node '{id}' (xem Query.status để xem id hiện có)"
            )));
        };
        let new_value: serde_json::Value = serde_json::from_str(&value).map_err(|e| {
            async_graphql::Error::new(format!("`{value}` không phải JSON hợp lệ: {e}"))
        })?;
        let patched = patch_json_pointer(target.clone(), &path, new_value)?;
        if patched == target {
            return Ok(EditResult {
                reloaded: false,
                nodes: s.status().await.nodes,
            });
        }

        // Validate TRƯỚC khi chạm runtime: ghép node vừa patch với các node còn
        // lại (lấy lại toàn bộ để không mất node nào) rồi deserialize hết.
        let mut all = s.components(None).await;
        let slot = all
            .iter_mut()
            .find(|c| c.get("id").and_then(serde_json::Value::as_str) == Some(id.as_str()))
            .ok_or_else(|| async_graphql::Error::new(format!("không có node '{id}'")))?;
        *slot = patched;
        let parsed = all
            .iter()
            .map(|c| parse_component_json(c.clone()))
            .collect::<async_graphql::Result<Vec<_>>>()?;

        let runtime = s.runtime.write().await;
        runtime
            .reload(parsed)
            .map_err(|e| async_graphql::Error::new(format!("runtime.reload: {e}")))?;
        drop(runtime);

        tracing::info!(node = %id, path = %path, "config patched qua GraphQL");
        Ok(EditResult {
            reloaded: true,
            nodes: s.status().await.nodes,
        })
    }

    async fn set_attribute(        &self,
        ctx: &Context<'_>,
        name: String,
        value: String,
    ) -> async_graphql::Result<SetAttributeResult> {
        let s = state(ctx);
        s.set_attribute(name.clone(), value).await;
        Ok(SetAttributeResult {
            ok: true,
            env_override_active: env_attr_override(&name),
        })
    }

    async fn remove_attribute(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> async_graphql::Result<bool> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema<QueryRoot, MutationRoot, EmptySubscription> {
        Schema::build(QueryRoot, MutationRoot, EmptySubscription).finish()
    }

    /// Tầng 1 GraphQL surface: pipeline status, **component config**, attributes
    /// and timeseries queries must be present in the SDL.
    #[tokio::test]
    async fn tier1_schema_surface() {
        let sdl = schema().sdl();
        for op in [
            "status",
            "components",
            "attributes",
            "queryTimeseries",
            "reload",
            "patchComponent",
            "setAttribute",
            "removeAttribute",
        ] {
            assert!(
                sdl.contains(op),
                "schema missing `{op}`\n--- SDL ---\n{sdl}"
            );
        }
    }

    /// `Runtime::components()` trả JSON typetag + `id` nhét thêm; map sang
    /// `ComponentConfig` phải giữ nguyên `config` (đó là thứ client patch).
    #[test]
    fn component_config_maps_typed_json() {
        let raw = serde_json::json!({
            "type": "rhai_transform",
            "id": "grid",
            "inputs": ["clock", "history"],
            "script_path": "strategies/binance/grid.rhai",
            "params": { "strategy": "rhai", "mode": "analysis" },
        });
        let cfg = component_config(raw.clone()).expect("map được");
        assert_eq!(cfg.id, "grid");
        assert_eq!(cfg.kind, "rhai_transform");
        assert_eq!(cfg.inputs, vec!["clock", "history"]);
        assert_eq!(cfg.config, raw, "config phải giữ nguyên JSON gốc");
        // `params` là thứ MCP/CLI đọc để sửa tiếp.
        assert_eq!(cfg.config["params"]["strategy"], "rhai");
    }

    /// JSON thiếu `id` (component lỗi) → bỏ qua chứ không làm hỏng cả query.
    #[test]
    fn component_config_skips_malformed_entries() {
        assert!(component_config(serde_json::json!({})).is_none());
        assert!(component_config(serde_json::json!({ "id": 7 })).is_none());
        // Không có `inputs` → rỗng, không panic.
        let cfg = component_config(serde_json::json!({ "id": "x", "type": "clock" }))
            .expect("map được");
        assert!(cfg.inputs.is_empty());
    }

    /// `patchComponent` chỉ được đổi **đúng một** chỗ, và phải tạo được knob mới
    /// (thêm key vào `params` là việc hợp lệ) nhưng không tự chế ra object cha.
    #[test]
    fn patch_json_pointer_touches_one_place() {
        let base = || {
            serde_json::json!({
                "type": "rhai_transform",
                "id": "grid",
                "script_path": "strategies/binance/grid.rhai",
                "params": { "strategy": "rhai", "sl_pct": 0.008 },
            })
        };

        // 1. Sửa giá trị có sẵn, phần còn lại giữ nguyên.
        let out = patch_json_pointer(base(), "/params/sl_pct", serde_json::json!(0.02)).unwrap();
        assert_eq!(out["params"]["sl_pct"], serde_json::json!(0.02));
        assert_eq!(out["params"]["strategy"], "rhai");
        assert_eq!(out["script_path"], "strategies/binance/grid.rhai");

        // 2. Thêm knob mới vào `params`, các key cũ giữ nguyên.
        let out = patch_json_pointer(
            base(),
            "/params/grid_min_trades",
            serde_json::json!(5),
        )
        .unwrap();
        assert_eq!(out["params"]["grid_min_trades"], serde_json::json!(5));
        assert_eq!(out["params"]["strategy"], "rhai");
        assert_eq!(out["params"]["sl_pct"], serde_json::json!(0.008));

        // 3. Path sai cú pháp / không có cha → lỗi, không im lặng bỏ qua.
        let err = patch_json_pointer(base(), "params.sl_pct", serde_json::json!(0.02))
            .unwrap_err();
        let err = err.message.clone();
        assert!(err.contains("JSON pointer"), "{err}");
        let err = patch_json_pointer(base(), "/params/a/b", serde_json::json!(1))
            .unwrap_err();
        let err = err.message.clone();
        assert!(err.contains("không có"), "{err}");
        // Dấu chấm là ký tự thường trong JSON pointer: `/params.a.b` là MỘT key
        // tên `params.a.b`, không phải hai tầng — giữ đúng chuẩn RFC 6901.
        let out = patch_json_pointer(base(), "/params.a.b", serde_json::json!(1)).unwrap();
        assert_eq!(out["params.a.b"], serde_json::json!(1));
    }

    /// Giá trị sai kiểu phải chết ở deserialize (typetag), **trước** khi runtime
    /// bị đụng tới — đây là chốt chặn "patch làm hỏng cả pipeline".
    #[test]
    fn parse_component_json_rejects_broken_config() {
        // Component không tồn tại trong inventory.
        let err = parse_component_json(serde_json::json!({
            "type": "khong_ton_tai", "id": "x"
        }))
        .expect_err("type lạ phải lỗi");
        assert!(
            err.message.contains("khong_ton_tai"),
            "{}",
            err.message
        );

        // Sai kiểu field: `interval_secs` của clock phải là số.
        let err = parse_component_json(serde_json::json!({
            "type": "clock", "id": "c", "interval_secs": "khong-phai-so"
        }))
        .expect_err("sai kiểu phải lỗi");
        assert!(err.message.contains('c'), "{}", err.message);

        // Hợp lệ thì deserialize được.
        parse_component_json(serde_json::json!({
            "type": "clock", "id": "c", "interval_secs": 10
        }))
        .expect("clock hợp lệ phải parse được");
    }
}
