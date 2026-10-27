//! API module registry — loads and manages HTTP API modules.

use std::collections::HashMap;
use std::sync::Arc;

#[cfg(not(feature = "dynamic-modules"))]
use std::ffi::c_void;

use axum::{
    Router,
    extract::State,
    http::{Request, Response},
    response::sse::{Event, KeepAlive, Sse},
    routing::{MethodFilter, MethodRouter, any, get},
};
use base64::Engine as _;
use serde_json::{Map, Value};
use sqlx::{Any, AnyPool, Column, Decode, Row, TypeInfo, ValueRef};
use tokio::sync::broadcast;

use opsense_ffi::cache::BoxFuture;
use opsense_ffi::db::{DbBackend, DbError};
use opsense_ffi::{
    HttpMethod, HttpRequest, HttpResponse, Manifest, OpsenseModule, TenantServices, Transport,
};
use opsense_model::resolver::DbKind;

#[cfg(feature = "dynamic-modules")]
use opsense_ffi::ModuleError;

#[cfg(not(feature = "dynamic-modules"))]
#[allow(unused_imports)]
use opsense_ffi::ModuleError;

#[cfg(feature = "dynamic-modules")]
use opsense_ffi::{HostServices, ModuleV1};

use crate::api::AppState;

#[cfg(feature = "dynamic-modules")]
use {opsense_core::Config, opsense_ffi::ffi::ModuleInitFn, std::path::Path};

/// Registry of loaded API modules.
pub struct ApiModuleRegistry {
    modules: HashMap<String, Arc<dyn OpsenseModule>>,

    #[cfg(feature = "dynamic-modules")]
    libraries: Vec<libloading::Library>, // Keep libraries alive
}

impl ApiModuleRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),

            #[cfg(feature = "dynamic-modules")]
            libraries: Vec::new(),
        }
    }

    /// Validate routes in a manifest.
    ///
    /// Checks for:
    /// - Duplicate `(method, path)` pairs (would cause axum panic at boot)
    /// - SSE transport with non-GET method (SSE only works with GET)
    /// - Duplicate shape (e.g. `/x/{a}` + `/x/{b}`) would cause axum panic at boot
    fn validate_routes(manifest: &Manifest) -> Result<(), ModuleError> {
        use std::collections::HashSet;

        let mut seen_exact = HashSet::new();
        let mut seen_shape = HashSet::new();
        for route in &manifest.routes {
            let exact_key = (route.method, route.path.as_str());
            if !seen_exact.insert(exact_key) {
                return Err(ModuleError::InitFailed(format!(
                    "duplicate (method, path) in manifest {}: {:?} {}",
                    manifest.id, route.method, route.path
                )));
            }

            // Check for duplicate shape — same method, same shape but different param names.
            // `/x/{a}` and `/x/{b}` are different keys but same shape → axum panic at boot.
            let shape = Self::path_shape(&route.path);
            let shape_key = (route.method, shape.clone());
            if !seen_shape.insert(shape_key) {
                return Err(ModuleError::InitFailed(format!(
                    "duplicate (method, path shape) in manifest {}: {:?} {} (shape: {})",
                    manifest.id, route.method, route.path, shape
                )));
            }

            if route.transport == Transport::Sse && route.method != HttpMethod::Get {
                return Err(ModuleError::InitFailed(format!(
                    "route {} in manifest {}: SSE transport requires GET method, got {:?}",
                    route.id, manifest.id, route.method
                )));
            }
        }
        Ok(())
    }

    /// Normalize path to shape by replacing `{param}` with `{}`.
    /// Only used for duplicate detection, NOT for mounting.
    fn path_shape(path: &str) -> String {
        let mut out = String::with_capacity(path.len());
        let mut in_brace = false;
        for ch in path.chars() {
            if ch == '{' {
                in_brace = true;
                out.push('{');
                out.push('}');
            } else if ch == '}' {
                in_brace = false;
            } else if !in_brace {
                out.push(ch);
            }
        }
        out
    }

    /// Register a built-in module.
    pub fn register_builtin(
        &mut self,
        module: Arc<dyn OpsenseModule>,
    ) -> Result<&mut Self, ModuleError> {
        let manifest = module.manifest();
        Self::validate_routes(&manifest)?;

        let id = manifest.id.clone();
        self.modules.insert(id, module);
        Ok(self)
    }

    /// Load dynamic modules khai báo trong `[[modules]]` (xem [`Config::modules`]).
    ///
    /// `config_dir` là thư mục chứa file config — `path` tương đối resolve theo nó.
    #[cfg(feature = "dynamic-modules")]
    pub fn load_dynamic(
        &mut self,
        cfg: &Config,
        config_dir: &Path,
        host_services: Arc<dyn HostServices>,
    ) -> Result<(), ModuleError> {
        use libloading::Library;

        for mcfg in cfg.enabled_modules() {
            if mcfg.builtin {
                tracing::debug!(module = %mcfg.id, "khai builtin=true → bỏ qua; register_builtin ở serve.rs");
                continue;
            }

            let path = cfg.module_path(mcfg, config_dir).ok_or_else(|| {
                ModuleError::InitFailed(format!("no path for dynamic module {}", mcfg.id))
            })?;

            tracing::info!(module = %mcfg.id, path = %path.display(), "loading dynamic module");
            let lib = unsafe { Library::new(&path) }.map_err(|e| {
                ModuleError::InitFailed(format!("dlopen {}: {}", path.display(), e))
            })?;

            // Symbol name: opsense_module_<id>_init (with - replaced by _)
            let sym_name = format!("opsense_module_{}_init", mcfg.id.replace('-', "_"));
            let init_fn: libloading::Symbol<ModuleInitFn> = unsafe { lib.get(sym_name.as_bytes()) }
                .map_err(|e| ModuleError::InitFailed(format!("symbol {}: {}", sym_name, e)))?;

            // Build HostV1 with context
            let host_v1 = build_host_v1(&host_services);

            // Initialize module
            let module_v1 = unsafe { init_fn(&host_v1) };
            if module_v1.is_null() {
                return Err(ModuleError::InitFailed(format!(
                    "init returned null for {}",
                    mcfg.id
                )));
            }

            // Verify ABI version
            unsafe {
                if (*module_v1).abi_version != opsense_ffi::ABI_VERSION {
                    return Err(ModuleError::AbiMismatch {
                        host: opsense_ffi::ABI_VERSION,
                        plugin: (*module_v1).abi_version,
                    });
                }
            }

            // Create wrapper that implements OpsenseModule
            let module = DynamicModuleWrapper {
                vtable: unsafe { Box::from_raw(module_v1) },
            };

            // Validate routes in the loaded module's manifest
            let manifest = module.manifest();
            Self::validate_routes(&manifest)?;

            let key = mcfg.id.clone();
            if manifest.id != key {
                tracing::warn!(module = %key, manifest_id = %manifest.id,
                    "manifest().id khác mcfg.id — registry key theo modules.toml");
            }
            self.modules.insert(key, Arc::new(module));
            self.libraries.push(lib);
        }

        Ok(())
    }

    /// Get all registered modules.
    pub fn modules(&self) -> impl Iterator<Item = &Arc<dyn OpsenseModule>> {
        self.modules.values()
    }

    /// Get a module by ID.
    pub fn get(&self, id: &str) -> Option<&Arc<dyn OpsenseModule>> {
        self.modules.get(id)
    }

    /// Build the combined axum router for all enabled modules.
    pub fn into_router(self, state: AppState) -> Router<AppState> {
        // `self.libraries` phải sống bằng vòng đời process: drop `Library` =
        // `dlclose` → code section của module bị unmap, request đầu tiên nhảy
        // vào vtable cũ ⇒ SIGSEGV. `into_router` consume `self` nên nếu không
        // forget, Vec bị drop ở cuối hàm.
        #[cfg(feature = "dynamic-modules")]
        std::mem::forget(self.libraries);

        let mut router = Router::new();

        for module in self.modules.values() {
            let manifest = module.manifest();
            let prefix = manifest.prefix.trim_end_matches('/').to_string();
            let module = module.clone();
            let state_for_handler = state.clone();
            let state_for_router = state.clone();

            // Mỗi route mount riêng theo `transport` — REST dùng đúng method,
            // SSE dùng `get()` vì SSE chỉ hợp với GET (client mở kết nối dài).
            let module_router = Router::new();
            let module_router = manifest.routes.iter().fold(module_router, |r, route| {
                let path = route.path.clone();
                let route_id = route.id.clone();
                match route.transport {
                    Transport::Rest => {
                        let method = MethodFilter::try_from(route.method.to_axum())
                            .expect("HttpMethod should convert to MethodFilter");
                        r.route(
                            &path,
                            MethodRouter::new().on(
                                method,
                                any({
                                    let module = module.clone();
                                    let state = state_for_handler.clone();
                                    let route_id = route_id.clone();
                                    move |req| {
                                        module_route_handler(
                                            State(state),
                                            req,
                                            module.clone(),
                                            route_id,
                                        )
                                    }
                                }),
                            ),
                        )
                    }
                    Transport::Sse => r.route(
                        &path,
                        get({
                            let module = module.clone();
                            let state = state_for_handler.clone();
                            let source_node = route.source_node.clone();
                            move |req| sse_handler(State(state), req, module.clone(), source_node)
                        }),
                    ),
                }
            });

            router = router.nest(&prefix, module_router.with_state(state_for_router));
        }

        router
    }
}

impl Default for ApiModuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Tenant id từ header `X-Tenant-Id`; thiếu/hỏng → `0` (chỉ warn lần đầu).
fn header_tenant_id(headers: &axum::http::HeaderMap) -> i64 {
    static MISSING_TENANT_WARNED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

    headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| {
            if MISSING_TENANT_WARNED.set(()).is_ok() {
                tracing::warn!("thiếu/parse lỗi X-Tenant-Id → tenant_id=0 (chỉ log lần đầu)");
            }
            0
        })
}

/// Dựng `TenantServices` per-request: cache + DB của tenant lấy từ resolver.
fn tenant_services(state: &AppState, tenant_id: i64) -> TenantServices {
    let cache = state
        .connector
        .cache(tenant_id)
        .map(opsense_mlib::cache::RedisBackend::new);

    let db = state
        .connector
        .database(tenant_id)
        .map(|pool| AnyDbBackend::arc(pool.clone(), state.connector.database_kind(tenant_id)));

    TenantServices {
        tenant_id,
        cache,
        db,
    }
}

// ───────────────────────── DB transport cho module ─────────────────────────
//
// Impl [`DbBackend`] trên `sqlx::AnyPool`. Module gửi SQL text (`$1..$n`) +
// params JSON; ở đây bind thành prepared statement của đúng dialect, chạy, rồi
// đọc row về JSON.
//
// **Vì sao dùng `try_get_raw` thay `try_get`:** `AnyRow::try_get` kiểm tra
// `T::compatible(&type_info)` trước khi decode, và `f64` không "compatible" với
// `REAL` — trong khi ta biết chắc mình muốn gì. `try_get_raw` + `Decode::decode`
// bỏ qua bước kiểm tra đó.
//
// **Giới hạn `sqlx::Any`:** chỉ 9 kiểu được map (xem `opsense_ffi::db`). Cột
// kiểu `TIMESTAMPTZ`/`NUMERIC`/`JSON`/`UUID` (Postgres) hay `BOOL` (SQLite) làm
// hỏng cả row ngay tại `AnyRow::map_from`, trước cả khi tới đây.

/// `DbBackend` dùng chung một `AnyPool` của tenant.
pub struct AnyDbBackend {
    pool: AnyPool,
    kind: DbKind,
    /// Runtime của **host**. Module `.so` link một bản `tokio` riêng, nên khi
    /// `drive()` trong opsense-ffi block_on future của module, thread-local
    /// runtime context là của bản tokio trong .so — `sqlx::rt::spawn` (link
    /// trong host) không thấy context và panic "requires a Tokio context".
    /// Vì vậy mọi query phải được spawn sang runtime host đã capture ở đây.
    rt: tokio::runtime::Handle,
}

impl AnyDbBackend {
    pub fn new(pool: AnyPool, kind: DbKind) -> Self {
        Self {
            pool,
            kind,
            rt: tokio::runtime::Handle::current(),
        }
    }

    /// Arc cho `TenantServices`. Giữ luôn `AnyDbBackend` để host có thể downcast.
    pub fn arc(pool: AnyPool, kind: DbKind) -> Arc<dyn DbBackend> {
        Arc::new(Self::new(pool, kind))
    }
}

/// JSON array → BLOB, chỉ chấp nhận byte 0..=255.
fn bytes_from_array(values: &[Value]) -> Result<Vec<u8>, DbError> {
    values
        .iter()
        .map(|v| {
            v.as_u64()
                .filter(|n| *n <= u8::MAX as u64)
                .map(|n| n as u8)
                .ok_or_else(|| DbError::Query(format!("BLOB chỉ nhận mảng byte 0..=255, gặp {v}")))
        })
        .collect()
}

/// Một row → JSON object (key = tên cột). BLOB encode base64.
fn row_to_json(row: &sqlx::any::AnyRow) -> Result<Value, DbError> {
    let mut map = Map::new();

    for col in row.columns() {
        let raw = row
            .try_get_raw(col.ordinal())
            .map_err(|e| DbError::Query(e.to_string()))?;

        let value = if raw.is_null() {
            Value::Null
        } else {
            // `type_info().name()` đã được host chuẩn hoá về 9 tên canonical.
            match raw.type_info().name() {
                "BOOLEAN" => Value::Bool(
                    <bool as Decode<Any>>::decode(raw)
                        .map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "SMALLINT" => Value::from(
                    <i16 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "INTEGER" => Value::from(
                    <i32 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "BIGINT" => Value::from(
                    <i64 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "REAL" => Value::from(
                    <f32 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "DOUBLE" => Value::from(
                    <f64 as Decode<Any>>::decode(raw).map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "TEXT" => Value::String(
                    <String as Decode<Any>>::decode(raw)
                        .map_err(|e| DbError::Query(e.to_string()))?,
                ),
                "BLOB" => {
                    let bytes = <Vec<u8> as Decode<Any>>::decode(raw)
                        .map_err(|e| DbError::Query(e.to_string()))?;
                    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
                }
                other => {
                    return Err(DbError::Query(format!(
                        "cột `{}` có kiểu `{other}` không hỗ trợ",
                        col.name()
                    )));
                }
            }
        };

        map.insert(col.name().to_string(), value);
    }

    Ok(Value::Object(map))
}

impl DbBackend for AnyDbBackend {
    fn query(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<Vec<Value>, DbError>> {
        let pool = self.pool.clone();
        let kind = self.kind;
        let sql = sql.to_string();

        let rt = self.rt.clone();
        Box::pin(async move {
            rt.spawn(async move {
                let (sql, params) = match kind {
                    DbKind::MySql => opsense_ffi::db::rewrite_placeholders_mysql(&sql, &params)?,
                    _ => (sql, params),
                };

                let mut q = sqlx::query::<Any>(&sql);
                for p in &params {
                    q = bind_value(q, p)?;
                }

                let rows = q
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| DbError::Backend(e.to_string()))?;

                rows.iter().map(row_to_json).collect()
            })
            .await
            .map_err(|e| DbError::Backend(format!("db task join: {e}")))?
        })
    }

    fn execute(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<u64, DbError>> {
        let pool = self.pool.clone();
        let kind = self.kind;
        let sql = sql.to_string();

        let rt = self.rt.clone();
        Box::pin(async move {
            rt.spawn(async move {
                let (sql, params) = match kind {
                    DbKind::MySql => opsense_ffi::db::rewrite_placeholders_mysql(&sql, &params)?,
                    _ => (sql, params),
                };

                let mut q = sqlx::query::<Any>(&sql);
                for p in &params {
                    q = bind_value(q, p)?;
                }

                let result = q
                    .execute(&pool)
                    .await
                    .map_err(|e| DbError::Backend(e.to_string()))?;

                Ok(result.rows_affected())
            })
            .await
            .map_err(|e| DbError::Backend(format!("db task join: {e}")))?
        })
    }
}

/// Bind một param JSON vào query, giữ nguyên kiểu `Query` để reassign.
fn bind_value<'q>(
    q: sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>>,
    value: &Value,
) -> Result<sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>>, DbError> {
    Ok(match value {
        // `Option<T>::None` mang theo kind của `T`; chọn TEXT vì nó là kiểu
        // "trung tính" nhất. NULL cho cột số nên viết `CAST($n AS BIGINT)`.
        Value::Null => q.bind(Option::<String>::None),
        Value::Bool(b) => q.bind(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                return Err(DbError::Query(format!("số không hợp lệ: {n}")));
            }
        }
        Value::String(s) => q.bind(s.clone()),
        Value::Array(a) => q.bind(bytes_from_array(a)?),
        // Object → TEXT (JSON đã serialize). Cột JSON của Postgres không đọc
        // lại được qua `Any`, nên chỉ dùng khi cột là TEXT.
        Value::Object(_) => q.bind(value.to_string()),
    })
}

/// converts response back.
async fn module_route_handler(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    module: Arc<dyn OpsenseModule>,
    route_id: String,
) -> impl axum::response::IntoResponse {
    let manifest = module.manifest();

    let tenant_id = header_tenant_id(req.headers());

    let services = tenant_services(&state, tenant_id);
    let method = HttpMethod::from_axum(req.method());
    let path = req
        .uri()
        .path()
        .trim_start_matches(&manifest.prefix)
        .to_string();
    let path = if path.is_empty() {
        "/".to_string()
    } else {
        path
    };

    let query: Vec<_> = req
        .uri()
        .query()
        .map(|q| form_urlencoded::parse(q.as_bytes()).into_owned().collect())
        .unwrap_or_default();

    let headers: Vec<_> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_lowercase(),
                v.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();

    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default()
        .to_vec();

    let http_req = HttpRequest {
        route_id,
        method,
        path,
        query,
        headers,
        body,
    };

    // Module handles its own route matching via manifest.routes
    match module.route(http_req, &services).await {
        Ok(resp) => {
            let mut response = Response::builder().status(resp.status);
            for (k, v) in &resp.headers {
                response = response.header(k, v);
            }
            response.body(axum::body::Body::from(resp.body)).unwrap()
        }
        Err(e) => {
            let resp = HttpResponse::from(e);
            let mut response = Response::builder().status(resp.status);
            for (k, v) in &resp.headers {
                response = response.header(k, v);
            }
            response.body(axum::body::Body::from(resp.body)).unwrap()
        }
    }
}

/// SSE handler — phát dữ liệu từ node `output` trong pipeline.
///
/// Host chạy vòng `recv()` trên broadcast channel của `source_node`
/// (`Runtime::broadcast`, `opsense-mlib/src/vector/runtime/engine.rs:708`).
/// Đây là event-driven: 0 CPU khi rảnh, không vòng poll.
///
/// Mỗi message nhận được được gửi qua `stream_push` của module — module quyết
/// định phát gì (trả `""` = bỏ qua). Đây là chỗ "xào trộn dữ liệu" sẽ nằm.
async fn sse_handler(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    module: Arc<dyn OpsenseModule>,
    source_node: Option<String>,
) -> Sse<impl futures::Stream<Item = Result<Event, axum::Error>>> {
    // Mọi nhánh lỗi đều quy về `None` + stream rỗng **cùng một kiểu**, vì
    // `impl Stream` trong return type chỉ nhận đúng một concrete type — trả
    // `stream::empty()` sớm ở nhánh nào đó sẽ là E0308.
    let rx = match source_node {
        Some(node) => match state.runtime.read().await.broadcast(node.clone()) {
            Ok(rx) => Some(rx),
            Err(e) => {
                tracing::warn!(error = %e, node = %node,
                    "không mở được broadcast channel — node phải là `type = \"output\"` và đã chạy");
                None
            }
        },
        None => {
            tracing::warn!("route SSE khai `transport = \"sse\"` nhưng thiếu `source_node`");
            None
        }
    };

    // Mở luồng qua FFI — module trả `stream_id` để định danh.
    let services = tenant_services(&state, header_tenant_id(req.headers()));
    let stream_id = match module
        .stream(
            HttpRequest {
                route_id: String::new(),
                method: HttpMethod::Get,
                path: req.uri().path().to_string(),
                query: req
                    .uri()
                    .query()
                    .map(|q| form_urlencoded::parse(q.as_bytes()).into_owned().collect())
                    .unwrap_or_default(),
                headers: req
                    .headers()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_lowercase(),
                            v.to_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect(),
                body: vec![],
            },
            &services,
        )
        .await
    {
        Ok(h) => Some(h.stream_id),
        Err(e) => {
            tracing::warn!(error = %e, "module không hỗ trợ SSE — cần override `stream` + `stream_push`");
            None
        }
    };

    let sse = async_stream::stream! {
        let (mut rx, stream_id) = match (rx, stream_id) {
            (Some(rx), Some(id)) => (rx, id),
            _ => return,   // lỗi cấu hình/module → stream rỗng, không yield gì
        };

        loop {
            match rx.recv().await {
                Ok(msg) => {
                    // Module quyết định có phát message này không (trả "" = bỏ qua).
                    match module.stream_push(stream_id.clone(), msg.payload.to_string()).await {
                        Ok(json) if !json.is_empty() => {
                            if let Ok(event) = Event::default().json_data(json) {
                                yield Ok(event);
                            }
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::warn!(error = %e, "stream_push lỗi");
                            continue;
                        }
                    }
                }
                // Client chậm hơn pipeline → có message bị bỏ qua. Báo rõ
                // thay vì im lặng, đúng như `Lagged` được sinh ra để làm.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let data = serde_json::json!({
                        "error": format!("Data feed out of sync. Skipped {n} messages.")
                    })
                    .to_string();
                    yield Ok(Event::default().event("error").data(data));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }

        // Client rời đi → báo module đóng luồng.
        module.stream_close(stream_id);
    };

    Sse::new(sse).keep_alive(KeepAlive::default().interval(std::time::Duration::from_secs(15)))
}

/// Wrapper for dynamically loaded modules that implements OpsenseModule.
/// Uses the ModuleHandle to safely access the module state across async boundaries.
#[cfg(feature = "dynamic-modules")]
struct DynamicModuleWrapper {
    vtable: Box<ModuleV1>,
}

#[cfg(feature = "dynamic-modules")]
impl OpsenseModule for DynamicModuleWrapper {
    fn manifest(&self) -> Manifest {
        let json = unsafe { std::ffi::CStr::from_ptr((self.vtable.manifest)(self.vtable.state)) }
            .to_str()
            .unwrap_or("{}");
        serde_json::from_str(json).unwrap_or_default()
    }

    fn route(
        &self,
        req: HttpRequest,
        services: &TenantServices,
    ) -> opsense_ffi::BoxFut<'static, Result<HttpResponse, ModuleError>> {
        let req_json = serde_json::to_string(&req).unwrap_or_default();
        let vtable: usize = &*self.vtable as *const ModuleV1 as usize;
        let handle = self.vtable.state;
        let services_ptr: usize = services as *const TenantServices as usize;

        Box::pin(async move {
            let vtable = vtable as *const ModuleV1;
            let route_id = std::ffi::CString::new(req.route_id.clone()).unwrap_or_default();
            let req_c = std::ffi::CString::new(req_json).unwrap_or_default();
            let resp_ptr = unsafe {
                ((*vtable).handle)(
                    handle,
                    services_ptr as *const TenantServices,
                    route_id.as_ptr(),
                    req_c.as_ptr(),
                )
            };
            if resp_ptr.is_null() {
                return Err(ModuleError::Internal("null response from plugin".into()));
            }
            let resp_json = unsafe { std::ffi::CStr::from_ptr(resp_ptr).to_str().unwrap_or("{}") };
            let resp: Result<HttpResponse, ModuleError> = serde_json::from_str(resp_json)
                .map_err(|e| ModuleError::Internal(format!("response parse: {e}")));
            unsafe { ((*vtable).free)(resp_ptr) };
            resp
        })
    }

    fn tool(
        &self,
        name: String,
        args: serde_json::Value,
        services: &TenantServices,
    ) -> opsense_ffi::BoxFut<'static, Result<String, ModuleError>> {
        let req_json = serde_json::to_string(&args).unwrap_or_default();
        let vtable: usize = &*self.vtable as *const ModuleV1 as usize;
        let handle = self.vtable.state;
        let services_ptr: usize = services as *const TenantServices as usize;

        Box::pin(async move {
            let vtable = vtable as *const ModuleV1;
            let name_c = std::ffi::CString::new(name).unwrap_or_default();
            let args_c = std::ffi::CString::new(req_json).unwrap_or_default();
            let resp_ptr = unsafe {
                ((*vtable).handle)(
                    handle,
                    services_ptr as *const TenantServices,
                    name_c.as_ptr(),
                    args_c.as_ptr(),
                )
            };
            if resp_ptr.is_null() {
                return Err(ModuleError::Internal("null response from plugin".into()));
            }
            let resp_json = unsafe { std::ffi::CStr::from_ptr(resp_ptr).to_str().unwrap_or("{}") };
            let result: Result<String, ModuleError> =
                match serde_json::from_str::<serde_json::Value>(resp_json) {
                    Ok(v) if v.get("text").is_some() => {
                        Ok(v["text"].as_str().unwrap_or("").to_string())
                    }
                    Ok(v) if v.get("error").is_some() => Err(ModuleError::Internal(
                        v["message"].as_str().unwrap_or("tool error").to_string(),
                    )),
                    Ok(v) => Ok(v.to_string()),
                    Err(e) => Err(ModuleError::Internal(format!("tool response parse: {e}"))),
                };
            unsafe { ((*vtable).free)(resp_ptr) };
            result
        })
    }

    fn stream(
        &self,
        req: HttpRequest,
        services: &TenantServices,
    ) -> opsense_ffi::BoxFut<'static, Result<opsense_ffi::StreamHandle, ModuleError>> {
        // ABI 3 (module build cũ) không có entry này → trả lỗi rõ ràng thay vì
        // gọi vào slot rỗng.
        let Some(open) = self.vtable.stream_open else {
            return Box::pin(async {
                Err(ModuleError::Internal(
                    "module không hỗ trợ SSE — build lại với opsense-ffi ABI >= 4".into(),
                ))
            });
        };
        let req_json = serde_json::to_string(&req).unwrap_or_default();
        let vtable: usize = &*self.vtable as *const ModuleV1 as usize;
        let handle = self.vtable.state;
        let services_ptr: usize = services as *const TenantServices as usize;

        Box::pin(async move {
            let vtable = vtable as *const ModuleV1;
            let req_c = std::ffi::CString::new(req_json).unwrap_or_default();
            let resp_ptr = unsafe {
                open(
                    handle,
                    services_ptr as *const TenantServices,
                    std::ptr::null(),
                    req_c.as_ptr(),
                )
            };
            if resp_ptr.is_null() {
                return Err(ModuleError::Internal("null stream_open response".into()));
            }
            let resp_json = unsafe { std::ffi::CStr::from_ptr(resp_ptr).to_str().unwrap_or("{}") };
            let result = match serde_json::from_str::<serde_json::Value>(resp_json) {
                Ok(v) if v.get("error").is_some() => Err(ModuleError::Internal(
                    v["message"]
                        .as_str()
                        .unwrap_or("stream_open failed")
                        .to_string(),
                )),
                Ok(v) => Ok(opsense_ffi::StreamHandle {
                    stream_id: v["stream_id"].as_str().unwrap_or("").to_string(),
                }),
                Err(e) => Err(ModuleError::Internal(format!("stream_open parse: {e}"))),
            };
            unsafe { ((*vtable).free)(resp_ptr) };
            result
        })
    }

    fn stream_push(
        &self,
        stream_id: String,
        payload: String,
    ) -> opsense_ffi::BoxFut<'static, Result<String, ModuleError>> {
        let Some(push) = self.vtable.stream_push else {
            // Không có entry ⇒ module không lọc, coi như bỏ qua hết.
            return Box::pin(async { Ok(String::new()) });
        };
        let vtable: usize = &*self.vtable as *const ModuleV1 as usize;
        let handle = self.vtable.state;

        Box::pin(async move {
            let vtable = vtable as *const ModuleV1;
            let id_c = std::ffi::CString::new(stream_id).unwrap_or_default();
            let payload_c = std::ffi::CString::new(payload).unwrap_or_default();
            let resp_ptr = unsafe { push(handle, id_c.as_ptr(), payload_c.as_ptr()) };
            if resp_ptr.is_null() {
                return Err(ModuleError::Internal("null stream_push response".into()));
            }
            let resp_json = unsafe { std::ffi::CStr::from_ptr(resp_ptr).to_str().unwrap_or("") };
            let result = match serde_json::from_str::<serde_json::Value>(resp_json) {
                Ok(v) if v.get("error").is_some() => Err(ModuleError::Internal(
                    v["message"]
                        .as_str()
                        .unwrap_or("stream_push failed")
                        .to_string(),
                )),
                // Chuỗi rỗng = module chọn bỏ qua message này.
                Ok(serde_json::Value::String(s)) => Ok(s),
                Ok(v) => Ok(v.to_string()),
                Err(_) => Ok(resp_json.to_string()),
            };
            unsafe { ((*vtable).free)(resp_ptr) };
            result
        })
    }

    fn stream_close(&self, stream_id: String) {
        let Some(close) = self.vtable.stream_close else {
            return;
        };
        let handle = self.vtable.state;
        let id_c = std::ffi::CString::new(stream_id).unwrap_or_default();
        unsafe { close(handle, id_c.as_ptr()) };
    }
}

// KHÔNG implement `Drop`: cả `ModuleV1` lẫn state bên trong đều do plugin cấp
// phát (`Box::new` trong `into_vtable`). Host chỉ mượn con trỏ; tự `Box::from_raw`
// sẽ free nhầm vùng nhớ của plugin.

/// Leak một `Arc<dyn HostServices>` thành `*mut c_void` — sống `'static`,
/// một lần mỗi `load_dynamic` (một lần boot). Đổi lại mỗi module nhận đúng
/// instance của `AppState` đang chạy.
#[cfg(feature = "dynamic-modules")]
fn host_context(host_services: &Arc<dyn HostServices>) -> *mut std::ffi::c_void {
    Box::leak(Box::new(host_services.clone())) as *mut _ as *mut std::ffi::c_void
}

/// Build the HostV1 vtable with context from host_services.
#[cfg(feature = "dynamic-modules")]
fn build_host_v1(host_services: &Arc<dyn HostServices>) -> opsense_ffi::HostV1 {
    opsense_ffi::HostV1 {
        abi_version: opsense_ffi::ABI_VERSION,
        context: host_context(host_services),
        call: host_call_ffi,
        log: host_log_ffi,
        free: host_free_ffi,
    }
}

/// Stub implementation when dynamic-modules feature is disabled.
#[cfg(not(feature = "dynamic-modules"))]
#[allow(dead_code)]
fn build_host_v1() -> opsense_ffi::HostV1 {
    use std::ffi::c_char;

    // Return a dummy HostV1 with null function pointers - should never be called
    // when the feature is disabled since load_dynamic won't be called.
    unsafe extern "C" fn dummy_call(
        _: *mut c_void,
        _: *const c_char,
        _: *const c_char,
    ) -> *mut c_char {
        std::ptr::null_mut()
    }
    unsafe extern "C" fn dummy_log(_: *mut c_void, _: u32, _: *const c_char, _: *const c_char) {}
    unsafe extern "C" fn dummy_free(_: *mut c_char) {}

    opsense_ffi::HostV1 {
        abi_version: opsense_ffi::ABI_VERSION,
        context: std::ptr::null_mut(),
        call: dummy_call,
        log: dummy_log,
        free: dummy_free,
    }
}

#[cfg(feature = "dynamic-modules")]
unsafe extern "C" fn host_call_ffi(
    ctx: *mut std::ffi::c_void,
    op: *const c_char,
    req: *const c_char,
) -> *mut c_char {
    let op = unsafe { std::ffi::CStr::from_ptr(op).to_str().unwrap_or("") };
    let req = unsafe { std::ffi::CStr::from_ptr(req).to_str().unwrap_or("{}") };
    let req_json: serde_json::Value = serde_json::from_str(req).unwrap_or_default();

    // Context do `build_host_v1` cấp, sống `'static`.
    let host: &Arc<dyn HostServices> = unsafe { &*(ctx as *const Arc<dyn HostServices>) };
    let host = host.clone();

    let out = opsense_ffi::drive(async move { host.call(op, req_json).await });

    let resp_json = match out {
        Ok(v) => serde_json::to_string(&v).unwrap_or_else(|e| {
            serde_json::json!({ "error": "internal", "message": format!("serialize: {e}") })
                .to_string()
        }),
        Err(e) => serde_json::json!({ "error": "internal", "message": format!("{e}") }).to_string(),
    };

    std::ffi::CString::new(resp_json)
        .unwrap_or_default()
        .into_raw()
}

#[cfg(feature = "dynamic-modules")]
unsafe extern "C" fn host_log_ffi(
    _ctx: *mut std::ffi::c_void,
    level: u32,
    target: *const c_char,
    msg: *const c_char,
) {
    let msg = unsafe { std::ffi::CStr::from_ptr(msg).to_str().unwrap_or("") };
    let target = unsafe {
        std::ffi::CStr::from_ptr(target)
            .to_str()
            .unwrap_or("module")
    };
    match level {
        0 => tracing::error!("{} {}", target, msg),
        1 => tracing::warn!("{} {}", target, msg),
        2 => tracing::info!("{} {}", target, msg),
        3 => tracing::debug!("{} {}", target, msg),
        _ => tracing::trace!("{} {}", target, msg),
    }
}

#[cfg(feature = "dynamic-modules")]
use std::ffi::c_char;

#[cfg(feature = "dynamic-modules")]
unsafe extern "C" fn host_free_ffi(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            drop(std::ffi::CString::from_raw(s));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_ffi::RouteSpec;

    #[test]
    fn registry_basic() {
        let reg = ApiModuleRegistry::new();
        assert!(reg.modules.is_empty());
    }

    /// Helper tạo manifest test.
    fn manifest_with(routes: Vec<RouteSpec>) -> Manifest {
        Manifest {
            id: "test-module".into(),
            version: "0".into(),
            builtin: true,
            prefix: "/api/test".into(),
            routes,
            tools: vec![],
        }
    }

    fn rest_route(id: &str, method: HttpMethod, path: &str) -> RouteSpec {
        RouteSpec {
            id: id.into(),
            method,
            path: path.into(),
            transport: Transport::Rest,
            source_node: None,
        }
    }

    /// Trùng `(method, path)` bị từ chối ngay lúc đăng ký — ngăn panic axum lúc boot.
    #[test]
    fn rejects_duplicate_method_path() {
        let m = manifest_with(vec![
            rest_route("a", HttpMethod::Get, "/x"),
            rest_route("b", HttpMethod::Get, "/x"),
        ]);
        let err = ApiModuleRegistry::validate_routes(&m).unwrap_err();
        assert!(err.to_string().contains("duplicate (method, path)"));
    }

    /// Cùng path khác method là OK.
    #[test]
    fn allows_same_path_different_method() {
        let m = manifest_with(vec![
            rest_route("get", HttpMethod::Get, "/x"),
            rest_route("post", HttpMethod::Post, "/x"),
        ]);
        ApiModuleRegistry::validate_routes(&m).expect("cùng path khác method là OK");
    }

    /// Trùng shape (khác tên param) bị từ chối — `/x/{a}` + `/x/{b}` gây panic axum.
    #[test]
    fn rejects_same_shape_different_param_name() {
        let m = manifest_with(vec![
            rest_route("a", HttpMethod::Get, "/x/{a}"),
            rest_route("b", HttpMethod::Get, "/x/{b}"),
        ]);
        let err = ApiModuleRegistry::validate_routes(&m).unwrap_err();
        assert!(err.to_string().contains("duplicate (method, path shape)"));
    }

    /// Shape khác nhau (khác số segment) là OK.
    #[test]
    fn allows_different_shape_same_prefix() {
        let m = manifest_with(vec![
            rest_route("a", HttpMethod::Get, "/x/{a}"),
            rest_route("b", HttpMethod::Get, "/x/{a}/y"),
        ]);
        ApiModuleRegistry::validate_routes(&m).expect("shape khác nhau là OK");
    }

    /// SSE + method != GET bị từ chối.
    #[test]
    fn rejects_sse_with_non_get() {
        let m = manifest_with(vec![RouteSpec {
            id: "bad".into(),
            method: HttpMethod::Post,
            path: "/stream".into(),
            transport: Transport::Sse,
            source_node: Some("output".into()),
        }]);
        let err = ApiModuleRegistry::validate_routes(&m).unwrap_err();
        assert!(err.to_string().contains("SSE transport requires GET"));
    }

    #[test]
    fn path_shape_normalizes_params() {
        assert_eq!(ApiModuleRegistry::path_shape("/x/{a}/y/{b}"), "/x/{}/y/{}");
        assert_eq!(ApiModuleRegistry::path_shape("/plain/path"), "/plain/path");
        assert_eq!(ApiModuleRegistry::path_shape("/{single}"), "/{}");
        assert_eq!(ApiModuleRegistry::path_shape(""), "");
    }
}
