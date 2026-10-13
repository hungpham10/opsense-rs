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
    routing::{any, get},
};
use tokio::sync::broadcast;

use opsense_ffi::{
    HttpMethod, HttpRequest, HttpResponse, Manifest, OpsenseModule, TenantServices, Transport,
};

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
    libs: Vec<libloading::Library>, // Keep libraries alive
}

impl ApiModuleRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
            #[cfg(feature = "dynamic-modules")]
            libs: Vec::new(),
        }
    }

    /// Register a built-in module.
    pub fn register_builtin(&mut self, module: Arc<dyn OpsenseModule>) -> &mut Self {
        let id = module.manifest().id.clone();
        self.modules.insert(id, module);
        self
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
            let key = mcfg.id.clone();
            if module.manifest().id != key {
                tracing::warn!(module = %key, manifest_id = %module.manifest().id,
                    "manifest().id khác mcfg.id — registry key theo modules.toml");
            }
            self.modules.insert(key, Arc::new(module));
            self.libs.push(lib);
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
        let mut router = Router::new();

        for module in self.modules.values() {
            let manifest = module.manifest();
            let prefix = manifest.prefix.trim_end_matches('/').to_string();
            let module = module.clone();
            let state_for_handler = state.clone();
            let state_for_router = state.clone();

            // Mỗi route mount riêng theo `transport` — REST dùng `any()`, SSE
            // dùng `get()` vì SSE chỉ hợp với GET (client mở kết nối dài).
            let module_router = Router::new();
            let module_router = manifest.routes.iter().fold(module_router, |r, route| {
                let path = route.path.clone();
                match route.transport {
                    Transport::Rest => r.route(
                        &path,
                        any({
                            let module = module.clone();
                            let state = state_for_handler.clone();
                            move |req| module_route_handler(State(state), req, module.clone())
                        }),
                    ),
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

/// Tra `manifest.routes[].id` cho `(method, path)`.
///
/// `route_id` **phải** là id khai báo trong manifest: module tự dispatch dựa
/// trên id này (`ReplApiModule` chỉ nhận `"graphql"`, trả `RouteNotFound` cho
/// bất cứ id nào khác). Gửi tên bất kỳ — hay để trống — biến mọi request thành
/// 404 trong im lặng.
///
/// Ưu tiên khớp cả method lẫn path; nếu không có thì chấp nhận chỉ khớp path
/// (một số module dùng `any`). Không khớp gì thì trả chính `path`, để lỗi
/// `RouteNotFound` mang đúng tên route thay vì chuỗi rỗng.
fn resolve_route_id(manifest: &Manifest, method: HttpMethod, path: &str) -> String {
    manifest
        .routes
        .iter()
        .find(|r| r.path == path && r.method == method)
        .or_else(|| manifest.routes.iter().find(|r| r.path == path))
        .map(|r| r.id.clone())
        .unwrap_or_else(|| path.to_string())
}

/// Handler for module routes — converts axum request to HttpRequest, dispatches,
/// converts response back.
async fn module_route_handler(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    module: Arc<dyn OpsenseModule>,
) -> impl axum::response::IntoResponse {
    let manifest = module.manifest();

    // Extract tenant_id from X-Tenant-Id header
    static MISSING_TENANT_WARNED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

    let tenant_id = req
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| {
            if MISSING_TENANT_WARNED.set(()).is_ok() {
                tracing::warn!("thiếu/parse lỗi X-Tenant-Id → tenant_id=0 (chỉ log lần đầu)");
            }
            0
        });

    // Build TenantServices with cache backend bound to this tenant
    let cache_backend = state
        .connector
        .cache(tenant_id)
        .map(opsense_mlib::cache::RedisBackend::new);
    let services = TenantServices {
        tenant_id,
        cache: cache_backend,
    };
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
        route_id: resolve_route_id(&manifest, method, &path),
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
            &TenantServices {
                tenant_id: 0,
                cache: None,
            },
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

    #[test]
    fn registry_basic() {
        let reg = ApiModuleRegistry::new();
        assert!(reg.modules.is_empty());
    }

    /// Manifest của `ReplApiModule` — route `graphql` dưới prefix `/api/repl`.
    fn repl_manifest() -> Manifest {
        Manifest {
            id: "repl".into(),
            version: "0".into(),
            builtin: true,
            prefix: "/api/repl".into(),
            routes: vec![opsense_ffi::RouteSpec {
                id: "graphql".into(),
                method: HttpMethod::Post,
                path: "/graphql".into(),
                transport: opsense_ffi::Transport::Rest,
                source_node: None,
            }],
            tools: vec![],
        }
    }

    /// Hồi quy cho bug đã làm CI đỏ: `route_id` từng được hardcode `"handler"`,
    /// `ReplApiModule` trả `RouteNotFound` ⇒ **mọi** endpoint `/api/repl/*` 404.
    #[test]
    fn route_id_matches_manifest_id() {
        let m = repl_manifest();
        assert_eq!(
            resolve_route_id(&m, HttpMethod::Post, "/graphql"),
            "graphql"
        );
    }

    /// Path lạ thì trả chính path để lỗi `RouteNotFound` nói đúng tên route.
    #[test]
    fn route_id_falls_back_to_path() {
        let m = repl_manifest();
        assert_eq!(
            resolve_route_id(&m, HttpMethod::Post, "/khong-co"),
            "/khong-co"
        );
    }

    /// Method lệch vẫn khớp theo path — module dùng `any` cần điều này.
    #[test]
    fn route_id_matches_path_even_when_method_differs() {
        let m = repl_manifest();
        assert_eq!(resolve_route_id(&m, HttpMethod::Get, "/graphql"), "graphql");
    }
}
