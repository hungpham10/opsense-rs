//! API module registry — loads and manages HTTP API modules.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    http::{Request, Response},
    routing::any,
};

use opsense_mod::{
    HttpMethod, HttpRequest, HttpResponse, OpsenseModule,
};

#[cfg(feature = "dynamic-modules")]
use opsense_mod::{
    HostServices, Manifest, ModuleError, ModuleHandle, ModuleV1,
};

use crate::api::AppState;

#[cfg(feature = "dynamic-modules")]
use {
    crate::modules_config::{ModuleConfig, ModulesConfig},
    libloading::Library,
};

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

    /// Load dynamic modules from the configured directory.
    #[cfg(feature = "dynamic-modules")]
    pub fn load_dynamic(
        &mut self,
        module_dir: &Path,
        cfg: &ModulesConfig,
        host_services: Arc<dyn HostServices>,
    ) -> Result<(), ModuleError> {
        use libloading::Library;

        // Store host services globally for FFI callbacks
        HOST_SERVICES.set(host_services.clone()).ok();

        for mcfg in cfg.enabled_modules() {
            if mcfg.builtin { continue; }

            let path = cfg.dynamic_path(module_dir, mcfg)
                .ok_or_else(|| ModuleError::InitFailed(format!("no path for dynamic module {}", mcfg.id)))?;

            tracing::info!(module = %mcfg.id, path = %path.display(), "loading dynamic module");
            let lib = unsafe { Library::new(&path) }
                .map_err(|e| ModuleError::InitFailed(format!("dlopen {}: {}", path.display(), e)))?;

            // Symbol name: opsense_module_<id>_init (with - replaced by _)
            let sym_name = format!("opsense_module_{}_init", mcfg.id.replace('-', "_"));
            let init_fn: libloading::Symbol<ModuleInitFn> = unsafe { lib.get(sym_name.as_bytes()) }
                .map_err(|e| ModuleError::InitFailed(format!("symbol {}: {}", sym_name, e)))?;

            // Build HostV1
            let host_v1 = build_host_v1();

            // Initialize module
            let module_v1 = unsafe { init_fn(&host_v1) };
            if module_v1.is_null() {
                return Err(ModuleError::InitFailed(format!("init returned null for {}", mcfg.id)));
            }

            // Verify ABI version
            unsafe {
                if (*module_v1).abi_version != opsense_mod::ABI_VERSION {
                    return Err(ModuleError::AbiMismatch {
                        host: opsense_mod::ABI_VERSION,
                        plugin: (*module_v1).abi_version,
                    });
                }
            }

            // Create wrapper that implements OpsenseModule
            let module = DynamicModuleWrapper {
                vtable: unsafe { Box::from_raw(module_v1) },
            };
            let id = module.manifest().id.clone();
            self.modules.insert(id, Arc::new(module));
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

            // Mount a wildcard handler at the module prefix
            let module_router = Router::new()
                .route("/{*path}", any(move |req| module_route_handler(State(state_for_handler), req, module.clone())))
                .with_state(state_for_router);

            router = router.nest(&prefix, module_router);
        }

        router
    }
}

impl Default for ApiModuleRegistry {
    fn default() -> Self { Self::new() }
}

/// Handler for module routes — converts axum request to HttpRequest, dispatches,
/// converts response back.
async fn module_route_handler(
    State(_state): State<AppState>,
    req: Request<axum::body::Body>,
    module: Arc<dyn OpsenseModule>,
) -> impl axum::response::IntoResponse {
    let manifest = module.manifest();
    let method = HttpMethod::from_axum(req.method());
    let path = req.uri().path().trim_start_matches(&manifest.prefix).to_string();
    let path = if path.is_empty() { "/".to_string() } else { path };

    let query: Vec<_> = req.uri().query()
        .map(|q| form_urlencoded::parse(q.as_bytes()).into_owned().collect())
        .unwrap_or_default();

    let headers: Vec<_> = req.headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_lowercase(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default()
        .to_vec();

    let http_req = HttpRequest {
        route_id: "handler".to_string(), // will be matched inside module
        method,
        path,
        query,
        headers,
        body,
    };

    // Module handles its own route matching via manifest.routes
    match module.route(http_req).await {
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

/// Wrapper for dynamically loaded modules that implements OpsenseModule.
/// Uses the ModuleHandle to safely access the module state across async boundaries.
#[cfg(feature = "dynamic-modules")]
struct DynamicModuleWrapper {
    vtable: Box<ModuleV1>,
    handle: ModuleHandle,
}

#[cfg(feature = "dynamic-modules")]
impl OpsenseModule for DynamicModuleWrapper {
    fn manifest(&self) -> Manifest {
        let json = unsafe { std::ffi::CStr::from_ptr((self.vtable.manifest)(self.handle)) }
            .to_str()
            .unwrap_or("{}");
        serde_json::from_str(json).unwrap_or_default()
    }

    fn route(&self, req: HttpRequest) -> opsense_mod::BoxFut<'static, Result<HttpResponse, ModuleError>> {
        let route_id = req.route_id.clone();
        let req_json = serde_json::to_string(&req).unwrap_or_default();
        let handle = self.handle; // Copy the handle (it's Copy)

        Box::pin(async move {
            let vtable = handle.0 as *const ModuleV1;
            let resp_ptr = unsafe { ((*vtable).handle)(handle, route_id.as_ptr() as *const _, req_json.as_ptr() as *const _) };
            if resp_ptr.is_null() {
                return Err(ModuleError::Internal("null response from plugin".into()));
            }
            let resp_json = unsafe { std::ffi::CStr::from_ptr(resp_ptr).to_str().unwrap_or("{}") };
            let resp: Result<HttpResponse, ModuleError> = serde_json::from_str(resp_json).unwrap_or_else(|e| {
                Err(ModuleError::Internal(format!("response parse: {e}")))
            });
            unsafe { ((*vtable).free)(resp_ptr) };
            resp
        })
    }

    fn tool(&self, name: String, args: serde_json::Value) -> opsense_mod::BoxFut<'static, Result<String, ModuleError>> {
        let req_json = serde_json::to_string(&args).unwrap_or_default();
        let handle = self.handle; // Copy the handle

        Box::pin(async move {
            let vtable = handle.0 as *const ModuleV1;
            let resp_ptr = unsafe { ((*vtable).handle)(handle, name.as_ptr() as *const _, req_json.as_ptr() as *const _) };
            if resp_ptr.is_null() {
                return Err(ModuleError::Internal("null response from plugin".into()));
            }
            let resp_json = unsafe { std::ffi::CStr::from_ptr(resp_ptr).to_str().unwrap_or("{}") };
            // Tool responses are wrapped in { "text": "..." } or { "error": "...", "message": "..." }
            let result: Result<String, ModuleError> = match serde_json::from_str::<serde_json::Value>(resp_json) {
                Ok(v) if v.get("text").is_some() => Ok(v["text"].as_str().unwrap_or("").to_string()),
                Ok(v) if v.get("error").is_some() => Err(ModuleError::Internal(v["message"].as_str().unwrap_or("tool error").to_string())),
                Ok(v) => Ok(v.to_string()), // fallback
                Err(e) => Err(ModuleError::Internal(format!("tool response parse: {e}"))),
            };
            unsafe { ((*vtable).free)(resp_ptr) };
            result
        })
    }
}

#[cfg(feature = "dynamic-modules")]
impl Drop for DynamicModuleWrapper {
    fn drop(&mut self) {
        // The module state is a pointer to ModuleWrapper
        // We need to drop it properly
        unsafe {
            let _ = Box::from_raw(self.vtable.state.0 as *mut opsense_mod::ffi::ModuleWrapper);
        }
    }
}

/// Global storage for host services (used by FFI callbacks).
#[cfg(feature = "dynamic-modules")]
static HOST_SERVICES: std::sync::OnceLock<Arc<dyn HostServices>> = std::sync::OnceLock::new();

/// Build the HostV1 vtable using global host services.
#[cfg(feature = "dynamic-modules")]
fn build_host_v1() -> opsense_mod::HostV1 {
    opsense_mod::HostV1 {
        abi_version: opsense_mod::ABI_VERSION,
        call: host_call_ffi,
        log: host_log_ffi,
        free: host_free_ffi,
    }
}

/// Stub implementation when dynamic-modules feature is disabled.
#[cfg(not(feature = "dynamic-modules"))]
#[allow(dead_code)]
fn build_host_v1() -> opsense_mod::HostV1 {
    use std::ffi::c_char;

    // Return a dummy HostV1 with null function pointers - should never be called
    // when the feature is disabled since load_dynamic won't be called.
    unsafe extern "C" fn dummy_call(_: *const c_char, _: *const c_char) -> *mut c_char {
        std::ptr::null_mut()
    }
    unsafe extern "C" fn dummy_log(_: u32, _: *const c_char, _: *const c_char) {}
    unsafe extern "C" fn dummy_free(_: *mut c_char) {}

    opsense_mod::HostV1 {
        abi_version: opsense_mod::ABI_VERSION,
        call: dummy_call,
        log: dummy_log,
        free: dummy_free,
    }
}

#[cfg(feature = "dynamic-modules")]
unsafe extern "C" fn host_call_ffi(op: *const c_char, req: *const c_char) -> *mut c_char {
    let op = unsafe { std::ffi::CStr::from_ptr(op).to_str().unwrap_or("") };
    let req = unsafe { std::ffi::CStr::from_ptr(req).to_str().unwrap_or("{}") };
    let req_json: serde_json::Value = serde_json::from_str(req).unwrap_or_default();

    let host = HOST_SERVICES.get().expect("host services not initialized");

    let rt = tokio::runtime::Handle::try_current()
        .ok()
        .map(|h| h.block_on(host.call(op, req_json.clone())))
        .unwrap_or_else(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(host.call(op, req_json))
        });

    let resp_json = match rt {
        Ok(v) => serde_json::to_string(&v).unwrap_or_else(|e| {
            serde_json::json!({ "error": "internal", "message": format!("serialize: {e}") }).to_string()
        }),
        Err(e) => serde_json::json!({ "error": "internal", "message": format!("{e}") }).to_string(),
    };

    std::ffi::CString::new(resp_json).unwrap_or_default().into_raw()
}

#[cfg(feature = "dynamic-modules")]
unsafe extern "C" fn host_log_ffi(level: u32, target: *const c_char, msg: *const c_char) {
    let msg = unsafe { std::ffi::CStr::from_ptr(msg).to_str().unwrap_or("") };
    let target = unsafe { std::ffi::CStr::from_ptr(target).to_str().unwrap_or("module") };
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
        unsafe { drop(std::ffi::CString::from_raw(s)); }
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
}