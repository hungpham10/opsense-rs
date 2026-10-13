//! FFI definitions for dynamic module loading.
//!
//! This module defines the C-ABI interface between the host (opsense gateway)
//! and dynamically loaded modules (.so files).
//!
//! # Safety Requirements
//!
//! - Both host and plugin MUST be compiled with the same rustc version,
//!   same target triple, and `panic = "unwind"`.
//! - `ABI_VERSION` must match exactly; host refuses mismatched plugins.
//! - All memory allocated by the host is freed by the host; plugin memory
//!   is freed by the plugin via the `free` callback.
//! - JSON strings are UTF-8 encoded, NUL-terminated C strings.

use std::ffi::c_char;

use super::{ABI_VERSION, HttpRequest, ModuleError, OpsenseModule, cache::TenantServices};

/// Opaque module state pointer for FFI (C-ABI compatible).
/// We use `*mut c_void` for FFI compatibility, but wrap it in a Send/Sync type on the host side.
pub type ModuleState = *mut std::ffi::c_void;

/// Host → Plugin vtable. Passed to `opsense_module_<name>_init`.
///
/// The plugin stores this pointer and uses `call`/`log` to communicate
/// with the host. All strings are UTF-8, NUL-terminated, owned by the caller.
#[repr(C)]
pub struct HostV1 {
    /// Must equal `ABI_VERSION`. Plugin should verify on init.
    pub abi_version: u32,

    /// Opaque context pointer — set by the host during `load_dynamic`.
    /// Each `HostV1` gets its own context so multiple `AppState` instances
    /// don't share the same global. The host passes this back to `call`/`log`.
    pub context: *mut std::ffi::c_void,

    /// Call a host operation. `op` and `req` are JSON strings.
    /// Returns a JSON response string (caller must free via `free`).
    /// The first argument is the `context` pointer from this `HostV1`.
    pub call: unsafe extern "C" fn(
        ctx: *mut std::ffi::c_void,
        op: *const c_char,
        req: *const c_char,
    ) -> *mut c_char,

    /// Log a message through host's tracing.
    /// `level`: 0=error, 1=warn, 2=info, 3=debug, 4=trace.
    /// The first argument is the `context` pointer from this `HostV1`.
    pub log: unsafe extern "C" fn(
        ctx: *mut std::ffi::c_void,
        level: u32,
        target: *const c_char,
        msg: *const c_char,
    ),

    /// Free a string returned by `call` or `manifest`/`handle`.
    pub free: unsafe extern "C" fn(s: *mut c_char),
}

/// Plugin → Host vtable. Returned by `opsense_module_<name>_init`.
///
/// The host calls `manifest()` once at load time, then `handle()` for
/// each request. The `state` pointer is the opaque `ModuleState`
/// returned by `init`.
#[repr(C)]
pub struct ModuleV1 {
    /// Must equal `ABI_VERSION`. Host verifies on load.
    pub abi_version: u32,

    /// Opaque module state (set by `init`). Uses `ModuleHandle` which is
    /// `#[repr(transparent)]` over `ModuleState` and implements Send/Sync.
    pub state: ModuleHandle,

    /// Return the module manifest as a JSON string.
    /// Host calls this once and parses the JSON.
    pub manifest: unsafe extern "C" fn(state: ModuleHandle) -> *mut c_char,

    /// Handle an HTTP request or tool call.
    ///
    /// `services` provides per-tenant cache and other host services.
    /// `route_id` is the route/tool identifier (from manifest).
    /// `req_json` is the request payload as JSON string.
    /// Returns a JSON response string (host must free via `free`).
    pub handle: unsafe extern "C" fn(
        state: ModuleHandle,
        services: *const TenantServices,
        route_id: *const c_char,
        req_json: *const c_char,
    ) -> *mut c_char,

    /// Free a string returned by `manifest` or `handle`.
    pub free: unsafe extern "C" fn(s: *mut c_char),

    // ── SSE (ABI 4) ────────────────────────────────────────────────────────
    //
    // Tất cả là `Option`: module build với ABI 3 không có entry này → vẫn
    // `dlopen` được, chỉ không có SSE. Host check `is_some()` trước khi gọi.
    //
    // Mô hình: host chạy vòng `recv()` trên broadcast channel của `source_node`
    // (crate `opsense`), gọi `stream_push` cho từng message, module quyết định
    // phát gì. `broadcast::Receiver` không FFI-safe nên vòng loop phải ở host.
    /// Mở luồng SSE. Trả JSON `{"stream_id": "..."}` hoặc null nếu lỗi.
    pub stream_open: Option<
        unsafe extern "C" fn(
            state: ModuleHandle,
            services: *const TenantServices,
            route_id: *const c_char,
            req_json: *const c_char,
        ) -> *mut c_char,
    >,

    /// Module quyết định có phát message này không.
    ///
    /// Trả chuỗi rỗng `""` = bỏ qua. Trả JSON = event cần phát xuống client.
    /// Đây là chỗ "xào trộn dữ liệu" sẽ nằm — host không đổi khi thêm lọc.
    pub stream_push: Option<
        unsafe extern "C" fn(
            state: ModuleHandle,
            stream_id: *const c_char,
            payload: *const c_char,
        ) -> *mut c_char,
    >,

    /// Đóng luồng SSE (client rời đi).
    pub stream_close: Option<unsafe extern "C" fn(state: ModuleHandle, stream_id: *const c_char)>,
}

/// Initialize a module.
///
/// Symbol name convention: `opsense_module_<crate_name>_init`
/// where `<crate_name>` is the Cargo crate name with `-` replaced by `_`.
///
/// # Safety
///
/// - `host` must be a valid pointer to a `HostV1` that lives for the
///   entire process lifetime.
/// - Returns a valid `ModuleV1` pointer on success, or null on failure.
/// - The returned `ModuleV1` must have `abi_version == ABI_VERSION`.
pub type ModuleInitFn = unsafe extern "C" fn(host: *const HostV1) -> *mut ModuleV1;

/// Internal wrapper to hold the module with proper lifetime and Send/Sync.
/// This is a fat pointer to `dyn OpsenseModule`, which implements Send/Sync.
pub struct ModuleWrapper {
    pub module: Box<dyn OpsenseModule>,
}

/// A Send/Sync wrapper around the raw module state pointer.
///
/// `#[repr(transparent)]` over `*mut c_void` so the struct is FFI-safe: it has
/// the same layout as a raw pointer, and the vtable fields in [`ModuleV1`]
/// can take it by value without tripping `improper_ctypes_definitions`.
///
/// # Safety
///
/// The pointer must either be null or point to a live [`ModuleWrapper`]
/// allocation that outlives every use. Only `ModuleExports::into_vtable`
/// hands these pointers out, and it keeps the `Box` alive for the process
/// lifetime.
#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct ModuleHandle(pub ModuleState);

// SAFETY: the pointer is only ever dereferenced inside the same crate's
// `ffi_*` shims, and `ModuleWrapper` holds a `Box<dyn OpsenseModule>` which is
// itself `Send + Sync`. Both guarantees are established at `into_vtable`.
unsafe impl Send for ModuleHandle {}
// SAFETY: see `Send` above — the pointee is immutable after construction.
unsafe impl Sync for ModuleHandle {}

impl ModuleHandle {
    /// Wrap a raw module-state pointer.
    pub fn new(state: ModuleState) -> Self {
        Self(state)
    }

    /// Recover the raw pointer.
    pub fn as_ptr(&self) -> ModuleState {
        self.0
    }

    /// Helper for FFI: convert `ModuleHandle` to `ModuleState`.
    ///
    /// `ModuleHandle` is `#[repr(transparent)]` over `ModuleState`, so the two
    /// have identical memory layout.
    pub fn to_state(self) -> ModuleState {
        self.0
    }
}

/// Helper: convert Rust string to C string (caller must free with `free`).
fn to_c_string(s: String) -> *mut c_char {
    use std::ffi::CString;
    CString::new(s)
        .unwrap_or_else(|_| CString::new("").unwrap())
        .into_raw()
}

/// Helper: convert C string to owned Rust String.
///
/// # Safety
///
/// `ptr` must be null or point to a valid NUL-terminated C string that stays
/// alive for the duration of the call.
unsafe fn from_c_str(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .unwrap_or("")
        .to_string()
}

/// Helper: call a host operation from plugin Rust code.
///
/// Usage inside plugin:
/// ```rust,ignore
/// let req = serde_json::json!({ "node": "grid" });
/// let res_json = host_call(host_vtable, "status", &req)?;
/// let response: Value = serde_json::from_str(&res_json)?;
/// ```
///
/// # Safety
///
/// `host` must be a live [`HostV1`] whose `call`/`free` callbacks are valid
/// for the whole call.
pub unsafe fn host_call(
    host: &HostV1,
    op: &str,
    req: &serde_json::Value,
) -> Result<String, ModuleError> {
    let op_c = to_c_string(op.into());
    let req_c =
        to_c_string(serde_json::to_string(req).map_err(|e| ModuleError::Json(e.to_string()))?);

    let res_ptr = unsafe { (host.call)(host.context, op_c, req_c) };
    unsafe { (host.free)(op_c) };
    unsafe { (host.free)(req_c) };

    if res_ptr.is_null() {
        return Err(ModuleError::HostCallFailed("null response".into()));
    }

    let res_str = unsafe { std::ffi::CStr::from_ptr(res_ptr) }
        .to_str()
        .map_err(|e| ModuleError::Internal(format!("UTF-8 error: {e}")))?
        .to_string();
    unsafe { (host.free)(res_ptr) };
    Ok(res_str)
}

/// Helper: log from plugin.
///
/// # Safety
///
/// `host` must be a live [`HostV1`] whose `log`/`free` callbacks are valid
/// for the whole call.
pub unsafe fn host_log(host: &HostV1, level: u32, target: &str, msg: &str) {
    let t = to_c_string(target.into());
    let m = to_c_string(msg.into());
    unsafe { (host.log)(host.context, level, t, m) };
    unsafe { (host.free)(t) };
    unsafe { (host.free)(m) };
}

/// Drive a future from FFI context, avoiding "runtime within a runtime" panic.
///
/// FFI `handle()` is sync; module route/tool is async. Must drive the future
/// but we're inside an async handler from axum ⇒ `Handle::block_on` panics.
///
/// Strategy:
/// - Multi-thread runtime: use `block_in_place` + `Handle::block_on`
/// - Current-thread or no runtime: spawn on dedicated multi-thread runtime with 1 worker
pub fn drive<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| h.block_on(fut))
        }
        _ => {
            static MODULE_RT: std::sync::OnceLock<tokio::runtime::Runtime> =
                std::sync::OnceLock::new();
            let rt = MODULE_RT.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .expect("module runtime")
            });
            futures::executor::block_on(rt.spawn(fut)).expect("module task panicked")
        }
    }
}

/// Wrapper trait implemented by plugins to bridge from the C ABI to Rust.
///
/// Plugins implement `OpsenseModule` (the Rust trait) and use this
/// wrapper to generate the `ModuleV1` vtable.
pub trait ModuleExports: super::OpsenseModule {
    /// Create the ModuleV1 vtable for this module.
    fn into_vtable(self) -> Box<ModuleV1>
    where
        Self: Sized,
    {
        // Box the module and get a raw pointer to it
        let wrapper = Box::new(ModuleWrapper {
            module: Box::new(self),
        });
        let state = ModuleHandle(Box::into_raw(wrapper) as ModuleState);
        Box::new(ModuleV1 {
            abi_version: ABI_VERSION,
            state,
            manifest: Self::ffi_manifest,
            handle: Self::ffi_handle,
            free: Self::ffi_free,
            // Module không override `stream` → default impl trả lỗi. Điền `None`
            // để host biết module này không hỗ trợ SSE, thay vì gọi vào slot
            // rỗng và segfault.
            stream_open: None,
            stream_push: None,
            stream_close: None,
        })
    }

    /// # Safety
    ///
    /// `state` must come from [`ModuleExports::into_vtable`] on this same
    /// module type (see the `ModuleHandle` safety contract).
    unsafe extern "C" fn ffi_manifest(state: ModuleHandle) -> *mut c_char {
        let wrapper = unsafe { &*(state.0 as *const ModuleWrapper) };
        let manifest = wrapper.module.manifest();
        let json = serde_json::to_string(&manifest).unwrap_or_else(|_| "{}".into());
        to_c_string(json)
    }

    /// # Safety
    ///
    /// `state` must come from [`ModuleExports::into_vtable`] on this same
    /// module type, and `services`/`route_id`/`req_json` must be null or valid
    /// NUL-terminated C strings.
    unsafe extern "C" fn ffi_handle(
        state: ModuleHandle,
        services: *const TenantServices,
        route_id: *const c_char,
        req_json: *const c_char,
    ) -> *mut c_char {
        let wrapper = unsafe { &*(state.0 as *const ModuleWrapper) };
        let services = unsafe { services.as_ref() };
        let route_id = unsafe { from_c_str(route_id) };
        let req_json = unsafe { from_c_str(req_json) };

        // Parse request
        let req: HttpRequest = match serde_json::from_str(&req_json) {
            Ok(r) => r,
            Err(e) => {
                let err = ModuleError::BadRequest(format!("request parse: {e}"));
                return to_c_string(
                    serde_json::to_string(&super::HttpResponse::from(err)).unwrap(),
                );
            }
        };

        let services = services.cloned().unwrap_or(TenantServices {
            tenant_id: 0,
            cache: None,
        });

        // Dispatch based on route_id — check if it's a route or tool
        let manifest = wrapper.module.manifest();
        let is_route = manifest.routes.iter().any(|r| r.id == route_id);

        // Both route and tool return BoxFut, but with different output types.
        // We need to handle them separately.
        let result_json = if is_route {
            let fut = wrapper.module.route(req, &services);
            let result = drive(fut);
            match result {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|e| {
                    let err = ModuleError::Internal(format!("response serialize: {e}"));
                    serde_json::to_string(&super::HttpResponse::from(err)).unwrap()
                }),
                Err(e) => serde_json::to_string(&super::HttpResponse::from(ModuleError::Internal(
                    format!("panic: {e:?}"),
                )))
                .unwrap(),
            }
        } else {
            // Tool call: args are in req.body as JSON
            let args: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            let fut = wrapper.module.tool(route_id, args, &services);
            let result = drive(fut);
            match result {
                Ok(text) => {
                    // Tool returns plain text, wrap in JSON for FFI
                    let resp = serde_json::json!({ "text": text });
                    serde_json::to_string(&resp).unwrap()
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": e.code(), "message": e.message() });
                    serde_json::to_string(&resp).unwrap()
                }
            }
        };

        to_c_string(result_json)
    }

    /// # Safety
    ///
    /// `s` must be null or a pointer previously returned by `ffi_manifest` /
    /// `ffi_handle` that has not been freed yet.
    unsafe extern "C" fn ffi_free(s: *mut c_char) {
        if !s.is_null() {
            drop(unsafe { std::ffi::CString::from_raw(s) });
        }
    }

    // ── SSE (ABI 4) ────────────────────────────────────────────────────────
    //
    // Module không override `stream` → default impl trả lỗi. Ba hàm này chỉ
    // được gọi khi module **có** override, nên `unwrap()` ở đây là an toàn:
    // host đã check `is_some()` trước khi gọi.

    /// # Safety
    ///
    /// `state` must come from [`ModuleExports::into_vtable`] on this same
    /// module type; `services`/`route_id`/`req_json` must be null or valid
    /// NUL-terminated C strings.
    unsafe extern "C" fn ffi_stream_open(
        state: ModuleHandle,
        services: *const TenantServices,
        route_id: *const c_char,
        req_json: *const c_char,
    ) -> *mut c_char {
        let wrapper = unsafe { &*(state.0 as *const ModuleWrapper) };
        let services = unsafe { services.as_ref() };
        let _route_id = unsafe { from_c_str(route_id) };
        let req_json = unsafe { from_c_str(req_json) };

        let req: HttpRequest = match serde_json::from_str(&req_json) {
            Ok(r) => r,
            Err(e) => {
                let err = ModuleError::BadRequest(format!("request parse: {e}"));
                return to_c_string(
                    serde_json::to_string(&super::HttpResponse::from(err)).unwrap(),
                );
            }
        };

        let services = services.cloned().unwrap_or(TenantServices {
            tenant_id: 0,
            cache: None,
        });

        let fut = wrapper.module.stream(req, &services);
        match drive(fut) {
            Ok(handle) => {
                to_c_string(serde_json::json!({ "stream_id": handle.stream_id }).to_string())
            }
            Err(e) => to_c_string(
                serde_json::json!({ "error": e.code(), "message": e.message() }).to_string(),
            ),
        }
    }

    /// # Safety
    ///
    /// `state` must come from [`ModuleExports::into_vtable`] on this same
    /// module type; `stream_id`/`payload` must be null or valid NUL-terminated
    /// C strings.
    unsafe extern "C" fn ffi_stream_push(
        state: ModuleHandle,
        stream_id: *const c_char,
        payload: *const c_char,
    ) -> *mut c_char {
        let wrapper = unsafe { &*(state.0 as *const ModuleWrapper) };
        let stream_id = unsafe { from_c_str(stream_id) };
        let payload = unsafe { from_c_str(payload) };

        // Module trả chuỗi rỗng = bỏ qua message này. Trả JSON = phát xuống
        // client. Đây là chỗ "xào trộn dữ liệu" sẽ nằm.
        let fut = wrapper.module.stream_push(stream_id, payload);
        match drive(fut) {
            Ok(json) => to_c_string(json),
            Err(e) => to_c_string(
                serde_json::json!({ "error": e.code(), "message": e.message() }).to_string(),
            ),
        }
    }

    /// # Safety
    ///
    /// `state` must come from [`ModuleExports::into_vtable`] on this same
    /// module type; `stream_id` must be null or a valid NUL-terminated C string.
    unsafe extern "C" fn ffi_stream_close(state: ModuleHandle, stream_id: *const c_char) {
        let wrapper = unsafe { &*(state.0 as *const ModuleWrapper) };
        let stream_id = unsafe { from_c_str(stream_id) };
        wrapper.module.stream_close(stream_id);
    }
}

/// Blanket implementation for all `OpsenseModule` types.
impl<T: super::OpsenseModule> ModuleExports for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c_string_roundtrip() {
        let s = "hello world";
        let c = to_c_string(s.into());
        unsafe {
            let back = std::ffi::CStr::from_ptr(c).to_str().unwrap();
            assert_eq!(back, s);
            unsafe extern "C" fn free_fn(s: *mut c_char) {
                if !s.is_null() {
                    drop(unsafe { std::ffi::CString::from_raw(s) });
                }
            }
            free_fn(c);
        }
    }

    #[test]
    fn host_v1_layout() {
        // u32(4) + pad(4) + context(8) + 3*fn_ptr(8) = 40
        assert_eq!(std::mem::size_of::<HostV1>(), 40);
        assert_eq!(std::mem::align_of::<HostV1>(), 8);
    }

    #[test]
    fn abi_bumped_to_4_for_sse() {
        // 4 = thêm 3 entry `stream_*` (Option) vào cuối `ModuleV1`. Module
        // build với ABI 3 vẫn dlopen được vì entry mới là `Option` — host
        // check `is_some()` và trả lỗi rõ ràng nếu thiếu.
        assert_eq!(ABI_VERSION, 4);
    }

    /// Verify the fallback branch works with current_thread runtime.
    /// Before the fix, this test would hang because `new_current_thread()` runtime
    /// has no worker thread to run spawned tasks.
    #[test]
    fn drive_works_inside_current_thread_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async { drive(async { 42 }) });
        assert_eq!(out, 42);
    }
}
