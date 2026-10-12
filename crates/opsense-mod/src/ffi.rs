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

use super::{HttpRequest, ModuleError, ABI_VERSION, OpsenseModule};

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

    /// Call a host operation. `op` and `req` are JSON strings.
    /// Returns a JSON response string (caller must free via `free`).
    pub call: unsafe extern "C" fn(
        op: *const c_char,
        req: *const c_char,
    ) -> *mut c_char,

    /// Log a message through host's tracing.
    /// `level`: 0=error, 1=warn, 2=info, 3=debug, 4=trace.
    pub log: unsafe extern "C" fn(
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
    /// `route_id` is the route/tool identifier (from manifest).
    /// `req_json` is the request payload as JSON string.
    /// Returns a JSON response string (host must free via `free`).
    pub handle: unsafe extern "C" fn(
        state: ModuleHandle,
        route_id: *const c_char,
        req_json: *const c_char,
    ) -> *mut c_char,

    /// Free a string returned by `manifest` or `handle`.
    pub free: unsafe extern "C" fn(s: *mut c_char),
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
    unsafe { std::ffi::CStr::from_ptr(ptr) }.to_str().unwrap_or("").to_string()
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
    let req_c = to_c_string(
        serde_json::to_string(req).map_err(|e| ModuleError::Json(e.to_string()))?,
    );

    let res_ptr = unsafe { (host.call)(op_c, req_c) };
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
    unsafe { (host.log)(level, t, m) };
    unsafe { (host.free)(t) };
    unsafe { (host.free)(m) };
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
    /// module type, and `route_id`/`req_json` must be null or valid
    /// NUL-terminated C strings.
    unsafe extern "C" fn ffi_handle(
        state: ModuleHandle,
        route_id: *const c_char,
        req_json: *const c_char,
    ) -> *mut c_char {
        let wrapper = unsafe { &*(state.0 as *const ModuleWrapper) };
        let route_id = unsafe { from_c_str(route_id) };
        let req_json = unsafe { from_c_str(req_json) };

        // Parse request
        let req: HttpRequest = match serde_json::from_str(&req_json) {
            Ok(r) => r,
            Err(e) => {
                let err = ModuleError::BadRequest(format!("request parse: {e}"));
                return to_c_string(serde_json::to_string(&super::HttpResponse::from(err)).unwrap());
            }
        };

        // Dispatch based on route_id — check if it's a route or tool
        let manifest = wrapper.module.manifest();
        let is_route = manifest.routes.iter().any(|r| r.id == route_id);

        // Both route and tool return BoxFut, but with different output types.
        // We need to handle them separately.
        let result_json = if is_route {
            let mut fut = wrapper.module.route(req);
            let rt = tokio::runtime::Handle::try_current()
                .ok()
                .map(|h| h.block_on(&mut fut))
                .unwrap_or_else(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("tokio runtime");
                    rt.block_on(&mut fut)
                });
            match rt {
                Ok(resp) => serde_json::to_string(&resp).unwrap_or_else(|e| {
                    let err = ModuleError::Internal(format!("response serialize: {e}"));
                    serde_json::to_string(&super::HttpResponse::from(err)).unwrap()
                }),
                Err(e) => serde_json::to_string(&super::HttpResponse::from(
                    ModuleError::Internal(format!("panic: {e:?}"))
                ))
                .unwrap(),
            }
        } else {
            // Tool call: args are in req.body as JSON
            let args: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            let mut fut = wrapper.module.tool(route_id, args);
            let rt = tokio::runtime::Handle::try_current()
                .ok()
                .map(|h| h.block_on(&mut fut))
                .unwrap_or_else(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("tokio runtime");
                    rt.block_on(&mut fut)
                });
            match rt {
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
}
