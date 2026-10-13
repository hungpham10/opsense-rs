//! Opsense Module SDK — ABI-stable interface for dynamic API/MCP modules.
//!
//! This crate defines the contract between the Opsense gateway (host) and
//! loadable modules (plugins). It uses only `#[repr(C)]` types + JSON over
//! raw pointers so that plugins can be compiled independently and loaded at
//! runtime via `dlopen` without Rust ABI compatibility issues.
//!
//! # Key Design Decisions
//!
//! 1. **No `async fn` in traits** — they are not dyn-compatible (E0038).
//!    All async operations return `Pin<Box<dyn Future<Output = T> + Send + '_>>`.
//! 2. **No Rust types across FFI** — only `char*` + JSON. Plugins are isolated.
//! 3. **Single host call** — `HostServices::call(op, req_json) -> res_json` is
//!    the only escape hatch; new ops are added without ABI changes.
//! 4. **ABI versioning** — `ABI_VERSION` constant; host refuses mismatched plugins.

pub mod cache;
pub mod error;
pub mod ffi;
pub mod host;
pub mod manifest;

use std::future::Future;
use std::pin::Pin;

pub use cache::{Cache, CacheBackend, CacheError, Page, TenantServices};
pub use error::ModuleError;
pub use ffi::*;
pub use host::HostServices;
pub use manifest::*;

/// Current ABI version. Increment when `HostV1` or `ModuleV1` layout changes.
pub const ABI_VERSION: u32 = 3;

/// Object-safe async return type used throughout the module contract.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Core module trait. Implement this in your plugin crate.
///
/// All methods are sync-to-async: they return a boxed future that the host
/// will `.await` inside a `tokio::task::spawn_blocking` call.
pub trait OpsenseModule: Send + Sync + 'static {
    /// Return the module manifest (id, version, routes, tools).
    fn manifest(&self) -> Manifest;

    /// Handle an HTTP request. The host converts the incoming axum request
    /// into `HttpRequest`, calls this, and converts the response back.
    /// `services` provides per-tenant cache and other host services.
    fn route(
        &self,
        req: HttpRequest,
        services: &TenantServices,
    ) -> BoxFut<'static, Result<HttpResponse, ModuleError>>;

    /// Handle an MCP tool call. The host converts the MCP `CallToolRequest`
    /// into `name` + `args` JSON, calls this, and wraps the result.
    /// `services` provides per-tenant cache and other host services.
    fn tool(
        &self,
        name: String,
        args: serde_json::Value,
        services: &TenantServices,
    ) -> BoxFut<'static, Result<String, ModuleError>>;
}

/// HTTP request envelope passed to `OpsenseModule::route`.
///
/// Contains everything needed to dispatch without axum types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpRequest {
    /// Route identifier from `Manifest::routes[i].id`.
    pub route_id: String,
    /// HTTP method.
    pub method: HttpMethod,
    /// Request path (without the module prefix).
    pub path: String,
    /// Query string as `Vec<(key, value)>`.
    pub query: Vec<(String, String)>,
    /// Headers as `Vec<(key, value)>`. Host normalizes to lowercase keys.
    pub headers: Vec<(String, String)>,
    /// Raw request body.
    pub body: Vec<u8>,
}

/// HTTP response envelope returned from `OpsenseModule::route`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// HTTP methods supported by module routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[derive(Default)]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
}

impl HttpMethod {
    pub fn from_axum(method: &axum::http::Method) -> Self {
        match method.as_str() {
            "GET" => HttpMethod::Get,
            "POST" => HttpMethod::Post,
            "PUT" => HttpMethod::Put,
            "PATCH" => HttpMethod::Patch,
            "DELETE" => HttpMethod::Delete,
            "HEAD" => HttpMethod::Head,
            "OPTIONS" => HttpMethod::Options,
            _ => HttpMethod::Get,
        }
    }
    pub fn to_axum(&self) -> axum::http::Method {
        match self {
            HttpMethod::Get => axum::http::Method::GET,
            HttpMethod::Post => axum::http::Method::POST,
            HttpMethod::Put => axum::http::Method::PUT,
            HttpMethod::Patch => axum::http::Method::PATCH,
            HttpMethod::Delete => axum::http::Method::DELETE,
            HttpMethod::Head => axum::http::Method::HEAD,
            HttpMethod::Options => axum::http::Method::OPTIONS,
        }
    }
}

/// Helper: create a successful JSON response.
pub fn json_response<T: serde::Serialize>(
    status: u16,
    value: &T,
) -> Result<HttpResponse, ModuleError> {
    let body = serde_json::to_vec(value)
        .map_err(|e| ModuleError::Internal(format!("JSON serialize: {e}")))?;
    Ok(HttpResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body,
    })
}

/// Helper: create an error JSON response.
pub fn error_response(status: u16, code: &str, message: &str) -> HttpResponse {
    let body = serde_json::json!({ "error": code, "message": message });
    let body = serde_json::to_vec(&body).unwrap_or_default();
    HttpResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body,
    }
}

/// Re-export commonly used types for convenience.
pub use serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn manifest_roundtrip() {
        let m = Manifest {
            id: "test".into(),
            version: "0.1.0".into(),
            builtin: false,
            prefix: "/api/test".into(),
            routes: vec![RouteSpec {
                id: "ping".into(),
                method: HttpMethod::Get,
                path: "/ping".into(),
            }],
            tools: vec![ToolSpec {
                name: "opsense_test".into(),
                description: "Test tool".into(),
                input_schema: json!({ "type": "object", "properties": {} }),
            }],
        };
        let s = serde_json::to_string(&m).unwrap();
        let m2: Manifest = serde_json::from_str(&s).unwrap();
        assert_eq!(m.id, m2.id);
        assert_eq!(m.routes.len(), m2.routes.len());
        assert_eq!(m.tools.len(), m2.tools.len());
    }

    #[test]
    fn http_envelope_roundtrip() {
        let req = HttpRequest {
            route_id: "ping".into(),
            method: HttpMethod::Get,
            path: "/ping".into(),
            query: vec![("key".into(), "val".into())],
            headers: vec![("content-type".into(), "application/json".into())],
            body: b"{}".to_vec(),
        };
        let s = serde_json::to_string(&req).unwrap();
        let req2: HttpRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(req.route_id, req2.route_id);
        assert_eq!(req.method, req2.method);
    }

    #[test]
    fn error_response_shape() {
        let resp = error_response(400, "bad_request", "missing field");
        assert_eq!(resp.status, 400);
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["error"], "bad_request");
        assert_eq!(v["message"], "missing field");
    }
}
