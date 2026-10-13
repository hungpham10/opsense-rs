//! Module error types.

use thiserror::Error;

/// Errors that can occur during module operation.
#[derive(Debug, Error, serde::Serialize, serde::Deserialize)]
#[serde(tag = "error", content = "message")]
pub enum ModuleError {
    #[error("route not found: {0}")]
    RouteNotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("host call failed: {0}")]
    HostCallFailed(String),

    #[error("ABI version mismatch: host={host}, plugin={plugin}")]
    AbiMismatch { host: u32, plugin: u32 },

    #[error("plugin not loaded: {0}")]
    NotLoaded(String),

    #[error("plugin init failed: {0}")]
    InitFailed(String),

    #[error("json error: {0}")]
    Json(String), // Manual conversion from serde_json::Error
}

impl From<serde_json::Error> for ModuleError {
    fn from(e: serde_json::Error) -> Self {
        ModuleError::Json(e.to_string())
    }
}

impl ModuleError {
    /// HTTP status code for this error.
    pub fn status(&self) -> u16 {
        match self {
            ModuleError::RouteNotFound(_) => 404,
            ModuleError::BadRequest(_) => 400,
            ModuleError::Internal(_) => 500,
            ModuleError::HostCallFailed(_) => 502,
            ModuleError::AbiMismatch { .. } => 500,
            ModuleError::NotLoaded(_) => 503,
            ModuleError::InitFailed(_) => 500,
            ModuleError::Json(_) => 400,
        }
    }

    /// Error code string for JSON responses.
    pub fn code(&self) -> &'static str {
        match self {
            ModuleError::RouteNotFound(_) => "route_not_found",
            ModuleError::BadRequest(_) => "bad_request",
            ModuleError::Internal(_) => "internal_error",
            ModuleError::HostCallFailed(_) => "host_call_failed",
            ModuleError::AbiMismatch { .. } => "abi_mismatch",
            ModuleError::NotLoaded(_) => "not_loaded",
            ModuleError::InitFailed(_) => "init_failed",
            ModuleError::Json(_) => "json_error",
        }
    }

    /// User-facing message.
    pub fn message(&self) -> String {
        self.to_string()
    }
}

/// Convert any `ModuleError` into an `HttpResponse` for the module route handler.
impl From<ModuleError> for super::HttpResponse {
    fn from(e: ModuleError) -> Self {
        super::error_response(e.status(), e.code(), &e.message())
    }
}