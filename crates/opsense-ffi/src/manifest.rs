//! Module manifest — declarative description of what a module provides.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::HttpMethod;

/// Complete module manifest returned by `OpsenseModule::manifest()`.
///
/// Serialized to JSON and passed to the host during initialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    /// Unique module identifier (e.g., "repl", "xt-pnl").
    pub id: String,
    /// Semantic version of the module.
    pub version: String,
    /// Whether this module is built into the binary (true) or loaded from .so (false).
    pub builtin: bool,
    /// URL prefix where this module's HTTP routes are mounted (e.g., "/api/repl").
    pub prefix: String,
    /// HTTP routes this module handles.
    pub routes: Vec<RouteSpec>,
    /// MCP tools this module provides.
    pub tools: Vec<ToolSpec>,
}

/// HTTP route specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteSpec {
    /// Unique identifier for this route within the module.
    pub id: String,
    /// HTTP method.
    pub method: HttpMethod,
    /// Path pattern (relative to module prefix). Supports axum-style patterns.
    pub path: String,
}

/// MCP tool specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Tool name (must match `^opsense_[a-z_]+$` convention).
    pub name: String,
    /// Human-readable description for `tools/list`.
    pub description: String,
    /// JSON Schema for tool arguments (MCP `inputSchema`).
    pub input_schema: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tool_spec_schema() {
        let spec = ToolSpec {
            name: "opsense_foo".into(),
            description: "Does foo".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "bar": { "type": "string" }
                },
                "required": ["bar"]
            }),
        };
        let s = serde_json::to_string(&spec).unwrap();
        let d: ToolSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(d.name, "opsense_foo");
    }
}