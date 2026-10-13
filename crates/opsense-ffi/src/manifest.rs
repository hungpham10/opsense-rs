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

/// Kiểu transport của một route trong [`Manifest::routes`].
///
/// Chỉ có `Rest` và `Sse` ở giai đoạn này. WebSocket chưa làm: đường đẩy
/// hiện tại là `Runtime::broadcast` (một chiều), WS hai chiều cần thêm
/// `socket_frame` vào vtable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// HTTP request/response thường — mặc định.
    #[default]
    Rest,
    /// Server-Sent Events. Host nối `source_node` rồi phát message xuống client.
    Sse,
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
    /// Cách phục vụ route này. Mặc định `Rest`.
    ///
    /// Không có trong manifest cũ thì nhận `Rest` nhờ `#[serde(default)]` —
    /// `hello-mod` và mọi module viết tay không bị vỡ.
    #[serde(default)]
    pub transport: Transport,
    /// Node `output` làm nguồn dữ liệu, chỉ dùng khi `transport = "sse"`.
    ///
    /// Phải là `id` của một node khai `type = "output"` trong config: chỉ node
    /// đó mới có broadcast channel. `#[sink]` (ví dụ station sink) không có.
    #[serde(default)]
    pub source_node: Option<String>,
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

    /// Manifest viết tay (không có `transport`/`source_node`) vẫn parse được.
    ///
    /// Đây là điều kiện sống còn: `hello-mod` và mọi module `.so` build từ
    /// trước khi có SSE đều thiếu field. Nếu mất `#[serde(default)]` thì
    /// **mọi** manifest cũ bị reject → không module nào load được.
    #[test]
    fn manifest_without_transport_defaults_to_rest() {
        let json = r#"{
            "id": "legacy",
            "version": "0.1.0",
            "builtin": false,
            "prefix": "/api/legacy",
            "routes": [{"id": "graphql", "method": "POST", "path": "/graphql"}],
            "tools": []
        }"#;
        let manifest: Manifest = serde_json::from_str(json).expect("manifest cũ phải parse");
        let route = &manifest.routes[0];
        assert_eq!(route.transport, Transport::Rest, "mặc định phải là Rest");
        assert_eq!(route.source_node, None, "không khai thì là None");
    }

    #[test]
    fn sse_route_deserializes_transport_and_source_node() {
        let json = r#"{
            "id": "ticker",
            "version": "0.1.0",
            "builtin": false,
            "prefix": "/api/ticker",
            "routes": [{
                "id": "stream",
                "method": "GET",
                "path": "/stream",
                "transport": "sse",
                "source_node": "market-out"
            }],
            "tools": []
        }"#;
        let manifest: Manifest = serde_json::from_str(json).expect("manifest SSE phải parse");
        let route = &manifest.routes[0];
        assert_eq!(route.transport, Transport::Sse);
        assert_eq!(route.source_node.as_deref(), Some("market-out"));
    }

    /// `transport` serialize ra lowercase — manifest là hợp đồng với host,
    /// không phải chi tiết implementation của enum.
    #[test]
    fn transport_serializes_lowercase() {
        assert_eq!(serde_json::to_string(&Transport::Rest).unwrap(), "\"rest\"");
        assert_eq!(serde_json::to_string(&Transport::Sse).unwrap(), "\"sse\"");
    }

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
