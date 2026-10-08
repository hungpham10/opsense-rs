//! Thin GraphQL client that talks to `opsense serve`'s `POST /graphql`.
//!
//! Every method is one HTTP call — no local state, no diffing, no
//! generation tracking. The server serialises writes internally via
//! `RwLock`, so the client doesn't need to worry about races.
//!
//! Kiểu ở đây là **DTO của riêng client**, không dùng lại kiểu lõi: `Observation`
//! vừa derive `Serialize/Deserialize` (tên field snake_case) vừa derive
//! `SimpleObject` (async-graphql đổi tên field sang camelCase). serde và
//! GraphQL của cùng một struct không thể cùng đúng, nên phải tách.

use std::collections::BTreeMap;
use std::collections::HashMap;

use opsense_core::TelemetryKind;
use opsense_core::{LogLevel, Signal};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::client::time_arg::TimeArg;

/// Một observation trả về từ `queryTimeseries`.
///
/// `metric_id` phải rename `metricId`: đây là **tên field trong JSON do GraphQL
/// trả về** (async-graphql camelCase hoá `SimpleObject`), không phải tên field
/// serde của `opsense_core::Observation` (`metric_id`). Dùng chung struct ⇒
/// decode hỏng với `missing field metric_id`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Observation {
    pub ts: i64,
    #[serde(rename = "metricId")]
    pub metric_id: String,
    pub kind: TelemetryKind,
    pub signal: Signal,
    pub value: f64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub severity: Option<LogLevel>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Response types (mirror of the GraphQL schema in api/repl/v1.rs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeSummary {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub inputs: Vec<String>,
    /// Có mặt ở đây là bắt buộc: selection set của `status()` ghi **tường minh**
    /// (`nodes { id type inputs description }`), nên quên field là MCP không thấy,
    /// chứ không phải lỗi schema. `default` vì `reload`/`patchComponent` cũng dùng
    /// `NodeSummary` và server có thể không gửi field này.
    #[serde(default)]
    pub description: Option<String>,
    /// Node còn chạy không — `false` cùng `last_error` là node chết.
    #[serde(default)]
    pub running: bool,
    /// Số lần báo lỗi: phân biệt lỗi tĩnh lặp với một lần rồi hết.
    ///
    /// `rename` là **bắt buộc**: đây là tên field trong JSON do async-graphql
    /// camelCase hoá, không phải tên field serde — cùng lớp lỗi với
    /// `Observation.metric_id`, và im lặng đọc thành 0.
    #[serde(rename = "faultCount", default)]
    pub fault_count: i64,
    /// Lỗi gần nhất; `None` khi node khoẻ. Đây là đường hỏi lỗi duy nhất sau
    /// khi container restart (`run()` phần lớn không trả `Err`).
    #[serde(rename = "lastError", default)]
    pub last_error: Option<NodeFault>,
}

/// Lỗi gần nhất của node. Tách riêng `NodeSummary` vì `reload`/`patchComponent`
/// cũng dùng `NodeSummary` mà **không** chọn field này.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeFault {
    /// `transient` | `corrupt` | `fatal`.
    pub severity: String,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub recovered: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StationSummary {
    pub id: String,
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Status {
    pub nodes: Vec<NodeSummary>,
    pub stations: Vec<StationSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EditResult {
    #[serde(rename = "reloaded")]
    pub reloaded: bool,
    pub nodes: Vec<NodeSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SetAttributeResult {
    pub ok: bool,
    /// `rename` bắt buộc: đây là tên field **trong JSON do GraphQL trả về**
    /// (async-graphql camelCase hoá), không phải tên field serde — cùng lớp lỗi
    /// với `Observation.metric_id`.
    #[serde(rename = "envOverrideActive")]
    pub env_override_active: bool,
}

/// Kết quả query có guard: luôn biết có bị cắt không.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryResult {
    pub observations: Vec<Observation>,
    pub truncated: bool,
    pub scanned: usize,
    /// Đơn vị server đã áp dụng (`asc`/`desc`). Default `"asc"` để client cũ đọc
    /// server mới vẫn decode được thay vì chết vì `missing field order`.
    #[serde(default = "asc_default")]
    pub order: String,
}

fn asc_default() -> String {
    "asc".to_string()
}

/// Cấu hình đang chạy của một component (`Query.components`).
/// Một station đã xoá — cùng shape với `Status.stations`.
///
/// `kind` giữ dạng `String` thay vì enum `StationKind` để client không phụ thuộc
/// vào tên biến thể enum bên server (đổi `rename_items` sau này không làm hỏng
/// client cũ).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClearedStation {
    pub id: String,
    pub kind: String,
}

/// Kết quả `clearStation` / `clearAllStations`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClearStationResult {
    pub cleared: Vec<ClearedStation>,
    #[serde(default)]
    pub failed: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComponentConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// JSON của typetag: `script_path`, `params`, … (cùng shape `ComponentInput`).
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GraphQLError {
    pub message: String,
    #[serde(default)]
    pub path: Option<Vec<String>>,
    #[serde(default)]
    pub extensions: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GqlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQLError>>,
}

impl<T> GqlResponse<T> {
    fn into_result(self) -> anyhow::Result<T> {
        if let Some(errors) = self.errors {
            let msg = errors
                .iter()
                .map(|e| {
                    // `path` + `extensions.code` là hai thứ duy nhất phân biệt
                    // được "lỗi ở đâu" với "lỗi gì"; bỏ chúng thì agent chỉ còn
                    // một câu tiếng Anh trần trụi.
                    let at = e
                        .path
                        .as_ref()
                        .map(|p| p.join("."))
                        .filter(|p| !p.is_empty())
                        .map(|p| format!("{p}: "))
                        .unwrap_or_default();
                    let code = e
                        .extensions
                        .as_ref()
                        .and_then(|x| x.get("code"))
                        .and_then(|c| c.as_str())
                        .map(|c| format!(" [{c}]"))
                        .unwrap_or_default();
                    format!("{at}{}{code}", e.message)
                })
                .collect::<Vec<_>>()
                .join("; ");
            anyhow::bail!("GraphQL error: {msg}");
        }
        self.data
            .ok_or_else(|| anyhow::anyhow!("no data in response"))
    }
}

/// Body lỗi HTTP có thể là cả stack trace / dump config — cắt để một lỗi không
/// nuốt tràn màn hình REPL.
fn truncate_body(text: &str) -> String {
    const MAX: usize = 2000;
    match text.char_indices().nth(MAX) {
        Some((idx, _)) => format!("{}…", text[..idx].trim()),
        None => text.trim().to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ComponentInput (matches the GraphQL input type)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentInput {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<Vec<String>>,
}

impl ComponentInput {
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            config: None,
            inputs: None,
        }
    }

    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_inputs(mut self, inputs: Vec<String>) -> Self {
        self.inputs = Some(inputs);
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Client
// ─────────────────────────────────────────────────────────────────────────────

pub struct OpsenseClient {
    endpoint: String,
    http: Client,
    /// Optional Bearer token (OAuth2 access_token). Khi None, request
    /// sẽ đi qua như guest (Nginx vẫn inject `X-User-Id = "guest"`).
    bearer: Option<String>,
}

impl OpsenseClient {
    pub fn new(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: endpoint.into(),
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            bearer: load_bearer_from_env(),
        })
    }

    /// Override Bearer token (dùng cho test hoặc sau khi device flow issue).
    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    /// Xoá Bearer (về guest mode).
    pub fn clear_bearer(&mut self) {
        self.bearer = None;
    }

    /// `root` là tên field gốc của operation (`status`, `components`, …).
    ///
    /// GraphQL trả `{"data": {"<root>": <giá trị>}}`, còn call site muốn chính
    /// `<giá trị>` đó. Deserialize thẳng `data` vào kiểu của call site ⇒ serde đi
    /// tìm field của kiểu đó ngay ở cấp `data` rồi báo "missing field" **dù
    /// response đúng** — lỗi này đã làm hỏng cả 7 query của client. Bóc `root` ở
    /// đây, một chỗ, thay vì bắt từng call site khai wrapper struct.
    async fn gql<Q, V>(&self, root: &str, query: &str, variables: V) -> anyhow::Result<Q>
    where
        for<'de> Q: serde::de::Deserialize<'de>,
        V: Serialize,
    {
        #[derive(Serialize)]
        struct Request<'a, V> {
            query: &'a str,
            variables: V,
        }
        let mut req = self
            .http
            .post(&self.endpoint)
            .json(&Request { query, variables });
        if let Some(token) = &self.bearer {
            req = req.bearer_auth(token);
        }
        // Deserialize `data` thành `Value` trước: vừa bóc được root field, vừa
        // để lỗi schema hiện dạng "thiếu field `X`" kèm vị trí, thay vì báo
        // "error decoding response body" trừng trơi.
        let resp = req.send().await?;
        let status = resp.status();
        // KHÔNG dùng `error_for_status()`: nó trả lỗi **không kèm body**, nên
        // REPL chỉ in `HTTP status client error (500 Internal Server Error)` —
        // mất sạch lý do server chết (thường là message của panik/validate).
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("HTTP {status}: {}", truncate_body(&text));
        }
        let request: GqlResponse<serde_json::Value> =
            serde_json::from_str(&text).map_err(|e| {
                anyhow::anyhow!("HTTP {status} nhưng body không phải JSON: {e}; body: {}", truncate_body(&text))
            })?;
        let data = request.into_result()?;
        let value = data
            .get(root)
            .ok_or_else(|| anyhow::anyhow!("response không có field gốc `{root}`: {data}"))?;
        Ok(serde_json::from_value(value.clone())?)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query methods
    // ─────────────────────────────────────────────────────────────────────────

    pub async fn status(&self) -> anyhow::Result<Status> {
        const QUERY: &str = r#"
            query {
                status {
                    nodes { id type inputs description running faultCount
                            lastError { severity code message recovered } }
                    stations { id kind }
                }
            }
        "#;
        self.gql("status", QUERY, ()).await
    }

    pub async fn attributes(&self) -> anyhow::Result<BTreeMap<String, String>> {
        const QUERY: &str = r#"query { attributes }"#;
        self.gql("attributes", QUERY, ()).await
    }

    /// Cấu hình đang chạy. Bỏ `id` → tất cả component.
    ///
    /// Đọc trước khi sửa: `reload` nhận danh sách đầy đủ nên phải biết cấu hình
    /// hiện tại, không thì sửa một param cũng phải gửi lại cả pipeline.
    pub async fn components(&self, id: Option<&str>) -> anyhow::Result<Vec<ComponentConfig>> {
        const QUERY: &str = r#"
            query($id: String) {
                components(id: $id) { id type inputs description config }
            }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            id: Option<&'a str>,
        }
        self.gql("components", QUERY, Vars { id }).await
    }

    /// Truy vấn observation của một `timeseries` station, có guard + filter.
    ///
    /// Server từ chối `limit`/cửa sổ vượt trần và lọc server-side theo `signal`
    /// (`"order"`, `"summary"`…) + `label_kind` (`"trading_step"`, `"snapshot"`…).
    /// `truncated = true` nghĩa là còn dữ liệu ngoài `limit`.
    ///
    /// Selection set phải ghi **đúng tên serde** của `opsense_core::Observation`
    /// (`metricId`, không phải `metric` — `metric_id` được rename camelCase) và
    /// phải có `kind`: field đó không `#[serde(default)]` nên thiếu là decode
    /// hỏng. `severity` có default nên bỏ được.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_station(
        &self,
        node: &str,
        from_ts: Option<TimeArg>,
        to_ts: Option<TimeArg>,
        limit: Option<i64>,
        signal: Option<&str>,
        label_kind: Option<&str>,
        status: Option<&str>,
        order: Option<&str>,
    ) -> anyhow::Result<QueryResult> {
        const QUERY: &str = r#"
            query($node: String!, $fromTs: Int, $toTs: Int, $limit: Int,
                  $signal: String, $labelKind: String, $status: String,
                  $order: String) {
                queryTimeseries(node: $node, fromTs: $fromTs, toTs: $toTs,
                                limit: $limit, signal: $signal, labelKind: $labelKind,
                                status: $status, order: $order) {
                    observations { ts metricId kind signal value labels }
                    truncated
                    scanned
                    order
                }
            }
        "#;
        // `rename_all` là **bắt buộc**: query khai biến `$fromTs`/`$toTs`/
        // `$labelKind` (camelCase, theo quy ước GraphQL) nhưng `derive(Serialize)`
        // mặc định dùng đúng tên field Rust (`from_ts`, …). Không rename thì các
        // biến đó không hề tới server, mà async-graphql coi biến không được cấp
        // là `None` chứ không báo lỗi — nên `opsense query` và MCP
        // `opsense_query_timeseries` **im lặng** bỏ qua cửa sổ thời gian và bộ lọc
        // `labelKind`, còn guard cửa sổ 30 ngày thì không bao giờ thấy cửa sổ
        // người dùng yêu cầu.
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Vars<'a> {
            node: &'a str,
            from_ts: Option<i64>,
            to_ts: Option<i64>,
            limit: Option<i64>,
            signal: Option<&'a str>,
            label_kind: Option<&'a str>,
            status: Option<&'a str>,
            order: Option<&'a str>,
        }
        // Neo "now" ở **client** (server vẫn nhận `Int`): đổi `fromTs`/`toTs`
        // sang scalar hỗn hợp sẽ phá client/test cũ khai `$fromTs: Int!`.
        let now = opsense_components::signal::now_secs();
        self.gql(
            "queryTimeseries",
            QUERY,
            Vars {
                node,
                from_ts: from_ts.map(|t| t.resolve(now)).transpose()?,
                to_ts: to_ts.map(|t| t.resolve(now)).transpose()?,
                limit,
                signal,
                label_kind,
                status,
                order,
            },
        )
        .await
    }

    /// Lệnh + cursor T+N của một station — parity với MCP `opsense_orders`.
    ///
    /// Gộp theo `order_id` và lọc `status` **server-side** (`Query.orders`):
    /// station append-only nên bản `open` cũ vẫn còn sau khi lệnh đóng, nên
    /// lọc ở client *trước* khi gộp sẽ trả một lệnh đã đóng là đang mở.
    #[allow(clippy::too_many_arguments)]
    pub async fn orders(
        &self,
        node: &str,
        status: Option<&str>,
        from_ts: Option<TimeArg>,
        to_ts: Option<TimeArg>,
        limit: Option<i64>,
        order: Option<&str>,
    ) -> anyhow::Result<QueryResult> {
        const QUERY: &str = r#"
            query($node: String!, $status: String, $fromTs: Int, $toTs: Int,
                  $limit: Int, $order: String) {
                orders(node: $node, status: $status, fromTs: $fromTs, toTs: $toTs,
                       limit: $limit, order: $order) {
                    observations { ts metricId kind signal value labels }
                    truncated
                    scanned
                    order
                }
            }
        "#;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Vars<'a> {
            node: &'a str,
            status: Option<&'a str>,
            from_ts: Option<i64>,
            to_ts: Option<i64>,
            limit: Option<i64>,
            order: Option<&'a str>,
        }
        let now = opsense_components::signal::now_secs();
        self.gql(
            "orders",
            QUERY,
            Vars {
                node,
                status,
                from_ts: from_ts.map(|t| t.resolve(now)).transpose()?,
                to_ts: to_ts.map(|t| t.resolve(now)).transpose()?,
                limit,
                order,
            },
        )
        .await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Mutation methods
    // ─────────────────────────────────────────────────────────────────────────

    /// Push full component list — add/update/remove đều qua đây.
    /// REPL client tính new list locally rồi call 1 lần.
    pub async fn reload(&self, components: Vec<ComponentInput>) -> anyhow::Result<EditResult> {
        const MUTATION: &str = r#"
            mutation($components: [ComponentInput!]!) {
                reload(components: $components) { reloaded nodes { id type inputs description } }
            }
        "#;
        #[derive(Serialize)]
        struct Vars {
            components: Vec<ComponentInput>,
        }
        self.gql("reload", MUTATION, Vars { components }).await
    }

    /// Sửa **một** thành phần của một node (vd `/params/sl_pct` → `0.02`).
    ///
    /// `value` là JSON literal. Server đọc cấu hình hiện tại, patch, validate
    /// toàn bộ danh sách qua typetag rồi mới reload — nên không cần gửi lại cả
    /// pipeline, và patch hỏng thì runtime giữ nguyên.
    pub async fn patch_component(
        &self,
        id: &str,
        path: &str,
        value: &str,
    ) -> anyhow::Result<EditResult> {
        const MUTATION: &str = r#"
            mutation($id: String!, $path: String!, $value: String!) {
                patchComponent(id: $id, path: $path, value: $value) { reloaded nodes { id type inputs description } }
            }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            id: &'a str,
            path: &'a str,
            value: &'a str,
        }
        self.gql("patchComponent", MUTATION, Vars { id, path, value }).await
    }

    pub async fn set_attribute(        &self,
        name: &str,
        value: &str,
    ) -> anyhow::Result<SetAttributeResult> {
        const MUTATION: &str = r#"
            mutation($name: String!, $value: String!) {
                setAttribute(name: $name, value: $value) { ok envOverrideActive }
            }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            name: &'a str,
            value: &'a str,
        }
        self.gql("setAttribute", MUTATION, Vars { name, value }).await
    }

    pub async fn remove_attribute(&self, name: &str) -> anyhow::Result<bool> {
        const MUTATION: &str = r#"
            mutation($name: String!) { removeAttribute(name: $name) }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            name: &'a str,
        }
        self.gql("removeAttribute", MUTATION, Vars { name }).await
    }

    /// Xoá sạch **một** station (RAM + storage). Node vẫn chạy và sẽ ghi lại
    /// dữ liệu mới tới — xoá có tác dụng, không phải xoá vĩnh viễn.
    pub async fn clear_station(&self, id: &str) -> anyhow::Result<ClearStationResult> {
        const MUTATION: &str = r#"
            mutation($id: String!) {
                clearStation(id: $id) { cleared failed }
            }
        "#;
        #[derive(Serialize)]
        struct Vars<'a> {
            id: &'a str,
        }
        self.gql("clearStation", MUTATION, Vars { id }).await
    }

    /// Xoá sạch **mọi** station đã đăng ký.
    pub async fn clear_all_stations(&self) -> anyhow::Result<ClearStationResult> {
        const MUTATION: &str = r#"
            mutation { clearAllStations { cleared failed } }
        "#;
        #[derive(Serialize)]
        struct NoVars {}
        self.gql("clearAllStations", MUTATION, NoVars {}).await
    }
}

/// Load Bearer token từ `OPSENSE_ACCESS_TOKEN` env var, hoặc file
/// `~/.config/opsense/token`. Trả None nếu không tìm thấy (guest mode).
fn load_bearer_from_env() -> Option<String> {
    if let Ok(token) = std::env::var("OPSENSE_ACCESS_TOKEN") {
        let trimmed = token.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    // File fallback
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::PathBuf::from(home)
            .join(".config")
            .join("opsense")
            .join("token");
        if let Ok(content) = std::fs::read_to_string(&path) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env là **process-wide** còn libtest chạy test **song song**, nên các test
    /// đụng `OPSENSE_ACCESS_TOKEN`/`HOME` phải khoá lại. Không khoá thì chúng đọc
    /// trúng env của nhau: `test_load_bearer_from_token_file` xoá
    /// `OPSENSE_ACCESS_TOKEN` đúng lúc `test_load_bearer_from_env` đang assert,
    /// và test đỏ theo lịch — chạy riêng module thì xanh, chạy cả crate thì đỏ
    /// (đã gặp đúng một lần trong `cargo test --workspace`).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// `OpsenseClient::new` parse endpoint, không panic khi host lạ.
    #[test]
    fn test_new_client_parses_endpoint() {
        let c = OpsenseClient::new("http://127.0.0.1:8080").unwrap();
        assert_eq!(c.endpoint, "http://127.0.0.1:8080");
    }

    /// `with_bearer` set bearer field.
    #[test]
    fn test_with_bearer() {
        let mut c = OpsenseClient::new("http://localhost").unwrap();
        c = c.with_bearer("tok-123");
        assert_eq!(c.bearer.as_deref(), Some("tok-123"));
        c.clear_bearer();
        assert!(c.bearer.is_none());
    }

    /// `load_bearer_from_env` đọc từ `OPSENSE_ACCESS_TOKEN` nếu có.
    ///
    /// Không xoá `~/.config/opsense/token`: env var được ưu tiên trước file nên
    /// xoá là thừa, và nó **xoá mất token thật của người dùng** mỗi lần chạy
    /// `cargo test --lib` — phải đăng nhập lại Dex sau đó. Test cũ còn dựa vào việc
    /// xoá file để "không bị pollute", nhưng file không ảnh hưởng kết quả assert.
    #[test]
    fn test_load_bearer_from_env() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: mọi test đụng env trong crate này đều giữ `ENV_LOCK`.
        unsafe { std::env::set_var("OPSENSE_ACCESS_TOKEN", "test-token-abc") };
        let loaded = load_bearer_from_env();
        unsafe { std::env::remove_var("OPSENSE_ACCESS_TOKEN") };
        assert_eq!(loaded.as_deref(), Some("test-token-abc"));
    }

    /// File fallback vẫn đọc được (không env var) — và phải **không** ghi file.
    ///
    /// Dùng `HOME` trỏ vào thư mục tạm để không đụng `~/.config/opsense/token` thật.
    #[test]
    fn test_load_bearer_from_token_file() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("opsense-token-{}", std::process::id()));
        std::fs::create_dir_all(dir.join(".config/opsense")).unwrap();
        std::fs::write(dir.join(".config/opsense/token"), "file-token-xyz\n").unwrap();
        let path = dir.join(".config/opsense/token");
        let before = std::fs::metadata(&path).unwrap().len();

        // SAFETY: mọi test đụng env trong crate này đều giữ `ENV_LOCK`.
        unsafe {
            std::env::remove_var("OPSENSE_ACCESS_TOKEN");
            std::env::set_var("HOME", &dir);
        }
        let loaded = load_bearer_from_env();
        let home = std::env::var("HOME").unwrap();
        unsafe { std::env::set_var("HOME", home) };

        assert_eq!(loaded.as_deref(), Some("file-token-xyz"), "phải trim newline");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            before,
            "đọc token không được ghi lại file"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Empty env var trả về None (trừ khi file fallback có giá trị).
    #[test]
    fn test_load_bearer_env_empty() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { std::env::remove_var("OPSENSE_ACCESS_TOKEN") };
        // Trừ khi dev có sẵn `~/.config/opsense/token` thì kết quả không None;
        // chỉ assert rằng hàm không panic và trả String rỗng được coi là None.
        let _ = load_bearer_from_env();
    }

    /// Hồi quy: `data` của GraphQL bọc root field, `gql` phải bóc ra.
    ///
    /// Trước khi có `root`, `gql` deserialize thẳng `data` vào kiểu call site nên
    /// **cả 8 query** của client trả `error decoding response body` dù server trả
    /// response đúng — `opsense mcp` không gọi được tool nào. Test này đóng vai
    /// server: body là của thật, chỉ thiếu bước bóc.
    #[test]
    fn gql_unwraps_root_field() {
        use std::io::Write;
        const BODY: &str = r#"{"data":{"status":{"nodes":[
            {"id":"tsdb","type":"Sink","inputs":["clock"],"description":"ghi observation"}
        ],"stations":[{"id":"tsdb","kind":"timeseries"}]},
        "components":[{"id":"clock","type":"Source","inputs":[],"description":"nhịp",
            "config":{"kind":"clock"}}]}}"#;

        // Server nhận 2 request (status + components) trên cùng listener.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut sock, _)) = listener.accept() else { return };
                // Đọc trọn request — xem `read_full_request`.
                let _ = read_full_request(&mut sock);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{BODY}",
                    BODY.len()
                );
                let _ = sock.write_all(resp.as_bytes());
            }
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let c = rt.block_on(async {
            let c = OpsenseClient::new(format!("http://127.0.0.1:{port}/api/repl/graphql")).unwrap();
            let st = c.status().await.expect("status phải bóc được root `status`");
            assert_eq!(st.nodes.len(), 1);
            assert_eq!(st.nodes[0].id, "tsdb");
            assert_eq!(st.nodes[0].description.as_deref(), Some("ghi observation"));
            assert_eq!(st.stations.len(), 1);

            let comps = c.components(None).await.expect("components phải bóc root `components`");
            assert_eq!(comps.len(), 1);
            assert_eq!(comps[0].id, "clock");
            assert_eq!(comps[0].kind, "Source", "`type` phải map vào `kind`");
        });
        let _ = c;
    }

    /// Hồi quy: tên field trong DTO phải là tên field **server thật trả về**.
    ///
    /// Không có `rename_all = "camelCase"` cho serde ở đây vì DTO phải giữ tên
    /// field Rust để đọc được trong code; chỉ field nào GraphQL camelCase hoá thì
    /// `rename` riêng. Hai chỗ đã sai theo kiểu này và cả hai đều làm tool chết
    /// với `missing field` dù response đúng: `Observation.metric_id` (server trả
    /// `metricId`) và `SetAttributeResult.env_override_active` (server trả
    /// `envOverrideActive`).
    ///
    /// Body dưới đây chép **nguyên văn** từ `opsense-serve` thật, nên test đỏ
    /// ngay nếu tên field lệch.
    #[test]
    fn dto_field_names_match_real_server_response() {
        use std::io::Write;

        // Ba body chép **nguyên văn** từ `opsense-serve` thật, theo đúng thứ tự
        // method dưới đây gọi.
        let bodies = [
            // mutation{setAttribute(name:"p",value:"1"){ok envOverrideActive}}
            r#"{"data":{"setAttribute":{"ok":true,"envOverrideActive":false}}}"#,
            // queryTimeseries(limit:1) — `metricId` camelCase.
            r#"{"data":{"queryTimeseries":{"observations":[
                 {"ts":1790512260,"metricId":"BTCUSDT","kind":"metric","signal":"summary",
                  "value":84970.58,"labels":{"grid_cell":"1"}}],
                 "truncated":false,"scanned":1}}}"#,
            // status — `type` -> `kind`, `description` optional.
            r#"{"data":{"status":{
                 "nodes":[{"id":"grid","type":"Transform","inputs":["tick-map"],
                           "description":"Đọc nến, dựng lưới"}],
                 "stations":[{"id":"grid","kind":"timeseries"}]}}}"#,
        ];
        let bodies: Vec<String> = bodies.iter().map(|s| s.to_string()).collect();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for body in bodies {
                let Ok((mut sock, _)) = listener.accept() else { return };
                // Đọc **trọn** request trước khi trả lời — xem `read_full_request`.
                let _ = read_full_request(&mut sock);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes());
            }
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let c = OpsenseClient::new(format!("http://127.0.0.1:{port}/api/repl/graphql")).unwrap();

            let r = c.set_attribute("p", "1").await.expect("setAttribute phải decode được");
            assert!(r.ok);
            assert!(!r.env_override_active, "envOverrideActive phải map vào `env_override_active`");

            let r = c.query_station("grid", None, None, Some(1), None, None, None, None)
                .await
                .expect("queryTimeseries phải decode được");
            assert_eq!(r.observations.len(), 1);
            assert_eq!(r.observations[0].metric_id, "BTCUSDT", "`metricId` phải map vào `metric_id`");
            assert_eq!(r.observations[0].labels.get("grid_cell").map(String::as_str), Some("1"));
            assert_eq!(r.scanned, 1);

            let r = c.status().await.expect("status phải decode được");
            assert_eq!(r.nodes[0].kind, "Transform", "`type` phải map vào `kind`");
            assert_eq!(r.nodes[0].description.as_deref(), Some("Đọc nến, dựng lưới"));
            assert_eq!(r.stations[0].kind, "timeseries");
        });
    }

    /// Đọc **trọn** một HTTP request từ socket: tới hết header `\r\n\r\n` **và**
    /// đủ `content-length` byte body.
    ///
    /// Một lần `read` là chưa đủ, và thiếu nó làm test đỏ theo lịch. Đã bắt
    /// được trên CI, chỉ ở `queryTimeseries` — vì đó là request có POST body
    /// lớn nhất:
    ///
    /// ```text
    /// queryTimeseries phải decode được: error sending request for url (...)
    ///   1: connection error
    ///   2: Connection reset by peer (os error 104)
    /// ```
    ///
    /// Server giả đọc một lần (chỉ đủ header), trả lời rồi drop socket **trong
    /// lúc client còn đang gý body** ⇒ client nhận RST, và lỗi báo ra là
    /// *connection*, không phải *decode* — dễ chẩn đoán nhầm thành lỗi DTO.
    fn read_full_request(sock: &mut std::net::TcpStream) -> std::io::Result<Vec<u8>> {
        use std::io::Read;
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 4096];
        // Vị trí ngay sau `\r\n\r\n` và số byte body còn phải chờ.
        let mut body_start: Option<usize> = None;
        let mut content_length = 0usize;
        loop {
            let n = sock.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if body_start.is_none() {
                let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let start = pos + 4;
                let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                content_length = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                body_start = Some(start);
            }
            // Kiểm tra **ngay trong lần lặt này**: request thường tới trong một
            // `read` duy nhất (header + body), nếu chờ lần lặt sau thì sẽ `read`
            // thêm một lần nữa và treo vì client đang chờ response.
            if let Some(start) = body_start
                && buf.len() >= start + content_length
            {
                break;
            }
        }
        Ok(buf)
    }

    /// `read_full_request` phải chờ đủ body, không phải chỉ tới hết header.
    ///
    /// Test này **tất định** (ngược lại với test trên): writer cố tình gửi
    /// header trước, body sau một độ trễ, và có ghi thêm một request thứ hai vào
    /// socket — nếu hàm trả về sớm ở mốc header thì hai request bị trộn làm một và
    /// assert bắt được.
    #[test]
    fn read_full_request_waits_for_the_whole_body() {
        use std::io::Write;
        use std::net::TcpListener as Listener;
        use std::time::Duration;

        let listener = Listener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let writer = std::thread::spawn(move || {
            let mut sock = std::net::TcpStream::connect(addr).expect("connect");
            sock.write_all(b"POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\n")
                .expect("headers");
            std::thread::sleep(Duration::from_millis(60));
            sock.write_all(b"hello world").expect("body");
            std::thread::sleep(Duration::from_millis(60));
            // Request thứ hai phải **không** lọt vào kết quả lần đọc đầu.
            sock.write_all(b"GET /y HTTP/1.1\r\nHost: h\r\n\r\n").expect("next");
            std::thread::sleep(Duration::from_millis(60));
        });

        let (mut sock, _) = listener.accept().expect("accept");
        sock.set_read_timeout(Some(Duration::from_secs(5))).expect("timeout");
        let got = read_full_request(&mut sock).expect("read");
        writer.join().expect("writer");

        let text = String::from_utf8_lossy(&got);
        assert!(text.contains("hello world"), "thiếu body: {got:?}");
        assert!(
            !text.contains("GET /y"),
            "lọt cả request thứ hai vào ⇒ hàm dừng sớm hơn hết body"
        );
    }

    /// Hồi quy: tên biến trong `variables` phải khớp `$…` khai trong query.
    ///
    /// `Vars` derive `Serialize` nên mặc định phát ra key `from_ts`/`to_ts`/
    /// `label_kind`, trong khi query khai `$fromTs`/`$toTs`/`$labelKind`. Lệch đó
    /// **không** báo lỗi: async-graphql coi biến không được cấp là `None`, nên
    /// `opsense query` và MCP `opsense_query_timeseries` âm thầm bỏ qua cửa sổ
    /// thời gian và bộ lọc `labelKind`, còn guard 30 ngày không bao giờ thấy cửa
    /// sổ người dùng yêu cầu. Test bắt request thật nên không để lệch này quay
    /// lại ở bất kỳ biến nào.
    #[test]
    fn query_variables_are_camel_case() {
        use std::io::Write;
        use std::sync::mpsc;

        let (tx, rx) = mpsc::channel::<String>();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            // Đọc trọn request: test assert trên *nội dung* request này, và đọc
            // một lần thì có thể cắt mất phần cuối của `variables`.
            let buf = read_full_request(&mut sock).unwrap_or_default();
            let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
            const BODY: &str =
                r#"{"data":{"queryTimeseries":{"observations":[],"truncated":false,"scanned":0}}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{BODY}",
                BODY.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let c = OpsenseClient::new(format!("http://127.0.0.1:{port}/api/repl/graphql")).unwrap();
            c.query_station(
                "grid",
                Some(TimeArg::Unix(111)),
                Some(TimeArg::Unix(222)),
                Some(9),
                None,
                Some("order"),
                None,
                None,
            )
                .await
                .expect("query");
        });
        let req = rx.recv().expect("request body");

        // Mọi biến query khai đều phải có mặt trong `variables` của request.
        for name in ["node", "fromTs", "toTs", "limit", "labelKind"] {
            assert!(
                req.contains(&format!("\"{name}\":")),
                "request phải có biến `{name}` — sai tên thì server âm thầm coi là None:\n{req}"
            );
        }
        // và không được dùng tên snake_case
        for name in ["from_ts", "to_ts", "label_kind"] {
            assert!(
                !req.contains(&format!("\"{name}\":")),
                "request vẫn còn key snake_case `{name}`:\n{req}"
            );
        }
        // giá trị phải đi kèm đúng chỗ
        assert!(req.contains("\"fromTs\":111"), "{req}");
        assert!(req.contains("\"toTs\":222"), "{req}");
        assert!(req.contains("\"labelKind\":\"order\""), "{req}");
    }

    /// Client **cũ** đọc server mới: thiếu `order` phải ra `"asc"` chứ không
    /// chết vì `missing field order`.
    #[test]
    fn query_result_order_defaults_to_asc() {
        let r: QueryResult =
            serde_json::from_str(r#"{"observations":[],"truncated":false,"scanned":0}"#)
                .expect("thiếu `order` vẫn phải decode");
        assert_eq!(r.order, "asc");
        let r: QueryResult =
            serde_json::from_str(r#"{"observations":[],"truncated":false,"scanned":0,"order":"desc"}"#)
                .expect("decode");
        assert_eq!(r.order, "desc");
    }

    /// `reload`/`patchComponent` dùng **cùng** `NodeSummary` mà không chọn field
    /// lỗi ⇒ `serde(default)` là bắt buộc, không có nó là decode hỏng.
    #[test]
    fn node_summary_tolerates_missing_fault_fields() {
        let n: NodeSummary =
            serde_json::from_str(r#"{"id":"grid","type":"rhai","inputs":["clock"]}"#)
                .expect("selection set cũ vẫn phải decode");
        assert!(!n.running);
        assert_eq!(n.fault_count, 0);
        assert!(n.last_error.is_none());
        let n: NodeSummary = serde_json::from_str(
            r#"{"id":"grid","type":"rhai","inputs":[],"running":false,"faultCount":7,
                "lastError":{"severity":"transient","code":"script_error","message":"boom"}}"#,
        )
        .expect("decode node lỗi");
        assert_eq!(n.fault_count, 7);
        let f = n.last_error.expect("lastError");
        assert_eq!(f.code, "script_error");
        assert_eq!(f.message, "boom");
    }

    /// `path` + `extensions.code` là thứ phân biệt "lỗi ở đâu" với "lỗi gì".
    #[test]
    fn graphql_error_carries_path_and_code() {
        let resp: GqlResponse<serde_json::Value> = serde_json::from_str(
            r#"{"data":null,"errors":[{"message":"boom","path":["queryTimeseries"],
                "extensions":{"code":"INTERNAL"}}]}"#,
        )
        .expect("decode");
        let err = resp.into_result().expect_err("phải báo lỗi");
        let msg = err.to_string();
        assert!(msg.contains("queryTimeseries"), "{msg}");
        assert!(msg.contains("INTERNAL"), "{msg}");
        assert!(msg.contains("boom"), "{msg}");
    }

    /// Root field sai phải báo đúng tên, không báo "missing field" mơ hồ.
    #[test]
    fn gql_reports_missing_root_by_name() {
        use std::io::Write;
        // Body không có `queryTimeseries` — mô phỏng gõ sai tên root ở call site.
        const BODY: &str = r#"{"data":{"somethingElse":{}}}"#;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            // Đọc trọn request — xem `read_full_request`.
            let _ = read_full_request(&mut sock);
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{BODY}",
                BODY.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let err = rt.block_on(async {
            let c = OpsenseClient::new(format!("http://127.0.0.1:{port}/api/repl/graphql")).unwrap();
            c.query_station("tsdb", None, None, Some(10), None, None, None, None)
                .await
                .expect_err("thiếu root phải báo lỗi")
        });
        let msg = err.to_string();
        assert!(
            msg.contains("queryTimeseries"),
            "lỗi phải nêu đúng tên root thiếu: {msg}"
        );
    }

    /// HTTP lỗi phải kèm **body**: `error_for_status()` chỉ trả
    /// `HTTP status client error (500 Internal Server Error)` ⇒ mất sạch lý do.
    #[test]
    fn gql_keeps_http_error_body() {
        use std::io::Write;
        const BODY: &str = r#"{"message":"boom: station grid is corrupt"}"#;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let _ = read_full_request(&mut sock);
            let resp = format!(
                "HTTP/1.1 500 Internal Server Error\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{BODY}",
                BODY.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let err = rt.block_on(async {
            let c =
                OpsenseClient::new(format!("http://127.0.0.1:{port}/api/repl/graphql")).unwrap();
            c.status().await.expect_err("HTTP 500 phải là lỗi")
        });
        let msg = err.to_string();
        assert!(msg.contains("500"), "thiếu status: {msg}");
        assert!(msg.contains("boom"), "thiếu body: {msg}");
    }

    /// `orders` phải cấp **đủ** biến: thiếu `rename_all` thì `fromTs`/`toTs`/
    /// `labelKind` không tới server và async-graphql coi là `None` — cửa sổ
    /// thời gian bị bỏ **im lặng**.
    #[test]
    fn orders_variables_are_camel_case() {
        use std::io::Write;
        use std::sync::mpsc;

        let (tx, rx) = mpsc::channel::<String>();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let buf = read_full_request(&mut sock).unwrap_or_default();
            let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
            const BODY: &str =
                r#"{"data":{"orders":{"observations":[],"truncated":false,"scanned":0,"order":"desc"}}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{BODY}",
                BODY.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let now = opsense_components::signal::now_secs();
        rt.block_on(async {
            let c =
                OpsenseClient::new(format!("http://127.0.0.1:{port}/api/repl/graphql")).unwrap();
            let out = c
                .orders(
                    "grid",
                    Some("open"),
                    Some(TimeArg::Expr("2h".into())),
                    Some(TimeArg::Unix(now)),
                    Some(5),
                    Some("asc"),
                )
                .await
                .expect("orders");
            assert_eq!(out.order, "desc");
        });
        let req = rx.recv().expect("request body");
        for name in ["node", "status", "fromTs", "toTs", "limit", "order"] {
            assert!(
                req.contains(&format!("\"{name}\":")),
                "orders phải có biến `{name}`:\n{req}"
            );
        }
        assert!(req.contains("\"fromTs\":"), "{req}");
        // `2h` phải được resolve thành unix giây trước khi gửi.
        assert!(!req.contains("\"2h\""), "chưa resolve TimeArg: {req}");
        assert!(req.contains("\"limit\":5"), "{req}");
        assert!(req.contains("\"order\":\"asc\""), "{req}");
    }
}
