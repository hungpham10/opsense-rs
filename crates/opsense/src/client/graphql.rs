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
    /// (`nodes { id type inputs }`), nên quên field là MCP không thấy, chứ không
    /// phải lỗi schema.
    #[serde(default)]
    pub description: Option<String>,
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
}

/// Cấu hình đang chạy của một component (`Query.components`).
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
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            anyhow::bail!("GraphQL error: {msg}");
        }
        self.data
            .ok_or_else(|| anyhow::anyhow!("no data in response"))
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
        let request: GqlResponse<serde_json::Value> =
            req.send().await?.error_for_status()?.json().await?;
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
                    nodes { id type inputs description }
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
        from_ts: Option<i64>,
        to_ts: Option<i64>,
        limit: Option<i64>,
        signal: Option<&str>,
        label_kind: Option<&str>,
        status: Option<&str>,
    ) -> anyhow::Result<QueryResult> {
        const QUERY: &str = r#"
            query($node: String!, $fromTs: Int, $toTs: Int, $limit: Int,
                  $signal: String, $labelKind: String, $status: String) {
                queryTimeseries(node: $node, fromTs: $fromTs, toTs: $toTs,
                                limit: $limit, signal: $signal, labelKind: $labelKind,
                                status: $status) {
                    observations { ts metricId kind signal value labels }
                    truncated
                    scanned
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
        }
        self.gql(
            "queryTimeseries",
            QUERY,
            Vars {
                node,
                from_ts,
                to_ts,
                limit,
                signal,
                label_kind,
                status,
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
                reload(components: $components) { reloaded nodes { id type inputs } }
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
                patchComponent(id: $id, path: $path, value: $value) { reloaded nodes { id type inputs } }
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
    #[test]
    fn test_load_bearer_from_env() {
        // Clear any existing token file that could pollute this test.
        if let Some(home) = std::env::var_os("HOME") {
            let token_path = std::path::PathBuf::from(home)
                .join(".config")
                .join("opsense")
                .join("token");
            let _ = std::fs::remove_file(&token_path);
        }
        // SAFETY: Test chạy đơn luồng, không race với threads khác.
        unsafe { std::env::set_var("OPSENSE_ACCESS_TOKEN", "test-token-abc") };
        let loaded = load_bearer_from_env();
        unsafe { std::env::remove_var("OPSENSE_ACCESS_TOKEN") };
        assert_eq!(loaded.as_deref(), Some("test-token-abc"));
    }

    /// Empty env var trả về None (trừ khi file fallback có giá trị).
    #[test]
    fn test_load_bearer_env_empty() {
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
        use std::io::{Read, Write};
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
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\n\r\n{BODY}",
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
        use std::io::{Read, Write};

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
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\n\r\n{body}",
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

            let r = c.query_station("grid", None, None, Some(1), None, None, None).await
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
        use std::io::{Read, Write};
        use std::sync::mpsc;

        let (tx, rx) = mpsc::channel::<String>();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut buf = [0u8; 8192];
            let n = sock.read(&mut buf).unwrap_or(0);
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
            const BODY: &str =
                r#"{"data":{"queryTimeseries":{"observations":[],"truncated":false,"scanned":0}}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\n\r\n{BODY}",
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
            c.query_station("grid", Some(111), Some(222), Some(9), None, Some("order"), None)
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

    /// Root field sai phải báo đúng tên, không báo "missing field" mơ hồ.
    #[test]
    fn gql_reports_missing_root_by_name() {
        use std::io::{Read, Write};
        // Body không có `queryTimeseries` — mô phỏng gõ sai tên root ở call site.
        const BODY: &str = r#"{"data":{"somethingElse":{}}}"#;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\n\r\n{BODY}",
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
            c.query_station("tsdb", None, None, Some(10), None, None, None)
                .await
                .expect_err("thiếu root phải báo lỗi")
        });
        let msg = err.to_string();
        assert!(
            msg.contains("queryTimeseries"),
            "lỗi phải nêu đúng tên root thiếu: {msg}"
        );
    }
}
