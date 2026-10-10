//! HTTP API của gateway.
//!
//! Route thật (xem `serve::routes`): `GET /health`, `POST /api/repl/graphql`,
//! `/api/admin/*`, `/api/oauth/*`. Không có `/reload`, `/sources` hay `/metrics`.
//! Đọc dữ liệu thì qua station: `Query.queryTimeseries` trong GraphQL, tức
//! `opsense query` / MCP tool; đổi cấu hình qua `Mutation.patchComponent`
//! hoặc `Mutation.reload`.

pub mod admin;
pub mod oauth;
pub mod prometheus;
pub mod repl;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::Mutex;

use async_graphql::SimpleObject;
use aws_sdk_s3::Client as S3Client;
use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;
use headers::Header;
use http::{HeaderName, HeaderValue};
use tokio::sync::RwLock;

use opsense_core::{Config, Context, Observation, StationKind};
use opsense_mlib::vector::components::{clock, null};
use opsense_mlib::vector::runtime::{Component, Event, Fault, Runtime, Severity};
use opsense_model::resolver::Resolver;
use opsense_model::secret::Secret;


use crate::api::oauth::OAuthMetrics;

/// Station chứa lịch sử thay đổi cấu hình (`labels.kind = "config_edit"`).
/// Đọc lại bằng `opsense query opsense-audit --label-kind config_edit`.
pub const AUDIT_STATION: &str = "opsense-audit";

#[derive(Debug)]
pub struct XTenantId(i64);

impl From<XTenantId> for i64 {
    fn from(tenant: XTenantId) -> Self {
        tenant.0
    }
}

impl Header for XTenantId {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static("x-tenant-id");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> std::result::Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values
            .next()
            .ok_or_else(headers::Error::invalid)?
            .to_str()
            .map_err(|_| headers::Error::invalid())?
            .parse::<i64>()
            .map_err(|_| headers::Error::invalid())?;

        Ok(XTenantId(value))
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let value = HeaderValue::from_str(&self.0.to_string()).unwrap();
        values.extend(std::iter::once(value));
    }
}

#[derive(Clone)]
pub struct AppState {
    s3: Arc<S3Client>,
    secret: Arc<Secret>,
    connector: Arc<Resolver>,
    context: Arc<Context>,
    runtime: Arc<RwLock<Runtime>>,
    admin_entity: Arc<opsense_model::entities::admin::Admin>,
    oauth_metrics: Arc<OAuthMetrics>,
    prometheus: axum_prometheus::Handle,
    pub prometheus_config: opsense_core::config::PrometheusConfig,
    // Set của key series đã thấy ở scrape trước — dùng để detect staleness
    stale_keys: Arc<Mutex<BTreeSet<String>>>,
}

impl AppState {
    pub async fn new(config: &Config) -> Result<Self, Error> {
        // `Resolver` pool dùng `sqlx::any`, nên driver phải được đăng ký trước.
        // Gọi ở đây (idempotent, `Once`) thay vì chỉ trong `main()` để crate này
        // tự đủ: embedder khác — integration test hay binary sau này — gọi
        // `AppState::new` mà không có `main` sẽ vỡ với
        // "No drivers installed. Please see the documentation in `sqlx::any`".
        sqlx::any::install_default_drivers();

        let runtime = Arc::new(RwLock::new(Runtime::new()));
        let secret = Arc::new(Secret::new().await?);
        let context = Arc::new(Context::new(config, secret.clone()));
        let connector = Arc::new(Resolver::new(secret.clone()).await?);

        let admin_entity = Arc::new(opsense_model::entities::admin::Admin::new(&connector));
        let oauth_metrics = Arc::new(OAuthMetrics::new());

        // Prometheus handle: pair() cài global recorder và trả về handle để render.
        // Chỉ được gọi **một lần** ở đây — gọi lần 2 sẽ panic.
        let (_, prometheus_handle) = axum_prometheus::PrometheusMetricLayer::pair();

        // Clone **trước** khi  bị shadow bởi write guard bên dưới —
        // handler cần `Arc`, không phải guard.
        let runtime_arc = Arc::clone(&runtime);
        {
            let mut runtime = runtime.write().await;

            // Mô tả node phải có mặt **trước** lần `status()` đầu tiên, và
            // `runtime.reload` có thể chạy script đọc context ngay trong
            // `prepare` — nên đặt trước cả `set_context`.
            context.set_node_descriptions(node_descriptions(config)).await;

            runtime.set_context(context.clone());
            runtime
                .reload(
                    pipeline_from_config(config)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                )
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
            // Không chỉ `println!`: lỗi phải **hỏi được** bằng `opsense_status`.
            // `println!` mất ngay khi container restart, và node vẫn hiện là
            // "đang chạy" trong status dù đã chết — nhầm lẫn rất dễ.
            //
            // Handler **không** giữ write guard: chỉ `read()` khoá lấy tên node
            // rồi thả, và chỉ khi có `Major`/`Panic`. Nó chạy ở task riêng nên
            // chờ guard ở khối này thả là bình thường, không phải deadlock.
            //
            // `Event::Minor` không ghi vào `last_error`: đó là lỗi bố trí (batch
            // lệch nối,…) còn pipeline vẫn chạy — ghi thành lỗi node sẽ ám mọi
            // node chỉ vì có một batch lệch.
            let handle = Arc::clone(&runtime_arc);
            runtime.start(move |event| {
                let handle = Arc::clone(&handle);
                async move {
                    // Lấy idx + loại trước: `event` bị `match` bên dưới.
                    let recovered = matches!(event, Event::Recovered(_));
                    let (idx, fault) = match &event {
                        Event::Recovered(i) => (*i, None),
                        Event::Minor((i, e)) => (
                            *i,
                            Some(Fault::new(Severity::Transient, "minor", e.to_string())),
                        ),
                        // Lỗi của chính engine: nó chỉ retry được chứ không sửa
                        // được, nên xếp `Fatal` — cần người/can thiệp.
                        Event::Major((i, e)) => (
                            *i,
                            Some(Fault::new(Severity::Fatal, "major", e.to_string())),
                        ),
                        Event::Panic((i, e)) => (
                            *i,
                            Some(Fault::new(Severity::Fatal, "panic", e.to_string())),
                        ),
                        Event::Fault((i, f)) => (*i, Some(f.clone())),
                    };
                    match event {
                        Event::Minor((id, error)) => println!("Minor error in node {id}: {error}"),
                        Event::Major((id, error)) => println!("Major error in node {id}: {error}"),
                        Event::Panic((id, error)) => println!("Panic in node {id}: {error}"),
                        Event::Fault((id, f)) => println!("Fault in node {id}: {}", f.summary()),
                        Event::Recovered(id) => println!("Node {id} recovered"),
                    }
                    // `Recovered` mang `None` ⇒ xoá `last_error`. Không suy được
                    // từ "im lặng" vì im lặng cũng là trạng thái hợp lệ (node
                    // chạy tốt ngay từ đầu), nên phải nói rõ.
                    if (fault.is_some() || recovered)
                        && let Some(name) = handle.read().await.node_name(idx)
                    {
                        handle.read().await.report_fault(&name, fault);
                    }
                }
            })?;
        }

        Ok(Self {
            s3: connector.s3(),
            context,
            runtime,
            admin_entity,
            oauth_metrics,
            secret,
            connector,
            prometheus: axum_prometheus::Handle(prometheus_handle),
            prometheus_config: config.prometheus.clone(),
            stale_keys: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    pub async fn stop(&self) -> Result<(), Error> {
        self.runtime.read().await.stop()
    }

    /// Chờ station `id` **đăng ký xong** — tức node tương ứng đã qua bước `prepare`
    /// và tự `registry()` vào context.
    ///
    /// `AppState::new` **không** chờ việc đó: engine khởi động từng node một, nên
    /// ngay sau khi `new` trả về thì station cuối cùng trong graph chưa chắc đã
    /// có. Đo được trên CI (`trading_orders_readable_via_graphql` fail, 3 test
    /// kia xanh): lần query đầu tiên trả `Station 'grid' not found` trong khi log
    /// engine vẫn đang *"prepare completed ok for tick-map"*. Cửa sổ này rộng
    /// hơn hẳn khi máy chậm — cùng log có
    /// `Error during connect to database: pool timed out`.
    ///
    /// Cổng này cho caller (và test) chờ đúng thứ cần chờ, thay vì mỗi nơi tự
    /// phát minh cách "thử lại và hy vọng".
    ///
    /// Trả `false` khi hết thời gian mà station vẫn chưa có.
    pub async fn wait_for_station(
        &self,
        id: &str,
        timeout: std::time::Duration,
    ) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.context.has_station(id).await {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                tracing::warn!(
                    station = %id,
                    "wait_for_station: hết thời gian mà station chưa đăng ký"
                );
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    pub async fn wait_for_shutdown(&self) -> Result<(), Error> {
        self.runtime.read().await.wait_for_shutdown().await
    }

    pub async fn variable<T>(&self, variable: &str) -> Result<T, Error>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        self.context.variable(variable).await
    }

    pub async fn station<T>(&self, station: &str) -> Result<T, Error>
    where
        T: for<'a> TryFrom<&'a opsense_core::Station, Error = Error>,
    {
        self.context.station(station).await
    }

    pub async fn set_attribute(&self, name: String, value: String) {
        self.context.set_attribute(name, value).await
    }

    pub async fn remove_attribute(&self, name: &str) -> bool {
        self.context.remove_attribute(name).await
    }

    /// Xoá sạch một station (RAM + storage). `Err` nếu không có station đó.
    pub async fn clear_station(&self, id: &str) -> Result<StationKind, Error> {
        self.context.clear_station(id).await
    }

    /// Xoá sạch mọi station đã đăng ký. Trả `(xoá được, lỗi từng cái)`.
    pub async fn clear_all_stations(&self) -> Result<(Vec<(String, StationKind)>, Vec<String>), Error> {
        self.context.clear_all_stations().await
    }

    fn default_pipeline(cfg: &Config) -> Vec<Arc<dyn Component>> {
        let interval_secs = cfg.engine.poll_interval_seconds.max(1);
        vec![
            Arc::new(clock::Clock {
                id: "clock".to_string(),
                interval_secs,
            }) as Arc<dyn Component>,
            Arc::new(null::Null {
                id: "null".to_string(),
                inputs: vec!["clock".to_string()],
            }) as Arc<dyn Component>,
        ]
    }
}

/// Build the pipeline component graph from `[pipeline]` (or the default
/// `clock -> null` graph when absent). Deserializes every component through
/// the typetag registry — this is where configs fail with e.g.
/// `unknown variant 'timeseries_station_sink'` when a component crate is not
/// linked into the binary. Used by [`AppState::new`] and exposed so
/// `opsense validate` catches the same failures before serve starts.
pub fn pipeline_from_config(cfg: &Config) -> Result<Vec<Arc<dyn Component>>, Error> {
    match &cfg.pipeline {
        Some(p) if !p.components.is_empty() => p
            .components
            .iter()
            .map(|value| {
                serde_json::from_value::<Box<dyn Component>>(strip_description(value.clone()))
                    .map(Arc::from)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::BrokenPipe,
                            format!("component `{value}`: {e}"),
                        )
                    })
            })
            .collect(),
        _ => Ok(AppState::default_pipeline(cfg)),
    }
}

/// Bỏ `description` khỏi JSON của component **trước** khi typetag deserialize.
///
/// Không bắt buộc — serde mặc định bỏ qua field lạ — nhưng macro đặt
/// `#[serde(deny_unknown_fields)]` lên mọi component struct
/// (`opsense-macros/src/configurable_component.rs:218`), nên `description` còn
/// trong JSON sẽ làm hỏng **mọi** node chứ không chỉ node đó. Bóc ở đây thay
/// vì sửa macro, vì `description` là metadata của deployment chứ không phải
/// field của code.
fn strip_description(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        obj.remove("description");
    }
    value
}

/// `id` → `description` từ `[[pipeline.components]]`.
///
/// Hàm thuần trên mảng JSON thay vì trên `Config`, để test được không cần
/// parser TOML — và để `pipeline_from_config` / `node_descriptions` chắc chắn
/// đọc **cùng một** nguồn, không lệch nhau.
#[must_use]
pub fn node_descriptions_from(components: &[serde_json::Value]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for value in components {
        let Some(obj) = value.as_object() else {
            continue;
        };
        let (Some(id), Some(desc)) = (
            obj.get("id").and_then(serde_json::Value::as_str),
            obj.get("description").and_then(serde_json::Value::as_str),
        ) else {
            continue;
        };
        let desc = desc.trim();
        if !desc.is_empty() {
            out.insert(id.to_string(), desc.to_string());
        }
    }
    out
}

/// `id` → `description` của pipeline trong config.
#[must_use]
pub fn node_descriptions(cfg: &Config) -> BTreeMap<String, String> {
    cfg.pipeline
        .as_ref()
        .map_or_else(BTreeMap::new, |p| node_descriptions_from(&p.components))
}

pub async fn health_check(State(_): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

/// Handler cho `/metrics` endpoint — render Prometheus text format.
/// Chỉ được gọi khi `app_state.prometheus_config.enabled == true`.
pub async fn prometheus_handler(State(state): State<AppState>) -> impl IntoResponse {
    if !state.prometheus_config.enabled {
        return (
            [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            "".to_string(),
        );
    }

    let mut series = Vec::new();

    // Tầng 1: Node health metrics
    let topology = state.runtime.read().await.topology();
    let now = opsense_components::signal::now_secs();
    series.extend(crate::api::prometheus::collect_nodes(&topology, now));

    // Tầng 2: Station metrics (opt-in qua config)
    let prom_cfg = &state.prometheus_config;
    for st in &prom_cfg.stations {
        if !st.enabled {
            continue;
        }
        // Window per station (nếu khai báo) — fallback global.
        let window_secs = st.window_secs.unwrap_or(prom_cfg.default_window_secs).max(1);
        if let Ok(station) = state
            .context
            .station::<Arc<RwLock<opsense_core::TimeseriesStation>>>(&st.station)
            .await
        {
            let obs = {
                let station = station.read().await;
                station
                    .query_recent(now - window_secs as i64, now)
                    .await
                    .unwrap_or_default()
            };
            series.extend(crate::api::prometheus::collect_station(&obs, st, now as i64));
        }
    }

    // Tầng 4: OAuth metrics
    series.extend(crate::api::prometheus::collect_oauth(&state.oauth_metrics.snapshot()));

    // Render HTTP metrics từ axum-prometheus layer
    let mut output = state.prometheus.0.render();

    // Render custom series
    let (series, dropped) = crate::api::prometheus::cap_series(
        series,
        state.prometheus_config.max_series as usize,
    );
    let custom = crate::api::prometheus::render_prometheus(&series, dropped);
    output.push_str(&custom);

    // Staleness: so sánh key cũ/mới, set NaN cho key biến mất
    let mut stale = state.stale_keys.lock().unwrap();
    let current_keys: BTreeSet<String> = series.iter().map(crate::api::prometheus::series_key).collect();
    for old in stale.iter() {
        if !current_keys.contains(old) {
            output.push_str(&format!(
                "# TYPE opsense_prometheus_stale gauge\nopsense_prometheus_stale{{key=\"{}\"}} NaN\n",
                old.replace('"', "\\\"")
            ));
        }
    }
    *stale = current_keys;

    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared GraphQL types + helpers (used by repl/v1.rs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(SimpleObject, Clone, Debug)]
pub struct Node {
    pub id: String,

    #[graphql(name = "type")]
    pub kind: String,

    pub inputs: Vec<String>,

    /// Node còn chạy không. `false` + `lastError` = chết, và `lastError` nói
    /// chết vì sao.
    pub running: bool,

    /// Số lần node báo lỗi. Phân biệt "lỗi tĩnh lặp mỗi nến" với "một lần rồi
    /// hết" — cùng một `lastError` nhưng sức nặng hoàn toàn khác.
    pub fault_count: i64,

    /// Lỗi gần nhất, `None` khi node khoẻ.
    pub last_error: Option<NodeFault>,

    /// Mô tả do người viết pipeline khai (`description` trong
    /// `[[pipeline.components]]`), nói node này **chứa dữ liệu gì** — thứ mà
    /// `type` không nói được. `None` khi node không khai mô tả.
    ///
    /// Đây là đường để LLM (qua MCP `opsense_status`) biết nên query station
    /// nào, hỏi `signal`/`labels` nào, thay vì đoán tên node.
    pub description: Option<String>,
}

/// Lỗi gần nhất của node — `None` khi node khoẻ.
#[derive(SimpleObject, Clone, Debug)]
pub struct NodeFault {
    /// `transient` | `corrupt` | `fatal`.
    pub severity: String,
    /// Mã ổn định để gom theo thứ, không phải message tiếng Anh đầy đủ.
    pub code: String,
    pub message: String,
    /// `None` = chưa/làm không được; `Some` = hành động đã tự áp dụng.
    pub recovered: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct Station {
    pub id: String,
    pub kind: StationKind,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct Status {
    pub nodes: Vec<Node>,
    pub stations: Vec<Station>,
}

impl AppState {
    /// Snapshot of the runtime topology + station registry.
    /// Used by `Query.status` and by mutations that need a post-edit node list.
    pub async fn status(&self) -> Status {
        let runtime = self.runtime.read().await;
        let topology = runtime.topology();
        // Mô tả đọc **ngoài** khoá runtime: nó nằm ở `Context`, không phải ở
        // node. Nếu đọc trong khoá này thì `Context` (lock riêng) sẽ bị khoá
        // runtime giữ suốt lúc await.
        let descriptions = self.context.node_descriptions().await;

        let nodes = topology
            .into_iter()
            .map(|n| Node {
                description: descriptions.get(&n.id).cloned(),
                id: n.id,
                kind: n.component_type,
                inputs: n.inputs,
                running: n.running,
                // `u64` → GraphQL `Int` (`i64`): giá trị vượt `i64::MAX` không
                // thể xảy ra với số lần lỗi, nhưng `try_from` để không bao giờ
                // làm hỏng cả `status` vì một con số.
                fault_count: i64::try_from(n.fault_count).unwrap_or(i64::MAX),
                // Không vứt: `run()` của hầu hết component không trả `Err` khi
                // hỏng (script lỗi chỉ `warn!` rồi bỏ batch) nên đây là đường
                // hỏi lỗi **duy nhất** sau khi container restart.
                last_error: n.last_error.map(|f| NodeFault {
                    severity: f.severity.as_str().to_string(),
                    code: f.code,
                    message: f.message,
                    recovered: f.recovered,
                }),
            })
            .collect();

        let stations = self
            .context
            .stations()
            .await
            .into_iter()
            .map(|(id, kind)| Station { id, kind })
            .collect();

        Status { nodes, stations }
    }

    /// Snapshot of every in-memory attribute.
    pub async fn attributes(&self) -> BTreeMap<String, String> {
        self.context.get_attributes().await
    }

    /// Cấu hình **đang chạy** của từng component, dạng JSON qua typetag.
    ///
    /// Nhờ đây client (REPL/CLI/MCP) đọc được `params` hiện tại của một node rồi
    /// sửa đúng một chỗ (xem `Mutation.patchComponent`) thay vì phải dựng lại cả
    /// danh sách component — đọc trước, sửa sau, không đoán.
    pub async fn components(&self, id: Option<&str>) -> Vec<serde_json::Value> {
        let runtime = self.runtime.read().await;
        let all = runtime.components();
        match id {
            None => all,
            Some(id) => all
                .into_iter()
                .filter(|c| c.get("id").and_then(serde_json::Value::as_str) == Some(id))
                .collect(),
        }
    }

    /// Ghi 1 dòng audit vào station [`AUDIT_STATION`].
    ///
    /// Đổi cấu hình mà không để lại dấu vết thì không ai biết chuyện gì vừa xảy
    /// ra. Audit cũng là **observation**, nên đọc lại được bằng đúng đường đọc
    /// của mọi state khác:
    /// `opsense query opsense-audit --label-kind config_edit`.
    ///
    /// Best-effort: audit hỏng không được làm hỏng thao tác đã thành công.
    pub async fn audit(&self, obs: Observation) {
        const ID: &str = AUDIT_STATION;
        let station = match self
            .context
            .station::<Arc<RwLock<opsense_core::TimeseriesStation>>>(ID)
            .await
        {
            Ok(s) => s,
            Err(_) => {
                let made = match opsense_core::TimeseriesStation::from_storage(ID, self.context.storage()).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "không tạo được station audit");
                        return;
                    }
                };
                let wrapped = Arc::new(RwLock::new(made));
                if let Err(e) = self
                    .context
                    .registry(ID, opsense_core::Station::Timeseries(wrapped.clone()))
                    .await
                {
                    // `AlreadyExists` = có người tạo trước → lấy lại từ context.
                    if e.kind() != ErrorKind::AlreadyExists {
                        tracing::warn!(error = %e, "không đăng ký được station audit");
                        return;
                    }
                }
                match self
                    .context
                    .station::<Arc<RwLock<opsense_core::TimeseriesStation>>>(ID)
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "không lấy được station audit");
                        return;
                    }
                }
            }
        };
        let ts = obs.ts;
        let st = station.read().await;
        st.update_range(std::slice::from_ref(&obs), ts, ts, ts);
    }
}

#[cfg(test)]
mod description_tests {
    use super::{node_descriptions, node_descriptions_from, pipeline_from_config, strip_description};
    use opsense_core::Config;
    use serde_json::json;

    fn parse(toml: &str) -> Config {
        toml::from_str(toml).expect("config phải parse")
    }

    /// `description` phải **không** làm hỏng deserialize.
    ///
    /// Đây là test khoá cho cả tính năng lẫn lý do nó phải là metadata chứ không
    /// phải field của struct: macro `#[source]/#[transform]/#[sink]` đặt
    /// `#[serde(deny_unknown_fields)]` lên **mọi** component struct
    /// (`opsense-macros/src/configurable_component.rs:218`), nên nếu ai đó xoá
    /// `strip_description` thì **mọi** node trong **mọi** config chết — chứ
    /// không chỉ node nào có mô tả. Test đi từ TOML thật cho tới
    /// `Runtime::reload`, tức đúng đường lúc boot.
    #[test]
    fn description_does_not_break_component_deserialize() {
        let cfg = parse(
            r#"
[[pipeline.components]]
type = "clock"
id = "clock"
interval_secs = 10
description = "nhịp thời gian"
"#,
        );
        let comps = pipeline_from_config(&cfg).expect("pipeline phải build được");
        assert_eq!(comps.len(), 1, "node có description vẫn phải build được");
    }

    /// Mô tả phải lấy được từ TOML, còn node không khai thì không có entry.
    #[test]
    fn reads_description_from_config() {
        let cfg = parse(
            r#"
[[pipeline.components]]
type = "clock"
id = "clock"
description = "nhịp thời gian"

[[pipeline.components]]
type = "null"
id = "sink"
inputs = ["clock"]
"#,
        );
        let map = node_descriptions(&cfg);
        assert_eq!(map.get("clock").map(String::as_str), Some("nhịp thời gian"));
        assert!(
            !map.contains_key("sink"),
            "node không khai description thì không có entry, không được điền rỗng"
        );
    }

    /// Mô tả trắng coi như không khai — nếu giữ chuỗi rỗng thì client phân biệt
    /// không được "chưa ai mô tả" với "mô tả rỗng".
    #[test]
    fn blank_description_is_treated_as_absent() {
        let map = node_descriptions_from(&[
            json!({ "id": "a", "description": "   " }),
            json!({ "id": "b", "description": "" }),
            json!({ "id": "c", "description": 7 }),
            json!({ "description": "không có id" }),
            json!("không phải object"),
        ]);
        assert!(map.is_empty(), "mô tả trắng/sai kiểu phải bị bỏ: {map:?}");
    }

    /// `description` phải bị bóc khỏi JSON, **không** lọt vào component.
    #[test]
    fn strip_description_removes_only_that_key() {
        let out = strip_description(json!({
            "type": "clock", "id": "clock", "interval_secs": 10, "description": "x",
        }));
        assert!(out.get("description").is_none());
        assert_eq!(out["id"], "clock");
        assert_eq!(out["interval_secs"], 10);
    }

    /// Không có `[pipeline]` thì không vỡ — `AppState` có pipeline mặc định
    /// (`clock → null`) và cũng không có mô tả nào.
    #[test]
    fn no_pipeline_yields_no_descriptions() {
        let cfg = parse("[engine]
poll_interval_seconds = 10
");
        assert!(node_descriptions(&cfg).is_empty());
        assert_eq!(
            pipeline_from_config(&cfg).expect("default build được").len(),
            2
        );
    }
}
