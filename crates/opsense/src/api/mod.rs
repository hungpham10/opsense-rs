//! HTTP API của gateway.
//!
//! Route thật (xem `serve::routes`): `GET /health`, `POST /api/repl/graphql`,
//! `/api/admin/*`, `/api/oauth/*`. Không có `/reload`, `/sources` hay `/metrics`.
//! Đọc dữ liệu thì qua station: `Query.queryTimeseries` trong GraphQL, tức
//! `opsense query` / MCP tool; đổi cấu hình qua `Mutation.patchComponent`
//! hoặc `Mutation.reload`.

pub mod admin;
pub mod oauth;
pub mod repl;

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use async_graphql::SimpleObject;
use aws_sdk_s3::Client as S3Client;
use axum::Json;
use axum::extract::State;
use headers::Header;
use http::{HeaderName, HeaderValue};
use tokio::sync::RwLock;

use opsense_core::{Config, Context, Observation, StationKind};
use opsense_mlib::vector::components::{clock, null};
use opsense_mlib::vector::runtime::{Component, Event, Runtime};
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
}

impl AppState {
    pub async fn new(config: &Config) -> Result<Self, Error> {
        let runtime = Arc::new(RwLock::new(Runtime::new()));
        let secret = Arc::new(Secret::new().await?);
        let context = Arc::new(Context::new(config, secret.clone()));
        let connector = Arc::new(Resolver::new(secret.clone()).await?);

        let admin_entity = Arc::new(opsense_model::entities::admin::Admin::new(&connector));
        let oauth_metrics = Arc::new(OAuthMetrics::new());

        {
            let mut runtime = runtime.write().await;

            runtime.set_context(context.clone());
            runtime
                .reload(
                    pipeline_from_config(config)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                )
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
            runtime.start(|event| async move {
                match event {
                    Event::Minor((id, error)) => println!("Minor error in node {id}: {error}"),
                    Event::Major((id, error)) => println!("Major error in node {id}: {error}"),
                    Event::Panic((id, error)) => println!("Panic in node {id}: {error}"),
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
        })
    }

    pub async fn stop(&self) -> Result<(), Error> {
        self.runtime.read().await.stop()
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
                serde_json::from_value::<Box<dyn Component>>(value.clone())
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

pub async fn health_check(State(_): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
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

        let nodes = topology
            .into_iter()
            .map(|n| Node {
                id: n.id,
                kind: n.component_type,
                inputs: n.inputs,
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
