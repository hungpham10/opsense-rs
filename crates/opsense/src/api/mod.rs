//! HTTP API: health, config reload, and the source-health surface.
//!
//! `GET /sources` returns source health; `/health`, `/reload` and `/metrics`
//! are carried over from the original skeleton. Observation data is queried
//! through the stores instead — MCP `opsense_query` or a station_sink's
//! `/observations` endpoint.

pub mod admin;
pub mod oauth;
pub mod repl;

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use async_graphql::SimpleObject;
use axum::extract::State;
use axum::Json;
use headers::Header;
use http::{HeaderName, HeaderValue};
use aws_sdk_s3::Client as S3Client;
use tokio::sync::RwLock;

use opsense_core::{Config, Context, StationKind};
use opsense_libs::vector::components::{clock, null};
use opsense_libs::vector::runtime::{Component, Event, Runtime};
use opsense_model::secret::Secret;
use opsense_model::resolver::Resolver;

use crate::api::oauth::OAuthMetrics;

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
                    Self::pipeline_from_config(config)
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

    fn pipeline_from_config(cfg: &Config) -> Result<Vec<Arc<dyn Component>>, Error> {
        match &cfg.pipeline {
            Some(p) if !p.components.is_empty() => p
                .components
                .iter()
                .map(|value| {
                    serde_json::from_value::<Box<dyn Component>>(value.clone())
                        .map(Arc::from)
                        .map_err(|e| {
                            Error::new(ErrorKind::BrokenPipe, format!("component `{value}`: {e}"))
                        })
                })
                .collect(),
            _ => Ok(Self::default_pipeline(cfg)),
        }
    }

    /// Build a minimal default pipeline (`clock -> null`) when no `[pipeline]`
    /// section is present in the config. This matches the documented behaviour
    /// promised in `opsense_core::Config`:
    ///   "when absent a default `clock -> ingest -> processor -> persist` graph
    ///   is built from `engine.poll_interval_seconds`"
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

pub async fn health_check(State(_): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared GraphQL types + helpers (used by repl/v1.rs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(SimpleObject, Clone, Debug)]
pub struct NodeSummary {
    pub id: String,

    #[graphql(name = "type")]
    pub kind: String,

    pub inputs: Vec<String>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct StationSummary {
    pub id: String,
    pub kind: StationKind,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct Status {
    pub nodes: Vec<NodeSummary>,
    pub stations: Vec<StationSummary>,
}

impl AppState {
    /// Snapshot of the runtime topology + station registry.
    /// Used by `Query.status` and by mutations that need a post-edit node list.
    pub async fn status(&self) -> Status {
        let runtime = self.runtime.read().await;
        let topology = runtime.topology();

        let nodes = topology
            .into_iter()
            .map(|n| NodeSummary {
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
            .map(|(id, kind)| StationSummary { id, kind })
            .collect();

        Status { nodes, stations }
    }

    /// Snapshot of every in-memory attribute.
    pub async fn attributes(&self) -> BTreeMap<String, String> {
        self.context.get_attributes().await
    }
}
