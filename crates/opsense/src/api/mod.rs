//! HTTP API: health, config reload, and the source-health surface.
//!
//! `GET /sources` returns source health; `/health`, `/reload` and `/metrics`
//! are carried over from the original skeleton. Observation data is queried
//! through the stores instead — MCP `opsense_query` or a station_sink's
//! `/observations` endpoint.

use std::io::Error;
use std::sync::Arc;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use opsense_components::Stations;
use opsense_core::collector::{Collector, SourceInfo};
use opsense_libs::vector::runtime::Runtime;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub collector: Arc<Collector>,
    pub runtime: Arc<RwLock<Runtime>>,

    /// Registry of stations this process manages, shared với `OpsenseContext`
    /// (nơi các transform publish) để API / MCP / Rhai đọc cùng một thể hiện.
    pub stations: Stations,
}

impl AppState {
    #[must_use]
    pub fn new(
        collector: Arc<Collector>,
        runtime: Arc<RwLock<Runtime>>,
        stations: Stations,
    ) -> Self {
        Self {
            collector,
            runtime,
            stations,
        }
    }

    pub async fn stop(&self) -> Result<(), Error> {
        self.runtime.read().await.stop()
    }

    pub async fn wait_for_shutdown(&self) -> Result<(), Error> {
        self.runtime.read().await.wait_for_shutdown().await
    }
}

async fn health_check() -> &'static str {
    "OK"
}

async fn reload(State(state): State<AppState>) -> &'static str {
    // Force one collection round on demand.
    state.collector.tick().await;
    "reloaded"
}

async fn sources(State(state): State<AppState>) -> Json<Vec<SourceInfo>> {
    Json(state.collector.sources_status())
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/reload", post(reload))
        .route("/sources", get(sources))
        .with_state(state)
}
