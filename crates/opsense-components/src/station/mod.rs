//! Timeseries station — the single cache/storage abstraction of a pipeline.
//!
//! A station is now a `Station` (see `opsense_core::Station`) published under
//! its component id in the process-global `opsense_core::registry`. Three
//! shapes exist:
//!
//! - [`station_sink`](sink) — leaf node snapshotting its input window.
//! - [`station_transform`](transform) — same snapshotting but forwards the
//!   signal downstream, so a station can sit mid-pipeline.
//! - source toggle — `http_source` with `station = true` registers everything
//!   it fetches as its own station (see `crate::http`).
//!
//! Pattern/category transforms publish `Station::Pattern` / `Station::Category`
//! through the same registry (`ensure_pattern` / `ensure_search`).
//!
//! Every station serves history over HTTP (`ensure_station` spawns the axum
//! server once per id):
//!
//! - `GET /api/v1/query_range?query=<selector>&start=&end=` and
//!   `/api/v1/query?query=<selector>&time=` answer in the Prometheus envelope
//!   (`metric.__name__`, `values: [[ts,"v"]]`), so an [`crate::HttpSource`]
//!   can point at another station instead of an external Prometheus.
//! - `GET /observations?metric=&from_ts=&to_ts=` returns plain observation JSON.
//! - `GET /describe` proxies `describe_station`.
//!
//! Selector syntax v1: a bare metric name plus optional equality matchers,
//! e.g. `cpu_usage{job="node"}`. The `step` parameter is accepted but not
//! downsampled yet. Registry handles are first-wins.

mod category_transform;
mod pattern_transform;
mod timeseries_sink;
mod timeseries_transform;

pub use category_transform::CategoryStationTransform;
pub use pattern_transform::PatternStationTransform;
pub use timeseries_sink::TimeseriesStationSink;
pub use timeseries_transform::TimeseriesStationTransform;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::extract::{Query as AxumQuery, State};
use axum::{Json, Router};
use tokio::sync::RwLock;

use opsense_libs::storage::{InMemoryStorage, TimeseriesStorage};

use crate::signal;
use opsense_core::registry;
use opsense_core::station::{Stage, Station};
use opsense_core::Observation;

#[cfg(feature = "sqlite")]
use opsense_libs::storage::SqliteStorage;

pub(crate) fn default_stage() -> String {
    "processed".to_string()
}

pub(crate) fn default_bind() -> String {
    "127.0.0.1:9190".to_string()
}

pub(crate) fn default_block_secs() -> i64 {
    300
}

pub(crate) fn default_max_hot_blocks() -> usize {
    288
}

pub(crate) fn default_max_hot_mb() -> usize {
    256
}

/// Everything [`ensure_station`] needs to build/attach one station.
/// Node structs convert into this via `StationOptions::from_*`.
#[derive(Debug, Clone)]
pub struct StationOptions {
    pub id: String,
    pub inputs: Vec<String>,
    pub bind: String,
    pub block_secs: i64,
    pub max_hot_blocks: usize,
    pub max_hot_mb: usize,
    pub data_dir: String,
    pub cold_retention_secs: i64,
    /// Source-attached station: kept for API compatibility; origin fallback is
    /// gone, so this is now a no-op flag.
    pub origin_enabled: bool,
    /// Stages the station is declared to hold — surfaced via `/describe`.
    pub stages: Vec<Stage>,
    /// Cold-tier storage backend (tier-2 behind the RAM LRU). `None` keeps the
    /// station pure-RAM (legacy behaviour); `InMemory`/`Sqlite` attach a
    /// `TimeseriesStorage` so evicted entries survive and can be reloaded.
    pub storage: StationStorage,
}

/// Cold-tier storage backend for a station (tier-2 behind the RAM LRU).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StationStorage {
    /// Pure RAM LRU — no disk persistence, misses return empty (legacy behaviour).
    #[default]
    None,
    /// In-process storage (lost on restart; handy for tests / single-process).
    InMemory,
    /// SQLite file at the given path (persistent across restarts).
    Sqlite(String),
}

/// Mỗi node SINH dữ liệu tự động có trạm riêng của mình (mặc định: chỉ RAM,
/// không có HTTP) — nơi duy nhất lưu đầu ra của nó cho các consumer đọc.
pub async fn own_station(id: &str, inputs: &[String], stages: &[Stage]) -> Arc<RwLock<Station>> {
    ensure_station(&StationOptions {
        id: id.to_string(),
        inputs: inputs.to_vec(),
        bind: String::new(),
        block_secs: default_block_secs(),
        max_hot_blocks: default_max_hot_blocks(),
        max_hot_mb: default_max_hot_mb(),
        data_dir: String::new(),
        cold_retention_secs: 0,
        origin_enabled: false,
        stages: stages.to_vec(),
        storage: StationStorage::None,
    })
    .await
}

/// Build once per process: reuse the existing station when the id was already
/// registered (reload swaps the node instance but must not fork the cache or
/// re-bind HTTP). The station lives in the process-global `opsense_core::
/// registry` (shared with HTTP/MCP/Rhai); the HTTP server keeps an
/// `Arc<RwLock<Station>>` handle to read from it.
pub async fn ensure_station(opts: &StationOptions) -> Arc<RwLock<Station>> {
    // First-wins through the global registry so HTTP/MCP/Rhai see the station.
    if let Some(existing) = registry::station(&opts.id).await {
        return existing;
    }

    let capacity = opts.max_hot_blocks.saturating_mul(16).max(16);
    let station = Arc::new(RwLock::new(Station::timeseries_with(
        capacity,
        opts.stages.clone(),
    )));
    if registry::register_station(&opts.id, station.clone()).await {
        // We won the race — only the first registration binds the HTTP server.
        spawn_server(opts.id.clone(), opts.bind.clone(), station.clone());
        // Attach the cold-tier storage when configured (storage + decode/encode
        // + coverage validate, no origin fallback). Source-attached stations
        // layer their own origin fallback on top via `attach_fallback`.
        if opts.storage != StationStorage::None {
            let storage = build_station_storage(&opts.storage).await;
            let mut st = station.write().await;
            if let Station::Timeseries(ts) = &mut *st {
                ts.cache.attach_storage(
                    storage,
                    station_series_of(),
                    station_encode(),
                    station_decode(),
                    station_validate(),
                    station_coverage_gap(),
                );
            }
        }
        station
    } else {
        // A concurrent caller registered first; serve from the canonical one.
        registry::station(&opts.id)
            .await
            .expect("station registered by a concurrent caller")
    }
}

/// Build the cold-tier storage backend described by [`StationStorage`].
async fn build_station_storage(cfg: &StationStorage) -> Arc<dyn TimeseriesStorage> {
    match cfg {
        StationStorage::None => unreachable!("build_station_storage called with None"),
        StationStorage::InMemory => Arc::new(InMemoryStorage::new()),
        StationStorage::Sqlite(_path) => {
            #[cfg(feature = "sqlite")]
            {
                match SqliteStorage::open(_path).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        tracing::warn!(
                            "station sqlite open failed ({e}); falling back to in-memory"
                        );
                        Arc::new(InMemoryStorage::new())
                    }
                }
            }
            #[cfg(not(feature = "sqlite"))]
            {
                tracing::warn!(
                    "opsense-libs sqlite feature disabled; station uses in-memory storage"
                );
                Arc::new(InMemoryStorage::new())
            }
        }
    }
}

/// Map `(stage, metric)` → opaque series name (bytes) for the timeseries store.
pub(crate) fn station_series_of() -> Arc<dyn Fn(&(Stage, String)) -> Vec<u8> + Send + Sync> {
    Arc::new(|(stage, metric): &(Stage, String)| {
        format!("{}:{}", stage.as_str(), metric).into_bytes()
    })
}

/// Encode a per-metric observation map to opaque bytes for the timeseries store.
pub(crate) fn station_encode() -> Arc<dyn Fn(&BTreeMap<i64, Observation>) -> Vec<u8> + Send + Sync>
{
    Arc::new(|v: &BTreeMap<i64, Observation>| serde_json::to_vec(v).unwrap_or_default())
}

/// Decode opaque bytes back into a per-metric observation map.
pub(crate) fn station_decode(
) -> Arc<dyn Fn(&[u8]) -> Option<BTreeMap<i64, Observation>> + Send + Sync> {
    Arc::new(|b: &[u8]| serde_json::from_slice::<BTreeMap<i64, Observation>>(b).ok())
}

/// Coverage validate: the map spans the requested window when it has a point at
/// or before `from_ts` *and* a point at or after `to_ts` (đủ điểm ở 2 biên).
pub(crate) fn station_validate(
) -> Arc<dyn Fn(&(Stage, String), &BTreeMap<i64, Observation>, u64, u64) -> bool + Send + Sync> {
    Arc::new(|_key, v, from_ts, to_ts| {
        if v.is_empty() {
            return false;
        }
        let from = from_ts as i64;
        let to = to_ts as i64;
        let first = *v.keys().next().expect("non-empty map has a first key");
        let last = *v.keys().next_back().expect("non-empty map has a last key");
        first <= from && last >= to
    })
}

/// Tính phần cửa sổ thực sự hổng so với `[from_ts, to_ts]` của map đã có:
/// - map phủ đủ → `None` (không cần fetch);
/// - hổng ở đầu (quá khứ, `from_ts < first`) → `Some((from_ts, first))` để
///   origin chỉ fetch phần thiếu;
/// - chỉ hổng ở đuôi (tương lai, `to_ts > last`) → `None` (không thể fetch dữ
///   liệu tương lai, cache đã giữ mọi thứ hiện có).
/// Quan trọng: chặn query "lấy mọi thứ" (`to = u64::MAX`) khỏi trigger fetch
/// nguyên cửa sổ vô lý → tránh treo / lãng phí.
pub(crate) fn station_coverage_gap(
) -> Arc<dyn Fn(&(Stage, String), &BTreeMap<i64, Observation>, u64, u64) -> Option<(u64, u64)>
       + Send
       + Sync> {
    Arc::new(|_key, v, from_ts, to_ts| {
        if v.is_empty() {
            return Some((from_ts, to_ts));
        }
        let first = *v.keys().next().expect("non-empty map has a first key");
        let last = *v.keys().next_back().expect("non-empty map has a last key");
        if (first as u64) <= from_ts && (last as u64) >= to_ts {
            return None; // đã phủ đủ
        }
        if from_ts < first as u64 {
            // Hổng ở quá khứ — fetchable. Chặn cả phần tương lai (to_ts) lẫn
            // phần đã có trong cache (first) để cửa sổ fetch nằm gọn trong cửa
            // sổ query → observation trả về nằm trong cửa sổ yêu cầu.
            return Some((from_ts, (first as u64).min(to_ts)));
        }
        // Chỉ hổng ở tương lai → không fetch.
        None
    })
}

fn spawn_server(id: String, bind: String, station: Arc<RwLock<Station>>) {
    if bind.is_empty() {
        return;
    }
    let state = Arc::new(StationState {
        id: id.clone(),
        station,
    });
    let app = Router::new()
        .route("/api/v1/query_range", axum::routing::get(query_range))
        .route("/api/v1/query", axum::routing::get(query_instant))
        .route("/observations", axum::routing::get(observations))
        .route("/describe", axum::routing::get(describe))
        .route("/health", axum::routing::get(|| async { "OK" }))
        .with_state(state);
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&bind).await {
            Ok(listener) => {
                tracing::info!("station {id} serving queries on http://{bind}");
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!("station {id} http server stopped: {e}");
                }
            }
            Err(e) => tracing::error!("station {id} cannot bind {bind}: {e}"),
        }
    });
}

struct StationState {
    id: String,
    station: Arc<RwLock<Station>>,
}

pub(crate) fn stage_of(name: &str) -> Stage {
    if name.eq_ignore_ascii_case("raw") {
        Stage::Raw
    } else {
        Stage::Processed
    }
}

fn parse_ts(value: Option<&String>) -> Option<i64> {
    let raw = value?.trim();
    raw.parse::<i64>()
        .ok()
        .or_else(|| raw.parse::<f64>().ok().map(|f| f as i64))
}

/// `cpu_usage{job="node",dc="hcm"}` → metric + equality matchers.
fn parse_selector(query: &str) -> (String, Vec<(String, String)>) {
    let query = query.trim();
    if let (Some(open), true) = (query.find('{'), query.ends_with('}')) {
        let name = query[..open].trim().to_string();
        let inner = &query[open + 1..query.len() - 1];
        let mut matchers = Vec::new();
        for part in inner.split(',') {
            if let Some((key, value)) = part.trim().split_once('=') {
                matchers.push((
                    key.trim().to_string(),
                    value.trim().trim_matches('"').to_string(),
                ));
            }
        }
        (name, matchers)
    } else {
        (query.to_string(), Vec::new())
    }
}

async fn fetch_series(
    station: &Arc<RwLock<Station>>,
    params: &HashMap<String, String>,
    from_ts: i64,
    to_ts: i64,
) -> Vec<Observation> {
    let (metric, matchers) =
        parse_selector(params.get("query").map(String::as_str).unwrap_or_default());
    let stage = stage_of(
        params
            .get("stage")
            .map(String::as_str)
            .unwrap_or("processed"),
    );
    let mut batch = if metric.is_empty() {
        station.read().await.query_all(stage, from_ts, to_ts).await
    } else {
        station
            .read()
            .await
            .query(stage, &metric, from_ts, to_ts)
            .await
    };
    batch.retain(|obs| {
        matchers
            .iter()
            .all(|(k, v)| obs.labels.get(k).map(String::as_str) == Some(v.as_str()))
    });
    batch
}

/// Prometheus matrix envelope: `{status, data:{resultType, result:[...]}}`.
type SamplePoint<'a> = (i64, f64, &'a HashMap<String, String>);

fn matrix_envelope(samples: Vec<Observation>) -> serde_json::Value {
    let mut by_metric: BTreeMap<String, Vec<SamplePoint>> = BTreeMap::new();
    for obs in &samples {
        by_metric
            .entry(obs.metric_id.clone())
            .or_default()
            .push((obs.ts, obs.value, &obs.labels));
    }

    let result: Vec<serde_json::Value> = by_metric
        .into_iter()
        .map(|(name, points)| {
            let mut labels = serde_json::Map::new();
            labels.insert("__name__".into(), serde_json::json!(name));
            if let Some((_, _, first)) = points.first() {
                for (k, v) in first.iter() {
                    labels.insert(k.clone(), serde_json::json!(v));
                }
            }
            let values: Vec<serde_json::Value> = points
                .iter()
                .map(|(ts, value, _)| serde_json::json!([*ts as f64, value.to_string()]))
                .collect();
            serde_json::json!({"metric": labels, "values": values})
        })
        .collect();

    serde_json::json!({"status": "success", "data": {"resultType": "matrix", "result": result}})
}

async fn query_range(
    State(state): State<Arc<StationState>>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let now = signal::now_secs();
    let to_ts = parse_ts(params.get("end")).unwrap_or(now);
    let from_ts = parse_ts(params.get("start")).unwrap_or(to_ts - 3_600);
    Json(matrix_envelope(
        fetch_series(&state.station, &params, from_ts, to_ts).await,
    ))
}

async fn query_instant(
    State(state): State<Arc<StationState>>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let now = signal::now_secs();
    let time = parse_ts(params.get("time")).unwrap_or(now);
    let lookback = params
        .get("lookback")
        .and_then(|l| l.parse::<i64>().ok())
        .unwrap_or(300);
    // Instant vector: newest sample per series within the lookback window.
    let samples = fetch_series(&state.station, &params, time - lookback, time).await;
    let mut newest: BTreeMap<String, Observation> = BTreeMap::new();
    for obs in samples {
        match newest.get(&obs.metric_id) {
            Some(current) if current.ts > obs.ts => {}
            _ => {
                newest.insert(obs.metric_id.clone(), obs);
            }
        }
    }

    let result: Vec<serde_json::Value> = newest
        .into_values()
        .map(|obs| {
            let mut labels = serde_json::Map::new();
            labels.insert("__name__".into(), serde_json::json!(obs.metric_id));
            for (k, v) in &obs.labels {
                labels.insert(k.clone(), serde_json::json!(v));
            }
            serde_json::json!({"metric": labels, "value": [obs.ts as f64, obs.value.to_string()]})
        })
        .collect();

    Json(serde_json::json!({
        "status": "success",
        "data": {"resultType": "vector", "result": result}
    }))
}

async fn observations(
    State(state): State<Arc<StationState>>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
) -> Json<Vec<Observation>> {
    let now = signal::now_secs();
    let to_ts = parse_ts(params.get("to_ts")).unwrap_or(now);
    let from_ts = parse_ts(params.get("from_ts")).unwrap_or(to_ts - 3_600);
    let (metric, _) = parse_selector(params.get("metric").map(String::as_str).unwrap_or_default());
    let stage = stage_of(
        params
            .get("stage")
            .map(String::as_str)
            .unwrap_or("processed"),
    );
    let batch = if metric.is_empty() {
        state
            .station
            .read()
            .await
            .query_all(stage, from_ts, to_ts)
            .await
    } else {
        state
            .station
            .read()
            .await
            .query(stage, &metric, from_ts, to_ts)
            .await
    };
    Json(batch)
}

/// `GET /describe?id=<station>` — backend for one station. Defaults to this
/// station when `id` is omitted. Unregistered ids return an empty object.
async fn describe(
    State(state): State<Arc<StationState>>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let id = params
        .get("id")
        .cloned()
        .unwrap_or_else(|| state.id.clone());
    Json(
        registry::describe_station(&id)
            .await
            .unwrap_or(serde_json::Value::Null),
    )
}
