//! Generic HTTP fetch node: scrape any API by declaring the request as
//! templates.
//!
//! Every part of the request — `url`, `headers` values, `params` values,
//! `body` — is a [`template`](opsense_core::template) rendered right before
//! the call, with the current watermark window as built-ins:
//!
//! ```text
//! {{from_ts}} / {{to_ts}} / {{ts}}   window bounds of this cycle; the fetch
//!                                    covers `(from_ts, to_ts]` — `from_ts` is
//!                                    rendered bumped by 1 because range APIs
//!                                    evaluate `start` inclusively
//! {{name}}                           [attributes] entry (env-overridable)
//! {{env.NAME}}                       raw environment variable
//! ```
//!
//! Data flows like every other node: a signal carrying `ts` drives the cycle,
//! the node keeps its own watermark cursor (`Watermarks.get_node/set_node`,
//! keyed by `id`) and renders `from_ts` from it — so windowed APIs (e.g. a
//! range query) only ask for the delta, and a failed cycle retries the whole
//! window on the next signal. On the very first cycle (`cursor == 0`) the
//! window starts `initial_lookback_secs` before `to_ts` instead, so a fresh
//! session backfills a bounded history instead of everything since epoch.
//!
//! The response body maps to observations with a declarative `jq`-style
//! extractor (see [`opsense_libs::jq`]):
//! - `items` — a path selecting the array of items to emit one observation each
//!   (empty = the body itself, expected to already be observation-shaped JSON);
//! - `fields` — for each output field (`ts`, `value`, `metric_id`, `kind`,
//!   `signal`, `labels`, `severity`, …) the path picking its value, plus an
//!   optional `cast_to` so strings/numbers land in the right type;
//! - `constants` — static key/values merged into every observation (e.g. a fixed
//!   `metric_id` or `labels`).
//!
//! Field paths may use `^` to climb from the item to its parent container —
//! e.g. Prometheus range responses with `items = "data.result[].values[]"`:
//! each item is a `[ts, value]` pair whose grandparent is the series object,
//! so `labels = { query = "^.^.metric" }` picks the series labels.
//!
//! The built object is turned into an [`opsense_core::source::observation_from_value`],
//! so any field the core model understands works without Rust changes.
//!
//! Results land in `Stage::Processed` of the node's own station (the sole
//! cache) and
//! `data_ready(ts)` fans out downstream — regardless of per-cycle errors, which
//! only hold the cursor back (same contract as the other nodes).

use opsense_core::Context;
use std::collections::{BTreeMap, HashMap};
use std::io::Error;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use opsense_core::station::Station;
use opsense_core::template::{self, TemplateVars};
use opsense_core::Observation;
use opsense_core::Stage;
use opsense_core::source::{observation_from_value, observations_from_body};

use opsense_libs::cast::{cast_value, CastType};
use opsense_libs::jq::JsonQuery;
use opsense_libs::lru::OriginSource;
use opsense_libs::storage::{InMemoryStorage, TimeseriesStorage};
use opsense_libs::vector::runtime::Context as RuntimeContext;

use crate::station::StationStorage;
use crate::vector::runtime::{Component, Identify, Message, Outbound};
use crate::{signal, OpsenseContext};
use opsense_macros::transform;
use tokio::sync::mpsc;

/// One output field of the jq mapping: a path into the item picking its value,
/// plus an optional cast so the picked JSON becomes the declared type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldSpec {
    /// `jq`-style path (parsing via [`opsense_libs::jq::JsonQuery::parse`]).
    pub query: String,
    /// Optional target type for the picked value.
    #[serde(default)]
    pub cast_to: Option<CastType>,
}

/// `station = true` biến node thành terminal: nó tự phục vụ dữ liệu qua
/// station/MCP/HTTP nên không bắt buộc phải có node downstream.
#[transform(terminal_field = "station")]
pub struct HttpSource {
    pub id: String,
    pub inputs: Vec<String>,

    /// Request URL — a template.
    pub url: String,

    #[serde(default = "default_method")]
    pub method: String,

    /// Header values are templates (e.g. `Bearer {{env.API_TOKEN}}`).
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Query parameters; keys are literal, values are templates. Sent through
    /// `reqwest`'s `.query()` so values are percent-encoded automatically —
    /// keep dynamic text out of `url` itself.
    #[serde(default)]
    pub params: BTreeMap<String, String>,

    /// Request body template (for POST/PUT APIs).
    #[serde(default)]
    pub body: Option<String>,

    /// Path selecting the array of items to emit one observation each. Empty =
    /// the body itself is treated as observation-shaped JSON (an array, or a
    /// single object).
    #[serde(default)]
    pub items: String,

    /// Per-field extraction. Each entry builds one key of the observation object
    /// from the matched item; the result is fed to
    /// [`opsense_core::source::observation_from_value`].
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,

    /// Static values merged into every observation (e.g. a fixed `metric_id` or
    /// `labels` map).
    #[serde(default)]
    pub constants: HashMap<String, serde_json::Value>,

    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// How far back the very first cycle looks (`cursor == 0`). Later cycles
    /// always resume from the cursor.
    #[serde(default)]
    pub initial_lookback_secs: i64,

    // ── station capability ──
    /// Register everything this source fetches as its own station:
    /// a private block store published under this node's id, queryable from
    /// Rhai (`ts_query("<id>", "raw", …)`), the MCP tools and HTTP endpoints.
    #[serde(default)]
    pub station: bool,

    /// Loại station đăng ký: `timeseries` (mặc định — lưu observations) hoặc
    /// `category` (index cặp key/value vào catalog, query qua MCP
    /// `catalog_list`/HTTP `/catalog`). Với `category`, key/value của mỗi
    /// observation lấy từ `key_field`/`value_field` (fallback về
    /// `metric_id`/`value`).
    #[serde(default)]
    pub station_kind: crate::station::StationKind,

    /// Label chứa key khi `station_kind = "category"` (default `"metric_id"`).
    #[serde(default = "default_key_field")]
    pub key_field: String,

    /// Label chứa value khi `station_kind = "category"` (default `"value"`).
    #[serde(default = "default_value_field")]
    pub value_field: String,

    /// Bind address of the station's query endpoint (empty = disabled).
    #[serde(default = "default_station_bind")]
    pub bind: String,

    #[serde(default = "default_station_block_secs")]
    pub block_secs: i64,

    #[serde(default = "default_station_max_hot_blocks")]
    pub max_hot_blocks: usize,

    #[serde(default = "default_station_max_hot_mb")]
    pub max_hot_mb: usize,

    /// Cold-tier directory (LMDB); empty keeps it RAM-only.
    #[serde(default)]
    pub data_dir: String,

    /// Delete cold observations older than this many seconds (0 = forever).
    #[serde(default)]
    pub cold_retention_secs: i64,

    /// Persist the raw batch to a cold tier for durability. When false (default)
    /// the node's station is RAM-only; when true it attaches an in-memory cold
    /// tier (or the configured `data_dir`/`sqlite` backend) so raw survives
    /// eviction and restart.
    #[serde(default)]
    pub store_raw: bool,
}

fn default_station_bind() -> String {
    String::new()
}
fn default_station_block_secs() -> i64 {
    300
}
fn default_station_max_hot_blocks() -> usize {
    288
}
fn default_station_max_hot_mb() -> usize {
    256
}

fn default_key_field() -> String {
    "metric_id".to_string()
}

fn default_value_field() -> String {
    "value".to_string()
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_timeout() -> u64 {
    30
}

impl HttpSource {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str], url: &str) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            url: url.to_string(),
            method: default_method(),
            headers: HashMap::new(),
            params: BTreeMap::new(),
            body: None,
            items: String::new(),
            fields: BTreeMap::new(),
            constants: HashMap::new(),
            timeout_secs: default_timeout(),
            initial_lookback_secs: 0,
            station: false,
            station_kind: Default::default(),
            key_field: default_key_field(),
            value_field: default_value_field(),
            bind: default_station_bind(),
            block_secs: default_station_block_secs(),
            max_hot_blocks: default_station_max_hot_blocks(),
            max_hot_mb: default_station_max_hot_mb(),
            data_dir: String::new(),
            cold_retention_secs: 0,
            store_raw: false,
        }
    }

    fn station_options(&self) -> crate::station::StationOptions {
        crate::station::StationOptions {
            id: self.id.clone(),
            inputs: self.inputs.clone(),
            kind: self.station_kind,

            // Sources default to no dedicated HTTP endpoint — Rhai/MCP reads
            // go through the registry handle instead.
            bind: self.bind.clone(),
            block_secs: self.block_secs,
            max_hot_blocks: self.max_hot_blocks,
            max_hot_mb: self.max_hot_mb,
            data_dir: self.data_dir.clone(),
            cold_retention_secs: self.cold_retention_secs,

            // Origin fallback removed: the station is a pure bounded cache.
            origin_enabled: false,

            // Sources append Stage::Processed into their station so the
            // default "processed" reads on HTTP/MCP/Rhai see the data.
            stages: vec![opsense_core::Stage::Processed],

            // `store_raw` opts into a durable cold tier; `data_dir` (sqlite) wins
            // when set. `run` also attaches the read-through tier (with origin
            // fallback) for this node, so keep the generic attach off when the
            // read-through path will handle it to avoid a redundant backend.
            storage: if !self.data_dir.is_empty() {
                #[cfg(feature = "sqlite")]
                {
                    StationStorage::Sqlite(self.data_dir.clone())
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    StationStorage::InMemory
                }
            } else if self.store_raw {
                StationStorage::InMemory
            } else {
                StationStorage::None
            },
        }
    }
}

/// One shared client pool per timeout so poll-cadence calls reuse connections.
fn client_for(timeout_secs: u64) -> Result<reqwest::Client, String> {
    static CLIENTS: OnceLock<Mutex<HashMap<u64, reqwest::Client>>> = OnceLock::new();
    let clients = CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = clients.lock().unwrap();

    let timeout_secs = timeout_secs.max(1);
    if let Some(client) = guard.get(&timeout_secs) {
        return Ok(client.clone());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;
    guard.insert(timeout_secs, client.clone());
    Ok(client)
}

impl_http_source!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx_arc: Arc<dyn RuntimeContext> = tx
            .ctx
            .clone()
            .ok_or_else(|| Error::other("OpsenseContext not injected into Runtime"))?;
        let ctx = ctx_arc
            .as_any()
            .downcast_ref::<OpsenseContext>()
            .ok_or_else(|| Error::other("OpsenseContext not downcastable"))?;

        // Station riêng của node (luôn bật) — nơi lưu duy nhất và cũng là
        // trạm query. Origin fallback đã bị xoá: station là pure bounded cache.
        let station_store = crate::station::ensure_station(&self.station_options()).await;

        // Gắn read-through tier: storage (tier-2) + coverage `validate` + origin
        // `fallback` (reuse `fetch_window`). Cache miss / hổng coverage sẽ tự
        // reload từ đĩa hoặc re-fetch đúng cửa sổ từ origin. Chỉ áp dụng cho
        // station timeseries — station category không dùng cache khối.
        if self.station_kind == crate::station::StationKind::Timeseries {
            let storage: Arc<dyn TimeseriesStorage> = if !self.data_dir.is_empty() {
                #[cfg(feature = "sqlite")]
                {
                    match SqliteStorage::open(&self.data_dir).await {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            tracing::warn!(
                                "http {} sqlite open failed ({e}); fall back to in-memory",
                                self.id
                            );
                            Arc::new(InMemoryStorage::new())
                        }
                    }
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    tracing::warn!(
                        "http {} sqlite feature disabled; using in-memory storage",
                        self.id
                    );
                    Arc::new(InMemoryStorage::new())
                }
            } else {
                Arc::new(InMemoryStorage::new())
            };

            {
                let mut st = station_store.write().await;
                if let Station::Timeseries(ts) = &mut *st {
                    ts.cache.attach_fallback(
                        Arc::clone(&storage),
                        crate::station::station_series_of(),
                        crate::station::station_encode(),
                        crate::station::station_decode(),
                        crate::station::station_validate(),
                        crate::station::station_coverage_gap(),
                        Arc::new(HttpOrigin {
                            src: Arc::new(self.clone()),
                            ctx: Arc::clone(&ctx_arc),
                        }),
                    );
                    // Persist points keyed by the latest observation ts (seconds) so
                    // disk reads align with the request window.
                    ts.cache.ts_timestamp_of = Some(Arc::new(
                        |_k: &(Stage, String), v: &BTreeMap<i64, Observation>| {
                            v.keys().next_back().copied().unwrap_or(0) as u64
                        },
                    ));
                    // Merge thay vì ghi đè khi origin backfill trả về một cửa sổ
                    // cũ hơn: các điểm mới hơn đã có trong cache phải sống sót.
                    ts.cache.ts_merge = Some(Arc::new(
                        |existing: &BTreeMap<i64, Observation>,
                         fresh: BTreeMap<i64, Observation>| {
                            let mut merged = existing.clone();
                            merged.extend(fresh);
                            merged
                        },
                    ));
                }
            }
        }

        while let Some(msg) = rx.recv().await {
            // Control event: manual backfill of an arbitrary OLD window
            // (`opsense_backfill`). Bypasses the watermark guard — the live
            // cursor never moves backwards, so normal cycles are unaffected.
            if signal::event(&msg) == Some(signal::BACKFILL) {
                let from_ts = msg
                    .payload
                    .get("from_ts")
                    .and_then(serde_json::Value::as_i64);
                let to_ts = msg.payload.get("to_ts").and_then(serde_json::Value::as_i64);
                let (Some(from_ts), Some(to_ts)) = (from_ts, to_ts) else {
                    tracing::warn!("http {} backfill missing from_ts/to_ts", self.id);
                    continue;
                };
                if to_ts <= from_ts {
                    tracing::warn!(
                        "http {} backfill window empty: ({from_ts},{to_ts}]",
                        self.id
                    );
                    continue;
                }
                match self.fetch_window(ctx, from_ts, to_ts).await {
                    Ok(batch) => {
                        if !batch.is_empty() {
                            self.ingest(&station_store, &batch).await;
                        }
                        tracing::info!(
                            "http {} backfilled {} observations for ({from_ts},{to_ts}]",
                            self.id,
                            batch.len()
                        );
                        let ready = signal::tagged(signal::data_ready(to_ts), &self.id);
                        for stream in &tx.streams {
                            let _ = stream.send(ready.clone()).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("http {} backfill failed ({from_ts},{to_ts}]: {e}", self.id)
                    }
                }
                continue;
            }

            // Any signal carrying a timestamp drives this node (tick from a
            // clock, data_ready/processed when chained after another node).
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            let cursor = ctx.get_node_watermark(&self.id);
            if cursor > 0 && ts <= cursor {
                continue; // nothing new since the last successful cycle
            }
            // First cycle ever: backfill a bounded window instead of skipping.
            let from_ts = if cursor == 0 {
                ts - self.initial_lookback_secs
            } else {
                cursor
            };

            match self.fetch_window(ctx, from_ts, ts).await {
                Ok(batch) => {
                    if !batch.is_empty() {
                        // Station của node là NƠI LƯU DUY NHẤT (cache+storage).
                        self.ingest(&station_store, &batch).await;
                    }
                    ctx.set_node_watermark(&self.id, ts);
                }
                // Hold the cursor: the window is retried on the next signal,
                // so fixing config/script/API recovers without data loss.
                Err(e) => {
                    tracing::warn!("http {} skipped window ({from_ts},{ts}]: {e}", self.id)
                }
            }

            let ready = signal::tagged(signal::data_ready(ts), &self.id);
            for stream in &tx.streams {
                let _ = stream.send(ready.clone()).await;
            }
        }
        Ok(())
    }
);

impl HttpSource {
    /// Ghi một batch vừa fetch vào station của node, theo `station_kind`:
    /// `timeseries` append observations; `category` index cặp key/value
    /// (key/value lấy từ `key_field`/`value_field`, fallback `metric_id`/`value`)
    /// qua `insert_entry` — idempotent per key nên re-fetch cùng cửa sổ không
    /// sinh entry trùng.
    async fn ingest(&self, store: &tokio::sync::RwLock<Station>, batch: &[Observation]) {
        if self.station_kind == crate::station::StationKind::Category {
            let mut st = store.write().await;
            for obs in batch {
                let key = obs
                    .labels
                    .get(&self.key_field)
                    .cloned()
                    .unwrap_or_else(|| obs.metric_id.clone());
                let value = obs
                    .labels
                    .get(&self.value_field)
                    .cloned()
                    .unwrap_or_else(|| obs.value.to_string());
                st.insert_entry(key.as_bytes(), &value).await;
            }
        } else {
            store.write().await.append(Stage::Processed, batch).await;
        }
    }

    async fn fetch_window(
        &self,
        ctx: &OpsenseContext,
        from_ts: i64,
        to_ts: i64,
    ) -> Result<Vec<Observation>, String> {
        // Cửa sổ fetch theo semantics `(from_ts, to_ts]`: các API range-query
        // (Prometheus…) đánh giá tại `start` **inclusively**, nên đẩy `start`
        // qua mốc `from_ts` — nếu không, sample nằm đúng tại watermark cũ bị
        // loại ở mọi lần đọc downstream (window đọc là from-exclusive) và
        // node downstream đói data vĩnh viễn.
        let vars = TemplateVars {
            from_ts: from_ts.saturating_add(1),
            to_ts,
        };
        let attributes = ctx.attributes();

        let url = template::render(&self.url, &vars, &attributes)?;
        let method_str = self.method.trim().to_ascii_uppercase();
        let method = reqwest::Method::from_bytes(method_str.as_bytes())
            .map_err(|e| format!("invalid method `{}`: {e}", self.method))?;

        let client = client_for(self.timeout_secs)?;
        let mut request = client.request(method, &url);
        for (key, value) in &self.headers {
            request = request.header(key, template::render(value, &vars, &attributes)?);
        }
        if !self.params.is_empty() {
            let mut pairs: Vec<(String, String)> = Vec::with_capacity(self.params.len());
            for (key, value) in &self.params {
                pairs.push((key.clone(), template::render(value, &vars, &attributes)?));
            }
            request = request.query(&pairs);
        }
        if let Some(body) = &self.body {
            request = request.body(template::render(body, &vars, &attributes)?);
        }

        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("endpoint answered {status}"));
        }
        let body = response.text().await.map_err(|e| e.to_string())?;

        if self.fields.is_empty() {
            // Body already observation-shaped JSON (array of objects or one object).
            observations_from_body(&self.id, &body).map_err(|e| e.to_string())
        } else {
            self.map_through_jq(&body)
        }
    }

    /// Map a raw JSON body into observations via the declarative `jq` extractor:
    /// pick the `items`, build each observation object from `fields` (+ `constants`)
    /// and turn it into an [`opsense_core::source::observation_from_value`].
    fn map_through_jq(&self, body: &str) -> Result<Vec<Observation>, String> {
        let payload: serde_json::Value =
            serde_json::from_str(body).map_err(|e| format!("response body: {e}"))?;

        let items_query = if self.items.is_empty() {
            None
        } else {
            Some(
                JsonQuery::parse(&self.items)
                    .map_err(|e| format!("http {} `items` query: {e}", self.id))?,
            )
        };

        // Precompile each field's query once for the whole batch.
        let mut field_queries: Vec<(&String, JsonQuery, Option<CastType>)> =
            Vec::with_capacity(self.fields.len());
        for (name, spec) in &self.fields {
            let q = JsonQuery::parse(&spec.query)
                .map_err(|e| format!("http {} field `{name}` query: {e}", self.id))?;
            field_queries.push((name, q, spec.cast_to));
        }

        let items: Vec<serde_json::Value> = match &items_query {
            Some(q) => q.execute(&payload),
            None => match payload {
                serde_json::Value::Array(arr) => arr,
                other => vec![other],
            },
        };

        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let mut obj = serde_json::Map::new();
            for (name, q, cast_to) in &field_queries {
                let Some(picked) = q.execute(&item).into_iter().next() else {
                    continue;
                };
                let value = match cast_to {
                    Some(c) => match cast_value(picked, c) {
                        Some(v) => v,
                        None => {
                            tracing::warn!(
                                "http {} field `{name}` cannot cast to {c:?}; skipped",
                                self.id
                            );
                            continue;
                        }
                    },
                    None => picked,
                };
                obj.insert((*name).clone(), value);
            }
            if !self.constants.is_empty() {
                for (k, v) in &self.constants {
                    obj.insert(k.clone(), v.clone());
                }
            }
            out.push(
                observation_from_value(&self.id, serde_json::Value::Object(obj))
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok(out)
    }
}

/// Origin re-fetch cho read-through của station: khi query một cửa sổ đã bị evict
/// khỏi cả cache lẫn đĩa, `get_with_load` gọi `fetch` này để re-fetch đúng cửa
/// sổ từ API gốc (`fetch_window` render template + jq) rồi lọc theo `metric_id`.
struct HttpOrigin {
    src: Arc<HttpSource>,
    ctx: Arc<dyn RuntimeContext>,
}

impl OriginSource<(Stage, String), BTreeMap<i64, Observation>> for HttpOrigin {
    fn fetch(
        &self,
        key: &(Stage, String),
        from_ts: u64,
        to_ts: u64,
    ) -> opsense_libs::lru::BoxFuture<Result<BTreeMap<i64, Observation>, String>> {
        let src = Arc::clone(&self.src);
        let ctx = Arc::clone(&self.ctx);
        let metric = key.1.clone();
        Box::pin(async move {
            let ctx_ref = ctx
                .as_any()
                .downcast_ref::<OpsenseContext>()
                .expect("OpsenseContext downcast");
            let batch = src.fetch_window(ctx_ref, from_ts as i64, to_ts as i64).await?;
            let mut map = BTreeMap::new();
            for obs in batch {
                if obs.metric_id == metric {
                    map.insert(obs.ts, obs);
                }
            }
            Ok(map)
        })
    }
}
