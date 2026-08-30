//! Shared application context handed to every runtime component.
//!
//! Since the "station is the only cache/storage" refactor there is NO shared
//! working store: every producing node owns a private station published under
//! its component id (`ensure_station`), and consumers read windows straight
//! from the upstream node's station — resolved through this context's
//! [`OpsenseContext::read_window`] helper.
//!
//! `read_window` is an `async fn` on the `Context` trait (resolved through
//! `tokio::sync::RwLock` guards that are `Send` across `.await`). The trait is
//! never used as `dyn`, so native `async fn` keeps the other accessors sync.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::collector::Collector;
use crate::config::Config;
use crate::station::{Cursor, Stage, Station};

/// Registry of stations keyed by component id. Shared between `OpsenseContext`
/// (where transforms publish stations) and `AppState` (where HTTP/MCP/Rhai
/// reads). Outer `RwLock<HashMap>` allows registration of new stations at
/// runtime (first-wins); inner `RwLock<Station>` allows each station to be
/// locked independently. Both layers are `tokio` locks so the guards survive
/// `.await` inside `read_window`.
pub type Stations = Arc<RwLock<HashMap<String, Arc<RwLock<Station>>>>>;

/// A runtime context that provides access to shared services.
///
/// Components receive this through `Outbound.ctx` and use its accessor methods
/// rather than accessing fields directly.
///
/// `read_window` is a native `async fn` (only it is async; the rest are sync).
/// The trait is never used as `dyn`, so we allow the `async_fn_in_trait` lint
/// rather than paying the `#[async_trait]` boxing/ripple cost.
#[allow(async_fn_in_trait)]
pub trait Context: Any + Send + Sync {
    /// Downcast to concrete type for type-specific access.
    fn as_any(&self) -> &dyn Any;

    /// Returns a clone of the collector for this context.
    fn collector(&self) -> Arc<Collector>;

    /// Returns a clone of the watermarks tracker for this context.
    fn watermarks(&self) -> Arc<Watermarks>;

    /// Returns a clone of the attributes map for this context.
    fn attributes(&self) -> Arc<BTreeMap<String, String>>;

    /// Returns a clone of the stations registry for this context.
    fn stations(&self) -> Stations;

    /// Get the global watermark for a cursor.
    fn get_watermark(&self, cursor: Cursor) -> i64;

    /// Set the global watermark for a cursor.
    fn set_watermark(&self, cursor: Cursor, ts: i64);

    /// Get the per-node watermark (component-specific cursor).
    fn get_node_watermark(&self, node_id: &str) -> i64;

    /// Set the per-node watermark (component-specific cursor).
    fn set_node_watermark(&self, node_id: &str, ts: i64);

    /// Read a window of observations from upstream stations.
    ///
    /// Resolution order: the signal payload's `"src"` tag (producers stamp
    /// their own id when emitting), else the single declared input, else the
    /// merge over every declared input.
    ///
    /// `stage == None` merges BOTH stages of the upstream station (raw first,
    /// deduped by `(metric_id, ts)`), which keeps pass-through chains working
    /// regardless of whether an intermediate node rewrote the data. An explicit
    /// stage reads only that stage of the upstream station.
    ///
    /// An unregistered upstream yields an empty window (debug-logged) — the
    /// pipeline keeps flowing instead of panicking.
    async fn read_window(
        &self,
        inputs: &[String],
        src: Option<&str>,
        from_ts: i64,
        to_ts: i64,
        stage: Option<Stage>,
    ) -> Vec<crate::Observation>;
}

/// Shared application context handed to every component by the `Runtime`.
///
/// Components are `#[typetag::serde]` (serializable state only), so everything
/// else — collector, durable store, watermark cursors, attributes — reaches
/// them through here (`Runtime::set_context` → `Outbound.ctx`).
#[derive(Clone)]
pub struct OpsenseContext {
    collector: Arc<Collector>,

    /// Resolved `[attributes]` (TOML + `OPSENSE_ATTR_*` env overrides) for
    /// template rendering in fetch nodes.
    attributes: Arc<BTreeMap<String, String>>,

    /// Watermark cursors journaled under the storage dir so a restart resumes
    /// where the last process stopped (with the memory backend every restart
    /// is a fresh world, so no journal).
    watermarks: Arc<Watermarks>,

    /// Registry of stations this process manages, keyed by component id
    /// (`Station::Category` / `Station::Pattern` / `Station::Timeseries`).
    /// Transforms publish here; `AppState` (HTTP/MCP/Rhai) reads from here.
    stations: Stations,
}

impl OpsenseContext {
    #[must_use]
    pub fn new(
        collector: Arc<Collector>,
        watermarks: Arc<Watermarks>,
        attributes: Arc<BTreeMap<String, String>>,
        stations: Stations,
    ) -> Self {
        Self {
            collector,
            watermarks,
            attributes,
            stations,
        }
    }

    /// Build the standard context from config: collector from `[sources]`,
    /// resolved `[attributes]` and the watermark cursors (journaled under
    /// `[storage].data_dir`). With the memory backend every restart is a fresh
    /// world, so no journal.
    #[must_use]
    pub fn from_config(cfg: &Config, stations: Stations) -> Arc<Self> {
        let collector = Arc::new(Collector::from_config(cfg));
        let attributes = Arc::new(cfg.resolved_attributes());
        let watermarks = if cfg.storage.backend == "memory" {
            Watermarks::new()
        } else {
            Watermarks::open(Path::new(&cfg.storage.data_dir))
        };
        Arc::new(Self::new(collector, watermarks, attributes, stations))
    }

    /// Create a new stations registry. Every context shares the single
    /// process-global [`registry::REGISTRY`], so a station published by one
    /// component is visible to HTTP/MCP/Rhai and to `read_window` of every
    /// context.
    #[must_use]
    pub fn new_stations() -> Stations {
        crate::registry::stations()
    }
}

impl Context for OpsenseContext {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn collector(&self) -> Arc<Collector> {
        self.collector.clone()
    }

    #[inline]
    fn watermarks(&self) -> Arc<Watermarks> {
        self.watermarks.clone()
    }

    #[inline]
    fn attributes(&self) -> Arc<BTreeMap<String, String>> {
        self.attributes.clone()
    }

    #[inline]
    fn stations(&self) -> Stations {
        self.stations.clone()
    }

    #[inline]
    fn get_watermark(&self, cursor: Cursor) -> i64 {
        self.watermarks.get(cursor)
    }

    #[inline]
    fn set_watermark(&self, cursor: Cursor, ts: i64) {
        self.watermarks.set(cursor, ts);
    }

    #[inline]
    fn get_node_watermark(&self, node_id: &str) -> i64 {
        self.watermarks.get_node(node_id)
    }

    #[inline]
    fn set_node_watermark(&self, node_id: &str, ts: i64) {
        self.watermarks.set_node(node_id, ts);
    }

    async fn read_window(
        &self,
        inputs: &[String],
        src: Option<&str>,
        from_ts: i64,
        to_ts: i64,
        stage: Option<Stage>,
    ) -> Vec<crate::Observation> {
        let targets: Vec<String> = match src {
            Some(id) => vec![id.to_string()],
            None => match inputs.len() {
                1 => vec![inputs[0].clone()],
                0 => Vec::new(),
                _ => inputs.to_vec(),
            },
        };

        let stations = self.stations.read().await;
        let mut out = Vec::new();
        let mut seen: BTreeSet<(String, i64)> = BTreeSet::new();
        for id in &targets {
            let Some(inner) = stations.get(id) else {
                tracing::debug!("read_window: no station `{id}` registered yet");
                continue;
            };
            let g = inner.read().await;
            let mut push_all = |batch: Vec<crate::Observation>| {
                for obs in batch {
                    let key = (obs.metric_id.clone(), obs.ts);
                    if seen.insert(key) {
                        out.push(obs);
                    }
                }
            };
            match stage {
                Some(one) => push_all(g.query_all(one, from_ts, to_ts).await),
                None => {
                    // Raw first, processed second — raw wins on duplicate keys.
                    push_all(g.query_all(Stage::Raw, from_ts, to_ts).await);
                    push_all(g.query_all(Stage::Processed, from_ts, to_ts).await);
                }
            }
        }
        out
    }
}

/// Implement the runtime-side `Context` trait (`as_any`) so an `OpsenseContext`
/// can be handed to `Runtime::set_context` (which wants
/// `Arc<dyn opsense_libs::vector::runtime::Context>`). The full
/// `opsense_core::Context` trait is the ergonomic one for components; this
/// impl is the type-erased "any blob" view the runtime stores.
impl opsense_libs::vector::runtime::Context for OpsenseContext {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Point-in-time copy of every cursor — the journal payload and a handy test
/// oracle for "what has this pipeline already done".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub ingest_done: i64,
    pub processed_done: i64,
    pub named: BTreeMap<String, i64>,
}

/// Per-cursor progress trackers, journaled to `<storage.data_dir>/watermarks.json`
/// so a restart resumes instead of reprocessing.
pub struct Watermarks {
    ingest_done: AtomicI64,
    processed_done: AtomicI64,
    named: Mutex<HashMap<String, AtomicI64>>,
    /// When set, every cursor advance rewrites this file so a restart resumes
    /// instead of reprocessing. `None` keeps the cursors RAM-only.
    journal: Mutex<Option<PathBuf>>,
}

impl Watermarks {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// RAM cursors backed by a JSON journal at `<dir>/watermarks.json`: an
    /// existing snapshot is restored on start and each advance rewrites the
    /// file atomically (tmp + rename). A corrupt journal logs and starts clean.
    #[must_use]
    pub fn open(dir: &Path) -> Arc<Self> {
        let wm = Self::default();
        let path = dir.join("watermarks.json");
        if let Ok(text) = std::fs::read_to_string(&path) {
            match serde_json::from_str::<Snapshot>(&text) {
                Ok(snapshot) => wm.restore(&snapshot),
                Err(error) => tracing::warn!(
                    "{}: ignoring corrupt watermark journal: {error}",
                    path.display()
                ),
            }
        }
        *wm.journal.lock().expect("watermark journal lock") = Some(path);
        Arc::new(wm)
    }

    pub fn get(&self, cursor: Cursor) -> i64 {
        self.slot(cursor).load(Ordering::Relaxed)
    }

    pub fn set(&self, cursor: Cursor, ts: i64) {
        self.slot(cursor).store(ts, Ordering::Relaxed);
        self.journal_write();
    }

    /// Cursor of a dynamically named node; 0 when it never advanced.
    pub fn get_node(&self, node: &str) -> i64 {
        self.named
            .lock()
            .expect("watermark named lock")
            .get(node)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    pub fn set_node(&self, node: &str, ts: i64) {
        let mut map = self.named.lock().expect("watermark named lock");
        let cursor = map.entry(node.to_string()).or_default();
        cursor.store(ts, Ordering::Relaxed);
        drop(map);
        self.journal_write();
    }

    /// Copy of every cursor, fixed slots and named nodes alike.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            ingest_done: self.ingest_done.load(Ordering::Relaxed),
            processed_done: self.processed_done.load(Ordering::Relaxed),
            named: self
                .named
                .lock()
                .expect("watermark named lock")
                .iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect(),
        }
    }

    fn slot(&self, cursor: Cursor) -> &AtomicI64 {
        match cursor {
            Cursor::IngestDone => &self.ingest_done,
            Cursor::ProcessedDone => &self.processed_done,
            Cursor::Named(_) => {
                // Named cursors live in `named`; callers that reach here with a
                // Named variant go through `get_node`/`set_node` instead.
                unreachable!("use get_node/set_node for named cursors")
            }
        }
    }

    fn restore(&self, snapshot: &Snapshot) {
        self.ingest_done
            .store(snapshot.ingest_done, Ordering::Relaxed);
        self.processed_done
            .store(snapshot.processed_done, Ordering::Relaxed);
        let mut map = self.named.lock().expect("watermark named lock");
        for (k, v) in &snapshot.named {
            map.insert(k.clone(), AtomicI64::new(*v));
        }
    }

    fn journal_write(&self) {
        let guard = self.journal.lock().expect("watermark journal lock");
        let Some(path) = guard.as_ref() else {
            return;
        };
        let text = match serde_json::to_string(&self.snapshot()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("watermark journal serialize failed: {e}");
                return;
            }
        };
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, &text) {
            tracing::warn!("watermark journal write failed: {e}");
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            tracing::warn!("watermark journal rename failed: {e}");
        }
    }
}

impl Default for Watermarks {
    fn default() -> Self {
        Self {
            ingest_done: AtomicI64::new(0),
            processed_done: AtomicI64::new(0),
            named: Mutex::new(HashMap::new()),
            journal: Mutex::new(None),
        }
    }
}
