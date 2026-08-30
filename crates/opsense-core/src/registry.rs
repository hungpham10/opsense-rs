//! Relocated station registry (formerly `store::{register_station, station,
//! station_ids, describe_station, ensure_pattern, ensure_search, text_index}`).
//!
//! After the "station is the only cache" refactor the observation backends
//! (`CacheStore`/`FallbackStore`/`LmdbStore`/`DuckDbStore`) are gone; what
//! remains is a single in-process `Stations` registry shared by every
//! `OpsenseContext` and by the HTTP/MCP/Rhai serving paths. This module is the
//! process-global handle to that registry.
//!
//! All functions are `async` because `Stations` uses `tokio::sync::RwLock`
//! (guards must be `Send` across `.await` inside `read_window`).

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock, Mutex};

use opsense_libs::ahocorasick::AhoCorasick;
use opsense_libs::search::Search;
use serde_json::Value as Json;
use tokio::sync::RwLock;

use crate::station::{CategoryStation, PatternStation, Station};
use crate::Stations;

/// Handle to a single registered station (the tokio `RwLock` lets the guard
/// survive `.await` inside `read_window` and the serving paths).
pub type StationHandle = Arc<RwLock<Station>>;

/// Process-global station registry. This is the ONE shared registry: every
/// `OpsenseContext::new_stations()` returns a clone of this, every component
/// publishes through it (via `ensure_station`/`ensure_pattern`/`ensure_search`),
/// and the HTTP/MCP/Rhai serving paths read through it directly — so a station
/// published by one component is visible to all of them.
///
/// Initialized to a fresh empty map (NOT via `OpsenseContext::new_stations`,
/// which clones `REGISTRY` itself — calling that here would recurse).
pub static REGISTRY: LazyLock<Stations> =
    LazyLock::new(|| Arc::new(RwLock::new(std::collections::HashMap::new())));

/// Clone of the global registry handle.
#[must_use]
pub fn stations() -> Stations {
    REGISTRY.clone()
}

/// Insert a station under `id`, first-wins. Returns `false` if already present.
pub async fn register_station(id: &str, station: Arc<RwLock<Station>>) -> bool {
    let mut map = REGISTRY.write().await;
    if map.contains_key(id) {
        false
    } else {
        map.insert(id.to_string(), station);
        true
    }
}

/// Fetch a station handle by id (without locking it for read yet).
pub async fn station(id: &str) -> Option<Arc<RwLock<Station>>> {
    REGISTRY.read().await.get(id).cloned()
}

/// All registered station ids.
pub async fn station_ids() -> Vec<String> {
    REGISTRY.read().await.keys().cloned().collect()
}

/// Non-blocking, best-effort snapshot of station ids for **sync** contexts
/// (e.g. REPL tab-completion) that cannot `.await`. Returns an empty list if
/// the registry write lock is contended; callers treat that as "no hints".
#[must_use]
pub fn station_ids_snapshot() -> Vec<String> {
    match REGISTRY.try_read() {
        Ok(map) => map.keys().cloned().collect(),
        Err(_) => Vec::new(),
    }
}

/// Describe a station for HTTP `/describe` and MCP (`backend` field).
pub async fn describe_station(id: &str) -> Option<Json> {
    let st = station(id).await?;
    let g = st.read().await;
    Some(g.describe())
}

/// Get-or-create the pattern (Aho-Corasick) station for `node`.
pub async fn ensure_pattern(node: &str) -> Arc<RwLock<Station>> {
    if let Some(existing) = station(node).await {
        return existing;
    }
    let created = Arc::new(RwLock::new(Station::Pattern(PatternStation {
        automaton: AhoCorasick::new(),
        hits: AtomicU64::new(0),
        misses: AtomicU64::new(0),
    })));
    register_station(node, created.clone()).await;
    created
}

/// Get-or-create the search (radix + KMP) station for `node`.
pub async fn ensure_search(node: &str) -> Arc<RwLock<Station>> {
    if let Some(existing) = station(node).await {
        return existing;
    }
    let created = Arc::new(RwLock::new(Station::Category(CategoryStation {
        search: Search::in_memory(1),
        entries: Mutex::new(BTreeMap::new()),
        next_idx: AtomicU64::new(1),
    })));
    register_station(node, created.clone()).await;
    created
}

/// Fetch the pattern/search station for `node` (used by Rhai/MCP queries).
pub async fn text_index(node: &str) -> Option<Arc<RwLock<Station>>> {
    station(node).await
}
