//! Opsense MCP session: the tools over the vector `Runtime`.
//!
//! - [`init`] (`opsense_init`): open a session from `.opsense/config.toml`,
//!   deserialize the `[pipeline]` components (typetag) and start the runtime.
//! - [`deinit`] (`opsense_deinit`): stop the pipeline, close the session.
//! - [`status`] (`opsense_status`): per-node state via `Runtime::topology`
//!   plus watermark cursors.
//! - [`edit`] (`opsense_edit`): realtime pipeline edit — pass the *complete*
//!   desired component list; `Runtime::reload` diffs it (add/update/remove),
//!   validates links and rejects dead nodes, reporting errors back.
//! - [`run`] (`opsense_run`): manual trigger — inject a `tick(ts)` into a
//!   node (`Runtime::inject`), the "retest" half of the plugin playground.
//! - [`query`] (`opsense_query`): read observations back from the working
//!   LRU or the persistence store.
//!
//! The MCP layer is built on the official `rmcp` SDK ([`server`]); swapping to
//! a network transport later only replaces the `stdio()` call in
//! `server::run`.

mod server;
mod lock;

pub use server::{mcp_handler, run};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use opsense_components::{new_station_registry, signal, OpsenseContext};
use opsense_core::collector::Collector;
use opsense_core::config::Config;
use opsense_core::registry::{describe_station, station_ids};
use opsense_core::Context;
use opsense_core::{Cursor, Watermarks};
use opsense_libs::vector::runtime::{Component, Event, Runtime};
// Force-link the scripted-transform crate so its typetag registration lands
// in the `Component` registry — `[pipeline]` tables can then deserialize
// `type = "rhai_transform"` nodes. (`init` also calls
// `opsense_rhai::register()` for the scripted `processor`/transform path.)
use tokio::sync::RwLock;

/// Maximum recent runtime events kept for `opsense_status`.
const MAX_EVENTS: usize = 100;

/// One open Opsense MCP session.
///
/// Dropping the session releases the directory lock, so exactly one session
/// can run per `.opsense` directory (see [`lock::SessionLock`]).
pub struct Session {
    pub config_path: PathBuf,
    pub collector: Arc<Collector>,
    /// Watermark cursors of the chain plus per-node cursors of plugins.
    watermarks: Arc<Watermarks>,
    pub runtime: Arc<RwLock<Runtime>>,
    events: Arc<Mutex<Vec<String>>>,
    _lock: lock::SessionLock,
}

/// Deserialize one `[pipeline.components]` table (typetag `type` key) into a
/// runtime-ready component.
pub fn deserialize_component(value: &serde_json::Value) -> Result<Arc<dyn Component>, String> {
    serde_json::from_value::<Box<dyn Component>>(value.clone())
        .map(Arc::from)
        .map_err(|e| format!("component `{value}`: {e}"))
}

/// `opsense_init`: load config, acquire the single-session lock, build the
/// pipeline (with its storage backends) and start the runtime. Returns the
/// session plus an initial status summary so callers need no extra round
/// trip.
pub async fn init(path: &Path) -> Result<(Session, serde_json::Value), String> {
    // Make `format = "script"` usable in http_source nodes.
    opsense_rhai::register();

    let cfg = Config::load(path).map_err(|e| e.to_string())?;

    // Exactly one session per config directory; dropped on any later failure.
    let lock = lock::SessionLock::acquire(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;

    let ctx = OpsenseContext::from_config(&cfg, new_station_registry());
    let collector = ctx.collector().clone();
    let watermarks = ctx.watermarks().clone();
    let components = opsense_components::pipeline_from_config(&cfg)?;

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    runtime.reload(components).map_err(|e| e.to_string())?;

    let ev = events.clone();
    runtime
        .start(move |event| {
            let ev = ev.clone();
            async move {
                let line = match event {
                    Event::Minor((id, e)) => format!("minor node {id}: {e}"),
                    Event::Major((id, e)) => format!("major node {id}: {e}"),
                    Event::Panic((id, e)) => format!("panic node {id}: {e}"),
                };
                let mut list = ev.lock().unwrap();
                list.push(line);
                let excess = list.len().saturating_sub(MAX_EVENTS);
                if excess > 0 {
                    list.drain(..excess);
                }
            }
        })
        .map_err(|e| e.to_string())?;

    let session = Session {
        config_path: path.to_path_buf(),
        collector,
        runtime: Arc::new(RwLock::new(runtime)),
        watermarks,
        events,
        _lock: lock,
    };
    let summary = status(&session).await?;
    Ok((session, summary))
}

/// `opsense_deinit`: stop the pipeline and close the session.
pub async fn deinit(session: &Session) -> Result<String, String> {
    let runtime = session.runtime.read().await;
    runtime.stop().map_err(|e| e.to_string())?;
    runtime
        .wait_for_shutdown()
        .await
        .map_err(|e| e.to_string())?;
    Ok("session closed".to_string())
}

/// `opsense_status`: per-node type/links/run-state, pipeline watermarks and
/// recent runtime events.
pub async fn status(session: &Session) -> Result<serde_json::Value, String> {
    let runtime = session.runtime.read().await;
    let wm = &session.watermarks;
    let named: BTreeMap<String, i64> = wm.snapshot().named;
    // Stations registered by the pipeline's TimeseriesStationSinks: ids plus a live
    // describe (backend + config + internal metrics + upstream deps).
    let mut stations: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for id in station_ids().await {
        if let Some(v) = describe_station(&id).await {
            stations.insert(id, v);
        }
    }
    Ok(serde_json::json!({
        "config": session.config_path.display().to_string(),
        "nodes": runtime.topology(),
        "watermarks": {
            "ingest_done": wm.get(Cursor::IngestDone),
            "processed_done": wm.get(Cursor::ProcessedDone),
            "named": named,
        },
        "stations": stations,
        "recent_events": *session.events.lock().unwrap(),
    }))
}

/// `opsense_edit`: realtime edit — `components` is the complete desired list;
/// the runtime diffs (add/update/remove), validates links and dead nodes.
pub async fn edit(
    session: &Session,
    components: Vec<serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let parsed = components
        .iter()
        .map(deserialize_component)
        .collect::<Result<Vec<_>, _>>()?;

    let runtime = session.runtime.read().await;
    runtime.reload(parsed).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "reloaded": true, "nodes": runtime.topology() }))
}

/// `opsense_run`: manually inject a `tick(ts)` into a pipeline node — the
/// trigger behind the "retest" playground loop (edit script → edit reload →
/// run → query). Defaults to the `ingest` node at the current timestamp.
/// (Named `run_pipeline` here: `server::run` owns the MCP stdio loop.)
pub async fn run_pipeline(
    session: &Session,
    node: Option<String>,
    ts: Option<i64>,
) -> Result<serde_json::Value, String> {
    let node = node.unwrap_or_else(|| "ingest".to_string());
    let ts = ts.unwrap_or_else(signal::now_secs);
    let message = signal::tick(ts);

    session
        .runtime
        .read()
        .await
        .inject(node.clone(), message)
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "injected": { "node": node, "event": "tick", "ts": ts } }))
}

/// `opsense_backfill`: yêu cầu một http_source re-fetch một cửa sổ cũ
/// `(from_ts, to_ts]` — phục hồi dữ liệu trạm đã bị evict/rotation. Watermark
/// của node không lùi nên luồng thu thập thường không bị ảnh hưởng; re-delivery
/// được tầng station dedup theo `(metric_id, ts)`.
pub async fn backfill(
    session: &Session,
    node: String,
    from_ts: i64,
    to_ts: i64,
) -> Result<serde_json::Value, String> {
    if to_ts <= from_ts {
        return Err(format!("backfill window empty: ({from_ts}, {to_ts}]"));
    }
    session
        .runtime
        .read()
        .await
        .inject(node.clone(), signal::backfill(from_ts, to_ts))
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "injected": { "node": node, "event": "backfill", "from_ts": from_ts, "to_ts": to_ts }
    }))
}

use std::time::Duration;

/// Wall-clock budget for a `opsense_query` read, overridable via
/// `OPSENSE_QUERY_TIMEOUT_SECS` (default 30). Long windows over the parquet
/// lakehouse are bounded so a caller never hangs; the read is cancelled and a
/// clear error returned.
pub(crate) fn query_timeout() -> Duration {
    let secs: u64 = std::env::var("OPSENSE_QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(30);
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_status_edit_deinit_cycle() {
        let dir = std::env::temp_dir().join(format!("opsense-mcp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("config.toml");
        std::fs::write(
            &cfg_path,
            r#"
[capacity]
cpu = 8.0

[pipeline]
[[pipeline.components]]
type = "clock_source"
id = "clock"
interval_secs = 1

[[pipeline.components]]
type = "collector_sink"
id = "collector"
inputs = ["clock"]
"#,
        )
        .unwrap();

        // init: both nodes running, clock wired into collector; the summary
        // comes back with the session (no separate status round trip).
        let (session, st) = init(&cfg_path).await.expect("init");
        let nodes = st["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);
        let collector = nodes.iter().find(|n| n["id"] == "collector").unwrap();
        assert_eq!(collector["inputs"], serde_json::json!(["clock"]));
        assert_eq!(collector["running"], true);

        // edit: remove every node (stop), then re-add the same graph.
        edit(&session, vec![]).await.expect("edit to empty");
        let st = status(&session).await.unwrap();
        assert!(st["nodes"].as_array().unwrap().is_empty());

        edit(
            &session,
            vec![
                serde_json::json!({"type": "clock_source", "id": "clock", "interval_secs": 1}),
                serde_json::json!({
                    "type": "collector_sink",
                    "id": "collector",
                    "inputs": ["clock"],
                }),
            ],
        )
        .await
        .expect("edit re-add");
        let st = status(&session).await.unwrap();
        assert_eq!(st["nodes"].as_array().unwrap().len(), 2);

        // edit must reject a broken graph (sink pointing at a missing node).
        let err = edit(
            &session,
            vec![serde_json::json!({
                "type": "collector_sink",
                "id": "orphan",
                "inputs": ["ghost"],
            })],
        )
        .await
        .unwrap_err();
        assert!(err.contains("ghost"), "unexpected error: {err}");

        deinit(&session).await.expect("deinit");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unknown_component_type_is_rejected() {
        assert!(deserialize_component(&serde_json::json!({"type": "nope"})).is_err());
    }

    #[test]
    fn single_session_per_directory() {
        let dir = std::env::temp_dir().join(format!("opsense-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lock_path = dir.join("session.lock");

        // Live owner (this process) blocks a second session…
        let held = lock::SessionLock::acquire(&dir).expect("first acquire");
        let err = lock::SessionLock::acquire(&dir).unwrap_err();
        assert!(err.contains("already running"), "unexpected error: {err}");
        assert!(lock_path.exists());

        // …dropping it releases the lock for the next one.
        drop(held);
        assert!(!lock_path.exists());
        let again = lock::SessionLock::acquire(&dir).expect("acquire after release");
        drop(again);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stale_lock_from_dead_process_is_reclaimed() {
        let dir = std::env::temp_dir().join(format!("opsense-stale-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // A child process that already exited → its PID is dead.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id() as i32;
        child.wait().unwrap();
        std::fs::write(dir.join("session.lock"), dead_pid.to_string()).unwrap();

        let reclaimed =
            lock::SessionLock::acquire(&dir).expect("stale lock must be reclaimed, not rejected");
        drop(reclaimed);
        std::fs::remove_dir_all(&dir).ok();
    }
}
