//! Per-call station lookups: `station_query(station, from_ts, to_ts)`.
//!
//! Scripts are otherwise stateless between calls, and the transform must not
//! grow per-feature plumbing (shared globals, snapshots, …). Instead a script
//! can read any registered station by name through the pipeline `Context` —
//! pull raw observations from a source station (`window-feed`, `live-feed`),
//! or hold state across calls by re-reading what it wrote to its own station
//! on a previous message.
//!
//! The bindings are synchronous to the engine: they block on a tokio handle
//! captured by the caller (`Handle::block_on`), so the thread-local script
//! evaluation never crosses an `.await` and station lookups layer cleanly on
//! the existing sync `spawn_blocking` runner.

use opsense_core::{Context, TimeseriesStation};
use rhai::Dynamic;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Register `station_query` on the engine for this call.
///
/// Stations are resolved through the per-call pipeline `Context`, so the
/// bindings are re-registered per call — same pattern as
/// [`crate::attributes::register`].
pub fn register(eng: &mut rhai::Engine, ctx: Arc<Context>, handle: tokio::runtime::Handle) {
    eng.register_fn(
        "station_query",
        move |name: String, from: i64, to: i64| -> Dynamic {
            match query_recent(&ctx, &handle, &name, from, to) {
                Some(obs) => rhai::serde::to_dynamic(&obs).unwrap_or(Dynamic::UNIT),
                None => Dynamic::UNIT,
            }
        },
    );
}

/// `station_query(station, from_ts, to_ts) -> Array of observation maps` (or
/// `()` when the station is missing — the script decides how to degrade).
///
/// Backed by [`TimeseriesStation::query_recent`]: trả mọi observation thực sự
/// có trong cửa sổ dù coverage hổng (`query_range` trả `None` khi bất kỳ
/// block nào trong cửa sổ chưa cover trọn → vô dụng cho script muốn "cho tôi
/// dữ liệu gần now").
fn query_recent(
    ctx: &Arc<Context>,
    handle: &tokio::runtime::Handle,
    name: &str,
    from_ts: i64,
    to_ts: i64,
) -> Option<Vec<opsense_core::Observation>> {
    handle.block_on(async {
        let Ok(st) = ctx.station::<Arc<RwLock<TimeseriesStation>>>(name).await else {
            return None;
        };
        st.write().await.query_recent(from_ts, to_ts).await
    })
}
