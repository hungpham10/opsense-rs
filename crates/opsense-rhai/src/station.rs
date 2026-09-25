//! Per-call station lookups: `station_query(station, from_ts, to_ts)` and
//! `station_candles(station, from_ts, to_ts, resolution)`.
//!
//! Scripts are otherwise stateless between calls, and the transform must not
//! grow per-feature plumbing (shared globals, snapshots, …). Instead a script
//! can read any registered station by name through the pipeline `Context` —
//! pull raw observations from a source station (`window-feed`, `live-feed`),
//! or hold state across calls by re-reading what it wrote to its own station
//! on a previous message. `station_candles` exposes the station through the
//! qlib [`DataLoader`] surface, so a grid strategy can consume real candles
//! exactly as the backtest domain does.
//!
//! The bindings are synchronous to the engine: they block on a tokio handle
//! captured by the caller (`Handle::block_on`), so the thread-local script
//! evaluation never crosses an `.await` and station lookups layer cleanly on
//! the existing sync `spawn_blocking` runner. Reads take the station's *read*
//! lock — no writer contention, so a candle-hungry script cannot stall the
//! collectors writing into the same station.

use opsense_core::{Context, TimeseriesStation, TimeseriesStationHandle};
use opsense_qlib::DataLoader;
use rhai::Dynamic;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Register `station_query` and `station_candles` on the engine for this call.
///
/// Stations are resolved through the per-call pipeline `Context`, so the
/// bindings are re-registered per call — same pattern as
/// [`crate::attributes::register`].
pub fn register(eng: &mut rhai::Engine, ctx: Arc<Context>, handle: tokio::runtime::Handle) {
    let (ctx2, handle2) = (ctx.clone(), handle.clone());
    eng.register_fn(
        "station_query",
        move |name: String, from: i64, to: i64| -> Dynamic {
            match query_recent(&ctx, &handle, &name, from, to) {
                Some(obs) => rhai::serde::to_dynamic(&obs).unwrap_or(Dynamic::UNIT),
                None => Dynamic::UNIT,
            }
        },
    );
    eng.register_fn(
        "station_candles",
        move |name: String, from: i64, to: i64, resolution: String| -> Dynamic {
            match query_candles(&ctx2, &handle2, &name, from, to, &resolution) {
                Some(candles) => rhai::serde::to_dynamic(&candles).unwrap_or(Dynamic::UNIT),
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
        let Ok(st) = station_handle(ctx, name).await else {
            return None;
        };
        st.read().await.query_recent(from_ts, to_ts).await
    })
}

/// `station_candles(station, from_ts, to_ts, resolution) -> Array of candle
/// maps` (`{t, o, h, l, c, v}`, or `()` when the station is missing).
///
/// The station is read through [`TimeseriesStationHandle`], which implements
/// the qlib [`DataLoader`] trait — same read path the trading domain uses.
/// Observations must follow the shared OHLCV convention (field o/h/l/c/v +
/// optional resolution label); `resolution` filters those labels.
///
/// Window bounds are signed (script-side, matching `station_query`) while the
/// qlib [`DataLoader::range`] window is unsigned: a negative bound is clamped
/// at zero instead of wrapping into an astronomically large window.
fn query_candles(
    ctx: &Arc<Context>,
    handle: &tokio::runtime::Handle,
    name: &str,
    from: i64,
    to: i64,
    resolution: &str,
) -> Option<Vec<opsense_qlib::CandleStick>> {
    let from = from.max(0) as u64;
    let to = to.max(0) as u64;
    handle.block_on(async {
        let Ok(st) = station_handle(ctx, name).await else {
            return None;
        };
        DataLoader::range(&TimeseriesStationHandle::new(st), from, to, resolution)
            .await
            .ok()
    })
}

async fn station_handle(
    ctx: &Context,
    name: &str,
) -> Result<Arc<RwLock<TimeseriesStation>>, std::io::Error> {
    ctx.station::<Arc<RwLock<TimeseriesStation>>>(name).await
}