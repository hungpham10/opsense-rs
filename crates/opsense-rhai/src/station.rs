//! Per-call station lookups: `station_query(station, from_ts, to_ts [, signal
//! [, label_kind]])` and `station_candles(station, from_ts, to_ts, resolution)`.
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
//! **Lọc server-side (`signal` / `label_kind`) là bắt buộc, không phải tiện ích.**
//! Giới hạn `max_map_size` của Rhai là *engine-wide*, tính trên mọi map sống
//! cùng lúc (`runtime.rs:164` = 100_000; xem [`crate::series`] giải thích kỹ).
//! Một observation là một map lồng `labels` map, nên trên station có tick —
//! aggTrade BTC vài nghìn obs/phút — lệnh `station_query` cả giờ sẽ vượt trần và
//! **cả batch script bị bỏ** với `Size of object map too large`. Đo thật trên
//! `strategies/binance`: 26_680 observation trong 1 giờ ⇒ node `grid` im bặt.
//! Lọc ở Rust rồi mới `to_dynamic` giữ kích thước tải về script nhỏ và đúng
//! thứ script cần — cùng bộ lọc mà `Query.queryTimeseries` đã có.
//!
//! `()` nghĩa là "không lọc" (giữ đúng ngữ nghĩa của lời gọi cũ 3 tham số).
//!
//! The bindings are synchronous to the engine: they block on a tokio handle
//! captured by the caller (`Handle::block_on`), so the thread-local script
//! evaluation never crosses an `.await` and station lookups layer cleanly on
//! the existing sync `spawn_blocking` runner. Reads take the station's *read*
//! lock — no writer contention, so a candle-hungry script cannot stall the
//! collectors writing into the same station.

use opsense_core::{Context, Observation, TimeseriesStation, TimeseriesStationHandle};
use opsense_model::events::Signal;
use opsense_qlib::DataLoader;
use rhai::{Dynamic, EvalAltResult};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Register `station_query` (3/4/5 tham số) and `station_candles` on the engine
/// for this call.
///
/// Stations are resolved through the per-call pipeline `Context`, so the
/// bindings are re-registered per call — same pattern as
/// [`crate::attributes::register`].
pub fn register(eng: &mut rhai::Engine, ctx: Arc<Context>, handle: tokio::runtime::Handle) {
    // Arity 3: không lọc (giữ hành vi cũ cho script đang chạy).
    let (ctx3, handle3) = (ctx.clone(), handle.clone());
    eng.register_fn(
        "station_query",
        move |name: String, from: i64, to: i64| -> Result<Dynamic, Box<EvalAltResult>> {
            run_query(&ctx3, &handle3, &name, from, to, None, None)
        },
    );
    // Arity 4: lọc theo `signal`.
    let (ctx4, handle4) = (ctx.clone(), handle.clone());
    eng.register_fn(
        "station_query",
        move |name: String, from: i64, to: i64, signal: Dynamic| -> Result<Dynamic, Box<EvalAltResult>> {
            run_query(
                &ctx4,
                &handle4,
                &name,
                from,
                to,
                read_signal(&signal),
                None,
            )
        },
    );
    // Arity 5: lọc theo `signal` + `labels.kind`.
    let (ctx5, handle5) = (ctx.clone(), handle.clone());
    eng.register_fn(
        "station_query",
        move |name: String,
              from: i64,
              to: i64,
              signal: Dynamic,
              label_kind: Dynamic|
              -> Result<Dynamic, Box<EvalAltResult>> {
            run_query(
                &ctx5,
                &handle5,
                &name,
                from,
                to,
                read_signal(&signal),
                read_string(&label_kind),
            )
        },
    );
    let (ctx6, handle6) = (ctx.clone(), handle.clone());
    eng.register_fn(
        "station_candles",
        move |name: String, from: i64, to: i64, resolution: String| -> Dynamic {
            match query_candles(&ctx6, &handle6, &name, from, to, &resolution) {
                Some(candles) => rhai::serde::to_dynamic(&candles).unwrap_or(Dynamic::UNIT),
                None => Dynamic::UNIT,
            }
        },
    );
}

/// Đọc 1 tham số lọc kiểu string; `()` / rỗng ⇒ `None` (không lọc).
fn read_string(v: &Dynamic) -> Option<String> {
    if v.is_unit() {
        return None;
    }
    v.clone().try_cast::<String>().filter(|s| !s.is_empty())
}

/// `Signal` từ chuỗi; `()` / rỗng ⇒ `None`. Chuỗi không hợp lệ cũng là `None`
/// (bỏ lọc) — script vẫn chạy được, chỉ là không lọc, thay vì ném lỗi làm cả
/// batch chết.
fn read_signal(v: &Dynamic) -> Option<Signal> {
    let raw = read_string(v)?;
    serde_json::from_value(serde_json::Value::String(raw)).ok()
}

/// Lọc **trước** khi chuyển sang Dynamic — nếu lọc sau thì mọi map vẫn được
/// dựng và vẫn chạm trần `max_map_size`.
fn filter_obs(rows: Vec<Observation>, signal: Option<Signal>, label_kind: Option<&str>) -> Vec<Observation> {
    rows.into_iter()
        .filter(|o| signal.is_none_or(|want| o.signal == want))
        .filter(|o| match label_kind {
            None => true,
            Some(k) => o.labels.get("kind").map(String::as_str) == Some(k),
        })
        .collect()
}

fn run_query(
    ctx: &Arc<Context>,
    handle: &tokio::runtime::Handle,
    name: &str,
    from: i64,
    to: i64,
    signal: Option<Signal>,
    label_kind: Option<String>,
) -> Result<Dynamic, Box<EvalAltResult>> {
    match query_recent(ctx, handle, name, from, to) {
        Some(rows) => {
            let rows = filter_obs(rows, signal, label_kind.as_deref());
            // KHÔNG `unwrap_or(UNIT)`: khi vượt trần `max_map_size`, `to_dynamic`
            // lỗi và trả UNIT ⇒ script thấy station **trống**, tức "không có
            // lệnh nào" — tệ hơn hẳn lỗi rõ ràng, vì chiến lược im lặng hỏng
            // thay vì báo. Ném lỗi để batch bị bỏ kèm lý do.
            rhai::serde::to_dynamic(&rows).map_err(|e| {
                let msg = format!(
                    "station_query({name:?}, {from}, {to}) trả {} observation nhưng \
                     không chuyển nổi sang Rhai (thường là vượt max_map_size — \
                     hãy lọc bằng tham số signal/label_kind): {e}",
                    rows.len()
                );
                EvalAltResult::ErrorRuntime(msg.into(), rhai::Position::NONE).into()
            })
        }
        // Station không tồn tại: script tự quyết định cách sống thiếu.
        None => Ok(Dynamic::UNIT),
    }
}

/// `station_query(station, from_ts, to_ts [, signal [, label_kind]]) -> Array of
/// observation maps` (or `()` when the station is missing — the script decides
/// how to degrade).
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
) -> Option<Vec<Observation>> {
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