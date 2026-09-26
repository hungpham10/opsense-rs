//! Rhai transform: process each observation window with a sandboxed Rhai
//! script (`examples/prometheus-demo/rhai/` holds examples).
//!
//! Data flow follows the modern component pattern:
//!  1. A message with `event: "data_ready"` (or similar) arrives from upstream.
//!  2. Extract observations from the message payload.
//!  3. Run the script's `process(observations)` function.
//!  4. Append script output to this node's own `TimeseriesStation`.
//!  5. Forward a `processed(ts)` message downstream.
//!
//! The script comes from `script` (inline) or `script_path` (.rhai file,
//! recompiled on mtime change). Exactly one of the two must be set.
//!
//! Configurable parameters (via `params` map in the pipeline config) are
/// exposed to the script as global variables.
use std::collections::BTreeMap;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;

use opsense_components::signal;
use opsense_components::station::{downcast_ctx, extract_observations};
use opsense_core::{Observation, Station, TimeseriesStation};
use opsense_macros::transform;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};

use crate::runtime::ScriptSource;
use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[transform]
pub struct RhaiTransform {
    pub id: String,
    pub inputs: Vec<String>,
    /// Inline Rhai script defining `fn process(observations)`.
    #[serde(default)]
    pub script: String,
    /// Path to a `.rhai` file instead of an inline script.
    #[serde(default)]
    pub script_path: String,
    /// Script parameters exposed as global variables.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
}

impl RhaiTransform {
    #[must_use]
    pub fn new_inline(id: &str, inputs: &[&str], script: &str) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            script: script.to_string(),
            script_path: String::new(),
            params: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn new_file(id: &str, inputs: &[&str], script_path: &str) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            script: String::new(),
            script_path: script_path.to_string(),
            params: BTreeMap::new(),
        }
    }

    fn script_source(&self) -> Result<ScriptSource, String> {
        if !self.script.is_empty() {
            Ok(ScriptSource::Inline(self.script.clone()))
        } else if !self.script_path.is_empty() {
            Ok(ScriptSource::File(PathBuf::from(&self.script_path)))
        } else {
            Err("rhai_transform: exactly one of `script` or `script_path` must be set".into())
        }
    }
}

impl_rhai_transform!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        let station = TimeseriesStation::from_storage(&self.id, ctx.storage()).await?;
        ctx.registry(
            &self.id,
            Station::Timeseries(Arc::new(RwLock::new(station))),
        )
        .await
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;

        let me = ctx
            .station::<Arc<RwLock<TimeseriesStation>>>(&self.id)
            .await?
            .clone();

        while let Some(msg) = rx.recv().await {
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            // Extract observations from the upstream message payload. Empty
            // batches ([...]) are still handed to the script: a clock ping
            // carries no observations, and the script drives itself off
            // station lookups (`station_query`) instead.
            let batch = extract_observations(&msg.payload);

            let source = match self.script_source() {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("rhai {}: {}", self.id, e);
                    continue;
                }
            };

            // Fetch attributes for this call (async, so do it per-batch)
            let attributes = ctx.get_attributes().await;

            // Which upstream produced this message? An explicit `trigger` field
            // (added by a passthrough json_2_json stage, e.g. constants) wins;
            // otherwise fall back to `src` (stamped by `signal::tagged`) — the
            // script branches on it via `trigger()` (see call_process_with).
            let trigger = msg
                .payload
                .get("trigger")
                .or_else(|| msg.payload.get("src"))
                .and_then(Value::as_str)
                .map(str::to_string);

            // Run the script. The pipeline context is handed over so the
            // script can read any registered station by name — no per-feature
            // globals or snapshots are injected here.
            //
            // Allowlist ghi: `params.write_stations` + own station. Own station
            // luôn được phép vì ghi ngầm (script trả về ⇒ ghi vào own station)
            // vẫn là mặc định; node muốn bỏ hẳn thì đặt
            // `params.implicit_station_write = false` và tự `station_write`.
            let mut write_stations: Vec<String> = self
                .params
                .get("write_stations")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            write_stations.push(self.id.clone());

            let items = match crate::call_process_with(
                source,
                serde_json::to_value(&batch).unwrap_or(Value::Array(Vec::new())),
                self.params.clone(),
                attributes,
                trigger,
                Some(Arc::new(ctx.clone())),
                Arc::new(write_stations),
            )
            .await
            {
                Ok(items) => items,
                Err(e) => {
                    tracing::warn!("rhai {} skipped batch at ts {ts}: {e}", self.id);
                    // Still forward processed to not stall downstream
                    let done = signal::tagged(signal::processed(ts), &self.id);
                    for s in &tx.streams {
                        let _ = s.send(done.clone()).await;
                    }
                    continue;
                }
            };

            // Convert script output back to Observations and write to own station
            let mut processed = Vec::with_capacity(items.len());
            for item in items {
                match serde_json::from_value::<Observation>(item) {
                    Ok(obs) => processed.push(obs),
                    Err(e) => tracing::warn!("rhai {}: script output parse error: {e}", self.id),
                }
            }

            // Ghi ngầm vào own station: MẶC ĐỊNH CÒN BẬT (script trả về gì thì
            // own station nhận nấy) vì 12 script trong repo dựa vào nó. Node muốn
            // "script tự quyết gì ghi" thì đặt
            // `params.implicit_station_write = false` và gọi `station_write` tường
            // minh — lúc đó return chỉ còn nghĩa forward xuống sink.
            let implicit = self
                .params
                .get("implicit_station_write")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            if implicit && !processed.is_empty() {
                // Range the write over the script's own output timestamps, so
                // observations whose ts differ from the trigger batch (a ping
                // with an empty payload) still land in their blocks.
                let from = processed.iter().map(|o| o.ts).min().unwrap_or(ts);
                let to = processed.iter().map(|o| o.ts).max().unwrap_or(ts);
                me.write().await.update_range(&processed, from, to, ts);
            }

            // Forward processed downstream; script output đi kèm dưới dạng
            // body `data` opaque để sink/transform phía sau tiêu thụ generic.
            let out = if processed.is_empty() {
                signal::processed(ts)
            } else {
                signal::processed_with(ts, serde_json::to_value(&processed).unwrap_or(Value::Null))
            };
            let done = signal::tagged(out, &self.id);
            for s in &tx.streams {
                let _ = s.send(done.clone()).await;
            }
        }
        Ok(())
    }
);
