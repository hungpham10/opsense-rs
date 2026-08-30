//! Rhai transform: process each observation window with a sandboxed Rhai
//! script (`scripts/` holds examples).
//!
//! Data flows exactly like the built-in processor: any signal carrying `ts`
//! advances this node's own watermark cursor (keyed by `id`), the window
//! `(cursor, ts]` is read from the upstream station's `input_stage`, handed to
//! the script's `process(observations)` function, and results are written to
//! `output_stage` in this node's own station (the only store since the
//! persistence tier was removed). Downstream receives `processed(ts)`, so a
//! node can be chained after it unchanged.
//!
//! The script comes from `script` (inline) or `script_path` (.rhai file,
//! recompiled on change). Exactly one of the two must be set.

use std::collections::BTreeMap;
use std::io::Error;
use std::path::PathBuf;

use opsense_core::Context;
use opsense_core::Observation;
use opsense_core::Stage;

use crate::runtime::ScriptSource;
use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_components::{signal, OpsenseContext};
use opsense_macros::transform;
use tokio::sync::mpsc;

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
    /// Stage read from the working LRU.
    #[serde(default = "default_input_stage")]
    pub input_stage: Stage,
    /// Stage written with the script output.
    #[serde(default = "default_output_stage")]
    pub output_stage: Stage,
    /// Append the script output to the working LRU.
    #[serde(default = "default_true")]
    pub write_lru: bool,
    /// Also append the script output to the persistent store.
    #[serde(default)]
    pub write_store: bool,
    /// Config parameters exposed to the script as `param_<name>` global
    /// variables (e.g. `factor` becomes `param_factor`).
    #[serde(default)]
    pub params: BTreeMap<String, serde_json::Value>,
}

fn default_input_stage() -> Stage {
    Stage::Processed
}

fn default_output_stage() -> Stage {
    Stage::Processed
}

fn default_true() -> bool {
    true
}

impl RhaiTransform {
    /// Node driven by an inline script.
    #[must_use]
    pub fn new_inline(id: &str, inputs: &[&str], script: &str) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            script: script.to_string(),
            script_path: String::new(),
            input_stage: default_input_stage(),
            output_stage: default_output_stage(),
            write_lru: true,
            write_store: false,
            params: BTreeMap::new(),
        }
    }

    /// Node driven by a `.rhai` file.
    #[must_use]
    pub fn new_file(id: &str, inputs: &[&str], script_path: &str) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            script: String::new(),
            script_path: script_path.to_string(),
            input_stage: default_input_stage(),
            output_stage: default_output_stage(),
            write_lru: true,
            write_store: false,
            params: BTreeMap::new(),
        }
    }

    fn script_source(&self) -> Result<ScriptSource, Error> {
        match (!self.script.is_empty(), !self.script_path.is_empty()) {
            (true, false) => Ok(ScriptSource::Inline(self.script.clone())),
            (false, true) => Ok(ScriptSource::File(PathBuf::from(&self.script_path))),
            _ => Err(Error::other(
                "rhai transform needs exactly one of `script` or `script_path`",
            )),
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
        while let Some(msg) = rx.recv().await {
            // Any signal with a timestamp drives the node (tick from a clock,
            // data_ready/processed when chained after another transform).
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            // Fail fast on a misconfigured node before touching any state.
            let source = self.script_source()?;

            let ctx = tx
                .ctx
                .as_ref()
                .and_then(|c| c.as_any().downcast_ref::<OpsenseContext>())
                .ok_or_else(|| Error::other("OpsenseContext not injected into Runtime"))?;

            let from = ctx.get_node_watermark(&self.id);
            if ts <= from {
                continue; // nothing new since the last cycle
            }

            match self.process_window(ctx, &msg, &source, from, ts).await {
                Ok(()) => ctx.set_node_watermark(&self.id, ts),
                // Keep the cursor: the window is retried on the next signal so
                // fixing the script recovers without data loss.
                Err(e) => tracing::warn!("rhai {} skipped batch at ts {ts}: {e}", self.id),
            }

            let done = signal::tagged(signal::processed(ts), &self.id);
            for stream in &tx.streams {
                let _ = stream.send(done.clone()).await;
            }
        }
        Ok(())
    }
);

impl RhaiTransform {
    async fn process_window(
        &self,
        ctx: &OpsenseContext,
        msg: &Message,
        source: &ScriptSource,
        from: i64,
        ts: i64,
    ) -> Result<(), String> {
        // Đọc cửa sổ từ station của upstream (merge cả hai stage).
        let batch = ctx
            .read_window(&self.inputs, signal::src(msg), from, ts, None)
            .await;
        if batch.is_empty() {
            return Ok(());
        }

        let input_json = serde_json::to_value(&batch).map_err(|e| e.to_string())?;
        let items = crate::call_process_with(
            source.clone(),
            input_json,
            self.params.clone(),
            (*ctx.attributes()).clone(),
        )
        .await?;

        let mut processed = Vec::with_capacity(items.len());
        for item in items {
            let obs: Observation =
                serde_json::from_value(item).map_err(|e| format!("script output: {e}"))?;
            processed.push(obs);
        }

        if !processed.is_empty() {
            // Output nằm trong station riêng của node (nơi lưu duy nhất;
            // persistence tier đã bị gỡ, nên station là kho duy nhất).
            let cache =
                opsense_components::own_station(&self.id, &self.inputs, &[self.output_stage]).await;
            let mut g = cache.write().await;
            g.append(self.output_stage, &processed).await;
        }
        Ok(())
    }
}
