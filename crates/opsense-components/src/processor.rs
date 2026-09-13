//! `processor` — pass-through sink that mirrors upstream observations into
//! the node's own `Timeseries` station and emits `processed(ts)` downstream.
//!
//! Today this is a thin copy station: a separate reshape node (e.g. a Rhai
//! or jq transform) would mutate the payload between this node and its
//! upstream. Keeping the copy in one place makes the contract obvious — every
//! signal that comes in lands in the station, every signal goes out.

use std::io::Error;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{RwLock, mpsc};

use opsense_core::Station;
use opsense_core::TimeseriesStation;
use opsense_macros::transform;

use crate::signal;
use crate::station::{downcast_ctx, extract_observations};
use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[transform]
pub struct ProcessorTransform {
    pub id: String,
    pub inputs: Vec<String>,
}

impl ProcessorTransform {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: "processor".to_string(),
            inputs: vec!["ingest".to_string()],
        }
    }
}

impl Default for ProcessorTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl_processor_transform!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        ctx.registry(
            &self.id,
            Station::Timeseries(Arc::new(RwLock::new(TimeseriesStation::default()))),
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
            .await?;

        while let Some(msg) = rx.recv().await {
            // Forward every signal so downstream nodes see the full stream.
            for s in &tx.streams {
                let _ = s.send(msg.clone()).await;
            }

            // Only `data_ready` (and other ts-bearing) signals move the
            // station forward; ticks/control events are forwarded but ignored.
            if signal::event(&msg) != Some(signal::DATA_READY) {
                continue;
            }
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            // The payload may already carry an `observations` array (set by
            // the upstream source) — extract it and write into our station.
            let batch = extract_observations(&msg.payload);
            if batch.is_empty() {
                let _ = ts;
                continue;
            }
            let from = batch.iter().map(|o| o.ts).min().unwrap_or(ts);
            let to = batch.iter().map(|o| o.ts).max().unwrap_or(ts);
            me.write().await.update_range(&batch, from, to, to);

            // Hint: payload could also be a single observation-shaped object
            // for sources that emit one item per cycle.
            if let Some(obj) = msg.payload.as_object()
                && obj.contains_key("metric_id")
                && !obj.contains_key("observations")
                && let Ok(obs) =
                    serde_json::from_value::<opsense_core::Observation>(Value::Object(obj.clone()))
            {
                let ts = obs.ts;
                me.write().await.update_range(&[obs], ts, ts, ts);
            }

            let done = signal::tagged(signal::processed(ts), &self.id);
            for s in &tx.streams {
                let _ = s.send(done.clone()).await;
            }
        }
        Ok(())
    }
);
