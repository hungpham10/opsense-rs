//! Processor transform: on each `data_ready(ts)` read the raw window since its
//! cursor and publish it into `Processed`, then emit `processed(ts)`
//! downstream. The copy is pass-through today — real reshaping belongs in a
//! `rhai_transform` node.

use opsense_core::Context;
use opsense_core::{Cursor, Stage};
use std::io::Error;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use crate::{signal, OpsenseContext};
use opsense_macros::transform;
use tokio::sync::mpsc;

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
        while let Some(msg) = rx.recv().await {
            if signal::event(&msg) != Some(signal::DATA_READY) {
                continue;
            }
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };

            let ctx = tx
                .ctx
                .as_ref()
                .and_then(|c| c.as_any().downcast_ref::<OpsenseContext>())
                .ok_or_else(|| Error::other("OpsenseContext not injected into Runtime"))?;

            let from = ctx.get_watermark(Cursor::ProcessedDone);
            if ts <= from {
                continue; // nothing new since the last cycle
            }

            // Đọc cửa sổ từ station của upstream (merge cả hai stage).
            let raw = ctx
                .read_window(&self.inputs, signal::src(&msg), from, ts, None)
                .await;
            // Pass-through: Processed mirrors the raw window; reshaping lives
            // in rhai_transform nodes. Output nằm trong station riêng của node.
            if !raw.is_empty() {
                let cache =
                    crate::station::own_station(&self.id, &self.inputs, &[Stage::Processed]).await;
                let mut g = cache.write().await;
                g.append(Stage::Processed, &raw).await;
            }
            ctx.set_watermark(Cursor::ProcessedDone, ts);

            let done = signal::tagged(signal::processed(ts), &self.id);
            for stream in &tx.streams {
                let _ = stream.send(done.clone()).await;
            }
        }
        Ok(())
    }
);
