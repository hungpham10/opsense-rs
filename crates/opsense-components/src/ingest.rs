//! Ingest transform: on each `tick(ts)` fetch every source, store the reduced
//! batch in the working `Raw` station and emit `data_ready(ts)` downstream.

use opsense_core::Context;
use opsense_core::Stage;
use std::io::Error;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use crate::{signal, OpsenseContext};
use opsense_macros::transform;
use tokio::sync::mpsc;

#[transform]
pub struct IngestSource {
    pub id: String,
    pub inputs: Vec<String>,
}

impl IngestSource {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: "ingest".to_string(),
            inputs: vec!["clock".to_string()],
        }
    }
}

impl Default for IngestSource {
    fn default() -> Self {
        Self::new()
    }
}

impl_ingest_source!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        while let Some(msg) = rx.recv().await {
            if signal::event(&msg) != Some(signal::TICK) {
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

            let batch = ctx.collector().collect().await;
            if !batch.is_empty() {
                // Station riêng của node = nơi lưu duy nhất cho output.
                let cache =
                    crate::station::own_station(&self.id, &self.inputs, &[Stage::Raw]).await;
                let mut g = cache.write().await;
                g.append(Stage::Raw, &batch).await;
            }
            ctx.set_watermark(opsense_core::Cursor::IngestDone, ts);

            let ready = signal::tagged(signal::data_ready(ts), &self.id);
            for stream in &tx.streams {
                let _ = stream.send(ready.clone()).await;
            }
        }
        Ok(())
    }
);
