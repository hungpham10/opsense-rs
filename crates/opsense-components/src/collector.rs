//! Collector sink component: runs the collector on every tick it receives.
//!
//! The `Collector` is intentionally *not* stored in this component — it is not
//! serializable, and the `Component` trait is `#[typetag::serde]`. Instead the
//! collector is fetched from the injected [`OpsenseContext`] via `Outbound.ctx`
//! on each `run`.

use std::io::Error;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_core::Context;
use opsense_macros::sink;
use tokio::sync::mpsc;

use crate::OpsenseContext;

#[sink]
pub struct CollectorSink {
    pub id: String,
    pub inputs: Vec<String>,
}

impl CollectorSink {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: "collector".to_string(),
            inputs: vec!["clock".to_string()],
        }
    }
}

impl Default for CollectorSink {
    fn default() -> Self {
        Self::new()
    }
}

impl_collector_sink!(
    async fn run(
        &self,
        id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = tx
            .ctx
            .as_ref()
            .and_then(|c| c.as_any().downcast_ref::<OpsenseContext>())
            .ok_or_else(|| Error::other("OpsenseContext not injected into Runtime"))?;
        let collector = ctx.collector();

        loop {
            match rx.recv().await {
                Some(_) => collector.tick().await,
                None => {
                    tracing::info!("collector sink {id}: upstream closed, stopping");
                    return Ok(());
                }
            }
        }
    }
);
