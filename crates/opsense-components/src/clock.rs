//! Clock source component: emits a tick on a fixed interval.
//!
//! It drives the collector cadence. The runtime's `execute` loop cancels this
//! component on `stop()`, which ends the `tokio::select!` and returns from
//! `run`. The [`crate::vector::runtime`] macros generate the `Identify`/`Component`
//! boilerplate; only the `id`/`interval_secs` state and the `run` body remain.

use std::io::Error;
use std::time::Duration;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_macros::source;
use tokio::sync::mpsc;

#[source]
pub struct ClockSource {
    pub id: String,
    pub interval_secs: u64,
}

impl ClockSource {
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            id: "clock".to_string(),
            interval_secs: interval.as_secs().max(1),
        }
    }
}

impl_clock_source!(
    async fn run(
        &self,
        id: usize,
        _rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.interval_secs));
        loop {
            ticker.tick().await;
            // The tick carries the current timestamp — the watermark every
            // downstream node works against.
            let msg = crate::signal::tick(crate::signal::now_secs());
            for stream in &tx.streams {
                if stream.send(msg.clone()).await.is_err() {
                    tracing::warn!("clock {id}: downstream closed, stopping tick emission");
                    return Ok(());
                }
            }
        }
    }
);
