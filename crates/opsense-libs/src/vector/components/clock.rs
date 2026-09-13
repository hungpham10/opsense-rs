use std::io::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use crate::vector::runtime::{Component, Identify, Message, Outbound};
use opsense_macros::source;
use tokio::sync::mpsc;

#[source]
pub struct Clock {
    pub id: String,
    pub interval_secs: u64,
}

impl Clock {
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            id: "clock".to_string(),
            interval_secs: interval.as_secs().max(1),
        }
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn tick_signal(ts: i64) -> Message {
    Message {
        payload: json!({"event": "tick", "ts": ts}),
    }
}

impl_clock!(
    async fn run(
        &self,
        id: usize,
        _rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.interval_secs));
        loop {
            ticker.tick().await;
            let msg = tick_signal(now_secs());

            for stream in &tx.streams {
                if stream.send(msg.clone()).await.is_err() {
                    log::warn!("clock {id}: downstream closed, stopping tick emission");
                    return Ok(());
                }
            }
        }
    }
);
