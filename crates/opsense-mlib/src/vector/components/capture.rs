use std::io::Error;
use std::sync::{Arc, Mutex};

use opsense_macros::sink;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::vector::runtime::{Component, Identify, Message, Outbound};

fn default_max_messages() -> usize {
    1024
}

/// Bounded in-memory sink for observing messages that passed through the runtime.
///
/// The retained payloads are intentionally test and diagnostics oriented; use a
/// finite `max_messages` in long-running pipelines.
#[sink(exclude(PartialEq))]
pub struct CaptureSink {
    pub id: String,
    pub inputs: Vec<String>,

    #[serde(default = "default_max_messages")]
    pub max_messages: usize,

    #[serde(skip, default)]
    messages: Arc<Mutex<Vec<Value>>>,
}

impl PartialEq for CaptureSink {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.inputs == other.inputs
            && self.max_messages == other.max_messages
    }
}

impl CaptureSink {
    #[must_use]
    pub fn new(id: impl Into<String>, inputs: Vec<String>, max_messages: usize) -> Self {
        Self {
            id: id.into(),
            inputs,
            max_messages: max_messages.max(1),
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a point-in-time copy so callers do not hold the sink lock.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Value> {
        self.messages
            .lock()
            .map(|messages| messages.clone())
            .unwrap_or_default()
    }
}

impl_capture_sink!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        _: Outbound,
    ) -> Result<(), Error> {
        let limit = self.max_messages.max(1);
        while let Some(message) = rx.recv().await {
            let mut messages = self
                .messages
                .lock()
                .map_err(|_| Error::other("capture state lock poisoned"))?;
            messages.push(message.payload);
            if messages.len() > limit {
                let overflow = messages.len() - limit;
                messages.drain(0..overflow);
            }
        }
        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_retains_only_the_newest_messages() {
        let sink = CaptureSink::new("capture", vec!["upstream".into()], 2);
        let (tx, mut rx) = mpsc::channel(3);
        let (event_tx, _) = mpsc::channel(1);
        let outbound = Outbound {
            streams: Vec::new(),
            broadcast: None,
            event: event_tx,
            ctx: None,
        };

        for value in [1, 2, 3] {
            tx.send(Message {
                payload: Value::from(value),
            })
            .await
            .unwrap();
        }
        drop(tx);

        sink.run(0, &mut rx, outbound).await.unwrap();
        assert_eq!(sink.snapshot(), vec![Value::from(2), Value::from(3)]);
    }

    #[test]
    fn component_roundtrips_without_capture_state() {
        let value = serde_json::json!({
            "type": "capture_sink",
            "id": "capture",
            "inputs": ["upstream"],
            "max_messages": 7,
        });
        let component: Box<dyn Component> = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(component).unwrap(), value);
    }
}
