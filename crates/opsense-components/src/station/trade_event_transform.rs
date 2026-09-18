//! `trade_event_station_transform` persists full JSON before forwarding it.
//!
//! Requires `[storage] backend = "sqlite"` and the `sqlite` feature. Writes
//! await the existing SQLite backend's WAL/autocommit; there is no extra fsync
//! or remote replication barrier. Memory and other backends fail closed.
//!
//! Identity is `(station id, broker, event_id)`, not timestamp. Identical
//! retries reuse the stored payload; conflicting reuse of an identity fails.
//! Use one writer per station: the storage trait has no cross-instance CAS.
//! Forwarding is at-least-once on upstream retry, not a transactional outbox.
//! Readback is by identity; this is not registered as an observation station.

use std::io::{Error, ErrorKind};
use std::sync::Arc;

use opsense_core::Context;
use opsense_libs::storage::TimeseriesStorage;
use opsense_macros::transform;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use super::downcast_ctx;
use crate::vector::runtime::{Component, Identify, Message, Outbound};

#[transform]
pub struct TradeEventStationTransform {
    pub id: String,
    pub inputs: Vec<String>,
}

impl TradeEventStationTransform {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str]) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
        }
    }
}

/// Identity-addressed JSON persistence using the Context storage configuration.
/// Reopen with the same id/config to read committed events after restart.
pub struct TradeEventStation {
    id: String,
    storage: Arc<dyn TimeseriesStorage>,
    writer: Mutex<()>,
}

impl TradeEventStation {
    /// Opens `<data_dir>/<id>.trade-events.sqlite`; ids are safe filename stems.
    /// Retention and mirroring are unsupported rather than silently ignored.
    pub async fn from_context(id: &str, ctx: &Context) -> Result<Self, Error> {
        let cfg = ctx.storage();
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "trade station id must be 1..128 ASCII letters, digits, '-' or '_'",
            ));
        }
        if cfg.backend != "sqlite"
            || cfg.data_dir.is_empty()
            || cfg.data_dir.contains("://")
            || cfg.mirror.is_some()
            || cfg.retention_secs != 0
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "trade events require local sqlite storage without retention or mirroring",
            ));
        }
        #[cfg(feature = "sqlite")]
        {
            let path =
                std::path::Path::new(&cfg.data_dir).join(format!("{id}.trade-events.sqlite"));
            let storage = opsense_libs::storage::SqliteStorage::open(&path.to_string_lossy())
                .await
                .map_err(Error::other)?;
            Ok(Self {
                id: id.to_string(),
                storage: Arc::new(storage),
                writer: Mutex::new(()),
            })
        }
        #[cfg(not(feature = "sqlite"))]
        Err(Error::new(
            ErrorKind::InvalidInput,
            "trade event persistence requires the `sqlite` feature",
        ))
    }

    fn key(&self, broker: &str, event_id: &Value) -> Result<Vec<u8>, Error> {
        let valid_id = event_id.as_str().is_some_and(|id| !id.trim().is_empty())
            || event_id.as_i64().is_some()
            || event_id.as_u64().is_some();
        if broker.trim().is_empty() || !valid_id {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "broker must be nonempty and event_id must be a nonempty string or integer",
            ));
        }
        serde_json::to_vec(&("trade-event-v1", &self.id, broker, event_id)).map_err(Error::other)
    }

    /// Loads the full event; numeric and string event ids are distinct.
    pub async fn read_event(&self, broker: &str, event_id: &Value) -> Result<Option<Value>, Error> {
        let key = self.key(broker, event_id)?;
        self.read_key(&key).await
    }

    async fn read_key(&self, key: &[u8]) -> Result<Option<Value>, Error> {
        self.storage
            .last(key)
            .await
            .map_err(Error::other)?
            .map(|(_, bytes)| {
                serde_json::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))
            })
            .transpose()
    }

    /// Commits once per identity for this writer; preserves all unknown fields.
    pub async fn persist(&self, payload: &Value) -> Result<(), Error> {
        let broker = payload.get("broker").and_then(Value::as_str).unwrap_or("");
        let event_id = payload.get("event_id").unwrap_or(&Value::Null);
        let key = self.key(broker, event_id)?;
        if payload.get("ts").and_then(Value::as_i64).is_none()
            || !payload
                .get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
            || payload.get("event").is_none_or(Value::is_null)
        {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "trade payload requires integer ts, nonempty symbol and non-null event",
            ));
        }
        let bytes = serde_json::to_vec(payload).map_err(Error::other)?;
        let _guard = self.writer.lock().await;
        if let Some(existing) = self.read_key(&key).await? {
            return if existing == *payload {
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "trade event identity already contains a different payload",
                ))
            };
        }
        // One series per identity; the original timestamp lives in the JSON.
        self.storage
            .append(&key, 0, &bytes)
            .await
            .map_err(Error::other)
    }

    async fn run(&self, rx: &mut mpsc::Receiver<Message>, tx: &Outbound) -> Result<(), Error> {
        while let Some(msg) = rx.recv().await {
            self.persist(&msg.payload).await?;
            for stream in &tx.streams {
                stream
                    .send(msg.clone())
                    .await
                    .map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;
            }
        }
        Ok(())
    }
}

impl_trade_event_station_transform!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        let station = TradeEventStation::from_context(&self.id, ctx).await?;
        station.run(rx, &tx).await
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_core::Config;
    use opsense_libs::storage::{InMemoryStorage, StorageError};
    use opsense_model::secret::Secret;
    use serde_json::json;
    use tokio::sync::Notify;

    fn payload(id: Value) -> Value {
        json!({"event_id": id, "ts": 1700000000, "broker": "paper", "symbol": "BTC/USD", "event": {"fill": {"qty": "0.01", "price": "42000.50"}}, "extra": [1, {"keep": true}]})
    }

    fn station(storage: Arc<dyn TimeseriesStorage>) -> TradeEventStation {
        TradeEventStation {
            id: "trades".into(),
            storage,
            writer: Mutex::new(()),
        }
    }

    fn outbound() -> (Outbound, mpsc::Receiver<Message>) {
        let (stream, rx) = mpsc::channel(8);
        let (event, _) = mpsc::channel(8);
        (
            Outbound {
                streams: vec![stream],
                broadcast: None,
                event,
                ctx: None,
            },
            rx,
        )
    }

    #[test]
    fn registered_component_roundtrips() {
        let value =
            json!({"type": "trade_event_station_transform", "id": "trades", "inputs": ["broker"]});
        let component: Box<dyn Component> = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&component).unwrap(), value);
        assert_eq!(
            component
                .as_any()
                .downcast_ref::<TradeEventStationTransform>()
                .unwrap(),
            &TradeEventStationTransform::new("trades", &["broker"])
        );
    }

    struct GatedStorage {
        memory: InMemoryStorage,
        entered: Notify,
        release: Notify,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl TimeseriesStorage for GatedStorage {
        async fn last(&self, key: &[u8]) -> opsense_libs::storage::Result<Option<(u64, Vec<u8>)>> {
            self.memory.last(key).await
        }

        async fn append(
            &self,
            key: &[u8],
            ts: u64,
            bytes: &[u8],
        ) -> opsense_libs::storage::Result<()> {
            self.entered.notify_one();
            self.release.notified().await;
            if self.fail {
                return Err(StorageError::Internal("injected write failure".into()));
            }
            self.memory.append(key, ts, bytes).await
        }
    }

    async fn gated_forward(fail: bool) {
        let storage = Arc::new(GatedStorage {
            memory: InMemoryStorage::new(),
            entered: Notify::new(),
            release: Notify::new(),
            fail,
        });
        let station = Arc::new(station(storage.clone()));
        let (input, mut rx) = mpsc::channel(1);
        let expected = payload(json!("a"));
        input
            .send(Message {
                payload: expected.clone(),
            })
            .await
            .unwrap();
        drop(input);
        let (tx, mut output) = outbound();
        let worker = station.clone();
        let task = tokio::spawn(async move { worker.run(&mut rx, &tx).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            storage.entered.notified(),
        )
        .await
        .unwrap();
        assert!(matches!(
            output.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            station.read_event("paper", &json!("a")).await.unwrap(),
            None
        );
        storage.release.notify_one();
        let result = task.await.unwrap();
        if fail {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected write failure")
            );
            assert!(output.recv().await.is_none());
            assert_eq!(
                station.read_event("paper", &json!("a")).await.unwrap(),
                None
            );
        } else {
            result.unwrap();
            assert_eq!(
                station.read_event("paper", &json!("a")).await.unwrap(),
                Some(expected.clone())
            );
            assert_eq!(output.recv().await.unwrap().payload, expected);
        }
    }

    #[tokio::test]
    async fn persistence_completes_before_forward() {
        gated_forward(false).await;
    }

    #[tokio::test]
    async fn persistence_failure_prevents_forward() {
        gated_forward(true).await;
    }

    #[tokio::test]
    async fn same_timestamp_events_survive_and_retries_are_idempotent() {
        let station = station(Arc::new(InMemoryStorage::new()));
        for id in [json!("a"), json!("b"), json!(7), json!("7")] {
            let value = payload(id.clone());
            station.persist(&value).await.unwrap();
            station.persist(&value).await.unwrap();
            assert_eq!(station.read_event("paper", &id).await.unwrap(), Some(value));
            let key = station.key("paper", &id).unwrap();
            assert_eq!(station.storage.range(&key, 0, 0).await.unwrap().len(), 1);
        }
        let mut conflict = payload(json!("a"));
        conflict["ts"] = json!(1700000001);
        assert_eq!(
            station.persist(&conflict).await.unwrap_err().kind(),
            ErrorKind::AlreadyExists
        );
        assert_eq!(
            station.read_event("paper", &json!("a")).await.unwrap(),
            Some(payload(json!("a")))
        );
        let mut other_broker = payload(json!("a"));
        other_broker["broker"] = json!("other");
        station.persist(&other_broker).await.unwrap();
        assert_eq!(
            station.read_event("other", &json!("a")).await.unwrap(),
            Some(other_broker)
        );
    }

    #[tokio::test]
    async fn malformed_payloads_fail_without_forwarding() {
        for field in ["event_id", "ts", "broker", "symbol", "event"] {
            let station = station(Arc::new(InMemoryStorage::new()));
            let mut value = payload(json!("a"));
            value.as_object_mut().unwrap().remove(field);
            let (input, mut rx) = mpsc::channel(1);
            input.send(Message { payload: value }).await.unwrap();
            drop(input);
            let (tx, mut output) = outbound();
            assert_eq!(
                station.run(&mut rx, &tx).await.unwrap_err().kind(),
                ErrorKind::InvalidData
            );
            assert!(output.try_recv().is_err());
        }
    }

    async fn context(storage: Value) -> Context {
        let cfg: Config = serde_json::from_value(json!({"storage": storage})).unwrap();
        Context::new(&cfg, Arc::new(Secret::new().await.unwrap()))
    }

    #[tokio::test]
    async fn rejects_nondurable_storage() {
        let memory = context(json!({})).await;
        assert!(
            TradeEventStation::from_context("trades", &memory)
                .await
                .is_err()
        );
        let sqlite = context(json!({"backend": "sqlite"})).await;
        assert!(
            TradeEventStation::from_context("../escape", &sqlite)
                .await
                .is_err()
        );
        #[cfg(not(feature = "sqlite"))]
        assert!(
            TradeEventStation::from_context("trades", &sqlite)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("feature")
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_restart_reads_full_events_and_deduplicates_retries() {
        let dir = std::env::temp_dir().join(format!(
            "opsense-trade-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let storage = json!({"backend": "sqlite", "data_dir": dir.to_string_lossy()});
        {
            let ctx = context(storage.clone()).await;
            let store = TradeEventStation::from_context("trades", &ctx)
                .await
                .unwrap();
            for id in ["a", "b"] {
                store.persist(&payload(json!(id))).await.unwrap();
            }
        }
        {
            let ctx = context(storage.clone()).await;
            let store = TradeEventStation::from_context("trades", &ctx)
                .await
                .unwrap();
            for id in ["a", "b"] {
                let value = payload(json!(id));
                assert_eq!(
                    store.read_event("paper", &json!(id)).await.unwrap(),
                    Some(value.clone())
                );
                store.persist(&value).await.unwrap();
                let key = store.key("paper", &json!(id)).unwrap();
                assert_eq!(store.storage.range(&key, 0, 0).await.unwrap().len(), 1);
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
