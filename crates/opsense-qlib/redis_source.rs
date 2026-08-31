use std::collections::HashMap;
use std::io::{Error, ErrorKind};

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

use models::resolver::Resolver;
use vector_config_macro::source;
use vector_runtime::{Component, Context, Event, Identify, Message as VectorMessage, Outbound};

/// Chế độ đọc của RedisSource.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum RedisSourceMode {
    /// Đọc Redis Stream qua consumer group (XREADGROUP). Job pending của
    /// consumer được drain lại lúc khởi động; ack tự động nếu `auto_ack`.
    Stream {
        key: String,
        group: String,
        consumer: String,
        #[serde(default = "default_block_ms")]
        block_ms: u64,
        #[serde(default = "default_auto_ack")]
        auto_ack: bool,
    },
    /// Subscribe Pub/Sub channel.
    PubSub { channel: String },
    /// BRPOP List (hàng đợi đơn giản).
    List {
        key: String,
        #[serde(default = "default_block_ms")]
        block_ms: u64,
    },
}

fn default_block_ms() -> u64 {
    5_000
}

fn default_auto_ack() -> bool {
    true
}

/// Reply của XREADGROUP: stream-key -> [(entry-id, field-map)]
type StreamReply = Vec<(String, Vec<(String, HashMap<String, Option<String>>)>)>;

/// Lấy connection từ Resolver trong runtime context (quản lý Redis tập trung:
/// DSN từ env/Infisical, shard theo tenant). Chỉ fallback sang `uri` khi
/// runtime không có context (vd chạy test độc lập).
pub(super) async fn resolve_connection(
    uri: Option<&str>,
    tenant_id: i64,
    ctx: Option<&dyn Context>,
) -> Result<redis::aio::MultiplexedConnection, Error> {
    if let Some(ctx) = ctx
        && let Some(resolver) = ctx.as_any().downcast_ref::<Resolver>()
    {
        return Ok(resolver.cache(tenant_id));
    }

    let uri = uri.ok_or_else(|| {
        Error::other("Thiếu Redis context (Resolver) trong runtime và không có `uri` fallback")
    })?;
    let client = redis::Client::open(uri)
        .map_err(|e| Error::other(format!("Invalid redis uri {uri}: {e}")))?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| Error::other(format!("Failed to connect redis {uri}: {e}")))
}

#[source]
pub struct RedisSource {
    pub id: String,
    /// Fallback khi runtime không có Resolver context (test độc lập)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Connection lấy theo tenant: resolver.cache(tenant_id)
    #[serde(default)]
    pub tenant_id: i64,
    pub mode: RedisSourceMode,
}

/// Entry của stream có field "payload" (chuỗi JSON) thì dùng luôn; nếu chỉ có
/// đúng 1 field thì lấy field đó; còn lại bọc nguyên map thành JSON object.
fn entry_payload(fields: &HashMap<String, Option<String>>) -> Result<Value, Error> {
    let invalid = |raw: &str, error: serde_json::Error| {
        Error::new(
            ErrorKind::InvalidData,
            format!("Malformed JSON from redis stream entry: {error} (raw: {raw})"),
        )
    };

    if let Some(Some(raw)) = fields.get("payload") {
        return serde_json::from_str(raw).map_err(|e| invalid(raw, e));
    }

    if fields.len() == 1
        && let Some(Some(raw)) = fields.values().next()
    {
        return serde_json::from_str(raw).map_err(|e| invalid(raw, e));
    }

    let flattened: HashMap<&str, &String> = fields
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k.as_str(), v)))
        .collect();
    serde_json::to_value(flattened)
        .map_err(|e| Error::other(format!("Failed to serialize stream entry fields: {e}")))
}

async fn forward(fields: &HashMap<String, Option<String>>, tx: &Outbound) -> Result<(), Error> {
    let payload = entry_payload(fields)?;
    for sender in tx.streams.as_slice() {
        let _ = sender
            .send(VectorMessage {
                payload: payload.clone(),
            })
            .await;
    }
    Ok(())
}

type StreamEntries = Vec<(String, HashMap<String, Option<String>>)>;

/// Đọc một batch pending (id "0") của consumer — dùng để redo sau restart.
async fn read_pending_batch(
    key: &str,
    group: &str,
    consumer: &str,
    conn: &mut redis::aio::MultiplexedConnection,
) -> Result<StreamEntries, Error> {
    let result: Option<StreamReply> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(16)
        .arg("STREAMS")
        .arg(key)
        .arg("0")
        .query_async(conn)
        .await
        .map_err(|e| Error::other(format!("XREADGROUP pending failed: {e}")))?;

    Ok(result
        .and_then(|mut v| v.pop())
        .map(|(_, entries)| entries)
        .unwrap_or_default())
}

async fn forward_entries(id: usize, entries: StreamEntries, tx: &Outbound) -> Vec<String> {
    let mut ack_ids: Vec<String> = Vec::with_capacity(entries.len());
    for (entry_id, fields) in entries {
        if let Err(error) = forward(&fields, tx).await {
            let _ = tx.event.send(Event::Minor((id, error))).await;
        }
        ack_ids.push(entry_id);
    }
    ack_ids
}

async fn ack_entries(
    key: &str,
    group: &str,
    ack_ids: &[String],
    conn: &mut redis::aio::MultiplexedConnection,
) {
    if ack_ids.is_empty() {
        return;
    }
    let _: Result<(), redis::RedisError> = redis::cmd("XACK")
        .arg(key)
        .arg(group)
        .arg(ack_ids)
        .query_async(conn)
        .await;
}

/// PubSub cần dedicated connection (`Client::get_async_pubsub`) nên chỉ dùng
/// được qua `uri` — Resolver context chỉ expose MultiplexedConnection.
async fn open_pubsub(uri: Option<&str>) -> Result<redis::aio::PubSub, Error> {
    let uri = uri
        .ok_or_else(|| Error::other("RedisSource chế độ pubsub yêu cầu `uri` (Resolver context không expose pubsub connection)"))?;
    let client = redis::Client::open(uri)
        .map_err(|e| Error::other(format!("Invalid redis uri {uri}: {e}")))?;
    client
        .get_async_pubsub()
        .await
        .map_err(|e| Error::other(format!("Failed to open redis pubsub {uri}: {e}")))
}

impl_redis_source!(
    async fn run(
        &self,
        id: usize,
        _: &mut mpsc::Receiver<VectorMessage>,
        tx: Outbound,
    ) -> Result<(), Error> {
        match self.mode.clone() {
            RedisSourceMode::Stream {
                key,
                group,
                consumer,
                block_ms,
                auto_ack,
            } => {
                let mut conn =
                    resolve_connection(self.uri.as_deref(), self.tenant_id, tx.ctx.as_deref())
                        .await?;
                // Tạo group nếu chưa có (bỏ qua BUSYGROUP)
                let _: Result<(), redis::RedisError> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(&key)
                    .arg(&group)
                    .arg("$")
                    .arg("MKSTREAM")
                    .query_async(&mut conn)
                    .await;

                // Drain pending của chính consumer này (redo sau restart)
                let pending = read_pending_batch(&key, &group, &consumer, &mut conn).await?;
                let ack_ids = forward_entries(id, pending, &tx).await;
                if auto_ack {
                    ack_entries(&key, &group, &ack_ids, &mut conn).await;
                }

                // Vòng chính: chỉ đọc entry mới
                loop {
                    let result: Option<StreamReply> = redis::cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&group)
                        .arg(&consumer)
                        .arg("COUNT")
                        .arg(16)
                        .arg("BLOCK")
                        .arg(block_ms)
                        .arg("STREAMS")
                        .arg(&key)
                        .arg(">")
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| Error::other(format!("XREADGROUP failed: {e}")))?;

                    let Some((_, entries)) = result.and_then(|mut v| v.pop()) else {
                        continue;
                    };

                    let ack_ids = forward_entries(id, entries, &tx).await;
                    if auto_ack {
                        ack_entries(&key, &group, &ack_ids, &mut conn).await;
                    }
                }
            }
            RedisSourceMode::PubSub { channel } => {
                let mut pubsub = open_pubsub(self.uri.as_deref()).await?;
                pubsub
                    .subscribe(&channel)
                    .await
                    .map_err(|e| Error::other(format!("SUBSCRIBE {channel} failed: {e}")))?;

                use futures_util::StreamExt;
                let mut stream = pubsub.on_message();
                while let Some(message) = stream.next().await {
                    let raw: String = message
                        .get_payload()
                        .map_err(|e| Error::other(format!("Invalid pubsub payload: {e}")))?;
                    if raw.is_empty() {
                        continue;
                    }
                    let payload: Value = match serde_json::from_str(&raw) {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = tx
                                .event
                                .send(Event::Minor((
                                    id,
                                    Error::new(
                                        ErrorKind::InvalidData,
                                        format!("Malformed JSON from pubsub: {error}"),
                                    ),
                                )))
                                .await;
                            continue;
                        }
                    };

                    for sender in tx.streams.as_slice() {
                        let _ = sender
                            .send(VectorMessage {
                                payload: payload.clone(),
                            })
                            .await;
                    }
                }

                Ok(())
            }
            RedisSourceMode::List { key, block_ms } => {
                let mut conn =
                    resolve_connection(self.uri.as_deref(), self.tenant_id, tx.ctx.as_deref())
                        .await?;
                let timeout_secs = (block_ms / 1000).max(1) as usize;
                loop {
                    let popped: Option<(String, String)> = redis::cmd("BRPOP")
                        .arg(&key)
                        .arg(timeout_secs)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| Error::other(format!("BRPOP {key} failed: {e}")))?;

                    let Some((_, raw)) = popped else {
                        continue;
                    };
                    if raw.is_empty() {
                        continue;
                    }

                    let payload: Value = match serde_json::from_str(&raw) {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = tx
                                .event
                                .send(Event::Minor((
                                    id,
                                    Error::new(
                                        ErrorKind::InvalidData,
                                        format!("Malformed JSON from list: {error}"),
                                    ),
                                )))
                                .await;
                            continue;
                        }
                    };

                    for sender in tx.streams.as_slice() {
                        let _ = sender
                            .send(VectorMessage {
                                payload: payload.clone(),
                            })
                            .await;
                    }
                }
            }
        }
    }
);
