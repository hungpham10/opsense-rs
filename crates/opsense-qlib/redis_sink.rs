use std::io::{Error, ErrorKind};

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

use vector_config_macro::sink;
use vector_runtime::{Component, Event, Identify, Message, Outbound};

use super::redis_source::resolve_connection;

/// Chế độ ghi của RedisSink.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum RedisSinkMode {
    /// XADD payload vào stream.
    Stream { key: String },
    /// LPUSH payload vào list; nếu có `cap` sẽ LTRIM giữ `cap` phần tử mới nhất.
    List { key: String, cap: Option<usize> },
    /// PUBLISH payload lên channel.
    Publish { channel: String },
    /// HSET field với giá trị payload.
    Hash { key: String, field: String },
}

#[sink]
pub struct RedisSink {
    pub id: String,
    pub inputs: Vec<String>,
    /// Fallback khi runtime không có Resolver context (test độc lập)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Connection lấy theo tenant: resolver.cache(tenant_id)
    #[serde(default)]
    pub tenant_id: i64,
    pub mode: RedisSinkMode,
}

/// Render template dạng `backtest:job:{job_id}:events`: thay `{field}` bằng
/// giá trị top-level tương ứng trong payload (giữ nguyên nếu không tìm thấy).
fn render_template(template: &str, payload: &Value) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars();

    while let Some(current) = chars.next() {
        if current != '{' {
            out.push(current);
            continue;
        }

        let mut field = String::new();
        let mut closed = false;
        for next in chars.by_ref() {
            if next == '}' {
                closed = true;
                break;
            }
            field.push(next);
        }

        if !closed || field.is_empty() {
            out.push('{');
            out.push_str(&field);
            if closed {
                out.push('}');
            }
            continue;
        }

        match payload.get(&field) {
            Some(Value::String(text)) => out.push_str(text),
            Some(Value::Number(number)) => out.push_str(&number.to_string()),
            Some(Value::Bool(flag)) => out.push_str(if *flag { "true" } else { "false" }),
            Some(Value::Null) => out.push_str("null"),
            Some(other) => out.push_str(&other.to_string()),
            None => {
                out.push('{');
                out.push_str(&field);
                out.push('}');
            }
        }
    }

    out
}

impl_redis_sink!(
    async fn run(
        &self,
        id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let mut conn =
            resolve_connection(self.uri.as_deref(), self.tenant_id, tx.ctx.as_deref()).await?;

        let invalid =
            |error: redis::RedisError| Error::other(format!("Redis write failed: {error}"));

        while let Some(message) = rx.recv().await {
            let raw = match serde_json::to_string(&message.payload) {
                Ok(raw) => raw,
                Err(error) => {
                    let _ = tx
                        .event
                        .send(Event::Minor((
                            id,
                            Error::new(
                                ErrorKind::InvalidData,
                                format!("Failed to serialize payload: {error}"),
                            ),
                        )))
                        .await;
                    continue;
                }
            };

            let result: Result<(), redis::RedisError> = match &self.mode {
                RedisSinkMode::Stream { key } => {
                    redis::cmd("XADD")
                        .arg(render_template(key, &message.payload))
                        .arg("*")
                        .arg("payload")
                        .arg(&raw)
                        .query_async(&mut conn)
                        .await
                }
                RedisSinkMode::List { key, cap } => {
                    let rendered = render_template(key, &message.payload);
                    let written: Result<(), redis::RedisError> = redis::cmd("LPUSH")
                        .arg(&rendered)
                        .arg(&raw)
                        .query_async(&mut conn)
                        .await;
                    if written.is_ok()
                        && let Some(cap) = cap
                    {
                        let _: Result<(), redis::RedisError> = redis::cmd("LTRIM")
                            .arg(&rendered)
                            .arg(0)
                            .arg((*cap as i64) - 1)
                            .query_async(&mut conn)
                            .await;
                    }
                    written
                }
                RedisSinkMode::Publish { channel } => {
                    redis::cmd("PUBLISH")
                        .arg(render_template(channel, &message.payload))
                        .arg(&raw)
                        .query_async(&mut conn)
                        .await
                }
                RedisSinkMode::Hash { key, field } => {
                    redis::cmd("HSET")
                        .arg(render_template(key, &message.payload))
                        .arg(render_template(field, &message.payload))
                        .arg(&raw)
                        .query_async(&mut conn)
                        .await
                }
            };

            if let Err(error) = result.map_err(invalid) {
                let _ = tx.event.send(Event::Minor((id, error))).await;
            }
        }

        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_template() {
        let payload = serde_json::json!({
            "job_id": "abc-123",
            "progress": 42,
            "active": true
        });

        assert_eq!(
            render_template("backtest:job:{job_id}:events", &payload),
            "backtest:job:abc-123:events"
        );
        assert_eq!(render_template("static:key", &payload), "static:key");
        assert_eq!(
            render_template("backtest:{job_id}:{progress}:{active}", &payload),
            "backtest:abc-123:42:true"
        );
        assert_eq!(
            render_template("keep:{unknown_field}", &payload),
            "keep:{unknown_field}"
        );
    }
}
