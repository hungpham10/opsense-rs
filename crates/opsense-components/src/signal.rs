//! Pipeline signal schema — the contract between graph nodes.
//!
//! Any Source can drive the chain by emitting these signals (Clock today; a
//! Kafka/Redis/RabbitMQ consumer tomorrow). A signal always carries the
//! timestamp ("watermark") it is valid for:
//!
//! ```json
//! {"event": "tick",        "ts": 1690003600}
//! {"event": "data_ready",  "ts": 1690003600}
//! {"event": "processed",   "ts": 1690003600}
//! ```

use serde_json::{json, Value};

use crate::vector::runtime::Message;

pub const TICK: &str = "tick";
pub const DATA_READY: &str = "data_ready";
pub const PROCESSED: &str = "processed";
pub const BACKFILL: &str = "backfill";

#[must_use]
pub fn tick(ts: i64) -> Message {
    Message {
        payload: json!({"event": TICK, "ts": ts}),
    }
}

#[must_use]
pub fn data_ready(ts: i64) -> Message {
    Message {
        payload: json!({"event": DATA_READY, "ts": ts}),
    }
}

#[must_use]
pub fn processed(ts: i64) -> Message {
    Message {
        payload: json!({"event": PROCESSED, "ts": ts}),
    }
}

#[must_use]
pub fn backfill(from_ts: i64, to_ts: i64) -> Message {
    Message {
        payload: json!({"event": BACKFILL, "from_ts": from_ts, "to_ts": to_ts}),
    }
}

/// Đóng dấu nguồn phát vào signal để consumer biết đọc station của ai.
#[must_use]
pub fn tagged(mut msg: Message, src: &str) -> Message {
    if let Some(obj) = msg.payload.as_object_mut() {
        obj.insert("src".into(), serde_json::json!(src));
    }
    msg
}

#[must_use]
pub fn event(msg: &Message) -> Option<&str> {
    msg.payload.get("event").and_then(Value::as_str)
}

#[must_use]
pub fn ts(msg: &Message) -> Option<i64> {
    msg.payload.get("ts").and_then(Value::as_i64)
}

/// Đọc tag `"src"` mà producer stamp lên signal (xem `signal::tagged`).
/// Trả về `None` nếu upstream không đóng dấu, cho phép `read_window` rơi về
/// logic fallback (single input / merge tất cả inputs).
#[must_use]
pub fn src(msg: &Message) -> Option<&str> {
    msg.payload.get("src").and_then(Value::as_str)
}

#[must_use]
pub fn now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
