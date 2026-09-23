//! Opsense components: the vector dataflow [`Component`]s that drive collection.
//!
//! This crate holds every Opsense-specific component registered into the
//! `opsense_mlib::vector` [`Runtime`]. It is split from `opsense-core` so the
//! growing pile of components cannot bloat the pure domain crate.
//!
//! Nodes register their own stations into the process-wide
//! [`opsense_core::Context`] via [`Context::registry`]. The runtime injects
//! `Context` into every component through `Outbound.ctx`, so each `run()` can
//! both write to its own station and read from upstream stations.

use std::collections::BTreeMap;

pub mod http;
pub mod processor;
pub mod qlib;
pub mod station;
pub mod telegram;

/// Pipeline signal helpers shared by graph nodes.
pub mod signal {
    use serde_json::{Value, json};

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

    /// Tag the signal with its producer so consumers know which station to read.
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

    /// Read the producer tag stamped by [`tagged`].
    #[must_use]
    pub fn src(msg: &Message) -> Option<&str> {
        msg.payload.get("src").and_then(Value::as_str)
    }

    #[must_use]
    pub fn now_secs() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0)
    }
}

pub use qlib::QlibEngine;
pub use station::{
    CategoryStationTransform, PatternStationTransform, TimeseriesStationSink,
    TimeseriesStationTransform,
};
pub use telegram::TelegramSink;

/// Render `{{name}}` placeholders in a template using the provided vars.
///
/// 1-pass scan: any `{{ ... }}` segment is trimmed and looked up in `vars`.
/// Missing variable → `Err`. No nesting, no escaping, no `{{` inside `{{`.
/// Used by `http.rs` to interpolate URL, headers, params, and body fields.
pub fn render(template: &str, vars: &BTreeMap<String, String>) -> Result<String, String> {
    let bytes = template.as_bytes();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            // Find the closing `}}`.
            let mut j = i + 2;
            while j + 1 < bytes.len() && !(bytes[j] == b'}' && bytes[j + 1] == b'}') {
                j += 1;
            }
            if j + 1 >= bytes.len() {
                return Err(format!("unterminated placeholder starting at byte {i}"));
            }
            let raw = &template[i + 2..j];
            let key = raw.trim();
            if key.is_empty() {
                return Err(format!("empty placeholder at byte {i}"));
            }
            let value = vars
                .get(key)
                .ok_or_else(|| format!("missing variable `{key}`"))?;
            out.push_str(value);
            i = j + 2;
        } else {
            // Push one char safely (template is UTF-8).
            let ch = template[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok(out)
}

/// Re-export of the `vector` runtime under `crate::vector::runtime`.
///
/// `opsense-macros`' `#[source]`/`#[sink]`/`#[transform]`/`#[input]`/`#[output]`
/// attributes expand to code that refers to `crate::vector::runtime::*`; this
/// mirror lets those macros be used from this crate exactly as they are from
/// `opsense-mlib`.
pub mod vector {
    pub use opsense_mlib::vector::runtime;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn render_basic() {
        let v = vars(&[("a", "1")]);
        assert_eq!(render("http://x/?a={{a}}", &v).unwrap(), "http://x/?a=1");
    }

    #[test]
    fn render_multiple() {
        let v = vars(&[("a", "1"), ("b", "2")]);
        assert_eq!(render("{{a}}-{{b}}", &v).unwrap(), "1-2");
    }

    #[test]
    fn render_whitespace() {
        let v = vars(&[("a", "1")]);
        assert_eq!(render("{{ a }}", &v).unwrap(), "1");
    }

    #[test]
    fn render_missing() {
        let v = vars(&[("a", "1")]);
        let err = render("{{b}}", &v).unwrap_err();
        assert!(err.contains("missing variable `b`"));
    }

    #[test]
    fn render_no_placeholder() {
        let v = vars(&[("a", "1")]);
        assert_eq!(render("plain text", &v).unwrap(), "plain text");
    }

    #[test]
    fn render_empty_placeholder() {
        let v = vars(&[]);
        assert!(render("{{}}", &v).is_err());
        assert!(render("{{   }}", &v).is_err());
    }

    #[test]
    fn render_unterminated() {
        let v = vars(&[("a", "1")]);
        assert!(render("{{a", &v).is_err());
    }

    #[test]
    fn render_literal_braces() {
        // Single `{` or `}` is passed through; only `{{` opens a placeholder.
        let v = vars(&[("a", "1")]);
        assert_eq!(render("curly {a} end", &v).unwrap(), "curly {a} end");
    }
}
