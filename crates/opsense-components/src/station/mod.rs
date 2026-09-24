//! Station components.
//!
//! Each component in this module is a `#[source]` / `#[transform]` / `#[sink]`
//! that owns a single station (Timeseries / Category / Pattern). Stations
//! are registered into the process-wide [`opsense_core::Context`] via
//! [`Context::registry`] and read back via [`Context::station`].
//!
//! Lookups: in-process via `Context::station::<T>(id)`, or through the
//! GraphQL `Query.stations` / `queryTimeseries` / `queryCatalog` /
//! `queryPattern` resolvers.

mod category_transform;
mod pattern_transform;
mod timeseries_sink;
mod timeseries_transform;

pub use category_transform::CategoryStationTransform;
pub use pattern_transform::PatternStationTransform;
pub use timeseries_sink::TimeseriesStationSink;
pub use timeseries_transform::TimeseriesStationTransform;

use opsense_core::Context;
use opsense_core::Observation;
use serde_json::Value;
use std::io::Error;

use crate::vector::runtime::Outbound;

pub fn downcast_ctx(tx: &Outbound) -> Result<&Context, Error> {
    tx.ctx
        .as_deref()
        .ok_or_else(|| Error::other("Context not injected into Runtime"))?
        .as_any()
        .downcast_ref::<Context>()
        .ok_or_else(|| Error::other("Runtime context is not opsense_core::Context"))
}

/// Trích observations từ một payload bất kỳ theo shape, KHÔNG gắn với một key
/// cứng nào. Các shape được hỗ trợ:
///
/// - array JSON top-level (array observation),
/// - object chứa `data` (envelope transit generic — [`signal::data_ready_with`] /
///   [`signal::processed_with`]),
/// - object chứa `observations` (tương thích producer ngoài / cấu trúc cũ),
/// - object dạng observation đơn (có `metric_id`).
///
/// Signal control (`tick`, `data_ready` không body, `processed` không body,
/// `backfill`) không thuộc shape nào nên trả về batch rỗng.
pub fn extract_observations(payload: &Value) -> Vec<Observation> {
    // 1. Top-level array observations.
    if let Some(arr) = payload.as_array() {
        return arr
            .iter()
            .filter_map(|v| serde_json::from_value::<Observation>(v.clone()).ok())
            .collect();
    }

    // 2. Observation đơn dạng object.
    if payload.get("metric_id").is_some()
        && let Ok(obs) = serde_json::from_value::<Observation>(payload.clone())
    {
        return vec![obs];
    }

    // 3. Envelope: `data` (transit generic) trước, `observations` (compat) sau.
    for key in ["data", "observations"] {
        if let Some(inner) = payload.get(key)
            && let obs = extract_observations(inner)
            && !obs.is_empty()
        {
            return obs;
        }
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::extract_observations;
    use serde_json::{Value, json};

    fn obs() -> Value {
        json!({
            "ts": 1700000001,
            "metric_id": "api_rps",
            "kind": "metric",
            "signal": "rate",
            "value": 42.0,
            "labels": {"dc": "hcm"}
        })
    }

    #[test]
    fn top_level_array() {
        let out = extract_observations(&json!([obs()]));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_id, "api_rps");
    }

    #[test]
    fn data_envelope() {
        let out = extract_observations(&json!({
            "event": "data_ready",
            "ts": 1700000001,
            "src": "fetch",
            "data": [obs()]
        }));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value, 42.0);
    }

    #[test]
    fn observations_key_compat() {
        let out = extract_observations(&json!({
            "event": "data_ready",
            "ts": 1700000001,
            "observations": [obs()]
        }));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn single_observation_object() {
        let out = extract_observations(&obs());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_id, "api_rps");
        assert_eq!(out[0].labels.get("dc").map(String::as_str), Some("hcm"));
    }

    #[test]
    fn control_signals_are_empty() {
        for payload in [
            json!({"event": "tick", "ts": 1}),
            json!({"event": "data_ready", "ts": 1}),
            json!({"event": "processed", "ts": 1}),
            json!({"event": "backfill", "from_ts": 1, "to_ts": 2}),
            json!("raw string"),
        ] {
            assert!(extract_observations(&payload).is_empty(), "payload: {payload}");
        }
    }
}
