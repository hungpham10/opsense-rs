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

pub fn extract_observations(payload: &Value) -> Vec<Observation> {
    let Some(arr) = payload.get("observations").and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|v| serde_json::from_value::<Observation>(v.clone()).ok())
        .collect()
}
