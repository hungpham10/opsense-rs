//! Lightweight telemetry data model — the shared vocabulary every other
//! crate talks in. Deliberately dependency-free (just `serde`) so the
//! `core` ↔ `store` cycle can be broken: both depend on `model`, neither
//! depends on the other for the data types themselves.

use std::collections::HashMap;

use async_graphql::{Enum, SimpleObject};
use serde::{Deserialize, Serialize};

/// The three telemetry pillars Opsense ingests. `Trace` = the "telemetry"
/// pillar (spans); it is modeled now but ingestion is implemented later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
#[graphql(rename_items = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum TelemetryKind {
    Metric,
    Log,
    Trace,
}

/// What a numeric value represents. Lets the engine (later) interpret a series
/// without guessing from the metric name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
#[graphql(rename_items = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    Utilization,
    Saturation,
    Rate,
    Errors,
    Duration,
    Raw,
}

/// Severity for `Log` observations (used to derive error-rate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[graphql(rename_items = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// Stable identifier of a metric (e.g. `"cpu_usage"`).
pub type MetricId = String;

/// A single measured data point from any source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SimpleObject)]
#[graphql(name = "Observation")]
pub struct Observation {
    /// Unix timestamp in seconds.
    pub ts: i64,
    pub metric_id: MetricId,
    pub kind: TelemetryKind,
    pub signal: Signal,
    pub value: f64,

    #[serde(default)]
    pub labels: HashMap<String, String>,

    #[serde(default)]
    pub severity: Option<LogLevel>,
}

impl Observation {
    #[must_use]
    pub fn new(
        ts: i64,
        metric_id: MetricId,
        kind: TelemetryKind,
        signal: Signal,
        value: f64,
    ) -> Self {
        Self {
            ts,
            metric_id,
            kind,
            signal,
            value,
            labels: HashMap::new(),
            severity: None,
        }
    }

    #[must_use]
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_severity(mut self, severity: LogLevel) -> Self {
        self.severity = Some(severity);
        self
    }
}

/// A sequence of observations for one (metric, signal).
pub type TimeSeries = Vec<Observation>;

/// Alias kept for compatibility with the earlier plan naming.
pub type RawSample = Observation;
