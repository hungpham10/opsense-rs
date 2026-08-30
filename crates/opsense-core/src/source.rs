//! Telemetry sources: the data-collection adapters.
//!
//! A [`TelemetrySource`] yields [`Observation`]s of any [`TelemetryKind`].
//! [`VectorSource`] is the generic HTTP adapter (payload filtered through the
//! user's `opsense_libs::jq::JsonQuery`, then each JSON object mapped to an
//! [`Observation`] via [`observation_from_value`]). Pipeline-level fetching
//! (arbitrary APIs, templated requests) lives in `HttpSource`
//! (`opsense-components`), which reuses [`observation_from_value`] too.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use opsense_model::{LogLevel, Observation, Signal, TelemetryKind};

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("http error from source '{id}': {err}")]
    Http {
        id: String,
        #[source]
        err: reqwest::Error,
    },

    #[error("json error from source '{id}': {err}")]
    Json {
        id: String,
        #[source]
        err: serde_json::Error,
    },

    #[error("jq error in source '{id}': {msg}")]
    Jq { id: String, msg: String },

    #[error("source '{id}' returned no usable payload")]
    Empty { id: String },

    #[error("invalid observation shape in source '{id}': {msg}")]
    Shape { id: String, msg: String },
}

/// A pluggable telemetry adapter.
#[async_trait]
pub trait TelemetrySource: Send + Sync {
    /// Stable identifier of this source (e.g. the config key).
    fn id(&self) -> &str;
    /// Short adapter type shown by `/sources` (e.g. `"vector"`).
    fn kind(&self) -> &str {
        "generic"
    }
    /// Fetch the current batch of observations.
    async fn fetch(&self) -> Result<Vec<Observation>, SourceError>;
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Generic HTTP source. The response body is either run through a `jq_filter`
/// (via `opsense_libs::jq::JsonQuery`) or taken as-is, then each JSON object is
/// mapped to an [`Observation`]. Works for metrics, logs and traces alike —
/// the emitted JSON decides `kind`/`signal`/`metric_id`.
pub struct VectorSource {
    id: String,
    url: String,
    jq_filter: Option<String>,
    metrics: Option<Vec<String>>,
    client: reqwest::Client,
}

impl VectorSource {
    #[must_use]
    pub fn new(
        id: String,
        url: String,
        jq_filter: Option<String>,
        metrics: Option<Vec<String>>,
    ) -> Self {
        Self {
            id,
            url,
            jq_filter,
            metrics,
            client: reqwest::Client::new(),
        }
    }

    /// Pure parse of a payload (testable without network).
    pub fn parse_payload(&self, body: &str) -> Result<Vec<Observation>, SourceError> {
        let payload: serde_json::Value =
            serde_json::from_str(body).map_err(|err| SourceError::Json {
                id: self.id.clone(),
                err,
            })?;

        let items: Vec<serde_json::Value> = if let Some(filter) = &self.jq_filter {
            let query =
                opsense_libs::jq::JsonQuery::parse(filter).map_err(|err| SourceError::Jq {
                    id: self.id.clone(),
                    msg: err.to_string(),
                })?;
            query.execute(&payload)
        } else {
            match payload {
                serde_json::Value::Array(arr) => arr,
                other => vec![other],
            }
        };

        let allow = self.metrics.as_ref().map(|m| m.to_vec());
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let obs = observation_from_value(&self.id, item)?;
            if let Some(allow) = &allow {
                if !allow.contains(&obs.metric_id) {
                    continue;
                }
            }
            out.push(obs);
        }
        if out.is_empty() {
            return Err(SourceError::Empty {
                id: self.id.clone(),
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl TelemetrySource for VectorSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        "vector"
    }

    async fn fetch(&self) -> Result<Vec<Observation>, SourceError> {
        let body = self
            .client
            .get(&self.url)
            .send()
            .await
            .map_err(|err| SourceError::Http {
                id: self.id.clone(),
                err,
            })?
            .text()
            .await
            .map_err(|err| SourceError::Http {
                id: self.id.clone(),
                err,
            })?;
        self.parse_payload(&body)
    }
}

/// Parse an HTTP response body holding observation-shaped JSON (an array of
/// objects, or one object) via [`observation_from_value`]. An empty array is a
/// valid empty batch for windowed pipeline nodes (unlike [`VectorSource`],
/// which treats empty as a source error).
pub fn observations_from_body(
    source_id: &str,
    body: &str,
) -> Result<Vec<Observation>, SourceError> {
    let payload: serde_json::Value =
        serde_json::from_str(body).map_err(|err| SourceError::Json {
            id: source_id.to_string(),
            err,
        })?;
    match payload {
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| observation_from_value(source_id, item))
            .collect(),
        other => Ok(vec![observation_from_value(source_id, other)?]),
    }
}

/// Map one JSON object to an [`Observation`] (`ts|timestamp`, `metric_id`,
/// `kind`, `signal`, `value`, optional `labels`/`severity`). Missing fields
/// fall back: `ts` → now, `metric_id` → `default_metric`. Shared by
/// [`VectorSource`] and the pipeline's HTTP fetch node.
pub fn observation_from_value(
    default_metric: &str,
    v: serde_json::Value,
) -> Result<Observation, SourceError> {
    let obj = v.as_object().ok_or_else(|| SourceError::Shape {
        id: default_metric.to_string(),
        msg: "expected a JSON object".into(),
    })?;

    let ts = obj
        .get("ts")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| obj.get("timestamp").and_then(serde_json::Value::as_i64))
        .unwrap_or_else(now_secs);

    let metric_id = obj
        .get("metric_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(default_metric)
        .to_string();

    let kind = match obj.get("kind").and_then(serde_json::Value::as_str) {
        Some("log") => TelemetryKind::Log,
        Some("trace") => TelemetryKind::Trace,
        _ => TelemetryKind::Metric,
    };
    let signal = match obj.get("signal").and_then(serde_json::Value::as_str) {
        Some("utilization") => Signal::Utilization,
        Some("saturation") => Signal::Saturation,
        Some("rate") => Signal::Rate,
        Some("errors") => Signal::Errors,
        Some("duration") => Signal::Duration,
        _ => Signal::Raw,
    };
    let value = obj
        .get("value")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| SourceError::Shape {
            id: metric_id.clone(),
            msg: "missing numeric 'value'".into(),
        })?;

    let mut obs = Observation::new(ts, metric_id, kind, signal, value);

    if let Some(labels) = obj.get("labels").and_then(serde_json::Value::as_object) {
        for (k, val) in labels {
            if let Some(s) = val.as_str() {
                obs.labels.insert(k.clone(), s.to_string());
            }
        }
    }
    if let Some(sev) = obj.get("severity").and_then(serde_json::Value::as_str) {
        obs.severity = Some(match sev.to_ascii_lowercase().as_str() {
            "error" => LogLevel::Error,
            "warn" => LogLevel::Warn,
            "info" => LogLevel::Info,
            _ => LogLevel::Debug,
        });
    }
    Ok(obs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_payload_metric_raw() {
        let src = VectorSource::new("vec".into(), "http://x".into(), None, None);
        let body = r#"[{"ts": 100, "metric_id": "cpu_usage", "value": 12.5}]"#;
        let obs = src.parse_payload(body).unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].metric_id, "cpu_usage");
        assert_eq!(obs[0].kind, TelemetryKind::Metric);
        assert_eq!(obs[0].signal, Signal::Raw);
        assert_eq!(obs[0].value, 12.5);
    }

    #[test]
    fn vector_payload_with_jq_and_log() {
        // The jq engine supports iter/select over already-shaped objects, so the
        // upstream is expected to emit observation-shaped JSON. `.[]` iterates the
        // root array; each object maps directly to an Observation.
        let src = VectorSource::new("vec".into(), "http://x".into(), Some(".[]".into()), None);
        let body = r#"[
            {"ts": 1, "metric_id": "app_log", "kind": "log", "signal": "errors", "value": 1.0, "severity": "error"},
            {"ts": 2, "metric_id": "app_log", "kind": "log", "signal": "errors", "value": 0.0, "severity": "info"}
        ]"#;
        let obs = src.parse_payload(body).unwrap();
        assert_eq!(obs.len(), 2);
        assert_eq!(obs[0].kind, TelemetryKind::Log);
        assert_eq!(obs[0].signal, Signal::Errors);
        assert_eq!(obs[0].severity, Some(LogLevel::Error));
        assert_eq!(obs[1].severity, Some(LogLevel::Info));
    }

    #[test]
    fn vector_payload_respects_metrics_allowlist() {
        let src = VectorSource::new(
            "vec".into(),
            "http://x".into(),
            None,
            Some(vec!["keep".into()]),
        );
        let body =
            r#"[{"ts":1,"metric_id":"keep","value":1.0},{"ts":2,"metric_id":"drop","value":2.0}]"#;
        let obs = src.parse_payload(body).unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].metric_id, "keep");
    }
}
