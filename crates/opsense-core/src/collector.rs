//! Collector: fetches sources and reduces their observations.
//!
//! The collector owns the list of [`TelemetrySource`]s (built from
//! [`Config`](crate::config::Config)). A [`run`] loop (the "Clock") periodically
//! calls [`Collector::tick`], which fetches every source and returns the
//! reduced batch for pipeline nodes to store — the stores own all retention;
//! the collector keeps no copy of the data itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::config::Config;
use crate::source::{TelemetrySource, VectorSource};
use opsense_model::{LogLevel, MetricId, Observation, Signal, TelemetryKind};
use serde::Serialize;

/// Read-only view of a source's health, for the review/monitoring API.
#[derive(Debug, Clone, Serialize)]
pub struct SourceInfo {
    pub id: String,
    pub kind: String,
    pub last_error: Option<String>,
}

pub struct Collector {
    sources: Vec<Box<dyn TelemetrySource>>,
    metric_ids: Vec<MetricId>,
    last_errors: Mutex<HashMap<String, Option<String>>>,
}

impl Collector {
    /// Build sources from configuration: one [`VectorSource`] if configured.
    /// Pipeline-level fetching (templated HTTP nodes) does not go through the
    /// collector at all, so an empty `[sources]` is fine too.
    #[must_use]
    pub fn from_config(cfg: &Config) -> Self {
        let mut sources: Vec<Box<dyn TelemetrySource>> = Vec::new();
        let metric_ids: Vec<MetricId> = cfg.capacity.keys().cloned().collect();

        if let Some(v) = &cfg.sources.vector {
            sources.push(Box::new(VectorSource::new(
                "vector".into(),
                v.url.clone(),
                v.jq_filter.clone(),
                v.metrics.clone(),
            )));
        }

        let last_errors = Mutex::new(HashMap::new());
        Self {
            sources,
            metric_ids,
            last_errors,
        }
    }

    /// Direct constructor for callers that build sources themselves
    /// (e.g. tests, or sources not derived from config).
    #[must_use]
    pub fn new(sources: Vec<Box<dyn TelemetrySource>>) -> Self {
        Self {
            sources,
            metric_ids: Vec::new(),
            last_errors: Mutex::new(HashMap::new()),
        }
    }

    /// Fetch every source once and return the reduced batch (so pipeline nodes
    /// can store it).
    pub async fn collect(&self) -> Vec<Observation> {
        let mut out = Vec::new();
        for src in &self.sources {
            let id = src.id().to_string();
            match src.fetch().await {
                Ok(obs) => {
                    let reduced = self.reduce(obs);
                    self.last_errors.lock().unwrap().insert(id.clone(), None);
                    tracing::debug!("source {id} fetched {} observations", reduced.len());
                    out.extend(reduced);
                }
                Err(e) => {
                    tracing::warn!("source {id} fetch failed: {e}");
                    self.last_errors
                        .lock()
                        .unwrap()
                        .insert(id, Some(e.to_string()));
                }
            }
        }
        out
    }

    /// Fetch every source once (kept for the clock loop / on-demand reload).
    pub async fn tick(&self) {
        self.collect().await;
    }

    /// Reduce a raw batch to the numeric signals we persist.
    ///
    /// Metric observations pass through. Logs are aggregated per metric into
    /// `error_rate` (signal `Rate`) and `volume` (signal `Raw`). Traces are
    /// dropped (trace analysis is deferred) — they are never stored raw.
    fn reduce(&self, batch: Vec<Observation>) -> Vec<Observation> {
        let now = now_secs();
        let mut out: Vec<Observation> = Vec::new();
        let mut logs: HashMap<MetricId, (usize, usize)> = HashMap::new(); // (total, errors)
        let mut traces = 0usize;

        for o in batch {
            match o.kind {
                TelemetryKind::Metric => out.push(o),
                TelemetryKind::Log => {
                    let errors = usize::from(o.severity == Some(LogLevel::Error));
                    let entry = logs.entry(o.metric_id.clone()).or_insert((0, 0));
                    entry.0 += 1;
                    entry.1 += errors;
                }
                TelemetryKind::Trace => {
                    traces += 1;
                }
            }
        }

        for (metric_id, (total, errors)) in logs {
            let rate = if total > 0 {
                errors as f64 / total as f64
            } else {
                0.0
            };
            out.push(Observation::new(
                now,
                metric_id.clone(),
                TelemetryKind::Log,
                Signal::Rate,
                rate,
            ));
            out.push(Observation::new(
                now,
                metric_id,
                TelemetryKind::Log,
                Signal::Raw,
                total as f64,
            ));
        }

        if traces > 0 {
            tracing::debug!(
                count = traces,
                "dropping trace observations (trace analysis deferred)"
            );
        }
        out
    }

    #[must_use]
    pub fn metrics(&self) -> &[MetricId] {
        &self.metric_ids
    }

    #[must_use]
    pub fn sources_status(&self) -> Vec<SourceInfo> {
        let guard = self.last_errors.lock().unwrap();
        self.sources
            .iter()
            .map(|s| {
                let id = s.id().to_string();
                SourceInfo {
                    id,
                    kind: s.kind().to_string(),
                    last_error: guard.get(&s.id().to_string()).cloned().flatten(),
                }
            })
            .collect()
    }
}

fn now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Clock loop: ticks the collector on a fixed interval until the task ends.
pub async fn run(collector: Arc<Collector>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        collector.tick().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{SourceError, TelemetrySource};

    struct MockSource {
        id: String,
        obs: Vec<Observation>,
    }

    #[async_trait::async_trait]
    impl TelemetrySource for MockSource {
        fn id(&self) -> &str {
            &self.id
        }
        async fn fetch(&self) -> Result<Vec<Observation>, SourceError> {
            Ok(self.obs.clone())
        }
    }

    #[tokio::test]
    async fn collect_returns_reduced_observations() {
        let src = MockSource {
            id: "mock".into(),
            obs: vec![
                Observation::new(
                    1,
                    "cpu".into(),
                    TelemetryKind::Metric,
                    Signal::Utilization,
                    12.0,
                ),
                Observation::new(2, "svc".into(), TelemetryKind::Log, Signal::Raw, 1.0)
                    .with_severity(LogLevel::Error),
                Observation::new(3, "svc".into(), TelemetryKind::Log, Signal::Raw, 1.0),
            ],
        };
        let c = Collector::new(vec![Box::new(src)]);
        let out = c.collect().await;

        // Metrics pass through untouched.
        let cpu: Vec<_> = out.iter().filter(|o| o.metric_id == "cpu").collect();
        assert_eq!(cpu.len(), 1);
        assert_eq!(cpu[0].value, 12.0);

        // Logs are reduced to error_rate (Rate) + volume (Raw) per metric.
        let svc: Vec<_> = out.iter().filter(|o| o.metric_id == "svc").collect();
        assert_eq!(svc.len(), 2);
        let rate = svc.iter().find(|o| o.signal == Signal::Rate).unwrap();
        assert_eq!(rate.value, 0.5);

        let status = c.sources_status();
        assert_eq!(status.len(), 1);
        assert!(status[0].last_error.is_none());
    }
}
