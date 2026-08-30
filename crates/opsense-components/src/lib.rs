//! Opsense components: the vector dataflow [`Component`]s that drive collection.
//!
//! This crate holds every Opsense-specific component registered into the
//! `opsense_libs::vector` [`Runtime`] (the executor). It is split from
//! `opsense-core` so the growing pile of components cannot bloat the pure
//! domain crate. The default hourly dataflow is:
//!
//! ```text
//! Clock (Source, tick+ts) → Ingest (Transform) → Processor (Transform)
//! ```
//!
//! (The durable `Persist` sink was removed — each node now owns its own
//! station and there is no shared persistence tier.)
//!
//! Nodes talk via the [`signal`] schema (`tick` / `data_ready` / `processed`,
//! each carrying a timestamp watermark), so any Source — Clock today, a
//! Kafka/Redis/RabbitMQ consumer tomorrow — can drive the same chain. Each
//! node advances its own cursor in [`Watermarks`] and the next node reads
//! exactly the delta. The runtime refuses a standalone node (every node must
//! be linked into the graph), so the chain is wired end-to-end.

mod clock;
mod collector;
mod http;
mod ingest;
mod processor;

pub mod catalog;
pub mod signal;
pub mod station;

use std::sync::Arc;
use std::time::Duration;

use opsense_core::config::Config;

pub use clock::ClockSource;
pub use collector::CollectorSink;
pub use opsense_core::{Context, OpsenseContext, Stations};
/// Alias for `OpsenseContext::new_stations()` — creates an empty in-memory
/// station registry.
pub fn new_station_registry() -> Stations {
    OpsenseContext::new_stations()
}
pub use http::{FieldSpec, HttpSource};
pub use ingest::IngestSource;
pub use processor::ProcessorTransform;
pub use station::{
    own_station, CategoryStationTransform, PatternStationTransform, StationKind,
    TimeseriesStationSink, TimeseriesStationTransform,
};

/// Re-export of the `vector` runtime under `crate::vector::runtime`.
///
/// `opsense-macros`' `#[source]`/`#[sink]`/`#[transform]`/`#[input]`/`#[output]`
/// attributes expand to code that refers to `crate::vector::runtime::*`; this
/// mirror lets those macros be used from this crate exactly as they are from
/// `opsense-libs`.
pub mod vector {
    pub use opsense_libs::vector::runtime;
}

/// The default graph: `clock -> ingest -> processor`, ticking every
/// `engine.poll_interval_seconds` (1h in production configs).
#[must_use]
pub fn default_pipeline(cfg: &Config) -> Vec<Arc<dyn vector::runtime::Component>> {
    let interval = Duration::from_secs(cfg.engine.poll_interval_seconds.max(1));
    let mut collector = CollectorSink::new();
    collector.inputs = vec!["processor".to_string()];
    vec![
        Arc::new(ClockSource::new(interval)),
        Arc::new(IngestSource::new()),
        Arc::new(ProcessorTransform::new()),
        Arc::new(collector),
    ]
}

/// The component list for a runtime session: the explicit `[pipeline]`
/// component tables when configured, otherwise [`default_pipeline`].
///
/// Shared by `opsense serve` and MCP `opsense_init` so both apply the same
/// override rules and report typetag deserialization errors identically.
pub fn pipeline_from_config(
    cfg: &Config,
) -> Result<Vec<Arc<dyn vector::runtime::Component>>, String> {
    match &cfg.pipeline {
        Some(p) if !p.components.is_empty() => p.components
            .iter()
            .map(|value| {
                serde_json::from_value::<Box<dyn vector::runtime::Component>>(value.clone())
                    .map(Arc::from)
                    .map_err(|e| format!("component `{value}`: {e}"))
            })
            .collect(),
        _ => Ok(default_pipeline(cfg)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use opsense_core::collector::Collector;
    use opsense_core::registry;
    use opsense_core::source::{SourceError, TelemetrySource};
    use opsense_core::Cursor;
    use opsense_core::Stage;
    use opsense_core::Watermarks;
    use opsense_model::{Observation, Signal, TelemetryKind};
    use vector::runtime::{Event, Runtime};

    struct MockSource {
        id: String,
        obs: Vec<Observation>,
    }

    fn dummy_config() -> Config {
        Config {
            engine: Default::default(),
            capacity: Default::default(),
            sources: Default::default(),
            attributes: Default::default(),
            storage: Default::default(),
            session: Default::default(),
            repl: Default::default(),
            pipeline: None,
        }
    }

    #[async_trait]
    impl TelemetrySource for MockSource {
        fn id(&self) -> &str {
            &self.id
        }
        async fn fetch(&self) -> Result<Vec<Observation>, SourceError> {
            Ok(self.obs.clone())
        }
    }

    #[tokio::test]
    async fn hourly_pipeline_runs_end_to_end() {
        let src = MockSource {
            id: "mock".into(),
            obs: vec![Observation::new(
                1_000,
                "cpu".into(),
                TelemetryKind::Metric,
                Signal::Utilization,
                12.0,
            )],
        };
        let collector = Arc::new(Collector::new(vec![Box::new(src)]));
        let ctx = Arc::new(OpsenseContext::new(
            collector,
            Watermarks::new(),
            Arc::new(BTreeMap::new()),
            OpsenseContext::new_stations(),
        ));

        let mut runtime = Runtime::new();
        let watermarks = ctx.watermarks().clone();
        runtime.set_context(ctx);
        let cfg = Config {
            engine: opsense_core::config::EngineConfig {
                poll_interval_seconds: 1, // fast for the test
                ..Default::default()
            },
            ..dummy_config()
        };
        runtime
            .reload(default_pipeline(&cfg))
            .expect("runtime must accept the clock -> ingest -> processor graph");

        let _handle = runtime
            .start(|_event: Event| async {})
            .expect("runtime start");

        tokio::time::sleep(Duration::from_millis(300)).await;

        runtime.stop().expect("runtime stop");
        runtime.wait_for_shutdown().await.expect("runtime shutdown");

        // Every stage saw the mock observation and cursors advanced.
        // Model mới: dữ liệu sống trong TRẠM RIÊNG của từng node.
        let ingest_st = registry::station("ingest")
            .await
            .expect("ingest_source must own a station");
        let proc_st = registry::station("processor")
            .await
            .expect("processor must own a station");
        let raw = ingest_st
            .read()
            .await
            .query(Stage::Raw, "cpu", 0, i64::MAX)
            .await;
        let processed = proc_st
            .read()
            .await
            .query(Stage::Processed, "cpu", 0, i64::MAX)
            .await;
        assert!(!raw.is_empty(), "ingest station must have the batch");
        assert!(!processed.is_empty(), "processor must have run");
        assert!(watermarks.get(Cursor::IngestDone) > 0);
    }
}
