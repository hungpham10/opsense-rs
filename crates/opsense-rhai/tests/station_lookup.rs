//! Tests for the new station read API via Context (replaces old registry-based
//! `ts_query`/`ts_mean` bindings which were removed).

use opsense_core::{Context, Observation, Station, TimeseriesStation};
use opsense_model::events::{Signal, TelemetryKind};
use opsense_model::secret::Secret;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

fn obs(ts: i64, value: f64) -> Observation {
    Observation::new(
        ts,
        "cpu".into(),
        TelemetryKind::Metric,
        Signal::Utilization,
        value,
    )
}

#[tokio::test]
async fn context_station_registry_and_query() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    // Create and register a TimeseriesStation
    let station = TimeseriesStation::from_storage("test-station", ctx.storage())
        .await
        .unwrap();
    let station_arc = Arc::new(RwLock::new(station));
    ctx.registry("test-station", Station::Timeseries(station_arc.clone()))
        .await
        .unwrap();

    // Write some data
    let batch = vec![obs(100, 30.0), obs(200, 40.0), obs(300, 50.0)];
    station_arc.write().await.update_range(&batch, 100, 300, 300);

    // Query via Context
    let retrieved = ctx.station::<Arc<RwLock<TimeseriesStation>>>("test-station").await.unwrap();
    // Query within the written data range (100-300)
    let data = retrieved.write().await.query_range(100, 300).await.unwrap();

    assert_eq!(data.len(), 3);
    assert_eq!(data[0].value, 30.0);
    assert_eq!(data[1].value, 40.0);
    assert_eq!(data[2].value, 50.0);
}

#[tokio::test]
async fn context_station_missing_returns_error() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    // Query non-existent station
    let result = ctx.station::<Arc<RwLock<TimeseriesStation>>>("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn context_attributes_roundtrip() {
    let mut attrs = HashMap::new();
    attrs.insert("foo".into(), "bar".into());

    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let mut cfg = cfg;
    cfg.attributes = attrs.clone();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let retrieved = ctx.get_attributes().await;
    assert_eq!(retrieved.get("foo").unwrap(), "bar");
}

#[tokio::test]
async fn context_capacity_lookup() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let mut cfg = cfg;
    cfg.capacity.insert("disk_usage".into(), 100.0);
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    assert_eq!(ctx.capacity("disk_usage"), Some(100.0));
    assert_eq!(ctx.capacity("unknown"), None);
}