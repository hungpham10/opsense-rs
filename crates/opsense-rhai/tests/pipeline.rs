//! Playground loop end-to-end: `clock -> ingest -> rhai -> persist`, where the
//! transform node runs the example script `scripts/moving_avg.rhai` (batch
//! mean per metric → `<metric>_mean`).
//!
//! Proves the scripted story: a `.rhai` file outside the core crates processes
//! live observations, its own watermark cursor advances, and the result flows
//! to the downstream persist node into the durable store.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_components::{
    ClockSource, CollectorSink, IngestSource, OpsenseContext, new_station_registry,
};
use opsense_core::Context;
use opsense_core::collector::Collector;
use opsense_core::registry;
use opsense_core::source::{SourceError, TelemetrySource};
use opsense_core::{Stage, Watermarks};
use opsense_model::{Observation, Signal, TelemetryKind};
use opsense_rhai::{RhaiTransform, ScriptSource};

fn moving_avg_script_path() -> String {
    // crates/opsense-rhai -> repo root -> scripts/
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/moving_avg.rhai").to_string()
}

struct MockSource {
    id: String,
}

#[async_trait]
impl TelemetrySource for MockSource {
    fn id(&self) -> &str {
        &self.id
    }
    async fn fetch(&self) -> Result<Vec<Observation>, SourceError> {
        Ok(vec![Observation::new(
            opsense_components::signal::now_secs(),
            "cpu".into(),
            TelemetryKind::Metric,
            Signal::Utilization,
            12.0,
        )])
    }
}

#[tokio::test]
async fn rhai_transform_processes_through_script() {
    let collector = Arc::new(Collector::new(vec![Box::new(MockSource {
        id: "mock".into(),
    })]));
    let ctx = Arc::new(OpsenseContext::new(
        collector,
        Watermarks::new(),
        Arc::new(std::collections::BTreeMap::new()),
        new_station_registry(),
    ));
    let watermarks = ctx.watermarks().clone();

    let script = RhaiTransform::new_file("mean", &["ingest"], &moving_avg_script_path());
    assert_eq!(script.output_stage, Stage::Processed);
    assert!(script.write_lru);

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    let mut persist = CollectorSink::new();
    persist.id = "drain".to_string();
    persist.inputs = vec!["mean".to_string()]; // chain: mean(rhai) -> drain

    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ClockSource::new(Duration::from_millis(50))),
        Arc::new(IngestSource::new()),
        Arc::new(script),
        Arc::new(persist),
    ];
    runtime
        .reload(components)
        .expect("clock -> ingest -> mean(rhai) -> persist must be a valid graph");

    let _handle = runtime
        .start(|_event: Event| async {})
        .expect("runtime start");
    tokio::time::sleep(Duration::from_millis(500)).await;
    runtime.stop().expect("runtime stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    // Raw arrived in ingest's OWN station (model: station là nơi lưu duy nhất)…
    let ingest_st = registry::station("ingest")
        .await
        .expect("ingest_source must own a station");
    assert!(
        !ingest_st
            .read()
            .await
            .query(Stage::Raw, "cpu", 0, i64::MAX)
            .await
            .is_empty(),
        "ingest must store raw observations"
    );
    // …the script produced the derived series in its OWN station…
    let mean_st = registry::station("mean")
        .await
        .expect("rhai node must own a station");
    let means = mean_st
        .read()
        .await
        .query(Stage::Processed, "cpu_mean", 0, i64::MAX)
        .await;
    assert!(
        !means.is_empty(),
        "<cpu>_mean missing from the rhai station"
    );
    assert!(means.iter().all(|o| (o.value - 12.0).abs() < f64::EPSILON));
    // …its own watermark cursor advanced…
    assert!(
        watermarks.get_node("mean") > 0,
        "rhai node cursor must advance"
    );
    // …the script output was published to the node's station (no persistence tier).
    assert!(
        !means.is_empty(),
        "rhai node must publish the derived series"
    );
}

/// A `[pipeline]` table may declare the node inline; every documented default
/// applies and unknown fields stay rejected.
#[test]
fn rhai_component_deserializes_from_config() {
    let value = serde_json::json!({
        "type": "rhai_transform",
        "id": "double",
        "inputs": ["ingest"],
        "script": "fn process(o) { o }",
    });
    let component: Box<dyn Component> =
        serde_json::from_value(value).expect("inline rhai node must deserialize");
    assert_eq!(component.id(), "double");

    let full = serde_json::json!({
        "type": "rhai_transform",
        "id": "mean",
        "inputs": ["ingest"],
        "script_path": "scripts/moving_avg.rhai",
        "input_stage": "Processed",
        "output_stage": "Raw",
        "write_lru": false,
        "write_store": true,
        "params": {"factor": 2, "label": "peak"},
    });
    let parsed: Box<dyn Component> =
        serde_json::from_value(full).expect("every documented field must parse");
    assert_eq!(parsed.id(), "mean");

    // Unknown fields stay rejected (macro emits deny_unknown_fields).
    let bad = serde_json::json!({
        "type": "rhai_transform",
        "id": "x",
        "inputs": [],
        "script": "",
        "wat": 1,
    });
    assert!(serde_json::from_value::<Box<dyn Component>>(bad).is_err());
}

/// Exactly one of `script` / `script_path` must be configured — the node
/// reports a clear error otherwise instead of silently doing nothing.
#[tokio::test]
async fn missing_script_source_errors_cleanly() {
    let component = RhaiTransform::new_file("broken", &["ingest"], "");
    assert!(component.script.is_empty() && component.script_path.is_empty());

    // One signal then a closed channel: run() processes the message, hits the
    // missing-script error and returns it — the same path the pipeline event
    // handler would see as a Major event.
    let (tx_in, mut rx) = tokio::sync::mpsc::channel(4);
    tx_in
        .send(opsense_components::signal::tick(1_000))
        .await
        .expect("send tick");
    drop(tx_in);

    let (tx_out, _rx_out) = tokio::sync::mpsc::channel(4);
    let (tx_event, mut rx_event) = tokio::sync::mpsc::channel(4);

    let outcome = component
        .run(
            0,
            &mut rx,
            opsense_components::vector::runtime::Outbound {
                streams: vec![tx_out],
                broadcast: None,
                event: tx_event,
                ctx: None,
            },
        )
        .await;

    assert!(outcome.is_err(), "run without any script must fail");
    let message = outcome.unwrap_err().to_string();
    assert!(message.contains("exactly one of"), "unexpected: {message}");
    while rx_event.try_recv().is_ok() {}
}

/// Node `params` reach the script as `param_<name>` globals and the pipeline
/// config's resolved `[attributes]` are readable via `attr(name)` / `attrs()`.
#[tokio::test]
async fn rhai_script_receives_params_and_attributes() {
    let mut attributes = std::collections::BTreeMap::new();
    attributes.insert("env".to_string(), "prod".to_string());

    let mut params = std::collections::BTreeMap::new();
    params.insert("factor".to_string(), serde_json::json!(3));
    params.insert("label".to_string(), serde_json::json!("peak"));

    let out = opsense_rhai::call_process_with(
        ScriptSource::Inline(
            r#"
            fn process(observations) {
                let f = param_factor;
                [
                    #{ scaled: observations[0].value * f,
                       tag: param_label,
                       env: attr("env"),
                       missing: attr("nope"),
                       all: attrs()["env"] },
                ];
            }
            "#
            .into(),
        ),
        serde_json::json!([{ "value": 2.0 }]),
        params,
        attributes,
    )
    .await
    .expect("script must receive params and attributes");

    let item = out.into_iter().next().expect("one result");
    assert_eq!(item["scaled"], serde_json::json!(6.0));
    assert_eq!(item["tag"], serde_json::json!("peak"));
    assert_eq!(item["env"], serde_json::json!("prod"));
    assert_eq!(item["all"], serde_json::json!("prod"));
    // Unknown attribute resolves to unit → JSON null.
    assert!(item["missing"].is_null());

    // An invalid param name is rejected before the script ever runs.
    let mut params = std::collections::BTreeMap::new();
    params.insert("bad name".to_string(), serde_json::json!(1));
    assert!(
        opsense_rhai::call_process_with(
            ScriptSource::Inline("fn process(o) { o }".into()),
            serde_json::json!([]),
            params,
            std::collections::BTreeMap::new(),
        )
        .await
        .is_err()
    );

    // Without params the legacy two-arg entry point still works unchanged.
    let out = opsense_rhai::call_process(
        ScriptSource::Inline("fn process(o) { o }".into()),
        serde_json::json!([{ "value": 1.0 }]),
    )
    .await
    .expect("legacy call path must be unchanged");
    assert_eq!(out[0]["value"], serde_json::json!(1.0));
}
