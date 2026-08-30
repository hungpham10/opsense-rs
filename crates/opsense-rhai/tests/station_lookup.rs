//! Rhai scripts can read station history natively: `ts_query` / `ts_mean`
//! over the global registry let a transform compare its current batch with
//! stored history (the "aggregate the past, judge the present" story).

use opsense_core::registry;
use opsense_core::{Stage, Station};
use opsense_model::{Observation, Signal, TelemetryKind};
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
async fn scripts_query_station_history() {
    opsense_rhai::register();
    let id = format!("st-rhai-{}", std::process::id());
    let st = Arc::new(RwLock::new(Station::timeseries(1024)));
    st.write()
        .await
        .append(Stage::Processed, &[obs(100, 30.0), obs(200, 40.0)])
        .await;
    assert!(registry::register_station(&id, st).await);

    // The script receives today's sample (50.0) and evaluates it against
    // history pulled straight from the registry.
    let script = format!(
        r#"
        fn process(body) {{
            let base = ts_mean("{id}", "processed", "cpu", 0, 1000);
            let history = ts_query("{id}", "processed", "cpu", 0, 1000);
            [ #{{
                ts: body.ts,
                metric_id: "cpu_dev",
                kind: "metric",
                signal: "utilization",
                value: body.v - base,
                points: history.len(),
                first: history[0].value,
            }} ]
        }}
        "#
    );

    let runner = opsense_core::script::script_runner().expect("runner registered");
    let out = runner
        .run(&script, "", serde_json::json!({"ts": 300, "v": 55.0}))
        .await
        .expect("script must run");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["value"], serde_json::json!(20.0)); // 55 - mean(30,40)=35
    assert_eq!(out[0]["points"], serde_json::json!(2));
    assert_eq!(out[0]["first"], serde_json::json!(30.0));
}

#[tokio::test]
async fn unknown_station_and_empty_windows_return_unit() {
    opsense_rhai::register();
    let script = r#"
        fn process(body) {
            [#{ missing: ts_query("nope", "processed", "cpu", 0, 1) == (), empty: ts_mean("nope", "processed", "cpu", 0, 1) == (), echo: body.ts }]
        }
    "#;

    let runner = opsense_core::script::script_runner().expect("runner registered");
    let out = runner
        .run(script, "", serde_json::json!({"ts": 7}))
        .await
        .expect("script must run");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["missing"], serde_json::json!(true));
    assert_eq!(out[0]["empty"], serde_json::json!(true));
}
