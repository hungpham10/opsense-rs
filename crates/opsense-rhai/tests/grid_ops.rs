//! Rhai bindings for the capacity-grid analysis (`opsense_libs::grid`):
//! fit the uniform band grid minimising boundary crossings, then inspect
//! bands / occupancy. Includes a station-seeded end-to-end chain.

use opsense_rhai::{call_process, ScriptSource};

fn obs(ts: i64, val: f64) -> serde_json::Value {
    serde_json::json!({
        "ts": ts,
        "metric_id": "disk_usage",
        "kind": "metric",
        "signal": "utilization",
        "value": val,
    })
}

#[tokio::test]
async fn grid_fit_and_inspect_from_values() {
    // Ramp 0..100 over 200 points: crossings grow steadily with refinement.
    let input: Vec<serde_json::Value> = (0..200).map(|i| obs(i, i as f64 / 2.0)).collect();

    let out = call_process(
        ScriptSource::Inline(
            r#"
            fn process(points) {
                let g = grid_fit(points, 0.0, 100.0, 8);
                [#{ 
                    cells: num_cells(g),
                    lines: num_lines(g),
                    step: grid_step(g),
                    cell_of_55: grid_cell(g, 55.0),
                    ranges: grid_ranges(g),
                }];
            }
            "#
            .into(),
        ),
        serde_json::Value::Array(input),
    )
    .await
    .expect("script must run");

    let m = out[0].as_object().expect("map result");
    let cells = m["cells"].as_i64().unwrap();
    assert_eq!(m["lines"].as_i64().unwrap(), cells + 1);
    assert!((2..=256).contains(&cells), "sieve stopped early: {cells}");
    assert!(m["step"].as_f64().unwrap() > 0.0);
    assert!(m["cell_of_55"].as_i64().unwrap() >= 1);

    let ranges = m["ranges"].as_array().unwrap();
    assert_eq!(ranges.len() as i64, cells);
    assert_eq!(ranges[0]["low"].as_f64().unwrap(), 0.0);
}

#[tokio::test]
async fn occupancy_histogram_shape() {
    let out = call_process(
        ScriptSource::Inline(
            r#"
            fn process(points) {
                let g = grid_fit_values([5.0, 15.0, 95.0], 0.0, 100.0, 4);
                [[#{ occ: grid_occupancy(g, points, 60), crossings: grid_crossings(g, points) }]];
            }
            "#
            .into(),
        ),
        serde_json::Value::Array(vec![obs(0, 5.0), obs(30, 5.0), obs(150, 95.0)]),
    )
    .await
    .expect("script must run");

    let occ = out[0][0]["occ"].as_array().unwrap();
    // Buckets theo first/last ts của input: 0..=150 → 3 bucket × 60s.
    assert_eq!(occ.len(), 3);
    let total: u64 = occ
        .iter()
        .flat_map(|bucket| bucket.as_array().unwrap())
        .filter_map(|v| v.as_u64())
        .sum();
    assert_eq!(total, 3, "không mất điểm qua histogram");
    // Hai điểm đầu cùng cell → có ít nhất một ô đếm ≥2.
    let max_in_bucket0 = occ[0]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_u64())
        .max()
        .unwrap();
    assert!(max_in_bucket0 >= 2);
    assert!(out[0][0]["crossings"].as_i64().unwrap() >= 1);
}

#[tokio::test]
async fn station_points_fit_capacity_grid_end_to_end() {
    use opsense_core::registry;
    use opsense_core::{Stage, Station};
    use opsense_model::{Observation, Signal, TelemetryKind};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    opsense_rhai::register();
    let id = format!("tsdb-grid-{}", std::process::id());
    // Usage dao động quanh hai vùng 20% và 70% của capacity 500GB.
    let seeds: Vec<Observation> = (0..40)
        .map(|i| {
            let value = if i % 10 < 5 { 100.0 } else { 350.0 };
            Observation::new(
                i * 60,
                "disk_usage".into(),
                TelemetryKind::Metric,
                Signal::Utilization,
                value,
            )
        })
        .collect();
    let st = Arc::new(RwLock::new(Station::timeseries(1024)));
    st.write().await.append(Stage::Processed, &seeds).await;
    assert!(registry::register_station(&id, st).await);

    let script = format!(
        r#"
        fn process(body) {{
            let pts = ts_query("{id}", "processed", "disk_usage", 0, 100000);
            let g = grid_fit(pts, 0.0, body.capacity, 10);
            [#{{ bands: num_cells(g), step: grid_step(g), ranges: grid_ranges(g).len() }}];
        }}
        "#
    );
    let runner = opsense_core::script::script_runner().expect("runner registered");
    let out = runner
        .run(&script, "", serde_json::json!({ "capacity": 500.0 }))
        .await
        .expect("script must run");

    let bands = out[0]["bands"].as_i64().unwrap();
    assert!((2..=1024).contains(&bands), "bands = {bands}");
    assert_eq!(out[0]["ranges"].as_i64().unwrap(), bands);
    let step = out[0]["step"].as_f64().unwrap();
    assert!(step > 0.0 && step <= 500.0);
}
