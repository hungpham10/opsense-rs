//! Rhai bindings for the capacity-grid analysis (`opsense_mlib::grid`):
//! fit the uniform band grid minimising boundary crossings, then inspect
//! bands / occupancy. Includes a station-seeded end-to-end chain.

use opsense_rhai::{ScriptSource, call_process};

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
    let first = ranges[0].as_object().unwrap();
    assert!((first["low"].as_f64().unwrap() - 0.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn grid_fit_values_and_occupancy() {
    let input: Vec<serde_json::Value> = (0..100).map(|i| obs(i, (i % 10) as f64 * 10.0)).collect();

    let out = call_process(
        ScriptSource::Inline(
            r#"
            fn process(points) {
                // Build grid from values directly
                let vals = points.map(|p| p.value);
                let g = grid_fit_values(vals, 0.0, 100.0, 6);

                // Occupancy over 60s buckets
                let occ = grid_occupancy(g, points, 60);

                // Crossings on raw series
                let crosses = grid_crossings(g, points.map(|p| p.value));

                [#{ cells: num_cells(g), occ_len: occ.len(), crosses: crosses }];
            }
            "#
            .into(),
        ),
        serde_json::Value::Array(input),
    )
    .await
    .expect("script must run");

    let m = out[0].as_object().unwrap();
    let cells = m["cells"].as_i64().unwrap();
    assert!((2..=64).contains(&cells));
    assert!(m["occ_len"].as_i64().unwrap() > 0);
    assert!(m["crosses"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn station_points_fit_capacity_grid_end_to_end() {
    // End-to-end using call_process: build synthetic points, fit grid, inspect.
    // This replaces the old test that used removed registry APIs.
    let now = 1_700_000_000i64;
    let input: Vec<serde_json::Value> = (0..500)
        .map(|i| {
            let ts = now + i * 60;
            let val = 30.0 + (i as f64 % 50.0); // oscillate 30..80
            obs(ts, val)
        })
        .collect();

    let out = call_process(
        ScriptSource::Inline(
            r#"
            fn process(points) {
                let g = grid_fit(points, 0.0, 100.0, 10);
                [#{ 
                    cells: num_cells(g),
                    step: grid_step(g),
                    cross: grid_crossings(g, points.map(|p| p.value)),
                }];
            }
            "#
            .into(),
        ),
        serde_json::Value::Array(input),
    )
    .await
    .expect("script must run");

    let m = out[0].as_object().unwrap();
    let cells = m["cells"].as_i64().unwrap();
    assert!((2..=1024).contains(&cells), "grid cells out of range: {cells}");
    assert!(m["step"].as_f64().unwrap() > 0.0);
    assert!(m["cross"].as_i64().unwrap() > 0);
}