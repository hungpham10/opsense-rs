//! Integration tests for the time-series operator library registered on the
//! Rhai engine (`ts_rate`, `ts_moving_avg`, `ts_resample`, `ts_quantile`,
//! `ts_p95`, `ts_p99`, `ts_delta`, `ts_pct_change`).

use opsense_rhai::{ScriptSource, call_process};

fn obs(ts: i64, val: f64) -> serde_json::Value {
    serde_json::json!({
        "ts": ts,
        "metric_id": "cpu",
        "kind": "metric",
        "signal": "utilization",
        "value": val,
    })
}

async fn run(script: &str, input: Vec<serde_json::Value>) -> serde_json::Value {
    let out = call_process(
        ScriptSource::Inline(script.to_string()),
        serde_json::Value::Array(input),
    )
    .await
    .expect("call_process must succeed");
    out.into_iter().next().expect("process returns one element")
}

#[tokio::test]
async fn ts_ops_compute_expected_values() {
    // Value rises by 1 every 60s: 10 points at ts 0,60,...,540.
    let input: Vec<serde_json::Value> = (0..10).map(|i| obs(i * 60, i as f64)).collect();

    let out = run(
        r#"
        fn process(observations) {
            let rate = ts_rate(observations);
            let ma = ts_moving_avg(observations, 120);    // 3 points per window
            let rs = ts_resample(observations, 120, "avg"); // 5 two-point buckets
            let q  = ts_quantile(observations, 0.5);      // median of 0..9
            let p95 = ts_p95(observations);
            let p99 = ts_p99(observations);
            let d  = ts_delta(observations);
            let pct = ts_pct_change(observations);
            return [#{
                rate: rate, ma_len: ma.len(), rs_len: rs.len(),
                q: q, p95: p95, p99: p99, d_len: d.len(), pct_len: pct.len()
            }];
        }
        "#,
        input,
    )
    .await;

    let m = out.as_object().unwrap();
    // rate = (9 - 0) / 540 = 1/60 ≈ 0.01667
    assert!((m["rate"].as_f64().unwrap() - 1.0 / 60.0).abs() < 1e-4);
    // ma with 120s window (3 points) over 10 points → 10 outputs
    assert_eq!(m["ma_len"].as_i64().unwrap(), 10);
    // resample 120s buckets over 540s range → 5 buckets, each 2 points avg
    assert_eq!(m["rs_len"].as_i64().unwrap(), 5);
    // median of 0..9 = 4.5
    assert!((m["q"].as_f64().unwrap() - 4.5).abs() < 1e-6);
    // p95 of 0..9 = 8.55 (index 8.55 → 8)
    assert!((m["p95"].as_f64().unwrap() - 8.0).abs() < 1e-6);
    // p99 of 0..9 ≈ 8.91 → 8
    assert!((m["p99"].as_f64().unwrap() - 8.0).abs() < 1e-6);
    // delta: 9 changes (10 points → 9 deltas)
    assert_eq!(m["d_len"].as_i64().unwrap(), 9);
    // pct_change: 9 changes
    assert_eq!(m["pct_len"].as_i64().unwrap(), 9);
}

#[tokio::test]
async fn ts_ops_handle_edge_cases_gracefully() {
    // Empty input → all ops return () or empty array
    let empty = vec![];

    let out = run(
        r#"
        fn process(observations) {
            return [#{
                rate: ts_rate(observations),
                ma: ts_moving_avg(observations, 60),
                rs: ts_resample(observations, 60, "avg"),
                q: ts_quantile(observations, 0.5),
                p95: ts_p95(observations),
                p99: ts_p99(observations),
                d: ts_delta(observations),
                pct: ts_pct_change(observations),
            }];
        }
        "#,
        empty,
    )
    .await;

    let m = out.as_object().unwrap();
    assert_eq!(m["rate"], serde_json::Value::Null);
    assert_eq!(m["ma"], serde_json::Value::Array(vec![]));
    assert_eq!(m["rs"], serde_json::Value::Array(vec![]));
    assert_eq!(m["q"], serde_json::Value::Null);
    assert_eq!(m["p95"], serde_json::Value::Null);
    assert_eq!(m["p99"], serde_json::Value::Null);
    assert_eq!(m["d"], serde_json::Value::Array(vec![]));
    assert_eq!(m["pct"], serde_json::Value::Array(vec![]));
}