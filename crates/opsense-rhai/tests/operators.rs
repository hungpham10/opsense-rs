//! Integration tests for the time-series operator library registered on the
//! Rhai engine (`ts_rate`, `ts_moving_avg`, `ts_resample`, `ts_quantile`,
//! `ts_p95`, `ts_p99`, `ts_delta`, `ts_pct_change`).

use opsense_rhai::{call_process, ScriptSource};

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

    let m = out.as_object().expect("result is a map");
    assert_eq!(m["rate"].as_f64().unwrap(), 1.0 / 60.0, "rate = Δvalue/Δt");
    assert_eq!(m["ma_len"].as_u64().unwrap(), 10);
    assert_eq!(
        m["rs_len"].as_u64().unwrap(),
        5,
        "10 points / 120s = 5 buckets"
    );
    assert_eq!(m["q"].as_f64().unwrap(), 4.5, "median of 0..9");
    assert!(m["p95"].as_f64().unwrap() > 8.5 && m["p95"].as_f64().unwrap() < 9.5);
    assert!(m["p99"].as_f64().unwrap() >= 8.9);
    assert_eq!(m["d_len"].as_u64().unwrap(), 10);
    assert_eq!(m["pct_len"].as_u64().unwrap(), 10);
}

#[tokio::test]
async fn ts_ops_handle_empty_and_single() {
    // Empty input → scalars are unit, series are empty.
    let out = run(
        r#"
        fn process(observations) {
            let r = ts_rate(observations);
            let q = ts_quantile(observations, 0.5);
            let d = ts_delta(observations);
            return [#{r: r, q: q, d_len: d.len()}];
        }
        "#,
        vec![],
    )
    .await;
    let m = out.as_object().unwrap();
    assert!(m["r"].is_null(), "rate of empty is unit");
    assert!(m["q"].is_null(), "quantile of empty is unit");
    assert_eq!(m["d_len"].as_u64().unwrap(), 0);

    // Single point → scalars unit, delta one entry (0.0).
    let out = run(
        r#"
        fn process(observations) {
            let r = ts_rate(observations);
            let q = ts_quantile(observations, 0.5);
            let d = ts_delta(observations);
            return [#{r: r, q: q, d_len: d.len()}];
        }
        "#,
        vec![obs(100, 5.0)],
    )
    .await;
    let m = out.as_object().unwrap();
    assert!(m["r"].is_null(), "rate needs ≥2 points");
    assert_eq!(
        m["q"].as_f64().unwrap(),
        5.0,
        "quantile of one point is that point"
    );
    assert_eq!(m["d_len"].as_u64().unwrap(), 1);
}
