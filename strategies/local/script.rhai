/// local.rhai — Local parquet storage test processor.
///
/// Processes observations from the clock → station pipeline.
/// Emits summary statistics for verification.

fn process(observations) {
    if observations.len() == 0 { return []; }

    let metric_ids = [];
    for obs in observations {
        let id = obs["metric_id"];
        let found = false;
        for existing in metric_ids {
            if existing == id { found = true; break; }
        }
        if !found { metric_ids.push(id); }
    }

    let out = [];
    let ts = now_secs();

    for id in metric_ids {
        let vals = [];
        for obs in observations {
            if obs["metric_id"] == id { vals.push(obs["value"]); }
        }
        if vals.len() == 0 { continue; }

        let v_mean = mean(vals);
        let v_var = variance(vals);
        let v_min = array_min(vals);
        let v_max = array_max(vals);
        let v_count = vals.len();

        out.push(#{
            ts: ts,
            metric_id: id,
            kind: "metric",
            signal: "summary",
            value: v_mean,
            labels: #{
                count: v_count.to_string(),
                min: v_min.to_string(),
                max: v_max.to_string(),
                variance: v_var.to_string(),
                mean: v_mean.to_string(),
                test_case: "local",
                storage_backend: "parquet",
            },
        });
    }

    out
}

fn mean(vals) { if vals.len() == 0 { return 0.0; } let s = 0.0; for x in vals { s = s + x.to_float(); } s / vals.len().to_float() }
fn variance(vals) { if vals.len() < 2 { return 0.0; } let m = mean(vals); let s = 0.0; for x in vals { s = s + (x.to_float() - m) * (x.to_float() - m); } s / (vals.len() - 1).to_float() }
fn array_min(vals) { if vals.len() == 0 { return 0.0; } let m = vals[0].to_float(); for x in vals { if x.to_float() < m { m = x.to_float(); } } m }
fn array_max(vals) { if vals.len() == 0 { return 0.0; } let m = vals[0].to_float(); for x in vals { if x.to_float() > m { m = x.to_float(); } } m }