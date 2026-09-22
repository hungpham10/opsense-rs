/// memory.rhai — Memory storage test processor.
///
/// In-memory only, no persistence. Verify data flows through.

fn process(observations) {
    if observations.len() == 0 { return []; }

    let mut metric_ids = [];
    for obs in observations {
        let id = obs["metric_id"];
        let found = false;
        for existing in metric_ids {
            if existing == id { found = true; break; }
        }
        if !found { metric_ids.push(id); }
    }

    let mut out = [];
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
                test_case: "memory",
                storage_backend: "memory",
            },
        });
    }

    out
}

fn mean(vals) { if vals.len() == 0 { return 0.0; } let s = 0.0; for x in vals { s = s + x as float; } s / vals.len() as float }
fn variance(vals) { if vals.len() < 2 { return 0.0; } let m = mean(vals); let s = 0.0; for x in vals { s = s + (x as float - m) * (x as float - m); } s / (vals.len() - 1) as float }
fn array_min(vals) { if vals.len() == 0 { return 0.0; } let mut m = vals[0] as float; for x in vals { if x as float < m { m = x as float; } } m }
fn array_max(vals) { if vals.len() == 0 { return 0.0; } let mut m = vals[0] as float; for x in vals { if x as float > m { m = x as float; } } m }