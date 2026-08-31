// Example Opsense Rhai transform: batch mean per metric.
//
// For each input observation it emits `<metric_id>_mean` carrying the mean
// value of that metric within the processed window. Input/output are arrays
// of observation maps: ts (int), metric_id (string), kind ("metric"),
// signal ("utilization"/...), value (float), optional labels map.

fn process(observations) {
    let totals = #{};
    let counts = #{};

    for obs in observations {
        let m = obs.metric_id;
        if !totals.contains(m) {
            totals[m] = 0.0;
            counts[m] = 0;
        }
        totals[m] += obs.value;
        counts[m] += 1;
    }

    let out = [];
    for obs in observations {
        let m = obs.metric_id;
        out.push(#{
            ts: obs.ts,
            metric_id: `${m}_mean`,
            kind: "metric",
            signal: "utilization",
            value: totals[m] / counts[m].to_float(),
            labels: #{}
        });
    }
    out
}
