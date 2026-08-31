// Map a Prometheus HTTP API response to Opsense observations.
//
// Works for BOTH result shapes:
//   /api/v1/query       -> data.result[i].value  = [ts, "value"]          (vector)
//   /api/v1/query_range -> data.result[i].values = [[ts,"v"], ...]        (matrix)
//
// metric_id comes from labels.__name__; every other label (instance, job, ...)
// is preserved. Pair this node config with a query_range request:
//
//   [[pipeline.components]]
//   type    = "http_source"
//   id      = "prom"
//   inputs  = ["clock"]
//   url     = "{{prom_url}}/api/v1/query_range"
//   format  = "script"
//   script_path = "scripts/prom_query_range.rhai"
//   [pipeline.components.params]
//   query = "rate(node_cpu_seconds_total[5m])"
//   start = "{{from_ts}}"
//   end   = "{{to_ts}}"
//   step  = "60"

fn process(body) {
    let out = [];

    if body.status != "success" {
        // Non-empty output expected by contract; surface the API status.
        throw `prometheus answered status=${body.status}`;
    }

    for r in body.data.result {
        let name = "unknown";
        let labels = #{};
        if r.metric != () {
            if r.metric.__name__ != () {
                name = r.metric.__name__;
            }
            for k in keys(r.metric) {
                if k != "__name__" {
                    labels[k] = r.metric[k];
                }
            }
        }

        // Matrix has `values`, instant vector has a single `value`.
        let samples = [];
        if r.values != () {
            samples = r.values;
        } else if r.value != () {
            samples = [r.value];
        }

        for s in samples {
            out.push(#{
                ts: s[0].to_int(),          // prometheus ts arrives as float
                metric_id: name,
                kind: "metric",
                signal: "utilization",
                value: parse_float(s[1]),   // sample value is a string
                labels: labels,
            });
        }
    }

    out
}
