/// disk_markov_projection.rhai
/// Project when disk will be full using AnalysisGrid + Markov chain.
///
/// Uses grid_fit_values to find the minimum viable range of disk usage
/// bands, then models transitions between bands as a Markov chain
/// to calculate probability of breaking upper/lower range within N steps.
///
/// Configuration via params:
///
///   [pipeline.components.params]
///   cells = 20          # number of grid bands (default 20)
///   n_steps = 10        # number of future steps to project (default 10)
///   recent_secs = 3600  # only use this many seconds of recent data
///
/// Output observations:
///   - disk_markov_upper_prob: probability of hitting upper band (disk full)
///   - disk_markov_lower_prob: probability of hitting lower band (underflow)
///   - disk_markov_expected_steps: expected steps to reach any boundary
///   - disk_markov_grid_step: width of each band
///   - disk_markov_num_bands: number of bands
///
/// Helper functions
fn matrix_mult(a, b, n) {
    let result = [];
    for i in 0..n {
        let row = [];
        for j in 0..n {
            let mut sum = 0.0;
            for k in 0..n {
                sum = sum + a[i][k] * b[k][j];
            }
            row.push(sum);
        }
        result.push(row);
    }
    result
}

fn expected_absorption_steps(trans_matrix, current_state, upper_state, lower_state) {
    let n = trans_matrix.len();
    let max_steps = 100;

    let mut prob_upper = 0.0;
    let mut prob_lower = 0.0;
    let mut current_prob = [];
    for i in 0..n {
        current_prob.push(0.0);
    }
    current_prob[current_state] = 1.0;

    let mut expected = 0.0;
    for step in 1..max_steps + 1 {
        let mut next_prob = [];
        for i in 0..n {
            next_prob.push(0.0);
        }
        let mut absorbed = 0.0;
        for i in 0..n {
            if current_prob[i] > 0.001 {
                if i == upper_state {
                    absorbed = absorbed + current_prob[i];
                } else if i == lower_state {
                    absorbed = absorbed + current_prob[i];
                } else {
                    for j in 0..n {
                        next_prob[j] = next_prob[j] + current_prob[i] * trans_matrix[i][j];
                    }
                }
            }
        }
        expected = expected + step * absorbed;
        let total_next: float = next_prob.iter().sum();
        if total_next < 0.001 {
            break;
        }
        current_prob = next_prob;
    }
    expected
}

/// Main entry point – receives an array of observation maps and returns an
/// array of new observation maps with the Markov‑projection results.
fn process(observations) {
    // -----------------------------------------------------------------
    // 1️⃣  Configuration (can be overridden via pipeline params)
    // -----------------------------------------------------------------
    let cells = 20;
    try { cells = param_cells } catch { 20 };
    let n_steps = 10;
    try { n_steps = param_n_steps } catch { 10 };
    let recent_secs = 3600;
    try { recent_secs = param_recent_secs } catch { 3600 };

    // -----------------------------------------------------------------
    // 2️⃣  Basic validation
    // -----------------------------------------------------------------
    if observations.len() == 0 {
        return [];
    }

    // -----------------------------------------------------------------
    // 3️⃣  Determine the most recent timestamp and filter recent data
    // -----------------------------------------------------------------
    let tmax = 0;
    for x in observations {
        let t = x["ts"];
        if t > tmax { tmax = t; }
    }
    let cutoff = tmax - recent_secs;
    let recent = [];
    for x in observations {
        if x["ts"] > cutoff { recent.push(x); }
    }
    if recent.len() == 0 { return []; }

    // -----------------------------------------------------------------
    // 4️⃣  Extract the raw usage values
    // -----------------------------------------------------------------
    let values = [];
    for x in recent { values.push(x["value"]); }

    // -----------------------------------------------------------------
    // 5️⃣  Fit an AnalysisGrid to the values (0‑100 % usage range)
    // -----------------------------------------------------------------
    let g = grid_fit_values(values, 0.0, 100.0, cells);
    if g == () { return []; }
    let num_bands = num_cells(g);
    let step = grid_step(g);

    // -----------------------------------------------------------------
    // 6️⃣  Map each value to its grid cell (band index)
    // -----------------------------------------------------------------
    let bands = [];
    for v in values { bands.push(grid_cell(g, v)); }

    // -----------------------------------------------------------------
    // 7️⃣  Build the raw transition count matrix from the band sequence
    // -----------------------------------------------------------------
    let mut trans_counts = [];
    for i in 0..num_bands {
        trans_counts.push(vec![0; num_bands]);
    }
    for i in 0..bands.len() - 1 {
        let from = bands[i];
        let to   = bands[i+1];
        if from >= 0 && from < num_bands && to >= 0 && to < num_bands {
            trans_counts[from][to] = trans_counts[from][to] + 1;
        }
    }

    // -----------------------------------------------------------------
    // 8️⃣  Normalise counts → transition probabilities
    // -----------------------------------------------------------------
    let mut trans_matrix = [];
    for i in 0..num_bands {
        let row_sum = 0;
        for j in 0..num_bands {
            row_sum = row_sum + trans_counts[i][j];
        }
        let row = [];
        for j in 0..num_bands {
            if row_sum > 0 {
                row.push(trans_counts[i][j] as float / row_sum as float);
            } else {
                row.push(0.0);
            }
        }
        trans_matrix.push(row);
    }

    // -----------------------------------------------------------------
    // 9️⃣  Compute the n‑step transition matrix (Tⁿ)
    // -----------------------------------------------------------------
    let mut n_step = trans_matrix;
    for _ in 2..n_steps + 1 {
        n_step = matrix_mult(n_step, trans_matrix, num_bands);
    }

    // -----------------------------------------------------------------
    // 🔟  Derive absorption probabilities and expected steps
    // -----------------------------------------------------------------
    let upper_band = num_bands - 1;   // “disk full” absorbing state
    let lower_band = 0;               // “underflow” absorbing state

    let mut upper_prob_sum = 0.0;
    let mut lower_prob_sum = 0.0;
    let start_states = 5; // examine only the first few states for efficiency
    for s in 0..start_states {
        if s < num_bands {
            upper_prob_sum = upper_prob_sum + n_step[s][upper_band];
            lower_prob_sum = lower_prob_sum + n_step[s][lower_band];
        }
    }
    let upper_prob = upper_prob_sum / start_states as float;
    let lower_prob = lower_prob_sum / start_states as float;

    // Expected steps to absorption (first‑step analysis)
    let current_band = bands[bands.len() - 1];
    let expected_steps = expected_absorption_steps(trans_matrix, current_band, upper_band, lower_band);

    // -----------------------------------------------------------------
    // 1️⃣1️⃣  Emit output observations
    // -----------------------------------------------------------------
    let ts = now_secs();

    // Upper‑boundary risk
    out.push(#{
        ts: ts,
        metric_id: "disk_markov_upper_prob",
        kind: "metric",
        signal: "risk",
        value: upper_prob,
        labels: #{ n_steps: n_steps.to_string(),
                   num_bands: num_bands.to_string(),
                   grid_step: step.to_string(),
                   current_band: current_band.to_string(),
        },
    });

    // Lower‑boundary risk
    out.push(#{
        ts: ts,
        metric_id: "disk_markov_lower_prob",
        kind: "metric",
        signal: "risk",
        value: lower_prob,
        labels: #{ n_steps: n_steps.to_string(),
                   num_bands: num_bands.to_string(),
                   grid_step: step.to_string(),
        },
    });

    // Expected steps to hit either boundary
    out.push(#{
        ts: ts,
        metric_id: "disk_markov_expected_steps",
        kind: "metric",
        signal: "risk",
        value: expected_steps,
        labels: #{ current_band: current_band.to_string(),
                   upper_band: upper_band.to_string(),
                   lower_band: lower_band.to_string(),
        },
    });

    // Raw grid information (num_bands, step size, latest usage %)
    out.push(#{
        ts: ts,
        metric_id: "disk_markov_grid_info",
        kind: "metric",
        signal: "raw",
        value: num_bands,
        labels: #{ step: step.to_string(),
                   num_cells: num_bands.to_string(),
                   current_usage: values[values.len() - 1].to_string(),
        },
    });

    out
}