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

fn process(observations) {
    let cells = 20;
    try { cells = param_cells } catch { 20 };
    let n_steps = 10;
    try { n_steps = param_n_steps } catch { 10 };
    let recent_secs = 3600;
    try { recent_secs = param_recent_secs } catch { 3600 };

    if observations.len() == 0 {
        return [];
    }

    // Pass 1: find max timestamp
    let tmax = 0;
    for x in observations {
        let t = x["ts"];
        if t > tmax {
            tmax = t;
        }
    }

    // Pass 2: filter to recent data
    let cutoff = tmax - recent_secs;
    let recent = [];
    for x in observations {
        if x["ts"] > cutoff {
            recent.push(x);
        }
    }
    if recent.len() == 0 {
        return [];
    }

    // Extract values
    let values = [];
    for x in recent {
        values.push(x["value"]);
    }

    // Fit AnalysisGrid on values (biên vật lý 0..100 for % usage)
    let g = grid_fit_values(values, 0.0, 100.0, cells);
    if type_of(g) == "()" {
        return [];
    }

    let num_bands = num_cells(g);
    let step = grid_step(g);

    // Map each value to a grid cell (band index)
    let bands = [];
    for v in values {
        bands.push(grid_cell(g, v));
    }

    // Build Markov transition matrix from band sequence
    let mut trans_counts = [];
    for i in 0..num_bands {
        let row = [];
        for j in 0..num_bands {
            row.push(0);
        }
        trans_counts.push(row);
    }

    for i in 0..bands.len() - 1 {
        let from = bands[i];
        let to = bands[i + 1];
        if from >= 0 and from < num_bands and to >= 0 and to < num_bands {
            trans_counts[from][to] = trans_counts[from][to] + 1;
        }
    }

    // Normalize to get transition probabilities
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

    // Compute n-step transition matrix (T^n) using matrix multiplication
    let mut n_step = trans_matrix;
    for _ in 2..n_steps + 1 {
        n_step = matrix_mult(n_step, trans_matrix, num_bands);
    }

    // Calculate absorption probabilities
    // Upper boundary: last band (num_bands - 1) = disk full
    // Lower boundary: first band (0) = underflow
    let upper_band = num_bands - 1;
    let lower_band = 0;

    // P(absorbing into upper) = n_step[any_start][upper_band] averaged
    let mut upper_prob_sum = 0.0;
    let mut lower_prob_sum = 0.0;
    let start_states = num_bands.min(5);
    for s in 0..start_states {
        upper_prob_sum = upper_prob_sum + n_step[s][upper_band];
        lower_prob_sum = lower_prob_sum + n_step[s][lower_band];
    }
    let upper_prob = upper_prob_sum / start_states as float;
    let lower_prob = lower_prob_sum / start_states as float;

    // Expected steps to absorption (simplified)
    let current_val = values[values.len() - 1];
    let current_band = grid_cell(g, current_val);
    let expected_steps = expected_absorption_steps(trans_matrix, current_band, upper_band, lower_band);

    // Build output observations
    let out = [];

    out.push(#{
        ts: now_secs(),
        metric_id: "disk_markov_upper_prob",
        kind: "metric",
        signal: "risk",
        value: upper_prob,
        labels: #{
            n_steps: n_steps.to_string(),
            num_bands: num_bands.to_string(),
            grid_step: step.to_string(),
            current_band: current_band.to_string(),
        },
    });

    out.push(#{
        ts: now_secs(),
        metric_id: "disk_markov_lower_prob",
        kind: "metric",
        signal: "risk",
        value: lower_prob,
        labels: #{
            n_steps: n_steps.to_string(),
            num_bands: num_bands.to_string(),
            grid_step: step.to_string(),
        },
    });

    out.push(#{
        ts: now_secs(),
        metric_id: "disk_markov_expected_steps",
        kind: "metric",
        signal: "risk",
        value: expected_steps,
        labels: #{
            current_band: current_band.to_string(),
            upper_band: upper_band.to_string(),
            lower_band: lower_band.to_string(),
        },
    });

    out.push(#{
        ts: now_secs(),
        metric_id: "disk_markov_grid_info",
        kind: "metric",
        signal: "raw",
        value: num_bands,
        labels: #{
            step: step.to_string(),
            num_cells: num_bands.to_string(),
            current_usage: current_val.to_string(),
        },
    });

    out
}

/// Matrix multiplication for square matrices of size n
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

/// Calculate expected steps to absorption from current state using
/// first-step analysis approximation
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
