# Disk Full Projection Using Markov Chains

This document explains the detailed approach for projecting when disk space will become full using a combination of AnalysisGrid for minimum viable range detection and Markov chain probability calculation for forecasting boundary breaches.

## 1. Mathematical Foundation

### 1.1 AnalysisGrid Component
The AnalysisGrid algorithm creates a hierarchical sieve of usage bands that minimizes boundary-crossing spikes. Key characteristics:
- Input: Continuous disk usage percentages (0-100%)
- Output: Discrete band indices representing usage occupancy levels
- Multi-resolution analysis with adaptive band selection based on crossing statistics
- Identifies "minimum viable range" where most usage observations cluster

### 1.2 Markov Chain Modeling
We model state transitions between grid bands to forecast future behavior:
- **States**: Grid cell indices (0..N-1) representing usage bands
- **Transitions**: Observed state-to-state movements in historical data
- **Transition Matrix**: Stochastic matrix P where P[i][j] = P(next=j|current=i)

### 1.3 Probabilistic Forecasting
Using matrix exponentiation, we compute:
- **n-step transition probabilities**: P(Xₙ=j|X₀=i)
- **Absorption probabilities**: Likelihood of reaching boundary states (full/empty)
- **Expected absorption time**: Forecast horizon until boundary breach

## 2. Algorithm Implementation

### 2.1 State Mapping
```python
# Pseudo-code for state assignment
band_index = AnalysisGrid.cell(usage_percentage)
```

### 2.2 Transition Matrix Construction
```python
# Count state transitions
for i in range(len(states)-1):
    from_state = states[i]
    to_state = states[i+1]
    transition_counts[from_state][to_state] += 1
```

### 2.3 Probability Calculation
```python
# Normalize to get probabilities
for each row in transition_matrix:
    row_sum = sum(row)
    normalized_row = row / row_sum if row_sum > 0 else row

# Compute n-step probabilities via matrix multiplication
n_step_matrix = matrix_power(transition_matrix, n)
absorption_prob = n_step_matrix[i][boundary_state]
```

## 3. Application Pipeline

### 3.1 Input Configuration
- `cells`: Number of grid bands (typically 10-20)
- `n_steps`: Forecast horizon in steps (default 10)
- `recent_secs`: Lookback window size (default 3600s)

### 3.2 Processing Steps
1. Parse recent observations from pipeline input
2. Fit AnalysisGrid to detect minimum viable usage range
3. Map usage values to grid band indices
4. Build Markov transition matrix from state sequence
5. Compute n-step transition probabilities
6. Calculate absorption probabilities for boundary states
7. Output risk metrics with detailed metadata

## 4. Expected Output Observations

The system emits risk assessment observations:
```json
[
  {
    "metric_id": "disk_markov_upper_prob",
    "value": 0.12,
    "labels": {
      "n_steps": "10",
      "num_bands": "20",
      "grid_step": "5.0",
      "current_band": "5"
    }
  },
  {
    "metric_id": "disk_markov_lower_prob", 
    "value": 0.01,
    "labels": {
      "n_steps": "10",
      "num_bands": "20",
      "grid_step": "5.0"
    }
  }
]
```

## 5. Verification and Testing

### 5.1 Unit Tests
- Validate state transition counting accuracy
- Verify matrix exponentiation correctness
- Test absorption probability calculations
- Ensure edge cases handled properly

### 5.2 Integration Tests
- End-to-end pipeline validation
- Configuration loading verification
- Backward compatibility assurance

## 6. Applications

This projection capability enables:
- Proactive disk space management
- Automated capacity planning triggers
- Predictive alerting for storage exhaustion
- Data-driven scaling decisions
- Enhanced system reliability through anticipation of resource constraints