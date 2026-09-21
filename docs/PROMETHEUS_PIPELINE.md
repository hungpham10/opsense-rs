# Prometheus Data Collection Pipeline

The Prometheus data collection pipeline is defined in `examples/prometheus-demo/config.toml` and consists of the following key components:

## 1. Clock Source
- Provides a regular trigger with `interval_secs` (default 30 seconds)
- Drives the timing for all subsequent components

## 2. HTTP Source (`prom-explore`)
- Queries Prometheus at `{{prom_url}}/api/v1/query_range`
- Configuration:
  - `initial_lookback_secs`: Size of initial query range (default 900 seconds)
  - `timeout_secs`: Request timeout (default 30 seconds)
  - `station = true`: Enables queryable Timeseries storage
- Parameters:
  - `query`: PromQL query string (e.g., `up`)
  - `start`: Start timestamp (`{{from_ts}}`)
  - `end`: End timestamp (`{{to_ts}}`)
  - `step`: Resolution step (`60`)

## 3. Fields Mapping
Converts Prometheus response matrix into structured observations:
- `ts`: Timestamp (integer, cast to i64)
- `value`: Metric value (float, cast to f64)
- `labels`: Extracted labels using regex `^.^.metric` (captures metric_id)

## 4. Disk Usage Component
- Queries disk usage with PromQL: `100 * (1 - node_filesystem_avail_bytes / node_filesystem_size_bytes)`
- Configuration matches HTTP Source parameters
- Encodes observations with:
  - `metric_id`: Mountpoint label
  - `value`: Percentage disk usage (0-100)
  - `labels`: `mountpoint` and `device`

## 5. Station Registration
- All `http_source` components register with `station = true`
- This creates queryable Timeseries stations accessible via:
  - `opsense_ts_query`
  - `opsense_ts_mean`
  - `opsense_ts_rate` and other time-series operators

## 6. Pipeline Flow
1. `clock_source` triggers periodically
2. `prom-explore` queries Prometheus
3. Response is parsed into observations
4. Observations flow through subsequent transforms
5. Disk usage data is processed by `disk-grid` Rhai script
6. Output observations are stored in Timeseries stations

## 7. Configuration Variables
- `prom_url`: Prometheus server URL (from `[attributes]`)
- `OPSENSE_ATTR_*`: Environment variable overrides for attributes
- `{{from_ts}}`, `{{to_ts}}`: Timestamp templates
- `{{step}}`: Resolution step template

## 8. Error Handling
- If cursor doesn't advance, window is retried on next tick
- Persistent watermark: `<data_dir>/watermarks.json`
  - Stores earliest timestamp for each station
  - Survives server restarts
  - Prevents reprocessing old data

## 9. Extending the Pipeline
To add new components:
1. Create new pipeline component with unique `id`
2. Connect it to existing `inputs`
3. Register appropriate `station` and `items` fields
4. Add parameters and mappings as needed
5. Wire into `null` sink to complete flow