//! TOML configuration for Opsense.
//!
//! Mirrors `opsense.conf.toml`:
//! ```toml
//! [engine]
//! poll_interval_seconds = 60
//! cache_block_seconds = 300
//! cache_max_blocks = 288
//!
//! [capacity]
//! cpu_usage = 32.0
//! mem_usage = 64.0
//!
//! [sources.vector]
//! url = "http://vector:8686"
//! jq_filter = ".data[]"
//! metrics = ["cpu_usage", "mem_usage"]
//!
//! [attributes]
//! dc = "hcm"
//! ```

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load config: {0}")]
    Load(#[from] config_crate::ConfigError),

    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Fields default individually (`#[serde(default)]` at the struct level) so a
/// config may override e.g. only `poll_interval_seconds`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub poll_interval_seconds: u64,
    pub cache_block_seconds: u64,
    pub cache_max_blocks: usize,

    /// Python interpreter used by analysis sessions (`opsense serve --repl`).
    /// Empty/absent = auto-detect `python3` on PATH.
    pub python_path: String,
    /// Packages probed at session start; missing ones warn but do not block
    /// (they only fail when a specific analysis actually imports them).
    pub python_packages: Vec<String>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 60,
            cache_block_seconds: 300,
            cache_max_blocks: 288,
            python_path: String::new(),
            python_packages: [
                "numpy",
                "pandas",
                "pyarrow",
                "scipy",
                "sklearn",
                "statsmodels",
                "matplotlib",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        }
    }
}

/// Interactive session limits (`[session]`). Applied to every new session;
/// Python sandboxes additionally enforce the memory cap at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionConfig {
    pub max_memory_mb: u64,
    pub max_cpu_time_secs: u64,
    pub max_result_rows: u64,
    pub max_execution_time_secs: u64,
    /// Sessions idle longer than this are closed automatically.
    pub idle_timeout_secs: u64,
    pub allow_fs: bool,
    pub allow_net: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 2048,
            max_cpu_time_secs: 300,
            max_result_rows: 1_000_000,
            max_execution_time_secs: 60,
            idle_timeout_secs: 1800,
            allow_fs: false,
            allow_net: false,
        }
    }
}

/// REPL shell settings (`[repl]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplConfig {
    pub history_file: String,
    pub max_history: usize,
    pub completion: bool,
    /// Station pre-selected as "current" when the REPL starts (empty = none).
    pub default_station: String,
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            history_file: "~/.opsense/history.txt".to_string(),
            max_history: 10_000,
            completion: true,
            default_station: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSourceConfig {
    pub url: String,

    /// jq-style filter applied to the Vector payload (uses `opsense_libs::jq::JsonQuery`).
    #[serde(default)]
    pub jq_filter: Option<String>,

    /// Optional allow-list of metric names to pull from the source.
    #[serde(default)]
    pub metrics: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesConfig {
    #[serde(default)]
    pub vector: Option<VectorSourceConfig>,
}

/// Capacity per metric (metric name -> maximum capacity, e.g. cores, GB).
pub type CapacityMap = HashMap<String, f64>;

/// Pipeline section: components registered into the vector `Runtime`.
///
/// Each entry is a typetag-tagged component table, e.g.
/// ```toml
/// [[pipeline.components]]
/// type = "clock_source"
/// id = "clock"
/// interval_secs = 30
///
/// [[pipeline.components]]
/// type = "collector_sink"
/// id = "collector"
/// inputs = ["clock"]
/// ```
/// The `type` selects the registered `Component` (typetag name = snake_case of
/// the struct). Unknown types fail at load time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineConfig {
    #[serde(default)]
    pub components: Vec<serde_json::Value>,
}

/// Storage backend selection for the pipeline stores.
///
/// `backend` selects the main store: `"memory"` (LRU, default — easiest for
/// tests), `"duckdb"` (Parquet lakehouse via DuckDB) or `"lmdb"` (local
/// key-value, range-scannable). `mirror` optionally double-writes to a second
/// backend (e.g. duckdb + lmdb).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub backend: String,
    pub data_dir: String,

    /// When > 0, a background task trims the main store every minute: points
    /// at or before `now - retention_secs` are dropped (whole block
    /// partitions for the parquet lakehouse). 0 keeps history forever.
    pub retention_secs: u64,

    /// Width of one parquet block partition for the lakehouse backends
    /// (`duckdb`, `s3`): points are bucketed into `floor(ts / block_secs)`
    /// and written as `<stage>/blk=<start_ts>/batch_NNN.parquet`. Larger
    /// blocks mean fewer files (cheaper listing) but coarser retention
    /// granularity. Default 3600 (one hour).
    pub block_secs: u64,

    pub mirror: Option<StorageBackendConfig>,

    /// Nén Parquet cho lakehouse writes: `zstd` (mặc định) | `snappy` |
    /// `gzip` | `uncompressed`. ZSTD giảm đáng kể dung lượng/chi phí S3.
    #[serde(default = "default_parquet_compression")]
    pub parquet_compression: String,

    /// Kết nối S3 cho DuckDB httpfs khi backend/data_dir là `s3://`.
    /// Mỗi field thiếu trong TOML sẽ được bù bằng env `OPSENSE_S3_*`.
    #[serde(default)]
    pub s3: Option<S3Config>,
}

/// Kết nối S3 cho DuckDB httpfs. Các field đều Option để cho phép dùng biến
/// môi trường AWS chuẩn của httpfs khi không khai báo gì.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    /// Endpoint tuỳ ý (MinIO/self-hosted). Bỏ trống = AWS public.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// `path` cho MinIO-style buckets; bỏ trống = virtual-host.
    pub url_style: Option<String>,
}

fn default_parquet_compression() -> String {
    "zstd".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: "memory".to_string(),
            data_dir: ".opsense/lakehouse".to_string(),
            retention_secs: 0,
            block_secs: 3600,
            mirror: None,
            parquet_compression: default_parquet_compression(),
            s3: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageBackendConfig {
    pub backend: String,
    pub data_dir: String,
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        Self {
            backend: "lmdb".to_string(),
            data_dir: ".opsense/lmdb".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub engine: EngineConfig,

    #[serde(default)]
    pub capacity: CapacityMap,

    #[serde(default)]
    pub sources: SourcesConfig,

    /// Free-form key/values available to pipeline components as template
    /// variables (`{{name}}` in an HTTP node's URL/headers/params/body).
    /// Environment variables named `OPSENSE_ATTR_<NAME>` (uppercase) override
    /// the TOML values at resolution time.
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// Storage backends for the raw/processed pipeline stores.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Optional explicit pipeline; when absent a default
    /// `clock -> ingest -> processor -> persist` graph is built from
    /// `engine.poll_interval_seconds`.
    #[serde(default)]
    pub pipeline: Option<PipelineConfig>,

    /// Interactive analysis sessions (`[session]`).
    #[serde(default)]
    pub session: SessionConfig,

    /// REPL shell settings (`[repl]`), used by `opsense serve --repl`.
    #[serde(default)]
    pub repl: ReplConfig,
}

impl Config {
    /// Load and validate a TOML config from `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from(path.to_path_buf()))
            .build()?;
        let cfg: Config = raw.try_deserialize()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate invariants that the TOML parser cannot enforce on its own.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.engine.poll_interval_seconds == 0 {
            return Err(ConfigError::Invalid(
                "engine.poll_interval_seconds must be > 0".into(),
            ));
        }
        if self.engine.cache_block_seconds == 0 {
            return Err(ConfigError::Invalid(
                "engine.cache_block_seconds must be > 0".into(),
            ));
        }
        if self.engine.cache_max_blocks == 0 {
            return Err(ConfigError::Invalid(
                "engine.cache_max_blocks must be > 0".into(),
            ));
        }
        if self.capacity.is_empty() {
            return Err(ConfigError::Invalid(
                "capacity must define at least one metric".into(),
            ));
        }
        Ok(())
    }

    /// `[attributes]` merged with their environment overrides: any variable
    /// `OPSENSE_ATTR_<NAME>` (uppercase of the key) wins over the TOML value,
    /// and such env entries are picked up even when absent from the file — so
    /// deployments can inject secrets (tokens, endpoints) without editing it.
    #[must_use]
    pub fn resolved_attributes(&self) -> BTreeMap<String, String> {
        let mut out: BTreeMap<String, String> = self.attributes.clone().into_iter().collect();
        const PREFIX: &str = "OPSENSE_ATTR_";
        for (env_key, value) in std::env::vars() {
            if let Some(name) = env_key.strip_prefix(PREFIX) {
                if !value.is_empty() {
                    out.insert(name.to_ascii_lowercase(), value);
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_crate::FileFormat;

    const SAMPLE: &str = r#"
[engine]
poll_interval_seconds = 60
cache_block_seconds = 300
cache_max_blocks = 288

[capacity]
cpu_usage = 32.0
mem_usage = 64.0

[sources.vector]
url = "http://vector:8686"
jq_filter = ".data[]"
metrics = ["cpu_usage", "mem_usage"]

[attributes]
dc = "hcm"
env_name = "prod"
"#;

    fn sample() -> Config {
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(SAMPLE, FileFormat::Toml))
            .build()
            .unwrap();
        raw.try_deserialize().unwrap()
    }

    #[test]
    fn parses_capacity_sources_and_attributes() {
        let cfg = sample();
        assert_eq!(cfg.capacity.get("cpu_usage"), Some(&32.0));
        assert_eq!(cfg.capacity.get("mem_usage"), Some(&64.0));
        assert_eq!(cfg.attributes.get("dc").map(String::as_str), Some("hcm"));
        let v = cfg.sources.vector.unwrap();
        assert_eq!(v.url, "http://vector:8686");
        assert!(v.jq_filter.is_some());
        assert_eq!(v.metrics.unwrap(), vec!["cpu_usage", "mem_usage"]);
    }

    #[test]
    fn defaults_engine_when_omitted() {
        let toml = r#"
[capacity]
cpu_usage = 32.0
"#;
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(toml, FileFormat::Toml))
            .build()
            .unwrap();
        let cfg: Config = raw.try_deserialize().unwrap();
        assert_eq!(cfg.engine.poll_interval_seconds, 60);
        // Sources are optional now — pipeline HTTP nodes fetch on their own.
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rejects_empty_capacity() {
        let toml = r#"
[sources.vector]
url = "http://vector:8686"
"#;
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(toml, FileFormat::Toml))
            .build()
            .unwrap();
        let cfg: Config = raw.try_deserialize().unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn attributes_resolve_with_env_override() {
        std::env::set_var("OPSENSE_ATTR_DC", "hn");
        std::env::set_var("OPSENSE_ATTR_TOKEN", "s3cret");
        let cfg = sample();

        let attrs = cfg.resolved_attributes();
        // env wins over the TOML value…
        assert_eq!(attrs.get("dc").map(String::as_str), Some("hn"));
        // …and env-only entries appear without any TOML declaration.
        assert_eq!(attrs.get("token").map(String::as_str), Some("s3cret"));
        assert_eq!(attrs.get("env_name").map(String::as_str), Some("prod"));

        std::env::remove_var("OPSENSE_ATTR_DC");
        std::env::remove_var("OPSENSE_ATTR_TOKEN");
    }
}
