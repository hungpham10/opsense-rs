//! TOML configuration for Opsense.
//!
//! Mirrors `opsense.conf.toml`:
//! ```toml
//! [engine]
//! poll_interval_seconds = 60
//! cache_block_seconds = 300
//! cache_max_blocks = 288
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
use std::error::Error as StdError;
use std::path::Path;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum ConfigError {
    Load(config_crate::ConfigError),
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Load(e) => write!(f, "failed to load config: {e}"),
            ConfigError::Invalid(msg) => write!(f, "invalid config: {msg}"),
        }
    }
}

impl StdError for ConfigError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ConfigError::Load(e) => Some(e),
            ConfigError::Invalid(_) => None,
        }
    }
}

impl From<config_crate::ConfigError> for ConfigError {
    fn from(e: config_crate::ConfigError) -> Self {
        ConfigError::Load(e)
    }
}

/// Fields default individually (`#[serde(default)]` at the struct level) so a
/// config may override e.g. only `poll_interval_seconds`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub poll_interval_seconds: u64,
    pub cache_block_seconds: u64,
    pub cache_max_blocks: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 60,
            cache_block_seconds: 300,
            cache_max_blocks: 288,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSourceConfig {
    pub url: String,

    /// jq-style filter applied to the Vector payload (uses `opsense_mlib::jq::JsonQuery`).
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
/// tests), `"parquet"` (Parquet storage — canonical; local filesystem hoặc
/// object store khi `data_dir` là `s3://…`) hoặc `"sqlite"` (local file).
/// `mirror` optionally double-writes to a second backend.
/// Tên backend cũ `"duckdb"`/`"s3"`/`"lakehouse"` vẫn được chấp nhận trong code
/// như alias deprecated (tất cả mở cùng Parquet storage) nhưng không nên dùng
/// trong config mới.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub backend: String,
    pub data_dir: String,

    /// When > 0, a background task trims the main store every minute: whole
    /// `ts/blk=<id>…` partitions whose data is older than `now - retention_secs`
    /// are dropped. 0 keeps history forever.
    pub retention_secs: u64,

    /// Width of one timeseries block partition (seconds) for the `"parquet"`
    /// backend: points are bucketed into `block_id = floor(ts / block_secs)`
    /// and written as `ts/blk=<block_id>/batch-<millis>.parquet`. External
    /// engines (Spark/DuckDB/Polars) prune on the `blk=` partition column.
    /// Larger blocks mean fewer files (cheaper S3 listing) but coarser
    /// retention granularity. Default 3600 (one hour).
    pub block_secs: u64,

    pub mirror: Option<StorageBackendConfig>,

    /// Nén Parquet cho parquet writes: `zstd` (mặc định) | `snappy` |
    /// `gzip` | `uncompressed`. ZSTD giảm đáng kể dung lượng/chi phí S3.
    #[serde(default = "default_parquet_compression")]
    pub parquet_compression: String,

    /// Kết nối S3 cho Parquet storage khi `data_dir` là `s3://`.
    /// Mỗi field thiếu trong TOML sẽ được bù bằng env `OPSENSE_S3_*`.
    #[serde(default)]
    pub s3: Option<S3Config>,

    /// Khi bật `[storage].s3`: tần suất (giây) station tự flush buffer timeseries
    /// ra lake parquet + mirror S3 (`ts/blk=…/batch-*.parquet`). 0 = tắt lịch
    /// (chỉ flush khi buffer đạt ngưỡng `flush_threshold`). Mặc định 60.
    #[serde(default = "default_s3_flush_interval_secs")]
    pub s3_flush_interval_secs: u64,

    /// Khi bật `[storage].s3`: tần suất (giây) snapshot/checkpoint định kỳ —
    /// compact WAL + mirror state lên `state/` để process khác restore được.
    /// Mặc định 600.
    #[serde(default = "default_s3_snapshot_interval_secs")]
    pub s3_snapshot_interval_secs: u64,
}

fn default_s3_flush_interval_secs() -> u64 {
    60
}

fn default_s3_snapshot_interval_secs() -> u64 {
    600
}

/// Kết nối S3 cho Parquet storage — nơi đặt lake: toàn bộ data-parquet sống ở
/// `s3://{bucket}/{prefix}/{station}/ts/…` (timeseries, đọc được bởi
/// Spark/Polars/DuckDB) và `…/{station}/state/…` (checkpoint để mở lại). Các
/// field đều Option để cho phép dùng biến môi trường AWS chuẩn khi không khai
/// báo gì.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    /// Bucket chứa lake (bắt buộc khi dùng S3). Bù bằng env `OPSENSE_S3_BUCKET`.
    pub bucket: String,
    /// Prefix gốc của lake, VD `"opsense/prod"` — không `/` ở hai đầu. Bù bằng
    /// env `OPSENSE_S3_PREFIX`.
    pub prefix: String,
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
            data_dir: ".opsense/parquet".to_string(),
            retention_secs: 0,
            block_secs: 3600,
            mirror: None,
            parquet_compression: default_parquet_compression(),
            s3: None,
            s3_flush_interval_secs: default_s3_flush_interval_secs(),
            s3_snapshot_interval_secs: default_s3_snapshot_interval_secs(),
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
            backend: "parquet".to_string(),
            data_dir: ".opsense/parquet".to_string(),
        }
    }
}

/// Cấu hình tầng gossip — xem [`crate::config::Config::gossip`].
///
/// Tách theo đúng ranh giới của 2 lib: ở đây chỉ có *quan sát* (ai sống, ai
/// chết sau bao lâu), không có quyết định master.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GossipConfig {
    /// Định danh node này. Rỗng ⇒ không bật mesh.
    pub node_id: String,
    /// URL mà node khác gọi tới node này (đưa vào roster cho node mới).
    pub own_url: String,
    /// Danh sách URL seed, phân tách bằng dấu phẩy. Rỗng ⇒ node không có seed.
    pub seeds: String,
    /// Secret dùng cho endpoint nội bộ (`/internal/*`). Rỗng ở môi trường thật
    /// là lỗi cấu hình — xem [`Config::validate_gossip`].
    pub token: String,
    /// Chu kỳ một vòng: ping peer, đồng bộ state, tái thu, rồi báo cáo.
    pub tick_secs: u64,
    /// Im lặng bao lâu thì chuyển `alive → suspect` (suy đoán, chưa kết luận).
    pub suspect_secs: u64,
    /// Im lặng bao lâu thì tái thu `suspect → dead`.
    pub dead_secs: u64,
    /// View ổn định bao lâu thì mới để tầng trên hành động (bật/dừng pipeline).
    pub settle_secs: u64,
    /// Số peer xác nhận "không thấy" để **rút ngắn** thời gian chờ tái thu.
    /// Không phải điều kiện quyết định — thời gian im lặng mới là.
    pub quorum: u32,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            own_url: String::new(),
            seeds: String::new(),
            token: String::new(),
            tick_secs: 5,
            suspect_secs: 5,
            dead_secs: 30,
            settle_secs: 10,
            quorum: 1,
        }
    }
}

impl GossipConfig {
    /// Có bật mesh không: phải có `node_id` **và** `own_url` (không có URL thì
    /// peer không gọi tới được).
    #[must_use]
    pub fn enabled(&self) -> bool {
        !self.node_id.is_empty()
    }

    /// Seed đã tách thành danh sách URL.
    #[must_use]
    pub fn seed_urls(&self) -> Vec<&str> {
        self.seeds.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()
    }
}

/// Cấu hình tầng raft — xem [`crate::config::Config::raft`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct RaftConfig {
    /// Pipeline mà node đứng một mình sẽ chạy (khi chưa ai gom nó vào cụm nào).
    pub pipeline: String,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub engine: EngineConfig,

    #[serde(default)]
    pub sources: SourcesConfig,

    /// Free-form key/values available to pipeline components as template
    /// variables (`{{name}}` in an HTTP node's URL/headers/body).
    /// Environment variables named `OPSENSE_ATTR_<NAME>` (uppercase) override
    /// the TOML values at resolution time.
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// Storage backends for the raw/processed pipeline stores.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Optional explicit pipeline; when absent a default
    /// `clock -> null` graph is built from
    /// `engine.poll_interval_seconds`.
    #[serde(default)]
    pub pipeline: Option<PipelineConfig>,

    /// Mesh membership settings (`[gossip]`) — **quan sát**: node này là ai, ai
    /// khác, và thời gian chờ bao lâu thì coi node khác là chết.
    ///
    /// Mỗi trường đều bị `OPSENSE_GOSSIP_<FIELD>` ghi đè khi chạy. `node_id` rỗng
    /// ⇒ **tắt mesh**, node chạy đơn lẻ như hiện tại.
    #[serde(default)]
    pub gossip: GossipConfig,

    /// Cluster/decision settings (`[raft]`) — **quyết định**: node đứng một mình
    /// chạy pipeline nào, và có cắm engine quyết định chưa.
    #[serde(default)]
    pub raft: RaftConfig,
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
        // S3 lake cần bucket — prefix (rỗng = gốc bucket) là tuỳ chọn.
        if let Some(s3) = &self.storage.s3
            && s3.bucket.is_empty()
        {
            return Err(ConfigError::Invalid(
                "storage.s3.bucket must be set (VD `opsense-lake`)".into(),
            ));
        }
        if self.storage.backend.trim().is_empty() {
            return Err(ConfigError::Invalid("storage.backend must not be empty".into()));
        }
        if self.storage.block_secs == 0 {
            return Err(ConfigError::Invalid(
                "storage.block_secs must be > 0 (parquet block partition width)".into(),
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
            if let Some(name) = env_key.strip_prefix(PREFIX)
                && !value.is_empty()
            {
                out.insert(name.to_ascii_lowercase(), value);
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
    fn parses_sources_and_attributes() {
        let cfg = sample();
        assert_eq!(cfg.attributes.get("dc").map(String::as_str), Some("hcm"));
        let v = cfg.sources.vector.unwrap();
        assert_eq!(v.url, "http://vector:8686");
        assert!(v.jq_filter.is_some());
        assert_eq!(v.metrics.unwrap(), vec!["cpu_usage", "mem_usage"]);
    }

    #[test]
    fn defaults_engine_when_omitted() {
        // TOML rỗng không hợp lệ (crate `config` đòi có ít nhất một dòng), nên
        // dùng một file chỉ có comment — tức "không khai gì".
        let toml = "# không khai gì";
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
    fn attributes_resolve_with_env_override() {
        unsafe {
            std::env::set_var("OPSENSE_ATTR_DC", "hn");
            std::env::set_var("OPSENSE_ATTR_TOKEN", "s3cret");
        }
        let cfg = sample();

        let attrs = cfg.resolved_attributes();
        // env wins over the TOML value…
        assert_eq!(attrs.get("dc").map(String::as_str), Some("hn"));
        // …and env-only entries appear without any TOML declaration.
        assert_eq!(attrs.get("token").map(String::as_str), Some("s3cret"));
        assert_eq!(attrs.get("env_name").map(String::as_str), Some("prod"));

        unsafe {
            std::env::remove_var("OPSENSE_ATTR_DC");
            std::env::remove_var("OPSENSE_ATTR_TOKEN");
        }
    }
}
