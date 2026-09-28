//! TOML configuration for Opsense.
//!
//! Mirrors `opsense.conf.toml`:
//! ```toml
//! [engine]
//! poll_interval_seconds = 60
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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 60,
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
///
/// `deny_unknown_fields` là cố ý: `s3_flush_interval_secs` /
/// `s3_snapshot_interval_secs` thuộc `[storage]` **cấp ngoài**, không phải đây.
/// Không có deny thì serde bỏ qua key lạ **im lặng** — đã xảy ra thật ở
/// `strategies/s3` + `conf/opsense-test.conf.toml`: config khai 15s/60s trong
/// `[storage.s3]`, thực tế chạy 60s/600s, không ai báo gì.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: "memory".to_string(),
            data_dir: ".opsense/parquet".to_string(),
            retention_secs: 0,
            block_secs: 3600,
            s3: None,
            s3_flush_interval_secs: default_s3_flush_interval_secs(),
            s3_snapshot_interval_secs: default_s3_snapshot_interval_secs(),
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
    /// là lỗi cấu hình — xem [`GossipConfig::validate`].
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
    /// Có bật mesh không: chỉ cần `node_id` khác rỗng — các điều kiện còn lại
    /// (`own_url`, `token`, thời gian hợp lý) do [`GossipConfig::validate`] bắt
    /// để node bật mesh nửa vời còn tệ hơn node không bật mesh.
    #[must_use]
    pub fn enabled(&self) -> bool {
        !self.node_id.trim().is_empty()
    }

    /// Seed đã tách thành danh sách URL.
    #[must_use]
    pub fn seed_urls(&self) -> Vec<&str> {
        self.seeds.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()
    }

    /// Cấu hình sau khi áp env override: `OPSENSE_GOSSIP_<FIELD>` (tên field viết
    /// hoa) thắng giá trị trong TOML — cùng cơ chế với
    /// [`Config::resolved_attributes`], nên deploy không phải sửa file.
    ///
    /// Env rỗng bị bỏ qua: `OPSENSE_GOSSIP_NODE_ID=""` là *không khai*, không
    /// phải "xoá node_id trong file".
    #[must_use]
    pub fn resolved(&self) -> Self {
        let mut out = self.clone();
        const PREFIX: &str = "OPSENSE_GOSSIP_";
        for (env_key, value) in std::env::vars() {
            let Some(field) = env_key.strip_prefix(PREFIX) else { continue };
            if value.is_empty() {
                continue;
            }
            match field {
                "NODE_ID" => out.node_id = value,
                "OWN_URL" => out.own_url = value,
                "SEEDS" => out.seeds = value,
                "TOKEN" => out.token = value,
                "TICK_SECS" => out.tick_secs = value.parse().unwrap_or(out.tick_secs),
                "SUSPECT_SECS" => out.suspect_secs = value.parse().unwrap_or(out.suspect_secs),
                "DEAD_SECS" => out.dead_secs = value.parse().unwrap_or(out.dead_secs),
                "SETTLE_SECS" => out.settle_secs = value.parse().unwrap_or(out.settle_secs),
                "QUORUM" => out.quorum = value.parse().unwrap_or(out.quorum),
                _ => {}
            }
        }
        out
    }

    /// Kiểm tra bất biến mà parser TOML không ép được. Chỉ kiểm khi mesh **bật**;
    /// để trống `[gossip]` (mặc định) thì mọi config hiện tại giữ nguyên hành vi.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled() {
            return Ok(());
        }
        if self.node_id.chars().any(char::is_whitespace) {
            return Err(ConfigError::Invalid(
                "gossip.node_id must not contain whitespace — nó là khoá JSON và đoạn URL".into(),
            ));
        }
        if self.own_url.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "gossip.own_url must be set when gossip.node_id is set — không có URL thì peer gọi tới đâu?".into(),
            ));
        }
        if !self.own_url.starts_with("http://") && !self.own_url.starts_with("https://") {
            return Err(ConfigError::Invalid(format!(
                "gossip.own_url must be http(s) (got {:?})",
                self.own_url
            )));
        }
        if self.token.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "gossip.token must be set when mesh is on — endpoint nội bộ trao đổi state, không để trống".into(),
            ));
        }
        if self.tick_secs == 0 {
            return Err(ConfigError::Invalid("gossip.tick_secs must be > 0".into()));
        }
        if self.suspect_secs == 0 {
            return Err(ConfigError::Invalid("gossip.suspect_secs must be > 0".into()));
        }
        if self.dead_secs < self.suspect_secs {
            return Err(ConfigError::Invalid(format!(
                "gossip.dead_secs ({}) must be >= gossip.suspect_secs ({}) — không thể kết luận chết trước lúc nghi ngờ",
                self.dead_secs, self.suspect_secs
            )));
        }
        if self.suspect_secs < self.tick_secs {
            return Err(ConfigError::Invalid(format!(
                "gossip.suspect_secs ({}) must be >= gossip.tick_secs ({}) — không thể kết luận node im lặng nhanh hơn tần suất ta hỏi nó",
                self.suspect_secs, self.tick_secs
            )));
        }
        if self.quorum == 0 {
            return Err(ConfigError::Invalid(
                "gossip.quorum must be >= 1 — 0 sẽ bị `Gossip::new` nâng lên 1, tức im lặng ý nghĩa khác".into(),
            ));
        }
        Ok(())
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
        // Kiểm trên bản **đã áp env**: `OPSENSE_GOSSIP_*` có thể làm hỏng cấu
        // hình sau khi file đã hợp lệ, và lỗi đó phải lộ ra lúc khởi động chứ
        // không phải lúc node đã vào mesh.
        self.gossip.resolved().validate()?;
        Ok(())
    }

    /// `[gossip]` sau khi áp `OPSENSE_GOSSIP_<FIELD>` — thứ tầng vận chuyển dùng,
    /// không phải bản thô trong file.
    #[must_use]
    pub fn resolved_gossip(&self) -> GossipConfig {
        self.gossip.resolved()
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

    /// Regression: key đặt **sai cấp** phải nổi lên lúc parse, không bị bỏ âm thầm.
    ///
    /// `s3_flush_interval_secs` / `s3_snapshot_interval_secs` thuộc `[storage]`
    /// cấp ngoài. Đã có 2 file khai chúng trong `[storage.s3]` và **không ai
    /// báo gì** — flush chạy 60s thay vì 15s, snapshot 600s thay vì 60s. Test
    /// integration vẫn xanh vì chỉ chờ đủ lâu.
    #[test]
    fn rejects_s3_keys_placed_under_storage_s3() {
        let toml = r#"
[storage]
backend = "parquet"

[storage.s3]
bucket = "b"
s3_flush_interval_secs = 15
"#;
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(toml, FileFormat::Toml))
            .build()
            .unwrap();
        let err = raw.try_deserialize::<Config>().expect_err(
            "key sai cấp phải bị từ chối, không được bỏ qua im lặng",
        );
        assert!(
            err.to_string().contains("s3_flush_interval_secs"),
            "lỗi phải nêu đúng tên key, thực tế: {err}"
        );
    }

    /// Đặt đúng cấp thì parse được và **giữ đúng giá trị** — chứng minh deny
    /// không phá đường hợp đệ.
    #[test]
    fn keeps_s3_schedule_when_placed_under_storage() {
        let toml = r#"
[storage]
backend = "parquet"
s3_flush_interval_secs = 15
s3_snapshot_interval_secs = 60

[storage.s3]
bucket = "b"
prefix = "p"
"#;
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(toml, FileFormat::Toml))
            .build()
            .unwrap();
        let cfg: Config = raw
            .try_deserialize()
            .expect("đặt đúng cấp phải parse được");
        assert_eq!(cfg.storage.s3_flush_interval_secs, 15);
        assert_eq!(cfg.storage.s3_snapshot_interval_secs, 60);
        assert_eq!(cfg.storage.s3.as_ref().map(|s| s.bucket.as_str()), Some("b"));
    }

    #[test]
    fn attributes_resolve_with_env_override() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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

    // ── [gossip]: env override + validate ───────────────────────────────────

    /// Env là **process-wide** còn libtest chạy test song song, nên mọi test đụng
    /// env đều phải khoá lại — không thì đọc trúng giá trị của nhau và fail
    /// ngẫu nhiên (đã gặp: `dead < suspect` không báo lỗi vì bị test khác set
    /// `SUSPECT_SECS`).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Gom mọi env `OPSENSE_GOSSIP_*` vào scope test rồi xoá, kể cả khi assert
    /// panic — không thì test sau đọc trúng giá trị rò rỉ.
    struct GossipEnv(Vec<String>);

    impl GossipEnv {
        fn set(pairs: &[(&str, &str)]) -> Self {
            let keys = pairs
                .iter()
                .map(|(k, v)| {
                    unsafe { std::env::set_var(k, v) };
                    (*k).to_string()
                })
                .collect();
            Self(keys)
        }
    }

    impl Drop for GossipEnv {
        fn drop(&mut self) {
            for k in &self.0 {
                unsafe { std::env::remove_var(k) };
            }
        }
    }

    fn gossip_toml(extra: &str) -> Config {
        let toml = format!(
            r#"
[engine]
poll_interval_seconds = 60
{extra}
"#
        );
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(&toml, FileFormat::Toml))
            .build()
            .unwrap();
        raw.try_deserialize().expect("parse")
    }

    /// Mesh tắt mặc định thì `validate` không được soi gì — config hiện tại của
    /// mọi deploy giữ nguyên hành vi.
    #[test]
    fn mesh_off_by_default_is_always_valid() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = gossip_toml("");
        assert!(!cfg.gossip.enabled());
        assert!(cfg.validate().is_ok());
    }

    /// Bật mesh mà thiếu `own_url`/`token` thì phải **báo lỗi lúc khởi động**,
    /// chứ không phải lúc node đã vào mesh rồi mới gọi hỏng.
    #[test]
    fn enabling_mesh_without_own_url_or_token_is_refused() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
"#,
        );
        let err = cfg.validate().expect_err("thiếu token thì phải báo");
        assert!(err.to_string().contains("gossip.token"), "{err}");

        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
token = "s3cret"
"#,
        );
        let err = cfg.validate().expect_err("thiếu own_url thì phải báo");
        assert!(err.to_string().contains("gossip.own_url"), "{err}");
    }

    /// `own_url` không phải http(s) thì peer ghép `/internal/...` vào sẽ ra
    /// URL vô nghĩa — bắt sớm lúc đọc config.
    #[test]
    fn own_url_must_be_http() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "node-a:8080"
token = "s3cret"
"#,
        );
        let err = cfg.validate().expect_err("own_url sai scheme phải báo");
        assert!(err.to_string().contains("must be http(s)"), "{err}");
    }

    /// Chết (`dead`) sớm hơn nghi ngờ (`suspect`) là vô nghĩa: node bị kết luận
    /// chết ở lúc chưa từng được nghi ngờ.
    #[test]
    fn dead_window_cannot_be_shorter_than_suspect_window() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
token = "s3cret"
suspect_secs = 30
dead_secs = 10
"#,
        );
        let err = cfg.validate().expect_err("dead < suspect phải báo");
        assert!(err.to_string().contains("gossip.dead_secs"), "{err}");

        let ok = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
token = "s3cret"
suspect_secs = 30
dead_secs = 30
"#,
        );
        assert!(ok.validate().is_ok(), "bằng nhau thì hợp lệ");
    }

    /// Không thể phát hiện node im lặng nhanh hơn tần suất ta hỏi nó.
    ///
    /// Đo được khi chạy 2 node thật: `tick_secs=2, suspect_secs=3` thì tick đầu
    /// ra `suspect` ngay vì peer được seed với `last_seen = 0`. Ngược lại
    /// `tick_secs=10, suspect_secs=5` thì **mọi** peer bị nghi ngờ ở tick đầu
    /// tiên, kể cả peer đang trả lời tốt — view nhiễu ngay từ đầu.
    #[test]
    fn suspect_window_cannot_be_shorter_than_the_probe_period() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let bad = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
token = "s3cret"
tick_secs = 10
suspect_secs = 5
dead_secs = 30
"#,
        );
        let err = bad.validate().expect_err("tick chậm hơn cửa sổ nghi ngờ phải báo");
        assert!(err.to_string().contains("gossip.suspect_secs"), "{err}");

        let ok = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
token = "s3cret"
tick_secs = 5
suspect_secs = 5
dead_secs = 30
"#,
        );
        assert!(ok.validate().is_ok(), "bằng nhau thì hợp lệ");
    }

    /// `quorum = 0` bị `Gossip::new` nâng lên 1 — im lặng khác với ý định.
    #[test]
    fn zero_quorum_is_refused_instead_of_silently_becoming_one() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
token = "s3cret"
quorum = 0
"#,
        );
        let err = cfg.validate().expect_err("quorum 0 phải báo");
        assert!(err.to_string().contains("gossip.quorum"), "{err}");
    }

    /// `node_id` đi vào khoá JSON **và** đoạn URL của endpoint nội bộ, nên
    /// khoảng trắng trong nó là lỗi cấu hình chứ không phải chi tiết vụn vặt.
    #[test]
    fn node_id_with_whitespace_is_refused() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node a"
own_url = "http://node-a:8080"
token = "s3cret"
"#,
        );
        let err = cfg.validate().expect_err("node_id có khoảng trắng phải báo");
        assert!(err.to_string().contains("node_id"), "{err}");
    }

    /// Env thắng TOML trên **mọi** field, kể cả field vốn không có trong file.
    #[test]
    fn gossip_env_overrides_every_field() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = GossipEnv::set(&[
            ("OPSENSE_GOSSIP_NODE_ID", "node-env"),
            ("OPSENSE_GOSSIP_OWN_URL", "http://env:8080"),
            ("OPSENSE_GOSSIP_SEEDS", "http://a:8080, http://b:8080"),
            ("OPSENSE_GOSSIP_TOKEN", "tok-env"),
            ("OPSENSE_GOSSIP_TICK_SECS", "11"),
            ("OPSENSE_GOSSIP_SUSPECT_SECS", "12"),
            ("OPSENSE_GOSSIP_DEAD_SECS", "13"),
            ("OPSENSE_GOSSIP_SETTLE_SECS", "14"),
            ("OPSENSE_GOSSIP_QUORUM", "3"),
        ]);
        let cfg = gossip_toml("");
        let g = cfg.resolved_gossip();
        assert_eq!(g.node_id, "node-env");
        assert_eq!(g.own_url, "http://env:8080");
        assert_eq!(g.seed_urls(), vec!["http://a:8080", "http://b:8080"]);
        assert_eq!(g.token, "tok-env");
        assert_eq!((g.tick_secs, g.suspect_secs, g.dead_secs), (11, 12, 13));
        assert_eq!((g.settle_secs, g.quorum), (14, 3));
        assert!(cfg.validate().is_ok(), "env đủ thì config hợp lệ");
    }

    /// Env rỗng = *không khai*, không phải "xoá giá trị trong file"; env sai
    /// kiểu số thì giữ nguyên giá trị cũ thay vì làm 0.
    #[test]
    fn empty_or_unparsable_env_falls_back_to_the_file() {
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-file"
own_url = "http://file:8080"
token = "tok-file"
tick_secs = 7
quorum = 2
"#,
        );
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = GossipEnv::set(&[
            ("OPSENSE_GOSSIP_NODE_ID", ""),
            ("OPSENSE_GOSSIP_TICK_SECS", "abc"),
            ("OPSENSE_GOSSIP_QUORUM", ""),
        ]);
        let g = cfg.resolved_gossip();
        assert_eq!(g.node_id, "node-file", "env rỗng không được xoá giá trị file");
        assert_eq!(g.tick_secs, 7, "env sai kiểu phải giữ giá trị file");
        assert_eq!(g.quorum, 2);
    }

    /// Env làm hỏng cấu hình sau khi file đã hợp lệ thì `validate` phải bắt —
    /// không thì lỗi chỉ lộ ra lúc node đã vào mesh.
    #[test]
    fn validate_sees_env_because_env_can_break_a_valid_file() {
        let cfg = gossip_toml(
            r#"
[gossip]
node_id = "node-a"
own_url = "http://node-a:8080"
token = "s3cret"
"#,
        );
        assert!(cfg.validate().is_ok());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = GossipEnv::set(&[("OPSENSE_GOSSIP_OWN_URL", "not-a-url")]);
        let err = cfg.validate().expect_err("env phá own_url thì phải báo");
        assert!(err.to_string().contains("must be http(s)"), "{err}");
    }
}
