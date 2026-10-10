//! TOML configuration for Opsense.
//!
//! Mirrors `opsense.conf.toml`:
//! ```toml
//! [engine]
//! poll_interval_seconds = 60
//!
//! [attributes]
//! dc = "hcm"
//! ```

use std::borrow::Cow;
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
/// object store khi `data_dir` là `s3://…`), `"sqlite"` (local file) hoặc
/// `"redis"` (Redis/Valkey server, cấu hình qua `[storage.redis]`).
/// Tên backend cũ `"duckdb"`/`"s3"`/`"lakehouse"` vẫn được chấp nhận trong code
/// như alias deprecated (tất cả mở cùng Parquet storage) nhưng không nên dùng
/// trong config mới.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub backend: String,
    pub data_dir: String,

    /// Ghi đè `[storage]` cho **từng station**, khoá là station id — xem
    /// [`StorageConfig::for_station`].
    ///
    /// Cần vì một pipeline thật có nhiều loại station: `tick-candle` ghi mỗi
    /// tick (thuần bộ nhớ tạm, đẩy lên S3 chỉ là phí), còn `grid` giữ plan +
    /// lệnh open (mất là mất tiền). Một `[storage]` chung buộc phải chọn: bật
    /// persist cho tất cả thì cháy I/O, hoặc không bật gì thì mất lệnh.
    #[serde(default)]
    pub stations: HashMap<String, StationStorageOverride>,

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

    /// Kết nối Redis cho backend `"redis"` — xem [`RedisConfig`].
    ///
    /// Không phải địa chỉ tới S3: `backend = "redis"` không đụng `data_dir`
    /// hay `[storage.s3]` cho tới phần này.
    #[serde(default)]
    pub redis: Option<RedisConfig>,
}

fn default_s3_flush_interval_secs() -> u64 {
    60
}

fn default_s3_snapshot_interval_secs() -> u64 {
    600
}

/// Ghi đè storage cho **một** station — khai dưới `[storage.stations.<id>]`,
/// xem [`StorageConfig::stations`].
///
/// Mọi field là `Option`: `None` = kế thừa từ `[storage]` cấp ngoài, nên khai
/// một station không phải lặp lại 7 field của cấp ngoài.
///
/// `deny_unknown_fields` là cố ý, cùng lý do với [`S3Config`]: key sai chính
/// tả (`retention_sec`, `block_second`) **phải nổi lúc parse**. Không có deny
/// thì serde bỏ im lặng và station đó lặng lẽ chạy bằng cấu hình cấp ngoài —
/// đúng kiểu lỗi đã xảy ra thật với `s3_flush_interval_secs`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StationStorageOverride {
    pub backend: Option<String>,
    pub data_dir: Option<String>,
    pub retention_secs: Option<u64>,
    pub block_secs: Option<u64>,

    /// S3 riêng cho station này. Thường **không** cần: prefix trên S3 đã có
    /// sẵn station id (`{bucket}/{prefix}/{station}/...`) nên các station không
    /// đè nhau. Chỉ khai khi muốn một station lên bucket khác hẳn.
    #[serde(default)]
    pub s3: Option<S3Config>,

    /// Redis riêng cho station này. Cùng lý do với `s3`: prefix trên Redis đã
    /// mang sẵn station id nên các station không đè nhau — chỉ khai khi muốn
    /// một station sang server/prefix khác hẳn.
    #[serde(default)]
    pub redis: Option<RedisConfig>,
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

/// Kết nối Redis/Valkey cho backend `"redis"` — nơi đặt state của station.
///
/// Station không ghi ra file: toàn bộ block, order và cursor nằm trên server
/// Redis, nên **restart process không mất lệnh đang mở** (khác `backend =
/// "memory"`). Đổi lại là mỗi lần đọc/evict block là một vòng round-trip mạng.
///
/// `deny_unknown_fields` là cố ý, cùng lý do với [`S3Config`]: key sai chính
/// tả (`host` thay vì `url`) phải nổi lúc parse chứ không lặng lẽ rơi về mặc
/// định rồi đổi mất state cũ.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    /// DSN đầy đủ, VD `"redis://opsense-valkey:6379"` hoặc
    /// `"redis://:secret@host:6379/0"`. Bù bằng env `OPSENSE_REDIS_URL` rồi
    /// `REDIS_URL`. Rỗng cả hai ⇒ validate báo lỗi — không có host mặc định,
    /// vì đoán sai host là mất state chứ không phải chỉ chậm.
    pub url: String,

    /// Prefix key gốc, mặc định `"opsense"`. Station `grid` sẽ nằm dưới
    /// `opsense:grid-timeseries`, `…-pattern`, `…-category` (xem
    /// `open_backend` trong `src/station.rs`).
    pub prefix: String,
}

impl RedisConfig {
    /// DSN đã bù env, theo thứ tự: `[storage.redis].url` → `OPSENSE_REDIS_URL`
    /// → `REDIS_URL`.
    ///
    /// Không có fallback "localhost" — xem [`RedisConfig::url`].
    #[must_use]
    pub fn resolved_url(&self) -> String {
        if !self.url.trim().is_empty() {
            return self.url.trim().to_string();
        }
        std::env::var("OPSENSE_REDIS_URL")
            .or_else(|_| std::env::var("REDIS_URL"))
            .unwrap_or_default()
            .trim()
            .to_string()
    }

    /// Prefix key gốc sau khi bù mặc định.
    #[must_use]
    pub fn resolved_prefix(&self) -> String {
        let prefix = self.prefix.trim();
        if prefix.is_empty() {
            "opsense".to_string()
        } else {
            prefix.to_string()
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: "memory".to_string(),
            data_dir: ".opsense/parquet".to_string(),
            stations: HashMap::new(),
            retention_secs: 0,
            block_secs: 3600,
            s3: None,
            s3_flush_interval_secs: default_s3_flush_interval_secs(),
            s3_snapshot_interval_secs: default_s3_snapshot_interval_secs(),
            redis: None,
        }
    }
}

impl StorageConfig {
    /// Storage áp dụng cho station `id`: cấp ngoài `[storage]` bị phủ bởi
    /// `[storage.stations.<id>]` nếu có.
    ///
    /// Trả `Cow` để **không có override thì không copy gì** — phần lớn station
    /// không ghi đè, mà `from_storage` gọi hàm này mỗi lần dựng station. Chỉ
    /// khi thật sự có override mới clone, tức một lần lúc khởi động chứ không
    /// phải mỗi nến.
    ///
    /// Map `stations` bị clear trong bản clone vì đã hết ý nghĩa sau khi hợp
    /// nhất: hàm này không gọi đệ quy, giữ lại map chỉ tốn bộ nhớ.
    pub fn for_station(&self, id: &str) -> Cow<'_, StorageConfig> {
        let Some(over) = self.stations.get(id) else {
            return Cow::Borrowed(self);
        };
        let mut merged = self.clone();
        if let Some(v) = &over.backend {
            merged.backend = v.clone();
        }
        if let Some(v) = &over.data_dir {
            merged.data_dir = v.clone();
        }
        if let Some(v) = over.retention_secs {
            merged.retention_secs = v;
        }
        if let Some(v) = over.block_secs {
            merged.block_secs = v;
        }
        if over.s3.is_some() {
            merged.s3 = over.s3.clone();
        }
        if over.redis.is_some() {
            merged.redis = over.redis.clone();
        }
        merged.stations.clear();
        Cow::Owned(merged)
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
    /// Env **rỗng** bị bỏ qua: `OPSENSE_GOSSIP_NODE_ID=""` là *không khai*, không
    /// phải "xoá node_id trong file".
    ///
    /// Env **sai kiểu** thì báo lỗi, không lặng lẽ giữ giá trị cũ: một dấu phẩy
    /// thừa trong `OPSENSE_GOSSIP_TICK_SECS=5s` sẽ khiến vận hành tin là mình
    /// đang đặt 5 giây trong khi thật ra vẫn là mặc định — đó là cấu hình *nói
    /// dối*, nguy hiểm hơn hẳn việc không khởi động được.
    pub fn resolved(&self) -> Result<Self, ConfigError> {
        let mut out = self.clone();
        const PREFIX: &str = "OPSENSE_GOSSIP_";
        for (env_key, value) in std::env::vars() {
            let Some(field) = env_key.strip_prefix(PREFIX) else { continue };
            if value.is_empty() {
                continue;
            }
            // Ghi rõ tên biến trong lỗi: "abc is not a positive integer" mà không
            // nói biến nào thì chẩn đoán kiểu mò mẫm.
            let num = || -> Result<u64, ConfigError> {
                value.parse::<u64>().map_err(|_| {
                    ConfigError::Invalid(format!("{env_key}={value:?} is not a positive integer"))
                })
            };
            match field {
                "NODE_ID" => out.node_id = value,
                "OWN_URL" => out.own_url = value,
                "SEEDS" => out.seeds = value,
                "TOKEN" => out.token = value,
                "TICK_SECS" => out.tick_secs = num()?,
                "SUSPECT_SECS" => out.suspect_secs = num()?,
                "DEAD_SECS" => out.dead_secs = num()?,
                "SETTLE_SECS" => out.settle_secs = num()?,
                "QUORUM" => out.quorum = u32::try_from(num()?).unwrap_or(u32::MAX),
                // Biến `OPSENSE_GOSSIP_*` lạ không phải lỗi: có thể thuộc phiên bản
                // sau. Bỏ qua, nhưng im lặng thì khó chẩn đoán.
                _ => continue,
            }
        }
        Ok(out)
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
        if self.quorum == 0 {
            return Err(ConfigError::Invalid(
                "gossip.quorum must be >= 1 — 0 sẽ bị `Gossip::new` nâng lên 1, tức im lặng ý nghĩa khác".into(),
            ));
        }
        Ok(())
    }
}

/// Cấu hình tầng raft — xem [`crate::config::Config::raft`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RaftConfig {
    /// Pipeline mà node đứng một mình sẽ chạy (khi chưa ai gom nó vào cụm nào).
    pub pipeline: String,
}


/// Cấu hình Prometheus /metrics endpoint.
///
/// Mặc định tắt (enabled = false) để không đổi hành vi hiện có.
/// Khi bật, expose /metrics trên HTTP server (route /metrics).
///
/// Cardinality: station data có thể rất lớn (tick-candle: tick/giây;
/// grid: order với nhiều label). Cơ chế kiểm soát:
/// - default_window_secs: chỉ expose observation mới hơn cái này (mặc định 120s)
/// - max_series: trần cứng series (mặc định 5000). Vượt trần ⇒ cắt deterministic
///   + expose opsense_prometheus_dropped_series để thấy bị cắt.
/// - Signal allowlist mặc định loại order (quyết định giao dịch) và trading_step (cursor).
///
/// Mặc định không khai báo [[prometheus.stations]] ⇒ tất cả station,
/// áp dụng gate mặc định. Khai báo để thu hẹp.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    /// Bật /metrics endpoint. Mặc định false để không đổi hành vi cũ.
    pub enabled: bool,
    /// Path của endpoint. Mặc định /metrics. Phải bắt đầu bằng /.
    pub path: String,
    /// Chỉ expose observation có ts >= now - default_window_secs.
    /// Mặc định 120s (2 phút).
    pub default_window_secs: u64,
    /// Trần số series. 0 = không giới hạn (không khuyến khích).
    /// Mặc định 5000.
    pub max_series: u64,
    /// Danh sách station muốn expose. Bỏ trống = tất cả station.
    pub stations: Vec<StationTarget>,
}

/// Một station trong allowlist Prometheus.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StationTarget {
    /// Station id (vd grid, tick-candle, history).
    pub station: String,
    /// Bật/tắt station này. Mặc định true.
    pub enabled: bool,
    /// Allowlist signal. Mặc định: [raw, summary] (loại order).
    /// Hợp lệ: raw, summary, utilization, saturation, rate,
    /// errors, duration, order.
    pub signals: Vec<String>,
    /// Cửa sổ thời gian (giây) cho station này — override global
    /// `default_window_secs`. `0` = dùng global.
    pub window_secs: Option<u64>,
    /// Danh sách `labels.kind` muốn loại khỏi export.
    /// Dùng để giảm cardinality (vd `snapshot`, `trend_probe` có nhiều label số).
    pub exclude_kinds: Vec<String>,
}

impl Default for StationTarget {
    fn default() -> Self {
        Self {
            station: String::new(),
            enabled: true,
            signals: vec!["raw".into(), "summary".into()],
            window_secs: None,
            exclude_kinds: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub engine: EngineConfig,

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

    /// Prometheus /metrics endpoint configuration.
    #[serde(default)]
    pub prometheus: PrometheusConfig,
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
        // Override per-station: cùng hai bất biến, nhưng phải báo **kèm tên
        // station** — không thì thông báo chung trỏ nhầm về `[storage]` và
        // người đọc đi sửa cấp ngoài rồi lỗi không hết.
        for (id, over) in &self.storage.stations {
            if over.backend.as_deref().is_some_and(|b| b.trim().is_empty()) {
                return Err(ConfigError::Invalid(format!(
                    "storage.stations.{id}.backend must not be empty"
                )));
            }
            if over.block_secs == Some(0) {
                return Err(ConfigError::Invalid(format!(
                    "storage.stations.{id}.block_secs must be > 0 (parquet block partition width)"
                )));
            }
            // Station ép backend = "redis" mà không có DSN nào để mở ⇒ lỗi phải
            // nói **kèm tên station**, và phải nhìn cả DSN ở cấp ngoài: override
            // chỉ đổi `backend`, phần `[storage.redis]` vẫn kế thừa từ trên.
            // So khớp **chính xác** như `open_backend`, không phải
            // case-insensitive: nếu validate nhận "Redis" mà `open_backend`
            // chỉ match "redis" thì config qua validate rồi chết lúc mở
            // station — lỗi ở chỗ khó truy hơn nhiều.
            let picks_redis = over.backend.as_deref().is_some_and(|b| b.trim() == "redis");
            if picks_redis {
                let r = over.redis.as_ref().or(self.storage.redis.as_ref());
                let has_url = r.is_some_and(|r| !r.resolved_url().is_empty());
                if !has_url {
                    return Err(ConfigError::Invalid(format!(
                        "storage.stations.{id}.backend = \"redis\" cần DSN: khai \
                         [storage.redis].url, hoặc env OPSENSE_REDIS_URL / REDIS_URL"
                    )));
                }
            }
        }
        // Backend cấp ngoài cũng cần DSN. Đặt **sau** vòng lặp station để khi
        // cả hai đều sai thì thông báo chi tiết theo tên station hiện trước.
        if self.storage.backend.trim() == "redis" {
            let has_url = self
                .storage
                .redis
                .as_ref()
                .is_some_and(|r| !r.resolved_url().is_empty());
            if !has_url {
                return Err(ConfigError::Invalid(
                    "storage.backend = \"redis\" cần DSN: khai [storage.redis].url, \
                     hoặc env OPSENSE_REDIS_URL / REDIS_URL"
                        .into(),
                ));
            }
        }
        // Kiểm tra Prometheus config.
        if self.prometheus.enabled {
            if self.prometheus.path.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "prometheus.path must not be empty".into(),
                ));
            }
            if !self.prometheus.path.starts_with('/') {
                return Err(ConfigError::Invalid(
                    "prometheus.path must start with '/'".into(),
                ));
            }
            if self.prometheus.max_series == 0 {
                return Err(ConfigError::Invalid(
                    "prometheus.max_series must be > 0 (use a positive integer, 0 disables limit)".into(),
                ));
            }
            if self.prometheus.default_window_secs == 0 {
                return Err(ConfigError::Invalid(
                    "prometheus.default_window_secs must be > 0".into(),
                ));
            }
            // Validate signals in station targets
            let valid_signals: std::collections::HashSet<&str> = [
                "raw", "summary", "utilization", "saturation", "rate",
                "errors", "duration", "order",
            ].into_iter().collect();
            for st in &self.prometheus.stations {
                if st.station.trim().is_empty() {
                    return Err(ConfigError::Invalid(
                        "prometheus.stations.station must not be empty".into(),
                    ));
                }
                for sig in &st.signals {
                    if !valid_signals.contains(sig.as_str()) {
                        return Err(ConfigError::Invalid(format!(
                            "prometheus.stations.signals contains unknown signal '{}'; valid: raw,summary,utilization,saturation,rate,errors,duration,order",
                            sig
                        )));
                    }
                }
            }
        }

        // Kiểm trên bản **đã áp env**: `OPSENSE_GOSSIP_*` có thể làm hỏng cấu
        // hình sau khi file đã hợp lệ, và lỗi đó phải lộ ra lúc khởi động chứ
        // không phải lúc node đã vào mesh.
        self.gossip.resolved()?.validate()?;
        Ok(())
    }

    /// `[gossip]` sau khi áp `OPSENSE_GOSSIP_<FIELD>` — thứ tầng vận chuyển dùng,
    /// không phải bản thô trong file. Lỗi env sai kiểu nổi ra ở đây.
    pub fn resolved_gossip(&self) -> Result<GossipConfig, ConfigError> {
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
    fn parses_attributes() {
        let cfg = sample();
        assert_eq!(cfg.attributes.get("dc").map(String::as_str), Some("hcm"));
        assert_eq!(cfg.attributes.get("env_name").map(String::as_str), Some("prod"));
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
        let g = cfg.resolved_gossip().expect("env hợp lệ");
        assert_eq!(g.node_id, "node-env");
        assert_eq!(g.own_url, "http://env:8080");
        assert_eq!(g.seed_urls(), vec!["http://a:8080", "http://b:8080"]);
        assert_eq!(g.token, "tok-env");
        assert_eq!((g.tick_secs, g.suspect_secs, g.dead_secs), (11, 12, 13));
        assert_eq!((g.settle_secs, g.quorum), (14, 3));
        assert!(cfg.validate().is_ok(), "env đủ thì config hợp lệ");
    }

    /// Env rỗng = *không khai*, không phải "xoá giá trị trong file".
    #[test]
    fn empty_env_means_unset_not_erase() {
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
            ("OPSENSE_GOSSIP_QUORUM", ""),
        ]);
        let g = cfg.resolved_gossip().expect("env rỗng không phải lỗi");
        assert_eq!(g.node_id, "node-file", "env rỗng không được xoá giá trị file");
        assert_eq!(g.quorum, 2);
    }

    /// Env sai kiểu phải **báo lỗi**, không lặng lẽ giữ giá trị cũ.
    ///
    /// Lý do: `TICK_SECS=5s` khiến vận hành tin mình đang đặt 5 giây trong khi
    /// thật ra vẫn là mặc định — cấu hình nói dối, nguy hiểm hơn hẳn việc không
    /// khởi động được, và lỗi sẽ chỉ lộ ra rất lâu sau đó.
    #[test]
    fn unparsable_env_is_rejected_not_silently_ignored() {
        for bad in ["abc", "5s", "-1", "1.5", "99999999999999999999999"] {
            let cfg = gossip_toml(
                r#"
[gossip]
node_id = "node-file"
own_url = "http://file:8080"
token = "tok-file"
tick_secs = 7
"#,
            );
            let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let _env = GossipEnv::set(&[("OPSENSE_GOSSIP_TICK_SECS", bad)]);
            let err = cfg
                .resolved_gossip()
                .expect_err(&format!("TICK_SECS={bad:?} phải bị từ chối"));
            assert!(
                err.to_string().contains("OPSENSE_GOSSIP_TICK_SECS"),
                "thông báo phải chỉ đúng biến sai: {err}"
            );
        }
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

    /// Override per-station chỉ ghi đè field nó khai; field còn lại kế thừa
    /// nguyên vẹn từ cấp ngoài. Nhờ vậy khai `[storage.stations.grid]` chỉ với
    /// `backend` mà không phải lặp lại 7 field của `[storage]`.
    #[test]
    fn station_override_merges_over_global_and_keeps_the_rest() {
        let cfg = gossip_toml(
            r#"
[storage]
backend = "memory"
retention_secs = 30
block_secs = 900

[storage.stations.grid]
backend = "parquet"
retention_secs = 0
"#,
        );
        let g = cfg.storage.for_station("grid");
        assert_eq!(g.backend, "parquet");
        assert_eq!(g.retention_secs, 0, "0 là giá trị thật, không phải thiếu");
        // Không khai ở override => kế thừa nguyên vẹn.
        assert_eq!(g.block_secs, 900);
        assert_eq!(g.data_dir, cfg.storage.data_dir);
    }

    /// Station không có override phải đi qua nguyên vẹn — đây là đường đi của
    /// đa số station trong mọi pipeline, và nó phải **rẻ**: `for_station` trả
    /// `Cow::Borrowed` nên không copy `StorageConfig` lúc khởi động.
    #[test]
    fn station_without_override_is_unchanged() {
        let cfg = gossip_toml(
            r#"
[storage]
backend = "parquet"
retention_secs = 7

[storage.stations.grid]
backend = "memory"
"#,
        );
        let other = cfg.storage.for_station("tick-candle");
        assert_eq!(other.backend, "parquet");
        assert_eq!(other.retention_secs, 7);
        assert!(
            matches!(other, Cow::Borrowed(_)),
            "không override thì không được copy"
        );
        assert!(matches!(
            cfg.storage.for_station("grid"),
            Cow::Owned(_)
        ));
    }

    /// Key sai chính tả trong override phải nổi lúc parse. Nếu serde bỏ qua
    /// im lặng thì station chạy bằng cấu hình cấp ngoài — người viết config
    /// tin là đã bật persist thực ra không có, đúng kiểu lỗi đã xảy ra với
    /// `s3_flush_interval_secs`.
    #[test]
    fn rejects_typo_inside_station_override() {
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(
                r#"
[engine]
poll_interval_seconds = 60

[storage]
backend = "memory"

[storage.stations.grid]
retention_sec = 0
"#,
                FileFormat::Toml,
            ))
            .build()
            .unwrap();
        let err = raw.try_deserialize::<Config>().expect_err(
            "key sai trong override phải bị từ chối, không được bỏ qua im lặng",
        );
        assert!(
            err.to_string().contains("retention_sec"),
            "lỗi phải nêu đúng tên key, thực tế: {err}"
        );
    }

    /// Lỗi override phải **nêu tên station** — thông báo chung trỏ về
    /// `[storage]` khiến người đọc đi sửa cấp ngoài rồi lỗi không hết.
    #[test]
    fn validate_names_the_station_that_broke_invariant() {
        let cfg = gossip_toml(
            r#"
[storage]
backend = "memory"

[storage.stations.grid]
block_secs = 0
"#,
        );
        let err = cfg.validate().expect_err("block_secs = 0 phải bị chặn");
        assert!(
            err.to_string().contains("storage.stations.grid.block_secs"),
            "{err}"
        );

        let empty = gossip_toml(
            r#"
[storage]
backend = "memory"

[storage.stations.tick-candle]
backend = ""
"#,
        );
        assert!(
            empty
                .validate()
                .expect_err("backend rỗng phải bị chặn")
                .to_string()
                .contains("storage.stations.tick-candle.backend"),
        );
    }

    /// Override phải đổi **backend thật**, không chỉ đổi con số trên config:
    /// `plain` (không override) không có storage đọc-through, còn `kept` thì có.
    /// Đây là test bảo vệ toàn bộ ý nghĩa của tính năng.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn station_override_changes_which_backend_opens() {
        let dir = std::env::temp_dir().join(format!("opsense-ovr-{}", std::process::id()));
        let mut cfg = StorageConfig::default();
        cfg.stations.insert(
            "kept".to_string(),
            StationStorageOverride {
                backend: Some("sqlite".to_string()),
                data_dir: Some(dir.to_string_lossy().into_owned()),
                ..StationStorageOverride::default()
            },
        );

        let plain = crate::station::TimeseriesStation::from_storage("plain", &cfg)
            .await
            .unwrap();
        assert!(
            plain.storage().is_none(),
            "station không override phải giữ backend memory"
        );

        let kept = crate::station::TimeseriesStation::from_storage("kept", &cfg)
            .await
            .unwrap();
        assert!(
            kept.storage().is_some(),
            "override backend phải thật sự mở storage khác"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ==================== Backend "redis" ====================

    fn parse(toml: &str) -> Result<Config, config_crate::ConfigError> {
        let raw = config_crate::Config::builder()
            .add_source(config_crate::File::from_str(toml, FileFormat::Toml))
            .build()
            .unwrap();
        raw.try_deserialize()
    }

    /// `backend = "redis"` + `[storage.redis].url` phải parse và validate qua —
    /// đây là hình dạng `strategies/binance/config.toml` dùng.
    #[test]
    fn redis_backend_parses_with_url() {
        let cfg = parse(
            r#"
[storage]
backend = "redis"

[storage.redis]
url = "redis://opsense-valkey:6379"
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let r = cfg.storage.redis.as_ref().expect("phải có [storage.redis]");
        assert_eq!(r.resolved_url(), "redis://opsense-valkey:6379");
        assert_eq!(r.resolved_prefix(), "opsense", "prefix rỗng = mặc định");
    }

    /// Không có DSN ở đâu thì phải **nổi lúc validate**, không phải lúc mở
    /// station — lúc đó pipeline đã chạy và log chỉ có `station '<id>'`.
    #[test]
    fn redis_backend_without_url_is_rejected() {
        let cfg = parse(
            r#"
[storage]
backend = "redis"
"#,
        )
        .unwrap();
        let err = cfg.validate().expect_err("redis mà không DSN thì phải bị chặn");
        assert!(err.to_string().contains("OPSENSE_REDIS_URL"), "lỗi: {err}");
    }

    /// Đặt `OPSENSE_REDIS_URL` trong scope test rồi xoá, kể cả khi assert
    /// panic — cùng lý do và cùng cách làm với [`GossipEnv`].
    struct RedisUrlEnv;

    impl RedisUrlEnv {
        fn set(v: &str) -> Self {
            unsafe { std::env::set_var("OPSENSE_REDIS_URL", v) };
            Self
        }
    }

    impl Drop for RedisUrlEnv {
        fn drop(&mut self) {
            unsafe { std::env::remove_var("OPSENSE_REDIS_URL") };
        }
    }

    /// Env là nguồn DSN hợp lệ — `docker-compose.yml` đặt `REDIS_HOST`/`REDIS_PORT`
    /// nên container không cần hardcode DSN trong file config.
    #[test]
    fn redis_url_falls_back_to_env() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let r = RedisConfig {
            url: String::new(),
            prefix: String::new(),
        };
        // Không có env ⇒ vẫn rỗng: không đoán "localhost", vì đoán sai host là
        // mất state chứ không phải chỉ chậm.
        assert!(r.resolved_url().is_empty(), "không có env thì không đoán host");

        let _env = RedisUrlEnv::set("redis://from-env:6379");
        assert_eq!(r.resolved_url(), "redis://from-env:6379");
    }

    /// `url` khai trong config phải **thắng** env — không phải bị env đè.
    #[test]
    fn redis_config_url_beats_env() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env = RedisUrlEnv::set("redis://from-env:6379");
        let r = RedisConfig {
            url: "redis://from-config:6379".to_string(),
            prefix: "custom".to_string(),
        };
        assert_eq!(r.resolved_url(), "redis://from-config:6379");
        assert_eq!(r.resolved_prefix(), "custom");
    }

    /// Station ép `backend = "redis"` mà không khai DSN phải báo **kèm tên
    /// station** — thông báo chung trỏ nhầm về `[storage]` rồi người đọc sửa
    /// cấp ngoài mà lỗi không hết.
    #[test]
    fn redis_station_override_without_url_names_the_station() {
        let cfg = parse(
            r#"
[storage]
backend = "memory"

[storage.stations.grid]
backend = "redis"
"#,
        )
        .unwrap();
        let err = cfg.validate().expect_err("station redis thiếu DSN phải bị chặn");
        assert!(
            err.to_string().contains("storage.stations.grid.backend"),
            "lỗi: {err}"
        );
    }

    /// Override chỉ đổi `backend` — phần `[storage.redis]` **kế thừa từ trên**,
    /// nên cấu hình này phải hợp lệ.
    #[test]
    fn redis_station_override_inherits_url_from_outer_level() {
        let cfg = parse(
            r#"
[storage]
backend = "memory"

[storage.redis]
url = "redis://outer:6379"

[storage.stations.grid]
backend = "redis"
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let merged = cfg.storage.for_station("grid");
        assert_eq!(merged.backend, "redis");
        assert_eq!(
            merged.redis.as_ref().map(|r| r.resolved_url()),
            Some("redis://outer:6379".to_string()),
            "DSN phải kế thừa từ [storage.redis] cấp ngoài"
        );
    }

    /// `[storage.redis]` phải `deny_unknown_fields` như `[storage.s3]`: khai
    /// `host` thay vì `url` phải nổi lúc parse, không lặng lẽ rơi về `url` rỗng
    /// rồi đổi mất state cũ.
    #[test]
    fn rejects_unknown_key_under_storage_redis() {
        let err = parse(
            r#"
[storage]
backend = "redis"

[storage.redis]
host = "opsense-valkey"
port = 6379
"#,
        )
        .expect_err("key lạ dưới [storage.redis] phải bị từ chối");
        // Không khẳng định tên field cụ thể: ở đây có **hai** key lạ (`host` và
        // `port`) mà serde chỉ báo một cái, và cái nào là chi tiết của toml crate
        // chứ không phải hợp đồng ta muốn giữ. Cái muốn giữ là: lỗi nói
        // "unknown field" và **chỉ đúng bảng** `[storage.redis]`.
        let msg = err.to_string();
        assert!(msg.contains("unknown field"), "lỗi: {msg}");
        assert!(msg.contains("storage.redis"), "lỗi: {msg}");
    }
}
