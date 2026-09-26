use std::io::{Error, ErrorKind};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_graphql::Enum;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use opsense_mlib::ahocorasick::AhoCorasick;
use opsense_mlib::lru::LruCache;
use opsense_mlib::search::Search;
use opsense_mlib::snowflake_id::SnowflakeId;
#[cfg(feature = "parquet")]
use opsense_mlib::storage::LakehouseStorage;
#[cfg(feature = "sqlite")]
use opsense_mlib::storage::SqliteStorage;
use opsense_mlib::storage::{CategoryStorage, PatternStorage, TimeseriesStorage};

/// Trần số block một lần [`TimeseriesStation::query_recent`] duyệt.
///
/// `Query.queryTimeseries` mặc định lấy cửa sổ 30 ngày
/// (`MAX_QUERY_WINDOW_SECS`); với `block_secs` nhỏ (5s) đó là ~518.400 block, mỗi
/// block một lần tra cache + một lần đọc storage. 20.000 block ≈ 27 giờ ở block
/// 5s, ~69 ngày ở block 300s — đủ cho mọi reader thực tế mà không để một query
/// quét nửa triệu block.
const MAX_BLOCKS_PER_QUERY: i64 = 20_000;
use opsense_model::events::Observation;

use crate::config::StorageConfig;

// ==================== Storage backend ====================
//
// Dựng storage backend từ `[storage]` config, dùng chung cho cả 3 station
// (timeseries / pattern / category). Backend `"memory"` (mặc định) trả về
// station thuần memory; `"parquet"` (canonical) mở Parquet storage;
// `"sqlite"` mở một file riêng cho từng station. Backend nhận diện nhưng
// chưa được biên dịch (feature tắt) báo lỗi rõ ràng thay vì lặng lẽ hạ cấp.
// Tên cũ `"duckdb"`/`"s3"`/`"lakehouse"` vẫn được chấp nhận như alias
// deprecated — tất cả đều mở cùng một Parquet storage (xem `open_backend`).

/// Số block tối đa giữ trong LRU hot của một [`TimeseriesStation`].
const HOT_BLOCKS: usize = 32;

/// Sanitize station `id` thành path segment an toàn (thay ký tự đường dẫn).
#[cfg(any(feature = "parquet", feature = "sqlite"))]
fn safe_segment(id: &str) -> String {
    id.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

/// Backend đã mở. Mọi backend đều implement đủ 3 trait
/// (`TimeseriesStorage`, `PatternStorage`, `CategoryStorage`) nên dùng chung
/// một enum và ép sang trait object theo nhu cầu từng station.
enum BackendStorage {
    /// Thuần memory — không persistence.
    Memory,
    #[cfg(feature = "parquet")]
    Parquet(LakehouseStorage),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteStorage),
}

impl BackendStorage {
    fn into_timeseries(self) -> Option<Arc<dyn TimeseriesStorage>> {
        match self {
            BackendStorage::Memory => None,
            #[cfg(feature = "parquet")]
            BackendStorage::Parquet(s) => Some(Arc::new(s)),
            #[cfg(feature = "sqlite")]
            BackendStorage::Sqlite(s) => Some(Arc::new(s)),
        }
    }

    fn into_pattern(self) -> Option<Arc<dyn PatternStorage>> {
        match self {
            BackendStorage::Memory => None,
            #[cfg(feature = "parquet")]
            BackendStorage::Parquet(s) => Some(Arc::new(s)),
            #[cfg(feature = "sqlite")]
            BackendStorage::Sqlite(s) => Some(Arc::new(s)),
        }
    }

    fn into_category(self) -> Option<Arc<RwLock<dyn CategoryStorage>>> {
        match self {
            BackendStorage::Memory => None,
            #[cfg(feature = "parquet")]
            BackendStorage::Parquet(s) => Some(Arc::new(RwLock::new(s))),
            #[cfg(feature = "sqlite")]
            BackendStorage::Sqlite(s) => Some(Arc::new(RwLock::new(s))),
        }
    }
}

/// Lỗi khi mở backend thất bại (chỉ xuất hiện khi có backend persistent).
#[cfg(any(feature = "parquet", feature = "sqlite"))]
fn backend_error(id: &str, backend: &str, e: impl std::fmt::Display) -> Error {
    Error::other(format!(
        "failed to open storage backend '{backend}' for station '{id}': {e}"
    ))
}

/// Lỗi khi backend được yêu cầu nhưng feature chưa được biên dịch.
#[cfg(not(all(feature = "parquet", feature = "sqlite")))]
fn unsupported(id: &str, backend: &str, feature: &str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        format!("storage backend '{backend}' for station '{id}' requires {feature}"),
    )
}

/// Mở backend theo `cfg.backend`. `kind` = `"timeseries"` | `"pattern"` |
/// `"category"` — tách data layout (path) giữa các loại station.
async fn open_backend(id: &str, cfg: &StorageConfig, kind: &str) -> Result<BackendStorage, Error> {
    let backend = cfg.backend.trim();
    #[cfg(any(feature = "parquet", feature = "sqlite"))]
    let data_dir = cfg.data_dir.trim_end_matches('/');
    #[cfg(any(feature = "parquet", feature = "sqlite"))]
    let segment = safe_segment(id);

    match backend {
        "memory" => Ok(BackendStorage::Memory),

        // Parquet storage — canonical (`"parquet"`); "duckdb"/"s3"/"lakehouse"
        // là alias cũ (deprecated) nhưng vẫn mở cùng một trình điều khiển.
        #[cfg(feature = "parquet")]
        "parquet" | "duckdb" | "s3" | "lakehouse" => {
            let local = format!("{data_dir}/{segment}-{kind}");
            let storage = match &cfg.s3 {
                // Cấu hình `[storage].s3` → lakehouse mirror: dữ liệu parquet
                // xuất ra `s3://{bucket}/{prefix}/{id}/ts/blk=…` + state → chính
                // là nơi Spark/DuckDB/Polars query trực tiếp.
                Some(s3) => {
                    // Creds/region có thể bỏ qua trong `[storage.s3]` (giữ file
                    // config sạch): fallback theo env `OPSENSE_S3_*` rồi `AWS_*`
                    // chuẩn. Đây là cách host (docker-compose / CI) cấp
                    // credential cho MinIO mà không cần hardcode trong config.
                    let access_key_id = s3
                        .access_key_id
                        .clone()
                        .or_else(|| std::env::var("OPSENSE_S3_ACCESS_KEY_ID").ok())
                        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok());
                    let secret_access_key = s3
                        .secret_access_key
                        .clone()
                        .or_else(|| std::env::var("OPSENSE_S3_SECRET_ACCESS_KEY").ok())
                        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok());
                    let region = s3
                        .region
                        .clone()
                        .or_else(|| std::env::var("OPSENSE_S3_REGION").ok())
                        .or_else(|| std::env::var("AWS_REGION").ok());
                    let mlib_s3 = opsense_mlib::storage::parquet::S3Config {
                        bucket: s3.bucket.clone(),
                        prefix: s3.prefix.clone(),
                        endpoint: s3.endpoint.clone(),
                        region,
                        access_key_id,
                        secret_access_key,
                        session_token: s3.session_token.clone(),
                    };
                    LakehouseStorage::open_with_s3(
                        &local,
                        mlib_s3,
                        4096,
                        cfg.block_secs as i64,
                        segment.clone(),
                    )
                    .await
                    .map_err(|e| backend_error(id, backend, e))?
                }
                None => LakehouseStorage::open(&local)
                    .await
                    .map_err(|e| backend_error(id, backend, e))?,
            };
            Ok(BackendStorage::Parquet(storage))
        }

        // SQLite local — một file cho mỗi station.
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let storage = SqliteStorage::open(&format!("{data_dir}/{segment}-{kind}.sqlite"))
                .await
                .map_err(|e| backend_error(id, backend, e))?;
            Ok(BackendStorage::Sqlite(storage))
        }

        // Backend nhận diện nhưng feature tương ứng chưa được biên dịch.
        #[cfg(not(feature = "parquet"))]
        "parquet" | "duckdb" | "s3" | "lakehouse" => {
            Err(unsupported(id, backend, "opsense-core feature 'parquet'"))
        }
        #[cfg(not(feature = "sqlite"))]
        "sqlite" => Err(unsupported(id, backend, "opsense-core feature 'sqlite'")),

        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported storage backend '{other}' for station '{id}' (kind '{kind}')"),
        )),
    }
}

/// Một block dữ liệu của station: các observation nằm trọn trong
/// `[range.0, range.1]`, cùng block id `floor(ts / block_duration)`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Block {
    pub items: Vec<Observation>,
    pub range: (i64, i64),
    pub last_updated: i64,
}

impl Default for Block {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            range: (i64::MAX, u64::MIN as i64),
            last_updated: 0,
        }
    }
}

pub struct TimeseriesStation {
    caches: LruCache<i64, Block, 32>,
    block_duration: i64,
    storage: Option<Arc<dyn TimeseriesStorage>>,
    /// Background sync task (flush lake + snapshot + retention theo
    /// `[storage]`). `None` khi không có storage hoặc không cấu hình lịch.
    bg: Option<tokio::task::JoinHandle<()>>,
}

impl Default for TimeseriesStation {
    fn default() -> Self {
        Self::new(HOT_BLOCKS, None)
    }
}

/// Toàn bộ những gì `PartialEq` của [`Observation`] so sánh, đóng gói thành
/// một khoá `Hash + Eq` — dùng để bỏ trùng trong
/// [`TimeseriesStation::update_range`].
///
/// `labels` phải vào khoá và phải **thứ tự ổn định**: `HashMap` không có thứ
/// tự, nên sắp xếp cặp khoá–giá trị trước rồi nối lại.
type ObsIdentity = (i64, String, u8, u8, u64, Option<u8>, String);

fn obs_identity(o: &Observation) -> ObsIdentity {
    let mut labels: Vec<(&String, &String)> = o.labels.iter().collect();
    labels.sort_unstable();
    let labels: String = labels
        .iter()
        .map(|(k, v)| format!("{k}={v}\u{1}"))
        .collect();
    (
        o.ts,
        o.metric_id.clone(),
        o.kind as u8,
        o.signal as u8,
        o.value.to_bits(),
        o.severity.map(|s| s as u8),
        labels,
    )
}

impl TimeseriesStation {
    /// Station thuần memory (block evict là mất).
    pub fn new(capacity: usize, block_duration_secs: Option<i64>) -> Self {
        const SECONDS_IN_WEEK: i64 = 7 * 24 * 60 * 60;

        Self {
            caches: LruCache::new(capacity),
            block_duration: block_duration_secs.unwrap_or(SECONDS_IN_WEEK),
            storage: None,
            bg: None,
        }
    }

    /// Constructor theo `[storage]` config: `backend = "parquet"` → Parquet
    /// storage (LRU hot + Parquet cho block lạnh — evict/update/remove tự
    /// persist qua hook `attach_timeseries` của LruCache, miss tự read-through
    /// từ storage), `backend = "sqlite"` → một file sqlite riêng, còn lại
    /// (mặc định `"memory"`) memory-only. Series key của một block là
    /// `blk:<block_id>`, value là Block serialize JSON; `block_duration` lấy từ
    /// `cfg.block_secs`.
    ///
    /// Khi có storage và cấu hình lịch (`s3_flush_interval_secs` /
    /// `s3_snapshot_interval_secs` / `retention_secs`), spawn một background
    /// task: flush buffer ra lake parquet + snapshot/compact + retention trim
    /// định kỳ, để S3 luôn tiến tới trạng thái mới nhất.
    pub async fn from_storage(id: &str, cfg: &StorageConfig) -> Result<Self, Error> {
        let mut station = Self::new(HOT_BLOCKS, Some(cfg.block_secs as i64));

        if let Some(ts) = open_backend(id, cfg, "timeseries").await?.into_timeseries() {
            station.caches.attach_timeseries(
                Arc::clone(&ts),
                Arc::new(|key: &i64| format!("blk:{key}").into_bytes()),
                Arc::new(|block: &Block| serde_json::to_vec(block).unwrap_or_default()),
            );
            station.storage = Some(Arc::clone(&ts));
            Self::spawn_bg_sync(&mut station, cfg, ts);
        }

        Ok(station)
    }

    /// Spawn background task đồng bộ station (nếu có lịch cấu hình). Spawn được
    /// skip khi không có tokio runtime (test thuần) — buffer vẫn được persist
    /// qua WAL/evict hook.
    fn spawn_bg_sync(station: &mut Self, cfg: &StorageConfig, ts: Arc<dyn TimeseriesStorage>) {
        use std::time::{Duration, SystemTime};

        let flush_every = cfg.s3_flush_interval_secs;
        let snapshot_every = cfg.s3_snapshot_interval_secs;
        let retention_secs = cfg.retention_secs;
        if flush_every == 0 && snapshot_every == 0 && retention_secs == 0 {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return; // không có runtime — bỏ lịch.
        };

        let now_secs = || {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        };
        let mut last_flush = now_secs();
        let mut last_snapshot = now_secs();
        let mut last_retention = now_secs();

        let handle = rt.spawn(async move {
            // Chu kỳ cơ bản: mỗi phút một lần, nhưng KHÔNG được chậm hơn cadence
            // nhỏ nhất được cấu hình (VD `s3_flush_interval_secs = 15`) — nếu
            // không, lịch < 60s bị đè thành nhịp 60s trong thực tế.
            let mut base = 60u64;
            for cadence in [flush_every, snapshot_every] {
                if cadence > 0 {
                    base = base.min(cadence);
                }
            }
            let mut ticker = tokio::time::interval(Duration::from_secs(base.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // bỏ tick đầu tiên tức thì.
            loop {
                ticker.tick().await;
                let now = now_secs();
                if flush_every > 0 && now.saturating_sub(last_flush) >= flush_every {
                    let _ = ts.flush().await;
                    last_flush = now;
                }
                if snapshot_every > 0 && now.saturating_sub(last_snapshot) >= snapshot_every {
                    let _ = ts.checkpoint().await;
                    last_snapshot = now;
                }
                if retention_secs > 0 && now.saturating_sub(last_retention) >= 60 {
                    let keep_after = now.saturating_sub(retention_secs);
                    let _ = ts.retain_older_than(keep_after).await;
                    last_retention = now;
                }
            }
        });
        station.bg = Some(handle);
    }

    /// Flush buffer ra lake + snapshot/compact một lần (dùng trước khi tắt
    /// process để S3 tiến tới trạng thái mới nhất), rồi dừng background task.
    pub async fn shutdown(&self) {
        if let Some(storage) = &self.storage {
            let _ = storage.flush().await;
            let _ = storage.checkpoint().await;
        }
        if let Some(bg) = &self.bg {
            bg.abort();
        }
    }

    #[inline]
    pub fn get_block_id(&self, timestamp: i64) -> i64 {
        timestamp / self.block_duration
    }

    /// Block mới nhất của `block_id` từ storage (cold read-through).
    /// `None` = không có storage hoặc storage trống cho block này.
    async fn load_cold_block(&self, block_id: i64) -> Option<Block> {
        let storage = self.storage.as_ref()?;
        let series = format!("blk:{block_id}").into_bytes();
        let points = storage.latest(&series, 1).await.ok()?;
        let (_, bytes) = points.into_iter().next()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// **Chỉ dùng khi thật sự cần ngữ nghĩa strict.** Bất kỳ reader nào (API,
    /// CLI, MCP) phải dùng [`Self::query_recent`].
    ///
    /// Strict nghĩa là: nếu BẤT KỲ block nào trong cửa sổ không "phủ trọn"
    /// khoảng yêu cầu thì trả `None` — kể cả khi block đó có dữ liệu và chỉ
    /// thiếu phần đuôi vốn dĩ không tồn tại. Với station sống, điều đó xảy ra
    /// gần như luôn: block chỉ chứa obs tại timestamp có thật, nên đuôi partition
    /// luôn trống (`range.1` < `block_end`), và block đầu tiên của cửa sổ luôn
    /// bắt đầu sau `from_ts` ⇒ `req_start >= range.0` sai. Đo được với
    /// strategy binance (block 300s, candle 1m): `to = now-120` → 40 dòng,
    /// `to = now-30` → 0 dòng.
    ///
    /// Xem [`Self::query_recent`] cho hành vi đúng của reader.
    pub async fn query_range(&self, from_ts: i64, to_ts: i64) -> Option<Vec<Observation>> {
        let start_block = self.get_block_id(from_ts);
        let end_block = self.get_block_id(to_ts);
        let mut result = Vec::new();

        for block_id in start_block..=end_block {
            let block_start = block_id * self.block_duration;
            let block_end = (block_id + 1) * self.block_duration - 1;

            let req_start = from_ts.max(block_start);
            let req_end = to_ts.min(block_end);

            let (covered, items): (bool, Vec<Observation>) = match self.caches.get(&block_id) {
                // Memory block là version mới nhất — coverage hổng nghĩa là
                // thực sự thiếu dữ liệu, không đi cold-load nữa.
                Some(block) => (
                    req_start >= block.range.0 && req_end <= block.range.1,
                    block
                        .items
                        .iter()
                        .filter(|o| o.ts >= req_start && o.ts <= req_end)
                        .cloned()
                        .collect(),
                ),
                None => match self.load_cold_block(block_id).await {
                    Some(block) => {
                        let covered = req_start >= block.range.0 && req_end <= block.range.1;
                        let items = block
                            .items
                            .iter()
                            .filter(|o| o.ts >= req_start && o.ts <= req_end)
                            .cloned()
                            .collect();
                        self.caches.put(block_id, block);
                        (covered, items)
                    }
                    // Cache miss do thiếu khoảng phủ dữ liệu.
                    None => return None,
                },
            };

            if !covered {
                return None;
            }
            result.extend(items);
        }

        Some(result)
    }

    /// Query dữ liệu mà station thực sự có trong cửa sổ — chịu được coverage
    /// hổng.
    ///
    /// Duyệt mọi block từ `from_ts` tới `to_ts`, gom observation nằm trong
    /// khoảng yêu cầu của từng block; block không tồn tại (chưa ghi, đã
    /// evict/retention) chỉ bị bỏ qua — KHÔNG dừng quét, vì dữ liệu có thể nằm
    /// rải rác ở block phía trước/sau lỗ hổng (vd station chỉ mới có một cửa
    /// sổ vài chục giây dưới `to_ts`). Không như [`query_range`] (trả `None`
    /// nếu BẤT KỲ block nào trong cửa sổ chưa cover trọn), method này trả
    /// *những gì thực sự có* — đây là hành vi đúng cho MỌI reader:
    /// script-facing `station_query` (`candles.rs`) và API `Query.queryTimeseries`
    /// mà `opsense query` / `opsense orders` / MCP `opsense_query_timeseries` dùng.
    pub async fn query_recent(&self, from_ts: i64, to_ts: i64) -> Option<Vec<Observation>> {
        let end_block = self.get_block_id(to_ts);
        let mut start_block = self.get_block_id(from_ts);

        // Chặn trần số block một query được duyệt. Không có trần này, cửa sổ
        // mặc định của `Query.queryTimeseries` (30 ngày) với block nhỏ là một
        // vòng lặp khổng lồ: `block_secs = 5` ⇒ ~518.400 block, mỗi block một
        // lần `caches.get` (miss) + `load_cold_block` ⇒ tới storage. Đo trên
        // `strategies/binance` với `cache_block_seconds = 5` đó là hàng trăm
        // nghìn lượt đọc cho **một** lệnh query.
        //
        // Cắt cửa sổ về `MAX_BLOCKS_PER_QUERY` block gần nhất thay vì quét hết:
        // dữ liệu "gần nhất" mới là thứ mọi reader thực sự hỏi, và việc bỏ
        // phần cũ được **log** chứ không im lặng.
        if end_block - start_block + 1 > MAX_BLOCKS_PER_QUERY {
            let dropped = end_block - start_block + 1 - MAX_BLOCKS_PER_QUERY;
            start_block = end_block - MAX_BLOCKS_PER_QUERY + 1;
            tracing::warn!(
                from_ts,
                to_ts,
                dropped_blocks = dropped,
                block_duration = self.block_duration,
                "query_recent: cửa sổ quá rộng so với số block — chỉ duyệt \
                 {} block gần nhất, bỏ {dropped} block cũ",
                MAX_BLOCKS_PER_QUERY
            );
        }

        let mut result = Vec::new();

        for block_id in start_block..=end_block {
            let block_start = block_id * self.block_duration;
            let block_end = (block_id + 1) * self.block_duration - 1;

            // `caches.get` đã trả về `Block` **sở hữu** (`LruCache::get` clone
            // bên trong) — clone thêm ở đây là tốn gấp đôi. Block của station
            // tick có thể vài trăm MB, nên giữ đúng một bản copy.
            let block = match self.caches.get(&block_id) {
                Some(b) => b,
                None => match self.load_cold_block(block_id).await {
                    Some(b) => {
                        self.caches.put(block_id, b.clone());
                        b
                    }
                    // Block không có dữ liệu — lỗ hổng của cửa sổ, bỏ qua.
                    None => continue,
                },
            };

            let req_start = from_ts.max(block_start);
            let req_end = to_ts.min(block_end);
            let items: Vec<Observation> = block
                .items
                .iter()
                .filter(|o| o.ts >= req_start && o.ts <= req_end)
                .cloned()
                .collect();
            result.extend(items);
        }

        Some(result)
    }

    pub fn update_range(
        &self,
        records: &[Observation],
        query_from: i64,
        query_to: i64,
        now: i64,
    ) {
        let start_block = self.get_block_id(query_from);
        let end_block = self.get_block_id(query_to);

        // Batch trải trên nhiều block hơn sức chứa LRU ⇒ các block cũ bị đẩy ra
        // ngay trong vòng lặp. Có storage thì hook persist cứu lại; **không có**
        // (station memory-only) thì mất vĩnh viễn, im lặng. Đo được: ghi 40.000
        // obs trên 667 block (cache 32) chỉ đọc lại 1.920.
        //
        // Thành thật trong `strategies/binance`: `history` fetch 500 nến mỗi 10s.
        // Với `[storage] block_secs = 3600` (mặc định) đó ~9 block/lần nên an
        // toàn; nhưng nếu ai đó hạ `block_secs` xuống 5 cho khớp
        // `cache_block_seconds`, 500 nến = hàng nghìn block ⇒ mất ~99% mỗi
        // lần fetch mà không có dấu vết.
        let span = end_block - start_block + 1;
        if span > HOT_BLOCKS as i64 && self.storage.is_none() {
            tracing::warn!(
                station_span_blocks = span,
                hot_blocks = HOT_BLOCKS,
                from_ts = query_from,
                to_ts = query_to,
                "update_range: batch trải {} block > sức chứa cache {} và station \
                 không có storage ⇒ block cũ bị mất. Hoặc tăng cache_max_blocks, \
                 hoặc tăng [storage] block_secs, hoặc bật backend có persist.",
                span,
                HOT_BLOCKS
            );
        }

        for block_id in start_block..=end_block {
            let mut block = self.caches.get(&block_id).unwrap_or_default();

            let block_start = block_id * self.block_duration;
            let block_end = (block_id + 1) * self.block_duration - 1;

            // Lọc các observation thuộc về block này
            for obs in records {
                if obs.ts >= block_start && obs.ts <= block_end {
                    block.items.push(obs.clone());
                }
            }

            // Bỏ trùng **chính xác**.
            //
            // Cách cũ là `sort_by_key(ts)` + `dedup_by(|a, b| a == b)`, và nó
            // bỏ sót gần như mọi thứ: `dedup_by` chỉ so **hai phần tử liền kề**,
            // mà nến 1m có 5 observation (`o/h/l/c/v`) dùng chung một `ts`, nên
            // bản cũ và bản mới của cùng một field không bao giờ đứng cạnh nhau
            // (`o h l c v o h l c v`) ⇒ không cặp nào giống nhau. Đo được: nạp 3
            // lần cùng một bộ 61 nến (305 dòng thật) cho **915** dòng. Node
            // `http_origin` nạp lại định kỳ nên đây là chuyện thường, không
            // phải rì hiếm.
            //
            // Vì sao không gộp theo `(ts, field, metric)` — cách nhìn hiển nhiên
            // hơn: nó **mất dữ liệu**. Record khác nhau hoàn toàn có thể trùng
            // `ts` và `metric_id` mà khác `labels` (hai lệnh cùng giây khác
            // `order_id`; portfolio nhiều dòng cùng mốc). Đo được: gộp kiểu đó làm
            // nhánh `trading` của pipeline binance **ngừng sinh lệnh**. Nên khoá
            // phải là *toàn bộ* những gì `PartialEq` so sánh — xem
            // [`obs_identity`].
            //
            // `sort_by_key` là **stable** nên các observation cùng `ts` giữ thứ
            // tự ghi, và `retain` giữ bản đầu tiên. Giữ bản đầu hay bản sau
            // không khác gì khi chúng *giống hệt nhau*.
            block.items.sort_by_key(|x| x.ts);
            let mut seen: std::collections::HashSet<ObsIdentity> =
                std::collections::HashSet::with_capacity(block.items.len());
            block.items.retain(|o| seen.insert(obs_identity(o)));

            // Cập nhật range bao phủ và timestamp sửa đổi
            let eff_from = query_from.max(block_start);
            let eff_to = query_to.min(block_end);

            block.range.0 = block.range.0.min(eff_from);
            block.range.1 = block.range.1.max(eff_to);
            block.last_updated = now;

            self.caches.put(block_id, block);
        }
    }
}

pub struct PatternStation {
    automaton: Arc<RwLock<AhoCorasick>>,
    hits: AtomicU64,
    misses: AtomicU64,
    storage: Option<Arc<dyn PatternStorage>>,
}

impl Default for PatternStation {
    fn default() -> Self {
        Self::new()
    }
}

impl PatternStation {
    pub fn new() -> Self {
        Self {
            automaton: Arc::new(RwLock::new(AhoCorasick::new())),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            storage: None,
        }
    }

    /// Constructor theo `[storage]` config: gắn `PatternStorage` để registry
    /// pattern persist — mỗi `set` ghi thêm vào storage, mở lại restore mọi
    /// pattern đã đăng ký rồi rebuild automaton. Backend `"memory"` (mặc định)
    /// là station thuần memory.
    pub async fn from_storage(id: &str, cfg: &StorageConfig) -> Result<Self, Error> {
        let station = Self::new();

        let Some(storage) = open_backend(id, cfg, "pattern").await?.into_pattern() else {
            return Ok(station);
        };

        // Restore registry pattern từ storage trên đĩa → rebuild automaton.
        let patterns = storage.get_all().await.map_err(|e| {
            Error::other(format!(
                "failed to load patterns from storage for station '{id}': {e}"
            ))
        })?;
        {
            let mut automaton = station.automaton.write().await;
            for pattern in patterns {
                automaton.add(pattern);
            }
        }
        station.automaton.write().await.optimize().await;

        Ok(Self {
            storage: Some(storage),
            ..station
        })
    }

    pub async fn set(&self, template: &str) {
        self.automaton.write().await.add(template.to_string());
        if let Some(storage) = &self.storage {
            let _ = storage.add(template).await;
        }
    }

    pub async fn commit(&self) {
        self.automaton.write().await.optimize().await;
    }

    pub async fn lookup(&self, sample: &str) -> bool {
        let matched = {
            self.automaton
                .read()
                .await
                .similar(&sample.to_string())
                .await
        };

        if matched {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        matched
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

pub struct CategoryStation {
    search: Search,
    id: SnowflakeId,
    storage: Option<Arc<RwLock<dyn CategoryStorage>>>,
}

impl Default for CategoryStation {
    fn default() -> Self {
        Self::new()
    }
}

impl CategoryStation {
    pub fn new() -> Self {
        Self {
            search: Search::<u8>::in_memory(1),
            id: SnowflakeId::new(1, 1),
            storage: None,
        }
    }

    /// Constructor theo `[storage]` config: `backend = "parquet"` → radix tree
    /// persists qua `CategoryStorage` của Parquet storage (mỗi `insert_chain`
    /// commit thẳng vào storage; mở lại có sẵn dữ liệu — snapshot/restore S3
    /// theo cơ chế riêng), `backend = "sqlite"` → file sqlite
    /// riêng, còn lại (mặc định `"memory"`) memory-only.
    pub async fn from_storage(id: &str, cfg: &StorageConfig) -> Result<Self, Error> {
        let Some(storage) = open_backend(id, cfg, "category").await?.into_category() else {
            return Ok(Self::new());
        };

        Ok(Self {
            search: Search::<u8>::new(1, Arc::clone(&storage)),
            id: SnowflakeId::new(1, 1),
            storage: Some(storage),
        })
    }

    /// Storage backend của radix tree (nếu có) — `None` cho station memory.
    #[must_use]
    pub fn storage(&self) -> Option<&Arc<RwLock<dyn CategoryStorage>>> {
        self.storage.as_ref()
    }

    pub async fn insert(&mut self, text: &str, metadata: &str) -> Result<u64, Error> {
        let record_id = self.id.generate() as u64;
        let key_bytes = text.as_bytes();

        let mut metas = vec![None; key_bytes.len()];
        if !key_bytes.is_empty() {
            metas[0] = Some(metadata.as_bytes());
        }

        self.search
            .insert_chain(record_id as usize, key_bytes, &metas)
            .await
            .map_err(|error| {
                Error::new(ErrorKind::BrokenPipe, format!("insert failed: {error}"))
            })?;
        Ok(record_id)
    }

    pub async fn contains(
        &self,
        sample: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, u64)>, Error> {
        let pattern_bytes = sample.as_bytes();

        Ok(self
            .search
            .search(pattern_bytes, None, offset, limit)
            .await
            .map_err(|error| Error::new(ErrorKind::BrokenPipe, format!("search failed: {error}")))?
            .into_iter()
            .map(|(record_id, _)| (record_id as u64, offset.unwrap_or(0) as u64))
            .collect())
    }
}

pub enum Station {
    Timeseries(Arc<RwLock<TimeseriesStation>>),
    Category(Arc<RwLock<CategoryStation>>),
    Pattern(Arc<RwLock<PatternStation>>),
}

impl Station {
    /// GraphQL `kind` discriminator.
    #[must_use]
    pub fn kind(&self) -> StationKind {
        match self {
            Station::Timeseries(_) => StationKind::Timeseries,
            Station::Category(_) => StationKind::Category,
            Station::Pattern(_) => StationKind::Pattern,
        }
    }
}

/// Discriminator cho GraphQL — bám sát các variant của [`Station`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
#[graphql(rename_items = "lowercase")]
pub enum StationKind {
    Timeseries,
    Category,
    Pattern,
}

impl TryFrom<&Station> for Arc<RwLock<TimeseriesStation>> {
    type Error = Error;

    fn try_from(station: &Station) -> Result<Self, Self::Error> {
        match station {
            Station::Timeseries(inner) => Ok(Arc::clone(inner)),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "Station is not of type Timeseries",
            )),
        }
    }
}

impl TryFrom<&Station> for Arc<RwLock<CategoryStation>> {
    type Error = Error;

    fn try_from(station: &Station) -> Result<Self, Self::Error> {
        match station {
            Station::Category(inner) => Ok(Arc::clone(inner)),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "Station is not of type Category",
            )),
        }
    }
}

impl TryFrom<&Station> for Arc<RwLock<PatternStation>> {
    type Error = Error;

    fn try_from(station: &Station) -> Result<Self, Self::Error> {
        match station {
            Station::Pattern(inner) => Ok(Arc::clone(inner)),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "Station is not of type Pattern",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_model::events::{Signal, TelemetryKind};

    fn obs(ts: i64, value: f64) -> Observation {
        Observation::new(ts, "cpu".into(), TelemetryKind::Metric, Signal::Raw, value)
    }

    /// Cửa sổ rộng hơn `MAX_BLOCKS_PER_QUERY` phải bị cắt về các block gần
    /// nhất — nếu không, cửa sổ mặc định 30 ngày với `block_secs = 5` là
    /// ~518.400 lần `caches.get` + `load_cold_block` cho **một** lệnh query.
    #[tokio::test]
    async fn query_recent_caps_blocks_visited() {
        let st = TimeseriesStation::new(32, Some(5)); // block 5s — nhỏ nhất thực tế
        let batch = vec![obs(1_000_000, 1.0)];
        st.update_range(&batch, 1_000_000, 1_000_000, 1_000_000);

        // 30 ngày ở block 5s = 518.400 block, vượt trần.
        let from = 1_000_000 - 30 * 24 * 3600;
        let to = 1_000_000;
        let span = (to - from) / 5;
        assert!(
            span > MAX_BLOCKS_PER_QUERY,
            "test chỉ có nghĩa khi cửa sổ vượt trần (span={span})"
        );

        // Không panic, không treo, vẫn trả dữ liệu ở phần được giữ.
        let got = st.query_recent(from, to).await.unwrap();
        assert!(
            got.iter().any(|o| o.ts == 1_000_000),
            "phải giữ được block gần nhất: {got:?}"
        );

        // Cửa sổ vừa đủ thì không bị cắt.
        let narrow = st.query_recent(1_000_000 - 600, 1_000_000).await.unwrap();
        assert_eq!(narrow.len(), 1);
    }

    /// `query_range` trả None khi bất kỳ block nào trong cửa sổ chưa cover
    /// trọn; `query_recent` trả vùng dữ liệu gần nhất dù cửa sổ hổng.
    #[tokio::test]
    async fn query_recent_returns_latest_suffix_over_holes() {
        let st = TimeseriesStation::new(32, Some(300)); // block 300s

        // Block 0 chỉ cover [100, 110].
        let batch = vec![obs(100, 1.0), obs(105, 2.0), obs(110, 3.0)];
        st.update_range(&batch, 100, 110, 200);

        // Cửa sổ rộng [0, 200]: block 0 không cover trọn bên trái → None.
        assert!(st.query_range(0, 200).await.is_none());

        // query_recent: đuôi [100, 110] vẫn trả về (dù từ_ts hổng).
        let got = st.query_recent(0, 200).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].value, 1.0);
        assert_eq!(got[2].value, 3.0);

        // Phần tương lai chưa ghi: block mới nhất không tồn tại → kết quả rỗng.
        assert!(st.query_recent(400, 2000).await.unwrap().is_empty());
    }
}
