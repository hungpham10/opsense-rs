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
use opsense_mlib::storage::{CategoryStorage, PatternStorage, TimeseriesStorage};
#[cfg(feature = "parquet")]
use opsense_mlib::storage::LakehouseStorage;
#[cfg(feature = "sqlite")]
use opsense_mlib::storage::SqliteStorage;
use opsense_model::events::Observation;

use crate::config::StorageConfig;

// ==================== Storage backend ====================
//
// Dựng storage backend từ `[storage]` config, dùng chung cho cả 3 station
// (timeseries / pattern / category). Backend `"memory"` (mặc định) trả về
// station thuần memory; `"duckdb"`/`"s3"`/`"lakehouse"` mở Parquet lakehouse;
// `"sqlite"` mở một file riêng cho từng station. Backend nhận diện nhưng
// chưa được biên dịch (feature tắt) báo lỗi rõ ràng thay vì lặng lẽ hạ cấp.

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

        // Parquet lakehouse (thay thế DuckDB cũ).
        #[cfg(feature = "parquet")]
        "duckdb" | "s3" | "lakehouse" => {
            let storage = LakehouseStorage::open(&format!("{data_dir}/{segment}-{kind}"))
                .await
                .map_err(|e| backend_error(id, backend, e))?;
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
        "duckdb" | "s3" | "lakehouse" => Err(unsupported(
            id,
            backend,
            "opsense-core feature 'parquet'",
        )),
        #[cfg(not(feature = "sqlite"))]
        "sqlite" => Err(unsupported(id, backend, "opsense-core feature 'sqlite'")),

        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "unsupported storage backend '{other}' for station '{id}' (kind '{kind}')"
            ),
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
}

impl Default for TimeseriesStation {
    fn default() -> Self {
        Self::new(HOT_BLOCKS, None)
    }
}

impl TimeseriesStation {
    /// Station thuần memory (block evict là mất).
    pub fn new(capacity: usize, block_duration_secs: Option<i64>) -> Self {
        const SECONDS_IN_WEEK: i64 = 7 * 24 * 60 * 60;

        Self {
            caches: LruCache::new(capacity),
            block_duration: block_duration_secs.unwrap_or(SECONDS_IN_WEEK),
            storage: None,
        }
    }

    /// Constructor theo `[storage]` config: `backend = "duckdb"` → lakehouse
    /// (LRU hot + Parquet qua lakehouse cho block lạnh — evict/update/remove tự
    /// persist qua hook `attach_timeseries` của LruCache, miss tự read-through
    /// từ storage), `backend = "sqlite"` → một file sqlite riêng, còn lại
    /// (mặc định `"memory"`) memory-only. Series key của một block là
    /// `blk:<block_id>`, value là Block serialize JSON; `block_duration` lấy từ
    /// `cfg.block_secs`.
    pub async fn from_storage(id: &str, cfg: &StorageConfig) -> Result<Self, Error> {
        let mut station = Self::new(HOT_BLOCKS, Some(cfg.block_secs as i64));

        if let Some(ts) = open_backend(id, cfg, "timeseries").await?.into_timeseries() {
            station.caches.attach_timeseries(
                Arc::clone(&ts),
                Arc::new(|key: &i64| format!("blk:{key}").into_bytes()),
                Arc::new(|block: &Block| serde_json::to_vec(block).unwrap_or_default()),
            );
            station.storage = Some(ts);
        }

        Ok(station)
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

    pub async fn query_range(&mut self, from_ts: i64, to_ts: i64) -> Option<Vec<Observation>> {
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

    pub fn update_range(
        &mut self,
        records: &[Observation],
        query_from: i64,
        query_to: i64,
        now: i64,
    ) {
        let start_block = self.get_block_id(query_from);
        let end_block = self.get_block_id(query_to);

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

            // Sap xep va xoa trung lặp
            block.items.sort_by_key(|x| x.ts);
            block.items.dedup_by_key(|x| x.ts);

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

    /// Constructor theo `[storage]` config: `backend = "duckdb"` → radix tree
    /// persists qua `CategoryStorage` của Parquet lakehouse (mỗi `insert_chain`
    /// commit thẳng vào storage; mở lại có sẵn dữ liệu — snapshot/restore S3
    /// theo cơ chế riêng của lakehouse), `backend = "sqlite"` → file sqlite
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

