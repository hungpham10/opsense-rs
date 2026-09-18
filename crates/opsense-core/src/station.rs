use std::io::{Error, ErrorKind};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_graphql::Enum;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use opsense_libs::ahocorasick::AhoCorasick;
use opsense_libs::lru::LruCache;
use opsense_libs::search::Search;
use opsense_libs::snowflake_id::SnowflakeId;
use opsense_libs::storage::TimeseriesStorage;
#[cfg(feature = "duckdb")]
use opsense_libs::storage::duckdb::DuckS3Storage;
use opsense_model::events::Observation;

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
    /// Persist/read-through backend (duckdb lakehouse khi được cấu hình).
    /// `None` = station thuần memory (block evict là mất — như cũ).
    storage: Option<Arc<dyn TimeseriesStorage>>,
}

impl Default for TimeseriesStation {
    fn default() -> Self {
        Self::new(32, None)
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
    /// (LRU hot + Parquet qua DuckDB cho block lạnh — evict/update/remove tự
    /// persist qua hook `attach_timeseries` của LruCache, miss tự read-through
    /// từ storage), còn lại memory-only. Series key của một block là
    /// `blk:<block_id>`, value là Block serialize JSON.
    pub async fn from_storage(id: &str, cfg: &crate::config::StorageConfig) -> Result<Self, Error> {
        if cfg.backend != "duckdb" {
            return Ok(Self::default());
        }
        #[cfg(not(feature = "duckdb"))]
        {
            let _ = id;
            Err(Error::new(
                ErrorKind::InvalidInput,
                "storage.backend = 'duckdb' requires the `duckdb` feature \
                 (build opsense with --features duckdb)",
            ))
        }
        #[cfg(feature = "duckdb")]
        {
            let store: Arc<dyn TimeseriesStorage> = Arc::new(open_station_store(id, cfg).await?);
            let mut caches = LruCache::new(32);
            caches.attach_timeseries(
                store.clone(),
                Arc::new(|block_id: &i64| format!("blk:{block_id}").into_bytes()),
                Arc::new(|block: &Block| serde_json::to_vec(block).unwrap_or_default()),
            );
            Ok(Self {
                caches,
                block_duration: cfg.block_secs.max(1) as i64,
                storage: Some(store),
            })
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

/// Mở `DuckS3Storage` dùng chung cho các station của node `id`: file DuckDB
/// cục bộ tại `<data_dir>/<id>.duckdb` (local) hoặc buffer trong temp dir khi
/// Parquet đi lên `s3://`. Return `Err` nếu thiếu credentials cho S3.
#[cfg(feature = "duckdb")]
async fn open_station_store(
    id: &str,
    cfg: &crate::config::StorageConfig,
) -> Result<DuckS3Storage, Error> {
    let s3 = s3_config_from_storage(cfg, id)?;
    let (db_path, s3cfg) = match &s3 {
        Some(cfg) => {
            let hash: u64 = id.bytes().fold(0xcbf29ce484222325u64, |acc, b| {
                (acc ^ u64::from(b)) * 0x100000001b3
            });
            let dir = std::env::temp_dir().join(format!("opsense-ts-{hash:016x}"));
            (
                dir.join("buffer.duckdb").to_string_lossy().into_owned(),
                Some(cfg.clone()),
            )
        }
        None => {
            let path = std::path::Path::new(&cfg.data_dir).join(format!("{id}.duckdb"));
            (path.to_string_lossy().into_owned(), None)
        }
    };
    let store = match s3cfg {
        Some(s3) => DuckS3Storage::open_with_s3(&db_path, s3, 4096).await,
        None => DuckS3Storage::open(&db_path).await,
    }
    .map_err(|e| Error::other(e.to_string()))?;
    Ok(store)
}

/// Map `[storage]` config → S3 credentials cho lakehouse (chỉ khi `data_dir`
/// là `s3://bucket[/prefix]`). Field thiếu trong TOML được bù bằng env
/// `OPSENSE_S3_*` rồi `AWS_*`. Prefix của station = `<prefix>/<station_id>`.
#[cfg(feature = "duckdb")]
fn s3_config_from_storage(
    cfg: &crate::config::StorageConfig,
    station_id: &str,
) -> Result<Option<opsense_libs::storage::duckdb::S3Config>, Error> {
    use opsense_libs::storage::duckdb::S3Config;

    let data_dir = cfg.data_dir.trim_end_matches('/');
    let Some(rest) = data_dir.strip_prefix("s3://").filter(|r| !r.is_empty()) else {
        return Ok(None);
    };
    let (bucket, mut prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket.to_string(), prefix.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if !prefix.is_empty() {
        prefix.push('/');
    }
    prefix.push_str(station_id);

    let sc = cfg.s3.clone().unwrap_or_default();
    let from = |v: &Option<String>, var: &str| {
        v.clone()
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var(var).ok().filter(|s| !s.is_empty()))
    };
    let access_key_id = from(&sc.access_key_id, "OPSENSE_S3_ACCESS_KEY_ID")
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok());
    let secret_access_key = from(&sc.secret_access_key, "OPSENSE_S3_SECRET_ACCESS_KEY")
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok());
    let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "storage.data_dir là s3:// nhưng thiếu credentials \
             ([storage.s3] hoặc OPSENSE_S3_ACCESS_KEY_ID / OPSENSE_S3_SECRET_ACCESS_KEY)",
        ));
    };

    Ok(Some(S3Config {
        bucket,
        prefix,
        endpoint: from(&sc.endpoint, "OPSENSE_S3_ENDPOINT"),
        region: from(&sc.region, "OPSENSE_S3_REGION"),
        access_key_id,
        secret_access_key,
        session_token: from(&sc.session_token, "OPSENSE_S3_SESSION_TOKEN"),
    }))
}

pub struct PatternStation {
    automaton: Arc<RwLock<AhoCorasick>>,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Persist backend cho patterns (`backend = "duckdb"`): `set()` ghi thẳng
    /// vào `ac_patterns`, mở lại load `get_all()`. `None` = memory.
    #[cfg(feature = "duckdb")]
    storage: Option<Arc<DuckS3Storage>>,
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
            #[cfg(feature = "duckdb")]
            storage: None,
        }
    }

    /// Constructor theo `[storage]` config — tương tự [`CategoryStation::from_storage`].
    pub async fn from_storage(id: &str, cfg: &crate::config::StorageConfig) -> Result<Self, Error> {
        if cfg.backend != "duckdb" {
            return Ok(Self::new());
        }
        #[cfg(not(feature = "duckdb"))]
        {
            let _ = id;
            Err(Error::new(
                ErrorKind::InvalidInput,
                "storage.backend = 'duckdb' requires the `duckdb` feature",
            ))
        }
        #[cfg(feature = "duckdb")]
        {
            use opsense_libs::storage::PatternStorage;
            let store = open_station_store(id, cfg).await?;
            let automaton = RwLock::new(AhoCorasick::new());
            for p in store
                .get_all()
                .await
                .map_err(|e| Error::other(format!("load patterns: {e}")))?
            {
                automaton.write().await.add(p);
            }
            Ok(Self {
                automaton: Arc::new(automaton),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                storage: Some(Arc::new(store)),
            })
        }
    }

    pub async fn set(&self, template: &str) {
        self.automaton.write().await.add(template.to_string());
        #[cfg(feature = "duckdb")]
        if let Some(storage) = &self.storage {
            use opsense_libs::storage::PatternStorage;
            let _ = storage.add(template).await; // best-effort, như persist_point
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
        }
    }

    /// Constructor theo `[storage]` config: `backend = "duckdb"` → radix tree
    /// persists qua `CategoryStorage` của `DuckS3Storage` (mỗi `insert_chain`
    /// commit thẳng vào storage; mở lại có sẵn dữ liệu — snapshot/restore S3
    /// theo cơ chế của DuckS3Storage), còn lại memory-only.
    pub async fn from_storage(id: &str, cfg: &crate::config::StorageConfig) -> Result<Self, Error> {
        if cfg.backend != "duckdb" {
            return Ok(Self::new());
        }
        #[cfg(not(feature = "duckdb"))]
        {
            let _ = id;
            Err(Error::new(
                ErrorKind::InvalidInput,
                "storage.backend = 'duckdb' requires the `duckdb` feature",
            ))
        }
        #[cfg(feature = "duckdb")]
        {
            let store = open_station_store(id, cfg).await?;
            let search = Search::new(1, Arc::new(tokio::sync::RwLock::new(store)));
            Ok(Self {
                search,
                id: SnowflakeId::new(1, 1),
            })
        }
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

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use opsense_model::events::{Signal, TelemetryKind};

    fn duck_cfg(dir: &std::path::Path, block_secs: u64) -> StorageConfig {
        StorageConfig {
            backend: "duckdb".into(),
            data_dir: dir.to_string_lossy().into_owned(),
            block_secs,
            ..StorageConfig::default()
        }
    }

    fn obs(ts: i64) -> Observation {
        Observation::new(
            ts,
            "m".into(),
            TelemetryKind::Metric,
            Signal::Raw,
            ts as f64,
        )
    }

    /// Block bị LRU evict → tự persist qua hook; query lại → cold load từ
    /// storage (read-through) trả đúng dữ liệu.
    #[tokio::test]
    async fn duck_station_persists_evicted_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = duck_cfg(dir.path(), 1);
        let mut st = TimeseriesStation::from_storage("test-ts", &cfg)
            .await
            .unwrap();
        // 34 block riêng biệt (block_secs = 1) → vượt LRU 32 → block đầu evict.
        for ts in 1..=34i64 {
            st.update_range(&[obs(ts)], ts, ts, ts);
        }
        // Đợi persist_point đã spawn xong (fire-and-forget trên tokio).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let rows = st
            .query_range(1, 1)
            .await
            .expect("cold block reload phải cover");
        assert_eq!(rows.len(), 1, "1 observation trong block đầu");
        assert_eq!(rows[0].ts, 1);
        assert_eq!(rows[0].value, 1.0);
    }

    /// Memory hit vẫn ưu tiên — query ngay sau ghi không cần chạm storage.
    #[tokio::test]
    async fn duck_station_hot_path_hits_memory() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = duck_cfg(dir.path(), 3600);
        let mut st = TimeseriesStation::from_storage("test-hot", &cfg)
            .await
            .unwrap();
        st.update_range(&[obs(100), obs(200)], 100, 200, 200);
        let rows = st.query_range(100, 200).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    /// Không có storage (memory backend) → miss khi block chưa từng ghi.
    #[tokio::test]
    async fn memory_station_misses_unknown_range() {
        let mut st = TimeseriesStation::default();
        assert!(st.query_range(1, 2).await.is_none());
        st.update_range(&[obs(1)], 1, 1, 1);
        assert_eq!(st.query_range(1, 1).await.unwrap().len(), 1);
        assert!(st.query_range(2, 2).await.is_none());
    }
}
