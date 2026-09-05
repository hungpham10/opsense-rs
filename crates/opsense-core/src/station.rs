use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_graphql::Enum;
use tokio::sync::RwLock;

use opsense_libs::ahocorasick::AhoCorasick;
use opsense_libs::lru::LruCache;
use opsense_libs::search::Search;
use opsense_libs::snowflake_id::SnowflakeId;
use opsense_model::events::Observation;

#[derive(Clone, Debug)]
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
}

impl Default for TimeseriesStation {
    fn default() -> Self {
        Self::new(32, None)
    }
}

impl TimeseriesStation {
    pub fn new(capacity: usize, block_duration_secs: Option<i64>) -> Self {
        const SECONDS_IN_WEEK: i64 = 7 * 24 * 60 * 60;

        Self {
            caches: LruCache::new(capacity),
            block_duration: block_duration_secs.unwrap_or(SECONDS_IN_WEEK),
        }
    }

    #[inline]
    pub fn get_block_id(&self, timestamp: i64) -> i64 {
        timestamp / self.block_duration
    }

    pub fn query_range(&mut self, from_ts: i64, to_ts: i64) -> Option<Vec<Observation>> {
        let start_block = self.get_block_id(from_ts);
        let end_block = self.get_block_id(to_ts);
        let mut result = Vec::new();

        for block_id in start_block..=end_block {
            let block = self.caches.get(&block_id)?;
            let block_start = (block_id) * self.block_duration;
            let block_end = (block_id + 1) * self.block_duration - 1;

            let req_start = from_ts.max(block_start);
            let req_end = to_ts.min(block_end);

            // Kiểm tra xem Block đã bao phủ (cover) đủ khoảng thời gian yêu cầu chưa
            if req_start >= block.range.0 && req_end <= block.range.1 {
                for item in &block.items {
                    if item.ts >= req_start && item.ts <= req_end {
                        result.push(item.clone());
                    }
                }
            } else {
                return None; // Cache Miss do thiếu khoảng phủ dữ liệu
            }
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
        }
    }

    pub async fn set(&self, template: &str) {
        self.automaton.write().await.add(template.to_string());
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
