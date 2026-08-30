//! `Station` — the unified station abstraction shared across crates.
//!
//! Mỗi node có thể publish một "station" dưới một id; consumer (Rhai, MCP,
//! HTTP, các transform khác) đọc qua `OpsenseContext.stations`. Ba hình thức:
//!
//! - `Timeseries` — bounded time-series cache bọc `LruCache`. Mỗi `(stage,
//!   metric_id)` là một entry; value là `BTreeMap<ts, Observation>` để range
//!   scan nhanh. Evict theo entry (nghĩa là theo `(stage, metric)`).
//! - `Category` — radix + KMP substring search (`Search<u8>`), kèm index
//!   key/value để trả về kết quả dạng `(key, value)`.
//! - `Pattern` — Aho-Corasick multi-pattern matcher (`AhoCorasick`), kèm
//!   bộ đếm hit/miss.
//!
//! `Station` là `Send + Sync` (cả `Search`/`AhoCorasick`/`LruCache` đều
//! `Send + Sync`), nên có thể nằm sau `Arc<RwLock<Station>>` trong registry.
//!
//! `query`/`append`/`add_pattern`/`is_known`/`insert_entry`/`search_entries`
//! là **async** vì `Search::*` và `AhoCorasick::*` đều async (còn `LruCache`
//! đã sync) — nhưng thực tế không await IO (chỉ bookkeeping/automaton), nên
//! chi phí trên mỗi lời gọi là một future tầm thường. Giữ async để
//! `read_window` (trên trait `Context`) cũng async và object-safe qua native
//! `async fn` trong trait. Rhai gọi các hàm này qua `block_on` (an toàn vì
//! script chạy trên `spawn_blocking`), còn MCP/REPL await trực tiếp.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use opsense_libs::ahocorasick::AhoCorasick;
use opsense_libs::lru::LruCache;
use opsense_libs::search::Search;
use opsense_model::Observation;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// Giai đoạn của một observation trong pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Stage {
    /// Freshly fetched from sources (post-reduce).
    Raw,

    /// Published by the processor node after each cycle.
    Processed,
}

impl Stage {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Stage::Raw => "raw",
            Stage::Processed => "processed",
        }
    }
}

/// Which node's cursor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Cursor {
    Named(String),
    IngestDone,
    ProcessedDone,
}

/// Time-series station: an LRU cache of `(stage, metric)` → `BTreeMap<ts, obs>`.
///
/// `metrics` is a side index of known metric ids so `query_all` can enumerate
/// them without reaching into the `LruCache`'s internal map.
pub struct TimeseriesStation {
    pub cache: LruCache<(Stage, String), BTreeMap<i64, Observation>, 16>,
    metrics: Mutex<HashMap<String, ()>>,
    /// Stages this station is declared to hold, fixed at creation time by the
    /// owning node (e.g. an `http_source` station is raw-only). `append` warns
    /// on a foreign stage and `describe` reports it, so a misconfigured sink
    /// snapshotting an empty stage is visible instead of silently `[]`.
    pub stages: Vec<Stage>,
}

/// Aho-Corasick pattern station: the matcher plus hit/miss counters (matching
/// the old `TextIndex::PatternInner` semantics so MCP/REPL stats survive).
pub struct PatternStation {
    pub automaton: AhoCorasick,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

/// Radix + KMP category station: the search index plus the key/value entries
/// map (matching the old `TextIndex::KeyValue` semantics).
pub struct CategoryStation {
    pub search: Search<u8>,
    pub entries: Mutex<BTreeMap<usize, (String, String)>>,
    pub next_idx: AtomicU64,
}

/// Một station, quản lý qua `Arc<RwLock<Station>>` trong
/// `OpsenseContext.stations` (keyed bởi component id).
pub enum Station {
    /// Time-series cache (bounded LRU over `(stage, metric)` entries).
    Timeseries(TimeseriesStation),
    /// Radix + KMP substring search trên key, kèm entries key/value.
    Category(CategoryStation),
    /// Aho-Corasick multi-pattern matcher, kèm bộ đếm hit/miss.
    Pattern(PatternStation),
}

impl Station {
    /// Tạo một timeseries station rỗng. `capacity` là tổng số entry (sẽ chia
    /// đều cho 16 shard — phải là bội của 16 để không lãng phí).
    #[must_use]
    pub fn timeseries(capacity: usize) -> Self {
        Self::timeseries_with(capacity, vec![Stage::Raw, Stage::Processed])
    }

    /// Như [`Station::timeseries`] nhưng khai báo rõ station chứa những stage
    /// nào (thứ tự hiển thị trong `describe` theo thứ tự khai báo).
    #[must_use]
    pub fn timeseries_with(capacity: usize, stages: Vec<Stage>) -> Self {
        Station::Timeseries(TimeseriesStation {
            cache: LruCache::new(capacity.next_multiple_of(16)),
            metrics: Mutex::new(HashMap::new()),
            stages,
        })
    }

    /// Append một batch observation vào stage tương ứng.
    ///
    /// Gom theo `metric_id` rồi merge vào `BTreeMap` hiện có (ghi đè theo ts),
    /// cuối cùng `put` lại vào `LruCache` — `LruCache` không có `entry()` nên
    /// dùng get-modify-put.
    pub async fn append(&mut self, stage: Stage, batch: &[Observation]) {
        let Station::Timeseries(ts) = self else {
            return;
        };
        if !ts.stages.contains(&stage) {
            tracing::warn!(
                "append to undeclared stage `{}` (station declares: {})",
                stage.as_str(),
                ts.stages.iter().map(Stage::as_str).collect::<Vec<_>>().join(", ")
            );
        }
        let mut by_metric: HashMap<String, BTreeMap<i64, Observation>> = HashMap::new();
        for obs in batch {
            by_metric
                .entry(obs.metric_id.clone())
                .or_default()
                .insert(obs.ts, obs.clone());
        }
        {
            let mut metrics = ts.metrics.lock().expect("station metrics lock");
            for m in by_metric.keys() {
                metrics.insert(m.clone(), ());
            }
        }
        for (metric, incoming) in by_metric {
            let key = (stage, metric);
            let mut existing = ts.cache.get(&key).unwrap_or_default();
            existing.extend(incoming);
            ts.cache.put(key, existing);
        }
    }

    /// Query một metric cụ thể trong `[from_ts, to_ts]` (exclusive from,
    /// inclusive to) — khớp semantics cũ của store.
    pub async fn query(
        &self,
        stage: Stage,
        metric: &str,
        from_ts: i64,
        to_ts: i64,
    ) -> Vec<Observation> {
        eprintln!("[Q] query stage={:?} metric={} ({},{})", stage, metric, from_ts, to_ts);
        let Station::Timeseries(ts) = self else {
            return Vec::new();
        };
        let Some(map) = ts
            .cache
            .get_with_load(&(stage, metric.to_string()), from_ts as u64, to_ts as u64)
            .await
        else {
            return Vec::new();
        };
        map.range((
            std::ops::Bound::Excluded(from_ts),
            std::ops::Bound::Included(to_ts),
        ))
        .map(|(_, v)| v.clone())
        .collect()
    }

    /// Query mọi metric của station trong `[from_ts, to_ts]`.
    pub async fn query_all(&self, stage: Stage, from_ts: i64, to_ts: i64) -> Vec<Observation> {
        let Station::Timeseries(ts) = self else {
            return Vec::new();
        };
        let metrics: Vec<String> = ts
            .metrics
            .lock()
            .expect("station metrics lock")
            .keys()
            .cloned()
            .collect();
        let mut out = Vec::new();
        for metric in metrics {
            let Some(map) = ts
                .cache
                .get_with_load(&(stage, metric), from_ts as u64, to_ts as u64)
                .await
            else {
                continue;
            };
            out.extend(
                map.range((
                    std::ops::Bound::Excluded(from_ts),
                    std::ops::Bound::Included(to_ts),
                ))
                .map(|(_, v)| v.clone()),
            );
        }
        out
    }

    // ── Pattern (Aho-Corasick) operations ──────────────────────────────────

    /// Thêm một pattern (chỉ có nghĩa trên variant `Pattern`). Tự động
    /// `optimize` lại automaton khi pattern mới được thêm.
    pub async fn add_pattern(&mut self, pattern: &str) {
        let Station::Pattern(p) = self else {
            return;
        };
        let before = p.automaton.pattern_count();
        p.automaton.add(pattern.to_string());
        if p.automaton.pattern_count() > before {
            p.automaton.optimize().await;
        }
    }

    /// Kiểm tra `text` có khớp pattern nào đã biết không (variant `Pattern`).
    /// Trả `None` khi gọi trên variant khác. Cập nhật bộ đếm hit/miss.
    pub async fn is_known(&self, text: &str) -> Option<bool> {
        let Station::Pattern(p) = self else {
            return None;
        };
        p.hits.fetch_add(1, Ordering::Relaxed);
        let matched = p.automaton.similar(&text.to_string()).await;
        if matched {
            p.hits.fetch_add(1, Ordering::Relaxed);
            Some(true)
        } else {
            p.misses.fetch_add(1, Ordering::Relaxed);
            Some(false)
        }
    }

    /// Danh sách pattern đã đăng ký (variant `Pattern`).
    #[must_use]
    pub fn patterns(&self) -> Vec<String> {
        match self {
            Station::Pattern(p) => p.automaton.patterns(),
            _ => Vec::new(),
        }
    }

    /// Thống kê pattern: `(tổng_pattern, hits, misses)`.
    #[must_use]
    pub fn pattern_stats(&self) -> (usize, u64, u64) {
        match self {
            Station::Pattern(p) => (
                p.automaton.pattern_count(),
                p.hits.load(Ordering::Relaxed),
                p.misses.load(Ordering::Relaxed),
            ),
            _ => (0, 0, 0),
        }
    }

    // ── Category (radix + KMP) operations ──────────────────────────────────

    /// Index một cặp key/value (variant `Category`). Idempotent per key.
    pub async fn insert_entry(&mut self, key: &[u8], value: &str) {
        let Station::Category(c) = self else {
            return;
        };
        let idx = c.next_idx.fetch_add(1, Ordering::Relaxed) as usize;
        let _ = c
            .search
            .insert_chain(idx, key, &vec![None; key.len()])
            .await;
        c.entries.lock().expect("category entries lock").insert(
            idx,
            (String::from_utf8_lossy(key).to_string(), value.to_string()),
        );
    }

    /// Substring search trên key đã index → các cặp `(key, value)`.
    pub async fn search_entries(
        &self,
        pattern: &str,
        depth: Option<usize>,
    ) -> Vec<(String, String)> {
        let Station::Category(c) = self else {
            return Vec::new();
        };
        let hits = match c.search.search(pattern.as_bytes(), depth).await {
            Ok(hits) => hits,
            Err(_) => return Vec::new(),
        };
        let entries = c.entries.lock().expect("category entries lock");
        hits.into_iter()
            .filter_map(|(rid, _)| entries.get(&rid).map(|(k, v)| (k.clone(), v.clone())))
            .collect()
    }

    /// Số entry đã index (variant `Category`).
    #[must_use]
    pub fn entry_count(&self) -> usize {
        match self {
            Station::Category(c) => c.entries.lock().expect("category entries lock").len(),
            _ => 0,
        }
    }

    /// Mọi entry đã index, sắp theo key.
    #[must_use]
    pub fn all_entries(&self) -> Vec<(String, String)> {
        match self {
            Station::Category(c) => c
                .entries
                .lock()
                .expect("category entries lock")
                .values()
                .cloned()
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Metadata cho HTTP `/describe` và MCP.
    #[must_use]
    pub fn describe(&self) -> Json {
        match self {
            Station::Timeseries(ts) => {
                let metrics = ts.metrics.lock().expect("station metrics lock").len();
                // Stage hiện đang có ít nhất một điểm trong cache (có thể rỗng
                // vì LRU eviction — khác với `stages` vốn là khai báo tĩnh).
                let populated: Vec<&str> = ts
                    .stages
                    .iter()
                    .filter(|stage| {
                        ts.metrics
                            .lock()
                            .expect("station metrics lock")
                            .keys()
                            .any(|m| ts.cache.get(&(**stage, m.clone())).is_some())
                    })
                    .map(Stage::as_str)
                    .collect();
                serde_json::json!({
                    "backend": "timeseries",
                    "stages": ts.stages.iter().map(Stage::as_str).collect::<Vec<_>>(),
                    "populated_stages": populated,
                    "metrics": metrics,
                })
            }
            Station::Category(_) => serde_json::json!({ "backend": "category" }),
            Station::Pattern(_) => serde_json::json!({ "backend": "pattern" }),
        }
    }
}

impl PatternStation {
    /// Pass-through: `AhoCorasick::add` (dùng bởi `PatternStationTransform`).
    pub fn add(&mut self, pattern: String) {
        self.automaton.add(pattern);
    }

    /// Pass-through: `AhoCorasick::optimize` (async).
    pub async fn optimize(&mut self) {
        self.automaton.optimize().await;
    }

    /// Pass-through: `AhoCorasick::similar` (async).
    pub async fn similar(&self, sample: &String) -> bool {
        self.automaton.similar(sample).await
    }
}

impl CategoryStation {
    /// Pass-through: `Search::insert_chain` (dùng bởi `CategoryStationTransform`).
    pub async fn insert_chain(
        &mut self,
        index: usize,
        key: &[u8],
        metas: &[Option<&[u8]>],
    ) -> Result<(), opsense_libs::search::Error> {
        self.search.insert_chain(index, key, metas).await
    }
}
