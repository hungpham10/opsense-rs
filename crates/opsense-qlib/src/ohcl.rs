use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opsense_libs::jq::JsonQuery;
use opsense_libs::lru::LruCache;
use itertools::izip;
use reqwest_middleware::ClientWithMiddleware;
use crate::candle::CandleStick;
use crate::reload::Reload;
use serde_json::Value;
use tracing::{debug, info};

const INDEXES: [&str; 3] = ["VNINDEX", "HNXINDEX", "VN30"];
const SECONDS_IN_WEEK: i64 = 7 * 24 * 60 * 60;

trait JsonValueExt {
    fn as_f64_lossy(&self) -> f64;
    fn as_i64_lossy(&self) -> i64;
}

impl JsonValueExt for Value {
    fn as_f64_lossy(&self) -> f64 {
        match self {
            Value::Number(n) => n.as_f64().unwrap_or(0.0),
            Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
            _ => 0.0,
        }
    }
    fn as_i64_lossy(&self) -> i64 {
        match self {
            Value::Number(n) => n.as_i64().unwrap_or(0),
            Value::String(s) => s.parse::<i64>().unwrap_or(0),
            _ => 0,
        }
    }
}

// Define the layers from the inside out
#[derive(Clone)]
struct CacheBlock {
    pub candles: Vec<CandleStick>,
    // Khoảng thời gian thực tế mà dữ liệu trong block này đã bao phủ
    // Ví dụ: [start_of_week .. last_sync_time]
    pub covered_range: (i64, i64),
    pub last_updated: u64,
}

type CandleCache = LruCache<i64, CacheBlock, 32>;

// Group by whatever your keys represent (e.g., Symbol and Interval)
type SymbolCacheMap = HashMap<String, CandleCache>;
type ExchangeCacheMap = HashMap<String, SymbolCacheMap>;

#[derive(Clone)]
pub struct QueryCandleSticks {
    client: Arc<ClientWithMiddleware>,
    // Much easier to read:
    caches: Arc<RwLock<ExchangeCacheMap>>,
    timers: Arc<RwLock<HashMap<String, u64>>>,
    mapping: Arc<RwLock<HashMap<String, String>>>,
    profiles: HashMap<String, CompiledProfile>,
    capacity_per_stack: usize,
}

#[derive(Clone)]
struct CompiledProfile {
    queries: [JsonQuery; 6],
    url_template: String,
}

impl Reload for QueryCandleSticks {
    fn reload(&self) -> Result<(), Error> {
        let mut mapping = self.mapping.write().map_err(|error| {
            Error::other(format!("Fail to request to write to `mapping`: {}", error))
        })?;
        let mapping_str = std::env::var("CANDLESTICK_MAPPING").unwrap_or_else(|_| "{}".to_string());

        *mapping = serde_json::from_str(&mapping_str).unwrap_or_default();
        Ok(())
    }

    fn keys(&self) -> Vec<&str> {
        vec!["CANDLESTICK_MAPPING"]
    }
}

impl QueryCandleSticks {
    pub fn new(client: Arc<ClientWithMiddleware>, capacity: usize) -> Result<Self, Error> {
        let mut profiles = HashMap::new();
        let raw_configs = vec![
            (
                "ssi",
                "https://iboard-api.ssi.com.vn/statistics/charts/history?from={from}&to={to}&symbol={stock}&resolution={res}",
                [
                    "data.t[]", "data.o[]", "data.h[]", "data.l[]", "data.c[]", "data.v[]",
                ],
            ),
            (
                "vix",
                " https://xpower.vixs.vn/tvchart/history?resolution={res}&symbol={stock}&from={from}&to={to}",
                [
                    "d[].time",
                    "d[].open",
                    "d[].high",
                    "d[].low",
                    "d[].close",
                    "d[].volume",
                ],
            ),
            (
                "dnse",
                "https://api.dnse.com.vn/chart-api/v2/ohlcs/{kind}?from={from}&to={to}&symbol={stock}&resolution={res}",
                ["t[]", "o[]", "h[]", "l[]", "c[]", "v[]"],
            ),
            (
                "dragon",
                "https://godragon.vdsc.com.vn/IdragonMarketDataServer/trading-view/rest/history?symbol={stock}&resolution={res}&from={from}&to={to}&countback={limit}",
                ["t[]", "o[]", "h[]", "l[]", "c[]", "v[]"],
            ),
            (
                "binance",
                "https://api.binance.com/api/v3/klines?startTime={from}&endTime={to}&symbol={stock}&interval={res}&limit={limit}",
                ["[].0", "[].1", "[].2", "[].3", "[].4", "[].5"],
            ),
            (
                "msn",
                "https://assets.msn.com/service/MSNFinance/Quotes/Chart?apikey=0Q_697_8_Z_S_1_1&ocid=finance-utils-peregrine&symbol={stock}&interval={res}&period={limit}",
                // MSN trả về mảng series.dataPoints, mỗi điểm là một mảng: [time, open, high, low, close, volume]
                [
                    "series.dataPoints[].0",
                    "series.dataPoints[].1",
                    "series.dataPoints[].2",
                    "series.dataPoints[].3",
                    "series.dataPoints[].4",
                    "series.dataPoints[].5",
                ],
            ),
            (
                "yahoo",
                "https://query1.finance.yahoo.com/v8/finance/chart/{stock}?interval={res}&period1={from}&period2={to}",
                // Yahoo trả về cấu trúc: chart.result.indicators.quote
                [
                    "chart.result.timestamp[]",
                    "chart.result.indicators.quote.open[]",
                    "chart.result.indicators.quote.high[]",
                    "chart.result.indicators.quote.low[]",
                    "chart.result.indicators.quote.close[]",
                    "chart.result.indicators.quote.volume[]",
                ],
            ),
            (
                "simplefx",
                "https://candles.simplefx.com/api/v3/candles?symbol={stock}&cPeriod={res}&timeFrom={from}&timeTo={to}",
                // SimpleFX trả về object data chứa mảng các candle objects
                [
                    "data[].time",
                    "data[].open",
                    "data[].high",
                    "data[].low",
                    "data[].close",
                    "data[].size",
                ],
            ),
            (
                "findaily",
                "https://findaily.vn/api/investing/v1/ohcl/candles/{provider}/{stock}?resolution={res}&from={from}&to={to}&limit={limit}",
                [
                    "ohcl[].t", "ohcl[].o", "ohcl[].h", "ohcl[].l", "ohcl[].c", "ohcl[].v",
                ],
            ),
        ];

        let mapping_str = std::env::var("CANDLESTICK_MAPPING").unwrap_or_else(|_| "{}".to_string());
        let mapping = Arc::new(RwLock::new(
            serde_json::from_str(&mapping_str).unwrap_or_default(),
        ));

        for (name, url, paths) in raw_configs {
            let queries = [
                JsonQuery::parse(paths[0])?,
                JsonQuery::parse(paths[1])?,
                JsonQuery::parse(paths[2])?,
                JsonQuery::parse(paths[3])?,
                JsonQuery::parse(paths[4])?,
                JsonQuery::parse(paths[5])?,
            ];
            profiles.insert(
                name.to_string(),
                CompiledProfile {
                    url_template: url.into(),
                    queries,
                },
            );
        }

        Ok(Self {
            client,
            mapping,
            caches: Arc::new(RwLock::new(HashMap::new())),
            timers: Arc::new(RwLock::new(HashMap::new())),
            profiles,
            capacity_per_stack: capacity,
        })
    }

    pub async fn get_candlesticks(
        &self,
        profile_name: &str,
        stock: &str,
        resolution: &str,
        from: i64,
        to: i64,
        limit: usize,
    ) -> Result<Vec<CandleStick>, Error> {
        self.get_candlesticks_with_url_provider(
            profile_name,
            stock,
            resolution,
            from,
            to,
            limit,
            None,
        )
        .await
    }

    /// Like `get_candlesticks` nhưng cho phép chỉ định `url_provider` riêng
    /// cho `{provider}` trong URL template (khi profile name khác với URL provider).
    #[allow(clippy::too_many_arguments)]
    pub async fn get_candlesticks_with_url_provider(
        &self,
        profile_name: &str,
        stock: &str,
        resolution: &str,
        from: i64,
        to: i64,
        limit: usize,
        url_provider: Option<&str>,
    ) -> Result<Vec<CandleStick>, Error> {
        if profile_name.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "Provider not specified"));
        }

        // Resolve URL provider: explicit > mapping > profile_name
        let resolved_url_provider = match url_provider {
            Some(p) => p.to_string(),
            None => {
                if let Ok(mapping) = self.mapping.read() {
                    mapping
                        .get(profile_name)
                        .cloned()
                        .unwrap_or_else(|| profile_name.to_string())
                } else {
                    profile_name.to_string()
                }
            }
        };

        if !self.is_invalidated(stock, resolution)
            && let Some(cached_data) = self.fetch_from_cache(stock, resolution, from, to)
        {
            return Ok(cached_data);
        }

        let fetched_candles = self
            .fetch_from_api(
                profile_name,
                stock,
                resolution,
                from,
                to,
                limit,
                Some(&resolved_url_provider),
            )
            .await?;

        self.update_cache(stock, resolution, &fetched_candles, from, to)?;
        Ok(fetched_candles)
    }

    fn fetch_from_cache(
        &self,
        stock: &str,
        resolution: &str,
        from: i64,
        to: i64,
    ) -> Option<Vec<CandleStick>> {
        let caches = self.caches.read().unwrap();
        let stock_cache = caches.get(stock)?.get(resolution)?;

        let start_block = from / SECONDS_IN_WEEK;
        let end_block = to / SECONDS_IN_WEEK;
        let mut result = Vec::new();

        for block_id in start_block..=end_block {
            if let Some(block) = stock_cache.get(&block_id) {
                let block_start = block_id * SECONDS_IN_WEEK;
                let block_end = (block_id + 1) * SECONDS_IN_WEEK;

                let needed_start = from.max(block_start);
                let needed_end = to.min(block_end);

                if needed_start >= block.covered_range.0 && needed_end <= block.covered_range.1 {
                    debug!(
                        block_id,
                        range = ?block.covered_range,
                        "Block coverage hit"
                    );

                    for c in &block.candles {
                        let ts = c.t;
                        if ts >= needed_start && ts <= needed_end {
                            result.push(*c);
                        }
                    }
                } else {
                    info!(
                        block_id,
                        needed = ?(needed_start, needed_end),
                        covered = ?block.covered_range,
                        "Cache miss: Range not covered"
                    );
                    return None;
                }
            } else {
                info!(block_id, "Cache miss: Block not found in LRU");
                return None;
            }
        }

        if result.is_empty() && from < to {
            return Some(vec![]);
        }

        result.sort_by_key(|c| c.t);
        result.dedup_by_key(|c| c.t);
        Some(result)
    }

    fn update_cache(
        &self,
        stock: &str,
        resolution: &str,
        candles: &[CandleStick],
        query_from: i64,
        query_to: i64,
    ) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::other(e.to_string()))?
            .as_secs();

        let mut caches = self.caches.write().unwrap();
        let stock_entry = caches.entry(stock.to_string()).or_default();
        let lru = stock_entry
            .entry(resolution.to_string())
            .or_insert_with(|| LruCache::new(self.capacity_per_stack));

        // 1. Nhóm nến theo tuần (block)
        let mut groups: HashMap<i64, Vec<CandleStick>> = HashMap::new();
        for c in candles {
            let bid = c.t / SECONDS_IN_WEEK;
            groups.entry(bid).or_default().push(*c);
        }

        // 2. Cập nhật từng block bị ảnh hưởng bởi query_from -> query_to
        let start_bid = query_from / SECONDS_IN_WEEK;
        let end_bid = query_to / SECONDS_IN_WEEK;

        for bid in start_bid..=end_bid {
            let mut block = lru.get(&bid).unwrap_or(CacheBlock {
                candles: vec![],
                covered_range: (i64::MAX, i64::MIN),
                last_updated: now,
            });

            // Nếu có nến mới cho block này thì merge vào
            if let Some(new_cands) = groups.get_mut(&bid) {
                block.candles.append(new_cands);
                block.candles.sort_by_key(|c| c.t);
                block.candles.dedup_by_key(|c| c.t);
            }

            // Cập nhật độ phủ (Coverage) cho block này
            // Độ phủ của block chỉ giới hạn trong biên giới của block đó
            let block_start = bid * SECONDS_IN_WEEK;
            let block_end = (bid + 1) * SECONDS_IN_WEEK;

            let effective_from = query_from.max(block_start);
            let effective_to = query_to.min(block_end);

            block.covered_range.0 = block.covered_range.0.min(effective_from);
            block.covered_range.1 = block.covered_range.1.max(effective_to);
            block.last_updated = now;

            lru.put(bid, block);
        }

        // 3. Cập nhật timer chung cho cặp Stock:Resolution
        self.timers
            .write()
            .unwrap()
            .insert(format!("{}:{}", stock, resolution), now);
        Ok(())
    }

    fn is_invalidated(&self, stock: &str, resolution: &str) -> bool {
        let timers = self.timers.read().unwrap();
        let key = format!("{}:{}", stock, resolution);

        match timers.get(&key) {
            Some(&last_update) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let res_upper = resolution.to_uppercase();
                let ttl = match res_upper.as_str() {
                    "1" | "5" => 60,
                    "1D" | "1W" | "1M" => 3600,
                    _ => 300,
                };

                if now < last_update {
                    return false;
                }

                now - last_update > ttl
            }
            None => true,
        }
    }

    /// `url_provider` — giá trị thay thế `{provider}` trong URL template.
    /// `None` = dùng `profile_name`.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_from_api(
        &self,
        profile_name: &str,
        stock: &str,
        resolution: &str,
        from: i64,
        to: i64,
        limit: usize,
        url_provider: Option<&str>,
    ) -> Result<Vec<CandleStick>, Error> {
        let kind = if INDEXES.contains(&stock) {
            "index"
        } else {
            "stock"
        };
        let profile = self
            .profiles
            .get(profile_name)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Provider not found"))?;

        if limit > 1000 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "`limit` mustn't be larger than 1000",
            ));
        }

        // Nếu url_provider không được chỉ định, dùng profile_name cho {provider}
        let url_provider_val = url_provider.unwrap_or(profile_name);

        let (adj_from, adj_to) = if profile_name == "binance" {
            (from * 1000, to * 1000)
        } else {
            (from, to)
        };
        let url = profile
            .url_template
            .replace("{kind}", kind)
            .replace("{stock}", stock)
            .replace("{provider}", url_provider_val)
            .replace("{res}", resolution)
            .replace("{from}", &adj_from.to_string())
            .replace("{to}", &adj_to.to_string())
            .replace("{limit}", &limit.to_string());

        if url.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid URL template"));
        }

        let resp = self
            .client
            .get(url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| {
                Error::other(format!("Failed to fetch data from {}: {}", profile_name, e))
            })?;

        let raw_json: Value = resp.json().await.map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Failed to parse JSON: {}", e),
            )
        })?;

        let t_ref = profile.queries[0].pick(&raw_json);
        if t_ref.is_empty() {
            return Ok(vec![]);
        }

        let o_ref = profile.queries[1].pick(&raw_json);
        let h_ref = profile.queries[2].pick(&raw_json);
        let l_ref = profile.queries[3].pick(&raw_json);
        let c_ref = profile.queries[4].pick(&raw_json);
        let v_ref = profile.queries[5].pick(&raw_json);

        let count = if limit > 0 {
            limit.min(t_ref.len())
        } else {
            t_ref.len()
        };
        let mut candles = Vec::with_capacity(count);

        for (t, o, h, l, c, v) in izip!(t_ref, o_ref, h_ref, l_ref, c_ref, v_ref).take(count) {
            candles.push(CandleStick {
                t: if profile_name == "binance" {
                    t.as_i64().unwrap_or(0) / 1000
                } else {
                    t.as_i64_lossy()
                },
                o: o.as_f64_lossy(),
                h: h.as_f64_lossy(),
                l: l.as_f64_lossy(),
                c: c.as_f64_lossy(),
                v: v.as_f64_lossy(),
            });
        }

        Ok(candles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client as HttpClient;
    use reqwest_middleware::ClientBuilder;
    use reqwest_tracing::TracingMiddleware;
    use serde_json::json;
    use std::time::Instant;

    async fn run_provider_test(
        service: &QueryCandleSticks,
        provider: &str,
        stock: &str,
        res: &str,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let from = now - (7 * 24 * 60 * 60); // 7 ngày trước
        let to = now;

        println!("\n🔍 Testing Provider: [{}] - Symbol: {}", provider, stock);

        // --- Lần 1: Gọi API thật ---
        let start_api = Instant::now();
        let result_api = service
            .get_candlesticks(provider, stock, res, from, to, 50)
            .await;

        match result_api {
            Ok(candles) => {
                let dur_api = start_api.elapsed();
                assert!(!candles.is_empty(), "Dữ liệu từ {} trả về rỗng!", provider);
                println!("✅ Lần 1 (API)  : {} nến - {:?}", candles.len(), dur_api);

                // --- Lần 2: Truy vấn lại (Cache) ---
                let start_cache = Instant::now();
                let result_cache = service
                    .get_candlesticks(provider, stock, res, from, to, 50)
                    .await
                    .unwrap();
                let _ = start_cache.elapsed();

                // Thay vì assert_eq!(candles.len(), result_cache.len(), ...);
                assert!(
                    result_cache.len() >= candles.len(),
                    "🚨 Cache thiếu nến! API: {}, Cache: {}",
                    candles.len(),
                    result_cache.len()
                );

                // Kiểm tra xem nến mới nhất có khớp nhau không (để đảm bảo không lấy nến rác)
                assert_eq!(
                    candles.last().unwrap().t,
                    result_cache.last().unwrap().t,
                    "Timestamp nến cuối bị lệch!"
                );
            }
            Err(e) => panic!("❌ Thất bại khi gọi sàn {}: {:?}", provider, e),
        }
    }

    // Helper tạo Mock Response cho SSI
    fn mock_ssi_data(size: usize) -> Value {
        let mut t = Vec::with_capacity(size);
        let mut o = Vec::with_capacity(size);
        for i in 0..size {
            t.push(1700000000 + i as i64);
            o.push(100.0 + i as f64);
        }
        json!({
            "data": {
                "t": t, "o": o, "h": o, "l": o, "c": o, "v": o
            }
        })
    }

    #[test]
    fn test_profile_initialization() {
        let client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let service = QueryCandleSticks::new(client, 70).unwrap();

        assert!(service.profiles.contains_key("ssi"));
        assert!(service.profiles.contains_key("binance"));
    }

    #[test]
    fn test_logic_extraction_ssi() {
        let client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let service = QueryCandleSticks::new(client, 70).unwrap();
        let data = mock_ssi_data(10);

        let profile = service.profiles.get("ssi").unwrap();
        let t_ref = profile.queries[0].pick(&data);

        assert_eq!(t_ref.len(), 10);
        assert_eq!(t_ref[0].as_i64_lossy(), 1700000000);
    }

    #[test]
    fn test_logic_extraction_binance() {
        let client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let service = QueryCandleSticks::new(client, 70).unwrap();

        // Binance format: [[t, o, h, l, c, v], ...]
        let data = json!([
            [170000000i32, "100.5", "101.0", "99.0", "100.8", "5000"],
            [170000000i32, "100.8", "102.0", "100.5", "101.5", "6000"]
        ]);

        let profile = service.profiles.get("binance").unwrap();
        let t_ref = profile.queries[0].pick(&data);
        let o_ref = profile.queries[1].pick(&data);

        assert_eq!(t_ref.len(), 2);
        assert_eq!(o_ref[0].as_f64_lossy(), 100.5);
        // Binance timestamp is ms, our lossy converter handles it
        assert_eq!(t_ref[0].as_i64_lossy(), 170000000);
    }

    #[tokio::test]
    async fn test_cache_block_logic() {
        let client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let service = QueryCandleSticks::new(client, 10).unwrap();

        let stock = "FPT";
        let res = "1D";

        // t1 thuộc Block A, t2 thuộc Block B (cách nhau 1 tuần)
        let t1 = 1700000000;
        let t2 = 1700000000 + SECONDS_IN_WEEK + 100;

        let candles = vec![
            CandleStick {
                t: t1,
                o: 10.0,
                h: 11.0,
                l: 9.0,
                c: 10.5,
                v: 1000.0,
            },
            CandleStick {
                t: t2,
                o: 20.0,
                h: 21.0,
                l: 19.0,
                c: 20.5,
                v: 2000.0,
            },
        ];

        // 1. Update cache: Giả sử query API từ (t1 - 1000) đến (t2 + 1000)
        let query_from = t1 - 1000;
        let query_to = t2 + 1000;
        service
            .update_cache(stock, res, &candles, query_from, query_to)
            .unwrap();

        // 2. Test hit hoàn toàn trong vùng đã phủ của Block A
        // Query nằm trong khoảng [query_from, query_to] nên phải HIT
        let hit = service.fetch_from_cache(stock, res, t1, t1 + 100);
        assert!(hit.is_some(), "Phải hit được vì nằm trong covered_range");
        assert_eq!(hit.unwrap().len(), 1);

        // 3. Test hit xuyên 2 blocks
        let hit_all = service.fetch_from_cache(stock, res, t1, t2);
        assert!(hit_all.is_some(), "Phải hit được cả 2 block");
        assert_eq!(hit_all.unwrap().len(), 2);

        // 4. Test miss do nằm ngoài covered_range (Dù vùng này có thể cùng Block ID)
        // Vùng này chưa được update_cache quét qua nên phải trả về None để gọi API
        let miss_outside = service.fetch_from_cache(stock, res, query_from - 5000, query_from - 1);
        assert!(
            miss_outside.is_none(),
            "Phải miss vì vùng này chưa được phủ (mặc dù có thể cùng block)"
        );

        // 5. Test hit vùng KHÔNG có nến nhưng ĐÃ phủ (ví dụ giữa t1 và t2)
        // Đây là điểm mạnh của logic mới: Trả về Some(empty) thay vì None
        let hit_empty = service.fetch_from_cache(stock, res, t1 + 10, t1 + 20);
        assert!(hit_empty.is_some(), "Phải hit (Some) vì đã được quét qua");
        assert_eq!(hit_empty.unwrap().len(), 0, "Vùng này không có nến thực tế");
    }

    #[tokio::test]
    async fn test_cache_invalidation_ttl() {
        let client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let service = QueryCandleSticks::new(client, 10).unwrap();
        let stock = "VIC";
        let res = "1";

        let t_base = 1700000000;
        let mock_candle = CandleStick {
            t: t_base,
            o: 10.0,
            h: 10.0,
            l: 10.0,
            c: 10.0,
            v: 0.0,
        };

        // Cập nhật cache với vùng phủ từ t_base - 60 đến t_base + 60
        service
            .update_cache(stock, res, &[mock_candle], t_base - 60, t_base + 60)
            .unwrap();

        // Kiểm tra ngay lập tức - Phải FALSE (valid) vì vừa mới update timer
        assert!(
            !service.is_invalidated(stock, res),
            "Vừa update xong timer phải còn valid (chưa quá TTL)!"
        );

        // Tiện thể test luôn fetch_from_cache tại đây để đảm bảo logic coverage hoạt động
        let hit = service.fetch_from_cache(stock, res, t_base, t_base);
        assert!(hit.is_some(), "Dữ liệu phải tồn tại trong vùng đã phủ");
    }

    #[tokio::test]
    #[ignore]
    async fn test_all_providers_real_data() {
        let client = Arc::new(
            ClientBuilder::new(HttpClient::new())
                .with(TracingMiddleware::default())
                .build(),
        );
        let service = QueryCandleSticks::new(client, 100).unwrap();

        // Chạy lần lượt các sàn
        // SSI
        run_provider_test(&service, "ssi", "FPT", "1D").await;

        // DSNE
        run_provider_test(&service, "dnse", "HPG", "1D").await;

        // VIX
        run_provider_test(&service, "vix", "HPG", "1D").await;

        // Binance - Dùng BTCUSDT
        run_provider_test(&service, "binance", "BTCUSDT", "1h").await;
        run_provider_test(&service, "binance", "BTCUSDT", "1d").await;
    }
}
