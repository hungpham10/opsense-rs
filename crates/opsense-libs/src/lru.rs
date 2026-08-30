use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use crate::storage::TimeseriesStorage;

const NULL: usize = usize::MAX;

/// Boxed, owned, `'static` future used by the `fallback` read-through callback.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Nguồn gốc để re-fetch dữ liệu khi cả cache lẫn đĩa đều hụt (tầng 3 của
/// read-through). Thay vì truyền một closure, gọi viên đóng gói thành trait này
/// để http_source (và bất kỳ nguồn nào) cung cấp impl rõ ràng, dễ test.
pub trait OriginSource<K, V>: Send + Sync {
    fn fetch(&self, key: &K, from_ts: u64, to_ts: u64) -> BoxFuture<Result<V, String>>;
}

/// Blanket impl: mọi `Fn(&K, u64, u64) -> BoxFuture<Result<V, String>>` đều là
/// một `OriginSource`, nên các caller cũ truyền closure vẫn biên dịch được.
impl<K, V, F> OriginSource<K, V> for F
where
    F: Fn(&K, u64, u64) -> BoxFuture<Result<V, String>> + Send + Sync + 'static,
{
    fn fetch(&self, key: &K, from_ts: u64, to_ts: u64) -> BoxFuture<Result<V, String>> {
        self(key, from_ts, to_ts)
    }
}

/// Callback tính cửa sổ thực sự hổng giữa `[from_ts, to_ts]` của một value đã
/// có trong cache — trả `Some((gap_from, gap_to))` để chỉ fetch phần thiếu,
/// hoặc `None` khi cache đã phủ đủ (hổng chỉ nằm trong tương lai).
type CoverageGap<K, V> =
    Arc<dyn Fn(&K, &V, u64, u64) -> Option<(u64, u64)> + Send + Sync>;

// --- CẤU TRÚC DỮ LIỆU ---

struct Node<K, V> {
    key: Option<K>,
    value: Option<V>,
    next: AtomicUsize,
    prev: AtomicUsize,
}

struct HeadTail {
    first: usize,
    last: usize,
}

/// AlignedShard giúp mỗi Mutex nằm riêng trên một Cache Line (64 bytes).
/// Điều này loại bỏ hiện tượng False Sharing, giúp tăng tốc ghi đa luồng.
#[repr(align(64))]
struct AlignedShard {
    mutex: Mutex<HeadTail>,
}

pub struct LruCache<K, V, const S: usize> {
    mapping: DashMap<K, usize>,
    caching: Box<[Node<K, V>]>,
    shards: [AlignedShard; S],
    shard_mask: usize,
    pub on_removing: Option<Arc<dyn Fn(K, V) + Send + Sync>>,
    pub on_updating: Option<Arc<dyn Fn(K, V) + Send + Sync>>,
    /// Persistence layer (optional). Khi được gắn, mỗi entry bị **evict** (do
    /// shard đầy) hoặc **update** (ghi đè key cũ) sẽ được append vào
    /// `TimeseriesStorage` dưới dạng điểm `(timestamp, value)`.
    pub timeseries: Option<Arc<dyn TimeseriesStorage>>,
    /// Map key → series name (opaque bytes). Quyết định entry ghi vào series nào.
    pub ts_series_of: Option<Arc<dyn Fn(&K) -> Vec<u8> + Send + Sync>>,
    /// Serialize value → opaque bytes lưu vào timeseries.
    pub ts_encode: Option<Arc<dyn Fn(&V) -> Vec<u8> + Send + Sync>>,
    /// Timestamp source (ms). Mặc định: `SystemTime::now()`.
    pub ts_clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    /// Decode opaque storage bytes về lại `V` (encode dùng lại `ts_encode`).
    pub decode: Option<Arc<dyn Fn(&[u8]) -> Option<V> + Send + Sync>>,
    /// Validate độ phủ của một value (cache hoặc đĩa) cho cửa sổ yêu cầu.
    /// Trả `true` khi dữ liệu đủ/đáng tin. `None` → mặc định coi là đủ (như
    /// hành vi cũ). Khi trả `false`, entry được coi là miss và đi tiếp xuống
    /// tầng đĩa / origin.
    pub validate: Option<Arc<dyn Fn(&K, &V, u64, u64) -> bool + Send + Sync>>,
    /// Tính cửa sổ thực sự hổng (chưa có dữ liệu) giữa `[from_ts, to_ts]` của
    /// một value đã có trong cache. Trả `Some((gap_from, gap_to))` để chỉ fetch
    /// phần thiếu, hoặc `None` khi cache đã phủ đủ (hổng chỉ nằm trong tương
    /// lai, không thể fetch). Chỉ dùng khi có `fallback`/`storage`.
    pub coverage_gap:
        Option<Arc<dyn Fn(&K, &V, u64, u64) -> Option<(u64, u64)> + Send + Sync>>,
    /// Nguồn gốc để re-fetch khi cả cache lẫn đĩa đều miss (tầng 3 read-through).
    /// Là `Arc<dyn OriginSource>` — xem [`OriginSource`].
    pub fallback: Option<Arc<dyn OriginSource<K, V>>>,
    /// Gộp slice vừa fetch từ origin với value đang có trong cache khi fallback
    /// thành công. `None` → value fetch được ghi đè nguyên khối (hành vi cũ).
    /// Station gán hook này để backfill quá khứ không xoá mất các điểm mới hơn
    /// đã có trong cache.
    pub ts_merge: Option<Arc<dyn Fn(&V, V) -> V + Send + Sync>>,
    /// Timestamp extractor cho điểm được persist xuống đĩa: từ entry sinh ra
    /// `u64` làm `ts` của point. Khi `None`, dùng `ts_clock` (mặc định
    /// wall-clock ms). Station gán callback này trả **ts quan sát mới nhất**
    /// để các điểm trên đĩa căn lề với cửa sổ request (tính bằng giây, không
    /// phải ms).
    pub ts_timestamp_of: Option<Arc<dyn Fn(&K, &V) -> u64 + Send + Sync>>,
}

impl<K, V, const S: usize> fmt::Debug for LruCache<K, V, S>
where
    K: fmt::Debug + std::hash::Hash + Eq,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LruCache")
            .field("mapping", &self.mapping)
            .field("caching_len", &self.caching.len())
            .field("shard_mask", &self.shard_mask)
            .field("on_removing", &self.on_removing.as_ref().map(|_| "Closure"))
            .field("on_updating", &self.on_updating.as_ref().map(|_| "Closure"))
            .finish()
    }
}
// --- IMPLEMENTATION ---

impl<K, V, const S: usize> LruCache<K, V, S>
where
    K: Clone + Hash + Eq + Send + Sync,
    V: Clone + Send + Sync,
{
    pub fn new(total_capacity: usize) -> Self {
        // S phải là lũy thừa của 2 để dùng bitwise AND thay cho phép chia lấy dư (%)
        assert!(
            S > 0 && S.is_power_of_two(),
            "SHARD_COUNT (S) phải là lũy thừa của 2 (ví dụ: 8, 16, 32)"
        );

        let capacity_per_shard = total_capacity.div_ceil(S);
        let actual_total = capacity_per_shard * S;

        // 1. Khởi tạo Arena bộ nhớ phẳng
        let mut caching_vec = Vec::with_capacity(actual_total);
        for shard_idx in 0..S {
            let offset = shard_idx * capacity_per_shard;
            for i in 0..capacity_per_shard {
                let current = offset + i;
                caching_vec.push(Node {
                    key: None,
                    value: None,
                    next: AtomicUsize::new(if i + 1 < capacity_per_shard {
                        current + 1
                    } else {
                        NULL
                    }),
                    prev: AtomicUsize::new(if i > 0 { current - 1 } else { NULL }),
                });
            }
        }

        // 2. Khởi tạo mảng các Shard Mutex (đã được aligned)
        let shards = std::array::from_fn(|i| {
            let offset = i * capacity_per_shard;
            AlignedShard {
                mutex: Mutex::new(HeadTail {
                    first: if capacity_per_shard > 0 { offset } else { NULL },
                    last: if capacity_per_shard > 0 {
                        offset + capacity_per_shard - 1
                    } else {
                        NULL
                    },
                }),
            }
        });

        Self {
            mapping: DashMap::with_capacity(actual_total),
            caching: caching_vec.into_boxed_slice(),
            shards,
            shard_mask: S - 1,
            on_removing: None,
            on_updating: None,
            timeseries: None,
            ts_series_of: None,
            ts_encode: None,
            ts_clock: None,
            decode: None,
            validate: None,
            coverage_gap: None,
            fallback: None,
            ts_merge: None,
            ts_timestamp_of: None,
        }
    }

    #[inline]
    pub fn get_shard_idx(&self, key: &K) -> usize {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) & self.shard_mask
    }

    /// Gắn một `TimeseriesStorage`: mỗi khi một entry bị **evict** (do shard đầy)
    /// hoặc **update** (ghi đè key cũ), snapshot `(timestamp, value)` của nó sẽ
    /// được append vào series tương ứng với key đó.
    ///
    /// - `series_of` map `&K → series name` (bytes). Nếu trả về rỗng (`b""`)
    ///   thì điểm đó bị bỏ qua (không ghi timeseries).
    /// - `encode` serialize `&V → bytes` (VD: `serde`, `bincode`, hoặc format
    ///   thủ công). Backend timeseries chỉ lưu opaque bytes.
    /// - `clock` cung cấp timestamp (ms). Mặc định `SystemTime::now()`.
    ///
    /// Ghi là **best-effort**: lỗi storage không làm fail `put`/`remove` của cache.
    pub fn attach_timeseries(
        &mut self,
        ts: Arc<dyn TimeseriesStorage>,
        series_of: Arc<dyn Fn(&K) -> Vec<u8> + Send + Sync>,
        encode: Arc<dyn Fn(&V) -> Vec<u8> + Send + Sync>,
    ) {
        self.timeseries = Some(ts);
        self.ts_series_of = Some(series_of);
        self.ts_encode = Some(encode);
    }

    /// Gắn một `TimeseriesStorage` kèm custom clock (tiện cho test / định dạng
    /// timestamp không phải ms). Xem [`Self::attach_timeseries`].
    pub fn attach_timeseries_with_clock(
        &mut self,
        ts: Arc<dyn TimeseriesStorage>,
        series_of: Arc<dyn Fn(&K) -> Vec<u8> + Send + Sync>,
        encode: Arc<dyn Fn(&V) -> Vec<u8> + Send + Sync>,
        clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) {
        self.timeseries = Some(ts);
        self.ts_series_of = Some(series_of);
        self.ts_encode = Some(encode);
        self.ts_clock = Some(clock);
    }

    /// Gỡ TimeseriesStorage (ngắt persistence).
    pub fn detach_timeseries(&mut self) {
        self.timeseries = None;
        self.ts_series_of = None;
        self.ts_encode = None;
        self.ts_clock = None;
    }

    /// Best-effort persistence hook: khi một `TimeseriesStorage` được gắn, toàn
    /// bộ entry `(timestamp, value)` được append vào series tương ứng mỗi khi
    /// entry bị **evict** (shard đầy), **update** (ghi đè), hoặc **remove**.
    ///
    /// Ghi là fire-and-forget: được spawn trên tokio runtime hiện tại, lỗi bị
    /// bỏ qua nên không bao giờ làm fail `put`/`remove`. Không có runtime (VD
    /// `#[test]` trần), ghi bị bỏ qua — persistence chỉ có ý nghĩa dưới async
    /// runtime điều khiển pipeline.
    fn persist_point(&self, key: &K, value: &V) {
        let (Some(storage), Some(series_of), Some(encode)) = (
            &self.timeseries,
            &self.ts_series_of,
            &self.ts_encode,
        ) else {
            return;
        };
        let series = series_of(key);
        let bytes = encode(value);
        let ts = match &self.ts_timestamp_of {
            Some(f) => f(key, value),
            None => self.clock_value(),
        };
        let storage = Arc::clone(storage);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = storage.append(&series, ts, &bytes).await;
            });
        }
    }

    #[inline]
    fn clock_value(&self) -> u64 {
        match &self.ts_clock {
            Some(c) => c(),
            None => SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    /// Gắn toàn bộ read-through tier: storage + encode/decode + callback
    /// `validate` coverage + future `fallback` origin. Sau khi gắn,
    /// `get_with_load` tự động reload từ đĩa và re-fetch từ origin khi cache
    /// miss / hổng coverage.
    pub fn attach_fallback(
        &mut self,
        ts: Arc<dyn TimeseriesStorage>,
        series_of: Arc<dyn Fn(&K) -> Vec<u8> + Send + Sync>,
        encode: Arc<dyn Fn(&V) -> Vec<u8> + Send + Sync>,
        decode: Arc<dyn Fn(&[u8]) -> Option<V> + Send + Sync>,
        validate: Arc<dyn Fn(&K, &V, u64, u64) -> bool + Send + Sync>,
        coverage_gap: CoverageGap<K, V>,
        fallback: Arc<dyn OriginSource<K, V>>,
    ) where
        V: 'static,
    {
        self.timeseries = Some(ts);
        self.ts_series_of = Some(series_of);
        self.ts_encode = Some(encode);
        self.decode = Some(decode);
        self.validate = Some(validate);
        self.coverage_gap = Some(coverage_gap);
        self.fallback = Some(fallback);
    }

    /// Gắn storage + encode/decode + `validate` nhưng KHÔNG có origin fallback.
    /// `get_with_load` sẽ reload từ đĩa khi miss, nhưng trả `None` khi cả cache
    /// lẫn đĩa đều không phủ cửa sổ (không có `fallback` để gọi).
    pub fn attach_storage(
        &mut self,
        ts: Arc<dyn TimeseriesStorage>,
        series_of: Arc<dyn Fn(&K) -> Vec<u8> + Send + Sync>,
        encode: Arc<dyn Fn(&V) -> Vec<u8> + Send + Sync>,
        decode: Arc<dyn Fn(&[u8]) -> Option<V> + Send + Sync>,
        validate: Arc<dyn Fn(&K, &V, u64, u64) -> bool + Send + Sync>,
        coverage_gap: CoverageGap<K, V>,
    ) where
        V: 'static,
    {
        self.timeseries = Some(ts);
        self.ts_series_of = Some(series_of);
        self.ts_encode = Some(encode);
        self.decode = Some(decode);
        self.validate = Some(validate);
        self.coverage_gap = Some(coverage_gap);
        // Không có origin: giữ `fallback` là None → get_with_load trả về None.
    }

    /// Gỡ bỏ read-through tier (storage + callbacks). Persistence và fallback
    /// dừng; cache giữ nguyên các entry trong RAM.
    pub fn detach_fallback(&mut self) {
        self.timeseries = None;
        self.ts_series_of = None;
        self.ts_encode = None;
        self.ts_clock = None;
        self.decode = None;
        self.validate = None;
        self.fallback = None;
        self.ts_timestamp_of = None;
    }

    /// Đọc value theo key.
    pub fn get(&self, key: &K) -> Option<V> {
        let index = *self.mapping.get(key)?;

        // Đọc giá trị an toàn (Node này chắc chắn tồn tại vì mapping đang giữ nó)
        let val = self.caching[index].value.as_ref()?.clone();

        // Optimistic LRU Update: Dùng try_lock để không làm chậm luồng Read
        let shard_idx = self.get_shard_idx(key);
        if let Ok(mut ht) = self.shards[shard_idx].mutex.try_lock() {
            self.move_to_front_inside_lock(&mut ht, index);
        }

        Some(val)
    }

    /// Ghi (hoặc cập nhật) key.
    pub fn put(&self, key: K, value: V) {
        let shard_idx = self.get_shard_idx(&key);

        // Case 1: Key đã tồn tại (Update)
        if let Some(entry) = self.mapping.get_mut(&key) {
            if let Some(cb) = &self.on_updating {
                cb(key.clone(), value.clone());
            }
            let index = *entry.value();
            drop(entry); // thả guard DashMap

            // MỚI: persist entry bị update ra timeseries (best-effort) — trước
            // khi `value` bị move vào node ở dưới.
            self.persist_point(&key, &value);

            unsafe {
                let node_ptr = &self.caching[index] as *const Node<K, V> as *mut Node<K, V>;
                (*node_ptr).value = Some(value);
            }

            // Cập nhật thứ tự (Có thể dùng try_lock hoặc lock tùy độ ưu tiên)
            if let Ok(mut ht) = self.shards[shard_idx].mutex.try_lock() {
                self.move_to_front_inside_lock(&mut ht, index);
            }
            return;
        }

        // Case 2: Ghi mới (Bắt buộc dùng lock cứng để bảo vệ tính nhất quán)
        let mut ht = self.shards[shard_idx].mutex.lock().unwrap();
        let last_idx = ht.last;
        if last_idx == NULL {
            return;
        }

        let node = &self.caching[last_idx];

        // Đuổi dữ liệu cũ nếu có — giữ snapshot để persist ra ngoài lock
        let evicted = node.key.as_ref().map(|old_key| {
            let old_val = node.value.as_ref().unwrap().clone();
            self.mapping.remove(old_key);
            if let Some(cb) = &self.on_removing {
                cb(old_key.clone(), old_val.clone());
            }
            (old_key.clone(), old_val)
        });

        // Ghi dữ liệu mới vào Node cuối của Shard
        unsafe {
            let node_ptr = node as *const Node<K, V> as *mut Node<K, V>;
            (*node_ptr).key = Some(key.clone());
            (*node_ptr).value = Some(value);
        }

        self.mapping.insert(key, last_idx);
        self.move_to_front_inside_lock(&mut ht, last_idx);
        drop(ht);

        // MỚI: persist entry bị evict vào TimeseriesStorage (ngoài shard-lock)
        if let Some((ek, ev)) = evicted {
            self.persist_point(&ek, &ev);
        }
    }

    /// Xoá entry khỏi cache theo key.
    /// Chỉ remove khỏi DashMap, slot trong arena được tái sử dụng khi `put` overwrite.
    pub fn remove(&self, key: &K) -> Option<V> {
        let (_, index) = self.mapping.remove(key)?;
        let value = self.caching[index].value.clone();
        // MỚI: persist entry bị xoá ra timeseries (best-effort)
        if let Some(v) = &value {
            self.persist_point(key, v);
        }
        value
    }

    /// Read-through `get`: giải quyết key theo thứ tự cache → đĩa → origin.
    ///
    /// 1. Cache hit: chạy `validate` trên value cache cho cửa sổ yêu cầu; đủ →
    ///    trả (LRU touch đã làm bởi `get`). Không đủ → coi như miss.
    /// 2. Miss (hoặc cache hit không qua validate): nếu có `TimeseriesStorage`,
    ///    `range(series, from_ts, to_ts)` được đọc, mỗi point decode + validate;
    ///    snapshot đầu tiên hợp lệ được nạp ngược vào cache và trả về.
    /// 3. Ngược lại gọi future `fallback`; kết quả ghi ngược vào cả cache lẫn
    ///    đĩa rồi trả về.
    /// 4. Fallback thành công → value được **merge** với cache (khi có
    ///    `ts_merge`) rồi ghi ngược vào cả cache lẫn đĩa. Fallback lỗi → trả
    ///    phần dữ liệu cache đang có (đọc kiểu best-effort), không có gì thì
    ///    `None` — khớp hành vi miss rỗng cũ.
    pub async fn get_with_load(&self, key: &K, from_ts: u64, to_ts: u64) -> Option<V>
    where
        V: 'static,
    {
        // 1. Cache hit (+ validate).
        if let Some(val) = self.get(key)
            && self
                .validate
                .as_ref()
                .is_none_or(|v| v(key, &val, from_ts, to_ts))
            {
                return Some(val);
            }
            // validate fail → coi như miss, đi tiếp xuống đĩa/origin.

        // 2. Tầng đĩa.
        if let (Some(ts), Some(series_of), Some(decode), Some(validate)) = (
            &self.timeseries,
            &self.ts_series_of,
            &self.decode,
            &self.validate,
        ) {
            let series = series_of(key);
            if let Ok(points) = ts.range(&series, from_ts, to_ts).await {
                // Ưu tiên snapshot mới nhất mà validate qua.
                for (_, bytes) in points.iter().rev() {
                    if let Some(decoded) = decode(bytes)
                        && validate(key, &decoded, from_ts, to_ts) {
                            self.put(key.clone(), decoded.clone());
                            return Some(decoded);
                        }
                }
            }
        }

        // 3. Tầng origin fallback. Chỉ fetch phần cửa sổ thực sự hổng
        // (`coverage_gap`), không fetch nguyên cửa sổ request — tránh treo /
        // lãng phí khi query "lấy mọi thứ" (to = u64::MAX) hay cửa sổ lan tới
        // tương lai không thể có dữ liệu.
        if let Some(fb) = &self.fallback {
            let cached = self.get(key);
            let (fetch_from, fetch_to) = match (&self.coverage_gap, &cached) {
                (Some(gap), Some(val)) => match gap(key, val, from_ts, to_ts) {
                    Some(g) => g,
                    // Cache đã phủ đủ (hổng chỉ nằm trong tương lai) → trả cache.
                    None => return Some(val.clone()),
                },
                _ => (from_ts, to_ts),
            };

            //eprintln!("[GAP] fallback window ({fetch_from},{fetch_to})");

            match fb.fetch(key, fetch_from, fetch_to).await {
                Ok(value) => {
                    //eprintln!("[GAP] fallback returned");
                    // Gộp slice vừa fetch với dữ liệu đã cache (khi có hook
                    // merge) — backfill quá khứ không được ghi đè mất các điểm
                    // mới hơn đang có.
                    let merged = match (&self.ts_merge, &cached) {
                        (Some(merge), Some(val)) => merge(val, value),
                        _ => value,
                    };
                    self.put(key.clone(), merged.clone());
                    self.persist_point(key, &merged);
                    return Some(merged);
                }
                Err(_e) => {
                    // eprintln!("[GAP] fallback fetch ({fetch_from},{fetch_to}) failed: {e}");
                    // Backfill lỗi (origin chết, cửa sổ quá lớn…) không được
                    // phép xoá sạch kết quả query: trả phần dữ liệu đang có,
                    // query phía trên tự lọc theo cửa sổ.
                    return cached;
                }
            }
        }

        None
    }

    fn move_to_front_inside_lock(&self, ht: &mut HeadTail, index: usize) {
        if ht.first == index || ht.first == NULL {
            return;
        }

        let node = &self.caching[index];
        let p = node.prev.load(Ordering::Acquire);
        let n = node.next.load(Ordering::Acquire);

        // Cắt node ra khỏi vị trí hiện tại
        if p != NULL {
            self.caching[p].next.store(n, Ordering::Release);
        }
        if n != NULL {
            self.caching[n].prev.store(p, Ordering::Release);
        }

        if index == ht.last {
            ht.last = p;
        }

        // Đưa lên đầu danh sách của Shard
        let old_first = ht.first;
        node.next.store(old_first, Ordering::Release);
        node.prev.store(NULL, Ordering::Release);

        if old_first != NULL {
            self.caching[old_first].prev.store(index, Ordering::Release);
        }

        ht.first = index;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::storage::{InMemoryStorage, TimeseriesStorage};

    type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

    const SHARD_COUNT: usize = 32;

    #[test]
    fn test_lru_cache_sharded_logic() {
        let capacity_per_shard = 2;
        let cache = LruCache::<usize, usize, 32>::new(capacity_per_shard * SHARD_COUNT);

        // Tìm 3 key rơi vào cùng 1 shard để test logic eviction
        let mut keys = Vec::new();
        for i in 0..1000 {
            if cache.get_shard_idx(&i) == 0 {
                keys.push(i);
                if keys.len() == 3 {
                    break;
                }
            }
        }
        let (k1, k2, k3) = (keys[0], keys[1], keys[2]);

        cache.put(k1, 10);
        cache.put(k2, 20);

        assert_eq!(cache.get(&k1), Some(10)); // k1 lên head của shard
        cache.put(k3, 30); // shard full (2 slot), evict k2 (vì k1 vừa được access)

        assert_eq!(cache.get(&k2), None); // k2 bị đuổi
        assert_eq!(cache.get(&k1), Some(10));
        assert_eq!(cache.get(&k3), Some(30));
    }

    #[test]
    fn test_update_existing_key() {
        let cache = LruCache::<usize, usize, 32>::new(16 * 2); // 2 slot mỗi shard
        cache.put(1, 10);
        cache.put(1, 20);

        assert_eq!(cache.get(&1), Some(20));
        assert_eq!(cache.mapping.len(), 1);

        let index = *cache.mapping.get(&1).unwrap();
        cache.put(1, 30);
        assert_eq!(index, *cache.mapping.get(&1).unwrap(), "Index không đổi");
    }

    #[test]
    fn test_empty_cache() {
        let cache = LruCache::<usize, usize, 32>::new(0);
        cache.put(1, 10);
        assert_eq!(cache.get(&1), None);
    }

    #[test]
    fn test_extreme_data_integrity() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let capacity_per_shard = 50;
        let total_capacity = capacity_per_shard * SHARD_COUNT;
        let cache = LruCache::<usize, usize, 32>::new(total_capacity);

        // Hàm tạo giá trị "chuẩn" theo Key để kiểm tra integrity
        let gen_value = |k: usize| -> usize {
            let mut s = DefaultHasher::new();
            k.hash(&mut s);
            s.finish() as usize
        };

        let num_threads = 12;
        let ops_per_thread = 2000;

        // --- PHASE 1: STRESS WRITE ---
        thread::scope(|s| {
            for t in 0..num_threads {
                let cache_ref = &cache;
                s.spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = t * ops_per_thread + i;
                        let val = gen_value(key);
                        cache_ref.put(key, val);
                    }
                });
            }
        });

        // --- PHASE 2: INTEGRITY VALIDATION ---

        // 1. Kiểm tra từng cặp Key-Value trong Mapping
        for entry in cache.mapping.iter() {
            let key = *entry.key();
            let index = *entry.value();

            let node = &cache.caching[index];
            let stored_key = node.key.expect("Node trong mapping phải có key");
            let stored_val = node.value.expect("Node trong mapping phải có value");

            assert_eq!(
                key, stored_key,
                "Data Corruption: Key trong mapping ({}) khác Key trong Node ({})",
                key, stored_key
            );
            assert_eq!(
                stored_val,
                gen_value(key),
                "Data Corruption: Value của key {} bị sai lệch!",
                key
            );

            // 2. Kiểm tra Shard Consistency: Key phải nằm đúng Shard của nó
            let expected_shard = cache.get_shard_idx(&key);
            // Kiểm tra xem index này có nằm trong dải bộ nhớ của Shard đó không
            let actual_shard = index / capacity_per_shard;
            assert_eq!(
                expected_shard, actual_shard,
                "Key {} nằm sai phân vùng Shard!",
                key
            );
        }

        // 3. Kiểm tra tính toàn vẹn của cấu trúc Danh sách liên kết (Double-ended check)
        for s_idx in 0..SHARD_COUNT {
            let ht = cache.shards[s_idx].mutex.lock().unwrap();
            let mut forward_count = 0;
            let mut backward_count = 0;

            // Duyệt xuôi: Head -> Tail
            let mut curr = ht.first;
            let mut last_seen = NULL;
            while curr != NULL {
                forward_count += 1;
                last_seen = curr;
                curr = cache.caching[curr].next.load(Ordering::Acquire);
            }
            assert_eq!(
                last_seen, ht.last,
                "Tail của Shard {} không khớp khi duyệt xuôi",
                s_idx
            );

            // Duyệt ngược: Tail -> Head
            let mut curr = ht.last;
            let mut first_seen = NULL;
            while curr != NULL {
                backward_count += 1;
                first_seen = curr;
                curr = cache.caching[curr].prev.load(Ordering::Acquire);
            }
            assert_eq!(
                first_seen, ht.first,
                "Head của Shard {} không khớp khi duyệt ngược",
                s_idx
            );
            assert_eq!(
                forward_count, backward_count,
                "Số lượng node duyệt xuôi và ngược không bằng nhau ở Shard {}",
                s_idx
            );
            assert_eq!(
                forward_count, capacity_per_shard,
                "Shard {} không đủ số lượng node",
                s_idx
            );
        }

        println!("🚀 [PASSED] Dữ liệu chuẩn 100%, không phát hiện Race Condition trên Node!");
    }

    #[test]
    fn test_internal_state_after_eviction_sharded() {
        // Để dễ test eviction, ta chọn capacity sao cho mỗi shard có đúng 2 slot
        let capacity_per_shard = 2;
        let total_capacity = capacity_per_shard * SHARD_COUNT;
        let cache = LruCache::<usize, usize, 32>::new(total_capacity);

        // 1. Tìm 3 key sao cho chúng rơi vào CÙNG MỘT SHARD
        // Điều này quan trọng vì mỗi shard tự quản lý việc đuổi (eviction) riêng
        let mut keys = Vec::new();

        for i in 0..1000 {
            if cache.get_shard_idx(&i) == 0 {
                keys.push(i);
                if keys.len() == 3 {
                    break;
                }
            }
        }

        let k1 = keys[0];
        let k2 = keys[1];
        let k3 = keys[2];

        // Giai đoạn lấp đầy 2 slot của Shard 0
        cache.put(k1, 10);
        cache.put(k2, 20);

        // Lấy index của k1 trước khi nó bị đuổi
        let index_of_k1 = *cache.mapping.get(&k1).expect("Key 1 phải tồn tại").value();

        // 2. Evict k1 bằng cách chèn k3 (vào cùng shard 0)
        cache.put(k3, 30);

        // Kiểm tra mapping
        assert_eq!(
            cache.mapping.get(&k3).map(|e| *e.value()),
            Some(index_of_k1),
            "Key 3 phải chiếm slot của Key 1"
        );
        assert!(cache.mapping.get(&k1).is_none(), "Key 1 phải bị đuổi");

        // 3. Lock đúng Shard 0 để kiểm tra Head/Tail
        let shard_idx = cache.get_shard_idx(&k3);
        let ht = cache.shards[shard_idx].mutex.lock().unwrap();

        let mru_index = *cache.mapping.get(&k3).unwrap().value();
        let lru_index = *cache.mapping.get(&k2).unwrap().value();

        assert_eq!(ht.first, mru_index, "Key 3 phải là đầu danh sách của shard");
        assert_eq!(ht.last, lru_index, "Key 2 phải là cuối danh sách của shard");

        // 4. Kiểm tra liên kết giữa các node trong Arena
        let mru_node = &cache.caching[mru_index];
        let lru_node = &cache.caching[lru_index];

        assert_eq!(mru_node.key, Some(k3));
        assert_eq!(mru_node.next.load(Ordering::Relaxed), lru_index);
        assert_eq!(mru_node.prev.load(Ordering::Relaxed), NULL);

        assert_eq!(lru_node.key, Some(k2));
        assert_eq!(lru_node.next.load(Ordering::Relaxed), NULL);
        assert_eq!(lru_node.prev.load(Ordering::Relaxed), mru_index);
    }

    #[test]
    fn test_lru_deadlock() {
        // Khởi tạo cache với capacity 10
        let cache = Arc::new(LruCache::<usize, String, 32>::new(16));

        // Giả lập dữ liệu ban đầu
        cache.put(1, "A".to_string());
        cache.put(2, "B".to_string());

        let cache_clone1 = Arc::clone(&cache);
        let t1 = thread::spawn(move || {
            for _ in 0..1000 {
                // Thread 1: Liên tục gọi put (chiếm nhiều lock bên trong)
                cache_clone1.put(1, "A_updated".to_string());
            }
        });

        let cache_clone2 = Arc::clone(&cache);
        let t2 = thread::spawn(move || {
            for _ in 0..1000 {
                // Thread 2: Liên tục gọi get (cũng gây move_to_front và chiếm lock)
                cache_clone2.get(&2);
            }
        });

        // Đợi 5 giây. Nếu code đúng O(1) thì 2000 thao tác này phải xong trong < 1s.
        // Nếu sau 5s không xong nghĩa là đã Deadlock.
        let result = thread::spawn(move || {
            t1.join().unwrap();
            t2.join().unwrap();
        });

        // Cơ chế check timeout cho test
        if wait_timeout(result, Duration::from_secs(5)).is_err() {
            panic!(
                "TEST FAILED: Deadlock detected! Cấu trúc nhiều RwLock lồng nhau đã làm treo thread."
            );
        }
    }

    fn wait_timeout<T: 'static>(
        handle: thread::JoinHandle<T>,
        timeout: Duration,
    ) -> Result<(), ()> {
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let _ = handle.join();
            let _ = tx.send(());
        });
        // Đợi kết quả từ thread trong khoảng timeout
        rx.recv_timeout(timeout).map_err(|_| ())
    }

    #[test]
    fn prove_deadlock_extremes() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let cache = Arc::new(LruCache::<usize, usize, 32>::new(100));

        // Nạp sẵn dữ liệu để thread 2 luôn rơi vào nhánh move_to_front
        for i in 0..100 {
            cache.put(i, i);
        }

        let cache_clone = cache.clone();
        let t1 = thread::spawn(move || {
            for i in 100..10000 {
                // Thread 1: Liên tục PUT key mới (gây áp lực lên chèn node và cập nhật first/last)
                cache_clone.put(i, i);
            }
        });

        let cache_clone2 = cache.clone();
        let t2 = thread::spawn(move || {
            for _ in 0..10000 {
                // Thread 2: Liên tục GET key cũ (gây áp lực lên move_to_front)
                // move_to_front sẽ chiếm caching.write rồi lại đòi first.write/read
                cache_clone2.get(&50);
            }
        });

        // Nếu không treo, 20.000 ops này phải xong trong < 1 giây
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            t1.join().unwrap();
            t2.join().unwrap();
            let _ = tx.send(());
        });

        if rx.recv_timeout(Duration::from_secs(10)).is_err() {
            panic!("DEADLOCK CONFIRMED: Hệ thống đã treo hoàn toàn sau 10 giây!");
        }
    }

    #[test]
    fn test_no_data_loss_and_leak() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let capacity_per_shard = 100;
        let total_capacity = capacity_per_shard * SHARD_COUNT;
        let evicted_count = Arc::new(AtomicUsize::new(0));

        // Setup cache với callback đếm số lần bị đuổi
        let evicted_clone = Arc::clone(&evicted_count);
        let mut cache = LruCache::<usize, usize, 32>::new(total_capacity);
        cache.on_removing = Some(Arc::new(move |_, _| {
            evicted_clone.fetch_add(1, Ordering::SeqCst);
        }));

        let num_threads = 8;
        let ops_per_thread = 5000;
        let total_ops = num_threads * ops_per_thread;

        thread::scope(|s| {
            for t in 0..num_threads {
                let cache_ref = &cache;
                s.spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = t * ops_per_thread + i;
                        cache_ref.put(key, i);
                    }
                });
            }
        });

        // --- BẮT ĐẦU VALIDATION ---

        // 1. Kiểm tra Mapping size
        // Số lượng phần tử hiện tại phải bằng total_capacity vì chúng ta chèn vượt ngưỡng rất nhiều
        assert_eq!(
            cache.mapping.len(),
            total_capacity,
            "Mapping phải đầy khít capacity"
        );

        // 2. Kiểm tra tính nhất quán của Linked List (Duyệt từng Shard)
        let mut total_nodes_in_lists = 0;
        for i in 0..SHARD_COUNT {
            let ht = cache.shards[i].mutex.lock().unwrap();
            let mut count = 0;
            let mut curr = ht.first;
            let mut visited = std::collections::HashSet::new();

            while curr != NULL {
                assert!(
                    visited.insert(curr),
                    "Phát hiện chu trình (vòng lặp vô hạn) trong Shard {}",
                    i
                );
                count += 1;
                curr = cache.caching[curr].next.load(Ordering::Acquire);
            }
            assert_eq!(
                count, capacity_per_shard,
                "Shard {} bị thiếu node trong danh sách liên kết",
                i
            );
            total_nodes_in_lists += count;
        }
        assert_eq!(total_nodes_in_lists, total_capacity);

        // 3. Kiểm tra số lượng đã bị đuổi (Eviction Balance)
        // Công thức: Tổng Put - Capacity = Số lần phải Evict
        let actual_evicted = evicted_count.load(Ordering::SeqCst);
        let expected_evicted = total_ops - total_capacity;
        assert_eq!(
            actual_evicted, expected_evicted,
            "Số lượng callback xóa không khớp với logic eviction"
        );

        println!("✅ Test passed: Không có dữ liệu bị 'lạc trôi', Linked List hoàn hảo!");
    }

    // ── Read-through tier (cache → disk → origin) ──────────────────────────

    #[tokio::test]
    async fn test_get_with_load_cache_hit_no_disk() {
        // Hit cache + validate pass → không đụng đĩa.
        let storage: Arc<dyn TimeseriesStorage> = Arc::new(InMemoryStorage::new());
        let mut cache: LruCache<String, String, 16> = LruCache::new(16);
        cache.attach_storage(
            Arc::clone(&storage),
            Arc::new(|k: &String| k.clone().into_bytes()),
            Arc::new(|v: &String| v.clone().into_bytes()),
            Arc::new(|b: &[u8]| String::from_utf8(b.to_vec()).ok()),
            Arc::new(|_, _, _, _| true),
            Arc::new(|_, _, _, _| None), // coverage_gap
        );
        cache.put("k".into(), "v".into());
        let got = cache.get_with_load(&"k".to_string(), 0, u64::MAX).await;
        assert_eq!(got, Some("v".to_string()));
        // Không phát sinh ghi đĩa (read path không persist).
        assert!(storage.range(b"k", 0, u64::MAX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_with_load_validate_fail_goes_to_disk() {
        // validate fail trên cache → coi miss → đọc đĩa → trả value đĩa.
        let storage: Arc<dyn TimeseriesStorage> = Arc::new(InMemoryStorage::new());
        let mut cache: LruCache<String, String, 16> = LruCache::new(16);
        // validate chỉ qua khi value bắt đầu bằng "disk".
        cache.attach_storage(
            Arc::clone(&storage),
            Arc::new(|k: &String| k.clone().into_bytes()),
            Arc::new(|v: &String| v.clone().into_bytes()),
            Arc::new(|b: &[u8]| String::from_utf8(b.to_vec()).ok()),
            Arc::new(|_: &String, v: &String, _: u64, _: u64| v.starts_with("disk")),
            Arc::new(|_, _, _, _| None), // coverage_gap
        );
        cache.put("k".into(), "cache:gap".into());
        storage.append(b"k", 500, b"disk:ok").await.unwrap();
        let got = cache.get_with_load(&"k".to_string(), 0, u64::MAX).await;
        assert_eq!(got, Some("disk:ok".to_string()));
    }

    #[tokio::test]
    async fn test_get_with_load_miss_reads_disk() {
        // Miss → đọc đĩa → nạp lại cache.
        let storage: Arc<dyn TimeseriesStorage> = Arc::new(InMemoryStorage::new());
        let mut cache: LruCache<String, String, 16> = LruCache::new(16);
        cache.attach_storage(
            Arc::clone(&storage),
            Arc::new(|k: &String| k.clone().into_bytes()),
            Arc::new(|v: &String| v.clone().into_bytes()),
            Arc::new(|b: &[u8]| String::from_utf8(b.to_vec()).ok()),
            Arc::new(|_, _, _, _| true),
            Arc::new(|_, _, _, _| None), // coverage_gap
        );
        storage.append(b"k", 500, b"disk-v").await.unwrap();
        let got = cache.get_with_load(&"k".to_string(), 0, u64::MAX).await;
        assert_eq!(got, Some("disk-v".to_string()));
        // Được nạp ngược vào cache.
        assert_eq!(cache.get(&"k".to_string()), Some("disk-v".to_string()));
    }

    #[tokio::test]
    async fn test_get_with_load_disk_empty_then_fallback() {
        // Đĩa rỗng → callback fallback được gọi.
        let storage: Arc<dyn TimeseriesStorage> = Arc::new(InMemoryStorage::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let mut cache: LruCache<String, String, 16> = LruCache::new(16);
        cache.attach_fallback(
            Arc::clone(&storage),
            Arc::new(|k: &String| k.clone().into_bytes()),
            Arc::new(|v: &String| v.clone().into_bytes()),
            Arc::new(|b: &[u8]| String::from_utf8(b.to_vec()).ok()),
            Arc::new(|_, _, _, _| true),
            Arc::new(|_, _, _, _| None), // coverage_gap
            Arc::new(move |_: &String, _: u64, _: u64| -> BoxFuture<Result<String, String>> {
                let c = Arc::clone(&calls2);
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok("fetched".to_string())
                })
            }),
        );
        let got = cache.get_with_load(&"k".to_string(), 0, u64::MAX).await;
        assert_eq!(got, Some("fetched".to_string()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_get_with_load_fallback_error_empty() {
        // Callback lỗi → rỗng.
        let storage: Arc<dyn TimeseriesStorage> = Arc::new(InMemoryStorage::new());
        let mut cache: LruCache<String, String, 16> = LruCache::new(16);
        cache.attach_fallback(
            Arc::clone(&storage),
            Arc::new(|k: &String| k.clone().into_bytes()),
            Arc::new(|v: &String| v.clone().into_bytes()),
            Arc::new(|b: &[u8]| String::from_utf8(b.to_vec()).ok()),
            Arc::new(|_, _, _, _| true),
            Arc::new(|_, _, _, _| None), // coverage_gap
            Arc::new(|_: &String, _: u64, _: u64| -> BoxFuture<Result<String, String>> {
                Box::pin(async move { Err("boom".to_string()) })
            }),
        );
        let got = cache.get_with_load(&"k".to_string(), 0, u64::MAX).await;
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn test_evict_persists_to_disk() {
        // Evict → dữ liệu nằm trên đĩa.
        let storage: Arc<dyn TimeseriesStorage> = Arc::new(InMemoryStorage::new());
        let mut cache: LruCache<String, String, 1> = LruCache::new(1);
        cache.attach_storage(
            Arc::clone(&storage),
            Arc::new(|k: &String| k.clone().into_bytes()),
            Arc::new(|v: &String| v.clone().into_bytes()),
            Arc::new(|b: &[u8]| String::from_utf8(b.to_vec()).ok()),
            Arc::new(|_, _, _, _| true),
            Arc::new(|_, _, _, _| None), // coverage_gap
        );
        cache.put("k1".into(), "v1".into());
        cache.put("k2".into(), "v2".into()); // evict k1 (1 slot)
        // Cho fire-and-forget persist task chạy.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let pts = storage.range(b"k1", 0, u64::MAX).await.unwrap();
        assert_eq!(pts.len(), 1);
        assert_eq!(pts[0].1, b"v1");
    }
}
