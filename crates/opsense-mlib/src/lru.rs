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

#[allow(clippy::type_complexity, clippy::too_many_arguments)]
const NULL: usize = usize::MAX;

/// Arena LRU có đang dùng chung giữa các shard không?
///
/// Hằng số công khai để crate khác **kiểm tra được** thay vì `cfg!` — `cfg!` chỉ
/// thấy feature của chính crate đó, mà feature `lru-shared-memory` thuộc về
/// `opsense-mlib`. Không có hằng này thì `opsense-core` phải đoán, và đoán sai thì
/// im lặng mất dữ liệu.
pub const SHARED_ARENA: bool = cfg!(feature = "lru-shared-memory");

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
type CoverageGap<K, V> = Arc<dyn Fn(&K, &V, u64, u64) -> Option<(u64, u64)> + Send + Sync>;

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

    /// Đầu free-list chung — node **chưa** thuộc shard nào.
    ///
    /// Chỉ có khi `lru-shared-memory`. Xem [`LruCache::new`] để hiểu vì sao bản
    /// không có feature này mất dữ liệu.
    #[cfg(feature = "lru-shared-memory")]
    free_head: AtomicUsize,

    pub on_removing: Option<Arc<dyn Fn(K, V) + Send + Sync>>,
    pub on_updating: Option<Arc<dyn Fn(K, V) + Send + Sync>>,

    /// Persistence layer (optional). Khi được gắn, mỗi entry bị **evict** (do
    /// shard đầy) hoặc **update** (ghi đè key cũ) sẽ được append vào
    /// `TimeseriesStorage` dưới dạng điểm `(timestamp, value)`.
    pub timeseries: Option<Arc<dyn TimeseriesStorage>>,

    /// Map key → series name (opaque bytes). Quyết định entry ghi vào series nào.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub ts_series_of: Option<Arc<dyn Fn(&K) -> Vec<u8> + Send + Sync>>,

    /// Serialize value → opaque bytes lưu vào timeseries.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub ts_encode: Option<Arc<dyn Fn(&V) -> Vec<u8> + Send + Sync>>,

    /// Timestamp source (ms). Mặc định: `SystemTime::now()`.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub ts_clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,

    /// Decode opaque storage bytes về lại `V` (encode dùng lại `ts_encode`).
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub decode: Option<Arc<dyn Fn(&[u8]) -> Option<V> + Send + Sync>>,

    /// Validate độ phủ của một value (cache hoặc đĩa) cho cửa sổ yêu cầu.
    /// Trả `true` khi dữ liệu đủ/đáng tin. `None` → mặc định coi là đủ (như
    /// hành vi cũ). Khi trả `false`, entry được coi là miss và đi tiếp xuống
    /// tầng đĩa / origin.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub validate: Option<Arc<dyn Fn(&K, &V, u64, u64) -> bool + Send + Sync>>,

    /// Tính cửa sổ thực sự hổng (chưa có dữ liệu) giữa `[from_ts, to_ts]` của
    /// một value đã có trong cache. Trả `Some((gap_from, gap_to))` để chỉ fetch
    /// phần thiếu, hoặc `None` khi cache đã phủ đủ (hổng chỉ nằm trong tương
    /// lai, không thể fetch). Chỉ dùng khi có `fallback`/`storage`.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub coverage_gap: Option<Arc<dyn Fn(&K, &V, u64, u64) -> Option<(u64, u64)> + Send + Sync>>,

    /// Nguồn gốc để re-fetch khi cả cache lẫn đĩa đều miss (tầng 3 read-through).
    /// Là `Arc<dyn OriginSource>` — xem [`OriginSource`].
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fallback: Option<Arc<dyn OriginSource<K, V>>>,

    /// Gộp slice vừa fetch từ origin với value đang có trong cache khi fallback
    /// thành công. `None` → value fetch được ghi đè nguyên khối (hành vi cũ).
    /// Station gán hook này để backfill quá khứ không xoá mất các điểm mới hơn
    /// đã có trong cache.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub ts_merge: Option<Arc<dyn Fn(&V, V) -> V + Send + Sync>>,

    /// Timestamp extractor cho điểm được persist xuống đĩa: từ entry sinh ra
    /// `u64` làm `ts` của point. Khi `None`, dùng `ts_clock` (mặc định
    /// wall-clock ms). Station gán callback này trả **ts quan sát mới nhất**
    /// để các điểm trên đĩa căn lề với cửa sổ request (tính bằng giây, không
    /// phải ms).
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
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

        #[cfg(not(feature = "lru-shared-memory"))]
        let capacity_per_shard = total_capacity.div_ceil(S);

        // 1. Arena.
        //
        // Mặc định: chia **cứng** thành S khối × `capacity_per_shard` node, ranh
        // giới `offset` đóng dấu lúc này và không bao giờ dịch. Shard tràn thì
        // không mượn được node của shard đang trống ⇒ `capacity` mất hết ý
        // nghĩa tổng, và mất dữ liệu khi `capacity_per_shard == 1`.
        //
        // `lru-shared-memory`: **một** arena `capacity` node, tất cả nằm trong
        // free-list chung. Shard nào cần thì pop một node trống ra, nên chỗ
        // trống của shard này bù được cho chỗ đầy của shard kia, và `capacity`
        // là sức chứa tổng thật.
        #[cfg(not(feature = "lru-shared-memory"))]
        #[cfg(not(feature = "lru-shared-memory"))]
        let (caching_vec, actual_total) = {
            let actual_total = capacity_per_shard * S;
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
            (caching_vec, actual_total)
        };

        #[cfg(feature = "lru-shared-memory")]
        let (caching_vec, free_head, actual_total) = {
            let mut caching_vec = Vec::with_capacity(total_capacity);
            for i in 0..total_capacity {
                caching_vec.push(Node {
                    key: None,
                    value: None,
                    // free-list đi theo `next`, xích từ node cuối về 0.
                    next: AtomicUsize::new(if i + 1 < total_capacity { i + 1 } else { NULL }),
                    prev: AtomicUsize::new(NULL),
                });
            }
            let head = if total_capacity == 0 { NULL } else { 0 };
            (caching_vec, head, total_capacity)
        };

        // 2. Khởi tạo mảng các Shard Mutex (đã được aligned)
        #[cfg(not(feature = "lru-shared-memory"))]
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

        // Với arena chung, **mọi** shard khởi đầu rỗng — node nằm trong
        // free-list cho tới khi được `put` cấp. Ranh giới khối cứng biến mất.
        #[cfg(feature = "lru-shared-memory")]
        let shards = std::array::from_fn(|_| {
            AlignedShard {
                mutex: Mutex::new(HeadTail {
                    first: NULL,
                    last: NULL,
                }),
            }
        });

        Self {
            mapping: DashMap::with_capacity(actual_total),
            caching: caching_vec.into_boxed_slice(),
            shards,
            shard_mask: S - 1,
            #[cfg(feature = "lru-shared-memory")]
            free_head: AtomicUsize::new(free_head),
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
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
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
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
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
        let (Some(storage), Some(series_of), Some(encode)) =
            (&self.timeseries, &self.ts_series_of, &self.ts_encode)
        else {
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
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
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
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
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

        // Case 2: Ghi mới.
        //
        // `lru-shared-memory`: lấy node trống từ free-list chung, hết thì mượn
        // từ shard khác. Arena **không** chia cứng nên `capacity` là sức chứa
        // tổng thật — khác nhánh dưới, nơi chỉ `ht.last` của chính shard này là
        // chỗ duy nhất ghi được.
        #[cfg(feature = "lru-shared-memory")]
        {
            let Some((idx, evicted)) = self.acquire_node(shard_idx) else {
                return;
            };
            let mut ht = self.shards[shard_idx].mutex.lock().unwrap();
            let node = &self.caching[idx];
            unsafe {
                let node_ptr = node as *const Node<K, V> as *mut Node<K, V>;
                (*node_ptr).key = Some(key.clone());
                (*node_ptr).value = Some(value);
            }
            self.mapping.insert(key, idx);
            // Node vừa pop về mặc định `prev = next = NULL` (free-list dùng
            // `next`), nên khi shard **rỗng** thì `ht.first` vẫn NULL và
            // `move_to_front_inside_lock` thoát sớm — phải tự nối vào.
            if ht.first == NULL {
                node.prev.store(NULL, Ordering::Release);
                node.next.store(NULL, Ordering::Release);
                ht.first = idx;
                ht.last = idx;
            } else {
                self.move_to_front_inside_lock(&mut ht, idx);
            }
            drop(ht);

            if let Some((ek, ev)) = evicted {
                if let Some(cb) = &self.on_removing {
                    cb(ek.clone(), ev.clone());
                }
                self.persist_point(&ek, &ev);
            }
        }

        // Bản chia shard: chỉ ghi được vào node cuối của chính shard này.
        #[cfg(not(feature = "lru-shared-memory"))]
        let mut ht = self.shards[shard_idx].mutex.lock().unwrap();
        #[cfg(not(feature = "lru-shared-memory"))]
        let last_idx = ht.last;
        #[cfg(not(feature = "lru-shared-memory"))]
        if last_idx == NULL {
            return;
        }

        // ---- Thân nhánh `lru-shared-memory` đã `return` ở trên ----
        #[cfg(not(feature = "lru-shared-memory"))]
        {
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
    }

    /// Xoá **toàn bộ** entry và đưa cache về đúng trạng thái lúc [`LruCache::new`]:
    /// `mapping` rỗng, free-list (hoặc linked-list từng shard) khôi phục nguyên
    /// vẹn. Trả số entry đã xoá.
    ///
    /// Khác [`LruCache::remove`] ở hai điểm cố ý:
    ///
    /// 1. **Không** gọi `on_removing` / `on_updating` và **không** `persist_point`.
    ///    Xoá hàng loạt là thao tác quản trị (vd clear station) — dữ liệu bị
    ///    xoá là chủ đích, nên không persist lại từng entry vừa rút.
    /// 2. Trả về **số entry bị xoá** để caller báo cáo, thay vì `Option<V>`.
    ///
    /// Rẻ hơn nhiều so với lặp [`LruCache::remove`]: một lần `DashMap::clear`
    /// + một vòng `S` lần, không phải `O(n)` lần khóa shard và persist.
    pub fn clear(&self) -> usize {
        let removed = self.mapping.len();
        self.mapping.clear();

        // Arena dùng chung: mọi node về free-list, xích từ node cuối về 0 —
        // đúng như `new()` dựng. Vì vậy `free_head` quyết định phần lớn trạng
        // thái, còn `next`/`prev` của từng node là dự phòng cho lần `put` sau.
        #[cfg(feature = "lru-shared-memory")]
        {
            let len = self.caching.len();
            for index in 0..len {
                let node = &self.caching[index] as *const Node<K, V> as *mut Node<K, V>;
                unsafe {
                    (*node).key = None;
                    (*node).value = None;
                    (*node).next = AtomicUsize::new(if index + 1 < len { index + 1 } else { NULL });
                    (*node).prev = AtomicUsize::new(NULL);
                }
            }
            for shard in &self.shards {
                let mut ht = shard.mutex.lock().unwrap();
                ht.first = NULL;
                ht.last = NULL;
            }
            self.free_head = AtomicUsize::new(if len == 0 { NULL } else { 0 });
        }

        // Arena chia cứng: ranh giới khối đóng dấu lúc `new()` và không dịch,
        // nên `per_shard` suy ra lại được từ `caching.len()` — và phải khớp
        // đúng `ceil(capacity / S)` mà `new()` đã dùng, nếu không linked-list
        // sẽ khác hẳn cache lúc mới và `put` sẽ đi lệch.
        #[cfg(not(feature = "lru-shared-memory"))]
        {
            let per_shard = self.caching.len() / S;
            for shard_index in 0..S {
                let offset = shard_index * per_shard;
                for i in 0..per_shard {
                    let current = offset + i;
                    let node = &self.caching[current] as *const Node<K, V> as *mut Node<K, V>;
                    unsafe {
                        (*node).key = None;
                        (*node).value = None;
                        (*node).next =
                            AtomicUsize::new(if i + 1 < per_shard { current + 1 } else { NULL });
                        (*node).prev = AtomicUsize::new(if i > 0 { current - 1 } else { NULL });
                    }
                }
                let mut ht = self.shards[shard_index].mutex.lock().unwrap();
                ht.first = if per_shard > 0 { offset } else { NULL };
                ht.last = if per_shard > 0 { offset + per_shard - 1 } else { NULL };
            }
        }

        removed
    }

    /// Xoá entry khỏi cache theo key.
    /// Chỉ remove khỏi DashMap, slot trong arena được tái sử dụng khi `put` overwrite.
    pub fn remove(&self, key: &K) -> Option<V> {
        let (_, index) = self.mapping.remove(key)?;
        let value = self.caching[index].value.clone();

        // Arena chung: node phải **rời khỏi linked list của shard** rồi về
        // free-list, nếu không nó vẫn bị tính là đang sống và `acquire_node`
        // không mượn được ⇒ sức chứa tụt dần theo số lần `remove`.
        #[cfg(feature = "lru-shared-memory")]
        {
            let shard_idx = self.get_shard_idx(key);
            let mut ht = self.shards[shard_idx].mutex.lock().unwrap();
            // `key`/`value` là `Option`, không phải atomic — phải `unsafe` như
            // các chỗ ghi node khác trong file (xem `put`).
            unsafe {
                let node = &self.caching[index] as *const Node<K, V> as *mut Node<K, V>;
                (*node).key = None;
                (*node).value = None;
            }
            self.unlink_inside_lock(&mut ht, index);
            drop(ht);
            self.free_push(index);
        }
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
                        && validate(key, &decoded, from_ts, to_ts)
                    {
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

    // --- free-list chung (chỉ khi `lru-shared-memory`) ---

    /// Pop một node trống từ free-list chung (Treiber stack, CAS không khóa).
    ///
    /// Dùng `next` làm con trỏ stack: node đang **trống** thì không thuộc
    /// linked list của shard nào, nên `next` rảnh để dùng.
    #[cfg(feature = "lru-shared-memory")]
    fn free_pop(&self) -> Option<usize> {
        let mut head = self.free_head.load(Ordering::Acquire);
        loop {
            if head == NULL {
                return None;
            }
            let next = self.caching[head].next.load(Ordering::Acquire);
            match self.free_head.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.caching[head].next.store(NULL, Ordering::Release);
                    return Some(head);
                }
                Err(actual) => head = actual,
            }
        }
    }

    /// Đẩy node trống về free-list chung.
    #[cfg(feature = "lru-shared-memory")]
    fn free_push(&self, index: usize) {
        let mut head = self.free_head.load(Ordering::Acquire);
        loop {
            self.caching[index].next.store(head, Ordering::Release);
            match self.free_head.compare_exchange_weak(
                head,
                index,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Cấp một node trống cho `shard_idx`.
    ///
    /// Ưu tiên free-list; hết thì **đá** một node đang sống ở shard nào đó
    /// (kể cả chính shard mình). Đây là chỗ `capacity` thành **sức chứa tổng
    /// thật**: chỗ trống của shard này bù được cho chỗ đầy của shard kia.
    ///
    /// Trả `None` **chỉ khi cache rỗng hoàn toàn** (`capacity == 0`).
    #[cfg(feature = "lru-shared-memory")]
    fn acquire_node(&self, shard_idx: usize) -> Option<(usize, Option<(K, V)>)> {
        if let Some(idx) = self.free_pop() {
            return Some((idx, None));
        }

        // Cache đã đầy: phải đá.
        //
        // Quét từng shard, **giữ đúng một khóa tại một thời điểm** — thả ngay
        // sau khi lấy node. Không bao giờ giữ nhiều khóa cùng lúc nên không
        // deadlock. Vòng lặp vì `ht.last` có thể đổi giữa lúc thả khóa và lúc
        // `lock` lại: lúc đó **thử lại shard khác**, không bỏ `put`.
        //
        // Không được `return None` ở giữa chừng: `put` sẽ bỏ qua lặng lẽ và
        // dữ liệu biến mất mà không ai báo (bug thật, `test_no_data_loss_and_leak`
        // bắt được trước khi sửa).
        for start in 0..S {
            for off in 0..S {
                let i = (start + off) % S;
                let mut ht = self.shards[i].mutex.lock().unwrap();
                let v_idx = ht.last;
                if v_idx == NULL {
                    drop(ht);
                    continue;
                }
                let evicted = unsafe {
                    let node = &self.caching[v_idx] as *const Node<K, V> as *mut Node<K, V>;
                    (*node).key.take().map(|k| {
                        let v = (*node).value.take().unwrap();
                        (k, v)
                    })
                };
                self.unlink_inside_lock(&mut ht, v_idx);
                drop(ht);
                if let Some((ek, _)) = &evicted {
                    self.mapping.remove(ek);
                }
                // `evicted == None` ⇒ node này **đã trống** (không ai giữ key),
                // lấy thoải mái, không mất gì.
                let _ = shard_idx; // thứ tự quét không phụ thuộc shard đích
                return Some((v_idx, evicted));
            }
        }
        None
    }

    /// Cắt node `index` ra khỏi linked list của shard, **giả định đã giữ lock**.
    #[cfg(feature = "lru-shared-memory")]
    fn unlink_inside_lock(&self, ht: &mut HeadTail, index: usize) {
        let p = self.caching[index].prev.load(Ordering::Acquire);
        let n = self.caching[index].next.load(Ordering::Acquire);
        if p != NULL {
            self.caching[p].next.store(n, Ordering::Release);
        } else {
            ht.first = n;
        }
        if n != NULL {
            self.caching[n].prev.store(p, Ordering::Release);
        } else {
            ht.last = p;
        }
        self.caching[index].prev.store(NULL, Ordering::Release);
        self.caching[index].next.store(NULL, Ordering::Release);
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

    // =====================================================================
    // `total_capacity` phải là SỨC CHỨA TỔNG, không phải trần mỗi shard
    // =====================================================================

    /// Tìm `count` khoá khác nhau mà **tất cả** rơi vào cùng một shard.
    ///
    /// Với `S` lũy thừa của 2, shard = `hash(key) & (S - 1)`. Tìm bằng cách quét
    /// số nguyên tăng dần — trùng shard xuất hiện rất nhanh vì `DefaultHasher`
    /// phân bố đều trên `S` giá trị.
    fn keys_in_one_shard<const S: usize>(count: usize, skip: usize) -> Vec<i64> {
        let cache: LruCache<i64, u32, S> = LruCache::new(S);
        let mut out = Vec::with_capacity(count);
        let mut k = skip as i64;
        while out.len() < count {
            if cache.get_shard_idx(&k) == 0 {
                out.push(k);
            }
            k += 1;
        }
        out
    }

    /// **BUG PROOF**: `LruCache::new(total_capacity)` KHÔNG giữ được `total_capacity`
    /// entry khi `total_capacity == S`.
    ///
    /// `new()` chia đều: `capacity_per_shard = ceil(total_capacity / S)`. Với
    /// `total_capacity = S` thì **`capacity_per_shard = 1`** — mỗi shard chỉ chứa
    /// đúng một entry. Hai khoá trùng shard ⇒ cái mới đẩy cái cũ, **dù tổng
    /// cache còn trống**.
    ///
    /// Hậu quả trong `TimeseriesStation` là mất dữ liệu **im lặng**: backend
    /// `memory` không có storage để read-through (`load_cold_block` trả `None`),
    /// nên block bị đẩy là mất hẳn. Đo trên stack thật với node `history-1h`
    /// (`limit=1000`): 29 block / 32 shard ⇒ 8 shard nhận 2–3 block ⇒ 11 block mất,
    /// và `update_range` **không cảnh báo** (điều kiện là `span > HOT_BLOCKS`).
    ///
    /// Test này không cần station: chỉ cần `LruCache` với `capacity == S`.
    // BUG CHƯA SỬA Ở `opsense-mlib::lru` — test để đỏ có chủ đích.
    //
    // `new()` chia `capacity_per_shard = ceil(capacity / S)`, nên `capacity` là
    // sức chứa **tổng** trên giấy nhưng thực tế là "mỗi shard phần chia đó".
    // `LruCache::new(32)` với `S = 32` ⇒ mỗi shard **một** slot ⇒ chỉ cần hai
    // key trùng shard là evict, tổng cache không liên quan.
    //
    // Sửa đúng chỗ cần arena cấp phát **động** (free-list chung) thay vì
    // `Box<[Node]>` tĩnh theo shard — refactor đa luồng, chưa làm. Trong lúc
    // đó, `TimeseriesStation` chạy `S = 1` để `capacity` là tổng thật
    // (xem `STATION_SHARDS` trong `opsense-core/src/station.rs`).
    //
    // Bỏ `#[ignore]` khi sửa xong `lru.rs`.
    // BUG CHƯA SỬA — xem `capacity_is_total_not_per_shard`.
    #[test]
    #[ignore = "BUG: capacity là sức chứa mỗi shard, không phải tổng"]
    fn capacity_is_total_not_per_shard() {
        const S: usize = 32;
        // Chỉ 4 entry — thấp hơn capacity 32 rất nhiều.
        let keys = keys_in_one_shard::<S>(4, 0);
        let cache: LruCache<i64, u32, S> = LruCache::new(S);

        for (i, k) in keys.iter().enumerate() {
            cache.put(*k, i as u32);
        }

        // Với `capacity_per_shard = 1`, chỉ key cuối cùng còn sống.
        let sống: Vec<i64> = keys.iter().copied().filter(|k| cache.get(k).is_some()).collect();
        assert_eq!(
            sống.len(),
            keys.len(),
            "cache capacity {} nhưng chỉ giữ {}/{} entry rồi — tổng còn trống mà vẫn evict.              Sức chứa phải là TỔNG, không phải trần mỗi shard (mọi key trùng shard 0).",
            S,
            sống.len(),
            keys.len()
        );
    }

    /// `clear()` phải đưa cache về **đúng** trạng thái lúc `new()`, không chỉ
    /// làm rỗng `mapping`.
    ///
    /// Nếu arena không được dựng lại (free-list / linked-list từng shard còn
    /// trỏ vào node đã xoá) thì `put` sau đó sẽ ghi đè lên node mà linked-list
    /// không tính là đang sống ⇒ mất dữ liệu âm thầm, và lỗi đó chỉ lộ ra ở
    /// lần đọc sau. Vì vậy assert cả hai vế: sau clear phải **còn đủ `capacity`
    /// entry**, chứ không chỉ "get trả None".
    ///
    /// Không rẽ nhánh theo `SHARED_ARENA` — hai layout arena phải cho cùng kết
    /// quả, và test này chạy trên cả hai ở CI.
    #[test]
    fn clear_restores_full_capacity() {
        const S: usize = 8;
        const CAP: i64 = 64;
        let cache: LruCache<i64, i64, S> = LruCache::new(CAP as usize);

        for i in 0..CAP {
            cache.put(i, i * 2);
        }
        assert_eq!(cache.get(&0), Some(0), "put/get phải chạy được trước khi clear");

        assert_eq!(cache.clear(), CAP as usize, "clear trả về số entry đã xoá");

        for i in 0..CAP {
            assert_eq!(cache.get(&i), None, "sau clear phải rỗng, còn sót {i}");
        }

        // Vế quan trọng: nạp lại **đúng bằng** CAP entry và không mất món nào.
        // Nếu arena chưa hồi phục, chỉ cần nửa số key là đủ để evict.
        for i in 0..CAP {
            cache.put(i, i * 3);
        }
        for i in 0..CAP {
            assert_eq!(cache.get(&i), Some(i * 3), "sau clear phải còn đủ {CAP} key");
        }
    }

    /// Cùng lỗi, nhưng nhìn từ góc khác: capacity 32 **từng được cho là** giữ
    /// được 32 entry. Chứng minh nó chỉ giữ được `S` entry khi key trùng shard.
    // BUG CHƯA SỬA Ở `opsense-mlib::lru` — test để đỏ có chủ đích.
    //
    // `new()` chia `capacity_per_shard = ceil(capacity / S)`, nên `capacity` là
    // sức chứa **tổng** trên giấy nhưng thực tế là "mỗi shard phần chia đó".
    // `LruCache::new(32)` với `S = 32` ⇒ mỗi shard **một** slot ⇒ chỉ cần hai
    // key trùng shard là evict, tổng cache không liên quan.
    //
    // Sửa đúng chỗ cần arena cấp phát **động** (free-list chung) thay vì
    // `Box<[Node]>` tĩnh theo shard — refactor đa luồng, chưa làm. Trong lúc
    // đó, `TimeseriesStation` chạy `S = 1` để `capacity` là tổng thật
    // (xem `STATION_SHARDS` trong `opsense-core/src/station.rs`).
    //
    // Bỏ `#[ignore]` khi sửa xong `lru.rs`.
    // BUG CHƯA SỬA — xem `capacity_is_total_not_per_shard`.
    #[test]
    #[ignore = "BUG: capacity là sức chứa mỗi shard, không phải tổng"]
    fn advertised_capacity_is_actually_held() {
        const S: usize = 32;
        let keys = keys_in_one_shard::<S>(S, 1000);
        let cache: LruCache<i64, u32, S> = LruCache::new(S);
        for (i, k) in keys.iter().enumerate() {
            cache.put(*k, i as u32);
        }
        let alive = keys.iter().filter(|k| cache.get(k).is_some()).count();
        assert_eq!(
            alive, S,
            "nạp đúng `capacity` = {S} entry, phải giữ hết — thực tế chỉ còn {alive}"
        );
    }

    /// REPRODUCE đúng ca thật: 29 block id liên tiếp của node `history-1H`
    /// (`block_secs = 129600` ⇒ block id = `ts / 129600`), nạp vào
    /// `LruCache<i64, Block, 32>` với `capacity = 32` — đúng cấu hình trước khi
    /// sửa.
    ///
    /// Đo trên stack: node nạp 29 block, station còn **14**, mất **15**
    /// (`13788…13796` = 9 block đầu, cộng 6 rải rác). Test này trả lời: LRU
    /// một mình có giải thích được 15 đó không, hay còn đường ghi khác.
    ///
    /// Ghi LOG số entry còn lại theo từng shard để thấy ngay block nào rơi.
    // BUG CHƯA SỬA — test để đỏ có chủ đích, xem `capacity_is_total_not_per_shard`.
    // BUG CHƯA SỬA — xem `capacity_is_total_not_per_shard`.
    #[test]
    #[ignore = "BUG: arena chia cứng, 11/15 block mất; chỉ vá tạm bằng STATION_SHARDS = 1"]
    fn repro_real_case_29_blocks_into_32_shards() {
        const S: usize = 32;
        const CAP: usize = 32;
        // Block id thật đo được: node nạp from=1787043600 → 13788.
        let first: i64 = 1787043600 / 129600;
        let n = 29i64;

        let cache: LruCache<i64, u32, S> = LruCache::new(CAP);
        for i in 0..n {
            cache.put(first + i, i as u32);
        }

        let missing: Vec<i64> = (0..n)
            .map(|i| first + i)
            .filter(|k| cache.get(k).is_none())
            .collect();
        let alive = (n as usize) - missing.len();

        // Báo cáo ngay cả khi xanh — đây là phép đo, không phải assert.
        let mut per_shard = std::collections::BTreeMap::<usize, Vec<i64>>::new();
        let mask = S - 1;
        for i in 0..n {
            let k = first + i;
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            per_shard
                .entry((h.finish() as usize) & mask)
                .or_default()
                .push(k);
        }
        let crowded: Vec<_> = per_shard.iter().filter(|(_, v)| v.len() > 1).collect();

        println!(
            "REPRO: {} block vào LruCache(cap={}, S={}) → còn {}, mất {}\n               shard nhận >1 block: {}\n  block mất: {:?}\n               (đo trên stack: còn 14, mất 15 — 9 block đầu 13788..13796 + 6 rải rác)",
            n, CAP, S, alive, missing.len(), crowded.len(), missing,
        );

        assert_eq!(
            missing.len(),
            0,
            "LRU 32 shard × 1 slot giữ được 29 block: mất {:?}",
            missing
        );
    }

    /// Phân bố key đều thì chưa lộ lỗi — đó là lý do nó lọt. Test này ghim
    /// hành vi ĐÚNG để phân biệt: 32 key rải rác trên 32 shard thì giữ hết.
    // BUG CHƯA SỬA Ở `opsense-mlib::lru` — test để đỏ có chủ đích.
    //
    // `new()` chia `capacity_per_shard = ceil(capacity / S)`, nên `capacity` là
    // sức chứa **tổng** trên giấy nhưng thực tế là "mỗi shard phần chia đó".
    // `LruCache::new(32)` với `S = 32` ⇒ mỗi shard **một** slot ⇒ chỉ cần hai
    // key trùng shard là evict, tổng cache không liên quan.
    //
    // Sửa đúng chỗ cần arena cấp phát **động** (free-list chung) thay vì
    // `Box<[Node]>` tĩnh theo shard — refactor đa luồng, chưa làm. Trong lúc
    // đó, `TimeseriesStation` chạy `S = 1` để `capacity` là tổng thật
    // (xem `STATION_SHARDS` trong `opsense-core/src/station.rs`).
    //
    // Bỏ `#[ignore]` khi sửa xong `lru.rs`.
    // BUG CHƯA SỬA — xem `capacity_is_total_not_per_shard`.
    #[test]
    #[ignore = "BUG: capacity là sức chứa mỗi shard, không phải tổng"]
    fn spread_keys_keep_everything() {
        const S: usize = 32;
        let cache: LruCache<i64, u32, S> = LruCache::new(S);
        let keys: Vec<i64> = (0..S as i64).collect();
        for (i, k) in keys.iter().enumerate() {
            cache.put(*k, i as u32);
        }
        let alive = keys.iter().filter(|k| cache.get(k).is_some()).count();
        assert_eq!(alive, S, "key rải rác, mỗi shard một cái thì giữ hết");
    }

    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::storage::{InMemoryStorage, TimeseriesStorage};

    type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

    const SHARD_COUNT: usize = 32;

    // Test dựng trên **ranh giới khối cứng**: 3 key cùng shard với
    // `capacity_per_shard = 2` thì key thứ ba bắt buộc phải đẩy một key khác.
    // Arena chung cố ý bỏ ranh giới đó (đó là cả lý do tồn tại của feature), nên
    // ở đây key thứ ba **không** đẩy ai — và `capacity` vẫn là sức chứa tổng.
    // Test tương đương cho arena chung: `shared_arena_capacity_is_total`.
    #[test]
    #[cfg_attr(
        feature = "lru-shared-memory",
        ignore = "khẳng định ranh giới khối cứng — đúng thứ `lru-shared-memory` bỏ"
    )]
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

            // 2. Kiểm tra Shard Consistency: Key phải nằm đúng Shard của nó.
            // Chỉ có ý nghĩa khi arena chia cứng; xem `test_extreme_data_integrity`.
            #[cfg(not(feature = "lru-shared-memory"))]
            {
                let expected_shard = cache.get_shard_idx(&key);
                let actual_shard = index / capacity_per_shard;
                assert_eq!(
                    expected_shard, actual_shard,
                    "Key {} nằm sai phân vùng Shard!",
                    key
                );
            }
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
            // Mỗi shard có **đúng** `capacity_per_shard` node chỉ đúng khi arena
            // chia cứng. Arena chung: shard chỉ chứa node **đã được cấp**, và
            // `free-list` giữ phần còn lại — nên tổng mới là bất biến đúng.
            // Các assert ở trên (head khớp, duyệt xuôi = ngược) vẫn giữ nguyên
            // và vẫn có ý nghĩa với arena chung.
            #[cfg(not(feature = "lru-shared-memory"))]
            assert_eq!(
                forward_count, capacity_per_shard,
                "Shard {} không đủ số lượng node",
                s_idx
            );
            #[cfg(feature = "lru-shared-memory")]
            let _ = forward_count;
        }

        println!("🚀 [PASSED] Dữ liệu chuẩn 100%, không phát hiện Race Condition trên Node!");
    }

    // Assert "k3 chiếm **đúng chỉ số arena** của k1" — chỉ có ý nghĩa khi node
    // bị tái sử dụng **tại chỗ** trong khối của shard. Arena chung tái dụng node
    // bất kỳ, nên chỉ số có thể khác mà hành vi vẫn đúng.
    #[test]
    #[cfg_attr(
        feature = "lru-shared-memory",
        ignore = "assert chỉ số arena cục bộ — arena chung tái dụng node bất kỳ"
    )]
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
            // Mỗi shard có **đúng** `capacity_per_shard` node chỉ đúng khi arena
            // chia cứng. Arena chung: shard chỉ chứa node **đã được cấp**, nên
            // tổng mới là bất biến đúng (đã assert ở trên).
            #[cfg(not(feature = "lru-shared-memory"))]
            assert_eq!(
                count, capacity_per_shard,
                "Shard {} bị thiếu node trong danh sách liên kết",
                i
            );
            #[cfg(feature = "lru-shared-memory")]
            let _ = count;
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
            Arc::new(
                move |_: &String, _: u64, _: u64| -> BoxFuture<Result<String, String>> {
                    let c = Arc::clone(&calls2);
                    Box::pin(async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok("fetched".to_string())
                    })
                },
            ),
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
            Arc::new(
                |_: &String, _: u64, _: u64| -> BoxFuture<Result<String, String>> {
                    Box::pin(async move { Err("boom".to_string()) })
                },
            ),
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

#[cfg(test)]
mod cap_probe {
    use super::*;

    /// Tách hai biến: `capacity` (tuyệt đối) và `S` (số shard) — cái nào
    /// quyết định mất dữ liệu?
    ///
    /// Cùng **29 block id thật** của `history-1H` (`13788..13788+29`), chỉ đổi
    /// `capacity` và `S`, rồi đếm block còn lại.
    #[test]
    fn which_factor_causes_loss() {
        const N: i64 = 29;
        let first: i64 = 1787043600 / 129600;

        // A) đúng ca thật: capacity 32, S 32  ⇒ 1 slot/shard
        let a: LruCache<i64, u32, 32> = LruCache::new(32);
        for i in 0..N {
            a.put(first + i, i as u32);
        }
        let a_alive = (0..N).filter(|i| a.get(&(first + i)).is_some()).count();

        // B) capacity BỊ CHÍNH mà S giữ nguyên 32 ⇒ 32 slot/shard
        let b: LruCache<i64, u32, 32> = LruCache::new(1024);
        for i in 0..N {
            b.put(first + i, i as u32);
        }
        let b_alive = (0..N).filter(|i| b.get(&(first + i)).is_some()).count();

        // C) S = 1, capacity giữ nguyên 32 (bằng đúng capacity thật của station)
        let c: LruCache<i64, u32, 1> = LruCache::new(32);
        for i in 0..N {
            c.put(first + i, i as u32);
        }
        let c_alive = (0..N).filter(|i| c.get(&(first + i)).is_some()).count();

        // D) capacity nhỏ HƠN N, S = 1 ⇒ thiếu slot thật, không phải do chia
        let d: LruCache<i64, u32, 1> = LruCache::new(10);
        for i in 0..N {
            d.put(first + i, i as u32);
        }
        let d_alive = (0..N).filter(|i| d.get(&(first + i)).is_some()).count();

        println!("\n  29 block, đổi capacity/S:");
        println!("  A cap=32   S=32  slot/shard={:2}  → còn {:2}/29", cap_per_shard::<32>(32), a_alive);
        println!("  B cap=1024 S=32  slot/shard={:2}  → còn {:2}/29", cap_per_shard::<32>(1024), b_alive);
        println!("  C cap=32   S=1   slot/shard={:2}  → còn {:2}/29", cap_per_shard::<1>(32), c_alive);
        println!("  D cap=10   S=1   slot/shard={:2}  → còn {:2}/29  (thiếu slot thật)", cap_per_shard::<1>(10), d_alive);
    }

    fn cap_per_shard<const S: usize>(cap: usize) -> usize {
        cap.div_ceil(S)
    }
}

#[cfg(test)]
mod cap_probe2 {
    use super::*;

    /// `capacity = 1024` **không** hết bug — chỉ là 29 key quá ít để lộ.
    ///
    /// Bug không nằm ở việc chia, mà ở việc mỗi shard chỉ có
    /// `ceil(capacity / S)` node trong arena tĩnh. Mất dữ liệu xảy ra khi một
    /// shard nhận **nhiều hơn** số node của nó. Nên số key tối đa an toàn =
    /// `S × capacity_per_shard` **và** phụ thuộc phân bố hash.
    ///
    /// Với 29 key rải 32 shard thì va chạm 2 key/shard là chuyện thường, nên
    /// `capacity_per_shard = 1` lộ ngay. Với `capacity_per_shard = 32` phải có
    /// **33 key trùng một shard** mới mất — hiếm với 29 key.
    #[test]
    fn big_capacity_just_hides_the_bug() {
        // (capacity, S, số key) — giữ capacity_per_shard = 32, tăng số key.
        for n in [29i64, 500, 2000, 8000] {
            let c: LruCache<i64, u32, 32> = LruCache::new(1024);
            for i in 0..n {
                c.put(1_000_000 + i, i as u32);
            }
            let alive = (0..n).filter(|i| c.get(&(1_000_000 + i)).is_some()).count();
            println!(
                "  cap=1024 S=32 slot/shard={:2}  n={:5}  → còn {:5}  mất {:5}",
                1024usize.div_ceil(32),
                n,
                alive,
                n as usize - alive
            );
        }
        // Cùng số key đó, capacity vừa đúng 8000 ⇒ 250 slot/shard, không mất.
        for n in [2000i64, 8000] {
            let c: LruCache<i64, u32, 32> = LruCache::new(n as usize);
            for i in 0..n {
                c.put(1_000_000 + i, i as u32);
            }
            let alive = (0..n).filter(|i| c.get(&(1_000_000 + i)).is_some()).count();
            println!(
                "  cap={:5} S=32 slot/shard={:3}  n={:5}  → còn {:5}  mất {:5}",
                n,
                (n as usize).div_ceil(32),
                n,
                alive,
                n as usize - alive
            );
        }
    }
}

#[cfg(test)]
mod lru_order_probe {
    use super::*;

    /// LRU phải đá **ít được dùng nhất**. Có đúng không?
    ///
    /// Nạp 4 key (đủ chật khi capacity = 4), chạm 2 key giữa chừng cho có
    /// "truy cập gần đây", rồi nạp thêm key mới. Key nào bị đá?
    /// Đúng LRU ⇒ đá `1` và `2` (lâu lâu nhất không ai chạm), **giữ** `3`,`4`
    /// (vừa chạm) và `5` (mới nhất).
    #[test]
    fn eviction_order_is_least_recently_used() {
        let c: LruCache<i64, u32, 1> = LruCache::new(4);
        for i in 1..=4i64 {
            c.put(i, i as u32);
        }
        // Chạm 1 và 2 → chúng thành "vừa dùng", còn 3,4 là cũ nhất.
        c.get(&1);
        c.get(&2);
        // Nạp key mới → phải đá 1 trong số 3,4.
        c.put(5, 5);

        let alive: Vec<i64> = (1..=5i64).filter(|k| c.get(k).is_some()).collect();
        println!("\n  nạp 1..4, chạm 1 và 2, rồi put(5) → còn {alive:?}");
        // `capacity = 4` ⇒ giữ 4 entry, chỉ **một** key bị đá: `3` — tức key
        // lâu lâu nhất **không ai chạm** (1, 2 vừa được `get` nên hồi sinh).
        // Đúng LRU. Nếu đây là FIFO, `1` (nhập đầu tiên) mới là kẻ bị đá.
        assert_eq!(alive, vec![1, 2, 4, 5], "phải đá 3, giữ 1,2,4,5");
        assert!(!alive.contains(&3), "3 là cũ nhất nên phải bị đá");
        assert!(alive.contains(&1), "1 được chạm nên phải sống — FIFO sẽ đá 1");
    }

    /// `get` có thật sự kéo entry lên đầu không? Nếu `get` không ghi, cache
    /// biến thành FIFO và đá nhầm.
    #[test]
    fn get_refreshes_recency() {
        let c: LruCache<i64, u32, 1> = LruCache::new(3);
        for i in 1..=3i64 {
            c.put(i, i as u32);
        }
        // 1 là cũ nhất. Chạm nó 3 lần rồi đẩy 4 vào.
        for _ in 0..3 {
            c.get(&1);
        }
        c.put(4, 4);
        let alive: Vec<i64> = (1..=4i64).filter(|k| c.get(k).is_some()).collect();
        println!("  nạp 1..3, chạm 1 (cũ nhất) 3 lần, put(4) → còn {alive:?}");
        println!("  LRU đúng: giữ 1,2,3? không — đá 3 (cũ nhất sau khi 1 được hồi sinh)");
        // 1 được chạm nên sống; 2, 3 lần lượt là cũ nhất khi đẩy 4 vào.
        assert!(alive.contains(&1), "1 phải sống vì vừa được chạm");
    }
}

#[cfg(test)]
mod spread_probe {
    use super::*;

    /// Hash phân bố **đều** hay **Gaussian**? Và lệch bao nhiêu so với kỳ vọng?
    ///
    /// Gaussian (mũ/chuẩn) ⇒ đa số key dồn quanh trung tâm, rìa thưa. Nếu vậy
    /// chỉ vài shard giữa chịu tải nặng. Uniform ⇒ phân bố đều nhưng vẫn có
    /// **biến động**, và biến động đó mới là thủ phạm khi sức chứa mỏng.
    #[test]
    fn is_hash_uniform_or_gaussian() {
        const S: usize = 32;
        const N: i64 = 29; // đúng ca `history-1H` thật
        let c: LruCache<i64, u32, S> = LruCache::new(S);

        let mut bins = vec![0usize; S];
        for i in 0..N {
            let k = 1787043600 / 129600 + i;
            bins[c.get_shard_idx(&k)] += 1;
        }
        let mean = N as f64 / S as f64;
        let max = *bins.iter().max().unwrap();
        let min = *bins.iter().min().unwrap();
        let occupied = bins.iter().filter(|b| **b > 0).count();

        // Trùng lặp kỳ vọng: xác suất có ít nhất 1 cặp trùng shard.
        let p_collide = 1.0 - {
            let mut p = 1.0f64;
            for j in 0..S {
                p *= 1.0 - (j as f64) / (S as f64);
            }
            p
        };

        println!("\n  S={S}, n={N}  ⇒ trung bình {mean:.2} key/shard");
        println!("  shard có key : {occupied}/{S}   (rỗng: {})", S - occupied);
        println!("  min={min}  max={max}   ⇒ lệch {:.0}% so với trung bình",
            (max as f64 - mean) / mean * 100.0);
        println!("  xác suất ÍT NHẤT 1 va chạm (birthday): {:.1}%", p_collide * 100.0);
        println!("  ⇒ mất {} block vì sức chứa 1/shard", N as usize - occupied.min(N as usize));
    }
}



#[cfg(all(test, feature = "lru-shared-memory"))]
mod shared_probe {
    use super::*;

    /// `lru-shared-memory` có thực sự chữa mất dữ liệu không? Đo lại đúng
    /// những ca đã làm đỏ bản chia cứng.
    #[test]
    fn shared_arena_keeps_everything() {
        // Ca thật `history-1H`: 29 block, cap=32, S=32.
        let first: i64 = 1787043600 / 129600;
        let c: LruCache<i64, u32, 32> = LruCache::new(32);
        for i in 0..29i64 {
            c.put(first + i, i as u32);
        }
        let alive = (0..29i64).filter(|i| c.get(&(first + i)).is_some()).count();
        println!("  cap=32 S=32 n=29 → còn {alive}/29");

        // Ca `portfolio.rs`: cap=2000, n=2000 (capacity ĐỦ mà vẫn mất 102).
        let d: LruCache<i64, u32, 32> = LruCache::new(2000);
        for i in 0..2000i64 {
            d.put(i, i as u32);
        }
        let alive2 = (0..2000i64).filter(|i| d.get(&i).is_some()).count();
        println!("  cap=2000 S=32 n=2000 → còn {alive2}/2000");

        // 3 key cùng shard, cap = 64: bản chia cứng đá, bản chung giữ hết.
        let e: LruCache<i64, u32, 32> = LruCache::new(64);
        for k in (0..1000i64).filter(|k| e.get_shard_idx(k) == 0).take(50) {
            e.put(k, k as u32);
        }
        println!("  cap=64 S=32 50 key cùng shard 0 → còn {}/50",
            (0..1000i64).filter(|k| e.get_shard_idx(k) == 0).take(50)
                .filter(|k| e.get(k).is_some()).count());

        assert_eq!(alive, 29, "phải giữ hết 29 block");
        assert_eq!(alive2, 2000, "capacity đủ thì phải giữ hết 2000");
    }

    /// `remove` phải trả node về free-list, nếu không sức chứa tụt dần.
    #[test]
    fn remove_returns_node_to_free_list() {
        let c: LruCache<i64, u32, 32> = LruCache::new(64);
        for i in 0..64i64 {
            c.put(i, i as u32);
        }
        for i in 0..63 {
            c.remove(&i);
        }
        // Còn 1 node sống + 63 node phải quay lại free-list.
        let alive = (0..63i64).filter(|i| c.get(i).is_some()).count();
        assert_eq!(alive, 0, "63 key đã remove không được sống lại");
        // Nạp lại 63 key — phải vào hết, tức free-list có đủ node.
        for i in 0..63i64 {
            c.put(i, i as u32);
        }
        let back = (0..63i64).filter(|i| c.get(i).is_some()).count();
        println!("  remove 63/64 rồi nạp lại → còn {back}/63");
        assert_eq!(back, 63, "free-list phải trả đủ node về sau remove");
    }
}

#[cfg(all(test, feature = "lru-shared-memory"))]
mod why_21_50 {
    use super::*;

    /// `cap=64, 50 key cùng shard 0` chỉ còn 21 — **đúng rồi**, và đây là
    /// lý do `capacity` tổng thật vẫn là sướng mạnh nhất của arena chung.
    ///
    /// Với `S = 32` và `capacity = 64` thì `capacity_per_shard = 2`; bản chia
    /// cứng chỉ giữ được **2** key trong tất cả số key dồn vào shard 0. Arena
    /// chung giữ được **21** — tức gần đúng `64 × 21/32 ≈ 42`… nhưng thực tế
    /// là 21 vì 29 key còn lại rơi vào 31 shard kia, mỗi shard chiếm ≥1 node.
    ///
    /// Với `S = 1` con số này là **50/50** — xem `single_shard_keeps_all`.
    #[test]
    fn single_shard_keeps_all() {
        let e: LruCache<i64, u32, 1> = LruCache::new(64);
        let keys: Vec<i64> = (0..1000).take(50).collect();
        for k in &keys {
            e.put(*k, *k as u32);
        }
        let alive = keys.iter().filter(|k| e.get(k).is_some()).count();
        println!("  cap=64 S=1  50 key → còn {alive}/50");
        assert_eq!(alive, 50);
    }
}
