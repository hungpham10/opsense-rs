//! Radix-node storage — the only persistence surface for the radix tree.
//!
//! Storage chỉ lưu các node của radix tree: prefix + record + children + root
//! của từng shard. Mọi thao tác thay đổi cấu trúc cây đi qua một **transaction**
//! (`Tx`) để áp dụng atomic — không có trạng thái trung gian lộ ra cho reader.
//!
//! Các khái niệm cũ (automaton, entries, blob, shard-compressed) đã bị xoá
//! trong đợt refactor — nếu cần persistence tầng cao hơn thì phải làm ở tầng
//! khác, không phải ở đây.

use async_trait::async_trait;
use std::fmt;

#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "redis")]
mod redis;

mod in_memory;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorage;

/// Re-export để `InMemoryStorage` vẫn reachable tại `crate::storage` (impl đã
/// chuyển vào `storage::in_memory`).
pub use in_memory::InMemoryStorage;

// ==================== Error Type ====================

#[derive(Debug)]
pub enum StorageError {
    BranchOutOfRange(usize),
    Internal(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::BranchOutOfRange(id) => write!(f, "branch id {id} out of range"),
            StorageError::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Node id 0 là sentinel (rỗng) — dùng để đánh dấu "không có" trong radix.
pub const EMPTY: usize = 0;

// ==================== Transaction ====================

/// Một mutation lẻ trong transaction.
#[derive(Clone, Debug)]
enum CategoryTxOp {
    AddChild {
        parent: usize,
        child: usize,
    },
    MoveChild {
        from: usize,
        to: usize,
        child: usize,
    },
    UpdateNode {
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    },
}

/// Transaction — buffer toàn bộ mutation và áp dụng atomic tại `commit`.
///
/// `new_node` reserve id **ngay lập tức** (từ counter của storage) để caller
/// (radix split) có thể dùng id làm tham chiếu trước khi commit; nhưng node
/// chưa lộ ra cho reader cho tới khi `commit` hoàn tất.
///
/// `commit(self: Box<Self>)` tiêu thụ chính transaction — không thể commit 2 lần.
#[async_trait]
pub trait CategoryTx: Send {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize>;
    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()>;
    async fn add_child(&mut self, parent: usize, child: usize) -> Result<()>;
    async fn move_child(&mut self, from: usize, to: usize, child: usize) -> Result<()>;
    async fn commit(self: Box<Self>) -> Result<()>;
}

// ==================== Storage traits ====================

/// TimeseriesStorage — append-only time-series persistence surface.
///
/// Mỗi series được identify bởi một opaque key (`&[u8]`). Mỗi điểm (point) gồm
/// `(timestamp, value)`: `timestamp` là u64 (caller quyết định đơn vị — ms hay
/// ns), `value` là opaque bytes (VD: serialize của `V` trong `LruCache`).
///
/// Dùng để lưu lịch sử các giá trị bị evict / update khỏi `crate::lru::LruCache`
/// (qua `LruCache::attach_timeseries`): mỗi lần một entry rời khỏi cache, một
/// điểm `(thời_điểm, giá_trị)` được append vào series tương ứng với key đó.
///
/// **Khác với `CategoryStorage`/`NodeMetaStorage`: các method ở đây lấy `&self`
/// (không phải `&mut self`).** Lý do: `LruCache` giữ storage sau
/// `Arc<dyn TimeseriesStorage>` và gọi `.append().await` *bên ngoài* shard-lock,
/// nên trait object phải dùng interior mutability (VD: `Mutex`/`RwLock` bên trong
/// impl). Các backend async (SQLite/Redis) vốn đã tự lock qua connection pool nên
/// `&self` rất tự nhiên. Method có default no-op → backend nào không quan tâm đến
/// timeseries có thể kế thừa trait này mà không cần impl.
#[async_trait]
pub trait TimeseriesStorage: Send + Sync {
    /// Append một điểm `(timestamp, value)` vào `series`. Mặc định: no-op.
    async fn append(&self, _series: &[u8], _timestamp: u64, _value: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Đọc các điểm của `series` trong khoảng `[start_ts, end_ts]` (inclusive),
    /// sắp xếp tăng dần theo timestamp. Mặc định: rỗng.
    async fn range(
        &self,
        _series: &[u8],
        _start_ts: u64,
        _end_ts: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        Ok(vec![])
    }

    /// Đọc `limit` điểm gần nhất (mới nhất) của `series`. Mặc định: rỗng.
    async fn latest(&self, _series: &[u8], _limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        Ok(vec![])
    }

    /// Đọc điểm cũ nhất của `series` — `None` nếu rỗng. Mặc định: `None`.
    async fn first(&self, _series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        Ok(None)
    }

    /// Đọc điểm mới nhất của `series` — `None` nếu rỗng. Mặc định: `None`.
    async fn last(&self, _series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        Ok(None)
    }

    /// Xoá toàn bộ điểm của một `series`. Mặc định: no-op.
    async fn clear_series(&self, _series: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Xoá toàn bộ series (dùng khi rebuild index). Mặc định: no-op.
    async fn clear_all_series(&self) -> Result<()> {
        Ok(())
    }
}

/// PatternStorage — persistence surface cho các pattern đã đăng ký trong
/// `crate::ahocorasick::AhoCorasick`.
///
/// Mỗi pattern là một `String` (lưu nguyên bản gốc — caller quyết định đơn vị
/// split khi build automaton qua `split_fn`). Dùng để lưu registry pattern
/// (tương đương `pattern_mapping` / `patterns` của `AhoCorasick`) sao cho khi
/// process restart có thể rebuild automaton qua `optimize()` mà không mất danh
/// sách pattern.
///
/// **Các method lấy `&self` (không phải `&mut self`).** Lý do: `AhoCorasick`
/// có thể giữ storage sau `Arc<dyn PatternStorage>` và gọi `.add().await` bên
/// ngoài bất kỳ lock nào, nên trait object phải dùng interior mutability
/// (`Mutex`/`RwLock` bên trong impl). Các backend async (SQLite/Redis) vốn tự
/// lock qua connection pool nên `&self` rất tự nhiên. Method có default no-op →
/// backend nào không quan tâm đến pattern persistence có thể kế thừa trait này
/// mà không cần impl.
// Public-API trait for the station rebuild (§1); not yet consumed within this
// crate, so silence the `-D warnings` dead-code lint until a backend uses it.
#[allow(dead_code)]
#[async_trait]
pub trait PatternStorage: Send + Sync {
    /// Đăng ký một pattern (dedup theo giá trị; pattern rỗng bị bỏ qua).
    /// Mặc định: no-op.
    async fn add(&self, _pattern: &str) -> Result<()> {
        Ok(())
    }

    /// Kiểm tra pattern đã được đăng ký chưa — `false` nếu chưa. Mặc định: `false`.
    async fn contains(&self, _pattern: &str) -> Result<bool> {
        Ok(false)
    }

    /// Đọc toàn bộ pattern đã đăng ký (sắp xếp theo thứ tự đăng ký). Mặc định: rỗng.
    async fn get_all(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    /// Số lượng pattern đã đăng ký. Mặc định: 0.
    async fn count(&self) -> Result<usize> {
        Ok(0)
    }

    /// Xoá một pattern, trả về `true` nếu pattern tồn tại trước khi xoá.
    /// Mặc định: `false`.
    async fn remove(&self, _pattern: &str) -> Result<bool> {
        Ok(false)
    }

    /// Xoá toàn bộ pattern (dùng khi rebuild index). Mặc định: no-op.
    async fn clear(&self) -> Result<()> {
        Ok(())
    }
}

/// Bloom-filter storage (feature `bloom-search`): lưu/đọc serialized bloom
/// filter của mỗi node để `Radix::search_dfs` prune nhánh không chứa substring.
///
/// Tách riêng khỏi `CategoryStorage` để trait lõi không bị rưới `#[cfg]`
/// feature. `CategoryStorage` kế thừa trait này (khi feature bật) → method vẫn
/// gọi được qua `dyn CategoryStorage` như cũ. Backend nào không override →
/// mặc định no-op (`None` khi đọc).
#[cfg(feature = "bloom-search")]
#[async_trait]
pub trait BloomStorage: Send + Sync {
    /// Lưu serialize bloom filter của node (opaque bytes). Mặc định: no-op.
    async fn set_node_bloom(&mut self, _id: usize, _bloom: &[u8]) -> Result<()> {
        Ok(())
    }
    /// Đọc serialize bloom filter của node — `None` nếu chưa có. Mặc định: `None`.
    async fn get_node_bloom(&self, _: usize) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// Node-metadata storage: lưu/đọc metadata của node (opaque bytes, VD Node JSON)
/// keyed theo element id, cùng `clear`. Tách riêng khỏi `CategoryStorage` để trait
/// lõi gọn. `CategoryStorage` kế thừa trait này (luôn) → method gọi được qua
/// `dyn CategoryStorage` (radix `register_node` / `Search::get_node_meta` dùng).
/// Mặc định no-op.
#[async_trait]
pub trait NodeMetaStorage: Send + Sync {
    /// Lưu metadata của node (opaque bytes, VD Node JSON) keyed theo element id.
    /// Mặc định: no-op.
    async fn set_node_meta(&mut self, _elem: usize, _meta: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Đọc node metadata — `None` nếu node chưa có. Mặc định: `None`.
    async fn get_node_meta(&self, _elem: usize) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Xoá toàn bộ node stream (dùng khi rebuild index). Mặc định: no-op.
    async fn clear_node_meta(&mut self) -> Result<()> {
        Ok(())
    }

    /// Lưu metadata (opaque bytes, VD: call-site info) cho một record — keyed
    /// theo record index (không phải element id). Mặc định: no-op.
    async fn set_meta(&mut self, _record: usize, _meta: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Đọc metadata của record — `None` nếu record chưa có meta. Mặc định: `None`.
    async fn get_meta(&self, _record: usize) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Lưu độ dài key (số element) của record — dùng filter `depth` khi search.
    /// Mặc định: no-op.
    async fn set_key_len(&mut self, _record: usize, _len: usize) -> Result<()> {
        Ok(())
    }

    /// Đọc độ dài key của record — `None` nếu record chưa insert. Mặc định: `None`.
    async fn get_key_len(&self, _record: usize) -> Result<Option<usize>> {
        Ok(None)
    }
}

/// Shortcut storage: auxiliary index for LIKE-search substring matching.
/// Stores which nodes contain each element in their prefix for fast candidate
/// lookup (KMP + DFS). Tách riêng khỏi `CategoryStorage` để trait lõi gọn.
/// `CategoryStorage` kế thừa trait này (luôn) → method gọi được qua
/// `dyn CategoryStorage` (Search::search dùng). Mặc định no-op.
#[async_trait]
pub trait ShortcutsStorage: Send + Sync {
    /// Thêm `node_id` vào shortcut set của element `elem` (encoded bytes).
    /// Shortcut set = mọi node có chứa element này trong prefix của nó — dùng
    /// làm candidate khi tìm substring (KMP + DFS).
    async fn add_shortcut_node(
        &mut self,
        _shard: usize,
        _elem: &[u8],
        _node_id: usize,
    ) -> Result<()> {
        Ok(())
    }

    /// Lấy toàn bộ node id chứa element `elem` trong shard.
    async fn get_shortcut_nodes(&self, _shard: usize, _elem: &[u8]) -> Result<Vec<usize>> {
        Ok(vec![])
    }

    /// Xoá toàn bộ shortcut sets (dùng khi rebuild index từ tree).
    async fn clear_shortcuts(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Edge-data storage: lưu/đọc metadata của mỗi edge id (opaque bytes, VD
/// CallEdgeMeta JSON) keyed theo edge id. Tách riêng khỏi `CategoryStorage` để
/// trait lõi gọn. `CategoryStorage` kế thừa trait này (luôn) → method gọi được
/// qua `dyn CategoryStorage` (Search dùng). Mặc định no-op.
#[async_trait]
pub trait EdgeDataStorage: Send + Sync {
    /// Lưu dữ liệu edge (opaque bytes, VD CallEdgeMeta JSON) keyed theo edge id.
    /// Mặc định: no-op.
    async fn set_edge_data(&mut self, _edge: usize, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Đọc dữ liệu edge — `None` nếu edge chưa có. Mặc định: `None`.
    async fn get_edge_data(&self, _: usize) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Xoá toàn bộ edge stream (dùng khi rebuild index). Mặc định: no-op.
    async fn clear_edges(&mut self) -> Result<()> {
        Ok(())
    }

    /// Duyệt toàn bộ edge data `(edge_id, meta)` theo thứ tự bất kỳ — dùng để
    /// rebuild edge registry khi reopen (CallEdgeMeta chứa from/to). Mặc định:
    /// không có edge nào.
    async fn for_each_edge_data(
        &self,
        _f: &mut (dyn for<'a> FnMut(usize, &'a [u8]) -> Result<()> + Send),
    ) -> Result<()> {
        Ok(())
    }
}

/// Chain storage: lưu/đọc per-owner chain (marker + symbol element ids),
/// encode u64 LE 8-byte/element. Tách riêng khỏi `CategoryStorage` để trait
/// lõi gọn. `CategoryStorage` kế thừa trait này (luôn) → method gọi được qua
/// `dyn CategoryStorage` (Search::insert_chain / Search::get_chain dùng).
/// Mặc định no-op.
#[async_trait]
pub trait ChainStorage: Send + Sync {
    /// Lưu chain của owner (keyed theo record của owner; u64 LE 8-byte/element).
    /// Mặc định: no-op.
    async fn set_chain(&mut self, _record: usize, _chain: &[u64]) -> Result<()> {
        Ok(())
    }

    /// Đọc chain của owner — `None` nếu owner chưa có chain. Mặc định: `None`.
    async fn get_chain(&self, _record: usize) -> Result<Option<Vec<u64>>> {
        Ok(None)
    }

    /// Xoá toàn bộ chains (dùng khi rebuild index). Mặc định: no-op.
    async fn clear_chains(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Khai báo `CategoryStorage` — macro emit TOÀN BỘ trait (gồm `#[async_trait]`)
/// nên async_trait biến đổi ĐÚNG sau khi macro nở (khắc lỗi macro body trong
/// trait). `$bounds` = danh sách supertrait: luôn `Send + Sync + NodeMetaStorage`,
/// cộng `BloomStorage` khi feature `bloom-search`. Thân method không có `#[cfg]`
/// rải rác.
macro_rules! declare_category_storage {
    ($($bounds:tt)*) => {
        /// Radix-node storage: node management + transaction.
        #[async_trait]
        pub trait CategoryStorage: $($bounds)* {
            // ── Node management ──
            async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize>;
            async fn update_node(
                &mut self,
                id: usize,
                prefix: Option<Vec<u8>>,
                record: Option<usize>,
            ) -> Result<()>;
            async fn get_node(&self, id: usize) -> Result<(Vec<u8>, usize)>;
            async fn get_children(&self, id: usize) -> Result<Vec<usize>>;

            // ── Shard roots (endpoint) ──
            async fn set_root(&mut self, shard: usize, root: usize) -> Result<()>;
            async fn get_root(&self, shard: usize) -> Result<usize>;

            // ── Transaction ──
            /// Bắt đầu một transaction (sync, không await — đúng theo cách radix gọi).
            /// Buffer ops; mọi thay đổi chỉ lộ ra khi `commit`.
            fn new_tx(&self) -> Box<dyn CategoryTx>;
        }
    };
}

#[cfg(feature = "bloom-search")]
declare_category_storage!(
    Send + Sync
        + NodeMetaStorage
        + ShortcutsStorage
        + EdgeDataStorage
        + ChainStorage
        + BloomStorage
);

#[cfg(not(feature = "bloom-search"))]
declare_category_storage!(
    Send + Sync + NodeMetaStorage + ShortcutsStorage + EdgeDataStorage + ChainStorage
);

/// Encode chain thành bytes (u64 little-endian, 8 byte/element) — format của
/// chain stream. Chain = chuỗi element id (marker + symbol) của một hàm.
#[inline]
fn encode_chain(chain: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(chain.len() * 8);
    for e in chain {
        out.extend_from_slice(&e.to_le_bytes());
    }
    out
}

/// Decode bytes trong chain stream về `Vec<u64>` element ids.
// chỉ dùng qua get_chain (test/sqlite builds)
#[inline]
fn decode_chain(bytes: &[u8]) -> Vec<u64> {
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| u64::from_le_bytes(*c))
        .collect()
}
