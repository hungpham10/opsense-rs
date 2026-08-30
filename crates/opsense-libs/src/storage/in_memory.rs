use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use super::{
    CategoryStorage, CategoryTx, CategoryTxOp, ChainStorage, EMPTY, EdgeDataStorage,
    NodeMetaStorage, PatternStorage, Result, ShortcutsStorage, StorageError, TimeseriesStorage,
    decode_chain, encode_chain,
};

/// Transaction cho `InMemoryStorage`: buffer toàn bộ mutation, áp dụng
/// atomic dưới 1 write lock tại `commit`.
pub(crate) struct InMemoryTx {
    data: Arc<RwLock<MemoryData>>,
    next_id: Arc<AtomicUsize>,
    /// (reserved_id, prefix, record) — được append tại commit.
    nodes: Vec<(usize, Vec<u8>, usize)>,
    ops: Vec<CategoryTxOp>,
}

impl InMemoryTx {
    pub(crate) fn new(data: Arc<RwLock<MemoryData>>, next_id: Arc<AtomicUsize>) -> Self {
        Self {
            data,
            next_id,
            nodes: Vec::new(),
            ops: Vec::new(),
        }
    }
}

#[async_trait]
impl CategoryTx for InMemoryTx {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.nodes.push((id, prefix, record));
        Ok(id)
    }

    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()> {
        self.ops
            .push(CategoryTxOp::UpdateNode { id, prefix, record });
        Ok(())
    }

    async fn add_child(&mut self, parent: usize, child: usize) -> Result<()> {
        self.ops.push(CategoryTxOp::AddChild { parent, child });
        Ok(())
    }

    async fn move_child(&mut self, from: usize, to: usize, child: usize) -> Result<()> {
        self.ops.push(CategoryTxOp::MoveChild { from, to, child });
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        let InMemoryTx {
            data, nodes, ops, ..
        } = *self;

        let mut d = data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;

        // 1. Materialize các node đã reserve (đảm bảo children[leg] tồn tại
        //    trước khi ops move/add trỏ tới).
        for (id, prefix, record) in nodes {
            if d.nodes.len() <= id {
                d.nodes.resize(id + 1, (vec![], EMPTY));
                d.children.resize(id + 1, vec![]);
            }
            d.nodes[id] = (prefix, record);
        }

        // 2. Áp dụng toàn bộ ops — tất cả cùng thành công hoặc cùng thất bại
        //    (single write lock → không lộ trạng thái trung gian).
        for op in ops {
            match op {
                CategoryTxOp::AddChild { parent, child } => {
                    if parent < d.children.len() && !d.children[parent].contains(&child) {
                        d.children[parent].push(child);
                    }
                }
                CategoryTxOp::MoveChild { from, to, child } => {
                    if from < d.children.len() {
                        d.children[from].retain(|&c| c != child);
                    }
                    if to < d.children.len() && !d.children[to].contains(&child) {
                        d.children[to].push(child);
                    }
                }
                CategoryTxOp::UpdateNode { id, prefix, record } => {
                    if id < d.nodes.len() {
                        if let Some(p) = prefix {
                            d.nodes[id].0 = p;
                        }
                        if let Some(r) = record {
                            d.nodes[id].1 = r;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

// ==================== In-Memory Storage ====================
//
pub(crate) struct MemoryData {
    /// (prefix, record) — index 0 là sentinel.
    pub(crate) nodes: Vec<(Vec<u8>, usize)>,
    /// children list per node (index 0 = sentinel).
    pub(crate) children: Vec<Vec<usize>>,
    /// root id per shard.
    pub(crate) roots: Vec<usize>,
    /// record_idx → metadata (opaque bytes, VD: call-site info).
    pub(crate) meta: HashMap<usize, Vec<u8>>,
    /// record_idx → độ dài key (số element) — dùng filter `depth` khi search.
    pub(crate) key_lens: HashMap<usize, usize>,
    /// shortcuts[shard][elem_bytes] = node ids chứa elem trong prefix.
    pub(crate) shortcuts: Vec<HashMap<Vec<u8>, HashSet<usize>>>,
    /// edge id → dữ liệu edge (opaque bytes, VD EdgeMeta JSON).
    pub(crate) edges: HashMap<usize, Vec<u8>>,
    /// element id → node metadata (Node JSON).
    pub(crate) node_meta: HashMap<usize, Vec<u8>>,
    /// node id → serialize bloom filter (prune nhánh trong search_dfs).
    #[cfg(feature = "bloom-search")]
    pub(crate) blooms: HashMap<usize, Vec<u8>>,
    /// record (owner) → chain bytes (u64 LE 8-byte/element).
    pub(crate) chains: HashMap<usize, Vec<u8>>,
    /// timeseries: series (bytes) → danh sách điểm `(timestamp, value)` tăng dần.
    /// Dùng chung state sau 1 RwLock nên gọi được qua `&self`.
    pub(crate) timeseries: HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>,
    /// patterns: registered Aho-Corasick patterns → auto-increment id (tương đương
    /// `pattern_mapping` của `AhoCorasick`). Dùng chung state sau 1 RwLock.
    #[allow(dead_code)]
    pub(crate) patterns: BTreeMap<String, usize>,
}

/// In-memory radix storage. Thread-safe: toàn bộ state nằm sau 1 RwLock;
/// id được cấp bằng AtomicUsize nên các transaction song song không trùng id.
pub struct InMemoryStorage {
    data: Arc<RwLock<MemoryData>>,
    next_id: Arc<AtomicUsize>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(MemoryData {
                nodes: vec![(vec![], EMPTY)], // sentinel
                children: vec![vec![]],
                roots: vec![],
                meta: HashMap::new(),
                key_lens: HashMap::new(),
                shortcuts: vec![],
                edges: HashMap::new(),
                node_meta: HashMap::new(),
                #[cfg(feature = "bloom-search")]
                blooms: HashMap::new(),
                chains: HashMap::new(),
                timeseries: HashMap::new(),
                patterns: BTreeMap::new(),
            })),
            next_id: Arc::new(AtomicUsize::new(1)),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    /// Reserve một id mới (dùng chung cho cả new_node trực tiếp lẫn tx).
    fn alloc_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl CategoryStorage for InMemoryStorage {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let id = self.alloc_id();
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        if d.nodes.len() <= id {
            d.nodes.resize(id + 1, (vec![], EMPTY));
            d.children.resize(id + 1, vec![]);
        }
        d.nodes[id] = (prefix, record);
        Ok(id)
    }

    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        if id >= d.nodes.len() {
            return Err(StorageError::BranchOutOfRange(id));
        }
        if let Some(p) = prefix {
            d.nodes[id].0 = p;
        }
        if let Some(r) = record {
            d.nodes[id].1 = r;
        }
        Ok(())
    }

    async fn get_node(&self, id: usize) -> Result<(Vec<u8>, usize)> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        if id >= d.nodes.len() {
            return Err(StorageError::BranchOutOfRange(id));
        }
        Ok(d.nodes[id].clone())
    }

    async fn get_children(&self, id: usize) -> Result<Vec<usize>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.children.get(id).cloned().unwrap_or_default())
    }

    async fn set_root(&mut self, shard: usize, root: usize) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        if shard >= d.roots.len() {
            d.roots.resize(shard + 1, EMPTY);
        }
        d.roots[shard] = root;
        Ok(())
    }

    async fn get_root(&self, shard: usize) -> Result<usize> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.roots.get(shard).copied().unwrap_or(EMPTY))
    }

    fn new_tx(&self) -> Box<dyn CategoryTx> {
        Box::new(InMemoryTx::new(self.data.clone(), self.next_id.clone()))
    }
}

#[async_trait]
impl EdgeDataStorage for InMemoryStorage {
    async fn set_edge_data(&mut self, edge: usize, data: &[u8]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.edges.insert(edge, data.to_vec());
        Ok(())
    }

    async fn get_edge_data(&self, edge: usize) -> Result<Option<Vec<u8>>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.edges.get(&edge).cloned())
    }

    async fn clear_edges(&mut self) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.edges.clear();
        Ok(())
    }

    async fn for_each_edge_data(
        &self,
        f: &mut (dyn for<'a> FnMut(usize, &'a [u8]) -> Result<()> + Send),
    ) -> Result<()> {
        let items: Vec<(usize, Vec<u8>)> = {
            let d = self
                .data
                .read()
                .map_err(|_| StorageError::Internal("poison".into()))?;
            d.edges
                .iter()
                .map(|(&id, data)| (id, data.clone()))
                .collect()
        };
        for (id, data) in items {
            f(id, &data)?;
        }
        Ok(())
    }
}

#[async_trait]
impl ChainStorage for InMemoryStorage {
    async fn set_chain(&mut self, record: usize, chain: &[u64]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.chains.insert(record, encode_chain(chain));
        Ok(())
    }

    async fn get_chain(&self, record: usize) -> Result<Option<Vec<u64>>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.chains.get(&record).map(|b| decode_chain(b)))
    }

    async fn clear_chains(&mut self) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.chains.clear();
        Ok(())
    }
}

#[async_trait]
impl ShortcutsStorage for InMemoryStorage {
    async fn add_shortcut_node(&mut self, shard: usize, elem: &[u8], node_id: usize) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        if shard >= d.shortcuts.len() {
            d.shortcuts.resize(shard + 1, HashMap::new());
        }
        d.shortcuts[shard]
            .entry(elem.to_vec())
            .or_default()
            .insert(node_id);
        Ok(())
    }

    async fn get_shortcut_nodes(&self, shard: usize, elem: &[u8]) -> Result<Vec<usize>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.shortcuts
            .get(shard)
            .and_then(|m| m.get(elem))
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default())
    }

    async fn clear_shortcuts(&mut self) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        for map in d.shortcuts.iter_mut() {
            map.clear();
        }
        Ok(())
    }
}

#[async_trait]
impl NodeMetaStorage for InMemoryStorage {
    async fn set_node_meta(&mut self, elem: usize, meta: &[u8]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.node_meta.insert(elem, meta.to_vec());
        Ok(())
    }

    async fn get_node_meta(&self, elem: usize) -> Result<Option<Vec<u8>>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.node_meta.get(&elem).cloned())
    }

    async fn clear_node_meta(&mut self) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.node_meta.clear();
        Ok(())
    }

    async fn set_meta(&mut self, record: usize, meta: &[u8]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.meta.insert(record, meta.to_vec());
        Ok(())
    }

    async fn get_meta(&self, record: usize) -> Result<Option<Vec<u8>>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.meta.get(&record).cloned())
    }

    async fn set_key_len(&mut self, record: usize, len: usize) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.key_lens.insert(record, len);
        Ok(())
    }

    async fn get_key_len(&self, record: usize) -> Result<Option<usize>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.key_lens.get(&record).copied())
    }
}

#[cfg(feature = "bloom-search")]
#[async_trait]
impl super::BloomStorage for InMemoryStorage {
    async fn set_node_bloom(&mut self, id: usize, bloom: &[u8]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.blooms.insert(id, bloom.to_vec());
        Ok(())
    }

    async fn get_node_bloom(&self, id: usize) -> Result<Option<Vec<u8>>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.blooms.get(&id).cloned())
    }
}

#[async_trait]
impl TimeseriesStorage for InMemoryStorage {
    async fn append(&self, series: &[u8], timestamp: u64, value: &[u8]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.timeseries
            .entry(series.to_vec())
            .or_default()
            .push((timestamp, value.to_vec()));
        Ok(())
    }

    async fn range(
        &self,
        series: &[u8],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.timeseries
            .get(series)
            .map(|pts| {
                pts.iter()
                    .filter(|(t, _)| *t >= start_ts && *t <= end_ts)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn latest(&self, series: &[u8], limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.timeseries
            .get(series)
            .map(|pts| {
                let start = pts.len().saturating_sub(limit);
                pts[start..].to_vec()
            })
            .unwrap_or_default())
    }

    async fn first(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.timeseries
            .get(series)
            .and_then(|pts| pts.first().cloned()))
    }

    async fn last(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.timeseries.get(series).and_then(|pts| pts.last().cloned()))
    }

    async fn clear_series(&self, series: &[u8]) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.timeseries.remove(series);
        Ok(())
    }

    async fn clear_all_series(&self) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.timeseries.clear();
        Ok(())
    }
}

#[async_trait]
impl PatternStorage for InMemoryStorage {
    async fn add(&self, pattern: &str) -> Result<()> {
        if pattern.is_empty() {
            return Ok(());
        }
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        if !d.patterns.contains_key(pattern) {
            let id = d.patterns.len();
            d.patterns.insert(pattern.to_string(), id);
        }
        Ok(())
    }

    async fn contains(&self, pattern: &str) -> Result<bool> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.patterns.contains_key(pattern))
    }

    async fn get_all(&self) -> Result<Vec<String>> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        // Sắp xếp theo id tăng dần → đúng thứ tự đăng ký.
        let mut v: Vec<(usize, &String)> = d.patterns.iter().map(|(k, &id)| (id, k)).collect();
        v.sort_by_key(|&(id, _)| id);
        Ok(v.into_iter().map(|(_, k)| k.clone()).collect())
    }

    async fn count(&self) -> Result<usize> {
        let d = self
            .data
            .read()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.patterns.len())
    }

    async fn remove(&self, pattern: &str) -> Result<bool> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        Ok(d.patterns.remove(pattern).is_some())
    }

    async fn clear(&self) -> Result<()> {
        let mut d = self
            .data
            .write()
            .map_err(|_| StorageError::Internal("poison".into()))?;
        d.patterns.clear();
        Ok(())
    }
}

// ==================== Tests (InMemory) ====================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{CategoryStorage, NodeMetaStorage, ShortcutsStorage};

    #[tokio::test]
    async fn test_new_node_and_get_node() {
        let mut s = InMemoryStorage::default();
        let id = s.new_node(b"hello".to_vec(), 42).await.unwrap();
        assert_ne!(id, EMPTY);
        let (prefix, record) = s.get_node(id).await.unwrap();
        assert_eq!(prefix, b"hello");
        assert_eq!(record, 42);
    }

    #[tokio::test]
    async fn test_update_node() {
        let mut s = InMemoryStorage::default();
        let id = s.new_node(b"init".to_vec(), 1).await.unwrap();
        s.update_node(id, Some(b"updated".to_vec()), Some(99))
            .await
            .unwrap();
        let (prefix, record) = s.get_node(id).await.unwrap();
        assert_eq!(prefix, b"updated");
        assert_eq!(record, 99);
    }

    #[tokio::test]
    async fn test_children_and_roots() {
        let mut s = InMemoryStorage::default();
        let parent = s.new_node(b"p".to_vec(), 0).await.unwrap();
        let c1 = s.new_node(b"c1".to_vec(), 1).await.unwrap();
        let c2 = s.new_node(b"c2".to_vec(), 2).await.unwrap();
        // Mutate qua Tx — production chỉ đi qua Tx, không có Storage::add_child.
        let mut tx = s.new_tx();
        tx.add_child(parent, c1).await.unwrap();
        tx.add_child(parent, c2).await.unwrap();
        tx.commit().await.unwrap();
        let children = s.get_children(parent).await.unwrap();
        assert_eq!(children.len(), 2);
        assert!(children.contains(&c1));
        assert!(children.contains(&c2));

        assert_eq!(s.get_root(3).await.unwrap(), EMPTY);
        s.set_root(3, parent).await.unwrap();
        assert_eq!(s.get_root(3).await.unwrap(), parent);
    }

    #[tokio::test]
    async fn test_meta_roundtrip() {
        let mut s = InMemoryStorage::default();
        // Chưa có gì → None.
        assert_eq!(s.get_meta(7).await.unwrap(), None);
        assert_eq!(s.get_key_len(7).await.unwrap(), None);
        s.set_meta(7, b"call-site-info".as_slice()).await.unwrap();
        s.set_key_len(7, 5).await.unwrap();
        assert_eq!(
            s.get_meta(7).await.unwrap().as_deref(),
            Some(b"call-site-info".as_slice())
        );
        assert_eq!(s.get_key_len(7).await.unwrap(), Some(5));
        // Ghi đè meta.
        s.set_meta(7, b"updated").await.unwrap();
        s.set_key_len(7, 6).await.unwrap();
        assert_eq!(
            s.get_meta(7).await.unwrap().as_deref(),
            Some(b"updated".as_slice())
        );
        assert_eq!(s.get_key_len(7).await.unwrap(), Some(6));
        // Record khác không ảnh hưởng.
        assert_eq!(s.get_meta(8).await.unwrap(), None);
        assert_eq!(s.get_key_len(8).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_shortcuts_roundtrip() {
        let mut s = InMemoryStorage::default();
        // Chưa có gì → empty.
        assert!(s.get_shortcut_nodes(1, b"l").await.unwrap().is_empty());
        s.add_shortcut_node(1, b"l", 10).await.unwrap();
        s.add_shortcut_node(1, b"l", 20).await.unwrap();
        s.add_shortcut_node(1, b"o", 10).await.unwrap();
        s.add_shortcut_node(2, b"l", 30).await.unwrap(); // shard khác
        let nodes = s.get_shortcut_nodes(1, b"l").await.unwrap();
        assert!(nodes.contains(&10) && nodes.contains(&20));
        assert_eq!(nodes.len(), 2);
        assert_eq!(s.get_shortcut_nodes(2, b"l").await.unwrap(), vec![30]);

        // Clear → rỗng hết.
        s.clear_shortcuts().await.unwrap();
        assert!(s.get_shortcut_nodes(1, b"l").await.unwrap().is_empty());
        assert!(s.get_shortcut_nodes(2, b"l").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_tx_commit_applies_atomically() {
        let mut s = InMemoryStorage::default();
        let parent = s.new_node(b"hello".to_vec(), 1).await.unwrap();

        let mut tx = s.new_tx();
        let new_id = tx.new_node(b"p".to_vec(), 2).await.unwrap();
        let leg_id = tx.new_node(b"lo".to_vec(), 1).await.unwrap();
        tx.move_child(parent, leg_id, 0).await.unwrap(); // no-op: 0 chưa phải child
        tx.add_child(parent, leg_id).await.unwrap();
        tx.add_child(parent, new_id).await.unwrap();
        tx.update_node(parent, Some(b"hel".to_vec()), Some(0))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let (prefix, record) = s.get_node(parent).await.unwrap();
        assert_eq!(prefix, b"hel");
        assert_eq!(record, 0);
        let children = s.get_children(parent).await.unwrap();
        assert!(children.contains(&leg_id));
        assert!(children.contains(&new_id));
        assert_eq!(s.get_node(new_id).await.unwrap().1, 2);
        assert_eq!(s.get_node(leg_id).await.unwrap().1, 1);
    }

    #[tokio::test]
    async fn test_tx_nodes_invisible_before_commit() {
        let s = InMemoryStorage::default();
        let mut tx = s.new_tx();
        let id = tx.new_node(b"pending".to_vec(), 9).await.unwrap();
        // Trước commit, node chưa materialize → get_node lỗi BranchOutOfRange.
        assert!(s.get_node(id).await.is_err());
        tx.commit().await.unwrap();
        assert_eq!(s.get_node(id).await.unwrap().1, 9);
    }

    #[tokio::test]
    async fn test_tx_move_child_migrates() {
        let mut s = InMemoryStorage::default();
        let parent = s.new_node(b"aaaaaa".to_vec(), 0).await.unwrap();
        let child = s.new_node(b"0".to_vec(), 1).await.unwrap();
        let mut seed = s.new_tx();
        seed.add_child(parent, child).await.unwrap();
        seed.commit().await.unwrap();

        let mut tx = s.new_tx();
        let leg = tx.new_node(b"a".to_vec(), 0).await.unwrap();
        tx.move_child(parent, leg, child).await.unwrap();
        tx.add_child(parent, leg).await.unwrap();
        tx.commit().await.unwrap();

        assert!(!s.get_children(parent).await.unwrap().contains(&child));
        assert!(s.get_children(leg).await.unwrap().contains(&child));
    }

    #[tokio::test]
    async fn test_edge_data_roundtrip() {
        let mut s = InMemoryStorage::default();
        // Chưa có edge → None.
        assert_eq!(s.get_edge_data(7).await.unwrap(), None);
        s.set_edge_data(7, b"call-site").await.unwrap();
        assert_eq!(
            s.get_edge_data(7).await.unwrap().as_deref(),
            Some(b"call-site".as_slice())
        );
        // Ghi đè dữ liệu edge.
        s.set_edge_data(7, b"updated").await.unwrap();
        assert_eq!(
            s.get_edge_data(7).await.unwrap().as_deref(),
            Some(b"updated".as_slice())
        );
        // Edge khác không ảnh hưởng.
        assert_eq!(s.get_edge_data(8).await.unwrap(), None);

        // Clear → sạch toàn bộ.
        s.set_edge_data(9, b"x").await.unwrap();
        s.clear_edges().await.unwrap();
        assert_eq!(s.get_edge_data(7).await.unwrap(), None);
        assert_eq!(s.get_edge_data(9).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_node_meta_roundtrip() {
        let mut s = InMemoryStorage::default();
        assert_eq!(s.get_node_meta(3).await.unwrap(), None);
        s.set_node_meta(3, b"node-json").await.unwrap();
        assert_eq!(
            s.get_node_meta(3).await.unwrap().as_deref(),
            Some(b"node-json".as_slice())
        );
        s.set_node_meta(3, b"node-json-2").await.unwrap();
        assert_eq!(
            s.get_node_meta(3).await.unwrap().as_deref(),
            Some(b"node-json-2".as_slice())
        );
        assert_eq!(s.get_node_meta(4).await.unwrap(), None);
        s.clear_node_meta().await.unwrap();
        assert_eq!(s.get_node_meta(3).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_chains_roundtrip() {
        let mut s = InMemoryStorage::default();
        assert_eq!(s.get_chain(9).await.unwrap(), None);
        s.set_chain(9, &[1, 2, 3]).await.unwrap();
        assert_eq!(s.get_chain(9).await.unwrap(), Some(vec![1, 2, 3]));
        s.set_chain(9, &[4]).await.unwrap();
        assert_eq!(s.get_chain(9).await.unwrap(), Some(vec![4]));
        assert_eq!(s.get_chain(10).await.unwrap(), None);
        s.clear_chains().await.unwrap();
        assert_eq!(s.get_chain(9).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_inmemory_storage_as_timeseries() {
        use crate::storage::TimeseriesStorage;

        let s = InMemoryStorage::default();
        s.append(b"cpu", 100, b"a").await.unwrap();
        s.append(b"cpu", 200, b"b").await.unwrap();
        s.append(b"cpu", 300, b"c").await.unwrap();

        assert_eq!(s.first(b"cpu").await.unwrap(), Some((100, b"a".to_vec())));
        assert_eq!(s.last(b"cpu").await.unwrap(), Some((300, b"c".to_vec())));
        let r = s.range(b"cpu", 150, 300).await.unwrap();
        assert_eq!(r, vec![(200, b"b".to_vec()), (300, b"c".to_vec())]);
        assert_eq!(
            s.latest(b"cpu", 1).await.unwrap(),
            vec![(300, b"c".to_vec())]
        );

        s.clear_series(b"cpu").await.unwrap();
        assert_eq!(s.last(b"cpu").await.unwrap(), None);
        s.clear_all_series().await.unwrap();
    }

    #[tokio::test]
    async fn test_inmemory_storage_as_pattern() {
        use crate::storage::PatternStorage;

        let s = InMemoryStorage::default();
        s.add("he").await.unwrap();
        s.add("she").await.unwrap();
        s.add("his").await.unwrap();
        // Dedup.
        s.add("he").await.unwrap();

        assert_eq!(s.count().await.unwrap(), 3);
        assert!(s.contains("she").await.unwrap());
        assert!(!s.contains("hers").await.unwrap());
        // Thứ tự theo id (thứ tự đăng ký).
        assert_eq!(s.get_all().await.unwrap(), vec!["he", "she", "his"]);

        assert!(s.remove("he").await.unwrap());
        assert!(!s.remove("he").await.unwrap());
        assert_eq!(s.count().await.unwrap(), 2);

        s.clear().await.unwrap();
        assert_eq!(s.count().await.unwrap(), 0);
        assert!(!s.contains("she").await.unwrap());

        // Pattern rỗng bị bỏ qua.
        s.add("").await.unwrap();
        assert_eq!(s.count().await.unwrap(), 0);
    }
}
