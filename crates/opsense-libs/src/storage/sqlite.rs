//! SQLite-backed radix-node storage (sqlx) — persistent backend cho `Search`.
//!
//! Đây là storage cho **radix tree**: prefix + record + children + root của
//! từng shard + metadata + key length + shortcuts. Mỗi `SqliteStorage` = 1 file
//! `.sqlite` riêng:
//!
//! - `Search::sqlite` mở nó.
//! - Forward/reverse index của `CallIndex` dùng 2 file khác nhau để không đụng
//!   id counter.
//!
//! Schema:
//! | Table          | Mục đích                               |
//! |----------------|----------------------------------------|
//! | `rt_nodes`     | id → (prefix, record); id 0 = sentinel |
//! | `rt_children`  | parent → children (PK (parent, child)) |
//! | `rt_roots`     | shard → root node id                   |
//! | `rt_meta`      | record → metadata (opaque bytes)       |
//! | `rt_keylen`    | record → key length (filter `depth`)   |
//! | `rt_shortcuts` | (shard, elem) → node ids chứa elem     |
//! | `rt_edges`     | edge id → edge data (CallEdgeMeta)     |
//! | `rt_node_meta` | element id → node metadata (Node JSON) |
//! | `rt_chains`    | record → chain bytes (u64 LE/element)  |
//! | `rt_counter`   | bộ cấp id (`next`)                     |
//!
//! Mỗi method tự acquire connection từ pool; `SqliteTx` buffer ops và áp dụng
//! atomic trong một SQLite transaction tại `commit` (giống InMemory/Redis).
//! Mọi query là runtime SQL (không dùng macro `query!` — tránh phụ thuộc
//! `DATABASE_URL` lúc build).

use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use super::{
    CategoryStorage, CategoryTx, CategoryTxOp, ChainStorage, EMPTY, EdgeDataStorage,
    NodeMetaStorage, PatternStorage, Result, ShortcutsStorage, StorageError, TimeseriesStorage,
    decode_chain, encode_chain,
};

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(e.to_string())
}

// ==================== SqliteStorage ====================

pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Mở (hoặc tạo mới nếu chưa tồn tại) file sqlite tại `path`.
    ///
    /// Idempotent với file cũ — schema `CREATE TABLE IF NOT EXISTS` + sentinel
    /// `INSERT OR IGNORE` nên reopen giữ nguyên toàn bộ dữ liệu.
    pub async fn open(path: &str) -> Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(db_err)?;
        let s = Self { pool };
        s.init().await?;
        Ok(s)
    }

    async fn init(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        for stmt in [
            "CREATE TABLE IF NOT EXISTS rt_nodes (
                id INTEGER PRIMARY KEY,
                prefix BLOB NOT NULL,
                record INTEGER NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_children (
                parent INTEGER NOT NULL,
                child INTEGER NOT NULL,
                PRIMARY KEY (parent, child)
            )",
            "CREATE INDEX IF NOT EXISTS idx_rt_children_parent ON rt_children(parent)",
            "CREATE TABLE IF NOT EXISTS rt_roots (
                shard INTEGER PRIMARY KEY,
                root INTEGER NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_meta (
                record INTEGER PRIMARY KEY,
                meta BLOB NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_keylen (
                record INTEGER PRIMARY KEY,
                len INTEGER NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_shortcuts (
                shard INTEGER NOT NULL,
                elem BLOB NOT NULL,
                node_id INTEGER NOT NULL,
                PRIMARY KEY (shard, elem, node_id)
            )",
            "CREATE INDEX IF NOT EXISTS idx_rt_shortcuts_lookup ON rt_shortcuts(shard, elem)",
            "CREATE TABLE IF NOT EXISTS rt_edges (
                id INTEGER PRIMARY KEY,
                data BLOB NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_node_meta (
                elem INTEGER PRIMARY KEY,
                meta BLOB NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_chains (
                record INTEGER PRIMARY KEY,
                chain BLOB NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS ac_patterns (
                pattern TEXT PRIMARY KEY,
                id INTEGER NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS ts_points (
                series BLOB NOT NULL,
                ts INTEGER NOT NULL,
                value BLOB NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS idx_ts_points_series_ts ON ts_points(series, ts)",
            "CREATE TABLE IF NOT EXISTS rt_node_blooms (
                id INTEGER PRIMARY KEY,
                bloom BLOB NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_node_blooms (
                id INTEGER PRIMARY KEY,
                bloom BLOB NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS rt_counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                next INTEGER NOT NULL
            )",
            // Sentinel node id 0 + counter bắt đầu từ 1.
            "INSERT OR IGNORE INTO rt_nodes (id, prefix, record) VALUES (0, X'', 0)",
            "INSERT OR IGNORE INTO rt_counter (id, next) VALUES (1, 1)",
        ] {
            sqlx::query(stmt)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
        }
        Ok(())
    }
}

#[async_trait]
impl CategoryStorage for SqliteStorage {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        // `UPDATE ... RETURNING next - 1` cấp id atomic — không cần SELECT rồi
        // UPDATE (2 bước có thể bị xen giữa bởi writer khác).
        let next: i64 = sqlx::query_scalar(
            "UPDATE rt_counter SET next = next + 1 WHERE id = 1 RETURNING next - 1",
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
        let id = next as usize;
        sqlx::query("INSERT INTO rt_nodes (id, prefix, record) VALUES (?1, ?2, ?3)")
            .bind(id as i64)
            .bind(prefix)
            .bind(record as i64)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(id)
    }

    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        if let Some(p) = prefix {
            let r = sqlx::query("UPDATE rt_nodes SET prefix = ?1 WHERE id = ?2")
                .bind(p)
                .bind(id as i64)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
            if r.rows_affected() == 0 {
                return Err(StorageError::BranchOutOfRange(id));
            }
        }
        if let Some(rec) = record {
            let r = sqlx::query("UPDATE rt_nodes SET record = ?1 WHERE id = ?2")
                .bind(rec as i64)
                .bind(id as i64)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
            if r.rows_affected() == 0 {
                return Err(StorageError::BranchOutOfRange(id));
            }
        }
        Ok(())
    }

    async fn get_node(&self, id: usize) -> Result<(Vec<u8>, usize)> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let row = sqlx::query("SELECT prefix, record FROM rt_nodes WHERE id = ?1")
            .bind(id as i64)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_err)?;
        let Some(row) = row else {
            return Err(StorageError::BranchOutOfRange(id));
        };
        let prefix: Vec<u8> = row.try_get(0).map_err(db_err)?;
        let record: i64 = row.try_get(1).map_err(db_err)?;
        Ok((prefix, record as usize))
    }

    async fn get_children(&self, id: usize) -> Result<Vec<usize>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let rows = sqlx::query("SELECT child FROM rt_children WHERE parent = ?1 ORDER BY child")
            .bind(id as i64)
            .fetch_all(&mut *conn)
            .await
            .map_err(db_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let c: i64 = r.try_get(0).map_err(db_err)?;
            out.push(c as usize);
        }
        Ok(out)
    }

    async fn set_root(&mut self, shard: usize, root: usize) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_roots (shard, root) VALUES (?1, ?2)
             ON CONFLICT(shard) DO UPDATE SET root = excluded.root",
        )
        .bind(shard as i64)
        .bind(root as i64)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_root(&self, shard: usize) -> Result<usize> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let root: Option<i64> = sqlx::query_scalar("SELECT root FROM rt_roots WHERE shard = ?1")
            .bind(shard as i64)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(root.unwrap_or(EMPTY as i64) as usize)
    }

    fn new_tx(&self) -> Box<dyn CategoryTx> {
        Box::new(SqliteTx {
            pool: self.pool.clone(),
            nodes: Vec::new(),
            ops: Vec::new(),
        })
    }
}

#[async_trait]
impl EdgeDataStorage for SqliteStorage {
    async fn set_edge_data(&mut self, edge: usize, data: &[u8]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_edges (id, data) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET data = excluded.data",
        )
        .bind(edge as i64)
        .bind(data)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_edge_data(&self, edge: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let data: Option<Vec<u8>> = sqlx::query_scalar("SELECT data FROM rt_edges WHERE id = ?1")
            .bind(edge as i64)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(data)
    }

    async fn clear_edges(&mut self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM rt_edges")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

#[async_trait]
impl ChainStorage for SqliteStorage {
    async fn set_chain(&mut self, record: usize, chain: &[u64]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_chains (record, chain) VALUES (?1, ?2)
             ON CONFLICT(record) DO UPDATE SET chain = excluded.chain",
        )
        .bind(record as i64)
        .bind(encode_chain(chain))
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_chain(&self, record: usize) -> Result<Option<Vec<u64>>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let bytes: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT chain FROM rt_chains WHERE record = ?1")
                .bind(record as i64)
                .fetch_optional(&mut *conn)
                .await
                .map_err(db_err)?;
        Ok(bytes.map(|b| decode_chain(&b)))
    }

    async fn clear_chains(&mut self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM rt_chains")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

#[async_trait]
impl ShortcutsStorage for SqliteStorage {
    async fn add_shortcut_node(&mut self, shard: usize, elem: &[u8], node_id: usize) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_shortcuts (shard, elem, node_id) VALUES (?1, ?2, ?3)
             ON CONFLICT DO NOTHING",
        )
        .bind(shard as i64)
        .bind(elem)
        .bind(node_id as i64)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_shortcut_nodes(&self, shard: usize, elem: &[u8]) -> Result<Vec<usize>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let rows = sqlx::query(
            "SELECT node_id FROM rt_shortcuts WHERE shard = ?1 AND elem = ?2 ORDER BY node_id",
        )
        .bind(shard as i64)
        .bind(elem)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let c: i64 = r.try_get(0).map_err(db_err)?;
            out.push(c as usize);
        }
        Ok(out)
    }

    async fn clear_shortcuts(&mut self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM rt_shortcuts")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

#[async_trait]
impl NodeMetaStorage for SqliteStorage {
    async fn set_node_meta(&mut self, elem: usize, meta: &[u8]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_node_meta (elem, meta) VALUES (?1, ?2)
             ON CONFLICT(elem) DO UPDATE SET meta = excluded.meta",
        )
        .bind(elem as i64)
        .bind(meta)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_node_meta(&self, elem: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let meta: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT meta FROM rt_node_meta WHERE elem = ?1")
                .bind(elem as i64)
                .fetch_optional(&mut *conn)
                .await
                .map_err(db_err)?;
        Ok(meta)
    }

    async fn clear_node_meta(&mut self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM rt_node_meta")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn set_meta(&mut self, record: usize, meta: &[u8]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_meta (record, meta) VALUES (?1, ?2)
             ON CONFLICT(record) DO UPDATE SET meta = excluded.meta",
        )
        .bind(record as i64)
        .bind(meta)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_meta(&self, record: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let meta: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT meta FROM rt_meta WHERE record = ?1")
                .bind(record as i64)
                .fetch_optional(&mut *conn)
                .await
                .map_err(db_err)?;
        Ok(meta)
    }

    async fn set_key_len(&mut self, record: usize, len: usize) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_keylen (record, len) VALUES (?1, ?2)
             ON CONFLICT(record) DO UPDATE SET len = excluded.len",
        )
        .bind(record as i64)
        .bind(len as i64)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_key_len(&self, record: usize) -> Result<Option<usize>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let len: Option<i64> = sqlx::query_scalar("SELECT len FROM rt_keylen WHERE record = ?1")
            .bind(record as i64)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(len.map(|x| x as usize))
    }
}

#[cfg(feature = "bloom-search")]
#[async_trait]
impl super::BloomStorage for SqliteStorage {
    async fn set_node_bloom(&mut self, id: usize, bloom: &[u8]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO rt_node_blooms (id, bloom) VALUES (?1, ?2) \
                     ON CONFLICT(id) DO UPDATE SET bloom = excluded.bloom",
        )
        .bind(id as i64)
        .bind(bloom)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_node_bloom(&self, id: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let row = sqlx::query("SELECT bloom FROM rt_node_blooms WHERE id = ?1")
            .bind(id as i64)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_err)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let bloom: Vec<u8> = row.try_get(0).map_err(db_err)?;
        Ok(Some(bloom))
    }
}

#[async_trait]
impl TimeseriesStorage for SqliteStorage {
    async fn append(&self, series: &[u8], timestamp: u64, value: &[u8]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("INSERT INTO ts_points (series, ts, value) VALUES (?1, ?2, ?3)")
            .bind(series)
            .bind(timestamp as i64)
            .bind(value)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn range(
        &self,
        series: &[u8],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let rows = sqlx::query(
            "SELECT ts, value FROM ts_points WHERE series = ?1 AND ts >= ?2 AND ts <= ?3 \
             ORDER BY ts ASC",
        )
        .bind(series)
        .bind(start_ts as i64)
        .bind(end_ts as i64)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(rows
            .iter()
            .map(|r| {
                let ts: i64 = r.try_get(0).map_err(db_err).unwrap();
                let val: Vec<u8> = r.try_get(1).map_err(db_err).unwrap();
                (ts as u64, val)
            })
            .collect())
    }

    async fn latest(&self, series: &[u8], limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let rows = sqlx::query(
            "SELECT ts, value FROM ts_points WHERE series = ?1 ORDER BY ts DESC LIMIT ?2",
        )
        .bind(series)
        .bind(limit as i64)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
        let mut out: Vec<(u64, Vec<u8>)> = rows
            .iter()
            .map(|r| {
                let ts: i64 = r.try_get(0).map_err(db_err).unwrap();
                let val: Vec<u8> = r.try_get(1).map_err(db_err).unwrap();
                (ts as u64, val)
            })
            .collect();
        out.reverse();
        Ok(out)
    }

    async fn first(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let row = sqlx::query(
            "SELECT ts, value FROM ts_points WHERE series = ?1 ORDER BY ts ASC LIMIT 1",
        )
        .bind(series)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| {
            let ts: i64 = r.try_get(0).map_err(db_err).unwrap();
            let val: Vec<u8> = r.try_get(1).map_err(db_err).unwrap();
            (ts as u64, val)
        }))
    }

    async fn last(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let row = sqlx::query(
            "SELECT ts, value FROM ts_points WHERE series = ?1 ORDER BY ts DESC LIMIT 1",
        )
        .bind(series)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| {
            let ts: i64 = r.try_get(0).map_err(db_err).unwrap();
            let val: Vec<u8> = r.try_get(1).map_err(db_err).unwrap();
            (ts as u64, val)
        }))
    }

    async fn clear_series(&self, series: &[u8]) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM ts_points WHERE series = ?1")
            .bind(series)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn clear_all_series(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM ts_points")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

#[async_trait]
impl PatternStorage for SqliteStorage {
    async fn add(&self, pattern: &str) -> Result<()> {
        if pattern.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM ac_patterns WHERE pattern = ?1")
                .bind(pattern)
                .fetch_optional(&mut *conn)
                .await
                .map_err(db_err)?;
        if exists.is_some() {
            return Ok(());
        }
        // id = MAX(id) + 1 (không dùng counter chung để tránh đụng rt_counter).
        sqlx::query(
            "INSERT INTO ac_patterns (pattern, id) \
             VALUES (?1, (SELECT COALESCE(MAX(id), 0) + 1 FROM ac_patterns))",
        )
        .bind(pattern)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn contains(&self, pattern: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let row = sqlx::query("SELECT 1 FROM ac_patterns WHERE pattern = ?1")
            .bind(pattern)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(row.is_some())
    }

    async fn get_all(&self) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        // ORDER BY id ASC → đúng thứ tự đăng ký.
        let rows = sqlx::query("SELECT pattern FROM ac_patterns ORDER BY id ASC")
            .fetch_all(&mut *conn)
            .await
            .map_err(db_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let p: String = r.try_get(0).map_err(db_err)?;
            out.push(p);
        }
        Ok(out)
    }

    async fn count(&self) -> Result<usize> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ac_patterns")
            .fetch_one(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(n as usize)
    }

    async fn remove(&self, pattern: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let r = sqlx::query("DELETE FROM ac_patterns WHERE pattern = ?1")
            .bind(pattern)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(r.rows_affected() > 0)
    }

    async fn clear(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        sqlx::query("DELETE FROM ac_patterns")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

// ==================== SqliteTx ====================

/// Transaction cho `SqliteStorage`: buffer toàn bộ mutation, áp dụng atomic
/// trong một SQLite transaction tại `commit`.
///
/// `new_node` đọc counter mới mỗi lần gọi, `id = next + nodes.len()` — giống
/// `RedisTx`; `commit` bump counter lên `max(reserved) + 1` (dùng `MAX` để
/// không hạ counter nếu writer khác đã bump) nên id không bao giờ trùng.
pub struct SqliteTx {
    pool: SqlitePool,
    nodes: Vec<(usize, Vec<u8>, usize)>,
    ops: Vec<CategoryTxOp>,
}

#[async_trait]
impl CategoryTx for SqliteTx {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let mut conn = self.pool.acquire().await.map_err(db_err)?;
        let next: i64 = sqlx::query_scalar("SELECT next FROM rt_counter WHERE id = 1")
            .fetch_one(&mut *conn)
            .await
            .map_err(db_err)?;
        let id = next as usize + self.nodes.len();
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
        let SqliteTx { pool, nodes, ops } = *self;
        let mut tx = pool.begin().await.map_err(db_err)?;

        // 1. Materialize node mới trước — để ops add/move trỏ tới hợp lệ.
        for (id, prefix, record) in &nodes {
            sqlx::query("INSERT INTO rt_nodes (id, prefix, record) VALUES (?1, ?2, ?3)")
                .bind(*id as i64)
                .bind(prefix)
                .bind(*record as i64)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
        }

        // 2. Bump counter lên max(reserved) + 1 — id tx cấp vẫn unique.
        if let Some(max_id) = nodes.iter().map(|(id, _, _)| *id).max() {
            sqlx::query("UPDATE rt_counter SET next = MAX(next, ?1) WHERE id = 1")
                .bind((max_id + 1) as i64)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
        }

        // 3. Áp dụng toàn bộ ops — atomic, không lộ trạng thái trung gian.
        for op in ops {
            match op {
                CategoryTxOp::AddChild { parent, child } => {
                    sqlx::query(
                        "INSERT INTO rt_children (parent, child) VALUES (?1, ?2)
                         ON CONFLICT DO NOTHING",
                    )
                    .bind(parent as i64)
                    .bind(child as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_err)?;
                }
                CategoryTxOp::MoveChild { from, to, child } => {
                    sqlx::query("DELETE FROM rt_children WHERE parent = ?1 AND child = ?2")
                        .bind(from as i64)
                        .bind(child as i64)
                        .execute(&mut *tx)
                        .await
                        .map_err(db_err)?;
                    sqlx::query(
                        "INSERT INTO rt_children (parent, child) VALUES (?1, ?2)
                         ON CONFLICT DO NOTHING",
                    )
                    .bind(to as i64)
                    .bind(child as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_err)?;
                }
                CategoryTxOp::UpdateNode { id, prefix, record } => {
                    if let Some(p) = prefix {
                        sqlx::query("UPDATE rt_nodes SET prefix = ?1 WHERE id = ?2")
                            .bind(p)
                            .bind(id as i64)
                            .execute(&mut *tx)
                            .await
                            .map_err(db_err)?;
                    }
                    if let Some(r) = record {
                        sqlx::query("UPDATE rt_nodes SET record = ?1 WHERE id = ?2")
                            .bind(r as i64)
                            .bind(id as i64)
                            .execute(&mut *tx)
                            .await
                            .map_err(db_err)?;
                    }
                }
            }
        }

        tx.commit().await.map_err(db_err)?;
        Ok(())
    }
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite");
        let path = path.to_string_lossy().into_owned();
        (dir, path)
    }

    #[tokio::test]
    async fn test_new_node_and_get_node() {
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
        let id = s.new_node(b"hello".to_vec(), 42).await.unwrap();
        assert_ne!(id, EMPTY);
        let (prefix, record) = s.get_node(id).await.unwrap();
        assert_eq!(prefix, b"hello");
        assert_eq!(record, 42);
    }

    #[tokio::test]
    async fn test_update_node() {
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
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
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
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
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
        assert_eq!(s.get_meta(7).await.unwrap(), None);
        assert_eq!(s.get_key_len(7).await.unwrap(), None);
        s.set_meta(7, b"call-site-info").await.unwrap();
        s.set_key_len(7, 5).await.unwrap();
        assert_eq!(
            s.get_meta(7).await.unwrap().as_deref(),
            Some(b"call-site-info".as_slice())
        );
        assert_eq!(s.get_key_len(7).await.unwrap(), Some(5));
        s.set_meta(7, b"updated").await.unwrap();
        s.set_key_len(7, 6).await.unwrap();
        assert_eq!(
            s.get_meta(7).await.unwrap().as_deref(),
            Some(b"updated".as_slice())
        );
        assert_eq!(s.get_key_len(7).await.unwrap(), Some(6));
        assert_eq!(s.get_meta(8).await.unwrap(), None);
        assert_eq!(s.get_key_len(8).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_shortcuts_roundtrip() {
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
        assert!(s.get_shortcut_nodes(1, b"l").await.unwrap().is_empty());
        s.add_shortcut_node(1, b"l", 10).await.unwrap();
        s.add_shortcut_node(1, b"l", 20).await.unwrap();
        s.add_shortcut_node(1, b"o", 10).await.unwrap();
        s.add_shortcut_node(2, b"l", 30).await.unwrap(); // shard khác
        let nodes = s.get_shortcut_nodes(1, b"l").await.unwrap();
        assert!(nodes.contains(&10) && nodes.contains(&20));
        assert_eq!(nodes.len(), 2);
        assert_eq!(s.get_shortcut_nodes(2, b"l").await.unwrap(), vec![30]);

        s.clear_shortcuts().await.unwrap();
        assert!(s.get_shortcut_nodes(1, b"l").await.unwrap().is_empty());
        assert!(s.get_shortcut_nodes(2, b"l").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_tx_commit_applies_atomically() {
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
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
        let (_d, path) = tmp_path();
        let s = SqliteStorage::open(&path).await.unwrap();
        let mut tx = s.new_tx();
        let id = tx.new_node(b"pending".to_vec(), 9).await.unwrap();
        // Trước commit, node chưa materialize → get_node lỗi BranchOutOfRange.
        assert!(s.get_node(id).await.is_err());
        tx.commit().await.unwrap();
        assert_eq!(s.get_node(id).await.unwrap().1, 9);
    }

    #[tokio::test]
    async fn test_tx_move_child_migrates() {
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
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
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
        assert_eq!(s.get_edge_data(7).await.unwrap(), None);
        s.set_edge_data(7, b"call-site").await.unwrap();
        assert_eq!(
            s.get_edge_data(7).await.unwrap().as_deref(),
            Some(b"call-site".as_slice())
        );
        // Overwrite.
        s.set_edge_data(7, b"call-site-2").await.unwrap();
        assert_eq!(
            s.get_edge_data(7).await.unwrap().as_deref(),
            Some(b"call-site-2".as_slice())
        );
        s.clear_edges().await.unwrap();
        assert_eq!(s.get_edge_data(7).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_node_meta_roundtrip() {
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
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
        let (_d, path) = tmp_path();
        let mut s = SqliteStorage::open(&path).await.unwrap();
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
    async fn test_persists_across_reopen() {
        let (_d, path) = tmp_path();
        let parent;
        {
            let mut s = SqliteStorage::open(&path).await.unwrap();
            parent = s.new_node(b"hello".to_vec(), 42).await.unwrap();
            let child = s.new_node(b"world".to_vec(), 7).await.unwrap();
            let mut seed = s.new_tx();
            seed.add_child(parent, child).await.unwrap();
            seed.commit().await.unwrap();
            s.set_root(3, parent).await.unwrap();
            s.set_meta(42, b"meta-42").await.unwrap();
            s.set_key_len(42, 5).await.unwrap();
            s.add_shortcut_node(1, b"h", parent).await.unwrap();
            s.set_node_meta(100, b"node-json").await.unwrap();
            s.set_chain(42, &[100, 101]).await.unwrap();

            let mut tx = s.new_tx();
            let extra = tx.new_node(b"z".to_vec(), 99).await.unwrap();
            tx.add_child(parent, extra).await.unwrap();
            tx.commit().await.unwrap();
        } // drop storage → pool đóng

        // Reopen: dữ liệu phải còn nguyên.
        let mut s = SqliteStorage::open(&path).await.unwrap();
        let (prefix, record) = s.get_node(parent).await.unwrap();
        assert_eq!(prefix, b"hello");
        assert_eq!(record, 42);
        assert_eq!(s.get_root(3).await.unwrap(), parent);
        assert_eq!(
            s.get_meta(42).await.unwrap().as_deref(),
            Some(b"meta-42".as_slice())
        );
        assert_eq!(s.get_key_len(42).await.unwrap(), Some(5));
        assert_eq!(
            s.get_node_meta(100).await.unwrap().as_deref(),
            Some(b"node-json".as_slice())
        );
        assert_eq!(s.get_chain(42).await.unwrap(), Some(vec![100, 101]));
        assert!(
            s.get_shortcut_nodes(1, b"h")
                .await
                .unwrap()
                .contains(&parent)
        );
        // Children gồm cả node tạo bằng tx (persist qua commit).
        let children = s.get_children(parent).await.unwrap();
        assert_eq!(children.len(), 2);
        // Node id mới tiếp tục cấp trên counter đã persist.
        let n = s.new_node(b"new".to_vec(), 1).await.unwrap();
        assert!(n > parent);
    }

    #[tokio::test]
    async fn test_timeseries_roundtrip() {
        use crate::storage::TimeseriesStorage;

        let (_d, path) = tmp_path();
        let s = SqliteStorage::open(&path).await.unwrap();
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

        // Persist qua reopen.
        s.append(b"mem", 1, b"x").await.unwrap();
        drop(s);
        let s = SqliteStorage::open(&path).await.unwrap();
        assert_eq!(s.last(b"mem").await.unwrap(), Some((1, b"x".to_vec())));
        s.clear_all_series().await.unwrap();
        assert_eq!(s.last(b"mem").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_pattern_roundtrip() {
        use crate::storage::PatternStorage;

        let (_d, path) = tmp_path();
        let s = SqliteStorage::open(&path).await.unwrap();
        s.add("he").await.unwrap();
        s.add("she").await.unwrap();
        s.add("his").await.unwrap();
        s.add("he").await.unwrap(); // dedup

        assert_eq!(s.count().await.unwrap(), 3);
        assert!(s.contains("she").await.unwrap());
        assert!(!s.contains("hers").await.unwrap());
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

        // Persist qua reopen.
        s.add("he").await.unwrap();
        s.add("she").await.unwrap();
        drop(s);
        let s = SqliteStorage::open(&path).await.unwrap();
        assert_eq!(s.get_all().await.unwrap(), vec!["he", "she"]);
    }
}
