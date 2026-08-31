//! DuckDB + S3 storage — backend persistent cho `Search` với dữ liệu lớn.
//!
//! Kiến trúc:
//! - **KV/radix layer**: DuckDB file cục bộ (`path`) giữ toàn bộ bảng
//!   `rt_*` / `ac_patterns` — schema giống hệt `SqliteStorage` nên cùng một
//!   contract trait. DuckDB là process-embedded (feature `bundled`), mọi query
//!   chạy qua một `Connection` duy nhất sau `parking_lot::Mutex` (DuckDB chỉ
//!   cho một writer trên cùng file nên 1 connection là đủ).
//! - **Timeseries trên S3**: `ts_points` cục bộ chỉ là buffer các điểm
//!   *chưa flush*. Khi số điểm vượt `flush_threshold`, `append` tự đẩy chúng
//!   ra Parquet trên S3 (`COPY ... TO 's3://…'` qua extension `httpfs`) rồi
//!   xoá khỏi bảng cục bộ. Mọi lần đọc (`range`/`latest`/…) là UNION của
//!   Parquet trên S3 với buffer cục bộ, dedup theo `(series, ts)` — buffer
//!   cục bộ luôn thắng vì nó mới hơn mọi file đã flush.
//! - **Snapshot/restore**: `snapshot()` COPY toàn bộ bảng KV ra Parquet trên
//!   S3 (một file/table + manifest). Khi `open_with_s3` mở một db *mới* (local
//!   chưa có dữ liệu radix) mà S3 đã có snapshot → restore toàn bộ trạng thái.
//!   Db cũ tự reopen giữ nguyên dữ liệu, không bị ghi đè bởi snapshot.
//!
//! Không cấu hình S3 (`DuckS3Storage::open`) → hoạt động như DuckDB thuần,
//! timeseries giữ full trong local, snapshot/flush là no-op.

use std::sync::Arc;

use async_trait::async_trait;
use duckdb::OptionalExt;
use duckdb::params;
use parking_lot::Mutex;

use super::{
    CategoryStorage, CategoryTx, CategoryTxOp, ChainStorage, EMPTY, EdgeDataStorage,
    NodeMetaStorage, PatternStorage, Result, ShortcutsStorage, StorageError, TimeseriesStorage,
    decode_chain, encode_chain,
};

fn db_err(e: duckdb::Error) -> StorageError {
    StorageError::Internal(e.to_string())
}

// ==================== Config ====================

/// Tham số kết nối S3 (compat AWS S3 / MinIO / R2 / GCS S3-interop).
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    /// Prefix key, VD `"opsense/proj1"` — không kèm `/` ở hai đầu.
    pub prefix: String,
    /// Custom endpoint (VD `http://minio:9000`). `None` = AWS S3 mặc định.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl S3Config {
    fn url(&self, rest: &str) -> String {
        format!("s3://{}/{}/{}", self.bucket, self.prefix.trim_matches('/'), rest)
    }

    fn escape(s: &str) -> String {
        s.replace('\'', "''")
    }

    /// `CREATE SECRET` — gọi một lần lúc open.
    fn secret_sql(&self) -> String {
        let mut sql = String::from("CREATE OR REPLACE SECRET opsense_s3 (TYPE S3");
        if let Some(ep) = &self.endpoint {
            sql.push_str(&format!(", ENDPOINT '{}'", Self::escape(ep)));
        }
        if let Some(r) = &self.region {
            sql.push_str(&format!(", REGION '{}'", Self::escape(r)));
        }
        sql.push_str(&format!(
            ", KEY_ID '{}', SECRET '{}'",
            Self::escape(&self.access_key_id),
            Self::escape(&self.secret_access_key)
        ));
        if let Some(t) = &self.session_token {
            sql.push_str(&format!(", SESSION_TOKEN '{}'", Self::escape(t)));
        }
        sql.push(')');
        sql
    }
}

// ==================== DuckS3Storage ====================

/// Bảng KV được snapshot/restore (trừ `ts_points` — đi đường Parquet riêng).
const SNAPSHOT_TABLES: &[&str] = &[
    "rt_nodes",
    "rt_children",
    "rt_roots",
    "rt_meta",
    "rt_keylen",
    "rt_shortcuts",
    "rt_edges",
    "rt_node_meta",
    "rt_chains",
    "ac_patterns",
    "rt_node_blooms",
    "rt_counter",
];

pub struct DuckS3Storage {
    conn: Arc<Mutex<duckdb::Connection>>,
    s3: Option<S3Config>,
    flush_threshold: usize,
}

impl DuckS3Storage {
    /// Mở (hoặc tạo mới) DuckDB file cục bộ — không S3.
    pub async fn open(path: &str) -> Result<Self> {
        Self::open_inner(path, None, 0)
    }

    /// Mở DuckDB file cục bộ + cấu hình S3 (httpfs). Nếu db cục bộ còn trống
    /// và S3 đã có snapshot → restore trạng thái từ snapshot.
    ///
    /// `flush_threshold` = số điểm timeseries buffer local trước khi tự flush
    /// ra Parquet trên S3 (mặc định 4096 nếu truyền 0).
    pub async fn open_with_s3(path: &str, s3: S3Config, flush_threshold: usize) -> Result<Self> {
        Self::open_inner(path, Some(s3), flush_threshold)
    }

    fn open_inner(path: &str, s3: Option<S3Config>, flush_threshold: usize) -> Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        let conn = duckdb::Connection::open(path).map_err(db_err)?;
        if s3.is_some() {
            // Extension httpfs được statically-linked qua feature `httpfs`
            // (bundled); INSTALL/LOAD chỉ cần cho build dùng extension dynamic.
            let _ = conn.execute_batch("INSTALL httpfs; LOAD httpfs;");
        }
        let s = Self {
            conn: Arc::new(Mutex::new(conn)),
            s3,
            flush_threshold: flush_threshold.max(1),
        };
        s.init()?;
        if s.s3.is_some() && !s.has_local_state()? {
            s.restore()?;
        }
        Ok(s)
    }

    fn init(&self) -> Result<()> {
        let conn = self.conn.lock();
        if let Some(s3) = &self.s3 {
            conn.execute_batch(&s3.secret_sql()).map_err(db_err)?;
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS rt_nodes (
                id BIGINT PRIMARY KEY,
                prefix BLOB NOT NULL,
                record BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_children (
                parent BIGINT NOT NULL,
                child BIGINT NOT NULL,
                PRIMARY KEY (parent, child)
            );
            CREATE INDEX IF NOT EXISTS idx_rt_children_parent ON rt_children(parent);
            CREATE TABLE IF NOT EXISTS rt_roots (
                shard BIGINT PRIMARY KEY,
                root BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_meta (
                record BIGINT PRIMARY KEY,
                meta BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_keylen (
                record BIGINT PRIMARY KEY,
                len BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_shortcuts (
                shard BIGINT NOT NULL,
                elem BLOB NOT NULL,
                node_id BIGINT NOT NULL,
                PRIMARY KEY (shard, elem, node_id)
            );
            CREATE INDEX IF NOT EXISTS idx_rt_shortcuts_lookup ON rt_shortcuts(shard, elem);
            CREATE TABLE IF NOT EXISTS rt_edges (
                id BIGINT PRIMARY KEY,
                data BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_node_meta (
                elem BIGINT PRIMARY KEY,
                meta BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_chains (
                record BIGINT PRIMARY KEY,
                chain BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ac_patterns (
                pattern VARCHAR PRIMARY KEY,
                id BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ts_points (
                id BIGINT NOT NULL,
                series BLOB NOT NULL,
                ts BIGINT NOT NULL,
                value BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ts_points_series_ts ON ts_points(series, ts);
            CREATE SEQUENCE IF NOT EXISTS ts_seq;
            CREATE TABLE IF NOT EXISTS rt_node_blooms (
                id BIGINT PRIMARY KEY,
                bloom BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rt_counter (
                id BIGINT PRIMARY KEY CHECK (id = 1),
                next BIGINT NOT NULL
            );",
        )
        .map_err(db_err)?;
        conn.execute(
            "INSERT INTO rt_nodes (id, prefix, record) VALUES (0, ?, 0) ON CONFLICT DO NOTHING",
            params![Vec::<u8>::new()],
        )
        .map_err(db_err)?;
        conn.execute(
            "INSERT INTO rt_counter (id, next) VALUES (1, 1) ON CONFLICT DO NOTHING",
            [],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Local db đã có dữ liệu radix chưa (`rt_counter` vượt seed = đã dùng).
    fn has_local_state(&self) -> Result<bool> {
        let conn = self.conn.lock();
        let next: i64 = conn
            .query_row("SELECT next FROM rt_counter WHERE id = 1", [], |r| r.get(0))
            .optional()
            .map_err(db_err)?
            .unwrap_or(0);
        Ok(next > 1)
    }

    // ==================== S3 helpers ====================

    /// Flush toàn bộ buffer timeseries local ra một Parquet file trên S3.
    /// COPY + DELETE trong cùng transaction; nếu crash giữa chừng, bước dedup
    /// lúc đọc lo phần trùng.
    pub fn flush_timeseries(&self) -> Result<()> {
        let Some(s3) = self.s3.clone() else {
            return Ok(());
        };
        let conn = self.conn.lock();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM ts_points", [], |r| r.get(0))
            .map_err(db_err)?;
        if n == 0 {
            return Ok(());
        }
        let file = format!(
            "ts_points/{}.parquet",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        conn.execute_batch("BEGIN TRANSACTION;").map_err(db_err)?;
        let result = conn.execute_batch(&format!(
                "COPY (SELECT id, series, ts, value FROM ts_points)
                   TO '{}' (FORMAT PARQUET);
                 DELETE FROM ts_points;",
                s3.url(&file)
            ))
            .map_err(db_err);
        match result {
            Ok(()) => conn.execute_batch("COMMIT;").map_err(db_err)?,
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(e);
            }
        }
        Ok(())
    }

    /// Snapshot toàn bộ bảng KV ra Parquet trên S3 (mỗi bảng một file +
    /// manifest ghi cuối cùng). Gọi định kỳ hoặc trước khi tắt để db khác
    /// restore được trạng thái.
    pub fn snapshot(&self) -> Result<()> {
        let Some(s3) = self.s3.clone() else {
            return Ok(());
        };
        let conn = self.conn.lock();
        let generation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        for table in SNAPSHOT_TABLES {
            conn.execute_batch(&format!(
                "COPY (SELECT * FROM {table}) TO '{}' (FORMAT PARQUET);",
                s3.url(&format!("snapshot/{table}.parquet"))
            ))
            .map_err(db_err)?;
        }
        conn.execute_batch(&format!(
            "COPY (SELECT {generation} AS ts) TO '{}' (FORMAT PARQUET);",
            s3.url("snapshot/_manifest.parquet")
        ))
        .map_err(db_err)?;
        Ok(())
    }

    /// Restore từ snapshot S3 vào local db (db local phải còn trống — điều
    /// kiện đã kiểm tra trong `open_inner`).
    fn restore(&self) -> Result<()> {
        let Some(s3) = self.s3.clone() else {
            return Ok(());
        };
        let conn = self.conn.lock();
        if !self.object_exists(&conn, &s3.url("snapshot/_manifest.parquet"))? {
            return Ok(()); // chưa có snapshot nào — db mới trắng, dùng như bình thường.
        }
        for table in SNAPSHOT_TABLES {
            let path = s3.url(&format!("snapshot/{table}.parquet"));
            if self.object_exists(&conn, &path)? {
                conn.execute_batch(&format!(
                    "INSERT INTO {table} SELECT * FROM read_parquet('{}');",
                    path
                ))
                .map_err(db_err)?;
            }
        }
        Ok(())
    }

    /// `true` nếu ít nhất một Parquet timeseries đã nằm trên S3.
    fn ts_files_exist(&self, conn: &duckdb::Connection, s3: &S3Config) -> Result<bool> {
        self.object_exists(conn, &s3.url("ts_points/*.parquet"))
    }

    /// Kiểm tra path/glob trên S3 tồn tại hay không (qua `glob()`).
    fn object_exists(&self, conn: &duckdb::Connection, path: &str) -> Result<bool> {
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM glob(?)", params![path], |r| r.get(0))
            .map_err(db_err)?;
        Ok(n > 0)
    }
}

// ==================== CategoryStorage ====================

#[async_trait]
impl CategoryStorage for DuckS3Storage {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let conn = self.conn.lock();
        // Cấp id atomic qua RETURNING (DuckDB hỗ trợ RETURNING của UPDATE).
        let next: i64 = conn
            .query_row(
                "UPDATE rt_counter SET next = next + 1 WHERE id = 1 RETURNING next - 1",
                [],
                |r| r.get(0),
            )
            .map_err(db_err)?;
        let id = next as usize;
        conn.execute(
            "INSERT INTO rt_nodes (id, prefix, record) VALUES (?, ?, ?)",
            params![id as i64, prefix, record as i64],
        )
        .map_err(db_err)?;
        Ok(id)
    }

    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()> {
        let conn = self.conn.lock();
        if let Some(p) = prefix {
            let n = conn
                .execute(
                    "UPDATE rt_nodes SET prefix = ? WHERE id = ?",
                    params![p, id as i64],
                )
                .map_err(db_err)?;
            if n == 0 {
                return Err(StorageError::BranchOutOfRange(id));
            }
        }
        if let Some(rec) = record {
            let n = conn
                .execute(
                    "UPDATE rt_nodes SET record = ? WHERE id = ?",
                    params![rec as i64, id as i64],
                )
                .map_err(db_err)?;
            if n == 0 {
                return Err(StorageError::BranchOutOfRange(id));
            }
        }
        Ok(())
    }

    async fn get_node(&self, id: usize) -> Result<(Vec<u8>, usize)> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT prefix, record FROM rt_nodes WHERE id = ?",
            params![id as i64],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)? as usize)),
        )
        .optional()
        .map_err(db_err)?
        .ok_or(StorageError::BranchOutOfRange(id))
    }

    async fn get_children(&self, id: usize) -> Result<Vec<usize>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT child FROM rt_children WHERE parent = ? ORDER BY child")
            .map_err(db_err)?;
        let rows = stmt
            .query_map(params![id as i64], |r| r.get::<_, i64>(0))
            .map_err(db_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(db_err)? as usize);
        }
        Ok(out)
    }

    async fn set_root(&mut self, shard: usize, root: usize) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_roots (shard, root) VALUES (?, ?)
             ON CONFLICT (shard) DO UPDATE SET root = excluded.root",
            params![shard as i64, root as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_root(&self, shard: usize) -> Result<usize> {
        let conn = self.conn.lock();
        let root: Option<i64> = conn
            .query_row(
                "SELECT root FROM rt_roots WHERE shard = ?",
                params![shard as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(db_err)?;
        Ok(root.unwrap_or(EMPTY as i64) as usize)
    }

    fn new_tx(&self) -> Box<dyn CategoryTx> {
        Box::new(DuckS3Tx {
            conn: self.conn.clone(),
            nodes: Vec::new(),
            ops: Vec::new(),
        })
    }
}

// ==================== EdgeDataStorage ====================

#[async_trait]
impl EdgeDataStorage for DuckS3Storage {
    async fn set_edge_data(&mut self, edge: usize, data: &[u8]) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_edges (id, data) VALUES (?, ?)
             ON CONFLICT (id) DO UPDATE SET data = excluded.data",
            params![edge as i64, data.to_vec()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_edge_data(&self, edge: usize) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT data FROM rt_edges WHERE id = ?",
            params![edge as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(db_err)
    }

    async fn clear_edges(&mut self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM rt_edges", []).map_err(db_err)?;
        Ok(())
    }

    async fn for_each_edge_data(
        &self,
        f: &mut (dyn for<'a> FnMut(usize, &'a [u8]) -> Result<()> + Send),
    ) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT id, data FROM rt_edges ORDER BY id")
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))
            .map_err(db_err)?;
        for r in rows {
            let (id, data) = r.map_err(db_err)?;
            f(id as usize, &data)?;
        }
        Ok(())
    }
}

// ==================== ChainStorage ====================

#[async_trait]
impl ChainStorage for DuckS3Storage {
    async fn set_chain(&mut self, record: usize, chain: &[u64]) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_chains (record, chain) VALUES (?, ?)
             ON CONFLICT (record) DO UPDATE SET chain = excluded.chain",
            params![record as i64, encode_chain(chain)],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_chain(&self, record: usize) -> Result<Option<Vec<u64>>> {
        let conn = self.conn.lock();
        let bytes: Option<Vec<u8>> = conn
            .query_row(
                "SELECT chain FROM rt_chains WHERE record = ?",
                params![record as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(db_err)?;
        Ok(bytes.map(|b| decode_chain(&b)))
    }

    async fn clear_chains(&mut self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM rt_chains", []).map_err(db_err)?;
        Ok(())
    }
}

// ==================== ShortcutsStorage ====================

#[async_trait]
impl ShortcutsStorage for DuckS3Storage {
    async fn add_shortcut_node(&mut self, shard: usize, elem: &[u8], node_id: usize) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_shortcuts (shard, elem, node_id) VALUES (?, ?, ?)
             ON CONFLICT DO NOTHING",
            params![shard as i64, elem.to_vec(), node_id as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_shortcut_nodes(&self, shard: usize, elem: &[u8]) -> Result<Vec<usize>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT node_id FROM rt_shortcuts WHERE shard = ? AND elem = ? ORDER BY node_id",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map(params![shard as i64, elem.to_vec()], |r| r.get::<_, i64>(0))
            .map_err(db_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(db_err)? as usize);
        }
        Ok(out)
    }

    async fn clear_shortcuts(&mut self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM rt_shortcuts", []).map_err(db_err)?;
        Ok(())
    }
}

// ==================== NodeMetaStorage ====================

#[async_trait]
impl NodeMetaStorage for DuckS3Storage {
    async fn set_node_meta(&mut self, elem: usize, meta: &[u8]) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_node_meta (elem, meta) VALUES (?, ?)
             ON CONFLICT (elem) DO UPDATE SET meta = excluded.meta",
            params![elem as i64, meta.to_vec()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_node_meta(&self, elem: usize) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT meta FROM rt_node_meta WHERE elem = ?",
            params![elem as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(db_err)
    }

    async fn clear_node_meta(&mut self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM rt_node_meta", []).map_err(db_err)?;
        Ok(())
    }

    async fn set_meta(&mut self, record: usize, meta: &[u8]) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_meta (record, meta) VALUES (?, ?)
             ON CONFLICT (record) DO UPDATE SET meta = excluded.meta",
            params![record as i64, meta.to_vec()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_meta(&self, record: usize) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT meta FROM rt_meta WHERE record = ?",
            params![record as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(db_err)
    }

    async fn set_key_len(&mut self, record: usize, len: usize) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_keylen (record, len) VALUES (?, ?)
             ON CONFLICT (record) DO UPDATE SET len = excluded.len",
            params![record as i64, len as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_key_len(&self, record: usize) -> Result<Option<usize>> {
        let conn = self.conn.lock();
        let len: Option<i64> = conn
            .query_row(
                "SELECT len FROM rt_keylen WHERE record = ?",
                params![record as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(db_err)?;
        Ok(len.map(|x| x as usize))
    }
}

// ==================== BloomStorage (feature bloom-search) ====================

#[cfg(feature = "bloom-search")]
#[async_trait]
impl super::BloomStorage for DuckS3Storage {
    async fn set_node_bloom(&mut self, id: usize, bloom: &[u8]) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rt_node_blooms (id, bloom) VALUES (?, ?)
             ON CONFLICT (id) DO UPDATE SET bloom = excluded.bloom",
            params![id as i64, bloom.to_vec()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_node_bloom(&self, id: usize) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT bloom FROM rt_node_blooms WHERE id = ?",
            params![id as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(db_err)
    }
}

// ==================== TimeseriesStorage ====================

/// SQL subquery nguồn dữ liệu timeseries: Parquet trên S3 (khi `has_s3`)
/// UNION buffer local. Placeholder thứ nhất = glob path (chỉ khi `has_s3`).
fn merged_ts_sql(has_s3: bool) -> String {
    if has_s3 {
        "(SELECT id, series, ts, value, 0 AS src FROM read_parquet(?, union_by_name = true) \
         UNION ALL \
         SELECT id, series, ts, value, 1 AS src FROM ts_points)".to_string()
    } else {
        "(SELECT id, series, ts, value, 1 AS src FROM ts_points)".to_string()
    }
}

/// Dedup `(series, ts)` — buffer local (`src = 1`) thắng Parquet đã flush;
/// bản trùng trong Parquet (crash giữa COPY và DELETE) cùng id → cùng value.
const TS_DEDUP: &str =
    "QUALIFY row_number() OVER (PARTITION BY series, ts ORDER BY src DESC, id DESC) = 1";

/// Thu thập `(ts, value)` từ `query_map`.
fn collect_rows(
    rows: impl Iterator<Item = duckdb::Result<(i64, Vec<u8>)>>,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let mut out = Vec::new();
    for r in rows {
        let (ts, value) = r.map_err(db_err)?;
        out.push((ts as u64, value));
    }
    Ok(out)
}

impl DuckS3Storage {
    /// `true` nếu có Parquet timeseries trên S3 — quyết định có đọc nhánh
    /// `read_parquet` hay chỉ buffer local.
    fn has_ts_files(&self, conn: &duckdb::Connection) -> Result<bool> {
        match &self.s3 {
            Some(cfg) => self.ts_files_exist(conn, cfg),
            None => Ok(false),
        }
    }

    /// Chuẩn hoá câu query timeseries (replace token) + prepare statement.
    /// Placeholder: khi có S3 → `[glob, series, ...]`, không → `[series, ...]`.
    fn prepare_ts<'c>(
        &self,
        conn: &'c duckdb::Connection,
        sql_tpl: &str,
    ) -> Result<(duckdb::Statement<'c>, bool)> {
        let has_s3 = self.has_ts_files(conn)?;
        let sql = sql_tpl
            .replacen("{merged}", &merged_ts_sql(has_s3), 1)
            .replace("{TS_DEDUP}", TS_DEDUP);
        let stmt = conn.prepare(&sql).map_err(db_err)?;
        Ok((stmt, has_s3))
    }
}

#[async_trait]
impl TimeseriesStorage for DuckS3Storage {
    async fn append(&self, series: &[u8], timestamp: u64, value: &[u8]) -> Result<()> {
        let should_flush = {
            let conn = self.conn.lock();
            conn.execute(
                "INSERT INTO ts_points (id, series, ts, value)
                 VALUES (nextval('ts_seq'), ?, ?, ?)",
                params![series.to_vec(), timestamp as i64, value.to_vec()],
            )
            .map_err(db_err)?;
            if self.s3.is_some() {
                let n: i64 = conn
                    .query_row("SELECT COUNT(*) FROM ts_points", [], |r| r.get(0))
                    .map_err(db_err)?;
                n as usize >= self.flush_threshold
            } else {
                false
            }
        };
        // Flush ngoài lock-append — flush tự lấy lock.
        if should_flush {
            self.flush_timeseries()?;
        }
        Ok(())
    }

    async fn range(
        &self,
        series: &[u8],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let conn = self.conn.lock();
        let (mut stmt, has_s3) = self.prepare_ts(
            &conn,
            "SELECT ts, value FROM {merged} WHERE series = ? AND ts >= ? AND ts <= ? \
             {TS_DEDUP} ORDER BY ts ASC",
        )?;
        if has_s3 {
            let glob = self.s3.as_ref().unwrap().url("ts_points/*.parquet");
            collect_rows(
                stmt.query_map(
                    params![glob, series.to_vec(), start_ts as i64, end_ts as i64],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .map_err(db_err)?,
            )
        } else {
            collect_rows(
                stmt.query_map(
                    params![series.to_vec(), start_ts as i64, end_ts as i64],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .map_err(db_err)?,
            )
        }
    }

    async fn latest(&self, series: &[u8], limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let conn = self.conn.lock();
        let (mut stmt, has_s3) = self.prepare_ts(
            &conn,
            "SELECT ts, value FROM {merged} WHERE series = ? \
             {TS_DEDUP} ORDER BY ts DESC LIMIT ?",
        )?;
        let mut out = if has_s3 {
            let glob = self.s3.as_ref().unwrap().url("ts_points/*.parquet");
            collect_rows(
                stmt.query_map(
                    params![glob, series.to_vec(), limit as i64],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .map_err(db_err)?,
            )?
        } else {
            collect_rows(
                stmt.query_map(
                    params![series.to_vec(), limit as i64],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .map_err(db_err)?,
            )?
        };
        out.reverse();
        Ok(out)
    }

    async fn first(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.first_last(series, "ASC").await
    }

    async fn last(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.first_last(series, "DESC").await
    }

    async fn clear_series(&self, series: &[u8]) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM ts_points WHERE series = ?",
            params![series.to_vec()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn clear_all_series(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM ts_points", []).map_err(db_err)?;
        Ok(())
    }
}

impl DuckS3Storage {
    async fn first_last(&self, series: &[u8], dir: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let conn = self.conn.lock();
        let (mut stmt, has_s3) = self.prepare_ts(
            &conn,
            &format!(
                "SELECT ts, value FROM {{merged}} WHERE series = ? \
                 {{TS_DEDUP}} ORDER BY ts {dir} LIMIT 1"
            ),
        )?;
        let rows = if has_s3 {
            let glob = self.s3.as_ref().unwrap().url("ts_points/*.parquet");
            collect_rows(
                stmt.query_map(params![glob, series.to_vec()], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(db_err)?,
            )
        } else {
            collect_rows(
                stmt.query_map(params![series.to_vec()], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(db_err)?,
            )
        };
        Ok(rows?.into_iter().next())
    }
}

// ==================== PatternStorage ====================

#[async_trait]
impl PatternStorage for DuckS3Storage {
    async fn add(&self, pattern: &str) -> Result<()> {
        if pattern.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock();
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM ac_patterns WHERE pattern = ?",
                params![pattern],
                |r| r.get(0),
            )
            .optional()
            .map_err(db_err)?;
        if exists.is_some() {
            return Ok(());
        }
        conn.execute(
            "INSERT INTO ac_patterns (pattern, id) VALUES (?, \
             (SELECT COALESCE(MAX(id), 0) + 1 FROM ac_patterns))",
            params![pattern],
        )
        .map_err(db_err)?;
        Ok(())
    }

    async fn contains(&self, pattern: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let row: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM ac_patterns WHERE pattern = ?",
                params![pattern],
                |r| r.get(0),
            )
            .optional()
            .map_err(db_err)?;
        Ok(row.is_some())
    }

    async fn get_all(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT pattern FROM ac_patterns ORDER BY id ASC")
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(db_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(db_err)?);
        }
        Ok(out)
    }

    async fn count(&self) -> Result<usize> {
        let conn = self.conn.lock();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM ac_patterns", [], |r| r.get(0))
            .map_err(db_err)?;
        Ok(n as usize)
    }

    async fn remove(&self, pattern: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let n = conn
            .execute("DELETE FROM ac_patterns WHERE pattern = ?", params![pattern])
            .map_err(db_err)?;
        Ok(n > 0)
    }

    async fn clear(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM ac_patterns", []).map_err(db_err)?;
        Ok(())
    }
}

// ==================== DuckS3Tx ====================

/// Transaction cho `DuckS3Storage`: buffer toàn bộ mutation, áp dụng atomic
/// trong một DuckDB transaction tại `commit` (giống `SqliteTx`).
///
/// `new_node` đọc counter hiện tại mỗi lần gọi, `id = next + nodes.len()` —
/// giống `RedisTx`; `commit` bump counter lên `GREATEST(next, max+1)` nên id
/// không bao giờ trùng.
pub struct DuckS3Tx {
    conn: Arc<Mutex<duckdb::Connection>>,
    nodes: Vec<(usize, Vec<u8>, usize)>,
    ops: Vec<CategoryTxOp>,
}

#[async_trait]
impl CategoryTx for DuckS3Tx {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let conn = self.conn.lock();
        let next: i64 = conn
            .query_row("SELECT next FROM rt_counter WHERE id = 1", [], |r| r.get(0))
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
        let DuckS3Tx { conn, nodes, ops } = *self;
        let conn = conn.lock();
        conn.execute_batch("BEGIN TRANSACTION;").map_err(db_err)?;
        let result = (|| -> Result<()> {
            // 1. Materialize node mới trước — để ops add/move trỏ tới hợp lệ.
            for (id, prefix, record) in &nodes {
                conn.execute(
                    "INSERT INTO rt_nodes (id, prefix, record) VALUES (?, ?, ?)",
                    params![*id as i64, prefix, *record as i64],
                )
                .map_err(db_err)?;
            }
            // 2. Bump counter lên max(reserved) + 1 — id tx cấp vẫn unique.
            if let Some(max_id) = nodes.iter().map(|(id, _, _)| *id).max() {
                conn.execute(
                    "UPDATE rt_counter SET next = GREATEST(next, ?) WHERE id = 1",
                    params![(max_id + 1) as i64],
                )
                .map_err(db_err)?;
            }
            // 3. Áp dụng toàn bộ ops — atomic, không lộ trạng thái trung gian.
            for op in &ops {
                match op {
                    CategoryTxOp::AddChild { parent, child } => {
                        conn.execute(
                            "INSERT INTO rt_children (parent, child) VALUES (?, ?)
                             ON CONFLICT DO NOTHING",
                            params![*parent as i64, *child as i64],
                        )
                        .map_err(db_err)?;
                    }
                    CategoryTxOp::MoveChild { from, to, child } => {
                        conn.execute(
                            "DELETE FROM rt_children WHERE parent = ? AND child = ?",
                            params![*from as i64, *child as i64],
                        )
                        .map_err(db_err)?;
                        conn.execute(
                            "INSERT INTO rt_children (parent, child) VALUES (?, ?)
                             ON CONFLICT DO NOTHING",
                            params![*to as i64, *child as i64],
                        )
                        .map_err(db_err)?;
                    }
                    CategoryTxOp::UpdateNode { id, prefix, record } => {
                        if let Some(p) = prefix {
                            conn.execute(
                                "UPDATE rt_nodes SET prefix = ? WHERE id = ?",
                                params![p, *id as i64],
                            )
                            .map_err(db_err)?;
                        }
                        if let Some(r) = record {
                            conn.execute(
                                "UPDATE rt_nodes SET record = ? WHERE id = ?",
                                params![r, *id as i64],
                            )
                            .map_err(db_err)?;
                        }
                    }
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => conn.execute_batch("COMMIT;").map_err(db_err)?,
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(e);
            }
        }
        Ok(())
    }
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.duckdb");
        let path = path.to_string_lossy().into_owned();
        (dir, path)
    }

    #[tokio::test]
    async fn test_new_node_and_get_node() {
        let (_d, path) = tmp_path();
        let mut s = DuckS3Storage::open(&path).await.unwrap();
        let id = s.new_node(b"hello".to_vec(), 42).await.unwrap();
        assert_ne!(id, EMPTY);
        let (prefix, record) = s.get_node(id).await.unwrap();
        assert_eq!(prefix, b"hello");
        assert_eq!(record, 42);
    }

    #[tokio::test]
    async fn test_update_node_missing() {
        let (_d, path) = tmp_path();
        let mut s = DuckS3Storage::open(&path).await.unwrap();
        assert!(matches!(
            s.update_node(9999, Some(b"x".to_vec()), None).await,
            Err(StorageError::BranchOutOfRange(9999))
        ));
        assert!(matches!(
            s.get_node(9999).await,
            Err(StorageError::BranchOutOfRange(9999))
        ));
    }

    #[tokio::test]
    async fn test_children_roots_and_tx() {
        let (_d, path) = tmp_path();
        let mut s = DuckS3Storage::open(&path).await.unwrap();
        let parent = s.new_node(b"p".to_vec(), 0).await.unwrap();
        let c1 = s.new_node(b"c1".to_vec(), 1).await.unwrap();
        let c2 = s.new_node(b"c2".to_vec(), 2).await.unwrap();
        // Mutate qua Tx — production chỉ đi qua Tx, không có Storage::add_child.
        let mut tx = s.new_tx();
        tx.add_child(parent, c1).await.unwrap();
        tx.add_child(parent, c2).await.unwrap();
        tx.commit().await.unwrap();
        let children = s.get_children(parent).await.unwrap();
        assert_eq!(children, vec![c1, c2]);

        assert_eq!(s.get_root(3).await.unwrap(), EMPTY);
        s.set_root(3, parent).await.unwrap();
        assert_eq!(s.get_root(3).await.unwrap(), parent);
    }

    #[tokio::test]
    async fn test_tx_nodes_invisible_before_commit() {
        let (_d, path) = tmp_path();
        let s = DuckS3Storage::open(&path).await.unwrap();
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
        let mut s = DuckS3Storage::open(&path).await.unwrap();
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
    async fn test_meta_chains_edges_shortcuts_roundtrip() {
        let (_d, path) = tmp_path();
        let mut s = DuckS3Storage::open(&path).await.unwrap();

        assert_eq!(s.get_meta(7).await.unwrap(), None);
        assert_eq!(s.get_key_len(7).await.unwrap(), None);
        s.set_meta(7, b"call-site-info").await.unwrap();
        s.set_key_len(7, 5).await.unwrap();
        assert_eq!(s.get_meta(7).await.unwrap().as_deref(), Some(&b"call-site-info"[..]));
        assert_eq!(s.get_key_len(7).await.unwrap(), Some(5));

        s.set_node_meta(3, b"node-json").await.unwrap();
        assert_eq!(s.get_node_meta(3).await.unwrap().as_deref(), Some(&b"node-json"[..]));

        s.set_chain(9, &[1, 2, 3]).await.unwrap();
        assert_eq!(s.get_chain(9).await.unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(s.get_chain(10).await.unwrap(), None);

        assert_eq!(s.get_edge_data(7).await.unwrap(), None);
        s.set_edge_data(7, b"call-edge").await.unwrap();
        s.set_edge_data(7, b"call-edge-2").await.unwrap();
        assert_eq!(s.get_edge_data(7).await.unwrap().as_deref(), Some(&b"call-edge-2"[..]));
        let mut edges = Vec::new();
        s.for_each_edge_data(&mut |id, data| {
            edges.push((id, data.to_vec()));
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(edges, vec![(7, b"call-edge-2".to_vec())]);

        s.add_shortcut_node(1, b"l", 10).await.unwrap();
        s.add_shortcut_node(1, b"l", 20).await.unwrap();
        s.add_shortcut_node(1, b"o", 10).await.unwrap();
        s.add_shortcut_node(2, b"l", 30).await.unwrap();
        assert_eq!(s.get_shortcut_nodes(1, b"l").await.unwrap(), vec![10, 20]);
        assert_eq!(s.get_shortcut_nodes(2, b"l").await.unwrap(), vec![30]);
    }

    #[tokio::test]
    async fn test_persists_across_reopen() {
        let (_d, path) = tmp_path();
        let parent;
        {
            let mut s = DuckS3Storage::open(&path).await.unwrap();
            parent = s.new_node(b"hello".to_vec(), 42).await.unwrap();
            s.set_root(3, parent).await.unwrap();
            s.set_meta(42, b"meta-42").await.unwrap();
            s.set_key_len(42, 5).await.unwrap();
            let mut tx = s.new_tx();
            let extra = tx.new_node(b"z".to_vec(), 99).await.unwrap();
            tx.add_child(parent, extra).await.unwrap();
            tx.commit().await.unwrap();
        }
        let mut s = DuckS3Storage::open(&path).await.unwrap();
        assert_eq!(s.get_node(parent).await.unwrap(), (b"hello".to_vec(), 42));
        assert_eq!(s.get_root(3).await.unwrap(), parent);
        assert_eq!(s.get_meta(42).await.unwrap().as_deref(), Some(&b"meta-42"[..]));
        assert_eq!(s.get_key_len(42).await.unwrap(), Some(5));
        assert_eq!(s.get_children(parent).await.unwrap().len(), 1);
        // Node id mới tiếp tục cấp trên counter đã persist.
        let n = s.new_node(b"new".to_vec(), 1).await.unwrap();
        assert!(n > parent);
    }

    #[tokio::test]
    async fn test_timeseries_roundtrip() {
        use crate::storage::TimeseriesStorage;

        let (_d, path) = tmp_path();
        let s = DuckS3Storage::open(&path).await.unwrap();
        s.append(b"cpu", 100, b"a").await.unwrap();
        s.append(b"cpu", 200, b"b").await.unwrap();
        s.append(b"cpu", 300, b"c").await.unwrap();
        s.append(b"mem", 50, b"x").await.unwrap();

        assert_eq!(s.first(b"cpu").await.unwrap(), Some((100, b"a".to_vec())));
        assert_eq!(s.last(b"cpu").await.unwrap(), Some((300, b"c".to_vec())));
        assert_eq!(
            s.range(b"cpu", 150, 300).await.unwrap(),
            vec![(200, b"b".to_vec()), (300, b"c".to_vec())]
        );
        assert_eq!(
            s.latest(b"cpu", 2).await.unwrap(),
            vec![(200, b"b".to_vec()), (300, b"c".to_vec())]
        );
        assert_eq!(s.last(b"mem").await.unwrap(), Some((50, b"x".to_vec())));

        s.clear_series(b"cpu").await.unwrap();
        assert_eq!(s.last(b"cpu").await.unwrap(), None);
        assert_eq!(s.last(b"mem").await.unwrap(), Some((50, b"x".to_vec())));

        s.clear_all_series().await.unwrap();
        assert_eq!(s.last(b"mem").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_pattern_roundtrip() {
        use crate::storage::PatternStorage;

        let (_d, path) = tmp_path();
        let s = DuckS3Storage::open(&path).await.unwrap();
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
        let s = DuckS3Storage::open(&path).await.unwrap();
        assert_eq!(s.get_all().await.unwrap(), vec!["he", "she"]);
    }
}
