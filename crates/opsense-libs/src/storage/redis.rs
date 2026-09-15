//! Redis-backed radix-node storage.
//!
//! Cấu trúc key:
//! | Key                      | Kiểu  | Mục đích                  |
//! |--------------------------|-------|---------------------------|
//! | `{prefix}:branch`        | List  | prefix của từng node      |
//! | `{prefix}:record`        | List  | record của từng node      |
//! | `{prefix}:forward:{id}`  | Set   | children list của node    |
//! | `{prefix}:endpoint`      | Hash  | root ID cho mỗi shard     |
//! | `{prefix}:meta`          | Hash  | record_idx → metadata     |
//! | `{prefix}:keylen`        | Hash  | record_idx → key length   |
//! | `{prefix}:edgedata`      | Hash  | edge id → edge metadata   |
//! | `{prefix}:nodemeta`      | Hash  | element id → node metadata|
//! | `{prefix}:chains`        | Hash  | record → chain bytes      |
//! | `{prefix}:shortcut:{shard}:{elem}` | Set | node ids chứa elem |

use std::sync::Arc;

use redis::aio::MultiplexedConnection;
use tokio::sync::Mutex;

use async_trait::async_trait;

use super::{
    CategoryStorage, CategoryTx, CategoryTxOp, ChainStorage, EdgeDataStorage, NodeMetaStorage,
    PatternStorage, Result, ShortcutsStorage, StorageError, TimeseriesStorage, decode_chain,
    encode_chain,
};

// ==================== KeyBuilder ====================

type KeyFormatter = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Cấu hình key cho Redis storage.
#[derive(Clone)]
pub struct KeyBuilder {
    prefix: String,
    formatter: Option<KeyFormatter>,
}

impl KeyBuilder {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            formatter: None,
        }
    }

    #[allow(dead_code)] // API tiện ích (caller tạo KeyBuilder tuỳ biến) — chưa dùng nội bộ.
    pub fn with_formatter(prefix: &str, f: KeyFormatter) -> Self {
        Self {
            prefix: prefix.to_string(),
            formatter: Some(f),
        }
    }

    /// `key("branch")` → `"{prefix}:branch"`
    pub fn key(&self, name: &str) -> String {
        match &self.formatter {
            Some(f) => f(name),
            None => format!("{}:{}", self.prefix, name),
        }
    }

    /// `indexed("forward", 5)` → `"{prefix}:forward:5"`
    pub fn indexed(&self, name: &str, idx: usize) -> String {
        self.key(&format!("{name}:{idx}"))
    }

    /// `shortcut(3, [0x01])` → `"{prefix}:shortcut:3:{0x01}"`
    /// (bytes của elem nối trực tiếp — Redis key binary-safe).
    pub fn shortcut(&self, shard: usize, elem: &[u8]) -> Vec<u8> {
        let mut k = self.key(&format!("shortcut:{shard}")).into_bytes();
        k.push(b':');
        k.extend_from_slice(elem);
        k
    }

    /// Prefix chung của mọi shortcut key: `"{prefix}:shortcut:"`.
    /// Dùng làm MATCH pattern khi SCAN để xoá toàn bộ shortcuts.
    pub fn shortcut_prefix(&self) -> String {
        self.key("shortcut") + ":"
    }
}

/// Helper shorthand: `cmd("LLEN")` → `redis::cmd("LLEN")`
fn cmd(name: &str) -> redis::Cmd {
    redis::cmd(name)
}

/// Map một `redis::RedisError` → `StorageError`.
fn redis_err(e: redis::RedisError) -> StorageError {
    StorageError::Internal(e.to_string())
}

// ==================== RedisStorage ====================

pub struct RedisStorage {
    conn: Arc<Mutex<MultiplexedConnection>>,
    kb: KeyBuilder,
}

impl RedisStorage {
    #[allow(dead_code)]
    async fn lock(&self) -> tokio::sync::MutexGuard<'_, MultiplexedConnection> {
        self.conn.lock().await
    }

    #[allow(dead_code)]
    pub async fn new(client: redis::Client, prefix: &str) -> Result<Self> {
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let s = Self {
            conn: Arc::new(Mutex::new(conn)),
            kb: KeyBuilder::new(prefix),
        };
        s.init().await?;
        Ok(s)
    }

    #[allow(dead_code)] // helper — chưa có caller nội bộ.
    pub async fn from_multiplexed(conn: MultiplexedConnection, prefix: &str) -> Result<Self> {
        let s = Self {
            conn: Arc::new(Mutex::new(conn)),
            kb: KeyBuilder::new(prefix),
        };
        s.init().await?;
        Ok(s)
    }

    #[allow(dead_code)] // helper — chưa có caller nội bộ.
    pub async fn with_key_builder(client: redis::Client, kb: KeyBuilder) -> Result<Self> {
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let s = Self {
            conn: Arc::new(Mutex::new(conn)),
            kb,
        };
        s.init().await?;
        Ok(s)
    }

    async fn init(&self) -> Result<()> {
        let mut conn = self.lock().await;
        let exists: bool = cmd("EXISTS")
            .arg(self.kb.key("branch"))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        if !exists {
            redis::pipe()
                .atomic()
                .rpush(self.kb.key("branch"), b"" as &[u8])
                .rpush(self.kb.key("record"), 0i64)
                .exec_async(&mut *conn)
                .await
                .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    /// Độ dài hiện tại của branch list = số node (gồm sentinel).
    /// Node id tiếp theo = len - 1.
    #[allow(dead_code)] // helper — chưa có caller nội bộ.
    async fn node_len(&self) -> Result<usize> {
        let mut conn = self.lock().await;
        let len: usize = cmd("LLEN")
            .arg(self.kb.key("branch"))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(len)
    }
}

#[async_trait]
impl CategoryStorage for RedisStorage {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let mut conn = self.lock().await;
        let result: redis::Value = redis::pipe()
            .atomic()
            .rpush(self.kb.key("branch"), &prefix[..])
            .rpush(self.kb.key("record"), record as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;

        let len: usize = match result {
            redis::Value::Array(ref items) => match items.first() {
                Some(redis::Value::Int(n)) => *n as usize,
                _ => cmd("LLEN")
                    .arg(self.kb.key("branch"))
                    .query_async::<usize>(&mut *conn)
                    .await
                    .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?,
            },
            _ => cmd("LLEN")
                .arg(self.kb.key("branch"))
                .query_async::<usize>(&mut *conn)
                .await
                .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?,
        };

        Ok(len - 1)
    }

    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()> {
        let mut conn = self.lock().await;
        let mut pipe = redis::pipe();
        pipe.atomic();
        if let Some(p) = prefix {
            pipe.lset(self.kb.key("branch"), id as isize, &p[..]);
        }
        if let Some(r) = record {
            pipe.lset(self.kb.key("record"), id as isize, r as i64);
        }
        pipe.exec_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_node(&self, id: usize) -> Result<(Vec<u8>, usize)> {
        let mut conn = self.lock().await;
        let prefix: Vec<u8> = cmd("LINDEX")
            .arg(self.kb.key("branch"))
            .arg(id as isize)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        let rec: i64 = cmd("LINDEX")
            .arg(self.kb.key("record"))
            .arg(id as isize)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok((prefix, rec as usize))
    }

    async fn get_children(&self, id: usize) -> Result<Vec<usize>> {
        let mut conn = self.lock().await;
        let children: Vec<i64> = cmd("SMEMBERS")
            .arg(self.kb.indexed("forward", id))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(children.into_iter().map(|x| x as usize).collect())
    }

    async fn set_root(&mut self, shard: usize, root: usize) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("endpoint"))
            .arg(shard as i64)
            .arg(root as i64)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_root(&self, shard: usize) -> Result<usize> {
        let mut conn = self.lock().await;
        let root: Option<i64> = cmd("HGET")
            .arg(self.kb.key("endpoint"))
            .arg(shard as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(root.unwrap_or(0) as usize)
    }

    fn new_tx(&self) -> Box<dyn CategoryTx> {
        Box::new(RedisTx {
            conn: self.conn.clone(),
            kb: self.kb.clone(),
            nodes: Vec::new(),
            ops: Vec::new(),
        })
    }
}

#[async_trait]
impl EdgeDataStorage for RedisStorage {
    async fn set_edge_data(&mut self, edge: usize, data: &[u8]) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("edgedata"))
            .arg(edge as i64)
            .arg(data)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_edge_data(&self, edge: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.lock().await;
        let data: Option<Vec<u8>> = cmd("HGET")
            .arg(self.kb.key("edgedata"))
            .arg(edge as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(data)
    }

    async fn clear_edges(&mut self) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("DEL")
            .arg(self.kb.key("edgedata"))
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ChainStorage for RedisStorage {
    async fn set_chain(&mut self, record: usize, chain: &[u64]) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("chains"))
            .arg(record as i64)
            .arg(encode_chain(chain))
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_chain(&self, record: usize) -> Result<Option<Vec<u64>>> {
        let mut conn = self.lock().await;
        let bytes: Option<Vec<u8>> = cmd("HGET")
            .arg(self.kb.key("chains"))
            .arg(record as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(bytes.map(|b| decode_chain(&b)))
    }

    async fn clear_chains(&mut self) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("DEL")
            .arg(self.kb.key("chains"))
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ShortcutsStorage for RedisStorage {
    async fn add_shortcut_node(&mut self, shard: usize, elem: &[u8], node_id: usize) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("SADD")
            .arg(self.kb.shortcut(shard, elem))
            .arg(node_id as i64)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_shortcut_nodes(&self, shard: usize, elem: &[u8]) -> Result<Vec<usize>> {
        let mut conn = self.lock().await;
        let nodes: Vec<i64> = cmd("SMEMBERS")
            .arg(self.kb.shortcut(shard, elem))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(nodes.into_iter().map(|x| x as usize).collect())
    }

    async fn clear_shortcuts(&mut self) -> Result<()> {
        let mut conn = self.lock().await;
        let pattern = format!("{}*", self.kb.shortcut_prefix());
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut *conn)
                .await
                .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
            for key in keys {
                cmd("DEL")
                    .arg(key)
                    .query_async::<()>(&mut *conn)
                    .await
                    .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
            }
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NodeMetaStorage for RedisStorage {
    async fn set_node_meta(&mut self, elem: usize, meta: &[u8]) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("nodemeta"))
            .arg(elem as i64)
            .arg(meta)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_node_meta(&self, elem: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.lock().await;
        let meta: Option<Vec<u8>> = cmd("HGET")
            .arg(self.kb.key("nodemeta"))
            .arg(elem as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(meta)
    }

    async fn clear_node_meta(&mut self) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("DEL")
            .arg(self.kb.key("nodemeta"))
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn set_meta(&mut self, record: usize, meta: &[u8]) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("meta"))
            .arg(record as i64)
            .arg(meta)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_meta(&self, record: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.lock().await;
        let meta: Option<Vec<u8>> = cmd("HGET")
            .arg(self.kb.key("meta"))
            .arg(record as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(meta)
    }

    async fn set_key_len(&mut self, record: usize, len: usize) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("keylen"))
            .arg(record as i64)
            .arg(len as i64)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_key_len(&self, record: usize) -> Result<Option<usize>> {
        let mut conn = self.lock().await;
        let len: Option<i64> = cmd("HGET")
            .arg(self.kb.key("keylen"))
            .arg(record as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(len.map(|x| x as usize))
    }
}

#[cfg(feature = "bloom-search")]
#[async_trait]
impl super::BloomStorage for RedisStorage {
    async fn set_node_bloom(&mut self, id: usize, bloom: &[u8]) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("HSET")
            .arg(self.kb.key("node_bloom"))
            .arg(id)
            .arg(bloom)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn get_node_bloom(&self, id: usize) -> Result<Option<Vec<u8>>> {
        let mut conn = self.lock().await;
        let bloom: Option<Vec<u8>> = cmd("HGET")
            .arg(self.kb.key("node_bloom"))
            .arg(id)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(bloom)
    }
}

#[async_trait]
impl TimeseriesStorage for RedisStorage {
    async fn append(&self, series: &[u8], timestamp: u64, value: &[u8]) -> Result<()> {
        let mut conn = self.lock().await;
        // Counter per-series để tạo member unique (ZSET member phải unique).
        let seq: i64 = cmd("INCR")
            .arg(self.ts_seq_key(series))
            .query_async(&mut *conn)
            .await
            .map_err(redis_err)?;
        // member = 8-byte BE seq || value → value lấy lại bằng cách bỏ prefix.
        let mut member = seq.to_be_bytes().to_vec();
        member.extend_from_slice(value);
        cmd("ZADD")
            .arg(self.ts_index_key(series))
            .arg(timestamp as f64)
            .arg(member)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn range(
        &self,
        series: &[u8],
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut conn = self.lock().await;
        let rows: Vec<(Vec<u8>, f64)> = cmd("ZRANGEBYSCORE")
            .arg(self.ts_index_key(series))
            .arg(start_ts as f64)
            .arg(end_ts as f64)
            .arg("WITHSCORES")
            .query_async(&mut *conn)
            .await
            .map_err(redis_err)?;
        Ok(rows
            .into_iter()
            .map(|(m, s)| (s as u64, ts_value_of(&m)))
            .collect())
    }

    async fn latest(&self, series: &[u8], limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut conn = self.lock().await;
        let rows: Vec<(Vec<u8>, f64)> = cmd("ZREVRANGE")
            .arg(self.ts_index_key(series))
            .arg(0)
            .arg(limit.saturating_sub(1) as isize)
            .arg("WITHSCORES")
            .query_async(&mut *conn)
            .await
            .map_err(redis_err)?;
        let mut out: Vec<(u64, Vec<u8>)> = rows
            .into_iter()
            .map(|(m, s)| (s as u64, ts_value_of(&m)))
            .collect();
        out.reverse();
        Ok(out)
    }

    async fn first(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let mut conn = self.lock().await;
        let rows: Vec<(Vec<u8>, f64)> = cmd("ZRANGE")
            .arg(self.ts_index_key(series))
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut *conn)
            .await
            .map_err(redis_err)?;
        Ok(rows
            .into_iter()
            .next()
            .map(|(m, s)| (s as u64, ts_value_of(&m))))
    }

    async fn last(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let mut conn = self.lock().await;
        let rows: Vec<(Vec<u8>, f64)> = cmd("ZREVRANGE")
            .arg(self.ts_index_key(series))
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut *conn)
            .await
            .map_err(redis_err)?;
        Ok(rows
            .into_iter()
            .next()
            .map(|(m, s)| (s as u64, ts_value_of(&m))))
    }

    async fn clear_series(&self, series: &[u8]) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("DEL")
            .arg(self.ts_index_key(series))
            .arg(self.ts_seq_key(series))
            .query_async::<()>(&mut *conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn clear_all_series(&self) -> Result<()> {
        let mut conn = self.lock().await;
        let patterns = [self.kb.key("ts") + ":*", self.kb.key("tsseq") + ":*"];
        for pat in patterns {
            let mut cursor: u64 = 0;
            loop {
                let (next, keys): (u64, Vec<String>) = cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pat)
                    .arg("COUNT")
                    .arg(500)
                    .query_async(&mut *conn)
                    .await
                    .map_err(redis_err)?;
                for k in keys {
                    cmd("DEL")
                        .arg(k)
                        .query_async::<()>(&mut *conn)
                        .await
                        .map_err(redis_err)?;
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl PatternStorage for RedisStorage {
    async fn add(&self, pattern: &str) -> Result<()> {
        if pattern.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock().await;
        let exists: bool = cmd("HEXISTS")
            .arg(self.kb.key("patterns"))
            .arg(pattern)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        if exists {
            return Ok(());
        }
        // id = INCR counter → unique ngay cả khi đã từng remove (tránh trùng id).
        let id: i64 = cmd("INCR")
            .arg(self.kb.key("pattern_seq"))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        cmd("HSET")
            .arg(self.kb.key("patterns"))
            .arg(pattern)
            .arg(id)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn contains(&self, pattern: &str) -> Result<bool> {
        let mut conn = self.lock().await;
        let exists: bool = cmd("HEXISTS")
            .arg(self.kb.key("patterns"))
            .arg(pattern)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(exists)
    }

    async fn get_all(&self) -> Result<Vec<String>> {
        let mut conn = self.lock().await;
        // HGETALL → (pattern, id); sort theo id để giữ thứ tự đăng ký.
        let items: Vec<(String, i64)> = cmd("HGETALL")
            .arg(self.kb.key("patterns"))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        let mut v: Vec<(i64, String)> = items.into_iter().map(|(p, id)| (id, p)).collect();
        v.sort_by_key(|(id, _)| *id);
        Ok(v.into_iter().map(|(_, p)| p).collect())
    }

    async fn count(&self) -> Result<usize> {
        let mut conn = self.lock().await;
        let n: i64 = cmd("HLEN")
            .arg(self.kb.key("patterns"))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(n as usize)
    }

    async fn remove(&self, pattern: &str) -> Result<bool> {
        let mut conn = self.lock().await;
        let n: i64 = cmd("HDEL")
            .arg(self.kb.key("patterns"))
            .arg(pattern)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(n > 0)
    }

    async fn clear(&self) -> Result<()> {
        let mut conn = self.lock().await;
        cmd("DEL")
            .arg(self.kb.key("patterns"))
            .arg(self.kb.key("pattern_seq"))
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

/// Key của ZSET index cho một series (score = timestamp).
impl RedisStorage {
    fn ts_index_key(&self, series: &[u8]) -> Vec<u8> {
        let mut k = self.kb.key("ts").into_bytes();
        k.push(b':');
        k.extend_from_slice(series);
        k
    }

    /// Key của counter per-series (dùng sinh member unique cho ZSET).
    fn ts_seq_key(&self, series: &[u8]) -> Vec<u8> {
        let mut k = self.kb.key("tsseq").into_bytes();
        k.push(b':');
        k.extend_from_slice(series);
        k
    }
}

/// Tách phần value khỏi member (bỏ 8-byte seq prefix).
fn ts_value_of(member: &[u8]) -> Vec<u8> {
    if member.len() >= 8 {
        member[8..].to_vec()
    } else {
        member.to_vec()
    }
}

// ==================== Redis Transaction ====================

/// Transaction cho `RedisStorage`.
///
/// - `new_node` snapshot độ dài branch list lúc tạo tx, id = base + n
///   (giả định single-connection — toàn bộ command đi qua cùng 1 mutex).
/// - `commit` build một MULTI/EXEC pipeline: RPUSH toàn bộ node mới trước,
///   rồi áp dụng các op cấu trúc — atomic, không lộ trạng thái trung gian.
pub struct RedisTx {
    conn: Arc<Mutex<MultiplexedConnection>>,
    kb: KeyBuilder,
    nodes: Vec<(usize, Vec<u8>, usize)>,
    ops: Vec<CategoryTxOp>,
}

#[async_trait]
impl CategoryTx for RedisTx {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let base = self.node_len_checked().await?;
        let id = base + self.nodes.len();
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
        let RedisTx {
            conn,
            kb,
            nodes,
            ops,
            ..
        } = *self;

        let mut conn = conn.lock().await;
        let mut pipe = redis::pipe();
        pipe.atomic();

        // 1. RPUSH toàn bộ node mới (sentinel đã có sẵn ở index 0).
        for (_, prefix, record) in &nodes {
            pipe.rpush(kb.key("branch"), &prefix[..]);
            pipe.rpush(kb.key("record"), *record as i64);
        }

        // 2. Áp dụng ops.
        for op in ops {
            match op {
                CategoryTxOp::AddChild { parent, child } => {
                    pipe.cmd("SADD")
                        .arg(kb.indexed("forward", parent))
                        .arg(child as i64)
                        .ignore();
                }
                CategoryTxOp::MoveChild { from, to, child } => {
                    pipe.cmd("SREM")
                        .arg(kb.indexed("forward", from))
                        .arg(child as i64)
                        .ignore();
                    pipe.cmd("SADD")
                        .arg(kb.indexed("forward", to))
                        .arg(child as i64)
                        .ignore();
                }
                CategoryTxOp::UpdateNode { id, prefix, record } => {
                    if let Some(p) = prefix {
                        pipe.lset(kb.key("branch"), id as isize, &p[..]);
                    }
                    if let Some(r) = record {
                        pipe.lset(kb.key("record"), id as isize, r as i64);
                    }
                }
            }
        }

        pipe.exec_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

impl RedisTx {
    async fn node_len_checked(&self) -> Result<usize> {
        let mut conn = self.conn.lock().await;
        let len: usize = cmd("LLEN")
            .arg(self.kb.key("branch"))
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::Internal(e.to_string()))?;
        Ok(len)
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU16, Ordering};

    use super::*;
    use crate::storage::CategoryStorage;
    use crate::storage::EMPTY;

    static COUNTER: AtomicU16 = AtomicU16::new(0);

    async fn new_test_storage() -> RedisStorage {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let client = redis::Client::open("redis://127.0.0.1:6379/15")
            .expect("redis connection failed — is redis-server running?");
        RedisStorage::new(client, &format!("test:radix:{}:{n}", pid))
            .await
            .expect("init failed")
    }

    #[tokio::test]
    async fn test_new_node_and_get_node() {
        let mut s = new_test_storage().await;
        let id = s.new_node(b"hello".to_vec(), 42).await.unwrap();
        assert_ne!(id, EMPTY);
        let (prefix, record) = s.get_node(id).await.unwrap();
        assert_eq!(prefix, b"hello");
        assert_eq!(record, 42);
    }

    #[tokio::test]
    async fn test_meta_roundtrip() {
        let mut s = new_test_storage().await;
        assert_eq!(s.get_meta(42).await.unwrap(), None);
        assert_eq!(s.get_key_len(42).await.unwrap(), None);
        s.set_meta(42, b"call-site-info").await.unwrap();
        s.set_key_len(42, 5).await.unwrap();
        assert_eq!(
            s.get_meta(42).await.unwrap().as_deref(),
            Some(b"call-site-info".as_slice())
        );
        assert_eq!(s.get_key_len(42).await.unwrap(), Some(5));
        s.set_meta(42, b"updated").await.unwrap();
        assert_eq!(
            s.get_meta(42).await.unwrap().as_deref(),
            Some(b"updated".as_slice())
        );
    }

    #[tokio::test]
    async fn test_shortcuts_roundtrip() {
        let mut s = new_test_storage().await;
        assert!(s.get_shortcut_nodes(1, b"l").await.unwrap().is_empty());
        s.add_shortcut_node(1, b"l", 10).await.unwrap();
        s.add_shortcut_node(1, b"l", 20).await.unwrap();
        s.add_shortcut_node(1, b"o", 10).await.unwrap();
        let nodes = s.get_shortcut_nodes(1, b"l").await.unwrap();
        assert!(nodes.contains(&10) && nodes.contains(&20));
        assert_eq!(nodes.len(), 2);
        s.clear_shortcuts().await.unwrap();
        assert!(s.get_shortcut_nodes(1, b"l").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_tx_split_commit() {
        let mut s = new_test_storage().await;
        let parent = s.new_node(b"hello".to_vec(), 1).await.unwrap();

        let mut tx = s.new_tx();
        let new_id = tx.new_node(b"p".to_vec(), 2).await.unwrap();
        let leg_id = tx.new_node(b"lo".to_vec(), 1).await.unwrap();
        tx.move_child(parent, leg_id, 0).await.unwrap();
        tx.add_child(parent, leg_id).await.unwrap();
        tx.add_child(parent, new_id).await.unwrap();
        tx.update_node(parent, Some(b"hel".to_vec()), Some(0))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let (prefix, _) = s.get_node(parent).await.unwrap();
        assert_eq!(prefix, b"hel");
        let children = s.get_children(parent).await.unwrap();
        assert!(children.contains(&leg_id));
        assert!(children.contains(&new_id));
    }

    #[tokio::test]
    async fn test_timeseries_roundtrip() {
        use crate::storage::TimeseriesStorage;

        let s = new_test_storage().await;
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
    async fn test_pattern_roundtrip() {
        use crate::storage::PatternStorage;

        let s = new_test_storage().await;
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
    }
}
