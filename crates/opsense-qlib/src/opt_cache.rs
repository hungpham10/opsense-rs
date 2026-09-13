//! Cache kết quả SGD optimize (params + điểm mỗi window) xuống đĩa qua LMDB.
//!
//! Mục đích: cho phép chạy nặng (`init`) một lần — fetch candles + optimize —
//! rồi lưu tham số đã tối ưu xuống đĩa. Về sau các session MCP có thể seed
//! `params` từ cache này thay vì chạy lại optimize tốn kém.
//!
//! Key là chữ ký session (broker + symbol + strategy + range), value là
//! [`OptResult`] được serialize JSON.

use std::io::Error;
use std::path::{Path, PathBuf};

use lmdb::Transaction;
use serde::{Deserialize, Serialize};

/// Kết quả một lần optimize, đủ để seed lại một session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptResult {
    /// Tham số đã tối ưu (weights/DNA).
    pub params: Vec<f64>,
    /// Điểm objective trên từng window (cùng thứ tự `windows` lúc optimize).
    pub scores: Vec<f64>,
}

/// Store LMDB keyed bởi chữ ký session.
pub struct OptCache {
    env: lmdb::Environment,
    db: lmdb::Database,
}

impl OptCache {
    /// Mở (tạo nếu chưa có) database tại `path`.
    ///
    /// LMDB coi `path` là một **thư mục** (chứa `data.mdb`/`lock.mdb`), nên ta
    /// tạo sẵn thư mục đó trước khi mở.
    pub fn open(path: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(path)
            .map_err(|e| Error::other(format!("create opt cache dir {path:?}: {e}")))?;
        let env = lmdb::Environment::new()
            .set_max_dbs(1)
            .set_map_size(64 * 1024 * 1024)
            .open(path)
            .map_err(|e| Error::other(format!("open opt cache {path:?}: {e}")))?;
        let db = env
            .create_db(None, lmdb::DatabaseFlags::empty())
            .map_err(|e| Error::other(format!("create opt db: {e}")))?;
        Ok(Self { env, db })
    }

    /// Lưu kết quả tối ưu theo `key`.
    pub fn put(&self, key: &str, val: &OptResult) -> Result<(), Error> {
        let bytes = serde_json::to_vec(val).map_err(|e| Error::other(e.to_string()))?;
        let mut txn = self
            .env
            .begin_rw_txn()
            .map_err(|e| Error::other(e.to_string()))?;
        txn.put(self.db, &key, &bytes, lmdb::WriteFlags::empty())
            .map_err(|e| Error::other(e.to_string()))?;
        txn.commit().map_err(|e| Error::other(e.to_string()))?;
        Ok(())
    }

    /// Đọc kết quả tối ưu theo `key`, `None` nếu chưa có.
    pub fn get(&self, key: &str) -> Option<OptResult> {
        let txn = self.env.begin_ro_txn().ok()?;
        let bytes = txn.get(self.db, &key).ok()?;
        serde_json::from_slice(bytes).ok()
    }
}

/// Đường dẫn LMDB mặc định: env `LOCAL_OPT_CACHE` hoặc
/// `$HOME/.finpath/opt_cache.lmdb`.
pub fn default_opt_cache_path() -> PathBuf {
    if let Ok(p) = std::env::var("LOCAL_OPT_CACHE") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".finpath").join("opt_cache.lmdb")
}
