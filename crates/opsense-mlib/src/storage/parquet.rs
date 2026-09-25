//! Parquet lakehouse storage — backend persistent cho `Search` với dữ liệu lớn.
//!
//! Thay thế backend DuckDB cũ: **không còn SQL engine nhúng nào cả**. Toàn bộ
//! dữ liệu sống trong các data file Parquet và được điều phối bởi một
//! lakehouse layout (cùng tư tưởng Delta/Iceberg: append-only log + checkpoint
//! + snapshot pointer), chạy trên local filesystem, và có thể **mirror lên S3**.
//!
//! Kiến trúc:
//! - **In-memory state (authoritative)**: toàn bộ bảng `rt_*` / `ac_patterns` /
//!   `ts_points` được giữ trong bộ nhớ sau **một** `parking_lot::Mutex`
//!   (giống hệt `DuckS3Storage` cũ dùng 1 connection + lock). Mọi điểm đọc/ghi
//!   và transaction đều là map ops — nhanh hơn query SQL, và giữ nguyên contract
//!   trait.
//! - **WAL (transaction log, kiểu Delta)**: mọi mutation được append dưới dạng
//!   JSON-lines vào `wal.log` + fsync ngay, nên crash giữa chừng không mất dữ
//!   liệu. Khi `open()` lại, WAL được replay trên checkpoint mới nhất.
//! - **Checkpoint (Parquet data files)**: `snapshot()`/`flush` dump toàn bộ
//!   state hiện tại ra các file `tables/<table>-<gen>.parquet` (một file/bảng).
//!   Việc đưa checkpoint vào hiệu lực là **atomic** nhờ file pointer `_current`
//!   (ghi tmp → rename): crash giữa chừng chỉ để lại file tmp thừa. Sau đó WAL
//!   được truncate (bằng chính checkpoint), cơ chế giống compact data files của
//!   lakehouse.
//! - **S3 mirror (lakehouse trên object storage)**: khi cấu hình `S3Config`,
//!   `flush_timeseries()` upload delta files lên `s3://bucket/prefix/{station}/
//!   ts/blk=<id>/batch-*.parquet` kèm `ts/manifest.json`; `snapshot()` upload
//!   checkpoint state lên `s3://bucket/prefix/{station}/state/`. Một local lake
//!   mới tinh (chưa từng checkpoint, WAL trống) mà S3 đã có snapshot → **restore**
//!   toàn bộ trạng thái (state + ts files) lúc `open`. Local đã có dữ liệu giữ
//!   nguyên, không bị đè bởi S3.
//! - **Timeseries lake (time-partitioned Parquet)**: mọi điểm timeseries vượt
//!   `flush_threshold` (hoặc theo lịch định kỳ / trước khi shutdown) được gom
//!   theo block của nó — `block_id = floor(ts / block_secs)`, hoặc đọc thẳng từ
//!   series dạng `blk:<id>` — và viết thành **file Parquet riêng từng block**:
//!   `ts/blk=<block_id>/batch-<epoch_millis>.parquet`, row `(id, series, ts,
//!   value)`. Buffer đã flush được đánh dấu trong WAL (`TsFlush`) nên replay
//!   không lặp. Đây chính là nơi các hệ thống data processing
//!   (Spark/Polars/DuckDB…) đọc trực tiếp — local hay S3 đều cùng 1 cấu trúc.
//!
//! Không cấu hình S3 (`LakehouseStorage::open`) → chỉ dùng local lake: timeseries
//! vẫn được cắt block partition ngay trên local (đúng 1 cấu trúc với S3, nên
//! công cụ ngoài process này đọc local cũng được), WAL/checkpoint giữ
//! crash-safety, `flush_timeseries` ghi file local (không upload).
//!
//! `block_id` của mỗi row quyết định partition:
//! - Series có dạng `blk:<id>` (luồng block của `TimeseriesStation`) → `<id>`.
//! - Series thường → `floor(ts / block_secs)`.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{
    CategoryStorage, CategoryTx, CategoryTxOp, ChainStorage, EMPTY, EdgeDataStorage,
    NodeMetaStorage, PatternStorage, Result, ShortcutsStorage, StorageError, TimeseriesStorage,
    decode_chain, encode_chain,
};

fn internal<E: std::fmt::Display>(e: E) -> StorageError {
    StorageError::Internal(e.to_string())
}

// ==================== Config ====================

/// Tham số kết nối S3 (compat AWS S3 / MinIO / R2 / GCS S3-interop).
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    /// Prefix key, VD `"opsense/proj1"` — không kèm `/` ở hai đầu.
    pub prefix: String,
    /// Custom endpoint (VD `http://rustfs:9000`). `None` = AWS S3 mặc định.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// `None` = để `object_store` tự rút từ env AWS chuẩn
    /// (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/…) hoặc instance role.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
}

impl S3Config {
    /// Object key trên S3 cho object nằm ở `rest` (tương đối theo prefix).
    fn key(&self, rest: &str) -> String {
        format!("{}/{}", self.prefix.trim_matches('/'), rest)
    }
}

// ==================== WAL ====================

/// Một mutation lẻ, ghi dưới dạng JSON-lines vào `wal.log`.
///
/// `apply_wal_op` là nơi DUY NHẤT biến một op thành thay đổi state — cả luồng
/// live lẫn replay đều đi qua nó (live thì cộng thêm append vào file WAL), nên
/// replay không bao giờ lệch khỏi trạng thái đã ghi.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum WalOp {
    Node {
        id: u64,
        prefix: Vec<u8>,
        record: u64,
    },
    Child {
        parent: u64,
        child: u64,
    },
    ChildDel {
        parent: u64,
        child: u64,
    },
    Root {
        shard: u64,
        root: u64,
    },
    Meta {
        record: u64,
        meta: Vec<u8>,
    },
    KeyLen {
        record: u64,
        len: u64,
    },
    Shortcut {
        shard: u64,
        elem: Vec<u8>,
        node_id: u64,
    },
    Edge {
        id: u64,
        data: Vec<u8>,
    },
    NodeMeta {
        elem: u64,
        meta: Vec<u8>,
    },
    Chain {
        record: u64,
        chain: Vec<u8>,
    },
    Pattern {
        pattern: String,
        id: u64,
    },
    PatternDel {
        pattern: String,
    },
    Bloom {
        id: u64,
        bloom: Vec<u8>,
    },
    Counter {
        next: u64,
    },
    TsPoint {
        series: Vec<u8>,
        ts: u64,
        id: u64,
        value: Vec<u8>,
    },
    /// Xoá các điểm buffer có `id <= max_id` (đã được flush ra delta file).
    TsFlush {
        max_id: u64,
    },
    /// Thay toàn bộ buffer bằng đúng `points` (dùng khi clear/compact
    /// timeseries — các điểm còn lại được đánh số lại id).
    TsReplace {
        points: Vec<(u64, u64, Vec<u8>, Vec<u8>)>, // (id, ts, series, value)
    },
    TsClearAll,
    ClearSeries {
        series: Vec<u8>,
    },
    ClearTable {
        table: String,
    },
}

// ==================== Lakehouse table layout ====================

/// Một cột trong data file Parquet.
#[derive(Clone, Copy, Debug)]
enum Col {
    I(&'static str),
    B(&'static str),
    S(&'static str),
}

impl Col {
    fn name(&self) -> &'static str {
        match self {
            Col::I(n) | Col::B(n) | Col::S(n) => n,
        }
    }

    fn dtype(&self) -> arrow_schema::DataType {
        use arrow_schema::DataType;
        match self {
            Col::I(_) => DataType::Int64,
            Col::B(_) => DataType::Binary,
            Col::S(_) => DataType::Utf8,
        }
    }
}

/// Danh sách bảng được checkpoint/dump ra Parquet — schema giống hệt DuckDB cũ.
const TABLES: &[(&str, &[Col])] = &[
    (
        "rt_nodes",
        &[Col::I("id"), Col::B("prefix"), Col::I("record")],
    ),
    ("rt_children", &[Col::I("parent"), Col::I("child")]),
    ("rt_roots", &[Col::I("shard"), Col::I("root")]),
    ("rt_meta", &[Col::I("record"), Col::B("meta")]),
    ("rt_keylen", &[Col::I("record"), Col::I("len")]),
    (
        "rt_shortcuts",
        &[Col::I("shard"), Col::B("elem"), Col::I("node_id")],
    ),
    ("rt_edges", &[Col::I("id"), Col::B("data")]),
    ("rt_node_meta", &[Col::I("elem"), Col::B("meta")]),
    ("rt_chains", &[Col::I("record"), Col::B("chain")]),
    ("ac_patterns", &[Col::S("pattern"), Col::I("id")]),
    ("rt_node_blooms", &[Col::I("id"), Col::B("bloom")]),
    ("rt_counter", &[Col::I("id"), Col::I("next")]),
    (
        "ts_points",
        &[
            Col::I("id"),
            Col::B("series"),
            Col::I("ts"),
            Col::S("value"),
        ],
    ),
];

fn cols_of(table: &str) -> &'static [Col] {
    TABLES
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, cols)| *cols)
        .unwrap_or(&[])
}

/// Một cell trong một row generic (dùng chung cho mọi bảng khi dump/load Parquet).
#[derive(Clone, Debug, PartialEq)]
enum Cell {
    I(u64),
    B(Vec<u8>),
    S(String),
}

/// Ghi `rows` (dạng generic, theo đúng thứ tự cột của `cols`) thành một Parquet
/// file tại `path` (arrow-rs, compression zstd).
fn write_parquet(path: &Path, cols: &[Col], rows: &[Vec<Cell>]) -> Result<()> {
    use arrow_array::RecordBatch;
    use arrow_array::array::*;
    use arrow_array::builder::StringBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};

    let schema = Arc::new(arrow_schema::Schema::new(
        cols.iter()
            .map(|c| arrow_schema::Field::new(c.name(), c.dtype(), false))
            .collect::<Vec<_>>(),
    ));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for (ci, col) in cols.iter().enumerate() {
        match col {
            Col::I(_) => {
                arrays.push(Arc::new(Int64Array::from_iter(rows.iter().map(|r| {
                    let Cell::I(v) = r[ci] else {
                        unreachable!("cell kind mismatch");
                    };
                    Some(v as i64)
                }))));
            }
            Col::B(_) => {
                arrays.push(Arc::new(BinaryArray::from_iter(rows.iter().map(|r| {
                    let Cell::B(b) = &r[ci] else {
                        unreachable!("cell kind mismatch");
                    };
                    Some(b.as_slice())
                }))));
            }
            Col::S(_) => {
                let mut b = StringBuilder::new();
                for r in rows {
                    let Cell::S(s) = &r[ci] else {
                        unreachable!("cell kind mismatch");
                    };
                    b.append_value(s);
                }
                arrays.push(Arc::new(b.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(internal)?;
    let file = std::fs::File::create(path).map_err(internal)?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(internal)?;
    writer.write(&batch).map_err(internal)?;
    writer.close().map_err(internal)?;
    Ok(())
}

/// Đọc toàn bộ row từ một Parquet file (generic theo `cols`).
fn read_parquet(path: &Path, cols: &[Col]) -> Result<Vec<Vec<Cell>>> {
    use arrow_array::array::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path).map_err(internal)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(internal)?;
    let reader = builder.build().map_err(internal)?;

    let mut out = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(internal)?;
        for i in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(cols.len());
            for (ci, col) in cols.iter().enumerate() {
                let cell = match col {
                    Col::I(_) => {
                        let arr = batch
                            .column(ci)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| internal("column layout mismatch"))?;
                        Cell::I(arr.value(i) as u64)
                    }
                    Col::B(_) => {
                        let arr = batch
                            .column(ci)
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| internal("column layout mismatch"))?;
                        Cell::B(arr.value(i).to_vec())
                    }
                    Col::S(_) => {
                        let arr = batch
                            .column(ci)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| internal("column layout mismatch"))?;
                        Cell::S(arr.value(i).to_string())
                    }
                };
                row.push(cell);
            }
            out.push(row);
        }
    }
    Ok(out)
}

fn atomic_write(path: &Path, content: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content).map_err(internal)?;
    std::fs::rename(&tmp, path).map_err(internal)?;
    Ok(())
}

// ==================== Inner (state + WAL + checkpoints) ====================

/// Toàn bộ trạng thái lưu trữ — sống sau **một** Mutex (giống bản DuckDB cũ
/// dùng 1 connection + lock). Map ops thay cho SQL.
struct Inner {
    local: PathBuf,
    /// Generation của checkpoint hiện tại (0 = chưa có checkpoint nào).
    generation: u64,
    /// `true` nếu local chưa từng có dữ liệu (chưa checkpoint, WAL rỗng).
    fresh: bool,

    nodes: HashMap<u64, (Vec<u8>, u64)>,
    children: HashMap<(u64, u64), ()>,
    roots: HashMap<u64, u64>,
    metas: HashMap<u64, Vec<u8>>,
    keylens: HashMap<u64, u64>,
    shortcuts: HashMap<(u64, Vec<u8>), Vec<u64>>,
    edges: HashMap<u64, Vec<u8>>,
    node_metas: HashMap<u64, Vec<u8>>,
    chains: HashMap<u64, Vec<u8>>,
    patterns: HashMap<String, u64>,
    pattern_next: u64,
    blooms: HashMap<u64, Vec<u8>>,
    counter: u64,

    /// (series, ts) → (id, value) — append id tăng dần để tie-break trong merge.
    ts: HashMap<(Vec<u8>, u64), (u64, Vec<u8>)>,
    ts_next_id: u64,
    /// Các delta file timeseries đã flush (tên file, thứ tự = thứ tự flush).
    ts_manifest: Vec<String>,

    /// File WAL (append-only; truncate khi checkpoint).
    wal: std::fs::File,
}

impl Inner {
    fn tables_dir(&self) -> PathBuf {
        self.local.join("tables")
    }

    fn ts_dir(&self) -> PathBuf {
        self.local.join("ts")
    }

    fn current_path(&self) -> PathBuf {
        self.local.join("_current")
    }

    fn ts_manifest_path(&self) -> PathBuf {
        self.local.join("ts_manifest.json")
    }

    fn table_file(&self, name: &str, generation: u64) -> PathBuf {
        self.tables_dir()
            .join(format!("{name}-{generation}.parquet"))
    }

    /// Mở (hoặc tạo mới) local lake tại `local`, load checkpoint + replay WAL.
    fn open(local: &Path) -> Result<Self> {
        std::fs::create_dir_all(local).map_err(internal)?;
        std::fs::create_dir_all(local.join("tables")).map_err(internal)?;
        std::fs::create_dir_all(local.join("ts")).map_err(internal)?;

        let mut s = Inner {
            local: local.to_path_buf(),
            generation: 0,
            fresh: true,
            nodes: HashMap::new(),
            children: HashMap::new(),
            roots: HashMap::new(),
            metas: HashMap::new(),
            keylens: HashMap::new(),
            shortcuts: HashMap::new(),
            edges: HashMap::new(),
            node_metas: HashMap::new(),
            chains: HashMap::new(),
            patterns: HashMap::new(),
            pattern_next: 1,
            blooms: HashMap::new(),
            counter: 1,
            ts: HashMap::new(),
            ts_next_id: 1,
            ts_manifest: Vec::new(),
            wal: std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(local.join("wal.log"))
                .map_err(internal)?,
        };
        s.seed();

        // 1. Checkpoint mới nhất (nếu có).
        let current = s.current_path();
        if current.exists() {
            let text = std::fs::read_to_string(&current).map_err(internal)?;
            if let Ok(generation) = text.trim().parse::<u64>() {
                s.generation = generation;
                s.fresh = false;
                s.load_tables(generation)?;
            }
        }

        // 2. Replay WAL (header + toàn bộ ops — idempotent, hội tụ đúng state).
        s.wal.seek(SeekFrom::Start(0)).map_err(internal)?;
        let mut buf = String::new();
        s.wal.read_to_string(&mut buf).map_err(internal)?;
        let mut ops = 0;
        let mut first = true;
        for line in buf.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if first {
                first = false;
                // Dòng đầu có thể là header `{"seq": N}` (ghi bởi `wal_head` sau
                // checkpoint). Chỉ tiêu thụ làm header khi có key "seq". Khi mở
                // lần đầu (chưa từng checkpoint) dòng đầu là op thật → rơi xuống
                // replay bình thường, không được bỏ qua.
                if let Ok(h) = serde_json::from_str::<serde_json::Value>(line)
                    && let Some(seq) = h.get("seq").and_then(|v| v.as_u64())
                {
                    s.generation = s.generation.max(seq);
                    continue;
                }
            }
            let op: WalOp = serde_json::from_str(line).map_err(internal)?;
            apply_wal_op(&mut s, &op);
            ops += 1;
        }
        if ops > 0 {
            s.fresh = false;
        }

        // 3. Load ts_manifest (danh sách delta file đã flush).
        let mpath = s.ts_manifest_path();
        if mpath.exists() {
            let text = std::fs::read_to_string(&mpath).map_err(internal)?;
            if let Ok(list) = serde_json::from_str::<Vec<String>>(&text) {
                s.ts_manifest = list;
            }
        }

        // FSync WAL trước khi trả về để mọi dòng đã đọc là bền.
        if s.wal.metadata().map(|m| m.len() > 0).unwrap_or(false) {
            s.wal.sync_all().map_err(internal)?;
        }
        Ok(s)
    }

    /// Sentinel node 0 + counter bắt đầu từ 1 (như `init()` của DuckDB cũ).
    fn seed(&mut self) {
        self.nodes.entry(0).or_insert((Vec::new(), 0));
        if self.counter < 1 {
            self.counter = 1;
        }
    }

    fn wal_head(&mut self) -> Result<()> {
        let line = format!("{}\n", serde_json::json!({ "seq": self.generation }));
        self.wal.set_len(0).map_err(internal)?;
        self.wal.seek(SeekFrom::Start(0)).map_err(internal)?;
        self.wal.write_all(line.as_bytes()).map_err(internal)?;
        self.wal.sync_all().map_err(internal)?;
        Ok(())
    }

    /// Apply một op lên state + append vào WAL (durable).
    fn mutate(&mut self, op: &WalOp) -> Result<()> {
        apply_wal_op(self, op);
        self.fresh = false;
        let line = serde_json::to_string(op).map_err(internal)?;
        self.wal.write_all(line.as_bytes()).map_err(internal)?;
        self.wal.write_all(b"\n").map_err(internal)?;
        self.wal.sync_all().map_err(internal)?;
        Ok(())
    }

    /// Load toàn bộ checkpoint generation `generation` vào state (bảng nào có file thì đọc).
    fn load_tables(&mut self, generation: u64) -> Result<()> {
        for (name, cols) in TABLES {
            let path = self.table_file(name, generation);
            if !path.exists() {
                continue;
            }
            let rows = read_parquet(&path, cols)?;
            apply_table_rows(self, name, rows);
        }
        self.seed();
        Ok(())
    }

    /// Ghi checkpoint generation `gen = self.gen + 1` (atomic qua `_current`),
    /// truncate WAL, rồi dọn các generation cũ.
    fn checkpoint(&mut self) -> Result<()> {
        self.generation += 1;
        let generation = self.generation;
        for (name, cols) in TABLES {
            let rows = table_rows(self, name);
            let tmp = self
                .table_file(name, generation)
                .with_extension("parquet.tmp");
            write_parquet(&tmp, cols, &rows)?;
            std::fs::rename(&tmp, self.table_file(name, generation)).map_err(internal)?;
        }
        atomic_write(&self.current_path(), generation.to_string().as_bytes())?;

        // WAL đã được fold vào checkpoint → truncate, chỉ giữ header.
        self.wal_rewind()?;

        // Dọn các generation cũ (chỉ giữ current gen).
        if let Ok(read_dir) = std::fs::read_dir(self.tables_dir()) {
            for entry in read_dir.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with("rt_")
                    && !name.starts_with("ac_")
                    && !name.starts_with("ts_points")
                {
                    continue;
                }
                // Giữ `<table>-<gen>.parquet`, xoá mọi thứ còn lại của bảng.
                let keep = TABLES
                    .iter()
                    .any(|(t, _)| name == format!("{t}-{generation}.parquet"));
                if !keep {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Ok(())
    }

    fn wal_rewind(&mut self) -> Result<()> {
        self.wal_head()
    }

    /// Đọc điểm timeseries sau khi merge (buffer local thắng files cũ).
    fn merged_ts(&self) -> Result<Vec<MergedTsRow>> {
        let mut map: MergedTsMap = HashMap::new();
        for (fi, rel) in self.ts_manifest.iter().enumerate() {
            let path = self.ts_dir().join(rel);
            if !path.exists() {
                continue;
            }
            let cols = cols_of("ts_points");
            for row in read_parquet(&path, cols)? {
                let (id, series, ts, value) = ts_row(&row);
                map.entry((series, ts))
                    .and_modify(|(f, i, v)| {
                        if *f < fi as u64 || (*f == fi as u64 && *i < id) {
                            *f = fi as u64;
                            *i = id;
                            *v = value.clone();
                        }
                    })
                    .or_insert((fi as u64, id, value));
            }
        }
        // Buffer local luôn thắng (overwrite toàn bộ).
        for ((series, ts), (id, value)) in &self.ts {
            map.insert((series.clone(), *ts), (u64::MAX, *id, value.clone()));
        }
        Ok(map
            .into_iter()
            .map(|((series, ts), (_f, id, value))| (series, ts, id, value))
            .collect())
    }
}

/// Hàng timeseries đã merge — (series, ts, id, value).
type MergedTsRow = (Vec<u8>, u64, u64, Vec<u8>);

/// Map (series, ts) → (file_seq, id, value); file_seq = +∞ cho buffer local.
type MergedTsMap = HashMap<(Vec<u8>, u64), (u64, u64, Vec<u8>)>;

/// Trích row ts_points thành (id, series, ts, value).
fn ts_row(row: &[Cell]) -> (u64, Vec<u8>, u64, Vec<u8>) {
    let Cell::I(id) = &row[0] else { unreachable!() };
    let Cell::B(series) = &row[1] else {
        unreachable!()
    };
    let Cell::I(ts) = &row[2] else { unreachable!() };
    let Cell::S(value) = &row[3] else {
        unreachable!()
    };
    (*id, series.clone(), *ts, value.clone().into_bytes())
}

/// Biến một `WalOp` thành thay đổi state — dùng chung cho live ops và replay.
fn apply_wal_op(s: &mut Inner, op: &WalOp) {
    match op {
        WalOp::Node { id, prefix, record } => {
            s.nodes.insert(*id, (prefix.clone(), *record));
        }
        WalOp::Child { parent, child } => {
            s.children.entry((*parent, *child)).or_insert(());
        }
        WalOp::ChildDel { parent, child } => {
            s.children.remove(&(*parent, *child));
        }
        WalOp::Root { shard, root } => {
            s.roots.insert(*shard, *root);
        }
        WalOp::Meta { record, meta } => {
            s.metas.insert(*record, meta.clone());
        }
        WalOp::KeyLen { record, len } => {
            s.keylens.insert(*record, *len);
        }
        WalOp::Shortcut {
            shard,
            elem,
            node_id,
        } => {
            let list = s.shortcuts.entry((*shard, elem.clone())).or_default();
            if !list.contains(node_id) {
                list.push(*node_id);
            }
        }
        WalOp::Edge { id, data } => {
            s.edges.insert(*id, data.clone());
        }
        WalOp::NodeMeta { elem, meta } => {
            s.node_metas.insert(*elem, meta.clone());
        }
        WalOp::Chain { record, chain } => {
            s.chains.insert(*record, chain.clone());
        }
        WalOp::Pattern { pattern, id } => {
            if !s.patterns.contains_key(pattern) {
                s.patterns.insert(pattern.clone(), *id);
            }
            s.pattern_next = s.pattern_next.max(*id + 1);
        }
        WalOp::PatternDel { pattern } => {
            s.patterns.remove(pattern);
        }
        WalOp::Bloom { id, bloom } => {
            s.blooms.insert(*id, bloom.clone());
        }
        WalOp::Counter { next } => {
            s.counter = *next;
        }
        WalOp::TsPoint {
            series,
            ts,
            id,
            value,
        } => {
            s.ts.insert((series.clone(), *ts), (*id, value.clone()));
            s.ts_next_id = s.ts_next_id.max(*id + 1);
        }
        WalOp::TsFlush { max_id } => {
            s.ts.retain(|_, (id, _)| *id > *max_id);
        }
        WalOp::TsReplace { points } => {
            s.ts.clear();
            for (id, ts, series, value) in points {
                s.ts.insert((series.clone(), *ts), (*id, value.clone()));
                s.ts_next_id = s.ts_next_id.max(*id + 1);
            }
        }
        WalOp::TsClearAll => {
            s.ts.clear();
        }
        WalOp::ClearSeries { series } => {
            s.ts.retain(|(s_, _), _| s_ != series);
        }
        WalOp::ClearTable { table } => match table.as_str() {
            "rt_edges" => s.edges.clear(),
            "rt_chains" => s.chains.clear(),
            "rt_shortcuts" => s.shortcuts.clear(),
            "rt_node_meta" => s.node_metas.clear(),
            "ac_patterns" => {
                s.patterns.clear();
                s.pattern_next = 1;
            }
            _ => {}
        },
    }
}

/// Dump state của bảng `name` ra rows generic (theo thứ tự cột của Parquet).
fn table_rows(s: &Inner, name: &str) -> Vec<Vec<Cell>> {
    let mut rows = Vec::new();
    match name {
        "rt_nodes" => {
            for (id, (prefix, record)) in &s.nodes {
                rows.push(vec![
                    Cell::I(*id),
                    Cell::B(prefix.clone()),
                    Cell::I(*record),
                ]);
            }
        }
        "rt_children" => {
            for (parent, child) in s.children.keys() {
                rows.push(vec![Cell::I(*parent), Cell::I(*child)]);
            }
        }
        "rt_roots" => {
            for (shard, root) in &s.roots {
                rows.push(vec![Cell::I(*shard), Cell::I(*root)]);
            }
        }
        "rt_meta" => {
            for (record, meta) in &s.metas {
                rows.push(vec![Cell::I(*record), Cell::B(meta.clone())]);
            }
        }
        "rt_keylen" => {
            for (record, len) in &s.keylens {
                rows.push(vec![Cell::I(*record), Cell::I(*len)]);
            }
        }
        "rt_shortcuts" => {
            for ((shard, elem), nodes) in &s.shortcuts {
                for node_id in nodes {
                    rows.push(vec![
                        Cell::I(*shard),
                        Cell::B(elem.clone()),
                        Cell::I(*node_id),
                    ]);
                }
            }
        }
        "rt_edges" => {
            for (id, data) in &s.edges {
                rows.push(vec![Cell::I(*id), Cell::B(data.clone())]);
            }
        }
        "rt_node_meta" => {
            for (elem, meta) in &s.node_metas {
                rows.push(vec![Cell::I(*elem), Cell::B(meta.clone())]);
            }
        }
        "rt_chains" => {
            for (record, chain) in &s.chains {
                rows.push(vec![Cell::I(*record), Cell::B(chain.clone())]);
            }
        }
        "ac_patterns" => {
            for (pattern, id) in &s.patterns {
                rows.push(vec![Cell::S(pattern.clone()), Cell::I(*id)]);
            }
        }
        "rt_node_blooms" => {
            for (id, bloom) in &s.blooms {
                rows.push(vec![Cell::I(*id), Cell::B(bloom.clone())]);
            }
        }
        "rt_counter" => {
            rows.push(vec![Cell::I(1), Cell::I(s.counter)]);
        }
        "ts_points" => {
            let mut pts: Vec<(&Vec<u8>, &u64, u64, &Vec<u8>)> = Vec::new();
            for ((series, ts), (id, value)) in &s.ts {
                pts.push((series, ts, *id, value));
            }
            pts.sort_by_key(|(_, _, id, _)| *id);
            for (series, ts, id, value) in pts {
                rows.push(vec![
                    Cell::I(id),
                    Cell::B(series.clone()),
                    Cell::I(*ts),
                    Cell::S(String::from_utf8(value.clone()).unwrap()),
                ]);
            }
        }
        _ => {}
    }
    rows
}

/// Nạp rows (đọc từ Parquet) vào state của bảng `name`.
fn apply_table_rows(s: &mut Inner, name: &str, rows: Vec<Vec<Cell>>) {
    match name {
        "rt_nodes" => {
            for r in rows {
                let Cell::I(id) = r[0] else { continue };
                let Cell::B(prefix) = &r[1] else { continue };
                let Cell::I(record) = r[2] else { continue };
                s.nodes.insert(id, (prefix.clone(), record));
            }
        }
        "rt_children" => {
            for r in rows {
                let Cell::I(parent) = r[0] else { continue };
                let Cell::I(child) = r[1] else { continue };
                s.children.entry((parent, child)).or_insert(());
            }
        }
        "rt_roots" => {
            for r in rows {
                let Cell::I(shard) = r[0] else { continue };
                let Cell::I(root) = r[1] else { continue };
                s.roots.insert(shard, root);
            }
        }
        "rt_meta" => {
            for r in rows {
                let Cell::I(record) = r[0] else { continue };
                let Cell::B(meta) = &r[1] else { continue };
                s.metas.insert(record, meta.clone());
            }
        }
        "rt_keylen" => {
            for r in rows {
                let Cell::I(record) = r[0] else { continue };
                let Cell::I(len) = r[1] else { continue };
                s.keylens.insert(record, len);
            }
        }
        "rt_shortcuts" => {
            for r in rows {
                let Cell::I(shard) = r[0] else { continue };
                let Cell::B(elem) = &r[1] else { continue };
                let Cell::I(node_id) = r[2] else { continue };
                let list = s.shortcuts.entry((shard, elem.clone())).or_default();
                if !list.contains(&node_id) {
                    list.push(node_id);
                }
            }
        }
        "rt_edges" => {
            for r in rows {
                let Cell::I(id) = r[0] else { continue };
                let Cell::B(data) = &r[1] else { continue };
                s.edges.insert(id, data.clone());
            }
        }
        "rt_node_meta" => {
            for r in rows {
                let Cell::I(elem) = r[0] else { continue };
                let Cell::B(meta) = &r[1] else { continue };
                s.node_metas.insert(elem, meta.clone());
            }
        }
        "rt_chains" => {
            for r in rows {
                let Cell::I(record) = r[0] else { continue };
                let Cell::B(chain) = &r[1] else { continue };
                s.chains.insert(record, chain.clone());
            }
        }
        "ac_patterns" => {
            for r in rows {
                let Cell::S(pattern) = &r[0] else { continue };
                let Cell::I(id) = r[1] else { continue };
                if !s.patterns.contains_key(pattern) {
                    s.patterns.insert(pattern.clone(), id);
                }
                s.pattern_next = s.pattern_next.max(id + 1);
            }
        }
        "rt_node_blooms" => {
            for r in rows {
                let Cell::I(id) = r[0] else { continue };
                let Cell::B(bloom) = &r[1] else { continue };
                s.blooms.insert(id, bloom.clone());
            }
        }
        "rt_counter" => {
            for r in rows {
                if let (Cell::I(_), Cell::I(next)) = (&r[0], &r[1]) {
                    s.counter = *next;
                }
            }
        }
        "ts_points" => {
            for r in rows {
                let (id, series, ts, value) = ts_row(&r);
                s.ts.insert((series, ts), (id, value));
                s.ts_next_id = s.ts_next_id.max(id + 1);
            }
        }
        _ => {}
    }
}

// ==================== S3 helpers (object_store) ====================

use object_store::ObjectStore as _;
use object_store::path::Path as ObjPath;

/// Chạy một future blocking — an toàn cả trong lẫn ngoài async context:
/// luôn tạo một runtime riêng trên một thread mới (không bao giờ block_on
/// trong runtime hiện tại).
fn block_on<T: Send>(fut: impl std::future::Future<Output = T> + Send) -> T {
    std::thread::scope(|s| {
        s.spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(fut)
        })
        .join()
        .expect("block_on task panicked")
    })
}

fn s3_store(cfg: &S3Config) -> Result<Arc<dyn object_store::ObjectStore>> {
    let mut b = object_store::aws::AmazonS3Builder::new().with_bucket_name(&cfg.bucket);
    if let Some(key) = &cfg.access_key_id {
        b = b.with_access_key_id(key);
    }
    if let Some(secret) = &cfg.secret_access_key {
        b = b.with_secret_access_key(secret);
    }
    if let Some(r) = &cfg.region {
        b = b.with_region(r);
    }
    if let Some(ep) = &cfg.endpoint {
        // Endpoint tuỳ biến (MinIO/R2/localstack) → cho phép HTTP + path-style.
        b = b
            .with_endpoint(ep)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }
    if let Some(t) = &cfg.session_token {
        b = b.with_token(t);
    }
    Ok(Arc::new(b.build().map_err(internal)?))
}

fn s3_put(store: &Arc<dyn object_store::ObjectStore>, key: &str, bytes: Vec<u8>) -> Result<()> {
    let store = store.clone();
    let key = ObjPath::from(key.to_string());
    block_on(async move {
        store
            .put(&key, object_store::PutPayload::from(bytes))
            .await
            .map(|_| ())
    })
    .map_err(internal)
}

fn s3_get(store: &Arc<dyn object_store::ObjectStore>, key: &str) -> Result<Vec<u8>> {
    let store = store.clone();
    let key = ObjPath::from(key.to_string());
    block_on(async move {
        let obj = store.get(&key).await.map_err(internal)?;
        let bytes = obj.bytes().await.map_err(internal)?;
        Ok::<_, StorageError>(bytes.to_vec())
    })
    .map_err(internal)
}

fn s3_delete(store: &Arc<dyn object_store::ObjectStore>, key: &str) -> Result<()> {
    let store = store.clone();
    let key = ObjPath::from(key.to_string());
    block_on(async move { store.delete(&key).await }).map_err(internal)
}

// ==================== LakehouseStorage ====================

/// `block_id` của một row timeseries — quyết định partition `ts/blk=<id>/`.
/// Series của luồng block (`TimeseriesStation`) đã mang sẵn `blk:<id>` → dùng
/// thẳng; series thường lấy `floor(ts / block_secs)`.
fn block_id_of(series: &[u8], ts: u64, block_secs: i64) -> u64 {
    if let Some(rest) = series.strip_prefix(b"blk:")
        && let Ok(id) = std::str::from_utf8(rest)
            .unwrap_or_default()
            .trim()
            .parse::<u64>()
        {
            return id;
        }
    if block_secs > 0 {
        ts / block_secs as u64
    } else {
        ts
    }
}

/// Timestamp hiện tại (millis) — làm batch name cho delta file.
fn now_millis() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}

/// Lake key trên S3 cho station này — lấy từ tên thư mục local, bỏ hậu tố
/// `-<kind>` (`-timeseries`/`-pattern`/`-category`) nếu có, nên key ổn định cho
/// cả 3 loại station của cùng một `id`.
fn lake_key_of(local: &str) -> String {
    let name = Path::new(local)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| local.to_string());
    for kind in ["-timeseries", "-pattern", "-category"] {
        if let Some(stripped) = name.strip_suffix(kind) {
            return stripped.to_string();
        }
    }
    name
}

/// Parquet lakehouse storage — thay thế `DuckS3Storage`.
pub struct LakehouseStorage {
    inner: Arc<Mutex<Inner>>,
    s3: Option<S3Config>,
    /// 0 = tắt auto-flush theo ngưỡng (chỉ flush khi station lifecycle gọi);
    /// >0 = `append` tự flush khi buffer đạt ngưỡng.
    flush_threshold: usize,
    /// Độ rộng một block timeseries (giây) — quyết định partition `blk=`.
    block_secs: i64,
    /// Không gian key trên S3 cho riêng station này (VD id station).
    lake_key: String,
}

impl LakehouseStorage {
    /// Mở (hoặc tạo mới) local lake tại `local` (một thư mục, không phải file)
    /// — không cấu hình S3. `block_secs` mặc định 3600, timeseries vẫn được cắt
    /// partition `ts/blk=<id>/…` ngay trên local.
    pub async fn open(local: &str) -> Result<Self> {
        Self::open_inner(local, None, 0, 3600, None).await
    }

    /// Mở local lake + cấu hình S3 (lakehouse mirror). Nếu local còn trống
    /// (chưa từng checkpoint, WAL rỗng) và S3 đã có snapshot → restore trạng
    /// thái từ S3.
    ///
    /// - `flush_threshold` = số điểm timeseries buffer local trước khi tự flush
    ///   (0 = tắt auto-flush; khuyến nghị 4096).
    /// - `block_secs` = độ rộng block partition (giây).
    /// - `lake_key` = namespace trên S3 cho riêng station (VD id station).
    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_s3(
        local: &str,
        s3: S3Config,
        flush_threshold: usize,
        block_secs: i64,
        lake_key: String,
    ) -> Result<Self> {
        Self::open_inner(local, Some(s3), flush_threshold, block_secs, Some(lake_key)).await
    }

    async fn open_inner(
        local: &str,
        s3: Option<S3Config>,
        flush_threshold: usize,
        block_secs: i64,
        lake_key: Option<String>,
    ) -> Result<Self> {
        std::fs::create_dir_all(local).map_err(internal)?;
        let mut inner = Inner::open(Path::new(local))?;
        if let Some(cfg) = &s3
            && inner.fresh
            && let Some(_gen) = restore_from_s3(cfg, Path::new(local), lake_key.as_deref())?
        {
            // S3 có snapshot → load lại checkpoint + ts lake vừa restore.
            inner = Inner::open(Path::new(local))?;
        }
        Ok(LakehouseStorage {
            inner: Arc::new(Mutex::new(inner)),
            s3,
            flush_threshold,
            block_secs: block_secs.max(1),
            lake_key: lake_key.unwrap_or_else(|| lake_key_of(local)),
        })
    }

    /// Flush toàn bộ buffer timeseries local ra các **file Parquet tách theo
    /// block**: `ts/blk=<block_id>/batch-<millis>.parquet` (local + mirror S3).
    /// Row giữ schema `(id, series, ts, value)` với `series='blk:<block_id>'`
    /// cho luồng block; partition `blk=` để Spark/DuckDB/Polars prune theo thời
    /// gian. Windows rỗng là no-op. Không cấu hình S3 vẫn ghi file local.
    pub fn flush_timeseries(&self) -> Result<()> {
        let s3 = self.s3.clone();
        let lake_key = self.lake_key.clone();
        let block_secs = self.block_secs;

        // 1. Snapshot buffer → nhóm theo block partition (trong lock).
        let files: Vec<String> = {
            let mut inner = self.inner.lock();
            if inner.ts.is_empty() {
                return Ok(());
            }
            let mut pts: Vec<(u64, Vec<u8>, u64, Vec<u8>)> = inner
                .ts
                .iter()
                .map(|((series, ts), (id, value))| (*id, series.clone(), *ts, value.clone()))
                .collect();
            pts.sort_by_key(|(id, _, _, _)| *id);
            let max_id = pts.last().map(|(id, _, _, _)| *id).unwrap_or(0);

            let mut by_block: BTreeMap<u64, Vec<(u64, Vec<u8>, u64, Vec<u8>)>> = BTreeMap::new();
            for (id, series, ts, value) in pts {
                by_block
                    .entry(block_id_of(&series, ts, block_secs))
                    .or_default()
                    .push((id, series, ts, value));
            }
            let stamp = now_millis();
            let mut written = Vec::with_capacity(by_block.len());
            for (block_id, rows) in &by_block {
                let rel = format!("blk={block_id}/batch-{stamp}.parquet");
                let path = inner.ts_dir().join(&rel);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(internal)?;
                }
                let cell_rows = rows
                    .iter()
                    .map(|(id, series, ts, value)| {
                        vec![
                            Cell::I(*id),
                            Cell::B(series.clone()),
                            Cell::I(*ts),
                            Cell::S(String::from_utf8(value.clone()).unwrap()),
                        ]
                    })
                    .collect::<Vec<_>>();
                write_parquet(&path, cols_of("ts_points"), &cell_rows)?;
                written.push(rel);
            }

            inner.ts_manifest.extend(written.iter().cloned());
            inner.ts.retain(|_, (id, _)| *id > max_id);

            // Manifest (danh sách relative key) durable local — atomic.
            let manifest_json = serde_json::to_string(&inner.ts_manifest).map_err(internal)?;
            atomic_write(&inner.ts_manifest_path(), manifest_json.as_bytes())?;

            // WAL: đánh dấu buffer đã flush (replay sẽ bỏ các điểm <= max_id).
            inner.mutate(&WalOp::TsFlush { max_id })?;
            inner.generation += 1;
            written
        };

        // 2. Upload delta files + manifest lên S3 (bên ngoài lock).
        if let Some(s3) = s3 {
            let store = s3_store(&s3)?;
            for rel in &files {
                let bytes =
                    std::fs::read(self.inner.lock().ts_dir().join(rel)).map_err(internal)?;
                s3_put(&store, &s3.key(&format!("{lake_key}/ts/{rel}")), bytes)?;
            }
            let manifest = serde_json::to_vec(&self.inner.lock().ts_manifest).map_err(internal)?;
            s3_put(
                &store,
                &s3.key(&format!("{lake_key}/ts/manifest.json")),
                manifest,
            )?;
        }
        Ok(())
    }

    /// Snapshot toàn bộ bảng ra Parquet checkpoint (local) + mirror state lên S3
    /// (`{lake_key}/state/`). Flush lake trước để checkpoint không còn mang
    /// buffer timeseries và S3 tiến tới trạng thái mới nhất. Gọi định kỳ hoặc
    /// trước khi tắt để process khác restore được trạng thái; local checkpoint
    /// cũng compact WAL.
    pub fn snapshot(&self) -> Result<()> {
        self.flush_timeseries()?;
        let mut inner = self.inner.lock();
        inner.checkpoint()?;
        let generation = inner.generation;
        drop(inner);

        if let Some(s3) = &self.s3 {
            let store = s3_store(s3)?;
            let lake_key = self.lake_key.clone();
            for (name, _) in TABLES {
                let bytes = std::fs::read(self.inner.lock().table_file(name, generation))
                    .map_err(internal)?;
                s3_put(
                    &store,
                    &s3.key(&format!("{lake_key}/state/{name}-{generation}.parquet")),
                    bytes,
                )?;
            }
            let manifest = serde_json::json!({
                "generation": generation,
                "tables": TABLES.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            });
            s3_put(
                &store,
                &s3.key(&format!("{lake_key}/state/manifest.json")),
                serde_json::to_vec(&manifest).map_err(internal)?,
            )?;
        }
        Ok(())
    }

    /// Retention: xoá toàn bộ partition `blk=<id>` chứa dữ liệu `ts < keep_after_ts`
    /// (local + S3). Dữ liệu vẫn nằm trong buffer (chưa flush) không bị động tới.
    pub fn retain_block_partitions(&self, keep_after_ts: u64) -> Result<()> {
        let block_secs = self.block_secs;
        let keep_block = if block_secs > 0 {
            keep_after_ts / block_secs as u64
        } else {
            keep_after_ts
        };
        let removed: Vec<String> = {
            let mut inner = self.inner.lock();
            // Giữ những entry có block_id >= keep_block; hủy file local của cái cũ.
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for rel in &inner.ts_manifest {
                let id = rel.split_once('/').map(|(p, _)| p).unwrap_or(rel);
                let block = id
                    .strip_prefix("blk=")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                if block < keep_block {
                    let path = inner.ts_dir().join(rel);
                    let _ = std::fs::remove_file(&path);
                    // Dọn cả partition dir (blk=<id>) nếu giờ rỗng.
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                    removed.push(rel.clone());
                } else {
                    kept.push(rel.clone());
                }
            }
            if removed.is_empty() {
                return Ok(());
            }
            inner.ts_manifest = kept;
            let manifest_json = serde_json::to_string(&inner.ts_manifest).map_err(internal)?;
            atomic_write(&inner.ts_manifest_path(), manifest_json.as_bytes())?;
            removed
        };

        if let Some(s3) = &self.s3 {
            let store = s3_store(s3)?;
            let lake_key = self.lake_key.clone();
            for rel in &removed {
                let _ = s3_delete(&store, &s3.key(&format!("{lake_key}/ts/{rel}")));
            }
            let manifest = serde_json::to_vec(&self.inner.lock().ts_manifest).map_err(internal)?;
            let _ = s3_put(
                &store,
                &s3.key(&format!("{lake_key}/ts/manifest.json")),
                manifest,
            );
        }
        Ok(())
    }
}

/// Restore checkpoint từ S3 vào local (đk: local trống). Trả về `Some(gen)`
/// nếu S3 có snapshot, `None` nếu chưa có gì. Restore cả state (`{lake_key}/
/// state/`) lẫn ts lake (`{lake_key}/ts/`) để cold-read ngay trên node mới.
fn restore_from_s3(cfg: &S3Config, local: &Path, lake_key: Option<&str>) -> Result<Option<u64>> {
    let lake_key = lake_key.unwrap_or("station");
    let store = s3_store(cfg)?;
    let manifest_key = cfg.key(&format!("{lake_key}/state/manifest.json"));
    let raw = match s3_get(&store, &manifest_key) {
        Ok(b) => b,
        Err(_) => return Ok(None), // chưa từng snapshot trên S3.
    };
    let manifest: serde_json::Value = serde_json::from_slice(&raw).map_err(internal)?;
    let Some(generation) = manifest
        .get("generation")
        .or_else(|| manifest.get("gen"))
        .and_then(|v| v.as_u64())
    else {
        return Ok(None);
    };
    let tables: Vec<String> = manifest
        .get("tables")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    std::fs::create_dir_all(local).map_err(internal)?;
    std::fs::create_dir_all(local.join("tables")).map_err(internal)?;
    for name in &tables {
        let key = cfg.key(&format!("{lake_key}/state/{name}-{generation}.parquet"));
        let bytes = s3_get(&store, &key)?;
        let tmp = local
            .join("tables")
            .join(format!("{name}-{generation}.parquet.tmp"));
        std::fs::write(&tmp, &bytes).map_err(internal)?;
        std::fs::rename(
            &tmp,
            local
                .join("tables")
                .join(format!("{name}-{generation}.parquet")),
        )
        .map_err(internal)?;
    }
    let current = local.join("_current");
    atomic_write(&current, generation.to_string().as_bytes())?;

    // Restore cả ts lake (delta files + manifest) — cold-read không mất block.
    let ts_key = cfg.key(&format!("{lake_key}/ts/manifest.json"));
    if let Ok(raw_ts) = s3_get(&store, &ts_key)
        && let Ok(list) = serde_json::from_slice::<Vec<String>>(&raw_ts) {
            std::fs::create_dir_all(local.join("ts")).map_err(internal)?;
            for rel in &list {
                let key = cfg.key(&format!("{lake_key}/ts/{rel}"));
                let Ok(bytes) = s3_get(&store, &key) else {
                    continue; // file đã bị dọn giữa chừng (retention) — bỏ qua.
                };
                let dst = local.join("ts").join(rel);
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent).map_err(internal)?;
                }
                std::fs::write(&dst, &bytes).map_err(internal)?;
            }
            atomic_write(&local.join("ts_manifest.json"), &raw_ts)?;
        }

    Ok(Some(generation))
}

// ==================== CategoryStorage ====================

#[async_trait]
impl CategoryStorage for LakehouseStorage {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let mut inner = self.inner.lock();
        let id = inner.counter;
        inner.mutate(&WalOp::Node {
            id,
            prefix,
            record: record as u64,
        })?;
        inner.mutate(&WalOp::Counter { next: id + 1 })?;
        Ok(id as usize)
    }

    async fn update_node(
        &mut self,
        id: usize,
        prefix: Option<Vec<u8>>,
        record: Option<usize>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        // Cần merge với giá trị hiện tại — node chưa tồn tại → BranchOutOfRange.
        let mut node = inner
            .nodes
            .get(&(id as u64))
            .cloned()
            .ok_or(StorageError::BranchOutOfRange(id))?;
        if let Some(p) = prefix {
            node.0 = p;
        }
        if let Some(r) = record {
            node.1 = r as u64;
        }
        inner.mutate(&WalOp::Node {
            id: id as u64,
            prefix: node.0,
            record: node.1,
        })?;
        Ok(())
    }

    async fn get_node(&self, id: usize) -> Result<(Vec<u8>, usize)> {
        let inner = self.inner.lock();
        inner
            .nodes
            .get(&(id as u64))
            .map(|(p, r)| (p.clone(), *r as usize))
            .ok_or(StorageError::BranchOutOfRange(id))
    }

    async fn get_children(&self, id: usize) -> Result<Vec<usize>> {
        let inner = self.inner.lock();
        let mut out: Vec<usize> = inner
            .children
            .keys()
            .filter(|(p, _)| *p == id as u64)
            .map(|(_, c)| *c as usize)
            .collect();
        out.sort_unstable();
        Ok(out)
    }

    async fn set_root(&mut self, shard: usize, root: usize) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::Root {
            shard: shard as u64,
            root: root as u64,
        })?;
        Ok(())
    }

    async fn get_root(&self, shard: usize) -> Result<usize> {
        let inner = self.inner.lock();
        Ok(inner
            .roots
            .get(&(shard as u64))
            .map(|r| *r as usize)
            .unwrap_or(EMPTY))
    }

    fn new_tx(&self) -> Box<dyn CategoryTx> {
        Box::new(LakeTx {
            inner: self.inner.clone(),
            nodes: Vec::new(),
            ops: Vec::new(),
        })
    }
}

// ==================== EdgeDataStorage ====================

#[async_trait]
impl EdgeDataStorage for LakehouseStorage {
    async fn set_edge_data(&mut self, edge: usize, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::Edge {
            id: edge as u64,
            data: data.to_vec(),
        })?;
        Ok(())
    }

    async fn get_edge_data(&self, edge: usize) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock();
        Ok(inner.edges.get(&(edge as u64)).cloned())
    }

    async fn clear_edges(&mut self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::ClearTable {
            table: "rt_edges".to_string(),
        })?;
        Ok(())
    }
}

// ==================== ChainStorage ====================

#[async_trait]
impl ChainStorage for LakehouseStorage {
    async fn set_chain(&mut self, record: usize, chain: &[u64]) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::Chain {
            record: record as u64,
            chain: encode_chain(chain),
        })?;
        Ok(())
    }

    async fn get_chain(&self, record: usize) -> Result<Option<Vec<u64>>> {
        let inner = self.inner.lock();
        Ok(inner.chains.get(&(record as u64)).map(|b| decode_chain(b)))
    }

    async fn clear_chains(&mut self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::ClearTable {
            table: "rt_chains".to_string(),
        })?;
        Ok(())
    }
}

// ==================== ShortcutsStorage ====================

#[async_trait]
impl ShortcutsStorage for LakehouseStorage {
    async fn add_shortcut_node(&mut self, shard: usize, elem: &[u8], node_id: usize) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::Shortcut {
            shard: shard as u64,
            elem: elem.to_vec(),
            node_id: node_id as u64,
        })?;
        Ok(())
    }

    async fn get_shortcut_nodes(&self, shard: usize, elem: &[u8]) -> Result<Vec<usize>> {
        let inner = self.inner.lock();
        let mut out: Vec<usize> = inner
            .shortcuts
            .get(&(shard as u64, elem.to_vec()))
            .map(|v| v.iter().map(|n| *n as usize).collect())
            .unwrap_or_default();
        out.sort_unstable();
        Ok(out)
    }

    async fn clear_shortcuts(&mut self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::ClearTable {
            table: "rt_shortcuts".to_string(),
        })?;
        Ok(())
    }
}

// ==================== NodeMetaStorage ====================

#[async_trait]
impl NodeMetaStorage for LakehouseStorage {
    async fn set_node_meta(&mut self, elem: usize, meta: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::NodeMeta {
            elem: elem as u64,
            meta: meta.to_vec(),
        })?;
        Ok(())
    }

    async fn get_node_meta(&self, elem: usize) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock();
        Ok(inner.node_metas.get(&(elem as u64)).cloned())
    }

    async fn clear_node_meta(&mut self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::ClearTable {
            table: "rt_node_meta".to_string(),
        })?;
        Ok(())
    }

    async fn set_meta(&mut self, record: usize, meta: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::Meta {
            record: record as u64,
            meta: meta.to_vec(),
        })?;
        Ok(())
    }

    async fn get_meta(&self, record: usize) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock();
        Ok(inner.metas.get(&(record as u64)).cloned())
    }

    async fn set_key_len(&mut self, record: usize, len: usize) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::KeyLen {
            record: record as u64,
            len: len as u64,
        })?;
        Ok(())
    }

    async fn get_key_len(&self, record: usize) -> Result<Option<usize>> {
        let inner = self.inner.lock();
        Ok(inner.keylens.get(&(record as u64)).map(|l| *l as usize))
    }
}

// ==================== BloomStorage (feature bloom-search) ====================

#[cfg(feature = "bloom-search")]
#[async_trait]
impl super::BloomStorage for LakehouseStorage {
    async fn set_node_bloom(&mut self, id: usize, bloom: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::Bloom {
            id: id as u64,
            bloom: bloom.to_vec(),
        })?;
        Ok(())
    }

    async fn get_node_bloom(&self, id: usize) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock();
        Ok(inner.blooms.get(&(id as u64)).cloned())
    }
}

// ==================== TimeseriesStorage ====================

#[async_trait]
impl TimeseriesStorage for LakehouseStorage {
    async fn append(&self, series: &[u8], timestamp: u64, value: &[u8]) -> Result<()> {
        let should_flush = {
            let mut inner = self.inner.lock();
            let id = inner.ts_next_id;
            inner.mutate(&WalOp::TsPoint {
                series: series.to_vec(),
                ts: timestamp,
                id,
                value: value.to_vec(),
            })?;
            inner.ts_next_id = id + 1;
            self.flush_threshold > 0 && inner.ts.len() >= self.flush_threshold
        };
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
        let inner = self.inner.lock();
        let mut out: Vec<(u64, Vec<u8>)> = inner
            .merged_ts()?
            .into_iter()
            .filter(|(s, ts, _, _)| s == series && *ts >= start_ts && *ts <= end_ts)
            .map(|(_, ts, _, value)| (ts, value))
            .collect();
        out.sort_by_key(|(ts, _)| *ts);
        Ok(out)
    }

    async fn latest(&self, series: &[u8], limit: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let inner = self.inner.lock();
        let mut pts: Vec<(u64, Vec<u8>)> = inner
            .merged_ts()?
            .into_iter()
            .filter(|(s, _, _, _)| s == series)
            .map(|(_, ts, _, value)| (ts, value))
            .collect();
        pts.sort_by_key(|a| std::cmp::Reverse(a.0));
        pts.truncate(limit);
        pts.reverse(); // trả về tăng dần — semantic giống DuckDB cũ.
        Ok(pts)
    }

    async fn first(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let inner = self.inner.lock();
        let mut pts: Vec<(u64, Vec<u8>)> = inner
            .merged_ts()?
            .into_iter()
            .filter(|(s, _, _, _)| s == series)
            .map(|(_, ts, _, value)| (ts, value))
            .collect();
        pts.sort_by_key(|(ts, _)| *ts);
        Ok(pts.into_iter().next())
    }

    async fn last(&self, series: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let inner = self.inner.lock();
        let mut pts: Vec<(u64, Vec<u8>)> = inner
            .merged_ts()?
            .into_iter()
            .filter(|(s, _, _, _)| s == series)
            .map(|(_, ts, _, value)| (ts, value))
            .collect();
        pts.sort_by_key(|a| std::cmp::Reverse(a.0));
        Ok(pts.into_iter().next())
    }

    async fn clear_series(&self, series: &[u8]) -> Result<()> {
        let old_files = {
            let mut inner = self.inner.lock();

            // Merge (file + buffer) rồi giữ lại mọi điểm KHÔNG thuộc series.
            let merged = inner.merged_ts()?;
            let mut kept: Vec<(u64, u64, Vec<u8>, Vec<u8>)> = Vec::new(); // (id, ts, series, value)
            for (s, ts, _, value) in merged {
                if s != series {
                    let id = inner.ts_next_id;
                    inner.ts_next_id += 1;
                    kept.push((id, ts, s, value));
                }
            }

            let old = std::mem::take(&mut inner.ts_manifest);
            // Xoá local lake files cũ.
            for rel in &old {
                let _ = std::fs::remove_file(inner.ts_dir().join(rel));
            }
            let manifest_json = serde_json::to_string(&Vec::<String>::new()).map_err(internal)?;
            atomic_write(&inner.ts_manifest_path(), manifest_json.as_bytes())?;

            // Buffer mới = kept (durable qua WAL).
            inner.mutate(&WalOp::TsReplace {
                points: kept.clone(),
            })?;
            old
        };

        // Dọn file trên S3 (nếu có).
        if let Some(s3) = &self.s3 {
            let store = s3_store(s3)?;
            let lake_key = self.lake_key.clone();
            for rel in &old_files {
                let _ = s3_delete(&store, &s3.key(&format!("{lake_key}/ts/{rel}")));
            }
        }
        // Flush lại remainders thành delta file (local lake + S3).
        self.flush_timeseries()?;
        Ok(())
    }

    async fn clear_all_series(&self) -> Result<()> {
        let old_files = {
            let mut inner = self.inner.lock();
            let old = std::mem::take(&mut inner.ts_manifest);
            for rel in &old {
                let _ = std::fs::remove_file(inner.ts_dir().join(rel));
            }
            let manifest_json = serde_json::to_string(&Vec::<String>::new()).map_err(internal)?;
            atomic_write(&inner.ts_manifest_path(), manifest_json.as_bytes())?;
            inner.ts.clear();
            inner.ts_next_id = 1;
            inner.mutate(&WalOp::TsClearAll)?;
            old
        };
        if let Some(s3) = &self.s3 {
            let store = s3_store(s3)?;
            let lake_key = self.lake_key.clone();
            for rel in &old_files {
                let _ = s3_delete(&store, &s3.key(&format!("{lake_key}/ts/{rel}")));
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.flush_timeseries()
    }

    async fn checkpoint(&self) -> Result<()> {
        self.snapshot()
    }

    async fn retain_older_than(&self, keep_after_ts: u64) -> Result<()> {
        self.retain_block_partitions(keep_after_ts)
    }
}

// ==================== PatternStorage ====================

#[async_trait]
impl PatternStorage for LakehouseStorage {
    async fn add(&self, pattern: &str) -> Result<()> {
        if pattern.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        if !inner.patterns.contains_key(pattern) {
            let next_id = inner.pattern_next;
            inner.mutate(&WalOp::Pattern {
                pattern: pattern.to_string(),
                id: next_id,
            })?;
        }
        Ok(())
    }

    async fn contains(&self, pattern: &str) -> Result<bool> {
        let inner = self.inner.lock();
        Ok(inner.patterns.contains_key(pattern))
    }

    async fn get_all(&self) -> Result<Vec<String>> {
        let inner = self.inner.lock();
        let mut v: Vec<(u64, String)> = inner
            .patterns
            .iter()
            .map(|(k, id)| (*id, k.clone()))
            .collect();
        v.sort_by_key(|(id, _)| *id);
        Ok(v.into_iter().map(|(_, k)| k).collect())
    }

    async fn count(&self) -> Result<usize> {
        let inner = self.inner.lock();
        Ok(inner.patterns.len())
    }

    async fn remove(&self, pattern: &str) -> Result<bool> {
        let mut inner = self.inner.lock();
        if !inner.patterns.contains_key(pattern) {
            return Ok(false);
        }
        inner.mutate(&WalOp::PatternDel {
            pattern: pattern.to_string(),
        })?;
        Ok(true)
    }

    async fn clear(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.mutate(&WalOp::ClearTable {
            table: "ac_patterns".to_string(),
        })?;
        Ok(())
    }
}

// ==================== LakeTx ====================

/// Transaction cho `LakehouseStorage`: buffer toàn bộ mutation, áp dụng atomic
/// (một lock) tại `commit` — mọi op được ghi vào WAL nên tái mở không mất.
///
/// `new_node` đọc counter hiện tại mỗi lần gọi, `id = counter + nodes.len()` —
/// giống `RedisTx`; `commit` bump counter lên `max(reserved)+1` nên id không bao
/// giờ trùng.
pub struct LakeTx {
    inner: Arc<Mutex<Inner>>,
    nodes: Vec<(u64, Vec<u8>, u64)>,
    ops: Vec<CategoryTxOp>,
}

#[async_trait]
impl CategoryTx for LakeTx {
    async fn new_node(&mut self, prefix: Vec<u8>, record: usize) -> Result<usize> {
        let inner = self.inner.lock();
        let id = inner.counter + 1 + self.nodes.len() as u64 - 1;
        self.nodes.push((id, prefix, record as u64));
        Ok(id as usize)
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
        let LakeTx { inner, nodes, ops } = *self;
        let mut inner = inner.lock();

        // 1. Materialize node mới trước — để ops add/move trỏ tới hợp lệ.
        for (id, prefix, record) in &nodes {
            inner.mutate(&WalOp::Node {
                id: *id,
                prefix: prefix.clone(),
                record: *record,
            })?;
        }

        // 2. Bump counter lên max(reserved) + 1 — id tx cấp vẫn unique.
        if let Some(max_id) = nodes.iter().map(|(id, _, _)| *id).max() {
            inner.mutate(&WalOp::Counter { next: max_id + 1 })?;
        }

        // 3. Áp dụng toàn bộ ops — atomic dưới một lock, không lộ trạng thái
        //    trung gian (giống DuckDB transaction thật).
        for op in &ops {
            match op {
                CategoryTxOp::AddChild { parent, child } => {
                    inner.mutate(&WalOp::Child {
                        parent: *parent as u64,
                        child: *child as u64,
                    })?;
                }
                CategoryTxOp::MoveChild { from, to, child } => {
                    inner.mutate(&WalOp::ChildDel {
                        parent: *from as u64,
                        child: *child as u64,
                    })?;
                    inner.mutate(&WalOp::Child {
                        parent: *to as u64,
                        child: *child as u64,
                    })?;
                }
                CategoryTxOp::UpdateNode { id, prefix, record } => {
                    let mut node = inner
                        .nodes
                        .get(&(*id as u64))
                        .cloned()
                        .ok_or(StorageError::BranchOutOfRange(*id))?;
                    if let Some(p) = prefix {
                        node.0 = p.clone();
                    }
                    if let Some(r) = record {
                        node.1 = *r as u64;
                    }
                    inner.mutate(&WalOp::Node {
                        id: *id as u64,
                        prefix: node.0,
                        record: node.1,
                    })?;
                }
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
        let path = dir.path().join("lake");
        let path = path.to_string_lossy().into_owned();
        (dir, path)
    }

    #[tokio::test]
    async fn test_new_node_and_get_node() {
        let (_d, path) = tmp_path();
        let mut s = LakehouseStorage::open(&path).await.unwrap();
        let id = s.new_node(b"hello".to_vec(), 42).await.unwrap();
        assert_ne!(id, EMPTY);
        let (prefix, record) = s.get_node(id).await.unwrap();
        assert_eq!(prefix, b"hello");
        assert_eq!(record, 42);
    }

    #[tokio::test]
    async fn test_update_node_missing() {
        let (_d, path) = tmp_path();
        let mut s = LakehouseStorage::open(&path).await.unwrap();
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
        let mut s = LakehouseStorage::open(&path).await.unwrap();
        let parent = s.new_node(b"p".to_vec(), 0).await.unwrap();
        let c1 = s.new_node(b"c1".to_vec(), 1).await.unwrap();
        let c2 = s.new_node(b"c2".to_vec(), 2).await.unwrap();
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
        let s = LakehouseStorage::open(&path).await.unwrap();
        let mut tx = s.new_tx();
        let id = tx.new_node(b"pending".to_vec(), 9).await.unwrap();
        assert!(s.get_node(id).await.is_err());
        tx.commit().await.unwrap();
        assert_eq!(s.get_node(id).await.unwrap().1, 9);
    }

    #[tokio::test]
    async fn test_tx_move_child_migrates() {
        let (_d, path) = tmp_path();
        let mut s = LakehouseStorage::open(&path).await.unwrap();
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
    async fn test_persists_across_reopen() {
        let (_d, path) = tmp_path();
        let parent;
        {
            let mut s = LakehouseStorage::open(&path).await.unwrap();
            parent = s.new_node(b"hello".to_vec(), 42).await.unwrap();
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
            s.snapshot().unwrap(); // checkpoint → WAL truncate
        }
        let mut s = LakehouseStorage::open(&path).await.unwrap();
        assert_eq!(s.get_node(parent).await.unwrap(), (b"hello".to_vec(), 42));
        assert_eq!(s.get_root(3).await.unwrap(), parent);
        assert_eq!(
            s.get_meta(42).await.unwrap().as_deref(),
            Some(&b"meta-42"[..])
        );
        assert_eq!(s.get_key_len(42).await.unwrap(), Some(5));
        assert_eq!(
            s.get_node_meta(100).await.unwrap().as_deref(),
            Some(&b"node-json"[..])
        );
        assert_eq!(s.get_chain(42).await.unwrap(), Some(vec![100, 101]));
        assert!(
            s.get_shortcut_nodes(1, b"h")
                .await
                .unwrap()
                .contains(&parent)
        );
        assert_eq!(s.get_children(parent).await.unwrap().len(), 1);
        let n = s.new_node(b"new".to_vec(), 1).await.unwrap();
        assert!(n > parent);
    }

    #[tokio::test]
    async fn test_persists_without_checkpoint_via_wal() {
        let (_d, path) = tmp_path();
        let parent;
        {
            let mut s = LakehouseStorage::open(&path).await.unwrap();
            parent = s.new_node(b"hello".to_vec(), 42).await.unwrap();
            s.set_root(3, parent).await.unwrap();
        }
        // Không gọi snapshot() — mọi thứ phải restore từ WAL.
        let s = LakehouseStorage::open(&path).await.unwrap();
        assert_eq!(s.get_node(parent).await.unwrap(), (b"hello".to_vec(), 42));
        assert_eq!(s.get_root(3).await.unwrap(), parent);
    }

    #[tokio::test]
    async fn test_timeseries_roundtrip() {
        use crate::storage::TimeseriesStorage;

        let (_d, path) = tmp_path();
        let s = LakehouseStorage::open(&path).await.unwrap();
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
    async fn test_timeseries_persists_wal_and_checkpoint() {
        use crate::storage::TimeseriesStorage;

        let (_d, path) = tmp_path();
        {
            let s = LakehouseStorage::open(&path).await.unwrap();
            s.append(b"cpu", 100, b"a").await.unwrap();
            s.append(b"cpu", 200, b"b").await.unwrap();
        }
        // Reopen trước checkpoint → WAL.
        let s = LakehouseStorage::open(&path).await.unwrap();
        assert_eq!(s.last(b"cpu").await.unwrap(), Some((200, b"b".to_vec())));
        drop(s);

        // Checkpoint rồi reopen → từ Parquet.
        let s = LakehouseStorage::open(&path).await.unwrap();
        s.snapshot().unwrap();
        drop(s);
        let s = LakehouseStorage::open(&path).await.unwrap();
        assert_eq!(s.first(b"cpu").await.unwrap(), Some((100, b"a".to_vec())));
        assert_eq!(s.last(b"cpu").await.unwrap(), Some((200, b"b".to_vec())));
    }

    #[tokio::test]
    async fn test_timeseries_lake_partitioned_by_block() {
        use crate::storage::TimeseriesStorage;

        let (_d, path) = tmp_path();
        // block_secs=100 → blk=1..10; series blk:N dùng thẳng id trong series.
        let mut s = LakehouseStorage::open(&path).await.unwrap();
        s.block_secs = 100;
        s.append(b"blk:3", 1, b"v3").await.unwrap();
        s.append(b"blk:7", 1, b"v7").await.unwrap();
        s.append(b"cpu", 250, b"v250").await.unwrap(); // 250/100 → blk=2
        s.flush_timeseries().unwrap();

        let ts_root = Path::new(&path).join("ts");
        // Hai block riêng biệt → hai partition khác nhau.
        assert!(ts_root.join("blk=3").read_dir().unwrap().count() >= 1);
        assert!(ts_root.join("blk=7").read_dir().unwrap().count() >= 1);
        assert!(ts_root.join("blk=2").read_dir().unwrap().count() >= 1);

        // Partition file là Parquet thật (đọc lại được đúng rows).
        let cols = cols_of("ts_points");
        let file = ts_root
            .join("blk=3")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let rows = read_parquet(&file, cols).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][1], &Cell::B(b"blk:3".to_vec()));

        // Buffer đã được dọn → dữ liệu chỉ ở lake files.
        assert!(s.inner.lock().ts.is_empty());
        assert_eq!(s.range(b"blk:3", 0, 100).await.unwrap(), vec![(1, b"v3".to_vec())]);

        // Reopen: restore từ lake files (merged_ts).
        drop(s);
        let mut s = LakehouseStorage::open(&path).await.unwrap();
        s.block_secs = 100;
        s.append(b"blk:3", 2, b"v3b").await.unwrap(); // cùng partition, điểm mới hơn
        s.append(b"cpu", 105, b"v105").await.unwrap();
        assert_eq!(
            s.latest(b"blk:3", 10).await.unwrap(),
            vec![(1, b"v3".to_vec()), (2, b"v3b".to_vec())]
        );
        assert_eq!(s.latest(b"cpu", 10).await.unwrap(), vec![(105, b"v105".to_vec()), (250, b"v250".to_vec())]);
    }

    #[tokio::test]
    async fn test_timeseries_lake_retention() {
        use crate::storage::TimeseriesStorage;

        let (_d, path) = tmp_path();
        let mut s = LakehouseStorage::open(&path).await.unwrap();
        s.block_secs = 100;
        // blk:1, blk:2, blk:5 (block_secs=100 giây; keep_after=250 → giữ blk>=2).
        s.append(b"blk:1", 1, b"a").await.unwrap();
        s.append(b"blk:2", 1, b"b").await.unwrap();
        s.append(b"blk:5", 1, b"c").await.unwrap();
        s.flush_timeseries().unwrap();
        s.retain_block_partitions(250).unwrap();

        let ts_root = Path::new(&path).join("ts");
        assert!(!ts_root.join("blk=1").exists());
        assert!(ts_root.join("blk=2").exists());
        let kept: Vec<String> = s.inner.lock().ts_manifest.clone();
        assert!(kept.iter().all(|rel| rel.starts_with("blk=2/") || rel.starts_with("blk=5/")));
    }

    #[tokio::test]
    async fn test_pattern_roundtrip() {
        use crate::storage::PatternStorage;

        let (_d, path) = tmp_path();
        let s = LakehouseStorage::open(&path).await.unwrap();
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

        s.add("").await.unwrap();
        assert_eq!(s.count().await.unwrap(), 0);

        s.add("he").await.unwrap();
        s.add("she").await.unwrap();
        drop(s);
        let s = LakehouseStorage::open(&path).await.unwrap();
        assert_eq!(s.get_all().await.unwrap(), vec!["he", "she"]);
    }

    #[tokio::test]
    async fn test_parquet_roundtrip_generic() {
        // Kiểm tra writer/reader Parquet tự thân (không dependency vào state).
        let dir = tempfile::tempdir().unwrap();
        let cols = cols_of("rt_nodes");
        let rows = vec![
            vec![Cell::I(0), Cell::B(Vec::new()), Cell::I(0)],
            vec![Cell::I(1), Cell::B(b"abc".to_vec()), Cell::I(2)],
        ];
        let path = dir.path().join("t.parquet");
        write_parquet(&path, cols, &rows).unwrap();
        let back = read_parquet(&path, cols).unwrap();
        assert_eq!(back, rows);

        // Empty table.
        let path2 = dir.path().join("e.parquet");
        write_parquet(&path2, cols, &Vec::new()).unwrap();
        let back2 = read_parquet(&path2, cols).unwrap();
        assert!(back2.is_empty());
    }
}
