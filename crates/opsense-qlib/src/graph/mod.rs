//! # Graph — ONNX DAG Genome as Strategy
//!
//! The `Graph` struct is the **sole Strategy** in opsense-qlib.
//! Each `Op` trait object defines one ONNX computation node.
//! The DAG (sequence of `Node`s referencing `ops`) is compiled to ONNX bytes
//! via `build_onnx()`, then to a tract `TypedRunnableModel` via `model()`.

pub mod ctx;
pub mod macros;
pub mod ops;

pub use ctx::OnnxEmitCtx;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Error;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use tract_onnx::pb::GraphProto;
use tract_onnx::prelude::*;
use tract_onnx::tract_core::model::typed::TypedRunnableModel;

use crate::grid::TradingGrid;
use crate::{Extractor, FetchFn, ParamFn, Strategy};

// ── Params layout ──────────────────────────────────────────────────
//
// `params` là vector phẳng mà `Portfolio` truyền cho `Strategy::rebuild` (và
// `SGD` tối ưu trên đó). Index 0..5 là **cùng layout** với `GridStrategy::init`
// để `Portfolio` dùng chung, từ 6 trở đi là trọng số + bias của DAG.

/// `kelly_fraction` cho `Portfolio::evaluate`.
pub const P_KELLY: usize = 0;
/// `base_capital` cho `Portfolio::evaluate`.
pub const P_CAPITAL: usize = 1;
/// Số bậc lưới khi dựng plan.
pub const P_GRID_LEVELS: usize = 2;
/// Stop-loss mỗi lệnh (fraction).
pub const P_SL_PCT: usize = 3;
/// Cửa sổ nến (giây) cho lần rebuild.
pub const P_LOOKBACK: usize = 4;
/// Bắt đầu trọng số mô hình (`n_feat × num_of_grids`).
pub const P_WEIGHTS: usize = 6;

const DEFAULT_KELLY_FRACTION: f64 = 0.25;
const DEFAULT_BASE_CAPITAL: f64 = 100_000.0;
const DEFAULT_GRID_LEVELS: usize = 5;
const DEFAULT_SL_PCT: f64 = 0.05;

// ── Op trait ---------------------------------------------------------

/// Trait đại diện cho một phép toán trong ONNX DAG.
/// Mỗi `Op` biết cách emit các ONNX nodes và initializers của chính nó.
#[typetag::serde(tag = "type")]
pub trait Op: Debug + Send + Sync {
    /// Emit ONNX nodes/initializers cho op này.
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error>;

    /// Trả về số feature đầu ra nếu đây là node Head.
    fn head_features(&self) -> Option<usize> {
        None
    }

    /// Trả về `true` nếu đây là ATR node (cho optional second output).
    fn is_atr(&self) -> bool {
        false
    }

    /// Số output của node Head (mặc định 0).
    fn head_n_out(&self) -> usize {
        0
    }
}

// ── In / Node --------------------------------------------------------

/// Input của một node: `FromExtractor(k)` = raw extractor thứ k,
/// `FromOperator(k)` = output của node thứ k.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum In {
    FromExtractor(usize),
    FromOperator(usize),
}

/// DAG node — references an `Op` by index in `Graph.ops`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub op: usize,
    pub inputs: Vec<In>,
}

// ── Graph ------------------------------------------------------------

/// Genome/DAG cho ONNX trading model.
/// `ops` là danh sách các Op trait objects, `nodes` là DAG referencing ops by index.
///
/// Không derive `Clone`: `Box<dyn Op>` không clone được, nên bản sao đi qua
/// serde (typetag giữ đủ `ops`/`extractors` — xem `with_prebuilt_onnx`).
#[derive(Debug, Serialize, Deserialize)]
pub struct Graph {
    ops: Vec<Box<dyn Op>>,
    nodes: Vec<Node>,
    #[serde(default)]
    extractors: Vec<Box<dyn Extractor>>,

    // @NOTE: initialize
    inited_bias: Vec<f32>,
    inited_weights: Vec<f32>,
    window_size: usize,
    num_of_grids: usize,
    lookback_time_to_rebuild: usize,
    interval_time_to_rebuild: usize,

    /// ONNX bytes đã build sẵn (artifact nạp từ file) — `None` nghĩa là tự emit
    /// từ `ops` + `nodes` như bình thường.
    ///
    /// Genome vẫn là nguồn sự thật: bytes chỉ là cache để khỏi emit lại, và
    /// **không** serialize kèm genome (tránh hai bản có thể lệch nhau).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prebuilt: Option<Arc<Vec<u8>>>,
}

impl Graph {
    pub fn new(
        window_size: usize,
        ops: Vec<Box<dyn Op>>,
        nodes: Vec<Node>,
        extractors: Vec<Box<dyn Extractor>>,
        inited_bias: Vec<f32>,
        inited_weights: Vec<f32>,
        num_of_grids: usize,
        lookback_time_to_rebuild: usize,
        interval_time_to_rebuild: usize,
    ) -> Self {
        Self {
            window_size,
            ops,
            nodes,
            extractors,
            inited_bias,
            inited_weights,
            num_of_grids,
            lookback_time_to_rebuild,
            interval_time_to_rebuild,
            prebuilt: None,
        }
    }

    /// Gắn ONNX bytes đã build sẵn — `model()` bỏ qua `build_onnx` và compile
    /// thẳng từ bytes này.
    ///
    /// Dùng khi ONNX được build ở Python (hoặc lâu hơn một lần trước) rồi lưu
    /// ra file. **Genome vẫn là nguồn sự thật**: bytes phải khớp `ops`+`nodes`+
    /// `window_size`+`num_of_grids`, nếu không `predict` sẽ lỗi shape — hãy
    /// gọi lại [`Self::write_onnx`] sau mỗi lần sửa genome.
    pub fn with_prebuilt_onnx(&self, onnx: Vec<u8>) -> Result<Self, Error> {
        // `Box<dyn Op>` không `Clone` được nên bản sao đi qua serde — typetag
        // bảo toàn `ops`/`extractors` (xem `graph_serializes_trait_objects_with_typetag`).
        let mut next: Self = serde_json::from_str(
            &serde_json::to_string(self)
                .map_err(|e| Error::other(format!("serialize genome thất bại: {e}")))?,
        )
        .map_err(|e| Error::other(format!("clone genome qua JSON thất bại: {e}")))?;
        next.prebuilt = Some(Arc::new(onnx));
        Ok(next)
    }

    /// ONNX bytes ứng với genome này: bytes đã nạp sẵn nếu có, nếu không thì
    /// emit từ `ops` + `nodes`. File `.onnx` chuẩn — Python (`onnx.load`) đọc
    /// được, không cần qlib.
    pub fn onnx_bytes(&self) -> Result<Vec<u8>, Error> {
        match &self.prebuilt {
            Some(bytes) => Ok(bytes.as_ref().clone()),
            None => self.build_onnx().map(|(bytes, _)| bytes),
        }
    }

    /// Ghi ONNX ra file (artifact để chia sẻ / inspect; không phải nguồn sự
    /// thật — genome mới là).
    pub fn write_onnx(&self, path: impl AsRef<std::path::Path>) -> Result<(), Error> {
        let bytes = self.onnx_bytes()?;
        std::fs::write(path, bytes)
            .map_err(|e| Error::other(format!("ghi ONNX thất bại: {e}")))
    }

    /// Nạp cặp **genome + ONNX**: đọc genome JSON (nguồn sự thật) rồi gắn
    /// bytes `.onnx` để khỏi emit lại.
    ///
    /// Sửa genome thì phải [`Self::write_onnx`] lại — bytes cũ sẽ lệch shape và
    /// `predict` báo lỗi (đây là chủ ý: sai model phải lộ ra, không âm thầm
    /// chạy kết quả cũ).
    pub fn load_artifact(
        genome_path: impl AsRef<std::path::Path>,
        onnx_path: impl AsRef<std::path::Path>,
    ) -> Result<Self, Error> {
        let genome_path = genome_path.as_ref();
        let onnx_path = onnx_path.as_ref();
        let genome = std::fs::read_to_string(genome_path)
            .map_err(|e| Error::other(format!("đọc genome thất bại ({genome_path:?}): {e}")))?;
        let graph: Self = serde_json::from_str(&genome)
            .map_err(|e| Error::other(format!("genome không hợp lệ: {e}")))?;
        let onnx = std::fs::read(onnx_path)
            .map_err(|e| Error::other(format!("đọc ONNX thất bại ({onnx_path:?}): {e}")))?;
        if onnx.is_empty() {
            return Err(Error::other("file ONNX rỗng"));
        }
        graph.with_prebuilt_onnx(onnx)
    }
}

// ═══════════════════════════════════════════════════════════════════
// Predictor cache
// ═══════════════════════════════════════════════════════════════════════════════

const PREDICTOR_CACHE_CAPACITY: usize = 32;

struct CompiledPredictorCache {
    entries: Mutex<VecDeque<(u64, Arc<TypedRunnableModel>)>>,
}

impl CompiledPredictorCache {
    fn new() -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(PREDICTOR_CACHE_CAPACITY)),
        }
    }

    fn get(&self, key: u64) -> Option<Arc<TypedRunnableModel>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, m)| m.clone())
    }

    fn put(&self, key: u64, model: Arc<TypedRunnableModel>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pos) = entries.iter().position(|(k, _)| *k == key) {
            entries.remove(pos);
        }
        entries.push_back((key, model));
        while entries.len() > PREDICTOR_CACHE_CAPACITY {
            entries.pop_front();
        }
    }
}

static PREDICTOR_CACHE: OnceLock<CompiledPredictorCache> = OnceLock::new();

fn predictor_cache() -> &'static CompiledPredictorCache {
    PREDICTOR_CACHE.get_or_init(CompiledPredictorCache::new)
}

// ── ONNX Build ────────────────────────────────────────────────────────

impl Graph {
    /// Compile DAG → ONNX bytes. Trả về `(bytes, n_feat)`.
    fn build_onnx(&self) -> Result<(Vec<u8>, usize), Error> {
        // ── Topological sort ──────────────────────────────────────────
        let mut irefs: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut iparams: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut levels = vec![0; self.nodes.len()];
        let mut scanned = HashSet::new();

        for (i, node) in self.nodes.iter().enumerate() {
            for input in &node.inputs {
                match input {
                    In::FromOperator(id) => {
                        irefs.entry(*id).or_default().push(i);
                    }
                    In::FromExtractor(id) => {
                        iparams.entry(*id).or_default().push(i);
                        scanned.insert(i);
                    }
                }
            }
        }

        let mut scanning = VecDeque::new();
        for inode in &scanned {
            if let Some(consumers) = irefs.get(inode) {
                for iref in consumers {
                    scanning.push_back(iref);
                }
            }
        }

        for _ in 0..(2 * self.nodes.len()) {
            if let Some(inode) = scanning.pop_front() {
                if self.nodes[*inode]
                    .inputs
                    .iter()
                    .find(
                        |input| matches!(input, In::FromOperator(iref) if !scanned.contains(iref)),
                    )
                    .is_some()
                {
                    continue;
                }

                levels[*inode] = 1 + self.nodes[*inode]
                    .inputs
                    .iter()
                    .filter_map(|input| match input {
                        In::FromOperator(iref) => Some(levels[*iref]),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(0);

                if let Some(consumers) = irefs.get(inode) {
                    for iref in consumers {
                        scanning.push_back(iref);
                    }
                }

                scanned.insert(*inode);
            }
        }

        if scanned.len() < self.nodes.len() {
            return Err(Error::other(
                "genome có chu trình (cycle) → không phải DAG hợp lệ",
            ));
        }

        let mut order = levels
            .iter()
            .enumerate()
            .map(|(i, &level)| (Reverse(level), i))
            .collect::<BinaryHeap<_>>();

        let mut graph = GraphProto::default();

        // Declare extractor inputs (in0, in1, ...)
        let mut ext_ids = iparams.keys().copied().collect::<Vec<_>>();
        ext_ids.sort_unstable();
        for k in ext_ids {
            graph.input.push(crate::onnx_value_info!(
                format!("in{k}"),
                1,
                self.window_size as i64
            ));
        }

        // ── Head: special handling (push W_pred_flat/B_pred inputs) ──
        let mut head_idx: Option<usize> = None;
        let mut head_n_out: usize = 0;
        let mut atr_idx: Option<usize> = None;
        let mut n_feat: Option<usize> = None;

        let mut ctx = OnnxEmitCtx::new(&mut graph, self.window_size);

        while let Some((Reverse(_level), i)) = order.pop() {
            let out = format!("n{i}");
            let inputs: Vec<String> = self.nodes[i]
                .inputs
                .iter()
                .map(|inp| match inp {
                    In::FromExtractor(k) => format!("in{k}"),
                    In::FromOperator(k) => format!("n{k}"),
                })
                .collect();
            let outputs = vec![out.clone()];

            let op = &self.ops[self.nodes[i].op];

            if op.head_features().is_some() {
                // ── Head: special handling (push W_pred_flat/B_pred inputs) ──
                let feat = op.head_features().unwrap();
                let n_out = op.head_n_out();
                head_idx = Some(i);
                head_n_out = n_out;
                n_feat = Some(feat);

                let shape = vec![feat as i64, n_out as i64];
                ctx.graph.initializer.push(crate::onnx_initializer!(
                    format!("rsh_{i}"),
                    &shape,
                    i64
                ));
                ctx.graph.input.push(crate::onnx_value_info!(
                    "W_pred_flat",
                    1,
                    (feat * n_out) as i64
                ));
                ctx.graph
                    .input
                    .push(crate::onnx_value_info!("B_pred", 1, n_out as i64));
                crate::prediction_layer!(
                    &mut ctx.graph,
                    features: inputs[0].clone(),
                    weights_input: "W_pred_flat",
                    bias_input: "B_pred",
                    reshape_shape: format!("rsh_{i}"),
                    output: out.clone(),
                    n_features: feat,
                    n_outputs: n_out
                );
                continue;
            }

            if op.is_atr() {
                atr_idx = Some(i);
            }

            op.emit(&mut ctx, &inputs, &outputs)?;
        }

        let head_idx = head_idx.ok_or_else(|| {
            Error::other("genome thiếu node Head (output grid_params) → không thể setup")
        })?;
        ctx.graph.output.push(crate::onnx_value_info!(
            format!("n{head_idx}"),
            1,
            head_n_out as i64
        ));
        if let Some(a) = atr_idx {
            ctx.graph
                .output
                .push(crate::onnx_value_info!(format!("n{a}"), 1, 1i64));
        }

        let bytes = crate::onnx_model! {
            name: "DagGenome",
            ir_version: 9,
            opset_version: 21,
            graph: graph,
        };

        let n_feat = n_feat.ok_or_else(|| Error::other("genome thiếu node Head"))?;

        Ok((bytes, n_feat))
    }

    /// Compiled ONNX predictor — cache theo fingerprint.
    ///
    /// Ưu tiên bytes nạp sẵn (`with_prebuilt_onnx`): bỏ qua `build_onnx`. Sai
    /// shape sẽ lộ ra ở `predict`/compile, kèm gợi ý build lại.
    fn model(&self) -> Result<(Arc<TypedRunnableModel>, usize), Error> {
        let key = self.fingerprint();
        if let Some(m) = predictor_cache().get(key) {
            return Ok((m, self.head_features()?));
        }
        let (bytes, n_feat) = match &self.prebuilt {
            Some(prebuilt) => (prebuilt.as_ref().clone(), self.head_features()?),
            None => self.build_onnx()?,
        };
        let prebuilt_hint = if self.prebuilt.is_some() {
            " (ONNX nạp sẵn có thể không khớp genome — thử write_onnx lại)"
        } else {
            ""
        };
        let model = tract_onnx::onnx()
            .model_for_read(&mut std::io::Cursor::new(&bytes))
            .and_then(|m| m.into_optimized())
            .and_then(|m| m.into_runnable())
            .map_err(|e| Error::other(format!("ONNX compile failed{prebuilt_hint}: {e}")))?;
        predictor_cache().put(key, model.clone());
        Ok((model, n_feat))
    }

    /// Fingerprint cấu trúc DAG **+ bytes nạp sẵn** (nếu có).
    ///
    /// Cần bytes trong key: hai artifact cùng cấu trúc nhưng khác bytes (ví dụ
    /// build lại sau khi đổi op khác) không được dùng chung cache entry.
    fn fingerprint(&self) -> u64 {
        let mut h = DefaultHasher::new();
        if let Ok(bytes) =
            serde_json::to_vec(&(self.window_size, self.num_of_grids, &self.ops, &self.nodes))
        {
            bytes.hash(&mut h);
        } else {
            self.window_size.hash(&mut h);
            self.num_of_grids.hash(&mut h);
        }
        if let Some(prebuilt) = &self.prebuilt {
            prebuilt.len().hash(&mut h);
            prebuilt.hash(&mut h);
        }
        h.finish()
    }

    fn head_features(&self) -> Result<usize, Error> {
        for node in &self.nodes {
            if let Some(feat) = self.ops[node.op].head_features() {
                return Ok(feat);
            }
        }
        Err(Error::other("genome thiếu node Head"))
    }

    /// Compile DAG → ONNX bytes (alias của [`Self::onnx_bytes`] cho code cũ).
    pub fn compile(&self) -> Result<Vec<u8>, Error> {
        self.onnx_bytes()
    }

    pub fn num_features(&self) -> Result<usize, Error> {
        self.head_features()
    }

    pub fn num_grids(&self) -> usize {
        self.num_of_grids
    }

    /// Chạy inference trên `inputs`.
    pub fn predict(&self, inputs: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, Error> {
        let (model, _n_feat) = self.model()?;
        Self::infer(model, inputs)
    }

    fn infer(model: Arc<TypedRunnableModel>, inputs: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, Error> {
        let tensors: Vec<TValue> = inputs
            .iter()
            .map(|input| {
                let array = tract_ndarray::Array2::from_shape_vec((1, input.len()), input.clone())
                    .map_err(|e| Error::other(format!("input tensor: {e}")))?;
                Ok(Tensor::from(array).into())
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let outputs = model
            .run(tensors.into())
            .map_err(|e| Error::other(format!("ONNX inference failed: {e}")))?;

        outputs
            .iter()
            .map(|output| {
                let view = output
                    .to_plain_array_view::<f32>()
                    .map_err(|e| Error::other(format!("output tensor: {e}")))?;
                Ok(view.iter().copied().collect())
            })
            .collect::<Result<Vec<_>, Error>>()
    }

    /// Decode ONNX outputs → TradingGrid.
    ///
    /// `grid_levels` / `sl_pct` đến từ `params` (`P_GRID_LEVELS`, `P_SL_PCT`) để
    /// cùng một DAG vẫn cấu hình được như `GridStrategy` (SGD tối ưu chung
    /// không gian params).
    fn setup(
        last_close: f64,
        outputs: &[Vec<f32>],
        grids: &[TradingGrid],
        grid_levels: usize,
        sl_pct: f64,
    ) -> Vec<TradingGrid> {
        let gp = outputs.first().map(|v| v.as_slice()).unwrap_or(&[]);
        let atr = Self::finite_or(
            outputs
                .get(1)
                .and_then(|v| v.first())
                .copied()
                .unwrap_or(0.0) as f64,
            0.0,
        );
        if (atr <= 0.0) || !last_close.is_finite() || last_close <= 0.0 {
            return Vec::new();
        }

        let market_bias = Self::finite_or(gp.first().copied().unwrap_or(0.5) as f64, 0.5);
        let epsilon = 1e-9;

        let (direction, strength) = match market_bias {
            mb if mb > 0.5 + epsilon => (1.0, ((mb - 0.5) * 2.0).clamp(0.0, 1.0)),
            mb if mb < 0.5 - epsilon => (-1.0, ((mb - 0.5).abs() * 2.0).clamp(0.0, 1.0)),
            _ => (0.0, 0.0),
        };

        let atr_mult = 2.0;
        let half_width = atr_mult * atr / 2.0;
        let (grid_min, grid_max) = (last_close - half_width, last_close + half_width);
        // `TradingGrid::new` cần ≥ 2 bậc; params có thể vô lý (SGD) → clamp.
        let grid_levels = grid_levels.clamp(2, 64);

        let Some(mut tg) = TradingGrid::new(grid_levels, grid_min, grid_max) else {
            return Vec::new();
        };

        tg = if direction == 0.0 {
            tg.with_weights_normal(4.0).with_sl_pct(sl_pct)
        } else {
            tg.with_weights_trend(direction, strength)
                .with_sl_pct(sl_pct)
        };

        let prob_base = Self::finite_or(gp.get(1).copied().unwrap_or(0.5) as f64, 0.5);
        let bias_scale = Self::finite_or(gp.get(2).copied().unwrap_or(0.22) as f64, 0.22);
        let level_trend = Self::finite_or(gp.get(3).copied().unwrap_or(0.05) as f64, 0.05);
        let prob_max = Self::finite_or(gp.get(4).copied().unwrap_or(0.78) as f64, 0.78);
        let prob_min = Self::finite_or(gp.get(5).copied().unwrap_or(0.35) as f64, 0.35);
        let prob_short_max = Self::finite_or(gp.get(6).copied().unwrap_or(0.65) as f64, 0.65);
        let prob_short_min = Self::finite_or(gp.get(7).copied().unwrap_or(0.22) as f64, 0.22);

        let k = tg.num_levels();
        let bias = strength * bias_scale;
        let (pmin, pmax) = (prob_min.min(prob_max), prob_min.max(prob_max));
        let (smin, smax) = (
            prob_short_min.min(prob_short_max),
            prob_short_min.max(prob_short_max),
        );
        let (long_win, short_win): (Vec<f64>, Vec<f64>) = (0..k)
            .map(|j| {
                let t = j as f64 / (k.max(2) - 1) as f64;
                let (lp, sp) = match direction {
                    d if d > 0.0 => (
                        prob_base + bias + (1.0 - t) * level_trend,
                        prob_base - bias - t * level_trend,
                    ),
                    d if d < 0.0 => (
                        prob_base + bias + (1.0 - t) * level_trend,
                        prob_base - bias - t * level_trend,
                    ),
                    _ => (prob_base, prob_base),
                };
                (lp.clamp(pmin, pmax), sp.clamp(smin, smax))
            })
            .unzip();

        let (long_win, short_win) =
            Self::blend_realized_win_rates(&long_win, &short_win, grids, 0.2, 5);

        tg = tg
            .with_sl_pct(0.008)
            .with_max_candles(48)
            .with_win_probabilities(long_win, short_win);

        vec![tg]
    }

    fn finite_or(v: f64, default: f64) -> f64 {
        if v.is_finite() { v } else { default }
    }

    fn blend_realized_win_rates(
        long_win: &[f64],
        short_win: &[f64],
        grids: &[TradingGrid],
        alpha: f64,
        min_samples: usize,
    ) -> (Vec<f64>, Vec<f64>) {
        let mut long_wins = 0usize;
        let mut long_total = 0usize;
        let mut short_wins = 0usize;
        let mut short_total = 0usize;

        for g in grids {
            for j in 0..g.num_levels() {
                long_wins += g.long_win_count(j);
                long_total += g.long_win_count(j) + g.long_lost_count(j);
                short_wins += g.short_win_count(j);
                short_total += g.short_win_count(j) + g.short_lost_count(j);
            }
        }
        let realized_long =
            (long_total >= min_samples).then(|| long_wins as f64 / long_total as f64);
        let realized_short =
            (short_total >= min_samples).then(|| short_wins as f64 / short_total as f64);

        #[cfg(debug_assertions)]
        if realized_long.is_some() || realized_short.is_some() {
            eprintln!(
                "  [debug] win_rate_prior: α={alpha} samples(long/short)=({long_total},{short_total}) \
                 realized=({:.3},{:.3})",
                realized_long.unwrap_or(f64::NAN),
                realized_short.unwrap_or(f64::NAN)
            );
        }

        let blend = |p: f64, realized: Option<f64>| match realized {
            Some(r) => (1.0 - alpha) * p + alpha * r,
            None => p,
        };
        let lp_out: Vec<f64> = long_win.iter().map(|&p| blend(p, realized_long)).collect();
        let sp_out: Vec<f64> = short_win
            .iter()
            .map(|&p| blend(p, realized_short))
            .collect();
        (lp_out, sp_out)
    }
}

#[typetag::serde(name = "dag")]
#[async_trait]
impl Strategy for Graph {
    /// Layout `params` (khớp `Portfolio` đọc qua `ParamFn`):
    ///
    /// | idx | ý nghĩa                                              |
    /// |-----|-------------------------------------------------------|
    /// | 0   | `kelly_fraction` (Portfolio::evaluate)                |
    /// | 1   | `base_capital` (Portfolio::evaluate)                  |
    /// | 2   | `grid_levels` — số bậc lưới khi dựng plan             |
    /// | 3   | `sl_pct` — stop-loss mỗi lệnh                          |
    /// | 4   | `lookback_secs` — cửa sổ nến cho rebuild              |
    /// | 5   | reserved (chưa dùng)                                   |
    /// | 6.. | trọng số mô hình (`n_feat × num_of_grids`)            |
    /// | +w  | bias (`num_of_grids`)                                  |
    ///
    /// 0..5 **phải có giá trị thật**: `base_capital = 0` khiến
    /// `calculate_order_size` trả 0 → mọi lệnh đặt ra không có size. Xem
    /// `graph::tests::init_gives_trading_params_not_zeros`.
    fn init(&self) -> Vec<f64> {
        let n_feat = self.head_features().unwrap_or(0);
        let w_len = n_feat * self.num_of_grids;
        let mut params = vec![0.0; 6 + w_len + self.num_of_grids];
        params[P_KELLY] = DEFAULT_KELLY_FRACTION;
        params[P_CAPITAL] = DEFAULT_BASE_CAPITAL;
        params[P_GRID_LEVELS] = DEFAULT_GRID_LEVELS as f64;
        params[P_SL_PCT] = DEFAULT_SL_PCT;
        params[P_LOOKBACK] = self.lookback_time_to_rebuild as f64;
        for (i, w) in self.inited_weights.iter().enumerate() {
            if i < w_len {
                params[P_WEIGHTS + i] = *w as f64;
            }
        }
        for (i, b) in self.inited_bias.iter().enumerate() {
            if i < self.num_of_grids {
                params[P_WEIGHTS + w_len + i] = *b as f64;
            }
        }
        params
    }

    async fn next(&self, current: u64) -> u64 {
        current + self.interval_time_to_rebuild as u64
    }

    async fn rebuild(
        &self,
        current_ts: u64,
        grids: &[TradingGrid],
        fetch: FetchFn<'_>,
        param: ParamFn<'_>,
    ) -> Result<Vec<TradingGrid>, Error> {
        let lookback = self.lookback_time_to_rebuild;
        let candles = fetch(current_ts.saturating_sub(lookback as u64), current_ts).await?;

        let mut inputs: Vec<Vec<f64>> = Vec::new();
        let (model, n_feat) = self.model()?;
        let w_len = n_feat * self.num_of_grids;
        let default_b = self.inited_bias.clone();

        for extractor in &self.extractors {
            inputs.extend(extractor.extract(candles.as_slice())?);
        }

        inputs.push(
            (0..w_len)
                .map(|i| {
                    Self::finite_or(
                        param(P_WEIGHTS + i),
                        self.inited_weights.get(i).copied().unwrap_or(0.0) as f64,
                    )
                })
                .collect(),
        );
        inputs.push(
            (0..self.num_of_grids)
                .map(|i| {
                    Self::finite_or(
                        param(P_WEIGHTS + w_len + i),
                        default_b.get(i).copied().unwrap_or(0.0) as f64,
                    )
                })
                .collect(),
        );

        Ok(Self::setup(
            candles.last().map_or(0.0, |c| c.c),
            &Self::infer(
                model,
                inputs
                    .iter()
                    .map(|v| v.iter().map(|&x| x as f32).collect())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?,
            grids,
            param(P_GRID_LEVELS) as usize,
            Self::finite_or(param(P_SL_PCT), DEFAULT_SL_PCT),
        ))
    }
}

#[cfg(test)]
mod tests;
