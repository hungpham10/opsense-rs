use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Error;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use tract_onnx::pb::{AttributeProto, GraphProto, NodeProto};
use tract_onnx::prelude::*;
use tract_onnx::tract_core::model::typed::TypedRunnableModel;

use analysis::TradingGrid;

use super::{
    Extractor, FetchFn, ParamFn, Strategy,
    models::{ema_weights, rsi_weights, sma_weights},
};
use crate::onnx_node;

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph {
    // @NOTE:
    ops: Vec<Op>,
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
}

/// Input của một node: `$k` = raw extractor thứ k, `#k` = output của node thứ k.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum In {
    FromExtractor(usize),
    FromOperator(usize),
}

/// Các building blocks cơ bản — "DNA alphabet".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Op {
    /// Lấy giá trị candle cuối cùng của một chuỗi (Last close, ...).
    Last,

    /// Rate of Change trên một chuỗi: `(last - ago) / ago`.
    Roc {
        period: usize,
    },

    /// Average True Range (Wilder) từ highs/lows/prev_closes.
    Atr {
        period: usize,
    },

    /// Đỉnh Donchian (max highs qua `period` candle trước).
    DonchianHigh {
        period: usize,
    },

    /// Đáy Donchian (min lows qua `period` candle trước).
    DonchianLow {
        period: usize,
    },

    /// Exponential Moving Average (Wilder) của một chuỗi.
    Ema {
        period: usize,
    },

    /// Simple Moving Average của một chuỗi.
    Ma {
        period: usize,
    },

    /// Relative Strength Index (Wilder), chuẩn hóa về `[0,1]`:
    /// `avg_gain / (avg_gain + avg_loss)`.
    Rsi {
        period: usize,
    },

    /// Ghép các feature scalar thành vector `[1, n]`.
    Concat {
        axis: i64,
    },

    /// Prediction head: MatMul(features, W_pred) + B_pred → Sigmoid.
    /// `n_feat` = số feature đầu vào, `n_out` = số tham số grid (thường = 8).
    Head {
        n_feat: usize,
        n_out: usize,
    },

    /// Phép toán phần tử.
    Add,
    Sub,
    Mul,
    Div,
    Abs,
    Max,
    Min,
    Neg,
    Sigmoid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub op: usize,
    pub inputs: Vec<In>,
}

impl Graph {
    pub fn new(
        window_size: usize,
        ops: Vec<Op>,
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
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Predictor cache — tránh recompile tract (~ms) cho cùng một DAG
// ═══════════════════════════════════════════════════════════════════════════════
//
// RL cần hàng nghìn eval trên cùng genotype; bước đắt nhất là compile ONNX →
// tract predictor (`model_for_read → into_optimized → into_runnable`). Cache
// bounded FIFO (~32 entries) theo fingerprint cấu trúc DAG:
// - `W_pred_flat`/`B_pred` là **graph inputs** override được lúc chạy (SGD/
//   `load_params`) nên cấu trúc predictor không phụ thuộc w/b → mọi eval cùng
//   genotype chia sẻ một predictor.

/// Số predictor tối đa giữ lại trong cache (FIFO).
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

impl Graph {
    /// Compile DAG → ONNX bytes (chưa chạy tract). Trả về `(bytes, n_feat)`.
    /// `n_feat` = số feature đầu vào của node Head (dùng tính `w_len`).
    fn build_onnx(&self) -> Result<(Vec<u8>, usize), Error> {
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

        // Priority queue: (level, node) → pop tăng dần level.
        // Cùng level = độc lập (layer song song); tract tự parallel khi chạy.
        // Gần inputs → level bé → duyệt trước; gần outputs → level lớn → duyệt sau.
        let mut order = levels
            .iter()
            .enumerate()
            .map(|(i, &level)| (Reverse(level), i))
            .collect::<BinaryHeap<_>>();

        let mut graph = GraphProto::default();
        graph.name = "DagGenome".to_string();

        // Declare extractor inputs (in0, in1, ...) ascending — khớp thứ tự
        // `rebuild` cung cấp (raw[0]→in0, raw[1]→in1, ...).
        let mut ext_ids: Vec<usize> = iparams.keys().copied().collect();
        ext_ids.sort_unstable();
        for k in ext_ids {
            graph.input.push(crate::onnx_value_info!(
                format!("in{k}"),
                1,
                self.window_size as i64
            ));
        }

        let mut head_idx: Option<usize> = None;
        let mut head_n_out: usize = 0;
        let mut atr_idx: Option<usize> = None;
        let mut n_feat: Option<usize> = None;

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

            match &self.ops[self.nodes[i].op] {
                Op::Last => {
                    let w = extract_w(self.window_size);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_last_{i}"),
                        &w,
                        self.window_size as i64,
                        1
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("mm_last_{i}"),
                        [inputs[0].clone(), format!("w_last_{i}")] -> [out.clone()]
                    ));
                }
                Op::Roc { period } => {
                    let we = extract_w(self.window_size);
                    let wr = roc_prev_w(self.window_size, *period);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_last_{i}"),
                        &we,
                        self.window_size as i64,
                        1
                    ));
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_roc_{i}"),
                        &wr,
                        self.window_size as i64,
                        1
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("rl_{i}"),
                        [inputs[0].clone(), format!("w_last_{i}")] -> [format!("rl_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("ra_{i}"),
                        [inputs[0].clone(), format!("w_roc_{i}")] -> [format!("ra_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Sub", format!("rd_{i}"),
                        [format!("rl_{i}"), format!("ra_{i}")] -> [format!("rd_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Div", format!("rdiv_{i}"),
                        [format!("rd_{i}"), format!("ra_{i}")] -> [out.clone()]
                    ));
                }
                Op::Atr { period } => {
                    let w = ema_weights(self.window_size, *period);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_atr_{i}"),
                        &w,
                        self.window_size as i64,
                        1
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Sub", format!("ahl_{i}"),
                        [inputs[0].clone(), inputs[1].clone()] -> [format!("ahl_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Sub", format!("ahmpc_{i}"),
                        [inputs[0].clone(), inputs[2].clone()] -> [format!("ahmpc_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Abs", format!("aahmpc_{i}"),
                        [format!("ahmpc_{i}")] -> [format!("aahmpc_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Sub", format!("almpc_{i}"),
                        [inputs[1].clone(), inputs[2].clone()] -> [format!("almpc_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Abs", format!("aalmpc_{i}"),
                        [format!("almpc_{i}")] -> [format!("aalmpc_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Max", format!("am1_{i}"),
                        [format!("ahl_{i}"), format!("aahmpc_{i}")] -> [format!("am1_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Max", format!("atr_{i}"),
                        [format!("am1_{i}"), format!("aalmpc_{i}")] -> [format!("atr_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("amm_{i}"),
                        [format!("atr_{i}"), format!("w_atr_{i}")] -> [out.clone()]
                    ));
                    atr_idx = Some(i);
                }
                Op::DonchianHigh { period } => {
                    let m = mask_donchian(self.window_size, *period);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("mh_{i}"),
                        &m,
                        1,
                        self.window_size as i64
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Mul", format!("mhm_{i}"),
                        [inputs[0].clone(), format!("mh_{i}")] -> [format!("mhm_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "ReduceMax", format!("rmax_{i}"),
                        [format!("mhm_{i}")] -> [out.clone()],
                        axes=[1i64], keepdims=1
                    ));
                }
                Op::DonchianLow { period } => {
                    let m = mask_min_complement(self.window_size, *period);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("ml_{i}"),
                        &m,
                        1,
                        self.window_size as i64
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Add", format!("mlm_{i}"),
                        [inputs[0].clone(), format!("ml_{i}")] -> [format!("mlm_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "ReduceMin", format!("rmin_{i}"),
                        [format!("mlm_{i}")] -> [out.clone()],
                        axes=[1i64], keepdims=1
                    ));
                }
                Op::Ema { period } => {
                    let w = ema_weights(self.window_size, *period);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_ema_{i}"),
                        &w,
                        self.window_size as i64,
                        1
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("ema_{i}"),
                        [inputs[0].clone(), format!("w_ema_{i}")] -> [out.clone()]
                    ));
                }
                Op::Ma { period } => {
                    let w = sma_weights(self.window_size, *period);
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_ma_{i}"),
                        &w,
                        self.window_size as i64,
                        1
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("ma_{i}"),
                        [inputs[0].clone(), format!("w_ma_{i}")] -> [out.clone()]
                    ));
                }
                Op::Rsi { period } => {
                    let w = rsi_weights(self.window_size, *period);
                    let zero = vec![0.0f32; self.window_size];
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("w_rsi_{i}"),
                        &w,
                        self.window_size as i64,
                        1
                    ));
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("zero_{i}"),
                        &zero,
                        1,
                        self.window_size as i64
                    ));
                    let closes = &inputs[0];
                    let prev = &inputs[1];
                    graph.node.push(crate::onnx_node!(
                        "Sub", format!("rsi_diff_{i}"),
                        [closes.clone(), prev.clone()] -> [format!("rsi_diff_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Max", format!("rsi_gains_{i}"),
                        [format!("rsi_diff_{i}"), format!("zero_{i}")] -> [format!("rsi_gains_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Neg", format!("rsi_neg_{i}"),
                        [format!("rsi_diff_{i}")] -> [format!("rsi_neg_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Max", format!("rsi_loss_{i}"),
                        [format!("rsi_neg_{i}"), format!("zero_{i}")] -> [format!("rsi_loss_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("rsi_ag_{i}"),
                        [format!("rsi_gains_{i}"), format!("w_rsi_{i}")] -> [format!("rsi_ag_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "MatMul", format!("rsi_al_{i}"),
                        [format!("rsi_loss_{i}"), format!("w_rsi_{i}")] -> [format!("rsi_al_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Add", format!("rsi_den_{i}"),
                        [format!("rsi_ag_{i}"), format!("rsi_al_{i}")] -> [format!("rsi_den_{i}")]
                    ));
                    graph.node.push(crate::onnx_node!(
                        "Div", format!("rsi_raw_{i}"),
                        [format!("rsi_ag_{i}"), format!("rsi_den_{i}")] -> [out.clone()]
                    ));
                }
                Op::Concat { axis } => {
                    graph.node.push(NodeProto {
                        op_type: "Concat".into(),
                        name: format!("concat_{i}"),
                        input: inputs,
                        output: vec![out.clone()],
                        attribute: vec![AttributeProto {
                            name: "axis".into(),
                            r#type: 2,
                            i: *axis,
                            ..Default::default()
                        }],
                        ..Default::default()
                    });
                }
                Op::Head {
                    n_feat: feat,
                    n_out,
                } => {
                    if head_idx.is_some() {
                        return Err(Error::other("genome có nhiều hơn 1 node Head"));
                    }
                    let shape = vec![*feat as i64, *n_out as i64];
                    graph.initializer.push(crate::onnx_initializer!(
                        format!("rsh_{i}"),
                        &shape,
                        i64
                    ));
                    graph.input.push(crate::onnx_value_info!(
                        "W_pred_flat",
                        1,
                        (*feat * *n_out) as i64
                    ));
                    graph
                        .input
                        .push(crate::onnx_value_info!("B_pred", 1, *n_out as i64));
                    crate::prediction_layer!(
                        graph,
                        features: inputs[0].clone(),
                        weights_input: "W_pred_flat",
                        bias_input: "B_pred",
                        reshape_shape: format!("rsh_{i}"),
                        output: out.clone(),
                        n_features: *feat,
                        n_outputs: *n_out
                    );
                    head_idx = Some(i);
                    head_n_out = *n_out;
                    n_feat = Some(*feat);
                }
                Op::Add => graph.node.push(crate::onnx_node!(
                    "Add", format!("add_{i}"),
                    [inputs[0].clone(), inputs[1].clone()] -> [out.clone()]
                )),
                Op::Sub => graph.node.push(crate::onnx_node!(
                    "Sub", format!("sub_{i}"),
                    [inputs[0].clone(), inputs[1].clone()] -> [out.clone()]
                )),
                Op::Mul => graph.node.push(crate::onnx_node!(
                    "Mul", format!("mul_{i}"),
                    [inputs[0].clone(), inputs[1].clone()] -> [out.clone()]
                )),
                Op::Div => graph.node.push(crate::onnx_node!(
                    "Div", format!("div_{i}"),
                    [inputs[0].clone(), inputs[1].clone()] -> [out.clone()]
                )),
                Op::Max => graph.node.push(crate::onnx_node!(
                    "Max", format!("max_{i}"),
                    [inputs[0].clone(), inputs[1].clone()] -> [out.clone()]
                )),
                Op::Min => graph.node.push(crate::onnx_node!(
                    "Min", format!("min_{i}"),
                    [inputs[0].clone(), inputs[1].clone()] -> [out.clone()]
                )),
                Op::Abs => graph.node.push(crate::onnx_node!(
                    "Abs", format!("abs_{i}"),
                    [inputs[0].clone()] -> [out.clone()]
                )),
                Op::Neg => graph.node.push(crate::onnx_node!(
                    "Neg", format!("neg_{i}"),
                    [inputs[0].clone()] -> [out.clone()]
                )),
                Op::Sigmoid => graph.node.push(crate::onnx_node!(
                    "Sigmoid", format!("sig_{i}"),
                    [inputs[0].clone()] -> [out.clone()]
                )),
            }
        }

        let head_idx = head_idx.ok_or_else(|| {
            Error::other("genome thiếu node Head (output grid_params) → không thể setup")
        })?;
        graph.output.push(crate::onnx_value_info!(
            format!("n{head_idx}"),
            1,
            head_n_out as i64
        ));
        if let Some(a) = atr_idx {
            graph
                .output
                .push(crate::onnx_value_info!(format!("n{a}"), 1, 1i64));
        }

        let bytes = crate::onnx_model! {
            name: "DagGenome",
            ir_version: 9,
            opset_version: 21,
            graph: graph,
        };

        let n_feat = n_feat.ok_or_else(|| Error::other("genome thiếu node Head (SigmoidHead)"))?;

        Ok((bytes, n_feat))
    }

    /// Compiled ONNX predictor từ DAG — cache theo fingerprint (structure của
    /// graph), không phụ thuộc w/b (graph inputs override được mỗi lần chạy).
    fn model(&self) -> Result<(Arc<TypedRunnableModel>, usize), Error> {
        let key = self.fingerprint();
        if let Some(m) = predictor_cache().get(key) {
            return Ok((m, self.head_features()?));
        }
        let (bytes, n_feat) = self.build_onnx()?;
        let model = tract_onnx::onnx()
            .model_for_read(&mut std::io::Cursor::new(&bytes))
            .and_then(|m| m.into_optimized())
            .and_then(|m| m.into_runnable())
            .map_err(|e| Error::other(format!("ONNX compile failed: {e}")))?;
        predictor_cache().put(key, model.clone());
        Ok((model, n_feat))
    }

    /// Fingerprint cấu trúc DAG — chỉ phụ thuộc vào ops/nodes/window/grids
    /// (không phụ thuộc extractors/weights) nên predictor cache dùng chung
    /// cho mọi rebuild với cùng một genome.
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
        h.finish()
    }

    /// Số feature đầu vào của Head (dùng tính `w_len = n_feat * num_of_grids`).
    fn head_features(&self) -> Result<usize, Error> {
        for node in &self.nodes {
            if let Op::Head { n_feat, .. } = &self.ops[node.op] {
                return Ok(*n_feat);
            }
        }
        Err(Error::other("genome thiếu node Head (output grid_params)"))
    }

    /// Compile DAG → ONNX bytes (chưa chạy tract). Dùng cho test / lưu genotype.
    pub fn compile(&self) -> Result<Vec<u8>, Error> {
        Ok(self.build_onnx()?.0)
    }

    /// Số feature đầu vào của Head.
    pub fn num_features(&self) -> Result<usize, Error> {
        self.head_features()
    }

    /// Số grid (thường = 8) — bằng `n_out` của node Head.
    pub fn num_grids(&self) -> usize {
        self.num_of_grids
    }

    /// Chạy inference trên `inputs` ([1, len] mỗi dòng) qua predictor đã cache.
    pub fn predict(&self, inputs: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, Error> {
        let (model, _n_feat) = self.model()?;
        Self::infer(model, inputs)
    }

    /// Static inference: chạy tract trên `inputs` (mỗi row reshape thành
    /// `[1, len]`). Trả về **tất cả** output nodes — contract:
    /// `outputs[0]` = grid_params, `outputs[1]` = atr_val; `setup` chỉ tiêu thụ 2 đầu.
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

    /// Decode ONNX outputs → market structure → `TradingGrid`.
    ///
    /// `outputs[0]` = grid_params [n_out] (market_bias + win-prob params),
    /// `outputs[1]` = atr_val [1]. Grid đặt quanh `last_close`, bán kính
    /// `atr_mult × atr / 2`. market_bias quyết định phân bổ khối lượng:
    /// bearish → nặng bậc cao (SHORT), bullish → nặng bậc thấp (LONG).
    fn setup(last_close: f64, outputs: &[Vec<f32>], grids: &[TradingGrid]) -> Vec<TradingGrid> {
        let gp = outputs.first().map(|v| v.as_slice()).unwrap_or(&[]);
        let atr = Self::finite_or(
            outputs
                .get(1)
                .and_then(|v| v.first())
                .copied()
                .unwrap_or(0.0) as f64,
            0.0,
        );
        if !(atr > 0.0) || !last_close.is_finite() || last_close <= 0.0 {
            return Vec::new();
        }

        // ── Market structure ──────────────────────────────────────────────
        let market_bias = Self::finite_or(gp.first().copied().unwrap_or(0.5) as f64, 0.5);
        let epsilon = 1e-9;

        let (direction, strength) = match market_bias {
            mb if mb > 0.5 + epsilon => (1.0, ((mb - 0.5) * 2.0).clamp(0.0, 1.0)), // bullish
            mb if mb < 0.5 - epsilon => (-1.0, ((mb - 0.5).abs() * 2.0).clamp(0.0, 1.0)), // bearish
            _ => (0.0, 0.0),
        };

        // ── Grid bounds ──────────────────────────────────────────────────
        let atr_mult = 2.0;
        let half_width = atr_mult * atr / 2.0;
        let (grid_min, grid_max) = (last_close - half_width, last_close + half_width);
        let grid_levels = 5.0_f64.round().clamp(2.0, 64.0) as usize;

        let Some(mut tg) = TradingGrid::new(grid_levels, grid_min, grid_max) else {
            return Vec::new();
        };

        // ── Volume theo xu hướng ─────────────────────────────────────────
        tg = if direction == 0.0 {
            tg.with_weights_normal(4.0)
        } else {
            tg.with_weights_trend(direction, strength)
        };

        // ── Win probabilities per level ──────────────────────────────────
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

        // Learn from past `grids`: nudge model win-probs toward realized rates.
        // Defaults mirror graph.rs meta `win_rate_prior` (α=0.2) and
        // `win_rate_prior_min_samples` (5).
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
    fn init(&self) -> Vec<f64> {
        // Vector params: [6 strategy params | w_len head weights | num_of_grids biases].
        // `Portfolio::evaluate`/`optimize` truyền params qua closure `param(id)` nên
        // phải đúng độ dài (để rỗng → `param(6+i)` panic index out-of-bounds).
        let n_feat = self.head_features().unwrap_or(0);
        let w_len = n_feat * self.num_of_grids;
        let mut params = vec![0.0; 6 + w_len + self.num_of_grids];
        for (i, w) in self.inited_weights.iter().enumerate() {
            if i < w_len {
                params[6 + i] = *w as f64;
            }
        }
        for (i, b) in self.inited_bias.iter().enumerate() {
            if i < self.num_of_grids {
                params[6 + w_len + i] = *b as f64;
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
        // ── 1. Fetch ─────────────────────────────────────────────────────────
        let lookback = self.lookback_time_to_rebuild;
        let candles = fetch(current_ts.saturating_sub(lookback as u64), current_ts).await?;

        // ── 2. Feature extraction ────────────────────────────────────────────
        let mut inputs: Vec<Vec<f64>> = Vec::new();
        let (model, n_feat) = self.model()?;
        let w_len = n_feat * self.num_of_grids;
        let default_b = self.inited_bias.clone();

        for extractor in &self.extractors {
            inputs.extend(extractor.extract(candles.as_slice())?);
        }

        // ── 3. n_feat lấy từ Head op; append W_pred_flat + B_pred ──
        inputs.push(
            (0..w_len)
                .map(|i| {
                    Self::finite_or(
                        param(6 + i),
                        self.inited_weights.get(i).copied().unwrap_or(0.0) as f64,
                    )
                })
                .collect(),
        );
        inputs.push(
            (0..self.num_of_grids)
                .map(|i| {
                    Self::finite_or(
                        param(6 + w_len + i),
                        default_b.get(i).copied().unwrap_or(0.0) as f64,
                    )
                })
                .collect(),
        );

        // ── 4. TradingGrid ──────────────────────────────────────────

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
        ))
    }
}

/// Trọng số lấy phần tử cuối: 1.0 tại vị trí cuối, còn lại 0. [ws]
fn extract_w(ws: usize) -> Vec<f32> {
    let mut w = vec![0.0f32; ws];
    if ws > 0 {
        w[ws - 1] = 1.0;
    }
    w
}

/// Trọng số lấy phần tử cách cuối `period` bước: 1.0 tại `ws-1-period`. [ws]
fn roc_prev_w(ws: usize, period: usize) -> Vec<f32> {
    let mut w = vec![0.0f32; ws];
    if ws > 0 && period < ws {
        w[ws - 1 - period] = 1.0;
    }
    w
}

/// Mặt nạ Donchian High: 1.0 trong `period` vị trí cuối, 0 ngoài. [1, ws]
fn mask_donchian(ws: usize, period: usize) -> Vec<f32> {
    let mut m = vec![0.0f32; ws];
    for v in &mut m[ws.saturating_sub(period)..] {
        *v = 1.0;
    }
    m
}

/// Mặt nạ Donchian Low (bù): 0 trong `period` vị trí cuối, 1e9 ngoài. [1, ws]
fn mask_min_complement(ws: usize, period: usize) -> Vec<f32> {
    let mut m = vec![1e9f32; ws];
    for v in &mut m[ws.saturating_sub(period)..] {
        *v = 0.0;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_ops() -> Vec<Op> {
        vec![
            Op::Last,
            Op::Last,
            Op::Atr { period: 14 },
            Op::Last,
            Op::Last,
            Op::Div,
            Op::Concat { axis: 1 },
            Op::Head {
                n_feat: 2,
                n_out: 8,
            },
        ]
    }

    fn default_nodes() -> Vec<Node> {
        vec![
            Node {
                op: 0,
                inputs: vec![In::FromExtractor(0)],
            },
            Node {
                op: 1,
                inputs: vec![In::FromExtractor(0)],
            },
            Node {
                op: 2,
                inputs: vec![
                    In::FromExtractor(1),
                    In::FromExtractor(2),
                    In::FromExtractor(3),
                ],
            },
            Node {
                op: 3,
                inputs: vec![In::FromExtractor(1)],
            },
            Node {
                op: 4,
                inputs: vec![In::FromExtractor(2)],
            },
            Node {
                op: 5,
                inputs: vec![In::FromOperator(2), In::FromOperator(0)],
            },
            Node {
                op: 6,
                inputs: vec![In::FromOperator(1), In::FromOperator(5)],
            },
            Node {
                op: 7,
                inputs: vec![In::FromOperator(6)],
            },
        ]
    }

    #[test]
    fn model_builds_and_infers() {
        let g = Graph::new(
            200,
            default_ops(),
            default_nodes(),
            vec![],
            vec![0.0; 16],
            vec![0.0; 8],
            8,
            200,
            60,
        );
        assert_eq!(g.num_features().expect("num_features"), 2);

        // inputs: in0..in3 (each [window_size]), W_pred_flat [16], B_pred [8]
        let mut inputs: Vec<Vec<f32>> = vec![vec![1.0; 200]; 4];
        inputs.push(vec![0.0; 16]);
        inputs.push(vec![0.0; 8]);

        let out = g.predict(&inputs).expect("predict");
        assert_eq!(out.len(), 2, "grid_params + atr");
        assert_eq!(out[0].len(), 8, "num_of_grids");
        assert_eq!(out[1].len(), 1, "atr");

        for v in &out[0] {
            assert!(
                (v - 0.5).abs() < 1e-4,
                "grid param ≈ 0.5 với trọng số 0, got {v}"
            );
        }
    }

    // TrendFollower: EMA fast/slow + ATR → features (n_feat=4)
    #[test]
    fn trend_follower_genotype_compiles() {
        let ops = vec![
            Op::Ema { period: 9 },
            Op::Ema { period: 21 },
            Op::Atr { period: 14 },
            Op::Sub,
            Op::Concat { axis: 1 },
            Op::Head {
                n_feat: 4,
                n_out: 8,
            },
        ];
        let nodes = vec![
            Node {
                op: 0,
                inputs: vec![In::FromExtractor(0)],
            },
            Node {
                op: 1,
                inputs: vec![In::FromExtractor(0)],
            },
            Node {
                op: 2,
                inputs: vec![
                    In::FromExtractor(1),
                    In::FromExtractor(2),
                    In::FromExtractor(3),
                ],
            },
            Node {
                op: 3,
                inputs: vec![In::FromOperator(0), In::FromOperator(1)],
            },
            Node {
                op: 4,
                inputs: vec![
                    In::FromOperator(0),
                    In::FromOperator(1),
                    In::FromOperator(2),
                    In::FromOperator(3),
                ],
            },
            Node {
                op: 5,
                inputs: vec![In::FromOperator(4)],
            },
        ];
        let g = Graph::new(
            200,
            ops,
            nodes,
            vec![],
            vec![0.0; 32],
            vec![0.0; 8],
            8,
            200,
            60,
        );
        assert_eq!(g.num_features().expect("num_features"), 4);
        let mut inputs: Vec<Vec<f32>> = vec![vec![1.0; 200]; 4];
        inputs.push(vec![0.0; 32]);
        inputs.push(vec![0.0; 8]);
        let out = g.predict(&inputs).expect("predict");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].len(), 8);
        assert_eq!(out[1].len(), 1);
        for v in &out[0] {
            assert!((v - 0.5).abs() < 1e-4);
        }
    }

    // MeanReversion: SMA + RSI + ATR → features (n_feat=4)
    #[test]
    fn mean_reversion_genotype_compiles() {
        let ops = vec![
            Op::Ma { period: 20 },
            Op::Last,
            Op::Div,
            Op::Rsi { period: 14 },
            Op::Atr { period: 14 },
            Op::Div,
            Op::Concat { axis: 1 },
            Op::Head {
                n_feat: 4,
                n_out: 8,
            },
        ];
        let nodes = vec![
            Node {
                op: 0,
                inputs: vec![In::FromExtractor(0)],
            },
            Node {
                op: 1,
                inputs: vec![In::FromExtractor(0)],
            },
            Node {
                op: 2,
                inputs: vec![In::FromOperator(0), In::FromOperator(1)],
            },
            Node {
                op: 3,
                inputs: vec![In::FromExtractor(0), In::FromExtractor(3)],
            },
            Node {
                op: 4,
                inputs: vec![
                    In::FromExtractor(1),
                    In::FromExtractor(2),
                    In::FromExtractor(3),
                ],
            },
            Node {
                op: 5,
                inputs: vec![In::FromOperator(4), In::FromOperator(1)],
            },
            Node {
                op: 6,
                inputs: vec![
                    In::FromOperator(2),
                    In::FromOperator(3),
                    In::FromOperator(5),
                    In::FromOperator(3),
                ],
            },
            Node {
                op: 7,
                inputs: vec![In::FromOperator(6)],
            },
        ];
        let g = Graph::new(
            200,
            ops,
            nodes,
            vec![],
            vec![0.0; 32],
            vec![0.0; 8],
            8,
            200,
            60,
        );
        assert_eq!(g.num_features().expect("num_features"), 4);
        // Ramp (không phẳng) để RSI có denom > 0 — giá phẳng sẽ cho 0/0 = NaN.
        let ramp: Vec<f32> = (0..200).map(|v| 1.0 + v as f32 * 0.1).collect();
        let mut inputs: Vec<Vec<f32>> = vec![ramp.clone(); 4];
        inputs.push(vec![0.0; 32]);
        inputs.push(vec![0.0; 8]);
        let out = g.predict(&inputs).expect("predict");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].len(), 8);
        assert_eq!(out[1].len(), 1);
        for v in &out[0] {
            assert!((v - 0.5).abs() < 1e-4);
        }
    }
}
