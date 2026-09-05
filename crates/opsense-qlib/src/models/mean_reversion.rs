//! MeanReversion ONNX graph builder.
//!
//! Graph: SMA → MatMul, RSI → Sub/Max/MatMul, ATR → Sub/Abs/Max/MatMul,
//! then features → MatMul(W_pred) → Add(B_pred) → Sigmoid → grid_params.
//!
//! Output 0: `grid_params` [1, 8] — market_bias + grid probability parameters
//! Output 1: `atr_val` [1, 1]
//! Output 2: `sma` [1, 1]  (diagnostic)

use super::{NUM_GRID_PARAMS, ema_weights, rsi_weights, sma_weights};
use crate::qlib::Extractor;
use crate::qlib::extractors::OhlcvExtractor;

/// Sigmoid inverse: given a target probability p in (0,1), returns the bias
/// value b such that sigmoid(b) ≈ p (with zero weights).
#[inline]
fn sigmoid_inv(p: f32) -> f32 {
    (p / (1.0 - p)).ln()
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Build MeanReversion ONNX graph bytes.
///
/// `w` must have `4 × {NUM_GRID_PARAMS}` elements (4 features × 8 outputs).
/// `b` must have `{NUM_GRID_PARAMS}` elements (one bias per output head).
///
/// Outputs: grid_params, atr, sma  (3 output tensors).
#[doc(hidden)]
pub fn build_mean_reversion_onnx_bytes(
    ma_period: usize,
    rsi_period: usize,
    atr_period: usize,
    window_size: usize,
    w: &[f32],
    b: &[f32],
) -> (Vec<u8>, Vec<Box<dyn Extractor>>) {
    let n_out = NUM_GRID_PARAMS;
    let n_feat = 4usize;
    assert_eq!(
        w.len(),
        n_feat * n_out,
        "w must have {n_feat} × {n_out} = {} elements",
        n_feat * n_out
    );
    assert_eq!(b.len(), n_out, "b must have {n_out} bias values");

    let sma_w = sma_weights(window_size, ma_period);
    let rsi_w = rsi_weights(window_size, rsi_period);
    let atr_w = ema_weights(window_size, atr_period);
    let extract_w = {
        let mut w = vec![0.0f32; window_size];
        w[window_size - 1] = 1.0;
        w
    };
    let zero_vec = vec![0.0f32; window_size];
    let hundred_vec = vec![100.0f32];

    let graph = crate::onnx_graph! {
        name: "MeanReversion",
        window_size: window_size,
        inputs: ["closes", "highs", "lows", "prev_closes"],
        extra_inputs: [
            ("W_pred_flat", 1, (n_feat * n_out) as i64),
            ("B_pred", 1, n_out as i64)
        ],
        initializers: [
            init: "W_sma" => crate::onnx_initializer!("W_sma", &sma_w, window_size as i64, 1),
            init: "W_rsi" => crate::onnx_initializer!("W_rsi", &rsi_w, window_size as i64, 1),
            init: "W_atr" => crate::onnx_initializer!("W_atr", &atr_w, window_size as i64, 1),
            init: "W_extract" => crate::onnx_initializer!("W_extract", &extract_w, window_size as i64, 1),
            init: "reshape_shape" => crate::onnx_initializer!("reshape_shape", &vec![n_feat as i64, n_out as i64], i64),
            init: "zero" => crate::onnx_initializer!("zero", &zero_vec, 1, window_size as i64),
            init: "hundred" => crate::onnx_initializer!("hundred", &hundred_vec, 1, 1),
        ],
        nodes: [
            Reshape(["W_pred_flat", "reshape_shape"] -> ["W_pred"]),
            MatMul(["closes", "W_sma"] -> ["sma"]),
            MatMul(["closes", "W_extract"] -> ["last_close"]),
            Sub(["closes", "prev_closes"] -> ["diffs"]),
            Max(["diffs", "zero"] -> ["gains"]),
            Neg(["diffs"] -> ["neg_diffs"]),
            Max(["neg_diffs", "zero"] -> ["losses"]),
            MatMul(["gains", "W_rsi"] -> ["avg_gain"]),
            MatMul(["losses", "W_rsi"] -> ["avg_loss"]),
            Add(["avg_gain", "avg_loss"] -> ["denom"]),
            Div(["avg_gain", "denom"] -> ["rsi_raw"]),
            Mul(["rsi_raw", "hundred"] -> ["rsi"]),
            Sub(["highs", "lows"] -> ["hl"]),
            Sub(["highs", "prev_closes"] -> ["hmpc"]),
            Abs(["hmpc"] -> ["ahmpc"]),
            Sub(["lows", "prev_closes"] -> ["lmpc"]),
            Abs(["lmpc"] -> ["almpc"]),
            Max(["hl", "ahmpc"] -> ["m1"]),
            Max(["m1", "almpc"] -> ["tr"]),
            MatMul(["tr", "W_atr"] -> ["atr_val"]),
            Div(["sma", "last_close"] -> ["sma_ratio"]),
            Div(["rsi", "hundred"] -> ["rsi_norm"]),
            Div(["atr_val", "last_close"] -> ["atr_norm"]),
            Concat(["sma_ratio", "rsi_norm", "atr_norm", "rsi_norm"] -> ["features"], axis=1),
            MatMul(["features", "W_pred"] -> ["dot"]),
            Add(["dot", "B_pred"] -> ["biased"]),
            Sigmoid(["biased"] -> ["grid_params"]),
        ],
        outputs: [("grid_params", n_out as i64), ("atr_val", 1i64), ("sma", 1i64)],
    };

    let default_w = w
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let default_b = b
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let model_bytes = crate::onnx_model! {
        name: "MeanReversion",
        ir_version: 9,
        opset_version: 21,
        graph: graph,
        metadata: [
            "default_w" => default_w,
            "default_b" => default_b
        ],
    };

    (
        model_bytes,
        vec![Box::new(OhlcvExtractor {
            window: window_size,
        })],
    )
}

/// Build a MeanReversion ONNX graph with default grid-parameter biases.
///
/// `pred_weights` must have 4 elements (one per feature).
/// `pred_bias` is the bias for market_bias output.
pub fn build_mean_reversion_onnx_default(
    ma_period: usize,
    rsi_period: usize,
    atr_period: usize,
    window_size: usize,
    pred_weights: &[f32],
    pred_bias: f32,
) -> (Vec<u8>, Vec<Box<dyn Extractor>>) {
    let (bytes, extractors) = build_mean_reversion_onnx_bytes(
        ma_period,
        rsi_period,
        atr_period,
        window_size,
        &{
            let n_out = NUM_GRID_PARAMS;
            let n_feat = 4usize;
            let mut w = vec![0.0f32; n_feat * n_out];
            for (i, &v) in pred_weights.iter().enumerate().take(n_feat) {
                w[i] = v;
            }
            w
        },
        &{
            let b: [f32; 8] = [
                pred_bias,
                0.0,
                sigmoid_inv(0.22),
                sigmoid_inv(0.05),
                sigmoid_inv(0.78),
                sigmoid_inv(0.35),
                sigmoid_inv(0.65),
                sigmoid_inv(0.22),
            ];
            b
        },
    );
    (bytes, extractors)
}
