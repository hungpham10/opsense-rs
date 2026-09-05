//! MomentumBreakout ONNX graph builder.
//!
//! The graph computes Donchian channel, ROC, ATR, and market bias — all
//! using ONNX operators.
//!
//! Output 0: `grid_params` [1, 8] — market_bias + grid probability parameters
//! Output 1: `atr_val` [1, 1]
//! Output 2: `prev_hh` [1, 1]  (diagnostic)
//! Output 3: `prev_ll` [1, 1]  (diagnostic)
//! Output 4: `roc_val` [1, 1]  (diagnostic)

use super::{NUM_GRID_PARAMS, ema_weights};
use crate::qlib::Extractor;
use crate::qlib::extractors::OhlcvExtractor;

/// Sigmoid inverse: given a target probability p in (0,1), returns the bias
/// value b such that sigmoid(b) ≈ p (with zero weights).
#[inline]
fn sigmoid_inv(p: f32) -> f32 {
    (p / (1.0 - p)).ln()
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Build a MomentumBreakout ONNX graph that computes Donchian channel, ROC, ATR,
/// and 8 grid parameters — all using ONNX operators.
///
/// `w` must have `2 × {NUM_GRID_PARAMS}` elements (2 features × 8 outputs).
/// `b` must have `{NUM_GRID_PARAMS}` elements (one bias per output head).
///
/// W_pred_flat and B_pred are both graph inputs (overridable) and initializers
/// (defaults), so the ONNX model works for both training (pass current weights)
/// and inference.
///
/// # Arguments
/// * `donchian_period` — Donchian channel period (e.g. 20)
/// * `momentum_period` — ROC period (e.g. 14)
/// * `atr_period` — ATR period (e.g. 14)
/// * `window_size` — lookback candle window (e.g. 200)
/// * `w` — flat weights, `2 × {NUM_GRID_PARAMS}` elements
/// * `b` — biases, `{NUM_GRID_PARAMS}` elements
#[doc(hidden)]
pub fn build_momentum_breakout_onnx_bytes(
    donchian_period: usize,
    momentum_period: usize,
    atr_period: usize,
    window_size: usize,
    w: &[f32],
    b: &[f32],
) -> (Vec<u8>, Vec<Box<dyn Extractor>>) {
    let n_out = NUM_GRID_PARAMS;
    let n_feat = 2usize;
    assert_eq!(
        w.len(),
        n_feat * n_out,
        "w must have {n_feat} × {n_out} = {} elements",
        n_feat * n_out
    );
    assert_eq!(b.len(), n_out, "b must have {n_out} bias values");

    // ── Weight helpers ──────────────────────────────────────────────────────
    let extract_w = {
        let mut w = vec![0.0f32; window_size];
        w[window_size - 1] = 1.0;
        w
    };
    let atr_w = ema_weights(window_size, atr_period);
    let roc_prev_w = {
        let mut w = vec![0.0f32; window_size];
        let pos = window_size.saturating_sub(momentum_period + 1);
        if pos < window_size {
            w[pos] = 1.0;
        }
        w
    };
    let mask_donchian = {
        let mut mask = vec![0.0f32; window_size];
        let start = window_size.saturating_sub(donchian_period + 1);
        let end = window_size - 1; // exclude last candle
        for item in mask.iter_mut().take(end).skip(start) {
            *item = 1.0;
        }
        mask
    };
    let mask_min_complement = {
        let mut mask = vec![f32::MAX; window_size];
        let start = window_size.saturating_sub(donchian_period + 1);
        let end = window_size - 1;
        for item in mask.iter_mut().take(end).skip(start) {
            *item = 0.0;
        }
        mask
    };

    let graph = crate::onnx_graph! {
        name: "MomentumBreakout",
        window_size: window_size,
        inputs: ["closes", "highs", "lows", "prev_closes"],
        extra_inputs: [
            ("W_pred_flat", 1, (n_feat * n_out) as i64),
            ("B_pred", 1, n_out as i64)
        ],
        initializers: [
            init: "W_extract" => crate::onnx_initializer!("W_extract", &extract_w, window_size as i64, 1),
            init: "W_atr" => crate::onnx_initializer!("W_atr", &atr_w, window_size as i64, 1),
            init: "W_roc_prev" => crate::onnx_initializer!("W_roc_prev", &roc_prev_w, window_size as i64, 1),
            init: "mask_donchian" => crate::onnx_initializer!("mask_donchian", &mask_donchian, 1, window_size as i64),
            init: "mask_min_complement" => crate::onnx_initializer!("mask_min_complement", &mask_min_complement, 1, window_size as i64),
            init: "reshape_shape" => crate::onnx_initializer!("reshape_shape", &vec![n_feat as i64, n_out as i64], i64),
        ],
        nodes: [
            Reshape(["W_pred_flat", "reshape_shape"] -> ["W_pred"]),
            MatMul(["closes", "W_extract"] -> ["last_close"]),
            Mul(["highs", "mask_donchian"] -> ["masked_highs"]),
            ReduceMax(["masked_highs"] -> ["prev_hh"], axes=[1], keepdims=1),
            Add(["lows", "mask_min_complement"] -> ["masked_lows"]),
            ReduceMin(["masked_lows"] -> ["prev_ll"], axes=[1], keepdims=1),
            MatMul(["closes", "W_roc_prev"] -> ["close_roc_ago"]),
            Sub(["last_close", "close_roc_ago"] -> ["roc_diff"]),
            Div(["roc_diff", "close_roc_ago"] -> ["roc_val"]),
            Sub(["highs", "lows"] -> ["hl"]),
            Sub(["highs", "prev_closes"] -> ["hmpc"]),
            Abs(["hmpc"] -> ["ahmpc"]),
            Sub(["lows", "prev_closes"] -> ["lmpc"]),
            Abs(["lmpc"] -> ["almpc"]),
            Max(["hl", "ahmpc"] -> ["m1"]),
            Max(["m1", "almpc"] -> ["tr"]),
            MatMul(["tr", "W_atr"] -> ["atr_val"]),
            Div(["atr_val", "last_close"] -> ["atr_norm"]),
            Concat(["roc_val", "atr_norm"] -> ["features"], axis=1),
            MatMul(["features", "W_pred"] -> ["dot"]),
            Add(["dot", "B_pred"] -> ["biased"]),
            Sigmoid(["biased"] -> ["grid_params"]),
        ],
        outputs: [("grid_params", n_out as i64), ("atr_val", 1i64), ("prev_hh", 1i64), ("prev_ll", 1i64), ("roc_val", 1i64)],
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
        name: "MomentumBreakout",
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

/// Build a MomentumBreakout ONNX graph with default grid-parameter biases.
///
/// `pred_weights` must have 2 elements (one per feature).
/// `pred_bias` is the bias for market_bias output.
pub fn build_momentum_breakout_onnx_default(
    donchian_period: usize,
    momentum_period: usize,
    atr_period: usize,
    window_size: usize,
    pred_weights: &[f32],
    pred_bias: f32,
) -> (Vec<u8>, Vec<Box<dyn Extractor>>) {
    let (bytes, extractors) = build_momentum_breakout_onnx_bytes(
        donchian_period,
        momentum_period,
        atr_period,
        window_size,
        &{
            let n_out = NUM_GRID_PARAMS;
            let n_feat = 2usize;
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
