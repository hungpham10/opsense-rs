//! TrendFollower ONNX graph builder.
//!
//! The graph computes EMA fast, EMA slow, ATR, and market bias from raw price
//! data — all using ONNX operators (via MatMul with pre-computed decay weights).
//!
//! Output 0: `grid_params` [1, 8] — market_bias + grid probability parameters
//! Output 1: `atr_val` [1, 1] — ATR value

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

/// Build a TrendFollower ONNX graph that computes EMA fast, EMA slow, ATR,
/// and 8 grid parameters from raw price data — all using ONNX operators.
///
/// `w` must have `4 × {NUM_GRID_PARAMS}` elements (4 features × 8 outputs).
/// `b` must have `{NUM_GRID_PARAMS}` elements (one bias per output head).
///
/// W_pred and B_pred are graph inputs (not initializers) so the ONNX model
/// can be used for both training (pass current SGD weights) and inference.
///
/// # Arguments
/// * `ema_fast_period` — fast EMA period (e.g. 9)
/// * `ema_slow_period` — slow EMA period (e.g. 21)
/// * `atr_period` — ATR period (e.g. 14)
/// * `window_size` — lookback candle window (e.g. 200)
/// * `w` — flat weights, `4 × {NUM_GRID_PARAMS}` elements
/// * `b` — biases, `{NUM_GRID_PARAMS}` elements
#[doc(hidden)]
pub fn build_trend_follower_onnx_bytes(
    ema_fast_period: usize,
    ema_slow_period: usize,
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

    let fast_w = ema_weights(window_size, ema_fast_period);
    let slow_w = ema_weights(window_size, ema_slow_period);
    let atr_w = ema_weights(window_size, atr_period);

    let graph = crate::onnx_graph! {
        name: "TrendFollower",
        window_size: window_size,
        inputs: ["closes", "highs", "lows", "prev_closes"],
        extra_inputs: [
            ("W_pred_flat", 1, (n_feat * n_out) as i64),
            ("B_pred", 1, n_out as i64)
        ],
        initializers: [
            init: "W_ema_fast" => crate::onnx_initializer!("W_ema_fast", &fast_w, window_size as i64, 1),
            init: "W_ema_slow" => crate::onnx_initializer!("W_ema_slow", &slow_w, window_size as i64, 1),
            init: "W_atr" => crate::onnx_initializer!("W_atr", &atr_w, window_size as i64, 1),
            init: "reshape_shape" => crate::onnx_initializer!("reshape_shape", &vec![n_feat as i64, n_out as i64], i64),
        ],
        nodes: [
            Reshape(["W_pred_flat", "reshape_shape"] -> ["W_pred"]),
            MatMul(["closes", "W_ema_fast"] -> ["ema_fast"]),
            MatMul(["closes", "W_ema_slow"] -> ["ema_slow"]),
            Sub(["highs", "lows"] -> ["hl"]),
            Sub(["highs", "prev_closes"] -> ["hmpc"]),
            Abs(["hmpc"] -> ["ahmpc"]),
            Sub(["lows", "prev_closes"] -> ["lmpc"]),
            Abs(["lmpc"] -> ["almpc"]),
            Max(["hl", "ahmpc"] -> ["m1"]),
            Max(["m1", "almpc"] -> ["tr"]),
            MatMul(["tr", "W_atr"] -> ["atr_val"]),
            Sub(["ema_fast", "ema_slow"] -> ["diff"]),
            Concat(["ema_fast", "ema_slow", "atr_val", "diff"] -> ["features"], axis=1),
            MatMul(["features", "W_pred"] -> ["dot"]),
            Add(["dot", "B_pred"] -> ["biased"]),
            Sigmoid(["biased"] -> ["grid_params"]),
        ],
        outputs: [("grid_params", n_out as i64), ("atr_val", 1i64)],
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
        name: "TrendFollower",
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

/// Build a TrendFollower ONNX graph with default grid-parameter biases.
///
/// `pred_weights` must have 4 elements (one per feature).
/// `pred_bias` is the bias for market_bias output.
/// Grid-parameter weights are zero; their biases produce classic hardcoded defaults.
pub fn build_trend_follower_onnx_default(
    ema_fast_period: usize,
    ema_slow_period: usize,
    atr_period: usize,
    window_size: usize,
    pred_weights: &[f32],
    pred_bias: f32,
) -> (Vec<u8>, Vec<Box<dyn Extractor>>) {
    let (bytes, extractors) = build_trend_follower_onnx_bytes(
        ema_fast_period,
        ema_slow_period,
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
