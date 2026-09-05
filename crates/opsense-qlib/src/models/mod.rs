//! Model implementations — each ONNX graph builder + its rebuild logic lives in its own module.
//!
//! Shared indicator-weight helpers are defined here and imported by the builder modules.

pub mod mean_reversion;
pub mod momentum_breakout;
pub mod trend_follower;

pub use mean_reversion::build_mean_reversion_onnx_bytes;
pub use mean_reversion::build_mean_reversion_onnx_default;
pub use momentum_breakout::build_momentum_breakout_onnx_bytes;
pub use momentum_breakout::build_momentum_breakout_onnx_default;
pub use trend_follower::build_trend_follower_onnx_bytes;
pub use trend_follower::build_trend_follower_onnx_default;

/// Number of grid parameters output by every model.
pub const NUM_GRID_PARAMS: usize = 8;

// ── Shared indicator-weight helpers ─────────────────────────────────────────────

/// Pre-compute EMA decay weights for a window of given size and period.
#[inline]
pub(crate) fn ema_weights(window_size: usize, period: usize) -> Vec<f32> {
    if period == 0 {
        return vec![0.0; window_size];
    }
    let k = 2.0 / (period as f32 + 1.0);
    let one_minus_k = 1.0 - k;
    let mut weights = vec![0.0f32; window_size];
    for (i, weight) in weights.iter_mut().enumerate().take(window_size) {
        // i=0 → oldest (least weight), i=window_size-1 → newest (most weight)
        let exp = (window_size - 1 - i) as i32;
        *weight = k * one_minus_k.powi(exp);
    }
    weights
}

/// Pre-compute SMA weights: last `period` entries = 1/period, rest = 0.
#[inline]
pub(crate) fn sma_weights(window_size: usize, period: usize) -> Vec<f32> {
    if period == 0 {
        return vec![0.0; window_size];
    }
    let mut weights = vec![0.0f32; window_size];
    let start = window_size.saturating_sub(period);
    for weight in weights.iter_mut().take(window_size).skip(start) {
        *weight = 1.0 / period as f32;
    }
    weights
}

/// Pre-compute Wilder's RSI smoothing weights.
///
/// The first `period` gains/losses are simple-averaged, then EMA-smoothed
/// with k = 1/period.  diffs[0] is always 0 (from Sub(closes, prev_closes)),
/// so weight[0] = 0.
#[inline]
pub(crate) fn rsi_weights(window_size: usize, period: usize) -> Vec<f32> {
    if period == 0 {
        return vec![0.0; window_size];
    }
    let n_real = window_size.saturating_sub(1); // real diffs are at indices [1, window_size-1]
    if n_real == 0 {
        return vec![0.0; window_size];
    }
    let k = 1.0 / period as f32;
    let one_minus_k = 1.0 - k;
    let mut weights = vec![0.0f32; window_size]; // weight[0] stays 0

    if n_real <= period {
        // Only SMA, no EMA steps
        let w = 1.0 / n_real as f32;
        for weight in weights.iter_mut().take(n_real + 1).skip(1) {
            *weight = w;
        }
    } else {
        // SMA initialization + EMA smoothing
        let sma_part = (1.0 / period as f32) * one_minus_k.powi((n_real - period) as i32);
        for weight in weights.iter_mut().take(period + 1).skip(1) {
            *weight = sma_part;
        }
        for (i, weight) in weights
            .iter_mut()
            .enumerate()
            .take(n_real + 1)
            .skip(period + 1)
        {
            let exp = (n_real - i) as i32;
            *weight = k * one_minus_k.powi(exp);
        }
    }
    weights
}
