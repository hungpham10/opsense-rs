//! OnnxEmitCtx — helper context for emitting ONNX nodes from Op trait objects.
//! Also contains weight helper functions (moved from models/mod.rs).

use tract_onnx::pb::{GraphProto, NodeProto, TensorProto};

/// Context cho việc emit ONNX nodes và initializers từ các Op trait object.
pub struct OnnxEmitCtx<'a> {
    pub graph: &'a mut GraphProto,
    window_size: usize,
}

impl<'a> OnnxEmitCtx<'a> {
    pub fn new(graph: &'a mut GraphProto, window_size: usize) -> Self {
        Self { graph, window_size }
    }

    pub fn push_node(&mut self, node: NodeProto) {
        self.graph.node.push(node);
    }

    pub fn push_initializer(&mut self, init: TensorProto) {
        self.graph.initializer.push(init);
    }

    pub fn window_size(&self) -> usize {
        self.window_size
    }

    pub fn ema_weights(&self, period: usize) -> Vec<f32> {
        ema_weights(self.window_size, period)
    }

    pub fn sma_weights(&self, period: usize) -> Vec<f32> {
        sma_weights(self.window_size, period)
    }

    pub fn rsi_weights(&self, period: usize) -> Vec<f32> {
        rsi_weights(self.window_size, period)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Weight helper functions (moved from models/mod.rs)
// ═══════════════════════════════════════════════════════════════════════

/// Pre-compute EMA decay weights.
pub fn ema_weights(window_size: usize, period: usize) -> Vec<f32> {
    if period == 0 {
        return vec![0.0; window_size];
    }
    let k = 2.0 / (period as f32 + 1.0);
    let one_minus_k = 1.0 - k;
    let mut weights = vec![0.0f32; window_size];
    for (i, weight) in weights.iter_mut().enumerate().take(window_size) {
        let exp = (window_size - 1 - i) as i32;
        *weight = k * one_minus_k.powi(exp);
    }
    weights
}

/// Pre-compute SMA weights.
pub fn sma_weights(window_size: usize, period: usize) -> Vec<f32> {
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
pub fn rsi_weights(window_size: usize, period: usize) -> Vec<f32> {
    if period == 0 {
        return vec![0.0; window_size];
    }
    let n_real = window_size.saturating_sub(1);
    if n_real == 0 {
        return vec![0.0; window_size];
    }
    let k = 1.0 / period as f32;
    let one_minus_k = 1.0 - k;
    let mut weights = vec![0.0f32; window_size];

    if n_real <= period {
        let w = 1.0 / n_real as f32;
        for weight in weights.iter_mut().take(n_real + 1).skip(1) {
            *weight = w;
        }
    } else {
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
