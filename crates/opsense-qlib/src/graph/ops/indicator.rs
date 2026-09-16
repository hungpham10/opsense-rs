use std::io::Error;

use crate::graph::Op;
use crate::graph::OnnxEmitCtx;
use serde::{Deserialize, Serialize};

// ── Last ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Last;

#[typetag::serde(name = "last")]
impl Op for Last {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let w = extract_w(ctx.window_size());
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_last_{}", outputs[0]),
            &w,
            ctx.window_size() as i64,
            1
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("mm_last_{}", outputs[0]),
            [inputs[0].clone(), format!("w_last_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        Ok(())
    }
}

fn extract_w(ws: usize) -> Vec<f32> {
    let mut w = vec![0.0f32; ws];
    if ws > 0 {
        w[ws - 1] = 1.0;
    }
    w
}

// ── Roc ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Roc {
    pub period: usize,
}

#[typetag::serde(name = "roc")]
impl Op for Roc {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let ws = ctx.window_size();
        let we = extract_w(ws);
        let wr = roc_prev_w(ws, self.period);
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_last_{}", outputs[0]),
            &we,
            ws as i64,
            1
        ));
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_roc_{}", outputs[0]),
            &wr,
            ws as i64,
            1
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("rl_{}", outputs[0]),
            [inputs[0].clone(), format!("w_last_{}", outputs[0])] -> [format!("rl_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("ra_{}", outputs[0]),
            [inputs[0].clone(), format!("w_roc_{}", outputs[0])] -> [format!("ra_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Sub",
            format!("rd_{}", outputs[0]),
            [format!("rl_{}", outputs[0]), format!("ra_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        ctx.push_node(crate::onnx_node!(
            "Div",
            format!("rdiv_{}", outputs[0]),
            [format!("rd_{}", outputs[0]), format!("ra_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        Ok(())
    }
}

fn roc_prev_w(ws: usize, period: usize) -> Vec<f32> {
    let mut w = vec![0.0f32; ws];
    if ws > 0 && period < ws {
        w[ws - 1 - period] = 1.0;
    }
    w
}

// ── Ema ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Ema {
    pub period: usize,
}

#[typetag::serde(name = "ema")]
impl Op for Ema {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let w = ctx.ema_weights(self.period);
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_ema_{}", outputs[0]),
            &w,
            ctx.window_size() as i64,
            1
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("ema_{}", outputs[0]),
            [inputs[0].clone(), format!("w_ema_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        Ok(())
    }
}

// ── Ma ────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Ma {
    pub period: usize,
}

#[typetag::serde(name = "ma")]
impl Op for Ma {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let w = ctx.sma_weights(self.period);
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_ma_{}", outputs[0]),
            &w,
            ctx.window_size() as i64,
            1
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("ma_{}", outputs[0]),
            [inputs[0].clone(), format!("w_ma_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        Ok(())
    }
}

// ── Rsi ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Rsi {
    pub period: usize,
}

#[typetag::serde(name = "rsi")]
impl Op for Rsi {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let ws = ctx.window_size();
        let w = ctx.rsi_weights(self.period);
        let zero = vec![0.0f32; ws];
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_rsi_{}", outputs[0]),
            &w,
            ws as i64,
            1
        ));
        ctx.push_initializer(crate::onnx_initializer!(
            format!("zero_{}", outputs[0]),
            &zero,
            1,
            ws as i64
        ));
        let closes = &inputs[0];
        let prev = &inputs[1];
        ctx.push_node(crate::onnx_node!(
            "Sub",
            format!("rsi_diff_{}", outputs[0]),
            [closes.clone(), prev.clone()] -> [format!("rsi_diff_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Max",
            format!("rsi_gains_{}", outputs[0]),
            [format!("rsi_diff_{}", outputs[0]), format!("zero_{}", outputs[0])] -> [format!("rsi_gains_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Neg",
            format!("rsi_neg_{}", outputs[0]),
            [format!("rsi_diff_{}", outputs[0])] -> [format!("rsi_neg_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Max",
            format!("rsi_loss_{}", outputs[0]),
            [format!("rsi_neg_{}", outputs[0]), format!("zero_{}", outputs[0])] -> [format!("rsi_loss_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("rsi_ag_{}", outputs[0]),
            [format!("rsi_gains_{}", outputs[0]), format!("w_rsi_{}", outputs[0])] -> [format!("rsi_ag_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("rsi_al_{}", outputs[0]),
            [format!("rsi_loss_{}", outputs[0]), format!("w_rsi_{}", outputs[0])] -> [format!("rsi_al_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Add",
            format!("rsi_den_{}", outputs[0]),
            [format!("rsi_ag_{}", outputs[0]), format!("rsi_al_{}", outputs[0])] -> [format!("rsi_den_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Div",
            format!("rsi_raw_{}", outputs[0]),
            [format!("rsi_ag_{}", outputs[0]), format!("rsi_den_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        Ok(())
    }
}

// ── Atr ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Atr {
    pub period: usize,
}

#[typetag::serde(name = "atr")]
impl Op for Atr {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let w = ctx.ema_weights(self.period);
        ctx.push_initializer(crate::onnx_initializer!(
            format!("w_atr_{}", outputs[0]),
            &w,
            ctx.window_size() as i64,
            1
        ));
        ctx.push_node(crate::onnx_node!(
            "Sub",
            format!("ahl_{}", outputs[0]),
            [inputs[0].clone(), inputs[1].clone()] -> [format!("ahl_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Sub",
            format!("ahmpc_{}", outputs[0]),
            [inputs[0].clone(), inputs[2].clone()] -> [format!("ahmpc_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Abs",
            format!("aahmpc_{}", outputs[0]),
            [format!("ahmpc_{}", outputs[0])] -> [format!("aahmpc_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Sub",
            format!("almpc_{}", outputs[0]),
            [inputs[1].clone(), inputs[2].clone()] -> [format!("almpc_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Abs",
            format!("aalmpc_{}", outputs[0]),
            [format!("almpc_{}", outputs[0])] -> [format!("aalmpc_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Max",
            format!("am1_{}", outputs[0]),
            [format!("ahl_{}", outputs[0]), format!("aahmpc_{}", outputs[0])] -> [format!("am1_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "Max",
            format!("atr_tr_{}", outputs[0]),
            [format!("am1_{}", outputs[0]), format!("aalmpc_{}", outputs[0])] -> [format!("atr_tr_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "MatMul",
            format!("amm_{}", outputs[0]),
            [format!("atr_tr_{}", outputs[0]), format!("w_atr_{}", outputs[0])] -> [outputs[0].clone()]
        ));
        Ok(())
    }

    fn is_atr(&self) -> bool {
        true
    }
}

// ── DonchianHigh ──────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct DonchianHigh {
    pub period: usize,
}

#[typetag::serde(name = "donchian_high")]
impl Op for DonchianHigh {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let ws = ctx.window_size();
        let m = mask_donchian(ws, self.period);
        ctx.push_initializer(crate::onnx_initializer!(
            format!("mh_{}", outputs[0]),
            &m,
            1,
            ws as i64
        ));
        ctx.push_node(crate::onnx_node!(
            "Mul",
            format!("mhm_{}", outputs[0]),
            [inputs[0].clone(), format!("mh_{}", outputs[0])] -> [format!("mhm_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "ReduceMax",
            format!("rmax_{}", outputs[0]),
            [format!("mhm_{}", outputs[0])] -> [outputs[0].clone()],
            axes=[1i64], keepdims=1
        ));
        Ok(())
    }
}

fn mask_donchian(ws: usize, period: usize) -> Vec<f32> {
    let mut m = vec![0.0f32; ws];
    for v in &mut m[ws.saturating_sub(period)..] {
        *v = 1.0;
    }
    m
}

// ── DonchianLow ───────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct DonchianLow {
    pub period: usize,
}

#[typetag::serde(name = "donchian_low")]
impl Op for DonchianLow {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let ws = ctx.window_size();
        let m = mask_min_complement(ws, self.period);
        ctx.push_initializer(crate::onnx_initializer!(
            format!("ml_{}", outputs[0]),
            &m,
            1,
            ws as i64
        ));
        ctx.push_node(crate::onnx_node!(
            "Add",
            format!("mlm_{}", outputs[0]),
            [inputs[0].clone(), format!("ml_{}", outputs[0])] -> [format!("mlm_{}", outputs[0])]
        ));
        ctx.push_node(crate::onnx_node!(
            "ReduceMin",
            format!("rmin_{}", outputs[0]),
            [format!("mlm_{}", outputs[0])] -> [outputs[0].clone()],
            axes=[1i64], keepdims=1
        ));
        Ok(())
    }
}

fn mask_min_complement(ws: usize, period: usize) -> Vec<f32> {
    let mut m = vec![1e9f32; ws];
    for v in &mut m[ws.saturating_sub(period)..] {
        *v = 0.0;
    }
    m
}
