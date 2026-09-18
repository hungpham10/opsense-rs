use std::io::Error;

use crate::graph::Op;
use crate::graph::OnnxEmitCtx;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Abs;

#[typetag::serde(name = "abs")]
impl Op for Abs {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Abs", format!("abs_{}", outputs[0]), [inputs[0].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Neg;

#[typetag::serde(name = "neg")]
impl Op for Neg {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Neg", format!("neg_{}", outputs[0]), [inputs[0].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sigmoid;

#[typetag::serde(name = "sigmoid")]
impl Op for Sigmoid {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Sigmoid", format!("sig_{}", outputs[0]), [inputs[0].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}
