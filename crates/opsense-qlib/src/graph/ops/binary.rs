use std::io::Error;

use crate::graph::OnnxEmitCtx;
use crate::graph::Op;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Add;

#[typetag::serde(name = "add")]
impl Op for Add {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Add", format!("add_{}", outputs[0]), [inputs[0].clone(), inputs[1].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sub;

#[typetag::serde(name = "sub")]
impl Op for Sub {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Sub", format!("sub_{}", outputs[0]), [inputs[0].clone(), inputs[1].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Mul;

#[typetag::serde(name = "mul")]
impl Op for Mul {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Mul", format!("mul_{}", outputs[0]), [inputs[0].clone(), inputs[1].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Div;

#[typetag::serde(name = "div")]
impl Op for Div {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Div", format!("div_{}", outputs[0]), [inputs[0].clone(), inputs[1].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Max;

#[typetag::serde(name = "max")]
impl Op for Max {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Max", format!("max_{}", outputs[0]), [inputs[0].clone(), inputs[1].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Min;

#[typetag::serde(name = "min")]
impl Op for Min {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        ctx.push_node(crate::onnx_node!("Min", format!("min_{}", outputs[0]), [inputs[0].clone(), inputs[1].clone()] -> [outputs[0].clone()]));
        Ok(())
    }
}
