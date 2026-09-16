use std::io::Error;

use tract_onnx::pb::{AttributeProto, NodeProto};

use crate::graph::Op;
use crate::graph::OnnxEmitCtx;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Concat {
    pub axis: i64,
}

#[typetag::serde(name = "concat")]
impl Op for Concat {
    fn emit(
        &self,
        ctx: &mut OnnxEmitCtx,
        inputs: &[String],
        outputs: &[String],
    ) -> Result<(), Error> {
        let attrs = vec![AttributeProto {
            name: "axis".into(),
            r#type: 2, // INT
            i: self.axis,
            ..Default::default()
        }];
        ctx.push_node(NodeProto {
            op_type: "Concat".into(),
            name: format!("concat_{}", outputs[0]),
            input: inputs.to_vec(),
            output: vec![outputs[0].clone()],
            attribute: attrs,
            ..Default::default()
        });
        Ok(())
    }
}
