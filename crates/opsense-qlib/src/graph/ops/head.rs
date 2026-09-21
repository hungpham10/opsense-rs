use std::io::Error;

use crate::graph::OnnxEmitCtx;
use crate::graph::Op;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Head {
    pub n_feat: usize,
    pub n_out: usize,
}

#[typetag::serde(name = "head")]
impl Op for Head {
    fn emit(
        &self,
        _ctx: &mut OnnxEmitCtx,
        _inputs: &[String],
        _outputs: &[String],
    ) -> Result<(), Error> {
        // Head is handled specially in Graph::build_onnx().
        Ok(())
    }

    fn head_features(&self) -> Option<usize> {
        Some(self.n_feat)
    }

    fn head_n_out(&self) -> usize {
        self.n_out
    }
}
