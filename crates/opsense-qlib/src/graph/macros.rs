//! Macros for building ONNX graph models with less boilerplate.
//! Moved from root macros.rs into graph/ folder.
//! These macros are #[macro_export] for crate-wide access via `crate::onnx_node!` etc.

/// Helper macro to create a ValueInfoProto for graph inputs/outputs
#[macro_export]
macro_rules! onnx_value_info {
    ($name:expr, $batch:expr, $seq:expr) => {{
        use tract_onnx::pb::*;
        ValueInfoProto {
            name: $name.into(),
            r#type: Some(TypeProto {
                denotation: String::new(),
                value: Some(type_proto::Value::TensorType(type_proto::Tensor {
                    elem_type: tensor_proto::DataType::Float as i32,
                    shape: Some(TensorShapeProto {
                        dim: vec![
                            tensor_shape_proto::Dimension {
                                denotation: String::new(),
                                value: Some(tensor_shape_proto::dimension::Value::DimValue($batch)),
                            },
                            tensor_shape_proto::Dimension {
                                denotation: String::new(),
                                value: Some(tensor_shape_proto::dimension::Value::DimValue($seq)),
                            },
                        ],
                    }),
                })),
            }),
            ..Default::default()
        }
    }};
}

/// Helper macro to create a TensorProto initializer
#[macro_export]
macro_rules! onnx_initializer {
    ($name:expr, $data:expr, $rows:expr, $cols:expr) => {{
        use tract_onnx::pb::*;
        TensorProto {
            dims: vec![$rows, $cols],
            data_type: tensor_proto::DataType::Float as i32,
            raw_data: $data.iter().flat_map(|v| v.to_le_bytes()).collect(),
            name: $name.into(),
            ..Default::default()
        }
    }};
    ($name:expr, $data:expr, i64) => {{
        use tract_onnx::pb::*;
        TensorProto {
            dims: vec![$data.len() as i64],
            data_type: tensor_proto::DataType::Int64 as i32,
            raw_data: $data.iter().flat_map(|v| v.to_le_bytes()).collect(),
            name: $name.into(),
            ..Default::default()
        }
    }};
}

/// Helper macro to create a NodeProto
#[macro_export]
macro_rules! onnx_node {
    ($op:expr, $name:expr, [$($input:expr),*] -> [$($output:expr),*]) => {{
        use tract_onnx::pb::*;
        NodeProto {
            op_type: $op.into(),
            name: $name.into(),
            input: vec![$($input.into()),*],
            output: vec![$($output.into()),*],
            ..Default::default()
        }
    }};
    ($op:expr, $name:expr, [$($input:expr),*] -> [$($output:expr),*], $($attr_name:expr => $attr_value:expr),*) => {{
        use tract_onnx::pb::*;
        let attrs = vec![$(
            AttributeProto {
                name: $attr_name.into(),
                r#type: 2, // INT
                i: $attr_value,
                ..Default::default()
            }
        ),*];
        NodeProto {
            op_type: $op.into(),
            name: $name.into(),
            input: vec![$($input.into()),*],
            output: vec![$($output.into()),*],
            attribute: attrs,
            ..Default::default()
        }
    }};
    ($op:expr, $name:expr, [$($input:expr),*] -> [$($output:expr),*], axes=$axes:expr, keepdims=$keepdims:expr) => {{
        use tract_onnx::pb::*;
        NodeProto {
            op_type: $op.into(),
            name: $name.into(),
            input: vec![$($input.into()),*],
            output: vec![$($output.into()),*],
            attribute: vec![
                AttributeProto {
                    name: "axes".into(),
                    r#type: 7, // INTS
                    ints: $axes.iter().map(|&x| x as i64).collect(),
                    ..Default::default()
                },
                AttributeProto {
                    name: "keepdims".into(),
                    r#type: 2, // INT
                    i: $keepdims as i64,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }};
}

/// Internal helper macro for adding nodes to a graph (recursive).
#[macro_export]
macro_rules! onnx_graph_nodes {
    ($graph:expr $(,)?) => {};
    ($graph:expr, MatMul([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("MatMul", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Sub([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Sub", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Add([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Add", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Mul([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Mul", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Div([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Div", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Abs([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Abs", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Max([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Max", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Min([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Min", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Concat([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*], axis=$axis:expr) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Concat", $out0, [$($inp),*] -> [$out0 $(, $outs)*], "axis" => $axis));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Reshape([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Reshape", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Sigmoid([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Sigmoid", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, ReduceMax([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*], axes=$axes:expr, keepdims=$kd:expr) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("ReduceMax", $out0, [$($inp),*] -> [$out0 $(, $outs)*], axes=$axes, keepdims=$kd));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, ReduceMin([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*], axes=$axes:expr, keepdims=$kd:expr) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("ReduceMin", $out0, [$($inp),*] -> [$out0 $(, $outs)*], axes=$axes, keepdims=$kd));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, Neg([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Neg", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
    ($graph:expr, $op:ident([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!(stringify!($op), $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
}

/// Macro to build a complete ONNX model with metadata
#[macro_export]
macro_rules! onnx_model {
    (
        name: $model_name:expr,
        ir_version: $ir_ver:expr,
        opset_version: $opset_ver:expr,
        graph: $graph:expr,
        $(metadata: [$($meta_key:expr => $meta_value:expr),*],)?
    ) => {{
        use prost::Message;
        use tract_onnx::pb::*;

        let model = ModelProto {
            ir_version: $ir_ver,
            opset_import: vec![OperatorSetIdProto {
                domain: "".into(),
                version: $opset_ver,
            }],
            graph: Some($graph),
            metadata_props: vec![
                $(
                    $(
                        StringStringEntryProto {
                            key: $meta_key.into(),
                            value: $meta_value.into(),
                        },
                    )*
                )?
            ],
            ..Default::default()
        };

        model.encode_to_vec()
    }};
}

/// Convenience macro for creating EMA indicator subgraph
#[macro_export]
macro_rules! ema_indicator {
    ($graph:expr, $input:expr, $weight_name:expr, $output:expr, $window_size:expr, $period:expr) => {{
        let ema_w = $crate::graph::ctx::ema_weights($window_size, $period);
        $graph.initializer.push($crate::onnx_initializer!($weight_name, &ema_w, $window_size as i64, 1));
        $graph.node.push($crate::onnx_node!("MatMul", concat!("ema_", $period), [$input, $weight_name] -> [$output]));
    }};
}

/// Convenience macro for creating ATR indicator subgraph
#[macro_export]
macro_rules! atr_indicator {
    (
        $graph:expr,
        highs: $highs:expr,
        lows: $lows:expr,
        prev_closes: $prev_closes:expr,
        weight_name: $w_name:expr,
        output: $out:expr,
        window_size: $ws:expr,
        period: $period:expr
    ) => {{
        let atr_w = $crate::graph::ctx::ema_weights($ws, $period);
        $graph.initializer.push($crate::onnx_initializer!($w_name, &atr_w, $ws as i64, 1));
        $graph.node.push($crate::onnx_node!("Sub", "hl_sub", [$highs, $lows] -> ["hl"]));
        $graph.node.push($crate::onnx_node!("Sub", "hmpc_sub", [$highs, $prev_closes] -> ["hmpc"]));
        $graph.node.push($crate::onnx_node!("Abs", "ahmpc_abs", ["hmpc"] -> ["ahmpc"]));
        $graph.node.push($crate::onnx_node!("Sub", "lmpc_sub", [$lows, $prev_closes] -> ["lmpc"]));
        $graph.node.push($crate::onnx_node!("Abs", "almpc_abs", ["lmpc"] -> ["almpc"]));
        $graph.node.push($crate::onnx_node!("Max", "max1", ["hl", "ahmpc"] -> ["m1"]));
        $graph.node.push($crate::onnx_node!("Max", "tr_max", ["m1", "almpc"] -> ["tr"]));
        $graph.node.push($crate::onnx_node!("MatMul", "atr_matmul", ["tr", $w_name] -> [$out]));
    }};
}

/// Convenience macro for creating RSI indicator subgraph
#[macro_export]
macro_rules! rsi_indicator {
    (
        $graph:expr,
        closes: $closes:expr,
        prev_closes: $prev_closes:expr,
        gain_weight: $gain_w:expr,
        loss_weight: $loss_w:expr,
        output: $out:expr,
        window_size: $ws:expr,
        period: $period:expr
    ) => {{
        let gain_weights = $crate::graph::ctx::rsi_weights($ws, $period);
        let loss_weights = $crate::graph::ctx::rsi_weights($ws, $period);
        $graph.initializer.push($crate::onnx_initializer!($gain_w, &gain_weights, $ws as i64, 1));
        $graph.initializer.push($crate::onnx_initializer!($loss_w, &loss_weights, $ws as i64, 1));
        $graph.node.push($crate::onnx_node!("Sub", "rsi_diff", [$closes, $prev_closes] -> ["diff"]));
        $graph.node.push($crate::onnx_node!("Clip", "gains_clip", ["diff"] -> ["gains"], "min" => 0.0f32.to_bits() as i64));
        $graph.node.push($crate::onnx_node!("Neg", "neg_diff", ["diff"] -> ["neg_diff"]));
        $graph.node.push($crate::onnx_node!("Clip", "losses_clip", ["neg_diff"] -> ["losses"], "min" => 0.0f32.to_bits() as i64));
        $graph.node.push($crate::onnx_node!("MatMul", "avg_gain", ["gains", $gain_w] -> ["avg_gain"]));
        $graph.node.push($crate::onnx_node!("MatMul", "avg_loss", ["losses", $loss_w] -> ["avg_loss"]));
        $graph.node.push($crate::onnx_node!("Div", "rs_div", ["avg_gain", "avg_loss"] -> ["rs"]));
        $graph.node.push($crate::onnx_node!("Add", "rs_plus_one", ["rs"] -> ["rs_plus_one"], "value" => 1.0f32.to_bits() as i64));
        $graph.node.push($crate::onnx_node!("Div", "rsi_div", ["100"] -> ["rsi_ratio"], "value_b" => "rs_plus_one"));
        $graph.node.push($crate::onnx_node!("Sub", "rsi_sub", ["100"] -> [$out], "value_b" => "rsi_ratio"));
    }};
}

/// Convenience macro for creating OBV indicator
#[macro_export]
macro_rules! obv_indicator {
    ($graph:expr, closes: $closes:expr, prev_closes: $prev_closes:expr, volumes: $volumes:expr, output: $out:expr) => {{
        $graph.node.push($crate::onnx_node!("Sub", "obv_diff", [$closes, $prev_closes] -> ["obv_diff"]));
        $graph.node.push($crate::onnx_node!("Sign", "obv_sign", ["obv_diff"] -> ["direction"]));
        $graph.node.push($crate::onnx_node!("Mul", "obv_flow", ["direction", $volumes] -> ["obv_flow"]));
        $graph.node.push($crate::onnx_node!("Identity", "obv_output", ["obv_flow"] -> [$out]));
    }};
}

/// Convenience macro for creating prediction layer (MatMul + Add + Sigmoid)
#[macro_export]
macro_rules! prediction_layer {
    ($graph:expr, features: $features:expr, weights_input: $w_inp:expr, bias_input: $b_inp:expr, reshape_shape: $shape_name:expr, output: $out:expr, n_features: $n_feat:expr, n_outputs: $n_out:expr) => {{
        $graph.node.push($crate::onnx_node!("Reshape", "w_reshape", [$w_inp, $shape_name] -> ["W_pred"]));
        $graph.node.push($crate::onnx_node!("MatMul", "pred_matmul", [$features, "W_pred"] -> ["dot"]));
        $graph.node.push($crate::onnx_node!("Add", "pred_add", ["dot", $b_inp] -> ["biased"]));
        $graph.node.push($crate::onnx_node!("Sigmoid", "pred_sigmoid", ["biased"] -> [$out]));
    }};
}
