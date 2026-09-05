//! Macros for building ONNX graph models with less boilerplate.
//!
//! This module provides declarative macros to simplify the construction of ONNX graphs
//! for quantitative trading indicators. The macros handle repetitive tasks like:
//! - Defining graph inputs with consistent tensor shapes
//! - Creating initializers (weights, biases, masks)
//! - Adding nodes with proper attribute handling
//! - Declaring outputs with standardized shapes
//!
//! # Example
//! ```rust,ignore
//! onnx_graph! {
//!     name: "MyModel",
//!     window_size: window_size,
//!     inputs: ["closes", "highs", "lows", "prev_closes"],
//!     initializers: [
//!         weight: "W_ema" => ema_weights(window_size, period),
//!         scalar: "ones" => vec![1.0f32; window_size],
//!     ],
//!     nodes: [
//!         MatMul(["closes", "W_ema"] -> "ema_val"),
//!         Sub(["highs", "lows"] -> "hl_range"),
//!     ],
//!     outputs: ["ema_val", "hl_range"],
//! }
//! ```

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
    // Weight matrix: name, data, rows, cols
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
    // Int64 vector (e.g., reshape shape): name, data
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
    // Simple node without attributes
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
    // Node with attributes
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
    // Node with axes attribute (for Reduce ops)
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

/// Internal helper macro for adding nodes to a graph.
///
/// **Node names** are derived from the **first output tensor name** so that
/// every node in the graph has a unique name (tract / ONNX requirement).
#[macro_export]
macro_rules! onnx_graph_nodes {
    // Base case: no more nodes
    ($graph:expr $(,)?) => {};

    // MatMul node
    ($graph:expr, MatMul([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("MatMul", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Sub node
    ($graph:expr, Sub([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Sub", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Add node
    ($graph:expr, Add([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Add", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Mul node
    ($graph:expr, Mul([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Mul", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Div node
    ($graph:expr, Div([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Div", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Abs node
    ($graph:expr, Abs([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Abs", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Max node
    ($graph:expr, Max([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Max", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Min node
    ($graph:expr, Min([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Min", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Concat node with axis attribute
    ($graph:expr, Concat([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*], axis=$axis:expr) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Concat", $out0, [$($inp),*] -> [$out0 $(, $outs)*], "axis" => $axis));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Reshape node
    ($graph:expr, Reshape([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Reshape", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Sigmoid node
    ($graph:expr, Sigmoid([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Sigmoid", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // ReduceMax node with axes and keepdims
    ($graph:expr, ReduceMax([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*], axes=$axes:expr, keepdims=$kd:expr) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("ReduceMax", $out0, [$($inp),*] -> [$out0 $(, $outs)*], axes=$axes, keepdims=$kd));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // ReduceMin node with axes and keepdims
    ($graph:expr, ReduceMin([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*], axes=$axes:expr, keepdims=$kd:expr) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("ReduceMin", $out0, [$($inp),*] -> [$out0 $(, $outs)*], axes=$axes, keepdims=$kd));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Neg node
    ($graph:expr, Neg([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!("Neg", $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };

    // Catch-all for custom nodes with explicit op type
    ($graph:expr, $op:ident([$($inp:expr),*] -> [$out0:expr $(, $outs:expr)*]) $(, $($rest:tt)*)?) => {
        $graph.node.push($crate::onnx_node!(stringify!($op), $out0, [$($inp),*] -> [$out0 $(, $outs)*]));
        $crate::onnx_graph_nodes!($graph, $($($rest)*)?);
    };
}

/// Main macro for building an ONNX graph with minimal boilerplate.
///
/// # Syntax
/// ```ignore
/// onnx_graph! {
///     name: "GraphName",
///     window_size: $window_size_expr,
///     inputs: ["input1", "input2", ...],
///     extra_inputs: [
///         ("W_pred_flat", batch_size, feature_size),
///         ("B_pred", batch_size, output_size),
///     ],
///     initializers: [
///         weight: "W_name" => weight_vector,
///         int64: "shape_name" => vec![dim1, dim2],
///     ],
///     nodes: [
///         OpType([inputs] -> [outputs]),
///         OpType([inputs] -> [outputs], attr_name => attr_value),
///         OpType([inputs] -> [outputs], axes=[...], keepdims=1),
///     ],
///     outputs: [("output1", size1), ("output2", size2), ...],
/// }
/// ```
#[macro_export]
macro_rules! onnx_graph {
    (
        name: $graph_name:expr,
        window_size: $window_size:expr,
        $(inputs: $inputs:expr,)?
        $(extra_inputs: [$(($ei_name:expr, $ei_batch:expr, $ei_dim:expr)),*],)?
        $(initializers: [$(init: $init_name:expr => $init_data:expr,)*],)?
        nodes: [$($node:tt)*],
        outputs: [$(($out_name:expr, $out_size:expr)),*],
    ) => {{
        use tract_onnx::pb::*;

        let mut graph = GraphProto::default();
        graph.name = $graph_name.to_string();

        // Add standard inputs (closes, highs, lows, prev_closes, etc.)
        $(
            for input_name in $inputs {
                graph.input.push($crate::onnx_value_info!(input_name, 1, $window_size as i64));
            }
        )?

        // Add extra inputs (e.g., W_pred_flat, B_pred)
        $(
            $(
                graph.input.push($crate::onnx_value_info!($ei_name, $ei_batch, $ei_dim as i64));
            )*
        )?

        // Add initializers
        $(
            $(
                graph.initializer.push($init_data);
            )*
        )?

        // Add nodes
        $crate::onnx_graph_nodes!(graph, $($node)*);

        // Add outputs
        $(
            graph.output.push($crate::onnx_value_info!($out_name, 1, $out_size));
        )*

        graph
    }};
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
        let ema_w = $crate::qlib::models::ema_weights($window_size, $period);
        $graph.initializer.push(onnx_initializer!($weight_name, &ema_w, $window_size as i64, 1));
        $graph.node.push(onnx_node!("MatMul", concat!("ema_", $period), [$input, $weight_name] -> [$output]));
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
        let atr_w = $crate::qlib::models::ema_weights($ws, $period);
        $graph.initializer.push(onnx_initializer!($w_name, &atr_w, $ws as i64, 1));

        // Sub(highs, lows) -> hl
        $graph.node.push(onnx_node!("Sub", "hl_sub", [$highs, $lows] -> ["hl"]));
        // Sub(highs, prev_closes) -> hmpc
        $graph.node.push(onnx_node!("Sub", "hmpc_sub", [$highs, $prev_closes] -> ["hmpc"]));
        // Abs(hmpc) -> ahmpc
        $graph.node.push(onnx_node!("Abs", "ahmpc_abs", ["hmpc"] -> ["ahmpc"]));
        // Sub(lows, prev_closes) -> lmpc
        $graph.node.push(onnx_node!("Sub", "lmpc_sub", [$lows, $prev_closes] -> ["lmpc"]));
        // Abs(lmpc) -> almpc
        $graph.node.push(onnx_node!("Abs", "almpc_abs", ["lmpc"] -> ["almpc"]));
        // Max(hl, ahmpc) -> m1
        $graph.node.push(onnx_node!("Max", "max1", ["hl", "ahmpc"] -> ["m1"]));
        // Max(m1, almpc) -> tr
        $graph.node.push(onnx_node!("Max", "tr_max", ["m1", "almpc"] -> ["tr"]));
        // MatMul(tr, W_atr) -> atr_output
        $graph.node.push(onnx_node!("MatMul", "atr_matmul", ["tr", $w_name] -> [$out]));
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
        let gain_weights = $crate::qlib::models::rsi_weights($ws, $period);
        let loss_weights = $crate::qlib::models::rsi_weights($ws, $period);

        $graph.initializer.push(onnx_initializer!($gain_w, &gain_weights, $ws as i64, 1));
        $graph.initializer.push(onnx_initializer!($loss_w, &loss_weights, $ws as i64, 1));

        // Sub(closes, prev_closes) -> diff
        $graph.node.push(onnx_node!("Sub", "rsi_diff", [$closes, $prev_closes] -> ["diff"]));
        // Clip(diff, 0, inf) -> gains (only positive)
        $graph.node.push(onnx_node!("Clip", "gains_clip", ["diff"] -> ["gains"], "min" => 0.0f32.to_bits() as i64));
        // Clip(-diff, 0, inf) -> losses (only negative, made positive)
        $graph.node.push(onnx_node!("Neg", "neg_diff", ["diff"] -> ["neg_diff"]));
        $graph.node.push(onnx_node!("Clip", "losses_clip", ["neg_diff"] -> ["losses"], "min" => 0.0f32.to_bits() as i64));
        // MatMul(gains, W_gain) -> avg_gain
        $graph.node.push(onnx_node!("MatMul", "avg_gain", ["gains", $gain_w] -> ["avg_gain"]));
        // MatMul(losses, W_loss) -> avg_loss
        $graph.node.push(onnx_node!("MatMul", "avg_loss", ["losses", $loss_w] -> ["avg_loss"]));
        // RS = avg_gain / avg_loss
        $graph.node.push(onnx_node!("Div", "rs_div", ["avg_gain", "avg_loss"] -> ["rs"]));
        // RSI = 100 - 100/(1+RS)
        $graph.node.push(onnx_node!("Add", "rs_plus_one", ["rs"] -> ["rs_plus_one"], "value" => 1.0f32.to_bits() as i64));
        $graph.node.push(onnx_node!("Div", "rsi_div", ["100"] -> ["rsi_ratio"], "value_b" => "rs_plus_one"));
        $graph.node.push(onnx_node!("Sub", "rsi_sub", ["100"] -> [$out], "value_b" => "rsi_ratio"));
    }};
}

/// Convenience macro for creating OBV (On-Balance Volume) indicator
#[macro_export]
macro_rules! obv_indicator {
    (
        $graph:expr,
        closes: $closes:expr,
        prev_closes: $prev_closes:expr,
        volumes: $volumes:expr,
        output: $out:expr
    ) => {{
        // Sign(closes - prev_closes) -> direction
        $graph.node.push(onnx_node!("Sub", "obv_diff", [$closes, $prev_closes] -> ["obv_diff"]));
        $graph.node.push(onnx_node!("Sign", "obv_sign", ["obv_diff"] -> ["direction"]));
        // Mul(direction, volumes) -> obv_flow
        $graph.node.push(onnx_node!("Mul", "obv_flow", ["direction", $volumes] -> ["obv_flow"]));
        // Cumulative sum would need a custom approach or loop in ONNX
        // For now, we output the flow which can be cumulated externally
        $graph.node.push(onnx_node!("Identity", "obv_output", ["obv_flow"] -> [$out]));
    }};
}

/// Convenience macro for creating prediction layer (MatMul + Add + Sigmoid)
#[macro_export]
macro_rules! prediction_layer {
    (
        $graph:expr,
        features: $features:expr,
        weights_input: $w_inp:expr,
        bias_input: $b_inp:expr,
        reshape_shape: $shape_name:expr,
        output: $out:expr,
        n_features: $n_feat:expr,
        n_outputs: $n_out:expr
    ) => {{
        // Reshape(W_pred_flat, [n_feat, n_out]) -> W_pred
        $graph.node.push(onnx_node!(
            "Reshape",
            "w_reshape",
            [$w_inp, $shape_name] -> ["W_pred"]
        ));

        // MatMul(features, W_pred) -> dot
        $graph.node.push(onnx_node!(
            "MatMul",
            "pred_matmul",
            [$features, "W_pred"] -> ["dot"]
        ));

        // Add(dot, B_pred) -> biased
        $graph.node.push(onnx_node!(
            "Add",
            "pred_add",
            ["dot", $b_inp] -> ["biased"]
        ));

        // Sigmoid(biased) -> output
        $graph.node.push(onnx_node!(
            "Sigmoid",
            "pred_sigmoid",
            ["biased"] -> [$out]
        ));
    }};
}
