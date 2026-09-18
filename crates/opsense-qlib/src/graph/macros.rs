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
