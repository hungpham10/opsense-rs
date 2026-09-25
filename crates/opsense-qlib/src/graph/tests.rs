use super::ops;
use crate::graph::{Graph, In, Node, Op};
use crate::Strategy;

fn default_ops() -> Vec<Box<dyn Op>> {
    vec![
        Box::new(ops::Last),
        Box::new(ops::Last),
        Box::new(ops::Atr { period: 14 }),
        Box::new(ops::Last),
        Box::new(ops::Last),
        Box::new(ops::Div),
        Box::new(ops::Concat { axis: 1 }),
        Box::new(ops::Head {
            n_feat: 2,
            n_out: 8,
        }),
    ]
}

fn default_nodes() -> Vec<Node> {
    vec![
        Node {
            op: 0,
            inputs: vec![In::FromExtractor(0)],
        },
        Node {
            op: 1,
            inputs: vec![In::FromExtractor(0)],
        },
        Node {
            op: 2,
            inputs: vec![
                In::FromExtractor(1),
                In::FromExtractor(2),
                In::FromExtractor(3),
            ],
        },
        Node {
            op: 3,
            inputs: vec![In::FromExtractor(1)],
        },
        Node {
            op: 4,
            inputs: vec![In::FromExtractor(2)],
        },
        Node {
            op: 5,
            inputs: vec![In::FromOperator(2), In::FromOperator(0)],
        },
        Node {
            op: 6,
            inputs: vec![In::FromOperator(1), In::FromOperator(5)],
        },
        Node {
            op: 7,
            inputs: vec![In::FromOperator(6)],
        },
    ]
}

/// `init()` phải trả params giao dịch **không phải 0**: `Portfolio` đọc
/// `params[0]=kelly`, `params[1]=base_capital`; bằng 0 thì mọi lệnh có size 0
/// (không đặt được gì) mà test inference vẫn xanh — lỗi này rất dễ lọt.
#[test]
fn init_gives_trading_params_not_zeros() {
    let g = Graph::new(
        200,
        default_ops(),
        default_nodes(),
        vec![],
        vec![0.0; 16],
        vec![0.0; 8],
        8,
        200,
        60,
    );
    let params = Strategy::init(&g);
    assert!(
        params[crate::graph::P_KELLY] > 0.0,
        "kelly phải > 0: {params:?}"
    );
    assert!(
        params[crate::graph::P_CAPITAL] > 0.0,
        "base_capital phải > 0: {params:?}"
    );
    assert!(params[crate::graph::P_GRID_LEVELS] >= 2.0, "grid_levels ≥ 2");
    assert!(params[crate::graph::P_SL_PCT] > 0.0, "sl_pct phải > 0");
    assert!(params[crate::graph::P_LOOKBACK] > 0.0, "lookback phải > 0");
    // Phần trọng số vẫn phải đúng vị trí (params tối ưu được).
    let n_feat = g.num_features().expect("num_features");
    assert_eq!(params.len(), 6 + n_feat * 8 + 8);
}

#[test]
fn model_builds_and_infers() {
    let g = Graph::new(
        200,
        default_ops(),
        default_nodes(),
        vec![],
        vec![0.0; 16],
        vec![0.0; 8],
        8,
        200,
        60,
    );
    assert_eq!(g.num_features().expect("num_features"), 2);

    let mut inputs: Vec<Vec<f32>> = vec![vec![1.0; 200]; 4];
    inputs.push(vec![0.0; 16]);
    inputs.push(vec![0.0; 8]);

    let out = g.predict(&inputs).expect("predict");
    assert_eq!(out.len(), 2, "grid_params + atr");
    assert_eq!(out[0].len(), 8, "num_of_grids");
    assert_eq!(out[1].len(), 1, "atr");

    for v in &out[0] {
        assert!(
            (v - 0.5).abs() < 1e-4,
            "grid param ≈ 0.5 với trọng số 0, got {v}"
        );
    }
}

// TrendFollower: EMA fast/slow + ATR → features (n_feat=4)
#[test]
fn graph_serializes_trait_objects_with_typetag() {
    let g = Graph::new(
        200,
        default_ops(),
        default_nodes(),
        vec![],
        vec![0.0; 16],
        vec![0.0; 8],
        8,
        200,
        60,
    );
    let json = serde_json::to_string(&g).expect("serialize graph");
    let decoded: Graph = serde_json::from_str(&json).expect("deserialize graph");
    assert_eq!(decoded.num_features().expect("num_features"), 2);
    assert!(json.contains(r#""type":"ema""#) || json.contains(r#""type":"last""#));
}

#[test]
fn trend_follower_genotype_compiles() {
    let ops: Vec<Box<dyn Op>> = vec![
        Box::new(ops::Ema { period: 9 }),
        Box::new(ops::Ema { period: 21 }),
        Box::new(ops::Atr { period: 14 }),
        Box::new(ops::Sub),
        Box::new(ops::Concat { axis: 1 }),
        Box::new(ops::Head {
            n_feat: 4,
            n_out: 8,
        }),
    ];
    let nodes = vec![
        Node {
            op: 0,
            inputs: vec![In::FromExtractor(0)],
        },
        Node {
            op: 1,
            inputs: vec![In::FromExtractor(0)],
        },
        Node {
            op: 2,
            inputs: vec![
                In::FromExtractor(1),
                In::FromExtractor(2),
                In::FromExtractor(3),
            ],
        },
        Node {
            op: 3,
            inputs: vec![In::FromOperator(0), In::FromOperator(1)],
        },
        Node {
            op: 4,
            inputs: vec![
                In::FromOperator(0),
                In::FromOperator(1),
                In::FromOperator(2),
                In::FromOperator(3),
            ],
        },
        Node {
            op: 5,
            inputs: vec![In::FromOperator(4)],
        },
    ];
    let g = Graph::new(
        200,
        ops,
        nodes,
        vec![],
        vec![0.0; 32],
        vec![0.0; 8],
        8,
        200,
        60,
    );
    assert_eq!(g.num_features().expect("num_features"), 4);
    let mut inputs: Vec<Vec<f32>> = vec![vec![1.0; 200]; 4];
    inputs.push(vec![0.0; 32]);
    inputs.push(vec![0.0; 8]);
    let out = g.predict(&inputs).expect("predict");
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].len(), 8);
    assert_eq!(out[1].len(), 1);
    for v in &out[0] {
        assert!((v - 0.5).abs() < 1e-4);
    }
}
