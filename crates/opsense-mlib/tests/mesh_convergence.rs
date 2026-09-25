//! Hội tụ của state phân cụm: các node nhận các bản ghi **theo thứ tự bất kỳ**
//! và với độ trễ khác nhau vẫn phải kết luận giống nhau.
//!
//! Đây là bất biến quan trọng nhất của lớp state — nếu nó đúng thì các node
//! tự hòa giải mà không cần Raft, và chỉ còn phần fencing để chặn hai bên
//! cùng ghi khi mạng đứt đôi.

use opsense_mlib::mesh::{Cluster, ClusterState};

/// Ba node, mỗi node có một cụm riêng, rồi tất cả trao đổi đầy đủ.
fn three_node_world() -> (ClusterState, ClusterState, ClusterState) {
    let mut a = ClusterState::new();
    a.set_local("node-a", Cluster::solo("c-a", "node-a", "grid-a"));
    let mut b = ClusterState::new();
    b.set_local("node-b", Cluster::solo("c-b", "node-b", "grid-b"));
    let mut c = ClusterState::new();
    c.set_local("node-c", Cluster::solo("c-c", "node-c", "grid-c"));
    (a, b, c)
}

#[test]
fn all_nodes_converge_regardless_of_message_order() {
    let (a, b, c) = three_node_world();

    // Cùng bộ dữ liệu, ba thứ tự nhận khác nhau.
    let mut order1 = a.clone();
    order1.apply(&b);
    order1.apply(&c);

    let mut order2 = a.clone();
    order2.apply(&c);
    order2.apply(&b);

    let mut order3 = c.clone();
    order3.apply(&b);
    order3.apply(&a);

    assert_eq!(order1.clusters, order2.clusters, "hoán đổi b/c phải cho kết quả giống nhau");
    assert_eq!(order1.clusters, order3.clusters, "khác cả node khởi đầu vẫn phải giống nhau");
    assert_eq!(order1.list().len(), 3);
}

#[test]
fn apply_is_idempotent() {
    let (mut a, b, _) = three_node_world();
    assert!(a.apply(&b));
    let snapshot = a.clusters.clone();
    // Nhận lại cùng một state (heartbeat lặp) không được đổi gì.
    assert!(!a.apply(&b));
    assert_eq!(a.clusters, snapshot);
}

#[test]
fn apply_is_idempotent_after_convergence() {
    let (a, b, c) = three_node_world();
    let mut x = a.clone();
    x.apply(&b);
    x.apply(&c);
    let converged = x.clone();

    // Hai lượt đồng bộ nữa, cả hai vòng đều phải là no-op.
    assert!(!x.apply(&a));
    assert!(!x.apply(&b));
    assert!(!x.apply(&c));
    assert_eq!(x.clusters, converged.clusters);
}

#[test]
fn later_lamport_wins_and_old_data_never_rolls_back() {
    let (mut a, mut b, _) = three_node_world();
    // c-a sống ở cả hai node; b sửa nó ở thời điểm mới hơn.
    b.set_local("node-b", Cluster::solo("c-a", "node-a", "grid-b-CHANGED"));
    a.apply(&b);
    assert_eq!(a.cluster("c-a").map(|c| c.pipeline.as_str()), Some("grid-b-CHANGED"));

    // Sau đó node-a nhận lại state cũ của chính nó (trễ chuyển mạng).
    let stale = a.clone();
    b.apply(&stale);
    assert_eq!(
        b.cluster("c-a").map(|c| c.pipeline.as_str()),
        Some("grid-b-CHANGED"),
        "dữ liệu cũ không được đè lên bản mới hơn"
    );
}

#[test]
fn tie_on_lamport_resolves_by_author() {
    let mut a = ClusterState::new();
    let mut b = ClusterState::new();
    // Cùng lamport (1) nhưng khác tác giả ⇒ so tên để chọn.
    a.set_local("node-a", Cluster::solo("c1", "node-a", "from-a"));
    b.set_local("node-b", Cluster::solo("c1", "node-b", "from-b"));
    a.apply(&b);
    b.apply(&a);
    assert_eq!(a.clusters, b.clusters, "tie phải phân giải giống nhau ở cả hai node");
    // "node-b" > "node-a" theo thứ tự từ điển.
    assert_eq!(a.cluster("c1").map(|c| c.pipeline.as_str()), Some("from-b"));
}

#[test]
fn state_survives_serde_roundtrip() {
    let (mut a, b, _) = three_node_world();
    a.apply(&b);
    let json = serde_json::to_string(&a).expect("serialize");
    let back: ClusterState = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(a, back, "state phải đi được qua HTTP dạng JSON");
}

#[test]
fn large_lamport_does_not_wrap_or_panic() {
    let mut a = ClusterState::new();
    let mut b = ClusterState::new();
    a.set_local("node-a", Cluster::solo("c1", "node-a", "x"));
    b.lamport = u64::MAX;
    b.set_local("node-b", Cluster::solo("c2", "node-b", "y"));
    a.apply(&b);
    assert!(a.cluster("c2").is_some(), "lamport cực lớn không được làm mất bản ghi");
}
