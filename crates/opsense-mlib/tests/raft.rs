//! Raft — kiểm chứng đúng ranh giới: **quyết định**, không quan sát.
//!
//! Câu hỏi mà bộ test này trả lời: "ai là master" có được lấy từ tầng quyết
//! định chứ không từ view của gossip không. Câu hỏi "ai còn sống" thuộc về
//! `gossip.rs` và cố ý không xuất hiện ở đây.

use opsense_mlib::raft::{Command, Outcome, Raft, Role, SingleNode};

fn raft(node_id: &str) -> Raft {
    Raft::new(node_id, Box::new(SingleNode::default()))
}

fn trio() -> Vec<String> {
    ["node-a", "node-b", "node-c"].iter().map(|s| (*s).to_string()).collect()
}

/// Node vừa join chưa ai nhốt vào cụm nào: tự tạo cụm riêng và là master của nó.
#[test]
fn a_fresh_node_is_master_of_its_own_cluster() {
    let mut a = raft("node-a");
    let id = a.bootstrap_own_cluster("grid").expect("bootstrap");
    assert_eq!(a.role_of(&id), Role::Master);
    assert_eq!(a.my_clusters().len(), 1);
    assert_eq!(a.my_clusters()[0].pipeline, "grid");
}

/// `POST /clusters` — gom node vào một cụm. Cục bộ thì ai cũng biết đầy đủ, nhưng
/// **vai trò phải lấy từ tầng quyết định**: với `SingleNode` thì thành viên đầu
/// tiên làm master, nên `node-a` master còn `node-b` standby.
#[test]
fn role_comes_from_the_decision_layer_not_from_membership() {
    let mut a = raft("node-a");
    let mut b = raft("node-b");
    for r in [&mut a, &mut b] {
        r.group("c1", trio(), "grid").expect("group");
    }
    assert_eq!(a.role_of("c1"), Role::Master);
    assert_eq!(b.role_of("c1"), Role::Standby);
    // Cùng một cấu hình, hai node phải cùng kết luận về master.
    assert_eq!(a.cluster("c1").unwrap().members, b.cluster("c1").unwrap().members);
}

#[test]
fn grouping_into_a_new_cluster_adds_every_member() {
    let mut a = raft("node-a");
    assert_eq!(a.group("c1", trio(), "grid").unwrap(), Outcome::Applied);
    let c = a.cluster("c1").unwrap();
    assert_eq!(c.members.len(), 3);
    assert!(c.has("node-b") && c.has("node-c"));
}

#[test]
fn grouping_is_idempotent_when_nothing_changes() {
    let mut a = raft("node-a");
    a.group("c1", trio(), "grid").unwrap();
    assert_eq!(a.group("c1", trio(), "grid").unwrap(), Outcome::Noop);
    assert_eq!(a.cluster("c1").unwrap().members.len(), 3);
}

#[test]
fn adding_a_node_to_an_existing_cluster_is_allowed_but_switching_pipeline_is_not() {
    let mut a = raft("node-a");
    a.group("c1", ["node-a".to_string()], "grid").unwrap();

    let out = a.propose(Command::AddMember {
        cluster_id: "c1".into(),
        node_id: "node-b".into(),
    });
    assert_eq!(out.unwrap(), Outcome::Applied);
    assert!(a.cluster("c1").unwrap().has("node-b"));

    // Pipeline của một cụm là bất biến: đổi nó nghĩa là dữ liệu đang chạy không
    // còn ý nghĩa với pipeline mới.
    let out = a.group("c1", ["node-a".to_string(), "node-b".to_string()], "predict");
    assert!(matches!(out.unwrap(), Outcome::Rejected(_)));
    assert_eq!(a.cluster("c1").unwrap().pipeline, "grid", "cấu hình phải giữ nguyên");
}

/// Lệnh hỏng phải bị từ chối **trước khi** vào log, và state phải giữ nguyên.
#[test]
fn rejected_command_never_touches_state() {
    let mut a = raft("node-a");
    a.group("c1", ["node-a".to_string()], "grid").unwrap();
    let before = a.clusters().len();

    for out in [
        a.group("c2", [] as [String; 0], "grid").unwrap(),
        a.propose(Command::AddMember { cluster_id: "nope".into(), node_id: "b".into() }).unwrap(),
        a.propose(Command::AddMember { cluster_id: "c1".into(), node_id: "  ".into() }).unwrap(),
        a.dissolve("khong-co").unwrap(),
    ] {
        assert!(matches!(out, Outcome::Rejected(_)), "phải từ chối, không phải áp dụng");
    }
    assert_eq!(a.clusters().len(), before);
}

#[test]
fn dissolving_the_last_member_drops_the_cluster_instead_of_leaving_it_empty() {
    let mut a = raft("node-a");
    a.group("c1", ["node-a".to_string()], "grid").unwrap();
    let out = a
        .propose(Command::RemoveMember { cluster_id: "c1".into(), node_id: "node-a".into() })
        .unwrap();
    assert_eq!(out, Outcome::Applied);
    assert!(a.cluster("c1").is_none(), "cụm rỗng thì không ai chạy được");
}

#[test]
fn dissolve_then_recreate_is_allowed() {
    let mut a = raft("node-a");
    a.group("c1", ["node-a".to_string()], "grid").unwrap();
    assert_eq!(a.dissolve("c1").unwrap(), Outcome::Applied);
    assert!(a.cluster("c1").is_none());
    assert_eq!(a.group("c1", ["node-a".to_string()], "predict").unwrap(), Outcome::Applied);
    assert_eq!(a.cluster("c1").unwrap().pipeline, "predict");
}

#[test]
fn creating_the_same_cluster_twice_is_a_noop_not_an_error() {
    let mut a = raft("node-a");
    let c = opsense_mlib::raft::Cluster::solo("c1", "node-a", "grid");
    assert_eq!(a.propose(Command::Create { cluster: c.clone() }).unwrap(), Outcome::Applied);
    assert_eq!(a.propose(Command::Create { cluster: c }).unwrap(), Outcome::Noop);
}

#[test]
fn creating_a_cluster_with_no_members_is_refused() {
    let mut a = raft("node-a");
    let empty = opsense_mlib::raft::Cluster { members: Default::default(), ..opsense_mlib::raft::Cluster::solo("c1", "x", "grid") };
    assert!(matches!(a.propose(Command::Create { cluster: empty }).unwrap(), Outcome::Rejected(_)));
    assert!(a.cluster("c1").is_none());
}

/// `epoch` là fencing token: mỗi lần term tăng thì node cũ không được ghi đè.
#[test]
fn epoch_starts_at_zero_and_is_the_fencing_token_for_writes() {
    let mut a = raft("node-a");
    a.bootstrap_own_cluster("grid").unwrap();
    assert_eq!(a.epoch_of("cluster-node-a"), 0);
    // Cụm lạ thì epoch = 0, không phải panic — caller có thể coi như "chưa có".
    assert_eq!(a.epoch_of("khong-co"), 0);
}

#[test]
fn commands_survive_json_round_trip_because_the_log_carries_them() {
    let cmd = Command::Group {
        cluster_id: "c1".into(),
        members: trio().into_iter().collect(),
        pipeline: "grid".into(),
    };
    let json = serde_json::to_string(&cmd).expect("serialize");
    assert_eq!(serde_json::from_str::<Command>(&json).expect("deserialize"), cmd);

    let mut a = raft("node-a");
    a.propose(cmd).expect("apply từ JSON");
    assert_eq!(a.cluster("c1").unwrap().members.len(), 3);
}
