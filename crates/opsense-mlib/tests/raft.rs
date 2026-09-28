//! Raft — kiểm chứng đúng ranh giới: **quyết định**, không quan sát.
//!
//! Ba điều cố ý kiểm ở đây:
//!
//! 1. **Node đứng một mình không có cụm nào** — không bầu, không log, và tự chạy
//!    pipeline của mình.
//! 2. "Ai là master" phải đến từ **tầng quyết định** (`Consensus`), không bao
//!    giờ từ view của gossip.
//! 3. Lệnh hỏng phải bị từ chối **trước khi** vào log, state giữ nguyên.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use opsense_mlib::raft::{Command, Consensus, Outcome, Raft, Role};

/// Engine giả để kiểm logic vai trò trước khi cắm `openraft`: master và term do
/// test điều khiển, `propose` chỉ ghi log mô phỏng.
///
/// Cố ý **không** đưa vào `src/` — đây là đồ dùng thử. Sản phẩm không được có
/// kiểu "master = thành viên đầu tiên" vì nó che mất đúng thứ Raft phải lo.
#[derive(Default)]
struct FakeInner {
    /// `cluster_id` → master được chỉ định.
    leaders: BTreeMap<String, String>,
    terms: BTreeMap<String, u64>,
}

struct FakeConsensus(Arc<Mutex<FakeInner>>);

/// Tay cầm phía test để điều khiển bầu cử.
#[derive(Clone)]
struct Handle(Arc<Mutex<FakeInner>>);

impl Handle {
    fn elect(&self, cluster_id: &str, leader: &str) {
        let mut g = self.0.lock().expect("lock");
        g.leaders.insert(cluster_id.to_string(), leader.to_string());
        *g.terms.entry(cluster_id.to_string()).or_insert(0) += 1;
    }

    /// Bầu lại cùng người (leader mất rồi quay lại) — term vẫn phải tăng.
    fn reelect(&self, cluster_id: &str, leader: &str) {
        self.elect(cluster_id, leader);
    }

    fn term(&self, cluster_id: &str) -> u64 {
        self.0.lock().expect("lock").terms.get(cluster_id).copied().unwrap_or(0)
    }
}

impl Consensus for FakeConsensus {
    fn propose(&mut self, _cmd: Command) -> Result<bool, String> {
        Ok(true)
    }

    fn leader_of(&self, cluster_id: &str) -> Option<String> {
        self.0.lock().expect("lock").leaders.get(cluster_id).cloned()
    }

    fn term_of(&self, cluster_id: &str) -> u64 {
        self.0.lock().expect("lock").terms.get(cluster_id).copied().unwrap_or(0)
    }
}

/// Engine dùng chung cho cả cụm + tay cầm để test điều khiển bầu cử.
///
/// Dùng chung là **cố ý mô phỏng kết quả của Raft thật**: sau khi log được nhân
/// bản và commit thành công thì mọi node hội tụ cùng một leader và cùng một term.
/// Nếu mỗi node có engine riêng thì test chỉ kiểm được "node này tự bầu" — vô
/// nghĩa, và che mất đúng thứ cần kiểm: node có cùng kết luận không.
fn engine() -> (Arc<Mutex<FakeInner>>, Handle) {
    let inner = Arc::new(Mutex::new(FakeInner::default()));
    (inner.clone(), Handle(inner))
}

fn node_on(inner: &Arc<Mutex<FakeInner>>, node_id: &str) -> Raft {
    Raft::with_consensus(node_id, Box::new(FakeConsensus(inner.clone())))
}

/// Một node đứng một mình có engine.
fn node(node_id: &str) -> (Raft, Handle) {
    let (inner, h) = engine();
    (node_on(&inner, node_id), h)
}

fn solo(node_id: &str) -> Raft {
    Raft::solo(node_id)
}

fn trio() -> Vec<String> {
    ["node-a", "node-b", "node-c"].iter().map(|s| (*s).to_string()).collect()
}

// ── 1. node một mình không có cụm ────────────────────────────────────────────

#[test]
fn a_lone_node_has_no_cluster_and_runs_on_its_own() {
    let r = solo("node-a");
    assert!(r.is_solo());
    assert!(r.my_cluster().is_none());
    assert!(r.clusters().is_empty());
    assert!(!r.has_consensus());
    // Không có gì để bầu nên nó là master của chính nó.
    assert_eq!(r.role(), Role::Master);
}

#[test]
fn a_lone_node_refuses_to_group_until_an_engine_is_wired() {
    let mut r = solo("node-a");
    // Gom node vào cụm cần commit vào log; chưa cắm engine thì không làm được —
    // trung thực hơn là bịa ra một cụm mà không ai làm master.
    let err = r.group("c1", trio(), "grid").expect_err("phải báo chưa cắm engine");
    assert!(err.contains("chưa cắm engine"), "phải nói rõ nguyên nhân: {err}");
    assert!(r.clusters().is_empty(), "lệnh lỗi không được đụng state");
    assert_eq!(r.role(), Role::Master, "vẫn chạy pipeline của mình");
}

// ── 2. vai trò đến từ tầng quyết định ─────────────────────────────────────────

#[test]
fn role_comes_from_the_decision_layer_not_from_membership() {
    let (inner, engine) = engine();
    let mut a = node_on(&inner, "node-a");
    let mut b = node_on(&inner, "node-b");
    a.group("c1", trio(), "grid").expect("group");
    b.group("c1", trio(), "grid").expect("group");

    // Chưa ai được bầu ⇒ cả hai đứng ngoài, dù cấu hình giống hệt nhau.
    assert!(!a.has_leader("c1"));
    assert_eq!(a.role(), Role::Standby);
    assert_eq!(b.role(), Role::Standby);

    engine.elect("c1", "node-a");
    assert_eq!(a.role(), Role::Master);
    assert_eq!(b.role(), Role::Standby, "node-b không tự phong master");
}

#[test]
fn a_newly_grouped_node_waits_instead_of_running_its_pipeline() {
    let (mut a, engine) = node("node-a");
    assert_eq!(a.role(), Role::Master, "đứng một mình thì chạy");
    a.group("c1", trio(), "grid").expect("group");
    // Vừa vào cụm: chưa có master thì đứng ngoài, không tự chạy.
    assert_eq!(a.role(), Role::Standby);
    engine.elect("c1", "node-a");
    assert_eq!(a.role(), Role::Master, "có master là chạy");
}

#[test]
fn failover_moves_master_and_bumps_the_fencing_epoch() {
    let (inner, engine) = engine();
    let mut a = node_on(&inner, "node-a");
    let mut c = node_on(&inner, "node-c");
    a.group("c1", trio(), "grid").expect("group");
    c.group("c1", trio(), "grid").expect("group");

    engine.elect("c1", "node-a");
    let epoch_before = a.epoch_of("c1");
    assert_eq!(a.role(), Role::Master);
    assert_eq!(c.role(), Role::Standby);

    // node-a chết: Raft bầu node-c, term tăng.
    engine.elect("c1", "node-c");
    assert!(c.epoch_of("c1") > epoch_before, "term phải tăng để fencing có tác dụng");
    assert_eq!(c.role(), Role::Master);
    assert_eq!(a.role(), Role::Standby, "node cũ phải biết mình không còn master");
}

#[test]
fn old_master_returning_never_wins_back_its_old_epoch() {
    let (inner, engine) = engine();
    let mut a = node_on(&inner, "node-a");
    let mut b = node_on(&inner, "node-b");
    let members = || ["node-a".to_string(), "node-b".to_string()];
    a.group("c1", members(), "grid").expect("group");
    b.group("c1", members(), "grid").expect("group");

    engine.elect("c1", "node-a");
    let stale_epoch = a.epoch_of("c1");
    assert_eq!(a.role(), Role::Master);

    // Failover: term tăng, master đổi.
    engine.elect("c1", "node-b");
    assert_eq!(b.role(), Role::Master);
    assert_eq!(a.role(), Role::Standby, "node cũ không được tự phong lại master");
    // `epoch` là **sự thật của cả cụm** (cùng đọc từ log), không phải bộ đếm
    // riêng của từng node — nên nó chỉ tăng và ai cũng thấy như nhau.
    assert_eq!(a.epoch_of("c1"), b.epoch_of("c1"), "mọi node cùng thấy một epoch");
    assert!(
        a.epoch_of("c1") > stale_epoch,
        "ghi state với epoch cũ ({stale_epoch}) phải bị fencing từ chối"
    );
}

#[test]
fn node_outside_a_cluster_has_no_say_over_it() {
    let (inner, engine) = engine();
    let mut a = node_on(&inner, "node-a");
    let z = node_on(&inner, "node-z");
    a.group("c1", ["node-a".to_string()], "grid").expect("group");
    engine.elect("c1", "node-a");
    // node-z không thuộc cụm ⇒ nó tự chạy pipeline của mình, không tranh.
    assert!(z.is_solo());
    assert_eq!(z.role_of("c1"), Role::Master, "không thuộc cụm thì coi như đứng ngoài");
}

// ── 3. lệnh hỏng bị từ chối trước khi vào log ─────────────────────────────────

#[test]
fn rejected_command_never_touches_state() {
    let (mut a, _) = node("node-a");
    a.group("c1", ["node-a".to_string()], "grid").expect("group");
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
fn grouping_is_idempotent_and_pipeline_of_a_cluster_is_immutable() {
    let (mut a, _) = node("node-a");
    assert_eq!(a.group("c1", trio(), "grid").unwrap(), Outcome::Applied);
    assert_eq!(a.group("c1", trio(), "grid").unwrap(), Outcome::Noop);
    assert_eq!(a.cluster("c1").unwrap().members.len(), 3);

    // Đổi pipeline của cụm đang chạy nghĩa là dữ liệu hiện tại không còn ý nghĩa
    // với pipeline mới ⇒ phải từ chối, hãy giải thể rồi tạo lại.
    assert!(matches!(
        a.group("c1", ["node-a".to_string()], "predict").unwrap(),
        Outcome::Rejected(_)
    ));
    assert_eq!(a.cluster("c1").unwrap().pipeline, "grid", "cấu hình phải giữ nguyên");
}

#[test]
fn dissolving_the_last_member_makes_the_node_solo_again() {
    let (mut a, _) = node("node-a");
    a.group("c1", ["node-a".to_string()], "grid").expect("group");
    assert!(!a.is_solo());

    let out = a
        .propose(Command::RemoveMember { cluster_id: "c1".into(), node_id: "node-a".into() })
        .unwrap();
    assert_eq!(out, Outcome::Applied);
    assert!(a.cluster("c1").is_none(), "không để lại cụm rỗng");
    assert!(a.is_solo());
    assert_eq!(a.role(), Role::Master, "rời cụm thì lại chạy pipeline của mình");
}

#[test]
fn dissolve_then_recreate_allows_a_new_pipeline() {
    let (mut a, _) = node("node-a");
    a.group("c1", ["node-a".to_string()], "grid").expect("group");
    assert_eq!(a.dissolve("c1").unwrap(), Outcome::Applied);
    assert_eq!(a.group("c1", ["node-a".to_string()], "predict").unwrap(), Outcome::Applied);
    assert_eq!(a.cluster("c1").unwrap().pipeline, "predict");
}

#[test]
fn creating_a_cluster_with_no_members_is_refused() {
    let (mut a, _) = node("node-a");
    let empty = opsense_mlib::raft::Cluster {
        members: Default::default(),
        ..opsense_mlib::raft::Cluster::solo("c1", "x", "grid")
    };
    assert!(matches!(a.propose(Command::Create { cluster: empty }).unwrap(), Outcome::Rejected(_)));
    assert!(a.cluster("c1").is_none());
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

    let (mut a, _) = node("node-a");
    a.propose(cmd).expect("apply từ JSON");
    assert_eq!(a.cluster("c1").unwrap().members.len(), 3);
}

#[test]
fn fake_engine_term_is_the_fencing_source() {
    let (a, engine) = node("node-a");
    engine.elect("c1", "node-a");
    assert_eq!(a.epoch_of("c1"), engine.term("c1"));
    assert_eq!(a.epoch_of("khong-co"), 0, "cụm lạ thì epoch = 0, không panic");
    engine.reelect("c1", "node-a");
    assert!(a.epoch_of("c1") > 1, "bầu lại vẫn phải tăng term");
}
