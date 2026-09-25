//! Hành vi của [`Mesh`] — struct chính, đi qua API công kật.
//!
//! Ba thứ phải đúng, và mỗi cái đều là một bất biến riêng:
//!
//! 1. **Bầu tất định** — hai node thấy cùng view thì cùng kết luận, không cần
//!    trao đổi vòng bầu cử nào.
//! 2. **Không bầu khi view chưa ổn định** — chặn split-brain lúc hai node cùng
//!    khởi động và đều tưởng mình cô lập.
//! 3. **Không kết luận chết sau một lần timeout** — phải đủ xác nhận của node
//!    khác mới sang `Dead`.

use opsense_mlib::mesh::{Cluster, ClusterState, Mesh, Role};

const SETTLE: u64 = 10;

/// `quorum = 1`: một lời xác nhận từ peer là đủ để rút ngắn, còn kết luận chết
/// chủ yếu do `tick_reap` theo thời gian quyết định.
fn mesh(node_id: &str, now: u64) -> Mesh {
    Mesh::new(node_id, format!("http://{node_id}"), 1, now)
}

/// Ba node đã gossip xong: cùng biết cụm `c1`, cùng thấy nhau, đã hết settle.
fn settled_trio() -> (Mesh, Mesh, Mesh) {
    let now = 100_000;
    let mut a = mesh("node-a", now);
    let mut b = mesh("node-b", now);
    let mut c = mesh("node-c", now);
    a.create_cluster("c1", ["node-b".to_string(), "node-c".to_string()], "grid", now);

    // Gossip: node a đẩy cụm cho b và c, rồi b/c đẩy ngược lại để ai cũng biết
    // các cụm mà node khác biết (giống `tick` pull/push thật).
    let sa = a.state().clone();
    b.apply(&sa, now);
    c.apply(&sa, now);
    let sb = b.state().clone();
    let sc = c.state().clone();
    a.apply(&sb, now);
    a.apply(&sc, now);
    b.apply(&sc, now);
    c.apply(&sb, now);

    for m in [&mut a, &mut b, &mut c] {
        m.add_peer("http://node-a", "node-a", now);
        m.add_peer("http://node-b", "node-b", now);
        m.add_peer("http://node-c", "node-c", now);
    }
    let later = now + SETTLE * 1_000;
    for m in [&mut a, &mut b, &mut c] {
        m.refresh_role(later, SETTLE);
    }
    (a, b, c)
}

#[test]
fn smallest_node_id_becomes_master_and_others_standby() {
    let (a, b, c) = settled_trio();
    assert_eq!(a.role(), Role::Master, "node-a nhỏ nhất nên là master");
    assert_eq!(b.role(), Role::Standby);
    assert_eq!(c.role(), Role::Standby);
    assert_eq!(a.master_of("c1").as_deref(), Some("node-a"));
}

#[test]
fn every_node_agrees_on_who_the_master_is() {
    let (a, b, c) = settled_trio();
    let on_a = a.master_of("c1");
    assert_eq!(on_a, b.master_of("c1"), "hai node cùng view phải cùng kết luận");
    assert_eq!(on_a, c.master_of("c1"));
}

#[test]
fn promotion_increments_epoch_so_stale_master_cannot_overwrite() {
    let (_, _, mut c) = settled_trio();
    let epoch_before = c.epoch_of("c1");
    assert_eq!(c.role(), Role::Standby);

    // node-c thấy a và b im lặng quá lâu ⇒ tái thu, không cần xác nhận của node
    // đã chết (đó là lý do quy tắc "đủ n/2+1 xác nhận" sẽ kẹt vô hạn).
    c.tick_suspects(0, 200_000);
    c.tick_reap(30_000, 200_000);
    assert!(c.nodes().iter().filter(|n| !n.alive && !n.suspect).count() >= 2);

    assert!(c.refresh_role(200_000 + SETTLE * 1_000, SETTLE), "phải báo đổi vai trò");
    assert_eq!(c.role(), Role::Master, "node-c là node sống duy nhất");
    assert!(
        c.epoch_of("c1") > epoch_before,
        "lên master phải tăng epoch, nếu không node cũ có thể ghi đè"
    );
    assert!(c.take_dirty(), "epoch mới phải được đẩy lên peer");
}

#[test]
fn settle_window_prevents_immediate_decision() {
    let now = 100_000;
    let mut a = mesh("node-a", now);
    a.create_cluster("c1", ["node-b".to_string()], "grid", now);
    a.add_peer("http://node-b", "node-b", now);

    // Chưa đủ thời gian yên ⇒ giữ Standby dù node-a rõ ràng là nhỏ nhất.
    assert!(!a.refresh_role(now + 1_000, SETTLE));
    assert_eq!(a.role(), Role::Standby);
    assert!(!a.is_settled(now + SETTLE * 1_000 - 1, SETTLE));

    // Đủ settle ⇒ mới bầu.
    assert!(a.refresh_role(now + SETTLE * 1_000, SETTLE));
    assert_eq!(a.role(), Role::Master);
}

#[test]
fn simultaneous_bootstrap_yields_exactly_one_master() {
    // Hai node cùng khởi động, mỗi node tưởng chỉ có mình trong cụm.
    let now = 100_000;
    let mut a = mesh("node-a", now);
    let mut b = mesh("node-b", now);
    for m in [&mut a, &mut b] {
        m.create_cluster("c1", ["node-a".to_string(), "node-b".to_string()], "grid", now);
    }
    // Chưa ai thấy ai, cả hai cùng tạo cụm riêng ⇒ state sẽ hội tụ sau.
    let masters_before_sync = usize::from(a.role() == Role::Master)
        + usize::from(b.role() == Role::Master);
    assert_eq!(masters_before_sync, 0, "chưa đủ settle thì không ai được làm master");

    // Giờ trao đổi state cho nhau như thật, rồi chờ settle.
    let sa = a.state().clone();
    let sb = b.state().clone();
    a.apply(&sb, now);
    b.apply(&sa, now);
    let later = now + SETTLE * 1_000;
    a.refresh_role(later, SETTLE);
    b.refresh_role(later, SETTLE);

    // Biết cả hai tồn tại nhưng chưa xác nhận được sống ⇒ cả hai đều đứng ngoài,
    // vì nếu một bên tự làm master lúc này thì bên kia cũng tưởng vậy.
    assert_eq!(a.role(), Role::Standby, "chưa phân giải được node-b thì không bầu");
    assert_eq!(b.role(), Role::Standby, "chưa phân giải được node-a thì không bầu");

    // Gossip thành công ⇒ hai bên cùng kết luận node nhỏ hơn thắng.
    a.add_peer("http://node-b", "node-b", later);
    b.add_peer("http://node-a", "node-a", later);
    let settled = later + SETTLE * 1_000;
    assert!(a.refresh_role(settled, SETTLE), "node-a lên master");
    assert!(!b.refresh_role(settled, SETTLE), "node-b vẫn standby nên không đổi");
    assert_eq!(a.role(), Role::Master);
    assert_eq!(b.role(), Role::Standby);
    let masters = usize::from(a.role() == Role::Master) + usize::from(b.role() == Role::Master);
    assert_eq!(masters, 1, "phải đúng một master");
    assert_eq!(a.master_of("c1"), b.master_of("c1"));
}

#[test]
fn one_timeout_is_not_enough_to_declare_dead() {
    let (mut a, _, _) = settled_trio();
    a.tick_suspects(0, 200_000);
    assert!(a.nodes().iter().any(|n| n.suspect), "node im lặng thành suspect");
    assert!(a.nodes().iter().all(|n| n.alive || n.suspect), "chưa node nào chết");

    // Một xác nhận là chưa đủ (quorum = 2).
    a.report_missing("node-b", "node-c");
    assert!(
        a.nodes().iter().all(|n| n.alive || n.suspect),
        "xác nhận của peer không được phép bỏ qua thời gian im lặng"
    );

    a.tick_reap(30_000, 200_000);
    assert!(
        a.nodes().iter().any(|n| !n.alive && !n.suspect),
        "im lặng quá dead_after thì mới chết"
    );
}

#[test]
fn any_success_clears_suspicion_so_flapping_network_does_not_churn_election() {
    let (mut a, _, _) = settled_trio();
    a.tick_suspects(0, 200_000);
    a.mark_alive("http://node-c", "node-c", 200_000);
    let n = a.nodes().into_iter().find(|n| n.node_id == "node-c").unwrap();
    assert!(n.alive && !n.suspect);
    assert!(!a.refresh_role(200_000, SETTLE), "view vừa ổn định lại thì không bầu lại");
}

#[test]
fn role_change_is_reported_once_not_every_tick() {
    let now = 100_000;
    let mut a = mesh("node-a", now);
    a.create_cluster("c1", [] as [String; 0], "grid", now);
    let later = now + SETTLE * 1_000;
    assert!(a.refresh_role(later, SETTLE), "lần đầu báo đã đổi");
    assert!(!a.refresh_role(later + 1, SETTLE), "các tick sau im lặng");
    assert_eq!(a.role(), Role::Master);
}

#[test]
fn mesh_survives_json_round_trip_for_the_internal_state_endpoint() {
    let (a, _, _) = settled_trio();
    let json = serde_json::to_string(a.state()).expect("serialize");
    let back: ClusterState = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(a.state().snapshot(), back.snapshot());

    // Cluster cũng phải đi được qua REST.
    let cj = serde_json::to_string(a.state().cluster("c1").unwrap()).expect("serialize cluster");
    let back_cluster: Cluster = serde_json::from_str(&cj).expect("deserialize cluster");
    assert_eq!(back_cluster.pipeline, "grid");
}

#[test]
fn dirty_flag_is_consumed_so_unchanged_state_is_not_pushed_every_tick() {
    let (mut a, _, _) = settled_trio();
    // create_cluster đã đánh dấu thay đổi.
    assert!(a.take_dirty());
    assert!(!a.take_dirty(), "đọc một lần là hết");
    a.apply(&ClusterState::new(), 200_000);
    assert!(!a.take_dirty(), "áp dụng state rỗng không phải thay đổi");
}

#[test]
fn zero_window_and_zero_quorum_edges_do_not_panic() {
    // Giá trị biên: window = u64::MAX và đồng hồ lùi.
    let mut a = mesh("node-a", 10_000);
    a.add_peer("http://node-b", "node-b", 10_000);
    assert!(a.tick_suspects(u64::MAX, 0).is_empty());
    assert!(!a.refresh_role(0, u64::MAX));

    // quorum = 0 nghĩa là cụm một node, không cần xác nhận nào.
    let mut solo = Mesh::new("solo", "http://solo", 0, 0);
    solo.add_peer("http://other", "other", 0);
    solo.tick_suspects(0, 1_000);
    solo.report_missing("other", "other");
    solo.tick_reap(0, 1_000);
    assert!(solo.nodes().iter().any(|n| !n.alive && !n.suspect));
}

#[test]
fn node_outside_the_cluster_has_no_role_but_still_sees_master() {
    let (a, _, _) = settled_trio();
    let mut outsider = mesh("node-z", 100_000);
    outsider.apply(a.state(), 100_000);
    outsider.add_peer("http://node-a", "node-a", 100_000);
    outsider.add_peer("http://node-b", "node-b", 100_000);
    outsider.add_peer("http://node-c", "node-c", 100_000);
    outsider.refresh_role(100_000 + SETTLE * 1_000, SETTLE);
    assert_eq!(outsider.role(), Role::Standby, "không thuộc cụm thì không chạy pipeline");
    assert_eq!(outsider.master_of("c1").as_deref(), Some("node-a"));
}

#[test]
fn self_health_result_is_never_treated_as_a_peer() {
    let mut a = mesh("node-a", 100_000);
    a.add_peer("http://node-a", "node-a", 100_000);
    assert!(a.nodes().is_empty(), "node không tự báo mình là peer");
}
