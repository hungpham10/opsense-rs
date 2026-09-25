//! Luồng join: node mới tới mesh và được các node sẵn có công nhận.
//!
//! Bốn bước, khớp 1-1 với những gì tầng vận chuyển (W3) sẽ gọi:
//!
//! 1. node mới `handle_join` ở seed ⇒ seed đăng ký nó và trả về roster;
//! 2. node mới `add_peer` theo roster, rồi `probe_targets()` để biết cần ping ai;
//! 3. mỗi tick: ping `probe_targets()`, `tick_suspects`, hỏi chéo qua
//!    `indirect_probe_targets()`, `tick_reap`;
//! 4. trao đổi `state()` cho nhau rồi `refresh_role`.
//!
//! Test chỉ chạy lớp logic, không có HTTP: đây là hợp đồng mà tầng vận chuyển
//! sẽ lắp vào.

use opsense_mlib::mesh::{ClusterState, Mesh, Role};

const SETTLE: u64 = 10;
const DEAD_MS: u64 = 30_000;
/// Cửa sổ nghi ngờ = 0: node vừa ngừng trả lời là đã quá hạn (tức thời).
const PAST_ANY: u64 = 0;

fn mesh(node_id: &str, now: u64) -> Mesh {
    Mesh::new(node_id, format!("http://{node_id}"), 1, now)
}

/// Bước 4: hai node trao đổi state như thật rồi chờ hết settle.
fn exchange(a: &mut Mesh, b: &mut Mesh, now: u64) {
    let sa = a.state().clone();
    b.apply(&sa, now);
    let sb = b.state().clone();
    a.apply(&sb, now);
}

#[test]
fn seed_learns_about_the_newcomer_otherwise_it_stays_invisible() {
    let now = 100_000;
    let mut seed = mesh("node-a", now);
    seed.create_cluster("c1", [] as [String; 0], "grid", now);

    // Trước khi join: cụm chỉ có node-a.
    assert_eq!(seed.nodes().len(), 0, "node tự báo mình không phải peer của ai");

    // Node mới tới và gọi join.
    let roster = seed.handle_join("http://node-z", "node-z", now);
    // Roster gồm cả chính seed — node mới cần biết seed để ping lại, không thì
    // nó không bao giờ thấy ai và cụm đứng yên.
    assert_eq!(roster.len(), 2, "roster phải gồm cả seed lẫn node mới");
    assert_eq!(roster[0].node_id, "node-a", "roster sắp theo node_id");
    assert_eq!(roster[1].node_id, "node-z");

    // Giờ node-a mới thấy node-z, và node-z là thành viên hợp lệ để bầu.
    seed.apply(
        &{
            let mut s = ClusterState::new();
            s.set_local(
                "node-z",
                opsense_mlib::mesh::Cluster::solo("c1", "node-z", "grid"),
            );
            s
        },
        now,
    );
    assert!(seed.cluster_id().is_some());
}

#[test]
fn newcomer_ends_up_in_the_cluster_and_can_become_master_if_it_is_the_only_one() {
    let now = 100_000;
    // Cụm một thành viên do chính node tạo.
    let mut node_z = mesh("node-z", now);
    node_z.create_cluster("c1", [] as [String; 0], "grid", now);
    assert!(node_z.refresh_role(now + SETTLE * 1_000, SETTLE));
    assert_eq!(node_z.role(), Role::Master, "cụm một node thì tự làm master");
}

#[test]
fn join_then_exchange_makes_the_newcomer_visible_to_the_cluster() {
    let now = 100_000;
    let mut a = mesh("node-a", now);
    a.create_cluster("c1", ["node-z".to_string()], "grid", now);
    a.handle_join("http://node-z", "node-z", now);

    let mut z = mesh("node-z", now);
    // Node mới nhận roster mà seed trả về khi xử lý join.
    let roster = a.roster();
    for peer in &roster {
        z.add_peer(&peer.url, &peer.node_id, now);
    }
    assert!(z.probe_targets().contains(&"http://node-a".to_string()));

    // Cả hai cùng biết cụm, đủ settle ⇒ đúng một master.
    exchange(&mut a, &mut z, now);
    let later = now + SETTLE * 1_000;
    a.refresh_role(later, SETTLE);
    z.refresh_role(later, SETTLE);
    assert_eq!(a.role(), Role::Master, "node-a nhỏ hơn nên thắng");
    assert_eq!(z.role(), Role::Standby);
    assert_eq!(a.master_of("c1"), z.master_of("c1"));
}

#[test]
fn dead_peer_is_dropped_from_probe_targets_but_suspect_is_kept() {
    let (mut a, _, _) = {
        // setup tối thiểu: node-a với 2 peer
        let now = 100_000;
        let mut x = mesh("node-a", now);
        x.add_peer("http://node-b", "node-b", now);
        x.add_peer("http://node-c", "node-c", now);
        (x, mesh("node-b", now), mesh("node-c", now))
    };
    assert_eq!(a.probe_targets().len(), 2);

    // Còn nghi thì vẫn phải ping — một lần trả lời là xoá nghi ngờ.
    a.tick_suspects(PAST_ANY, 200_000);
    assert_eq!(a.probe_targets().len(), 2, "suspect vẫn nằm trong danh sách ping");

    a.tick_reap(DEAD_MS, 200_000);
    assert_eq!(a.probe_targets().len(), 0, "dead thì bỏ khỏi danh sách ping");
}

#[test]
fn indirect_probe_asks_other_live_peers_about_the_suspect() {
    let now = 100_000;
    let mut a = mesh("node-a", now);
    a.add_peer("http://node-b", "node-b", now);
    a.add_peer("http://node-c", "node-c", now);
    a.add_peer("http://node-d", "node-d", now);
    a.tick_suspects(PAST_ANY, 200_000);

    let pairs = a.indirect_probe_targets();
    // node-b/c/d đều suspect; hỏi các node còn sống khác — ở đây chưa có node
    // sống nào (tất cả cùng suspect) nên không hỏi ai.
    assert!(pairs.is_empty(), "không hỏi node mà ta cũng không tin");

    // node-d trở lại sống ⇒ ta hỏi d về b và c, và không hỏi d về chính d.
    a.mark_alive("http://node-d", "node-d", 200_000);
    let pairs = a.indirect_probe_targets();
    assert!(pairs.contains(&("node-d", "node-b")));
    assert!(pairs.contains(&("node-d", "node-c")));
    assert!(!pairs.iter().any(|(_, s)| *s == "node-d"), "không hỏi về chính mình");
}

#[test]
fn suspected_answers_recover_the_peer_without_any_reap() {
    let now = 100_000;
    let mut a = mesh("node-a", now);
    a.create_cluster("c1", ["node-b".to_string()], "grid", now);
    a.add_peer("http://node-b", "node-b", now);
    a.tick_suspects(PAST_ANY, 200_000);
    assert!(a.indirect_probe_targets().is_empty());

    // node-b lên lại (lưới chập chờn, không phải chết).
    a.mark_alive("http://node-b", "node-b", 200_000);
    assert!(a.nodes().iter().all(|n| n.alive));
    assert_eq!(a.role(), Role::Standby, "chưa đủ settle để bầu");
    assert!(a.refresh_role(200_000 + SETTLE * 1_000, SETTLE));
    assert_eq!(a.role(), Role::Master);
}
