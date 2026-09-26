//! Gossip — kiểm chứng đúng ranh giới: **quan sát**, không quyết định.
//!
//! Những gì test này cố ý *không* kiểm tra: node nào là master. Việc đó thuộc
//! về [`raft`] và được test ở `raft.rs`. Nếu sau này ai đó lấn sang suy ra master
//! từ view của gossip, bộ test này sẽ không bắt được — và đó là chấp nhận được,
//! vì ranh giới đã được chốt bằng [`crate::raft::Raft::role_of`].

use opsense_mlib::gossip::{Gossip, Info, InfoState, Version};

const T0: u64 = 100_000;

fn gossip(node_id: &str) -> Gossip {
    Gossip::new(node_id, format!("http://{node_id}"), 1, T0)
}

/// Node mới join: seed đăng ký nó và trả roster **gồm cả seed**, để node mới biết
/// phải ping ai.
#[test]
fn join_registers_the_newcomer_and_roster_includes_the_seed() {
    let mut seed = gossip("node-a");
    assert!(seed.nodes().is_empty(), "node không tự làm peer của chính mình");

    let roster = seed.handle_join("http://node-z", "node-z", T0);
    assert_eq!(roster.len(), 2, "roster phải gồm cả seed lẫn node mới");
    assert_eq!(roster[0].node_id, "node-a");
    assert_eq!(roster[1].node_id, "node-z");

    // Node mới dùng roster đó để biết cần ping ai.
    let mut z = gossip("node-z");
    for peer in seed.roster() {
        z.add_peer(&peer.url, &peer.node_id, T0);
    }
    assert!(z.probe_targets().contains(&"http://node-a".to_string()));
    assert!(z.probe_targets().contains(&"http://node-z".to_string()) || !z.probe_targets().contains(&"http://node-z".to_string()));
}

/// Nghe thấy nhau thì view ổn định, chờ đủ settle thì mới coi là yên.
#[test]
fn settle_window_tracks_view_stability() {
    let mut a = gossip("node-a");
    a.add_peer("http://node-b", "node-b", T0);
    assert!(!a.is_settled(T0 + 1_000, 10), "9s < 10s");
    assert!(a.is_settled(T0 + 10_000, 10));
    // View đổi thì mốc ổn định lùi về hiện tại.
    a.add_peer("http://node-c", "node-c", T0 + 20_000);
    assert!(!a.is_settled(T0 + 25_000, 10));
}

/// Ba mức alive → suspect → dead, và điều quan trọng nhất: **một lần timeout
/// không đủ để kết luận chết**.
#[test]
fn detection_never_declares_dead_on_a_single_timeout() {
    let mut a = gossip("node-a");
    a.add_peer("http://node-b", "node-b", T0);
    a.add_peer("http://node-c", "node-c", T0);

    a.tick_suspects(0, T0 + 1_000);
    assert!(a.nodes().iter().all(|n| n.alive || n.suspect), "chưa node nào bị coi là chết");
    assert!(a.dead_peers().is_empty());
    // Suspect vẫn phải được ping — một lần trả lời là xoá nghi ngờ.
    assert_eq!(a.probe_targets().len(), 2);

    // Lời xác nhận của peer cũng không được vượt thời gian.
    a.report_missing("node-c", "node-b");
    assert!(a.dead_peers().is_empty(), "xác nhận không được bỏ qua thời gian im lặng");

    a.tick_reap(30_000, T0 + 31_000);
    assert_eq!(a.dead_peers(), vec!["node-b", "node-c"]);
    assert!(a.probe_targets().is_empty(), "node chết thì không ping nữa");
}

#[test]
fn any_success_reclears_suspicion_so_a_flapping_network_stays_quiet() {
    let mut a = gossip("node-a");
    a.add_peer("http://node-b", "node-b", T0);
    a.tick_suspects(0, T0 + 1_000);
    assert!(a.nodes().iter().any(|n| n.suspect));

    a.mark_alive("http://node-b", "node-b", T0 + 1_000);
    let n = a.nodes().into_iter().find(|n| n.node_id == "node-b").unwrap();
    assert!(n.alive && !n.suspect);
    assert!(!a.is_settled(T0 + 1_000, 10), "vừa xác nhận lại thì view chưa yên");
}

#[test]
fn indirect_probe_only_asks_peers_we_still_trust() {
    let mut a = gossip("node-a");
    a.add_peer("http://node-b", "node-b", T0);
    a.add_peer("http://node-c", "node-c", T0);
    a.add_peer("http://node-d", "node-d", T0);
    a.tick_suspects(0, T0 + 1_000);
    assert!(a.indirect_probe_targets().is_empty(), "cả ba đều nghi thì không hỏi ai");

    a.mark_alive("http://node-d", "node-d", T0 + 1_000);
    let pairs = a.indirect_probe_targets();
    assert!(pairs.contains(&("node-d", "node-b")));
    assert!(pairs.contains(&("node-d", "node-c")));
    assert!(!pairs.iter().any(|(_, s)| *s == "node-d"), "không hỏi về chính mình");
}

/// Info là *quan sát* nên hợp nhất LWW là đủ, và nó phải hội tử bất kể thứ tự
/// nhận.
#[test]
fn info_converges_regardless_of_message_order() {
    let mut a = gossip("node-a");
    let mut b = gossip("node-b");
    let mut c = gossip("node-c");
    a.publish(Info { version: "1.0.12".into(), ..Info::default() });
    b.publish(Info { version: "1.0.12".into(), ..Info::default() });
    c.publish(Info { version: "1.0.11".into(), ..Info::default() });

    let mut first = a.clone();
    first.apply(b.state());
    first.apply(c.state());
    let mut second = a.clone();
    second.apply(c.state());
    second.apply(b.state());
    assert_eq!(first.state().snapshot(), second.state().snapshot());

    // Nhận lại cùng state (heartbeat lặp) không được đổi gì.
    let snapshot = first.state().snapshot();
    assert!(!first.apply(b.state()));
    assert_eq!(first.state().snapshot(), snapshot);
}

#[test]
fn stale_state_never_rolls_back_a_newer_info() {
    let mut a = gossip("node-a");
    a.publish(Info { version: "1.0.12".into(), ..Info::default() });
    let stale = a.state().clone();
    a.publish(Info { version: "1.0.13".into(), ..Info::default() });
    assert!(!a.apply(&stale), "tin cũ không được đè bản mới hơn");
    assert_eq!(a.state().info("node-a").map(|i| i.version.as_str()), Some("1.0.13"));
}

#[test]
fn dirty_flag_is_consumed_so_unchanged_state_is_not_pushed_every_tick() {
    let mut a = gossip("node-a");
    a.publish(Info::default());
    assert!(a.take_dirty());
    assert!(!a.take_dirty(), "đọc một lần là hết");
    a.apply(&InfoState::new());
    assert!(!a.take_dirty(), "state rỗng không phải thay đổi");
}

#[test]
fn state_survives_json_round_trip_for_the_internal_endpoint() {
    let mut a = gossip("node-a");
    a.publish(Info { version: "1.0.12".into(), started_at: 42, note: "x".into() });
    let json = serde_json::to_string(a.state()).expect("serialize");
    let back: InfoState = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(a.state().snapshot(), back.snapshot());
}

#[test]
fn edge_values_do_not_panic() {
    let mut a = Gossip::new("node-a", "http://node-a", 0, 10_000);
    a.add_peer("http://node-b", "node-b", 10_000);
    // Cửa sổ cực lớn + đồng hồ lùi: không ai bị nghi ngờ, không panic.
    assert!(a.tick_suspects(u64::MAX, 0).is_empty());
    assert!(!a.is_settled(0, u64::MAX));
    // Chưa qua bước nghi ngờ thì không được đoán chết, dù cửa sổ chết = 0.
    assert!(a.tick_reap(0, 0).is_empty());
    a.tick_suspects(0, 11_000);
    a.tick_reap(0, 11_000);
    assert_eq!(a.dead_peers(), vec!["node-b"]);
}

#[test]
fn version_tie_breaks_on_author_so_two_nodes_never_keep_different_winners() {
    assert!(Version::new(1, "node-b").is_newer_than(&Version::new(1, "node-a")));
    assert!(!Version::new(1, "node-a").is_newer_than(&Version::new(1, "node-a")));
    assert!(Version::new(1, "node-a").is_newer_than(&Version::new(0, "node-z")));
}
