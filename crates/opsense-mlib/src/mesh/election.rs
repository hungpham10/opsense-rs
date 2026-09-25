//! Bầu master trong cụm — **tất định**, không cần Raft.
//!
//! Vì sao không dùng Raft: mỗi cụm phục vụ **một** pipeline và chỉ một node ghi
//! (xem plan §9). Thứ cần không phải đồng thuận nhiều writer, mà là đảm bảo
//! "chỉ một node tin là mình master". Ta đạt điều đó bằng hai quy tắc rẻ:
//!
//! 1. **Tất định**: master là node có `node_id` nhỏ nhất trong cụm mà vẫn sống.
//!    Hai node có cùng view thì kết luận **giống nhau**, không cần trao đổi
//!    thêm một vòng bầu cử nào.
//! 2. **Settle window**: không kết luận ngay khi view vừa đổi. Đây là chỗ chặn
//!    split-brain lúc khởi động — hai node cùng `cluster_id` bật cùng lúc đều
//!    thấy "chỉ có mình" nếu bầu ngay lập tức.
//!
//! Cái mà cách này **không** đảm bảo: khi mạng đứt đôi, cả hai bên đều có thể
//! tin mình là master. Vì vậy mọi lần ghi state đều mang [`Cluster::epoch`] và
//! bên có `epoch` cũ không đè được bên mới — đó là fencing.

use serde::{Deserialize, Serialize};

use super::state::Cluster;
use super::view::Membership;

/// Vai trò của node này với một cụm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Đang chạy pipeline của cụm.
    Master,
    /// Chờ thay master.
    Standby,
}

/// Kết quả bầu cử cho một cụm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Election {
    /// Node được bầu, `None` khi mọi thành viên đều đã chết.
    pub master: Option<String>,
    /// Node này có phải master không.
    pub role: Role,
    /// `epoch` cần dùng khi ghi state: giữ nguyên nếu đã là master, tăng nếu
    /// vừa lên master (lần đầu thì `1`).
    pub epoch: u64,
}

/// Bầu cử: master là node có `node_id` nhỏ nhất trong số thành viên còn sống.
///
/// Node không phải thành viên của cụm thì không có vai trò gì — nó chỉ quan
/// sát (`role = Standby` dù cụm vẫn có master).
///
/// Vì quy tắc là hàm thuần của `(cluster, view, self_id)`, hai node thấy cùng
/// một view thì **chắc chắn** cùng kết luận — không cần vòng bầu cử trao đổi.
#[must_use]
pub fn decide(cluster: &Cluster, membership: &Membership, self_node_id: &str) -> Election {
    let master = cluster
        .members
        .iter()
        .filter(|id| {
            membership.peers.get(*id).is_some_and(|p| p.state == super::view::NodeState::Alive)
        })
        .min()
        .cloned();

    match master {
        Some(m) if m == self_node_id => {
            Election { master: Some(m), role: Role::Master, epoch: cluster.epoch }
        }
        other => Election { master: other, role: Role::Standby, epoch: cluster.epoch },
    }
}

/// Cửa sổ chờ trước khi tin view là ổn định.
///
/// `since_change_ms` = mốc lần cuối view của cụm thay đổi (thêm/bớt thành viên
/// hoặc đổi trạng thái sống). Trả `false` nếu chưa đủ thời gian — lúc đó caller
/// giữ vai trò cũ, **không** bầu lại.
#[must_use]
pub fn is_settled(since_change_ms: u64, now_ms: u64, settle_secs: u64) -> bool {
    now_ms.saturating_sub(since_change_ms) >= settle_secs.saturating_mul(1_000)
}

/// Tăng `epoch` khi một node lên master.
///
/// Fencing ở chỗ ghi state: node có `epoch` cũ không đè được bản ghi của node đã
/// lên master mới. Vì `epoch` chỉ tăng, so sánh được là thứ tự, không cần đồng
/// hồ.
#[must_use]
pub fn promoted(mut cluster: Cluster) -> Cluster {
    cluster.epoch = cluster.epoch.saturating_add(1).max(1);
    cluster
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::state::Cluster;
    use crate::mesh::view::Membership;

    fn cluster() -> Cluster {
        let mut c = Cluster::solo("c1", "node-a", "grid");
        c.members.extend(["node-b".to_string(), "node-c".to_string()]);
        c
    }

    fn all_alive() -> Membership {
        let mut m = Membership::new(2);
        for id in ["node-a", "node-b", "node-c"] {
            m.mark_alive(&format!("http://{id}"), id, 0);
        }
        m
    }

    #[test]
    fn smallest_node_id_wins() {
        let e = decide(&cluster(), &all_alive(), "node-a");
        assert_eq!(e.master.as_deref(), Some("node-a"));
        assert_eq!(e.role, Role::Master);
        let e = decide(&cluster(), &all_alive(), "node-b");
        assert_eq!(e.role, Role::Standby);
    }

    #[test]
    fn dead_master_hands_over_to_next_smallest() {
        let mut m = all_alive();
        // node-c vẫn trả lời; chỉ a và b im lặng quá lâu.
        m.mark_alive("http://node-c", "node-c", 10_000);
        assert_eq!(m.tick_suspects(1_000, 10_000), vec!["node-a", "node-b"]);

        // node-c xác nhận "không thấy" cả hai, đủ quorum(2) cho từng node.
        for target in ["node-a", "node-b"] {
            m.report_missing("node-c", target);
            m.report_missing("node-c", target);
        }
        assert_eq!(m.peers["node-a"].state, crate::mesh::view::NodeState::Dead);
        assert_eq!(m.peers["node-b"].state, crate::mesh::view::NodeState::Dead);

        let e = decide(&cluster(), &m, "node-c");
        assert_eq!(e.master.as_deref(), Some("node-c"));
        assert_eq!(e.role, Role::Master);
    }

    #[test]
    fn suspect_is_not_eligible_to_be_master() {
        // Chỉ một nghi ngờ, chưa đủ xác nhận ⇒ cụm không được bầu nhầm node đó,
        // mà cứ bám master cũ đang sống.
        let mut m = all_alive();
        m.tick_suspects(0, 10_000);
        let e = decide(&cluster(), &m, "node-a");
        assert_eq!(e.master, None, "mọi thành viên đều đang suspect");
        assert_eq!(e.role, Role::Standby);
    }

    #[test]
    fn all_dead_means_no_master() {
        let mut m = all_alive();
        m.tick_suspects(0, 10_000);
        for target in ["node-a", "node-b", "node-c"] {
            m.report_missing("node-b", target);
            m.report_missing("node-c", target);
        }
        let e = decide(&cluster(), &m, "node-a");
        assert_eq!(e.master, None);
        assert_eq!(e.role, Role::Standby);
    }

    #[test]
    fn settle_window_blocks_immediate_decision() {
        assert!(!is_settled(0, 9_000, 10), "9s < 10s");
        assert!(is_settled(0, 10_000, 10));
        assert!(is_settled(0, 11_000, 10));
        // Đồng hồ lùi không được panic.
        assert!(!is_settled(5_000, 0, 10));
    }

    #[test]
    fn promotion_increments_epoch_monotonically() {
        let c = promoted(cluster());
        assert_eq!(c.epoch, cluster().epoch + 1);
        let c2 = promoted(c);
        assert!(c2.epoch > cluster().epoch);
    }

    #[test]
    fn outsider_has_no_role() {
        let e = decide(&cluster(), &all_alive(), "node-z");
        assert_eq!(e.role, Role::Standby);
        assert_eq!(e.master.as_deref(), Some("node-a"));
    }
}
