//! Quan sát thành viên: `alive → suspect → dead`.
//!
//! Cơ chế phát hiện chết là **gọi `/health/live` của peer theo chu kỳ** (xem
//! [`crate::mesh::election::Membership`]). Vấn đề kinh điển của cách này là
//! **false positive**: một lần timeout không có nghĩa node chết, chỉ có thể là
//! mạng chập chờn. Vì vậy một node không bao giờ bị coi là chết ngay:
//!
//! 1. quá `suspect_secs` không trả lời ⇒ sang `Suspect` (chỉ là giả thuyết);
//! 2. node hỏi **node khác** xem có thấy node đó không — *indirect probe*;
//! 3. đủ `quorum` (= phần lớn, tức `n/2 + 1`) số peer xác nhận "không thấy"
//!    ⇒ mới sang `Dead`.
//!
//! Quy tắc này rẻ hơn nhiều so với bầu cử Raft và đủ cho trường hợp dùng
//! (1 cụm = 1 pipeline, ghi chủ yếu là cache) — xem §9 của plan.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Trạng thái sống của một peer trong mắt node này.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Trả lời health gần đây.
    Alive,
    /// Quá hạn không trả lời — **chưa** kết luận chết, đang chờ xác nhận.
    Suspect,
    /// Đã có đủ peer xác nhận không thấy.
    Dead,
}

/// Một peer quan sát được.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// Base URL để gọi API của peer.
    pub url: String,
    pub node_id: String,
    pub state: NodeState,
    /// Mili-giây (đồng hồ đơn điệu của caller) lần cuối thấy còn sống.
    pub last_seen_ms: u64,
    /// Số peer đã báo "không thấy node này" trong vòng xác nhận.
    pub confirmations: u32,
}

impl Peer {
    #[must_use]
    pub fn new(url: impl Into<String>, node_id: impl Into<String>, now_ms: u64) -> Self {
        Self {
            url: url.into(),
            node_id: node_id.into(),
            state: NodeState::Alive,
            last_seen_ms: now_ms,
            confirmations: 0,
        }
    }
}

/// View của node này về toàn mesh.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub peers: BTreeMap<String, Peer>,
    /// Số peer phải xác nhận "không thấy" trước khi kết luận chết:
    /// phần lớn trong số peer đang **sống** — tức `n/2 + 1`.
    pub quorum: u32,
}

impl Membership {
    #[must_use]
    pub fn new(quorum: u32) -> Self {
        Self { peers: BTreeMap::new(), quorum: quorum.max(1) }
    }

    /// Thêm hoặc cập nhật peer khi vừa gọi health thành công.
    ///
    /// Một lần thành công **xoá** mọi nghi ngờ trước đó: đây là hành vi quan
    /// trọng nhất để mạng chập chờn không làm cụm bầu cử liên tục.
    pub fn mark_alive(&mut self, url: &str, node_id: &str, now_ms: u64) {
        match self.peers.get_mut(node_id) {
            Some(peer) => {
                peer.url = url.to_string();
                peer.state = NodeState::Alive;
                peer.last_seen_ms = now_ms;
                peer.confirmations = 0;
            }
            None => {
                self.peers
                    .insert(node_id.to_string(), Peer::new(url, node_id, now_ms));
            }
        }
    }

    /// Không thấy peer trong `window_ms` ⇒ chuyển sang `Suspect` nếu đang `Alive`.
    /// Trả `true` nếu vừa chuyển (dùng để phát sự kiện/log).
    pub fn tick_suspects(&mut self, window_ms: u64, now_ms: u64) -> Vec<String> {
        let mut newly = Vec::new();
        for peer in self.peers.values_mut() {
            if peer.state == NodeState::Alive
                && now_ms.saturating_sub(peer.last_seen_ms) > window_ms
            {
                peer.state = NodeState::Suspect;
                peer.confirmations = 0;
                newly.push(peer.node_id.clone());
            }
        }
        newly
    }

    /// Node `node_id` báo "tôi không thấy `target`".
    ///
    /// Đạt [`Membership::quorum`] xác nhận ⇒ chuyển `Suspect → Dead`. Xác nhận
    /// từ một node đã `Dead` bị bỏ qua — node chết không có ý kiến đáng tin.
    pub fn report_missing(&mut self, reporter: &str, target: &str) {
        let reporter_dead = self
            .peers
            .get(reporter)
            .is_some_and(|p| p.state == NodeState::Dead);
        if reporter_dead {
            return;
        }
        // `quorum = 0` nghĩa là không cần xác nhận nào (chế độ một node).
        if self.quorum <= 1 {
            if let Some(peer) = self.peers.get_mut(target) {
                if peer.state == NodeState::Suspect {
                    peer.state = NodeState::Dead;
                }
            }
            return;
        }
        if let Some(peer) = self.peers.get_mut(target) {
            if peer.state != NodeState::Suspect {
                return;
            }
            peer.confirmations += 1;
            if peer.confirmations >= self.quorum {
                peer.state = NodeState::Dead;
            }
        }
    }

    /// Số peer đang sống, **không** tính `self_node_id`.
    #[must_use]
    pub fn alive_peers(&self, self_node_id: &str) -> Vec<&Peer> {
        self.peers
            .values()
            .filter(|p| p.state == NodeState::Alive && p.node_id != self_node_id)
            .collect()
    }

    /// Tổng số peer **đang sống hoặc đang nghi** — cơ số của quorum.
    #[must_use]
    pub fn live_count(&self) -> u32 {
        u32::try_from(
            self.peers
                .values()
                .filter(|p| p.state != NodeState::Dead)
                .count(),
        )
        .unwrap_or(u32::MAX)
    }

    /// Bỏ peer khỏi view (khi nó tự rời cụm).
    pub fn remove(&mut self, node_id: &str) {
        self.peers.remove(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(quorum: u32) -> Membership {
        let mut m = Membership::new(quorum);
        m.mark_alive("http://a", "a", 0);
        m.mark_alive("http://b", "b", 0);
        m.mark_alive("http://c", "c", 0);
        m
    }

    #[test]
    fn success_clears_suspicion() {
        let mut m = view(2);
        assert_eq!(m.tick_suspects(1_000, 2_000), vec!["a", "b", "c"]);
        assert_eq!(m.peers["a"].state, NodeState::Suspect);
        m.mark_alive("http://a", "a", 2_000);
        assert_eq!(m.peers["a"].state, NodeState::Alive);
        assert_eq!(m.peers["a"].confirmations, 0);
    }

    #[test]
    fn dead_needs_quorum_confirmations() {
        let mut m = view(2);
        m.tick_suspects(1_000, 2_000);
        m.report_missing("b", "a");
        assert_eq!(m.peers["a"].state, NodeState::Suspect, "1 xác nhận chưa đủ");
        m.report_missing("c", "a");
        assert_eq!(m.peers["a"].state, NodeState::Dead);
    }

    #[test]
    fn report_from_dead_peer_is_ignored() {
        let mut m = view(2);
        m.tick_suspects(1_000, 2_000);
        m.report_missing("b", "a");
        m.report_missing("b", "a");
        // `b` bị coi là chết trong khi chính nó xác nhận `a` ⇒ phải bỏ qua.
        m.report_missing("c", "a");
        assert_eq!(m.peers["a"].state, NodeState::Dead);
        assert_eq!(m.peers["a"].confirmations, 2, "chỉ 2 xác nhận hợp lệ");
    }

    #[test]
    fn suspect_is_not_considered_alive() {
        let mut m = view(2);
        m.tick_suspects(1_000, 2_000);
        assert!(m.alive_peers("a").is_empty(), "self cũng bị loại khỏi danh sách peer");
        assert_eq!(m.live_count(), 3, "suspect vẫn còn trong cơ số quorum");
    }

    #[test]
    fn saturating_arithmetic_on_zero_clock() {
        let mut m = view(2);
        m.mark_alive("http://a", "a", 0);
        // now < last_seen (đồng hồ lùi) không được phải panic.
        assert!(m.tick_suspects(u64::MAX, 0).is_empty());
    }
}
