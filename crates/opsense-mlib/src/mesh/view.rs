//! View về peer — **chi tiết triển khai**, không public.
//!
//! Cơ chế phát hiện chết là gọi `/health/live` của peer theo chu kỳ. Vấn đề
//! kinh điển là **false positive**: một lần timeout không có nghĩa node chết, chỉ
//! có thể là mạng chập chờn. Vì vậy một node không bao giờ bị coi là chết ngay:
//!
//! 1. quá `suspect_secs` không trả lời ⇒ sang `Suspect` (chỉ là giả thuyết);
//! 2. node hỏi **node khác** xem có thấy node đó không — *indirect probe*;
//! 3. im lặng quá `dead_secs` (hoặc đủ số peer xác nhận "không thấy") ⇒ `Dead`.
//!
//! Bước 3 **không** dùng quy tắc "đủ n/2+1 xác nhận": node sống sót không lấy
//! được xác nhận từ node đã chết, nên quy tắc đó sẽ kẹt vô hạn đúng lúc cần
//! promote nhất. Thời gian im lặng là điều kiện quyết định, xác nhận chỉ rút ngắn.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{NodeInfo, state::Cluster};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Alive,
    /// Quá hạn không trả lời — chưa kết luận chết.
    Suspect,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub url: String,
    pub node_id: String,
    pub state: NodeState,
    /// Mili-giây (đồng hồ đơn điệu của caller) lần cuối thấy còn sống.
    pub last_seen_ms: u64,
    /// Số peer đã báo "không thấy node này".
    pub confirmations: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Membership {
    peers: BTreeMap<String, Peer>,
    /// Số xác nhận tối thiểu trước khi kết luận chết; `0`/`1` = không cần.
    quorum: u32,
}

impl Membership {
    #[must_use]
    pub fn new(quorum: u32) -> Self {
        Self { peers: BTreeMap::new(), quorum: quorum.max(1) }
    }

    /// Ghi nhận peer sống. Trả `true` nếu trạng thái thực sự đổi.
    pub fn mark_alive(&mut self, url: &str, node_id: &str, now_ms: u64) -> bool {
        match self.peers.get_mut(node_id) {
            Some(peer) => {
                let changed = peer.state != NodeState::Alive;
                peer.url = url.to_string();
                peer.state = NodeState::Alive;
                peer.last_seen_ms = now_ms;
                peer.confirmations = 0;
                changed
            }
            None => {
                self.peers.insert(
                    node_id.to_string(),
                    Peer {
                        url: url.to_string(),
                        node_id: node_id.to_string(),
                        state: NodeState::Alive,
                        last_seen_ms: now_ms,
                        confirmations: 0,
                    },
                );
                true
            }
        }
    }

    /// Không thấy peer trong `window_ms` ⇒ sang `Suspect`. Trả vên danh sách vừa
    /// chuyển.
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

    /// Node `reporter` báo "không thấy `target`" (kết quả indirect probe).
    ///
    /// Đây chỉ là **đường tắt** cho [`Membership::tick_reap`], không phải điều
    /// kiện đủ để kết luận chết — xem giải thích ở đầu module.
    ///
    /// Xác nhận từ node đã `Dead` bị bỏ qua: node chết không có ý kiến đáng tin.
    pub fn report_missing(&mut self, reporter: &str, target: &str) {
        if self.peers.get(reporter).is_some_and(|p| p.state == NodeState::Dead) {
            return;
        }
        if let Some(peer) = self.peers.get_mut(target)
            && peer.state == NodeState::Suspect
        {
            peer.confirmations += 1;
        }
    }

    /// Tái thu: `Suspect → Dead` khi im lặng quá `dead_after_ms`, **hoặc** đã đủ
    /// `quorum` peer xác nhận "không thấy".
    ///
    /// Chỉ xét node đã sang `Suspect` — chưa có chu kỳ im lặng nào thì chưa đoán.
    pub fn tick_reap(&mut self, dead_after_ms: u64, now_ms: u64) -> Vec<String> {
        let mut newly = Vec::new();
        for peer in self.peers.values_mut() {
            if peer.state != NodeState::Suspect {
                continue;
            }
            let silent = now_ms.saturating_sub(peer.last_seen_ms);
            if silent > dead_after_ms || peer.confirmations >= self.quorum {
                peer.state = NodeState::Dead;
                newly.push(peer.node_id.clone());
            }
        }
        newly
    }

    /// URL cần gọi `/health/live` ở tick tới (chưa `Dead`).
    #[must_use]
    pub fn probe_targets(&self) -> Vec<String> {
        self.peers
            .values()
            .filter(|p| p.state != NodeState::Dead)
            .map(|p| p.url.clone())
            .collect()
    }

    /// Cặp `(peer cần hỏi, node đang bị nghi)` cho indirect probe: với mỗi node
    /// đang `Suspect`, hỏi một peer còn sống khác xem có thấy nó không.
    ///
    /// Bỏ qua peer cũng đang bị nghi — hỏi node mà ta không tin thì vô nghĩa.
    #[must_use]
    pub fn indirect_probe_targets(&self, self_node_id: &str) -> Vec<(&str, &str)> {
        let suspects: Vec<&str> = self
            .peers
            .values()
            .filter(|p| p.state == NodeState::Suspect)
            .map(|p| p.node_id.as_str())
            .collect();
        let reporters: Vec<&str> = self
            .peers
            .values()
            .filter(|p| p.state == NodeState::Alive && p.node_id != self_node_id)
            .map(|p| p.node_id.as_str())
            .collect();
        let mut out = Vec::new();
        for suspect in suspects {
            for reporter in &reporters {
                if *reporter != suspect {
                    out.push((*reporter, suspect));
                }
            }
        }
        out
    }

    /// Danh sách peer cho API, sắp theo `node_id` cho output ổn định.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeInfo> {
        let mut out: Vec<NodeInfo> = self
            .peers
            .values()
            .map(|p| NodeInfo {
                node_id: p.node_id.clone(),
                url: p.url.clone(),
                alive: p.state == NodeState::Alive,
                suspect: p.state == NodeState::Suspect,
            })
            .collect();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        out
    }

    /// Thành viên mà ta **chưa phân giải** được: không có trong view (chưa từng
    /// gọi) hoặc đang ở trạng thái nghi ngờ.
    ///
    /// Đây là mấu chốt chống split-brain: node nhỏ nhất mà đang bị nghi ngờ vẫn
    /// có thể tự tin là master, nên **ta tuyệt đối không tự làm master khi còn
    /// thành viên nào chưa rõ sống chết**. Chờ tới khi mọi thành viên được phân
    /// giải thành sống hoặc chết thì mới bầu — lúc đó kết luận là tất định và
    /// không node nào có thể cùng lúc tin mình là master.
    #[must_use]
    pub fn is_unresolved(&self, node_id: &str, self_node_id: &str) -> bool {
        if node_id == self_node_id {
            return false;
        }
        self.peers.get(node_id).is_none_or(|p| p.state != NodeState::Alive && p.state != NodeState::Dead)
    }

    /// Master của cụm: node có `node_id` nhỏ nhất trong số thành viên **đang
    /// sống**.
    ///
    /// Vì là hàm thuần của `(cluster, view)`, hai node thấy cùng view thì chắc
    /// chắn cùng kết luận — không cần vòng bầu cử trao đổi. Đổi lại: khi mạng
    /// đứt đôi thì hai bên đều có thể tin mình là master, và đó là lý do mọi
    /// lần ghi state đều phải mang `epoch` (fencing).
    #[must_use]
    pub fn master_of(&self, cluster: &Cluster, self_node_id: &str) -> Option<String> {
        cluster
            .members
            .iter()
            // Node này luôn sống theo cách nó tự biết — không cần, và không
            // thể, tự ping qua HTTP. Bỏ qua điều này thì không node nào bao giờ
            // đủ điều kiện làm master vì bản thân nó không có trong view.
            .filter(|id| {
                *id == self_node_id
                    || self.peers.get(*id).is_some_and(|p| p.state == NodeState::Alive)
            })
            .min()
            .cloned()
    }
}
