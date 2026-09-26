//! Gossip — **quan sát**: ai còn sống, ai chạy version nào.
//!
//! Đối xứng với [`crate::raft`]:
//!
//! | | gossip | raft |
//! |---|---|---|
//! | câu hỏi | ai còn sống? | ai là master? |
//! | tính chất | suy đoán, chấp nhận tạm sai | quyết định, **không được sai** |
//! | cơ chế | ping định kỳ + hợp nhất LWW | log nhân bản + quorum |
//!
//! Vì sao phải tách: gossip **không** bảo đảm được "chỉ một master" khi mạng đứt
//! đôi — hai bên đều có thể tin mình còn sống. Chỉ Raft mới giải được. Ngược
//! lại, cấu hình chỉ là *quan sát* thì LWW là đúng và rẻ: hai node tạm thời giữ
//! version khác nhau vẫn hành xử đúng, và hội tử khi tin đến nhanh hơn.
//!
//! **Đừng bao giờ suy ra master từ dữ liệu ở đây.** Không I/O — vận chuyển nằm ở
//! `opsense::cluster`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ────────────────────────────── info được công bố ──────────────────────────────

/// Phiên bản của một bản ghi, dùng để hợp nhất LWW.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    /// Đồng hồ Lamport của tác giả.
    pub lamport: u64,
    /// Node đã ghi bản ghi — phá thế hoà bằng thứ tự từ điển.
    pub author: String,
}

impl Version {
    #[must_use]
    pub fn new(lamport: u64, author: impl Into<String>) -> Self {
        Self { lamport, author: author.into() }
    }

    /// `true` nếu `self` mới hơn `other`; bằng nhau thì so `author`.
    #[must_use]
    pub fn is_newer_than(&self, other: &Self) -> bool {
        (self.lamport, &self.author) > (other.lamport, &other.author)
    }
}

/// Info do một node công bố.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Info {
    /// Phiên bản binary — phát hiện lẫn nhau giữa các bản build.
    #[serde(default)]
    pub version: String,
    /// Mốc lúc process khởi động (unix giây) — phân biệt restart với còn chạy.
    #[serde(default)]
    pub started_at: u64,
    /// Ghi chú tuỳ ý, chỉ để hiển thị.
    #[serde(default)]
    pub note: String,
}

/// Toàn bộ info một node biết — **payload trao đổi qua API**
/// (`GET/POST /api/cluster/v1/internal/state`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfoState {
    /// Lamport lớn nhất từng thấy.
    pub lamport: u64,
    entries: BTreeMap<String, (Info, Version)>,
}

impl InfoState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn info(&self, node_id: &str) -> Option<&Info> {
        self.entries.get(node_id).map(|(i, _)| i)
    }

    /// Ghi đè info của chính mình: tăng lamport rồi đặt bản ghi.
    pub fn set_local(&mut self, node_id: &str, info: Info) {
        // `saturating_add` thay vì `+= 1`: bản ghi từ peer mang lamport tuỳ ý,
        // và tràn số ở đây sẽ phá thứ tự so sánh của toàn cụm.
        self.lamport = self.lamport.saturating_add(1);
        let version = Version::new(self.lamport, node_id);
        self.entries.insert(node_id.to_string(), (info, version));
    }

    /// Hợp nhất info nhận từ peer. Trả `true` nếu có thay đổi thật.
    pub fn apply(&mut self, remote: &InfoState) -> bool {
        let mut changed = false;
        self.lamport = self.lamport.max(remote.lamport);
        for (id, (info, version)) in &remote.entries {
            match self.entries.get(id) {
                None => {
                    self.entries.insert(id.clone(), (info.clone(), version.clone()));
                    changed = true;
                }
                Some((_, local)) if version.is_newer_than(local) => {
                    self.entries.insert(id.clone(), (info.clone(), version.clone()));
                    changed = true;
                }
                Some(_) => {}
            }
        }
        if changed {
            self.lamport = self.lamport.saturating_add(1);
        }
        changed
    }

    /// **Nội dung** info, bỏ qua lamport — dùng để so sánh "hai node đã hội tử
    /// chưa" mà không vướng đồng hồ.
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<String, Info> {
        self.entries.iter().map(|(id, (i, _))| (id.clone(), i.clone())).collect()
    }
}

// ────────────────────────────────── view ───────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Alive,
    /// Quá hạn không trả lời — **chưa** kết luận chết.
    Suspect,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Peer {
    url: String,
    state: NodeState,
    /// Mili-giây theo đồng hồ đơn điệu của caller.
    last_seen_ms: u64,
    /// Số peer đã báo "không thấy node này".
    confirmations: u32,
}

/// Một dòng cho `GET /api/cluster/v1/nodes` — quan sát được, **không** phải quyết
/// định.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub url: String,
    /// Còn trả lời health.
    pub alive: bool,
    /// Đã quá hạn, chưa kết luận chết.
    pub suspect: bool,
    /// Info node công bố; rỗng nếu node mới chưa kịp đẩy.
    pub info: Info,
}

// ──────────────────────────── gốc của cả module ─────────────────────────────────

/// View của node này về mesh, cộng info mà node đó công bố.
#[derive(Debug, Clone)]
pub struct Gossip {
    node_id: String,
    /// URL của chính node này — node khác sẽ gọi tới.
    own_url: String,
    published: Info,
    known: InfoState,
    peers: BTreeMap<String, Peer>,
    /// Số xác nhận để **rút ngắn** chờ; kết luận chết vẫn theo thời gian.
    quorum: u32,
    /// Mốc lần cuối view đổi — dùng để *trì hoãn* hành động, không quyết định.
    settled_since_ms: u64,
    dirty: bool,
}

impl Gossip {
    /// `quorum` chỉ dùng để rút ngắn việc chờ. Quy tắc "đủ n/2+1 xác nhận" sẽ
    /// **vô dụng** ở đây: node sống sót không lấy được xác nhận từ node đã chết,
    /// nên nó sẽ kẹt vô hạn đúng lúc cần biết node nào chết nhất.
    #[must_use]
    pub fn new(node_id: impl Into<String>, own_url: impl Into<String>, quorum: u32, now_ms: u64) -> Self {
        Self {
            node_id: node_id.into(),
            own_url: own_url.into(),
            published: Info::default(),
            known: InfoState::new(),
            peers: BTreeMap::new(),
            quorum: quorum.max(1),
            settled_since_ms: now_ms,
            dirty: false,
        }
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    #[must_use]
    pub fn own_url(&self) -> &str {
        &self.own_url
    }

    /// View đã ổn định chưa — dùng để trì hoãn, không dùng để quyết định master.
    #[must_use]
    pub fn is_settled(&self, now_ms: u64, settle_secs: u64) -> bool {
        now_ms.saturating_sub(self.settled_since_ms) >= settle_secs.saturating_mul(1_000)
    }

    // ── info của chính mình ────────────────────────────────────────────────

    #[must_use]
    pub fn published(&self) -> &Info {
        &self.published
    }

    /// Cập nhật info của chính mình và đánh dấu state cần đẩy cho peer.
    pub fn publish(&mut self, info: Info) {
        self.published = info.clone();
        self.known.set_local(&self.node_id, info);
        self.dirty = true;
    }

    /// State để gửi cho peer: info của mình cộng info của các node tôi thấy.
    #[must_use]
    pub fn state(&self) -> &InfoState {
        &self.known
    }

    /// Hợp nhất info nhận từ peer.
    pub fn apply(&mut self, remote: &InfoState) -> bool {
        let changed = self.known.apply(remote);
        self.dirty |= changed;
        changed
    }

    /// `true` nếu state đã đổi từ lần gọi gần nhất — tránh đẩy thừa mỗi tick.
    pub fn take_dirty(&mut self) -> bool {
        std::mem::take(&mut self.dirty)
    }

    // ── join ────────────────────────────────────────────────────────────────

    /// Node mới báo "tôi vào mesh" (xử lý `POST /internal/join` phía **seed**).
    ///
    /// Phải đăng ký node mới vào view, nếu không nó vô hình với các node khác và
    /// không bao giờ được ping.
    pub fn handle_join(&mut self, url: &str, node_id: &str, now_ms: u64) -> Vec<NodeInfo> {
        self.add_peer(url, node_id, now_ms);
        self.roster()
    }

    /// Roster trả về cho node mới: **gồm cả chính node này**.
    ///
    /// Node không tự ping chính mình nên không có trong [`Gossip::nodes`], nhưng
    /// node mới cần biết seed để ping lại — thiếu nó thì node mới không bao giờ
    /// thấy ai.
    #[must_use]
    pub fn roster(&self) -> Vec<NodeInfo> {
        let mut out = vec![NodeInfo {
            node_id: self.node_id.clone(),
            url: self.own_url.clone(),
            alive: true,
            suspect: false,
            info: self.published.clone(),
        }];
        out.extend(self.nodes());
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        out
    }

    // ── quan sát ────────────────────────────────────────────────────────────

    /// Cấu hình ban đầu từ seed: biết peer trước khi gọi health lần nào.
    pub fn add_peer(&mut self, url: &str, node_id: &str, now_ms: u64) {
        self.mark_alive(url, node_id, now_ms);
    }

    /// Thấy peer trả lời. Một lần thành công **xoá** mọi nghi ngờ trước đó —
    /// quan trọng nhất để mạng chập chờn không gây giật đình liên tục.
    pub fn mark_alive(&mut self, url: &str, node_id: &str, now_ms: u64) {
        if node_id == self.node_id {
            return;
        }
        let changed = match self.peers.get_mut(node_id) {
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
                    Peer { url: url.to_string(), state: NodeState::Alive, last_seen_ms: now_ms, confirmations: 0 },
                );
                true
            }
        };
        if changed {
            self.settled_since_ms = now_ms;
        }
    }

    /// Không thấy peer trong `window_ms` ⇒ nghi ngờ. Trả danh sách vừa chuyển.
    pub fn tick_suspects(&mut self, window_ms: u64, now_ms: u64) -> Vec<String> {
        let newly = self.suspect_stale(window_ms, now_ms);
        if !newly.is_empty() {
            self.settled_since_ms = now_ms;
        }
        newly
    }

    /// Tái thu: nghi ngờ → chết khi im lặng quá `dead_after_ms`, hoặc đã đủ
    /// `quorum` peer xác nhận "không thấy".
    pub fn tick_reap(&mut self, dead_after_ms: u64, now_ms: u64) -> Vec<String> {
        let mut newly = Vec::new();
        for (id, peer) in &mut self.peers {
            if peer.state != NodeState::Suspect {
                continue;
            }
            if now_ms.saturating_sub(peer.last_seen_ms) > dead_after_ms
                || peer.confirmations >= self.quorum
            {
                peer.state = NodeState::Dead;
                newly.push(id.clone());
            }
        }
        if !newly.is_empty() {
            self.settled_since_ms = now_ms;
        }
        newly
    }

    /// Node `reporter` báo "không thấy `target`" (indirect probe) — chỉ tăng bộ
    /// đếm, không tự kết luận chết. Xác nhận từ node đã chết bị bỏ qua: node
    /// chết không có ý kiến đáng tin.
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

    /// URL cần gọi `/health/live` ở tick tới (chưa `Dead`).
    ///
    /// Giữ cả node đang nghi ngờ: một lần trả lời là xoá nghi ngờ, và đó là thứ
    /// giữ mạng chập chờn không gây giật đình.
    #[must_use]
    pub fn probe_targets(&self) -> Vec<String> {
        self.peers
            .values()
            .filter(|p| p.state != NodeState::Dead)
            .map(|p| p.url.clone())
            .collect()
    }

    /// Cặp `(peer hỏi, node đang bị nghi)` cho indirect probe. Bỏ qua peer cũng
    /// đang bị nghi — hỏi node ta không tin thì vô nghĩa.
    #[must_use]
    pub fn indirect_probe_targets(&self) -> Vec<(&str, &str)> {
        let suspects: Vec<&String> = self
            .peers
            .iter()
            .filter(|(_, p)| p.state == NodeState::Suspect)
            .map(|(id, _)| id)
            .collect();
        let reporters: Vec<&String> = self
            .peers
            .iter()
            .filter(|(id, p)| p.state == NodeState::Alive && *id != &self.node_id)
            .map(|(id, _)| id)
            .collect();
        let mut out = Vec::new();
        for suspect in suspects {
            for reporter in &reporters {
                if reporter.as_str() != suspect.as_str() {
                    out.push((reporter.as_str(), suspect.as_str()));
                }
            }
        }
        out
    }

    /// Node đang sống, không tính chính mình.
    #[must_use]
    pub fn alive_peers(&self) -> Vec<&str> {
        self.peers
            .iter()
            .filter(|(id, p)| p.state == NodeState::Alive && *id != &self.node_id)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Node đã chết.
    #[must_use]
    pub fn dead_peers(&self) -> Vec<&str> {
        self.peers
            .iter()
            .filter(|(_, p)| p.state == NodeState::Dead)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Danh sách node cho API, sắp theo `node_id` cho output ổn định.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeInfo> {
        let mut out: Vec<NodeInfo> = self
            .peers
            .iter()
            .map(|(id, p)| NodeInfo {
                node_id: id.clone(),
                url: p.url.clone(),
                alive: p.state == NodeState::Alive,
                suspect: p.state == NodeState::Suspect,
                info: self.known.info(id).cloned().unwrap_or_default(),
            })
            .collect();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        out
    }

    /// Bỏ node khỏi view (khi nó tự rời mesh).
    pub fn remove(&mut self, node_id: &str) {
        self.peers.remove(node_id);
    }

    fn suspect_stale(&mut self, window_ms: u64, now_ms: u64) -> Vec<String> {
        let mut newly = Vec::new();
        for (id, peer) in &mut self.peers {
            if peer.state == NodeState::Alive
                && now_ms.saturating_sub(peer.last_seen_ms) > window_ms
            {
                peer.state = NodeState::Suspect;
                peer.confirmations = 0;
                newly.push(id.clone());
            }
        }
        newly
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_tie_breaks_on_author() {
        assert!(Version::new(1, "node-b").is_newer_than(&Version::new(1, "node-a")));
        assert!(!Version::new(1, "node-a").is_newer_than(&Version::new(1, "node-a")));
        assert!(Version::new(1, "node-a").is_newer_than(&Version::new(0, "node-z")));
    }

    #[test]
    fn set_local_bumps_lamport_saturating() {
        let mut s = InfoState::new();
        s.set_local("me", Info::default());
        assert_eq!(s.lamport, 1);
        s.lamport = u64::MAX;
        s.set_local("me", Info::default());
        assert_eq!(s.lamport, u64::MAX, "phải dừng ở MAX, không tràn");
    }

    #[test]
    fn older_state_never_rolls_back_a_newer_one() {
        let mut a = InfoState::new();
        a.set_local("me", Info { version: "new".into(), ..Info::default() });
        let mut b = InfoState::new();
        b.set_local("me", Info { version: "old".into(), ..Info::default() });
        assert!(!a.apply(&b));
        assert_eq!(a.info("me").map(|i| i.version.as_str()), Some("new"));
    }

    #[test]
    fn self_is_never_its_own_peer() {
        let mut g = Gossip::new("node-a", "http://node-a", 1, 0);
        g.add_peer("http://node-a", "node-a", 0);
        assert!(g.nodes().is_empty());
    }

    #[test]
    fn success_clears_suspicion() {
        let mut g = Gossip::new("node-a", "http://node-a", 1, 0);
        g.add_peer("http://node-b", "node-b", 0);
        g.tick_suspects(1_000, 2_000);
        g.mark_alive("http://node-b", "node-b", 2_000);
        let n = g.nodes().into_iter().find(|n| n.node_id == "node-b").unwrap();
        assert!(n.alive && !n.suspect);
    }

    #[test]
    fn dead_only_after_silence_not_on_a_single_timeout() {
        let mut g = Gossip::new("node-a", "http://node-a", 1, 0);
        g.add_peer("http://node-b", "node-b", 0);
        g.tick_suspects(0, 1_000);
        assert!(g.nodes().iter().all(|n| n.alive || n.suspect), "chưa được kết luận chết");
        g.report_missing("node-c", "node-b");
        assert!(g.dead_peers().is_empty(), "lời xác nhận không được bỏ qua thời gian");
        g.tick_reap(30_000, 31_000);
        assert_eq!(g.dead_peers(), vec!["node-b"]);
    }

    #[test]
    fn dead_peers_drop_out_of_probe_targets_but_suspect_ones_stay() {
        let mut g = Gossip::new("node-a", "http://node-a", 1, 0);
        g.add_peer("http://node-b", "node-b", 0);
        g.tick_suspects(0, 1_000);
        assert_eq!(g.probe_targets().len(), 1, "suspect vẫn phải ping");
        g.tick_reap(0, 1_000);
        assert!(g.probe_targets().is_empty(), "dead thì bỏ khỏi danh sách ping");
    }

    #[test]
    fn info_converges_regardless_of_message_order() {
        let mut a = Gossip::new("node-a", "http://node-a", 1, 0);
        let mut b = Gossip::new("node-b", "http://node-b", 1, 0);
        let mut c = Gossip::new("node-c", "http://node-c", 1, 0);
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
    }

    #[test]
    fn state_survives_json_round_trip_for_the_internal_endpoint() {
        let mut g = Gossip::new("node-a", "http://node-a", 1, 0);
        g.publish(Info { version: "1.0.12".into(), started_at: 42, note: String::new() });
        let json = serde_json::to_string(g.state()).unwrap();
        let back: InfoState = serde_json::from_str(&json).unwrap();
        assert_eq!(g.state().snapshot(), back.snapshot());
    }

    #[test]
    fn edge_values_do_not_panic() {
        let mut g = Gossip::new("node-a", "http://node-a", 0, 10_000);
        g.add_peer("http://node-b", "node-b", 10_000);
        // Cửa sổ cực lớn + đồng hồ lùi: không ai bị nghi ngờ, không panic.
        assert!(g.tick_suspects(u64::MAX, 0).is_empty());
        assert!(!g.is_settled(0, u64::MAX));
        // Chưa qua bước nghi ngờ thì không được đoán chết, dù cửa sổ chết = 0.
        assert!(g.tick_reap(0, 0).is_empty());
        g.tick_suspects(0, 11_000);
        g.tick_reap(0, 11_000);
        assert_eq!(g.dead_peers(), vec!["node-b"]);
    }
}
