//! Cluster membership, state và election.
//!
//! Một struct duy nhất — [`Mesh`] — giữ toàn bộ trạng thái của node: các cụm
//! nó biết, view về peer, và vai trò hiện tại. Các kiểu còn lại chỉ là **dữ liệu
//! trao đổi qua API** (`Cluster`, `ClusterState`, `Version`) hoặc enum nhỏ.
//!
//! Module này **không** biết gì về pipeline, `Runtime`, HTTP hay socket; vận
//! chuyển nằm ở `opsense::cluster` (xem plan `mesh-cluster.md`). Nhờ vậy test ở
//! đây chạy không cần mạng và không cần mock HTTP.

mod state;
mod view;

pub use state::{Cluster, ClusterState, Version};
use view::Membership;

/// Vai trò của node này.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Role {
    /// Đang chạy pipeline của cụm.
    Master,
    /// Chờ thay master (hoặc chưa thuộc cụm nào).
    Standby,
}

/// Trạng thái mesh của một node — đây là struct chính.
#[derive(Debug, Clone)]
pub struct Mesh {
    node_id: String,
    /// URL của chính node này — node nào cần biết cũng hỏi seed.
    own_url: String,
    state: ClusterState,
    view: Membership,
    /// Cụm mà node này đang phục vụ (nếu thuộc cụm nào).
    cluster_id: Option<String>,
    role: Role,
    /// Mốc lần cuối view đổi — chờ hết `settle_secs` mới bầu lại.
    settled_since_ms: u64,
    /// `state` đã đổi kể từ lần gọi [`Mesh::take_dirty`] gần nhất.
    dirty: bool,
}

impl Mesh {
    /// `quorum` = số peer tối thiểu phải xác nhận "không thấy" node thứ k trước
    /// khi kết luận chết. `0` nghĩa là cụm một node, không cần xác nhận.
    ///
    /// `own_url` là URL mà node khác sẽ gọi tới node này (nằm trong
    /// `[mesh].seeds` hoặc khai ở config); nó xuất hiện trong roster trả về cho
    /// node mới.
    #[must_use]
    pub fn new(node_id: impl Into<String>, own_url: impl Into<String>, quorum: u32, now_ms: u64) -> Self {
        Self {
            node_id: node_id.into(),
            own_url: own_url.into(),
            state: ClusterState::new(),
            view: Membership::new(quorum),
            cluster_id: None,
            role: Role::Standby,
            settled_since_ms: now_ms,
            dirty: false,
        }
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Cụm node này đang phục vụ.
    #[must_use]
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// State để đẩy lên peer (`GET/POST /internal/state`).
    #[must_use]
    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    /// `true` nếu state đã đổi từ lần gọi gần nhất — dùng để quyết định có đẩy
    /// tiếp không (tránh POST mỗi tick khi không có gì mới).
    pub fn take_dirty(&mut self) -> bool {
        std::mem::take(&mut self.dirty)
    }

    // ── quản lý cụm ─────────────────────────────────────────────────────────

    /// Tạo cụm và đưa node này vào làm thành viên.
    pub fn create_cluster(
        &mut self,
        id: impl Into<String>,
        members: impl IntoIterator<Item = String>,
        pipeline: impl Into<String>,
        now_ms: u64,
    ) {
        let mut cluster = Cluster::solo(id, self.node_id.clone(), pipeline);
        cluster.members.extend(members);
        self.cluster_id = Some(cluster.id.clone());
        self.state.set_local(&self.node_id, cluster);
        self.settled_since_ms = now_ms;
        self.dirty = true;
    }

    /// Hợp nhất state nhận từ peer. Trả `true` nếu có thay đổi thật.
    ///
    /// Cụm mà node này thuộc về có thể lần đầu xuất hiện ở đây (nó được gán từ
    /// xa) — đánh dấu view chưa ổn định để chờ hết settle rồi mới bầu.
    pub fn apply(&mut self, remote: &ClusterState, now_ms: u64) -> bool {
        let mut joined_cluster = false;
        for cluster in remote.iter() {
            if cluster.members.contains(&self.node_id) {
                let known = self.cluster_id.as_deref() == Some(cluster.id.as_str());
                if !known {
                    self.cluster_id = Some(cluster.id.clone());
                    joined_cluster = true;
                }
            }
        }
        if self.state.apply(remote) {
            self.dirty = true;
        }
        if joined_cluster {
            self.settled_since_ms = now_ms;
        }
        self.state.apply(remote)
    }

    // ── quan sát peer ───────────────────────────────────────────────────────

    /// Thấy peer trả lời. Một lần thành công **xoá** mọi nghi ngờ trước đó —
    /// hành vi quan trọng nhất để mạng chập chờn không làm cụm bầu cử liên tục.
    pub fn mark_alive(&mut self, url: &str, node_id: &str, now_ms: u64) {
        if node_id == self.node_id {
            return;
        }
        if self.view.mark_alive(url, node_id, now_ms) {
            self.settled_since_ms = now_ms;
        }
    }

    /// Cấu hình ban đầu từ seed: biết peer trước khi gọi health lần nào.
    pub fn add_peer(&mut self, url: &str, node_id: &str, now_ms: u64) {
        self.mark_alive(url, node_id, now_ms);
    }

    /// Node mới báo "tôi vào mesh" (xử lý `POST /internal/join` ở phía **seed**).
    ///
    /// Phải đăng ký node mới vào view, nếu không nó sẽ vô hình với các node khác
    /// và không bao giờ được ai bầu làm master.
    pub fn handle_join(&mut self, url: &str, node_id: &str, now_ms: u64) -> Vec<NodeInfo> {
        self.add_peer(url, node_id, now_ms);
        self.roster()
    }

    /// Roster trả về cho node mới: **gồm cả chính node này**.
    ///
    /// Không tự xuất hiện trong `nodes()` (node không tự ping chính mình qua
    /// HTTP), nhưng node mới cần biết chính seed để ping lại — thiếu nó thì node
    /// mới không bao giờ thấy ai và cụm sẽ đứng yên.
    #[must_use]
    pub fn roster(&self) -> Vec<NodeInfo> {
        let mut out = vec![NodeInfo {
            node_id: self.node_id.clone(),
            url: self.own_url.clone(),
            alive: true,
            suspect: false,
        }];
        out.extend(self.nodes());
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        out
    }

    /// Danh sách peer cần gọi `/health/live` ở tick tới.
    ///
    /// Gồm cả node đang nghi ngờ: một lần trả lời là xoá nghi ngờ, và đó là thứ
    /// giữ cho mạng chập chờn không làm cụm bầu cử liên tục. Node đã `Dead` thì
    /// bỏ khỏi danh sách để không hỏi vô ích.
    #[must_use]
    pub fn probe_targets(&self) -> Vec<String> {
        self.view.probe_targets()
    }

    /// Các node cần hỏi "mày còn thấy `X` không" cho những node đang nghi ngờ
    /// (indirect probe).
    #[must_use]
    pub fn indirect_probe_targets(&self) -> Vec<(&str, &str)> {
        self.view.indirect_probe_targets(&self.node_id)
    }

    /// Không thấy peer trong `window_ms` ⇒ chuyển sang nghi ngờ. Trả về danh
    /// sách vừa chuyển (để log/khởi động indirect probe).
    pub fn tick_suspects(&mut self, window_ms: u64, now_ms: u64) -> Vec<String> {
        let newly = self.view.tick_suspects(window_ms, now_ms);
        if !newly.is_empty() {
            self.settled_since_ms = now_ms;
        }
        newly
    }

    /// Node `reporter` báo "không thấy `target`" (indirect probe). Chỉ tăng bộ
    /// đếm xác nhận — kết luận chết do [`Mesh::tick_reap`] đưa ra.
    pub fn report_missing(&mut self, reporter: &str, target: &str) {
        self.view.report_missing(reporter, target);
    }

    /// Tái thu node im lặng quá `dead_after_ms`: `Suspect → Dead`. Trả danh
    /// sách vừa chết.
    pub fn tick_reap(&mut self, dead_after_ms: u64, now_ms: u64) -> Vec<String> {
        let newly = self.view.tick_reap(dead_after_ms, now_ms);
        if !newly.is_empty() {
            self.settled_since_ms = now_ms;
        }
        newly
    }

    /// View hiện tại, để hiển thị qua `GET /nodes`.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.view.nodes()
    }

    // ── bầu vai trò ─────────────────────────────────────────────────────────

    /// Có đủ điều kiện bầu chưa? Chưa đủ thì giữ vai trò cũ — đây là chỗ chặn
    /// split-brain khi hai node cùng khởi động và đều tưởng mình cô lập.
    #[must_use]
    pub fn is_settled(&self, now_ms: u64, settle_secs: u64) -> bool {
        now_ms.saturating_sub(self.settled_since_ms) >= settle_secs.saturating_mul(1_000)
    }

    /// Bầu lại vai trò. Trả `true` nếu vai trò **đổi** — đây là tín hiệu duy
    /// nhất để `AppState` bật/dừng pipeline.
    pub fn refresh_role(&mut self, now_ms: u64, settle_secs: u64) -> bool {
        if !self.is_settled(now_ms, settle_secs) {
            return false;
        }
        let Some(id) = self.cluster_id.clone() else {
            let changed = self.role == Role::Master;
            self.role = Role::Standby;
            return changed;
        };
        let Some(cluster) = self.state.cluster(&id) else {
            return false;
        };
        // Còn thành viên nào chưa rõ sống chết (chưa từng gặp, hoặc đang bị
        // nghi ngờ) ⇒ không bầu. Xem `Membership::is_unresolved`.
        if cluster
            .members
            .iter()
            .any(|id| self.view.is_unresolved(id, &self.node_id))
        {
            return false;
        }
        let master = self.view.master_of(cluster, &self.node_id);
        let next = if master.as_deref() == Some(self.node_id.as_str()) {
            Role::Master
        } else {
            Role::Standby
        };
        if next == self.role {
            return false;
        }
        self.role = next;
        if next == Role::Master {
            // Lên master ⇒ tăng `epoch` để fencing chặn node cũ ghi đè.
            if let Some(cluster) = self.state.cluster_mut(&id) {
                cluster.epoch = cluster.epoch.saturating_add(1).max(1);
            }
            self.dirty = true;
        }
        true
    }

    /// Node đang giữ vai trò master của cụm `id` (theo view hiện tại).
    #[must_use]
    pub fn master_of(&self, id: &str) -> Option<String> {
        self.state.cluster(id).and_then(|c| self.view.master_of(c, &self.node_id))
    }

    /// `epoch` hiện tại của cụm — dùng làm fencing token khi ghi state.
    #[must_use]
    pub fn epoch_of(&self, id: &str) -> u64 {
        self.state.cluster(id).map_or(0, |c| c.epoch)
    }
}

/// Một dòng cho `GET /api/cluster/v1/nodes`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub url: String,
    /// Còn trả lời health.
    pub alive: bool,
    /// Đã quá hạn, **chưa** kết luận chết.
    pub suspect: bool,
}
