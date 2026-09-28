//! Raft — **quyết định**: cụm gồm node nào, ai là master.
//!
//! Đối xứng với [`crate::gossip`]. Vì sao phải tách: gossip **không** bảo đảm
//! được "chỉ một master" khi mạng đứt đôi — hai bên đều có thể tin mình còn
//! sống, và chỉ Raft (log + term + quorum) mới loại được. Ngược lại, đưa quyết
//! định vào gossip nghĩa là tự chế lại Raft một cách tệ hơn.
//!
//! **Một node đứng một mình thì không có cụm nào cả** — không cần bầu, không cần
//! log. Cụm chỉ tồn tại khi thực sự có người gom node vào (`POST /clusters`), tức
//! là khi đã cần quyết định "ai chạy pipeline".
//!
//! **Phần này KHÔNG tự cài đặt thuật toán Raft** — làm thế là ăn cướp tiến bộ
//! và làm nên nhiều bug. Ở đây chỉ có kiểu miền + trait [`Consensus`] làm khe
//! cắm cho engine (`openraft` sẽ cài sau). Chưa cắm engine thì cụm nhiều thành
//! viên **chưa ai làm master** — đó là trạng thái trung thực, không phải lỗi.
//!
//! Không I/O — vận chuyển nằm ở `opsense::cluster`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Vai trò của node này.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Đang chạy pipeline.
    Master,
    /// Chờ thay master.
    Standby,
}

/// Một cụm: tập node phục vụ **một** pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    pub id: String,
    pub members: BTreeSet<String>,
    pub pipeline: String,
    /// Tăng mỗi lần term tăng — dùng làm **fencing token**: node giữ master với
    /// `epoch` cũ không được ghi đè bản ghi của node giữ `epoch` mới.
    pub epoch: u64,
}

impl Cluster {
    /// Cụm một thành viên — chỉ dùng khi *người dùng* gom một node vào cụm tên;
    /// node đứng một mình thì không có cụm (xem [`Raft::solo`]).
    #[must_use]
    pub fn solo(id: impl Into<String>, node_id: impl Into<String>, pipeline: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            members: BTreeSet::from([node_id.into()]),
            pipeline: pipeline.into(),
            epoch: 0,
        }
    }

    #[must_use]
    pub fn has(&self, node_id: &str) -> bool {
        self.members.contains(node_id)
    }
}

/// Lệnh thay đổi cấu hình — thứ được ghi vào **log** và nhân bản tới mọi node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Command {
    /// Tạo cụm mới.
    Create { cluster: Cluster },
    /// Thêm node vào cụm.
    AddMember { cluster_id: String, node_id: String },
    /// Bỏ node khỏi cụm.
    RemoveMember { cluster_id: String, node_id: String },
    /// Gom các node vào một cụm (tạo nếu chưa có) — thao tác của
    /// `POST /api/cluster/v1/clusters`.
    Group { cluster_id: String, members: BTreeSet<String>, pipeline: String },
    /// Giải thể cụm.
    Dissolve { cluster_id: String },
}

/// Kết quả áp dụng một [`Command`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Không có gì thay đổi (lệnh lặp lại) — vẫn commit được.
    Noop,
    /// Đã thay đổi state.
    Applied,
    /// Lệnh không hợp lệ — **từ chối trước khi vào log**, state giữ nguyên.
    Rejected(String),
}

/// Tầng quyết định — **khe cắm** cho engine Raft thật.
pub trait Consensus {
    /// Đưa lệnh vào log; trả `true` nếu đã commit (đã nhân bản tới đủ quorum).
    fn propose(&mut self, cmd: Command) -> Result<bool, String>;

    /// Master của cụm theo **log đã commit**: mỗi term chỉ một leader, term chỉ
    /// tăng.
    fn leader_of(&self, cluster_id: &str) -> Option<String>;

    /// Term hiện tại — sinh ra `epoch` (fencing token).
    fn term_of(&self, cluster_id: &str) -> u64;
}

/// State cục bộ + tầng quyết định.
pub struct Raft {
    node_id: String,
    state: BTreeMap<String, Cluster>,
    /// `None` = chưa cắm engine ⇒ cụm nhiều thành viên chưa ai làm master.
    consensus: Option<Box<dyn Consensus>>,
}

impl Raft {
    /// Node đứng một mình: chạy pipeline của mình, không thuộc cụm nào.
    #[must_use]
    pub fn solo(node_id: impl Into<String>) -> Self {
        Self { node_id: node_id.into(), state: BTreeMap::new(), consensus: None }
    }

    /// Node đã cắm engine quyết định (sẽ là `openraft`).
    #[must_use]
    pub fn with_consensus(node_id: impl Into<String>, consensus: Box<dyn Consensus>) -> Self {
        Self { node_id: node_id.into(), state: BTreeMap::new(), consensus: Some(consensus) }
    }

    /// Gán các node vào một cụm — thao tác dùng `POST /api/cluster/v1/clusters`.
    pub fn group(
        &mut self,
        cluster_id: &str,
        members: impl IntoIterator<Item = String>,
        pipeline: &str,
    ) -> Result<Outcome, String> {
        self.propose(Command::Group {
            cluster_id: cluster_id.to_string(),
            members: members.into_iter().collect(),
            pipeline: pipeline.to_string(),
        })
    }

    pub fn dissolve(&mut self, cluster_id: &str) -> Result<Outcome, String> {
        self.propose(Command::Dissolve { cluster_id: cluster_id.to_string() })
    }

    /// Đề xuất một lệnh. Lệnh không hợp lệ bị từ chối **trước khi** vào log, nên
    /// state không bao giờ nhận cấu hình hỏng.
    pub fn propose(&mut self, cmd: Command) -> Result<Outcome, String> {
        // Chạy trên bản sao: lệnh bị từ chối thì không đụng state thật.
        let mut trial = self.state.clone();
        let outcome = apply(&mut trial, cmd.clone());
        if matches!(outcome, Outcome::Rejected(_)) {
            return Ok(outcome);
        }
        let Some(consensus) = self.consensus.as_mut() else {
            return Err("chưa cắm engine quyết định: không thể commit lệnh vào log".into());
        };
        consensus.propose(cmd)?;
        self.state = trial;
        Ok(outcome)
    }

    /// Node này có thuộc cụm nào không. `false` = đứng một mình.
    #[must_use]
    pub fn is_solo(&self) -> bool {
        self.my_cluster().is_none()
    }

    /// Cụm mà node này phục vụ, nếu có.
    #[must_use]
    pub fn my_cluster(&self) -> Option<&Cluster> {
        self.state.values().find(|c| c.has(&self.node_id))
    }

    /// Vai trò — **lấy từ tầng quyết định**, không suy ra từ quan sát của gossip.
    ///
    /// Đứng một mình ⇒ [`Role::Master`]: không có gì để bầu, cứ chạy pipeline
    /// của mình. Thuộc cụm ⇒ master hay không là quyết định của tầng quyết định.
    #[must_use]
    pub fn role(&self) -> Role {
        self.my_cluster().map_or(Role::Master, |c| self.role_of(&c.id))
    }

    /// Vai trò với một cụm cụ thể. Node không thuộc cụm đó thì không có vai trò
    /// gì với nó ⇒ coi như đứng ngoài.
    #[must_use]
    pub fn role_of(&self, cluster_id: &str) -> Role {
        if !self.cluster(cluster_id).is_some_and(|c| c.has(&self.node_id)) {
            return Role::Master;
        }
        let leader = self.consensus.as_ref().and_then(|c| c.leader_of(cluster_id));
        if leader.as_deref() == Some(self.node_id.as_str()) {
            Role::Master
        } else {
            Role::Standby
        }
    }

    /// Cụm đã có master chưa. Cụm nhiều thành viên mà chưa cắm engine sẽ là
    /// `false` — pipeline đứng yên cho tới khi cắm, đây là trạng thái trung thực.
    #[must_use]
    pub fn has_leader(&self, cluster_id: &str) -> bool {
        self.consensus.as_ref().and_then(|c| c.leader_of(cluster_id)).is_some()
    }

    /// `epoch` dùng làm fencing token khi ghi state.
    #[must_use]
    pub fn epoch_of(&self, cluster_id: &str) -> u64 {
        self.consensus.as_ref().map_or(0, |c| c.term_of(cluster_id))
    }

    #[must_use]
    pub fn cluster(&self, id: &str) -> Option<&Cluster> {
        self.state.get(id)
    }

    /// Danh sách cụm, sắp theo id (vì `BTreeMap`).
    #[must_use]
    pub fn clusters(&self) -> Vec<&Cluster> {
        self.state.values().collect()
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Đã cắm engine quyết định chưa.
    #[must_use]
    pub fn has_consensus(&self) -> bool {
        self.consensus.is_some()
    }
}

/// Áp dụng lệnh lên state. Tách khỏi `Raft` để kiểm thử không cần mock.
fn apply(state: &mut BTreeMap<String, Cluster>, cmd: Command) -> Outcome {
    match cmd {
        Command::Create { cluster } => {
            if cluster.members.is_empty() {
                return Outcome::Rejected("cụm phải có ít nhất một thành viên".into());
            }
            match state.get_mut(&cluster.id) {
                Some(existing) if *existing == cluster => Outcome::Noop,
                Some(_) => Outcome::Rejected(format!("cụm {} đã tồn tại", cluster.id)),
                None => {
                    state.insert(cluster.id.clone(), cluster);
                    Outcome::Applied
                }
            }
        }
        Command::AddMember { cluster_id, node_id } => add_member(state, &cluster_id, &node_id),
        Command::RemoveMember { cluster_id, node_id } => remove_member(state, &cluster_id, &node_id),
        Command::Group { cluster_id, members, pipeline } => {
            if members.is_empty() {
                return Outcome::Rejected("cụm phải có ít nhất một thành viên".into());
            }
            match state.get_mut(&cluster_id) {
                Some(existing) => {
                    if existing.pipeline != pipeline {
                        return Outcome::Rejected(format!(
                            "cụm {cluster_id} đang phục vụ pipeline {:?}, không thể đổi sang {pipeline:?} — hãy giải thể rồi tạo lại",
                            existing.pipeline
                        ));
                    }
                    if existing.members.is_subset(&members) {
                        Outcome::Noop
                    } else {
                        existing.members.extend(members);
                        Outcome::Applied
                    }
                }
                None => {
                    let mut cluster = Cluster::solo(
                        &cluster_id,
                        members.iter().next().expect("đã kiểm tra rỗng ở trên"),
                        &pipeline,
                    );
                    cluster.members.extend(members);
                    state.insert(cluster_id, cluster);
                    Outcome::Applied
                }
            }
        }
        Command::Dissolve { cluster_id } => match state.remove(&cluster_id) {
            Some(_) => Outcome::Applied,
            None => Outcome::Rejected(format!("không có cụm {cluster_id}")),
        },
    }
}

fn add_member(state: &mut BTreeMap<String, Cluster>, cluster_id: &str, node_id: &str) -> Outcome {
    if node_id.trim().is_empty() {
        return Outcome::Rejected("node_id rỗng".into());
    }
    let Some(c) = state.get_mut(cluster_id) else {
        return Outcome::Rejected(format!("không có cụm {cluster_id}"));
    };
    if c.members.insert(node_id.to_string()) {
        Outcome::Applied
    } else {
        Outcome::Noop
    }
}

fn remove_member(
    state: &mut BTreeMap<String, Cluster>,
    cluster_id: &str,
    node_id: &str,
) -> Outcome {
    let Some(c) = state.get_mut(cluster_id) else {
        return Outcome::Rejected(format!("không có cụm {cluster_id}"));
    };
    if !c.members.remove(node_id) {
        return Outcome::Noop;
    }
    // Không để cụm rỗng: cụm một thành viên vẫn hợp lệ, rỗng thì không ai chạy.
    if c.members.is_empty() {
        state.remove(cluster_id);
    }
    Outcome::Applied
}
