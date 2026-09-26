//! Raft — **quyết định**: cụm gồm node nào, ai là master.
//!
//! Đối xứng với [`crate::gossip`]. Vì sao phải tách: gossip **không** bảo đảm
//! được "chỉ một master" khi mạng đứt đôi — hai bên đều có thể tin mình còn
//! sống, và chỉ Raft (log + term + quorum) mới loại được. Ngược lại, đưa quyết
//! định vào gossip nghĩa là tự chế lại Raft một cách tệ hơn.
//!
//! **Phần này KHÔNG tự cài đặt thuật toán Raft** — làm thế là ăn cướp tiến bộ
//! và làm nên nhiều bug. Ở đây chỉ có:
//!
//! - kiểu miền: cụm, thành viên, lệnh, vai trò;
//! - trait [`Consensus`] làm **khe cắm** cho engine (`openraft` sẽ cài sau);
//! - cài đặt [`SingleNode`] cho node đơn, đủ để chạy và test trước khi cắm engine.
//!
//! Không I/O — vận chuyển nằm ở `opsense::cluster`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Vai trò của node này với một cụm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Đang chạy pipeline của cụm.
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
    /// Cụm một thành viên — trạng thái mặc định sau khi join: chưa ai nhốt node
    /// này vào cụm nào, nên nó là cụm riêng của mình.
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
    /// Gộp các node vào một cụm (tạo nếu chưa có) — thao tác của
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

/// Cài đặt tối thiểu: mỗi node một cụm riêng, tự làm master.
///
/// Dùng cho node đơn (chưa ai nhốt vào cụm nào) và cho test. Khi cắm
/// `openraft`, cài đặt này bị thay bằng engine thật — phần còn lại không đổi.
#[derive(Debug, Default)]
pub struct SingleNode {
    clusters: BTreeMap<String, Cluster>,
}

impl Consensus for SingleNode {
    fn propose(&mut self, cmd: Command) -> Result<bool, String> {
        // Cài đặt tối giểu không nhân bản: từ chối thì trả lỗi. `Raft::propose`
        // đã kiểm tra trên bản sao nên thực tế không tới nhánh này.
        match apply(&mut self.clusters, cmd) {
            Outcome::Rejected(msg) => Err(msg),
            Outcome::Noop | Outcome::Applied => Ok(true),
        }
    }

    fn leader_of(&self, cluster_id: &str) -> Option<String> {
        self.clusters.get(cluster_id).and_then(|c| c.members.iter().next().cloned())
    }

    fn term_of(&self, cluster_id: &str) -> u64 {
        self.clusters.get(cluster_id).map_or(0, |c| c.epoch)
    }
}

/// State cục bộ + tầng quyết định.
pub struct Raft {
    node_id: String,
    state: BTreeMap<String, Cluster>,
    consensus: Box<dyn Consensus>,
}

impl Raft {
    #[must_use]
    pub fn new(node_id: impl Into<String>, consensus: Box<dyn Consensus>) -> Self {
        Self { node_id: node_id.into(), state: BTreeMap::new(), consensus }
    }

    /// Node chưa nhốt vào cụm nào: tự tạo cụm riêng và là master của nó.
    pub fn bootstrap_own_cluster(&mut self, pipeline: &str) -> Result<String, String> {
        let id = format!("cluster-{}", self.node_id);
        self.propose(Command::Create { cluster: Cluster::solo(&id, &self.node_id, pipeline) })?;
        Ok(id)
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
        self.consensus.propose(cmd)?;
        self.state = trial;
        Ok(outcome)
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

    /// Vai trò của node này — **lấy từ tầng quyết định**, không suy ra từ quan
    /// sát của gossip.
    #[must_use]
    pub fn role_of(&self, cluster_id: &str) -> Role {
        if self.consensus.leader_of(cluster_id).as_deref() == Some(self.node_id.as_str()) {
            Role::Master
        } else {
            Role::Standby
        }
    }

    /// `epoch` dùng làm fencing token khi ghi state.
    #[must_use]
    pub fn epoch_of(&self, cluster_id: &str) -> u64 {
        self.consensus.term_of(cluster_id)
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

    /// Cụm mà node này phục vụ.
    #[must_use]
    pub fn my_clusters(&self) -> Vec<&Cluster> {
        self.state.values().filter(|c| c.has(&self.node_id)).collect()
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
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

#[cfg(test)]
mod tests {
    use super::*;

    fn raft(node_id: &str) -> Raft {
        Raft::new(node_id, Box::new(SingleNode::default()))
    }

    #[test]
    fn bootstrap_makes_node_master_of_its_own_cluster() {
        let mut r = raft("node-a");
        let id = r.bootstrap_own_cluster("grid").unwrap();
        assert_eq!(r.role_of(&id), Role::Master);
        assert_eq!(r.my_clusters().len(), 1);
    }

    #[test]
    fn rejected_command_leaves_state_untouched() {
        let mut r = raft("node-a");
        r.bootstrap_own_cluster("grid").unwrap();
        let before = r.clusters().len();
        assert!(matches!(r.group("c2", [] as [String; 0], "grid").unwrap(), Outcome::Rejected(_)));
        assert_eq!(r.clusters().len(), before);
    }

    #[test]
    fn grouping_nodes_creates_the_cluster() {
        let mut r = raft("node-a");
        assert_eq!(r.group("c1", ["node-a".to_string(), "node-b".to_string()], "grid").unwrap(), Outcome::Applied);
        let c = r.cluster("c1").unwrap();
        assert_eq!(c.members.len(), 2);
        assert!(c.has("node-b"));
    }

    #[test]
    fn changing_pipeline_of_existing_cluster_is_refused() {
        let mut r = raft("node-a");
        r.group("c1", ["node-a".to_string()], "grid").unwrap();
        assert!(matches!(r.group("c1", ["node-a".to_string()], "predict").unwrap(), Outcome::Rejected(_)));
        assert_eq!(r.cluster("c1").unwrap().pipeline, "grid");
    }

    #[test]
    fn role_comes_from_the_decision_layer_not_from_observation() {
        let members = || ["node-a".to_string(), "node-b".to_string()];
        let mut a = raft("node-a");
        a.group("c1", members(), "grid").unwrap();
        let mut b = raft("node-b");
        b.group("c1", members(), "grid").unwrap();
        assert_eq!(a.role_of("c1"), Role::Master);
        assert_eq!(b.role_of("c1"), Role::Standby);
    }

    #[test]
    fn dissolve_then_recreate_works() {
        let mut r = raft("node-a");
        r.group("c1", ["node-a".to_string()], "grid").unwrap();
        assert_eq!(r.dissolve("c1").unwrap(), Outcome::Applied);
        assert!(r.cluster("c1").is_none());
        assert!(matches!(r.dissolve("c1").unwrap(), Outcome::Rejected(_)));
    }

    #[test]
    fn removing_last_member_drops_the_cluster() {
        let mut r = raft("node-a");
        r.group("c1", ["node-a".to_string()], "grid").unwrap();
        let out = r
            .propose(Command::RemoveMember { cluster_id: "c1".into(), node_id: "node-a".into() })
            .unwrap();
        assert_eq!(out, Outcome::Applied);
        assert!(r.cluster("c1").is_none());
    }

    #[test]
    fn commands_survive_json_round_trip_so_the_log_can_carry_them() {
        let cmd = Command::Group {
            cluster_id: "c1".into(),
            members: BTreeSet::from(["a".to_string(), "b".to_string()]),
            pipeline: "grid".into(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert_eq!(serde_json::from_str::<Command>(&json).unwrap(), cmd);
    }
}
