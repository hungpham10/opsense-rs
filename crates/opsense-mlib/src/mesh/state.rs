//! Cluster membership state — **logic thuần, không I/O**.
//!
//! Mô hình: cả cụm giữ một bản sao của `ClusterState` (bản ghi cụm nào gồm
//! những node nào, phục vụ pipeline nào, epoch bao nhiêu). Các node trao đổi
//! bản sao qua API theo chu kỳ rồi [`ClusterState::apply`] hợp nhất.
//!
//! Vì sao hợp nhất được mà không cần vector clock: mỗi bản ghi mang
//! `(lamport, author)`. Quy tắc chọn thắng là **LWW register** — `lamport` lớn
//! hơn thắng, hoàn bằng so `author` lớn hơn theo thứ tự từ điển. Hai node nhận
//! hai bản ghi theo **thứ tự bất kỳ** đều kết luận giống nhau, nên state hội tụ.
//!
//! Ranh giới: module này **không** biết gì về pipeline, `Runtime` hay mạng.
//! Nó chỉ trả lời "cụm nào gồm những ai" và "ai là master".

use std::collections::{BTreeMap, BTreeSet};

/// Một cụm: tập node phục vụ **một** pipeline, cùng `epoch` hiện tại.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cluster {
    /// Định danh cụm, do API cấp.
    pub id: String,
    /// Node thuộc cụm.
    pub members: BTreeSet<String>,
    /// Pipeline mà cụm này phục vụ.
    pub pipeline: String,
    /// Tăng mỗi lần một node lên master — dùng để fencing, xem
    /// [`crate::mesh::election`].
    pub epoch: u64,
}

impl Cluster {
    /// Tạo cụm một thành viên (một node tự làm master).
    #[must_use]
    pub fn solo(id: impl Into<String>, node_id: impl Into<String>, pipeline: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            members: BTreeSet::from([node_id.into()]),
            pipeline: pipeline.into(),
            epoch: 1,
        }
    }
}

/// Phiên bản của một bản ghi, dùng để hợp nhất LWW.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Version {
    /// Đồng hồ Lamport của tác giả.
    pub lamport: u64,
    /// Node đã sửa bản ghi — phá thế hoà bằng thứ tự từ điển.
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

/// Một bản ghi cụm kèm phiên bản — chi tiết triển khai, không public.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Entry {
    cluster: Cluster,
    version: Version,
}

/// Toàn bộ trạng thái phân cụm mà một node biết — đây là **payload trao đổi qua
/// API** (`GET/POST /api/cluster/v1/internal/state`).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterState {
    /// Lamport lớn nhất từng thấy — tăng khi áp dụng state của node khác.
    pub lamport: u64,
    /// Cụm theo id, kèm phiên bản để hợp nhất LWW.
    #[serde(default)]
    clusters: BTreeMap<String, Entry>,
}

impl ClusterState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Cụm theo id.
    #[must_use]
    pub fn cluster(&self, id: &str) -> Option<&Cluster> {
        self.clusters.get(id).map(|e| &e.cluster)
    }

    /// Sửa cụm tại chỗ (dùng khi tăng `epoch` khi lên master).
    pub fn cluster_mut(&mut self, id: &str) -> Option<&mut Cluster> {
        self.clusters.get_mut(id).map(|e| &mut e.cluster)
    }

    /// Danh sách cụm.
    #[must_use]
    pub fn iter(&self) -> impl Iterator<Item = &Cluster> {
        self.clusters.values().map(|e| &e.cluster)
    }

    /// **Nội dung** các cụm, bỏ qua `lamport`/version — dùng để so sánh "hai node
    /// đã hội tử chưa" mà không vướng đồng hồ Lamport (vốn có thể lệch nhau
    /// khi tới đồng hồ khác nhau).
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<String, Cluster> {
        self.clusters.iter().map(|(id, e)| (id.clone(), e.cluster.clone())).collect()
    }

    /// Ghi đè cụm ở phía cục bộ: tăng lamport của mình rồi đặt bản ghi.
    pub fn set_local(&mut self, node_id: &str, cluster: Cluster) {
        // `saturating_add` thay vì `+= 1`: bản ghi từ peer có thể mang lamport
        // bất kỳ, và tràn số ở đây sẽ làm hỏng thứ tự so sánh của toàn cụm.
        self.lamport = self.lamport.saturating_add(1);
        let version = Version::new(self.lamport, node_id);
        self.clusters.insert(cluster.id.clone(), Entry { cluster, version });
    }

    /// Hợp nhất state nhận từ node khác. Trả `true` nếu có thay đổi thật
    /// (dùng để quyết định có đẩy tiếp hay không).
    pub fn apply(&mut self, remote: &ClusterState) -> bool {
        let mut changed = false;
        self.lamport = self.lamport.max(remote.lamport);
        for (id, remote_entry) in &remote.clusters {
            match self.clusters.get(id) {
                None => {
                    self.clusters.insert(id.clone(), remote_entry.clone());
                    changed = true;
                }
                Some(local) if remote_entry.version.is_newer_than(&local.version) => {
                    self.clusters.insert(id.clone(), remote_entry.clone());
                    changed = true;
                }
                Some(_) => {}
            }
        }
        // Lamport phải lớn hơn mọi thứ đã thấy, kể cả bản ghi của chính mình,
        // để lần sửa kế tiếp không bị coi là cũ.
        if changed {
            self.lamport = self.lamport.saturating_add(1);
        }
        changed
    }

    /// Danh sách cụm.
    #[must_use]
    pub fn list(&self) -> Vec<&Cluster> {
        self.clusters.values().map(|e| &e.cluster).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_tie_breaks_on_author() {
        let a = Version::new(1, "node-a");
        let b = Version::new(1, "node-b");
        assert!(b.is_newer_than(&a));
        assert!(!a.is_newer_than(&b));
        assert!(!a.is_newer_than(&Version::new(1, "node-a")));
        assert!(a.is_newer_than(&Version::new(0, "node-z")));
    }

    #[test]
    fn set_local_bumps_lamport() {
        let mut s = ClusterState::new();
        s.set_local("me", Cluster::solo("c1", "me", "grid"));
        assert_eq!(s.lamport, 1);
        s.set_local("me", Cluster::solo("c1", "me", "grid"));
        assert_eq!(s.lamport, 2);
        assert_eq!(s.cluster("c1").map(|c| c.pipeline.as_str()), Some("grid"));
    }

    #[test]
    fn apply_ignores_older_version() {
        let mut a = ClusterState::new();
        a.set_local("me", Cluster::solo("c1", "me", "grid"));
        let mut b = ClusterState::new();
        b.set_local("me", Cluster::solo("c1", "me", "other-pipeline"));
        // `a` vừa sửa nên phiên bản mới hơn ⇒ `b` không được đè.
        assert!(!a.apply(&b));
        assert_eq!(a.cluster("c1").map(|c| c.pipeline.as_str()), Some("grid"));
    }
}
