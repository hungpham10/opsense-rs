//! Cluster membership, state và election — **logic thuần, không I/O**.
//!
//! Module này là phần "suy nghĩ" của mesh. Nó **không** biết gì về pipeline,
//! `Runtime`, HTTP hay socket; vận chuyển nằm ở `opsense::cluster` (xem
//! `docs`/plan `mesh-cluster.md`). Nhờ vậy test ở đây chạy không cần mạng và
//! không cần mock HTTP.
//!
//! Ba phần:
//!
//! - [`state`] — trạng thái phân cụm dùng chung, hợp nhất LWW theo
//!   `(lamport, author)` nên hội tụ bất kể thứ tự nhận;
//! - [`view`] — phát hiện node chết bằng `alive → suspect → dead` có indirect
//!   probe, chống false positive do mạng chập chờn;
//! - [`election`] — master tất định (`node_id` nhỏ nhất) + settle window chặn
//!   split-brain lúc khởi động.

pub mod election;
pub mod state;
pub mod view;

pub use election::{decide, is_settled, promoted, Election, Role};
pub use state::{Cluster, ClusterState, Entry, Version};
pub use view::{Membership, NodeState, Peer};
