//! Opsense kernel wire protocol: protobuf schema, tonic codegen, frame codec.
//!
//! One schema drives both boundaries of the runner architecture:
//! - serve ↔ runner over gRPC ([`pb::kernel_runner_client`] / [`pb::kernel_runner_server`]),
//! - host ↔ kernel process over framed stdio (CONTROL = [`pb::Envelope`],
//!   ARROW = Arrow IPC segment), see [`frame`].
//!
//! Kernels never see gRPC; the runner only translates between the two
//! encodings of these same messages.

pub mod frame;
pub mod host;

/// Wire-protocol revision; bumped on breaking changes. The handshake
/// (`Hello`/`Welcome`) rejects mismatches early.
pub const PROTOCOL_VERSION: u32 = 1;

/// Generated protobuf + gRPC types from `proto/opsense.proto`.
pub mod pb {
    tonic::include_proto!("opsense.kernel.v1");
}

pub use pb::*;
