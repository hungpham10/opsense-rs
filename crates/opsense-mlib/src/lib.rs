//! Opsense shared utility libraries.
//!
//! Low-level, dependency-light building blocks reused by the capacity engine
//! (`opsense-core`) and the binary (`opsense`):
//! - [`jq`] – a small jq-style JSON query engine (`JsonQuery`).
//! - [`lru`] – a sharded, false-sharing-free LRU cache.
//! - [`vector`] – the data-pipeline subsystem (sources, transforms, sinks),
//!   formerly split into the separate `vector_components`/`vector_runtime` crates.

pub mod ahocorasick;
pub mod binarysearch;
pub mod bloom;
pub mod grid;
pub mod lru;
pub mod radix;
pub mod rcf;
pub mod search;
pub mod sgd;
pub mod snowflake_id;
pub mod tls;
pub mod transition;

// Plan §1: public so external crates (opsense-store, opsense-components) can
// name `TimeseriesStorage`/`PatternStorage`/`CategoryStorage`.
pub mod storage;

#[cfg(feature = "sops")]
pub mod sops;

#[cfg(feature = "json")]
pub mod cast;

#[cfg(feature = "json")]
pub mod jq;

#[cfg(feature = "json")]
pub mod vector;

#[cfg(feature = "rhai")]
pub mod script;

// Cluster cho nhiều node, tách làm hai lib theo **bản chất dữ liệu**:
// - `gossip`: quan sát — ai còn sống, ai chạy version nào. Suy đoán, chấp nhận
//   tạm thời sai.
// - `raft`: quyết định — cụm gồm node nào, ai là master. Nhất quán, không được sai.
#[cfg(feature = "gossip")]
pub mod gossip;

#[cfg(feature = "raft")]
pub mod raft;
