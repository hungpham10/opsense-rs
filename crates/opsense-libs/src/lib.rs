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
pub mod cast;
pub mod grid;
pub mod jq;
pub mod lru;
pub mod radix;
pub mod search;
pub mod snowflake_id;
pub mod sops;
pub mod vector;

// Plan §1: public so external crates (opsense-store, opsense-components) can
// name `TimeseriesStorage`/`PatternStorage`/`CategoryStorage`.
pub mod storage;
