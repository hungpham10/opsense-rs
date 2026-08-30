//! Vector data-pipeline subsystem (sources, transforms, sinks).
//!
//! Merged into `opsense-libs` from the previously separate `vector_components`
//! and `vector_runtime` crates. The common component traits live in [`runtime`].
pub mod components;
pub mod runtime;
