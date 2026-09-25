//! Scripted transforms for the Opsense pipeline, powered by the sandboxed
//! [Rhai](https://rhai.rs) language.
//!
//! This crate owns everything Rhai: the AST cache/runtime ([`runtime`]), the
//! [`RhaiTransform`] registered into the vector `Runtime` (typetag
//! name `rhai_transform`). Keeping it separate mirrors `opsense-store`: the
//! heavy dependency stays in one crate and the rest of the workspace is
//! untouched by it.
//!
//! A transform node's logic lives in a Rhai script defining
//!
//! ```rhai
//! fn process(observations) { ... }
//! ```
//!
//! `observations` is an array of observation maps (`ts`, `metric_id`,
//! `kind`, `signal`, `value`, optional `labels`/`severity`) and the function
//! returns a new array of the same shape. The script comes either inline from
//! the pipeline config or from a `.rhai` file — file scripts are recompiled
//! automatically when they change on disk (mtime), so editing a script is
//! picked up by the next batch without restarting the session. See
//! `examples/prometheus-demo/rhai/` for example scripts.

mod attributes;
mod orders;
mod rhai_collect;
mod runtime;
mod station;
mod strategy;
mod time_fns;
mod tools;
mod transform;
mod ts_ops;

pub use runtime::{ScriptSource, call_process, call_process_with};
pub use strategy::ScriptStrategy;
pub use transform::RhaiTransform;

/// Re-export of the `vector` runtime under `crate::vector::runtime`.
///
/// `opsense-macros`' `#[transform]` etc. attributes expand to code that refers
/// to `crate::vector::runtime::*`; this mirror lets those macros be used from
/// this crate exactly as they are from `opsense-components`.
pub mod vector {
    pub use opsense_mlib::vector::runtime;
}
