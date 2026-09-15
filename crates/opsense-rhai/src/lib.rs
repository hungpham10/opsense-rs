//! Scripted transforms for the Opsense pipeline, powered by the sandboxed
//! [Rhai](https://rhai.rs) language.
//!
//! This crate owns everything Rhai: the AST cache/runtime ([`runtime`]), the
//! [`RhaiTransform`] registered into the vector `Runtime` (typetag
//! name `rhai_transform`) and the process-wide [`ScriptRunner`] that
//! lets `http_source`'s `format = "script"` map arbitrary API
//! responses. Keeping it separate mirrors `opsense-store`: the heavy
//! dependency stays in one crate and the rest of the workspace is untouched
//! by it.
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
//! `scripts/README.md` for the playground workflow.

mod runtime;
mod tools;
mod transform;

pub use runtime::{ScriptSource, call_process, call_process_with};
pub use transform::RhaiTransform;

use std::sync::Arc;

/// [`opsense_core::script::ScriptRunner`] backed by this crate's sandboxed
/// Rhai runtime: picks inline vs file source, compiles/caches and runs
/// `fn process(body)` off the async worker threads.
pub struct RhaiScriptRunner;

#[async_trait::async_trait]
impl opsense_core::script::ScriptRunner for RhaiScriptRunner {
    async fn run(
        &self,
        script: &str,
        script_path: &str,
        input: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, String> {
        let source = if script_path.is_empty() {
            ScriptSource::Inline(script.to_string())
        } else {
            ScriptSource::File(std::path::PathBuf::from(script_path))
        };
        call_process_with(
            source,
            input,
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        )
        .await
    }
}

/// Register [`RhaiScriptRunner`] as the process-wide script engine so HTTP
/// fetch nodes with `format = "script"` work in this binary. Idempotent —
/// call it early in every binary entry point (`serve`, MCP init).
pub fn register() -> bool {
    opsense_core::script::set_script_runner(Arc::new(RhaiScriptRunner))
}

/// Re-export of the `vector` runtime under `crate::vector::runtime`.
///
/// `opsense-macros`' `#[transform]` etc. attributes expand to code that refers
/// to `crate::vector::runtime::*`; this mirror lets those macros be used from
/// this crate exactly as they are from `opsense-components`.
pub mod vector {
    pub use opsense_libs::vector::runtime;
}
