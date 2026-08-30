//! Hook for external mapping scripts, without coupling this crate (or
//! `opsense-components`) to any scripting engine.
//!
//! The pipeline's HTTP fetch node supports `format = "script"`: the raw JSON
//! body is handed to a user function that returns observations. Executing that
//! function is engine-specific — today Rhai via `opsense-rhai`, tomorrow
//! anything else. The engine registers an implementation of [`ScriptRunner`]
//! once at startup ([`set_script_runner`]); components look it up with
//! [`script_runner`]. Nothing here knows about Rhai and vice versa — only the
//! final binary links both sides.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;

/// Runs one mapping invocation: `script` (inline) or `script_path` (file)
/// applied to `input` (the parsed response body), yielding a JSON array of
/// observation-shaped values. Errors are plain strings by contract — a broken
/// script must not abort the pipeline, only fail the current window.
#[async_trait]
pub trait ScriptRunner: Send + Sync {
    async fn run(
        &self,
        script: &str,
        script_path: &str,
        input: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, String>;
}

static RUNNER: OnceLock<Arc<dyn ScriptRunner>> = OnceLock::new();

/// Register the process-wide runner. Idempotent: the first registration wins
/// and later calls are ignored (returns `false`).
pub fn set_script_runner(runner: Arc<dyn ScriptRunner>) -> bool {
    RUNNER.set(runner).is_ok()
}

/// The registered runner, if any engine was linked into this binary.
#[must_use]
pub fn script_runner() -> Option<Arc<dyn ScriptRunner>> {
    RUNNER.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;

    #[async_trait]
    impl ScriptRunner for Echo {
        async fn run(
            &self,
            _script: &str,
            _script_path: &str,
            input: serde_json::Value,
        ) -> Result<Vec<serde_json::Value>, String> {
            Ok(vec![input])
        }
    }

    #[tokio::test]
    async fn first_registration_wins() {
        assert!(set_script_runner(Arc::new(Echo)));
        // A second set is ignored…
        assert!(!set_script_runner(Arc::new(Echo)));
        let runner = script_runner().expect("runner registered");
        let out = runner
            .run("", "", serde_json::json!({"n": 1}))
            .await
            .unwrap();
        assert_eq!(out, vec![serde_json::json!({"n": 1})]);
    }
}
