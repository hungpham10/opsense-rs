//! Rhai script runtime: compile-once AST cache and sandboxed evaluation.
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
//! picked up by the next batch without restarting the session.
//!
//! Rhai is sandboxed: no filesystem/network access beyond the registered
//! station lookups (`ts_query`/`ts_mean` — read-only queries into stores
//! published by `station_sink`), plus operation and size limits below, so a
//! runaway script cannot take the pipeline down. A transform also gets a
//! time-series operator library (`ts_rate`, `ts_moving_avg`, `ts_resample`,
//! `ts_quantile`, `ts_p95`, `ts_p99`, `ts_delta`, `ts_pct_change`) so common
//! analytics run natively instead of being reinvented in Rhai. Every
//! `process` call is additionally bounded by `OPSENSE_RHAI_TIMEOUT_SECS`
//! (default 30) of wall-clock time.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

/// Where a node's script comes from.
#[derive(Debug, Clone)]
pub enum ScriptSource {
    /// Inline script declared in the pipeline config table.
    Inline(String),
    /// External `.rhai` file; recompiled on mtime change.
    File(PathBuf),
}

impl ScriptSource {
    fn content_hash(src: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        src.hash(&mut hasher);
        hasher.finish()
    }

    fn cache_key(&self) -> String {
        match self {
            // The key carries the content hash so distinct inline scripts
            // never collide in the shared AST cache.
            Self::Inline(src) => format!("inline:{:016x}", Self::content_hash(src)),
            Self::File(path) => format!("file:{}", path.display()),
        }
    }
}

enum Fingerprint {
    Content(u64),
    Mtime(SystemTime),
}

struct CompiledScript {
    fingerprint: Fingerprint,
    ast: Arc<rhai::AST>,
}

fn cache() -> &'static Mutex<HashMap<String, CompiledScript>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CompiledScript>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn source_fingerprint(script: &ScriptSource) -> Result<Fingerprint, String> {
    match script {
        ScriptSource::Inline(src) => Ok(Fingerprint::Content(ScriptSource::content_hash(src))),
        ScriptSource::File(path) => {
            let mtime = std::fs::metadata(path)
                .and_then(|m| m.modified())
                .map_err(|e| format!("{}: {e}", path.display()))?;
            Ok(Fingerprint::Mtime(mtime))
        }
    }
}

fn script_text(script: &ScriptSource) -> Result<String, String> {
    match script {
        ScriptSource::Inline(src) => Ok(src.clone()),
        ScriptSource::File(path) => {
            std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
        }
    }
}

/// Compile (or reuse the cached) AST for `script`.
fn acquire(script: &ScriptSource) -> Result<Arc<rhai::AST>, String> {
    let key = script.cache_key();
    let fingerprint = source_fingerprint(script)?;

    let mut cache = cache().lock().unwrap();
    if let Some(cached) = cache.get(&key) {
        let fresh = match (&cached.fingerprint, &fingerprint) {
            (Fingerprint::Content(a), Fingerprint::Content(b)) => a == b,
            (Fingerprint::Mtime(a), Fingerprint::Mtime(b)) => a == b,
            _ => false,
        };
        if fresh {
            return Ok(cached.ast.clone());
        }
    }

    tracing::info!("compiling rhai script ({})", script.cache_key());
    let text = script_text(script)?;
    let ast = Arc::new(
        engine()
            .compile(&text)
            .map_err(|e| format!("rhaiscript compile error: {e}"))?,
    );
    // A script without `process` fails at first call with a clear message;
    // catch it here so the error names the contract instead of the call site.
    if ast.iter_functions().all(|f| f.name != "process") {
        return Err("script must define `fn process(observations)`".to_string());
    }
    let compiled = CompiledScript { fingerprint, ast };
    let ast = compiled.ast.clone();
    cache.insert(key, compiled);
    Ok(ast)
}

/// Sandboxed engine: pure data ops plus read-only history lookups into
/// stations registered by `station_sink`, bounded work.
fn engine() -> rhai::Engine {
    let mut eng = rhai::Engine::new();
    eng.set_max_operations(1_000_000);
    eng.set_max_array_size(100_000);
    eng.set_max_map_size(100_000);
    eng.set_max_string_size(1_000_000);
    // Defaults (32/32) choke on ordinary map literals with interpolation;
    // these bounds still stop pathological nesting.
    eng.set_max_expr_depths(256, 256);

    // Real wall-clock budget: a runaway or deadlocked `process` cannot pin the
    // blocking worker forever. `on_progress` is polled during evaluation and
    // aborts the script once the budget elapses — unlike the outer
    // `tokio::time::timeout` in `call_process`, which only cancels the *wait*
    // and never stops the thread. (A script stuck inside a sync native tool
    // that never yields to the engine still can't be aborted — acceptable.)
    let deadline = std::time::Instant::now() + rhai_timeout();
    eng.on_progress(move |_| {
        if std::time::Instant::now() <= deadline {
            None
        } else {
            // Past budget: abort the script (cooperative cancellation).
            Some(rhai::Dynamic::UNIT)
        }
    });

    // Every script-facing native function lives in one place: `tools`.
    crate::tools::register_all(&mut eng);
    eng
}

/// Wall-clock budget for any single `process` call, overridable via
/// `OPSENSE_RHAI_TIMEOUT_SECS` (default 30). Enforced cooperatively by the
/// engine's `on_progress` callback (which aborts the script) and as a
/// backstop by `tokio::time::timeout` around the `spawn_blocking` join.
fn rhai_timeout() -> std::time::Duration {
    let secs: u64 = std::env::var("OPSENSE_RHAI_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(30);
    std::time::Duration::from_secs(secs)
}

/// Run one batch through the script's `process(observations)` function off
/// the async runtime. Input/output are JSON arrays of observations.
///
/// The script runs on a blocking thread (`spawn_blocking`) so its CPU-bound
/// work never stalls async tasks. The wall-clock budget is enforced for real
/// by the engine's `on_progress` abort (see [`engine`]); the outer
/// `tokio::time::timeout` is a backstop.
///
/// Errors are plain strings: a broken script must not abort the pipeline —
/// the caller logs, keeps its watermark cursor and retries the window on the
/// next signal.
pub async fn call_process(
    script: ScriptSource,
    input_json: serde_json::Value,
) -> Result<Vec<serde_json::Value>, String> {
    let join = tokio::task::spawn_blocking(move || {
        let ast = acquire(&script)?;
        let arg =
            rhai::serde::to_dynamic(&input_json).map_err(|e| format!("input conversion: {e}"))?;

        let result = engine()
            .call_fn(&mut rhai::Scope::new(), &ast, "process", (arg,))
            .map_err(|e| format!("script error: {e}"))?;

        let value: serde_json::Value =
            rhai::serde::from_dynamic(&result).map_err(|e| format!("result conversion: {e}"))?;
        match value {
            serde_json::Value::Array(items) => Ok(items),
            _ => Err("process() must return an array".to_string()),
        }
    });
    match tokio::time::timeout(rhai_timeout(), join).await {
        Ok(inner) => inner.map_err(|e| format!("script task failed: {e}"))?,
        Err(_) => Err(format!(
            "rhai script timed out after {}s",
            rhai_timeout().as_secs()
        )),
    }
}
