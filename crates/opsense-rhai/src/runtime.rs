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
        ENGINE
            .with_borrow(|eng| eng.compile(&text))
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
///
/// One engine per blocking thread (`thread_local`): the thread pool reuses
/// threads across batches, so the tool registrations (`register_all`) are
/// paid once per thread instead of per `process` call, while distinct threads
/// never contend — there is deliberately no `Mutex` around the engine, since
/// `call_fn` needs `&mut` and serializing script runs across pipeline nodes
/// would serialize the whole pipeline. Per-call state (the wall-clock budget
/// via `on_progress`, the attribute snapshot) is installed onto the thread's
/// engine right before evaluation; each blocking thread runs one script at a
/// time, so those installs never race.
fn thread_engine() -> std::cell::RefCell<rhai::Engine> {
    let mut eng = rhai::Engine::new();
    eng.set_max_operations(1_000_000);
    eng.set_max_array_size(100_000);
    eng.set_max_map_size(100_000);
    eng.set_max_string_size(1_000_000);
    // Defaults (32/32) choke on ordinary map literals with interpolation;
    // these bounds still stop pathological nesting.
    eng.set_max_expr_depths(256, 256);

    // Every script-facing native function lives in one place: `tools`.
    crate::tools::register_all(&mut eng);
    std::cell::RefCell::new(eng)
}

thread_local! {
    static ENGINE: std::cell::RefCell<rhai::Engine> = thread_engine();
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

/// Run one batch through the script's `process(observations)` function with no
/// node params and no config attributes (no `param_*` globals, `attr` absent).
///
/// Errors are plain strings: a broken script must not abort the pipeline —
/// the caller logs, keeps its watermark cursor and retries the window on the
/// next signal.
pub async fn call_process(
    script: ScriptSource,
    input_json: serde_json::Value,
) -> Result<Vec<serde_json::Value>, String> {
    call_process_with(script, input_json, Default::default(), Default::default()).await
}

/// [`call_process`] with the node's `params` config table and the pipeline
/// config's resolved `[attributes]` passed through to the script.
///
/// `params` are seeded into the scope as `param_<name>` globals;
/// `attributes` are exposed read-only via the native `attr(name)` /
/// `attrs()` lookups. Both are copied per call, so a script can never mutate
/// pipeline state.
///
/// The script runs on a blocking thread (`spawn_blocking`) so its CPU-bound
/// work never stalls async tasks. The wall-clock budget is enforced for real
/// by the engine's `on_progress` abort; the outer `tokio::time::timeout` is a
/// backstop that only cancels the *wait*, never the thread.
pub async fn call_process_with(
    script: ScriptSource,
    input_json: serde_json::Value,
    params: std::collections::BTreeMap<String, serde_json::Value>,
    attributes: std::collections::BTreeMap<String, String>,
) -> Result<Vec<serde_json::Value>, String> {
    let join = tokio::task::spawn_blocking(move || {
        let ast = acquire(&script)?;
        let arg =
            rhai::serde::to_dynamic(&input_json).map_err(|e| format!("input conversion: {e}"))?;

        let mut scope = rhai::Scope::new();
        for (name, value) in &params {
            // Only identifier-shaped names reach the scope — anything else
            // would make the script unreachable or the call fail outright.
            let valid =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !valid {
                return Err(format!("invalid param name `{name}` (use [A-Za-z0-9_])"));
            }
            let dynamic = rhai::serde::to_dynamic(value)
                .map_err(|e| format!("param `{name}` conversion: {e}"))?;
            scope.push_dynamic(format!("param_{name}"), dynamic);
        }

        // Install this call's wall-clock budget and attribute snapshot onto
        // the thread's engine (safe: one script runs per blocking thread).
        //
        // Real wall-clock budget: a runaway or deadlocked `process` cannot pin
        // the blocking worker forever. `on_progress` is polled during
        // evaluation and aborts the script once the budget elapses — unlike
        // the outer `tokio::time::timeout`, which only cancels the *wait* and
        // never stops the thread. (A script stuck inside a sync native tool
        // that never yields to the engine still can't be aborted —
        // acceptable.)
        let deadline = std::time::Instant::now() + rhai_timeout();

        let result = ENGINE.with_borrow_mut(|eng| {
            eng.on_progress(move |_| {
                if std::time::Instant::now() <= deadline {
                    None
                } else {
                    // Past budget: abort the script (cooperative cancellation).
                    Some(rhai::Dynamic::UNIT)
                }
            });
            crate::tools::register_attributes(eng, attributes);
            eng.call_fn(&mut scope, &ast, "process", (arg,))
                .map_err(|e| format!("script error: {e}"))
        })?;

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
