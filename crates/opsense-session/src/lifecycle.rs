//! Session lifecycle management.
//!
//! Sessions are driven from plain threads (REPL loop, MCP tool handlers) and
//! the kernel runs as a separate IPC process behind [`crate::backend`]. The
//! manager owns a small tokio runtime used only to drive backend I/O; the
//! analysis API itself stays synchronous so the REPL and existing callers do
//! not need an async context.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use opsense_core::config::Config;
use opsense_proto::pb::{CodeRequest, DatasetHeader, SessionParams};

use crate::backend::{KernelBackend, KernelOutput};
use crate::limits::ResourceLimits;
use crate::state::{HistoryEntry, SessionState};

/// Session status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SessionStatus {
    Creating,
    Active,
    Idle,
    Closing,
    Closed,
    Error,
}

/// Handle handed out to callers (REPL, MCP tools). Backed by one kernel
/// session reached through the configured [`KernelBackend`].
pub struct Session {
    id: Uuid,
    status: Arc<RwLock<SessionStatus>>,
    pub(crate) state: Arc<Mutex<SessionState>>,
    // Swappable: `:runner connect` moves a live session from local IPC to a
    // gRPC runner (or back) without touching variables/history (checklist §6).
    backend: std::sync::RwLock<Arc<dyn KernelBackend>>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl Session {
    #[must_use]
    pub fn id(&self) -> Uuid {
        self.id
    }

    #[must_use]
    pub fn backend(&self) -> Arc<dyn KernelBackend> {
        self.backend.read().unwrap().clone()
    }

    /// Move this live session onto another backend. Host-side state
    /// (variables, history, artifacts) is untouched — datasets are re-pushed
    /// per execute, so nothing needs migrating. The old kernel session is
    /// closed best-effort.
    ///
    /// # Errors
    /// New backend failed to start the kernel session; the old backend is
    /// kept in that case.
    pub async fn switch_backend(&self, backend: Arc<dyn KernelBackend>) -> Result<()> {
        let sid = self.id.to_string();
        let params = {
            let state = self.state.lock().unwrap();
            SessionParams {
                session_id: sid.clone(),
                env: Default::default(),
                allow_fs: state.limits.allow_fs,
                allow_net: state.limits.allow_net,
                max_memory_mb: state.limits.max_memory_mb,
                packages: vec![],
            }
        };
        backend.start_session(params).await?;
        let old = self.backend.write().unwrap().clone();
        let _ = old.close_session(&sid).await;
        *self.backend.write().unwrap() = backend;
        Ok(())
    }

    #[must_use]
    pub fn status(&self) -> SessionStatus {
        *self.status.read().unwrap()
    }

    pub fn set_status(&self, status: SessionStatus) {
        *self.status.write().unwrap() = status;
    }

    #[must_use]
    pub fn state(&self) -> Arc<Mutex<SessionState>> {
        self.state.clone()
    }

    /// Run `f` with exclusive access to the session state, refreshing
    /// `last_active`. All REPL/MCP operations go through this.
    ///
    /// # Panics
    /// Panics when the session's own mutex is poisoned.
    pub fn with_state<T>(&self, f: impl FnOnce(&mut SessionState) -> T) -> T {
        let mut state = self.state.lock().unwrap();
        state.last_active = Utc::now();
        f(&mut state)
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self.status(), SessionStatus::Active)
    }

    /// Append one history entry and stamp activity.
    pub fn record_history(&self, entry: HistoryEntry) {
        self.with_state(|s| s.add_history(entry));
    }

    /// Execute `code` in this session, pre-loading `inputs` DataFrames into the
    /// kernel namespace under their variable names.
    ///
    /// # Errors
    /// Transport or kernel failures (kernel-reported failures surface as
    /// `KernelOutput::error`, not as `Err`).
    pub async fn execute_with(
        &self,
        code: &str,
        inputs: HashMap<String, RecordBatch>,
    ) -> Result<KernelOutput> {
        let sid = self.id.to_string();
        let input_names: Vec<String> = inputs.keys().cloned().collect();
        for (name, rb) in inputs {
            let segments = crate::backend::chunk_record_batch(&rb)?;
            let header = DatasetHeader {
                session_id: sid.clone(),
                dataset_ref: name,
                rows: rb.num_rows() as i64,
                cols: rb.num_columns() as i64,
                columns: rb
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
            };
            self.backend().send_dataset(&sid, header, segments).await?;
        }
        let req = CodeRequest {
            request_id: Uuid::new_v4().to_string(),
            session_id: sid.clone(),
            code: code.to_string(),
            input_names: input_names.clone(),
            timeout_ms: 30_000,
        };
        let backend = self.backend();
        let outcome = backend.execute(&sid, req).await?;
        Ok(KernelOutput::from_outcome(&outcome))
    }
}

/// Owns every live session; also the factory for new ones.
pub struct SessionManager {
    config: Config,
    backend: Arc<dyn KernelBackend>,
    /// Named remote runners registered at runtime (`:runner connect`), keyed
    /// by user-chosen name.
    runners: RwLock<HashMap<String, Arc<dyn KernelBackend>>>,
    rt: Arc<tokio::runtime::Runtime>,
    sessions: RwLock<HashMap<Uuid, Arc<Session>>>,
    sweeper: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SessionManager {
    /// Build a manager with an explicit backend (used by tests and custom
    /// runners). The manager owns a tokio runtime to drive backend I/O; the
    /// public analysis API stays synchronous via [`SessionManager::block_on`].
    #[must_use]
    pub fn new(config: Config, backend: Arc<dyn KernelBackend>) -> Self {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build session tokio runtime"),
        );
        let manager = Self {
            config,
            backend,
            runners: RwLock::new(HashMap::new()),
            rt: rt.clone(),
            sessions: RwLock::new(HashMap::new()),
            sweeper: Mutex::new(None),
        };
        let jh = rt.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.tick().await; // first tick fires immediately — skip it
            loop {
                ticker.tick().await;
                if let Some(manager) = crate::session_manager() {
                    manager.sweep_idle_sessions();
                }
            }
        });
        *manager.sweeper.lock().unwrap() = Some(jh);
        manager
    }

    /// Run a future on the manager's runtime (used by synchronous REPL callers).
    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    /// Lazy-create a Julia-specific kernel backend. Cached so repeated `:jl`
    /// commands reuse the same backend instance (and thus the same process).
    pub async fn julia_backend(&self) -> Result<Arc<dyn KernelBackend>, String> {
        // Check cache.
        {
            let runners = self.runners.read().unwrap();
            if let Some(b) = runners.get("__julia__") {
                return Ok(b.clone());
            }
        }
        let binary = resolve_kernel_binary("opsense-kernel-julia", "OPSENSE_KERNEL_JULIA");
        let cfg = crate::backend::KernelConfig {
            command: binary,
            ..crate::backend::KernelConfig::default()
        };
        let backend = Arc::new(crate::backend::LocalIpcBackend::new(cfg));
        self.runners
            .write()
            .unwrap()
            .insert("__julia__".to_string(), backend.clone());
        Ok(backend)
    }

    /// Lazy-create a Python-specific kernel backend. Cached so repeated `:py`
    /// commands reuse the same backend instance (and thus the same process).
    /// Python is opt-in: the default kernel is `echo`, which needs no
    /// interpreter or protobuf bindings.
    pub async fn python_backend(&self) -> Result<Arc<dyn KernelBackend>, String> {
        // Check cache.
        {
            let runners = self.runners.read().unwrap();
            if let Some(b) = runners.get("__python__") {
                return Ok(b.clone());
            }
        }
        let binary = resolve_kernel_binary("opsense-kernel-python", "OPSENSE_KERNEL_PYTHON");
        let cfg = crate::backend::KernelConfig {
            command: binary,
            ..crate::backend::KernelConfig::default()
        };
        let backend = Arc::new(crate::backend::LocalIpcBackend::new(cfg));
        self.runners
            .write()
            .unwrap()
            .insert("__python__".to_string(), backend.clone());
        Ok(backend)
    }

    /// Create a session on an explicit backend (for multi-language support).
    pub async fn create_session_with(
        &self,
        backend: Arc<dyn KernelBackend>,
    ) -> Result<Arc<Session>> {
        let id = uuid::Uuid::now_v7();
        let limits = ResourceLimits::from_config(&self.config);
        let params = SessionParams {
            session_id: id.to_string(),
            env: Default::default(),
            allow_fs: limits.allow_fs,
            allow_net: limits.allow_net,
            max_memory_mb: limits.max_memory_mb,
            packages: vec![],
        };
        backend.start_session(params).await?;

        let mut state = SessionState::new();
        state.id = id;
        state.limits = limits;
        let session = Arc::new(Session {
            id,
            status: Arc::new(RwLock::new(SessionStatus::Active)),
            state: Arc::new(Mutex::new(state)),
            backend: std::sync::RwLock::new(backend),
        });
        self.sessions.write().unwrap().insert(id, session.clone());
        Ok(session)
    }

    /// Create a new analysis session (spawns a kernel process). Async form —
    /// call this when already running on the manager's runtime (REPL command
    /// dispatch); the synchronous [`Self::create_session`] wraps it with
    /// `block_on`.
    ///
    /// # Errors
    /// Propagates backend/kernel spawn or handshake failures.
    pub async fn create_session_async(&self) -> Result<Arc<Session>> {
        let id = Uuid::now_v7();
        let limits = ResourceLimits::from_config(&self.config);
        let params = SessionParams {
            session_id: id.to_string(),
            env: Default::default(),
            allow_fs: limits.allow_fs,
            allow_net: limits.allow_net,
            max_memory_mb: limits.max_memory_mb,
            packages: vec![],
        };
        self.backend.start_session(params).await?;

        let mut state = SessionState::new();
        state.id = id;
        state.limits = limits;
        let session = Arc::new(Session {
            id,
            status: Arc::new(RwLock::new(SessionStatus::Active)),
            state: Arc::new(Mutex::new(state)),
            backend: std::sync::RwLock::new(self.backend.clone()),
        });
        self.sessions.write().unwrap().insert(id, session.clone());
        Ok(session)
    }

    /// Create a new analysis session (spawns a kernel process).
    ///
    /// # Errors
    /// Propagates backend/kernel spawn or handshake failures.
    pub fn create_session(&self) -> Result<Arc<Session>> {
        self.block_on(self.create_session_async())
    }

    /// Register a remote runner under `name`; later used by
    /// [`Session::switch_backend`] through `:runner connect`.
    ///
    /// # Errors
    /// Connection to the runner failed.
    pub async fn register_runner(
        &self,
        name: &str,
        addr: &str,
    ) -> Result<std::sync::Arc<dyn KernelBackend>> {
        let backend: std::sync::Arc<dyn KernelBackend> =
            std::sync::Arc::new(crate::grpc_backend::GrpcRunnerBackend::connect(addr).await?);
        self.runners
            .write()
            .unwrap()
            .insert(name.to_string(), backend.clone());
        tracing::info!(name, addr, "registered runner");
        Ok(backend)
    }

    /// The default local-IPC backend every session starts on.
    #[must_use]
    pub fn local_backend(&self) -> std::sync::Arc<dyn KernelBackend> {
        self.backend.clone()
    }

    #[must_use]
    pub fn runner(&self, name: &str) -> Option<std::sync::Arc<dyn KernelBackend>> {
        self.runners.read().unwrap().get(name).cloned()
    }

    /// `(name, kind)` of every registered runner plus the local default.
    #[must_use]
    pub fn list_backends(&self) -> Vec<(String, &'static str)> {
        let mut rows = vec![("local".to_string(), self.backend.kind())];
        rows.extend(
            self.runners
                .read()
                .unwrap()
                .iter()
                .map(|(n, b)| (n.clone(), b.kind())),
        );
        rows
    }

    #[must_use]
    pub fn get_session(&self, id: Uuid) -> Option<Arc<Session>> {
        self.sweep_idle_sessions();
        self.sessions.read().unwrap().get(&id).cloned()
    }

    /// `(id, status, created_at)` for every live session, oldest first.
    #[must_use]
    pub fn list_sessions(&self) -> Vec<(Uuid, SessionStatus, DateTime<Utc>)> {
        self.sweep_idle_sessions();
        let mut rows: Vec<_> = self
            .sessions
            .read()
            .unwrap()
            .iter()
            .map(|(id, s)| (*id, s.status(), s.state.lock().unwrap().created_at))
            .collect();
        rows.sort_by_key(|(_, _, created)| *created);
        rows
    }

    async fn close_session_inner(&self, id: Uuid) -> bool {
        // Take the session out first so the registry guard never spans an
        // await; kernel teardown happens without holding it.
        let session = self.sessions.write().unwrap().remove(&id);
        match session {
            Some(session) => {
                session.set_status(SessionStatus::Closing);
                let _ = session.backend().close_session(&id.to_string()).await;
                session.set_status(SessionStatus::Closed);
                true
            }
            None => false,
        }
    }

    /// Close one session: cancel pending work, release the kernel process and
    /// drop it from the registry.
    pub fn close_session(&self, id: Uuid) -> bool {
        self.block_on(self.close_session_inner(id))
    }

    /// Async form of [`Self::close_session`] for callers already on the
    /// manager runtime.
    pub async fn close_session_async(&self, id: Uuid) -> bool {
        self.close_session_inner(id).await
    }

    /// Close everything (REPL exit, shutdown hook).
    pub fn close_all(&self) {
        let ids: Vec<Uuid> = self.sessions.read().unwrap().keys().copied().collect();
        for id in ids {
            self.close_session(id);
        }
        let _ = self.block_on(self.backend.shutdown());
    }

    /// Close sessions whose `last_active` exceeded their idle timeout.
    /// Called lazily on access and by the background sweeper when present.
    pub fn sweep_idle_sessions(&self) {
        let now = Utc::now();
        let expired: Vec<Uuid> = self
            .sessions
            .read()
            .unwrap()
            .iter()
            .filter(|(_, s)| {
                let st = s.state.lock().unwrap();
                now.signed_duration_since(st.last_active)
                    > chrono::Duration::seconds(st.limits.idle_timeout_secs as i64)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            tracing::info!("closing idle session {id}");
            self.close_session(id);
        }
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        if let Some(jh) = self.sweeper.lock().unwrap().take() {
            jh.abort();
        }
    }
}

/// Resolve a kernel binary: env var → sibling of current exe → workspace target → PATH.
#[must_use]
pub fn resolve_kernel_binary(name: &str, env_var: &str) -> std::path::PathBuf {
    if let Ok(p) = std::env::var(env_var) {
        return std::path::PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join(name);
            if candidate.exists() {
                return candidate;
            }
        }
    }
    let local = std::path::Path::new("target/debug").join(name);
    if local.exists() {
        return local;
    }
    std::path::PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::HealthInfo;
    use crate::state::SessionValue;
    use async_trait::async_trait;
    use bytes::Bytes;
    use opsense_core::config::{
        Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
    };
    use opsense_proto::host::ExecOutcome;
    use opsense_proto::pb::{CodeRequest, DatasetAck, DatasetHeader, SessionParams};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_config() -> Config {
        Config {
            engine: EngineConfig::default(),
            capacity: HashMap::new(),
            sources: SourcesConfig::default(),
            attributes: HashMap::new(),
            storage: StorageConfig::default(),
            pipeline: None,
            session: SessionConfig::default(),
            repl: ReplConfig::default(),
        }
    }

    struct NullBackend;

    #[async_trait]
    impl crate::backend::KernelBackend for NullBackend {
        fn kind(&self) -> &'static str {
            "null"
        }
        async fn health(&self) -> anyhow::Result<HealthInfo> {
            Ok(HealthInfo {
                name: "null".into(),
                ok: true,
                detail: String::new(),
                packages: vec![],
            })
        }
        async fn start_session(
            &self,
            params: SessionParams,
        ) -> anyhow::Result<String> {
            Ok(params.session_id)
        }
        async fn execute(
            &self,
            _session_id: &str,
            _req: CodeRequest,
        ) -> anyhow::Result<ExecOutcome> {
            Ok(ExecOutcome::default())
        }
        async fn send_dataset(
            &self,
            _session_id: &str,
            header: DatasetHeader,
            _chunks: Vec<Bytes>,
        ) -> anyhow::Result<DatasetAck> {
            Ok(DatasetAck {
                dataset_ref: header.dataset_ref,
                rows: header.rows,
                ok: true,
                error: String::new(),
            })
        }
        async fn interrupt(
            &self,
            _session_id: &str,
            _request_id: &str,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn close_session(&self, _session_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn shutdown(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn session_manager_creates_session_with_default_backend() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        assert!(session.is_active());
        assert_eq!(session.status(), SessionStatus::Active);
    }

    #[test]
    fn list_sessions_returns_all_active() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let s1 = manager.create_session().expect("s1");
        let s2 = manager.create_session().expect("s2");
        let sessions = manager.list_sessions();
        assert_eq!(sessions.len(), 2);
        let ids: Vec<_> = sessions.iter().map(|(id, _, _)| *id).collect();
        assert!(ids.contains(&s1.id()));
        assert!(ids.contains(&s2.id()));
    }

    #[test]
    fn get_session_returns_existing_session() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        let retrieved = manager.get_session(session.id());
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id(), session.id());
    }

    #[test]
    fn get_session_returns_none_for_unknown_id() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let unknown = uuid::Uuid::now_v7();
        assert!(manager.get_session(unknown).is_none());
    }

    #[test]
    fn close_session_returns_true_and_removes_session() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        assert!(manager.close_session(session.id()));
        assert!(manager.get_session(session.id()).is_none());
    }

    #[test]
    fn close_session_returns_false_for_unknown_id() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let unknown = uuid::Uuid::now_v7();
        assert!(!manager.close_session(unknown));
    }

    #[test]
    fn double_close_session_returns_false_second_time() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        assert!(manager.close_session(session.id()));
        assert!(!manager.close_session(session.id()));
    }

    #[test]
    fn close_all_closes_every_session() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        manager.create_session().expect("s1");
        manager.create_session().expect("s2");
        manager.create_session().expect("s3");
        assert_eq!(manager.list_sessions().len(), 3);
        manager.close_all();
        assert_eq!(manager.list_sessions().len(), 0);
    }

    #[test]
    fn session_status_setter_works() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        session.set_status(SessionStatus::Idle);
        assert_eq!(session.status(), SessionStatus::Idle);
        session.set_status(SessionStatus::Closed);
        assert_eq!(session.status(), SessionStatus::Closed);
    }

    #[test]
    fn session_is_active_only_when_status_active() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        assert!(session.is_active());
        session.set_status(SessionStatus::Idle);
        assert!(!session.is_active());
        session.set_status(SessionStatus::Active);
        assert!(session.is_active());
    }

    #[test]
    fn session_with_state_mutates_and_refreshes_last_active() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        let initial_last_active = {
            let state = session.state.lock().unwrap();
            state.last_active
        };
        std::thread::sleep(std::time::Duration::from_millis(10));
        session.with_state(|s| {
            s.set_variable("@1".into(), SessionValue::scalar(1.0f64));
        });
        let new_last_active = {
            let state = session.state.lock().unwrap();
            state.last_active
        };
        assert!(new_last_active > initial_last_active);
    }

    #[test]
    fn session_record_history_appends_entry() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        session.record_history(HistoryEntry {
            timestamp: chrono::Utc::now(),
            command: "do thing".into(),
            result_var: Some("@1".into()),
            success: true,
            error: None,
            duration_ms: 100,
        });
        let state = session.state.lock().unwrap();
        assert_eq!(state.history.len(), 1);
        assert_eq!(state.history[0].command, "do thing");
    }

    #[test]
    fn session_backend_returns_cloned_arc() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        let b1 = session.backend();
        let b2 = session.backend();
        assert_eq!(b1.kind(), "null");
        assert_eq!(b2.kind(), "null");
    }

    #[test]
    fn list_backends_includes_local() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let rows = manager.list_backends();
        assert!(!rows.is_empty());
        assert_eq!(rows[0].0, "local");
        assert_eq!(rows[0].1, "null");
    }

    #[test]
    fn local_backend_returns_default_backend() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let backend = manager.local_backend();
        assert_eq!(backend.kind(), "null");
    }

    #[test]
    fn runner_returns_none_for_unknown_name() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        assert!(manager.runner("nope").is_none());
    }

    #[test]
    fn sweep_idle_sessions_keeps_fresh_sessions() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        // default idle_timeout is 1800s — should NOT sweep.
        manager.sweep_idle_sessions();
        assert!(manager.get_session(session.id()).is_some());
    }

    #[test]
    fn session_id_returns_uuid() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        let id = session.id();
        // UUID v7 is non-zero.
        assert!(!id.is_nil());
    }

    #[test]
    fn session_debug_format_includes_id_and_status() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        let dbg = format!("{:?}", session);
        assert!(dbg.contains("Session"));
        assert!(dbg.contains(&session.id().to_string()));
    }

    #[test]
    fn resolve_kernel_binary_returns_env_var_when_set() {
        // Set env var and check resolution.
        let key = "OPSENSE_TEST_RESOLVE";
        let original = std::env::var(key).ok();
        std::env::set_var(key, "/some/explicit/path");
        let resolved = resolve_kernel_binary("foo", key);
        assert_eq!(resolved, std::path::PathBuf::from("/some/explicit/path"));
        // Restore.
        if let Some(orig) = original {
            std::env::set_var(key, orig);
        } else {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn resolve_kernel_binary_returns_fallback_when_env_unset() {
        let key = "OPSENSE_TEST_RESOLVE_MISSING";
        std::env::remove_var(key);
        // Should NOT panic; returns a PathBuf (may be just the name or a found path).
        let resolved = resolve_kernel_binary("nonexistent-binary-xyz", key);
        assert!(!resolved.as_os_str().is_empty());
    }

    struct CountingBackend {
        starts: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::backend::KernelBackend for CountingBackend {
        fn kind(&self) -> &'static str {
            "counting"
        }
        async fn health(&self) -> anyhow::Result<HealthInfo> {
            Ok(HealthInfo {
                name: "counting".into(),
                ok: true,
                detail: String::new(),
                packages: vec![],
            })
        }
        async fn start_session(
            &self,
            params: SessionParams,
        ) -> anyhow::Result<String> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(params.session_id)
        }
        async fn execute(
            &self,
            _session_id: &str,
            _req: CodeRequest,
        ) -> anyhow::Result<ExecOutcome> {
            Ok(ExecOutcome::default())
        }
        async fn send_dataset(
            &self,
            _session_id: &str,
            _header: DatasetHeader,
            _chunks: Vec<Bytes>,
        ) -> anyhow::Result<DatasetAck> {
            Ok(DatasetAck::default())
        }
        async fn interrupt(
            &self,
            _session_id: &str,
            _request_id: &str,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn close_session(&self, _session_id: &str) -> anyhow::Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn shutdown(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn switch_backend_uses_new_backend() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");

        let starts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let new_backend: Arc<dyn crate::backend::KernelBackend> = Arc::new(CountingBackend {
            starts: starts.clone(),
            closes: closes.clone(),
        });
        session.switch_backend(new_backend).await.expect("switch");

        assert_eq!(starts.load(Ordering::SeqCst), 1);
        // old NullBackend should have been asked to close
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(session.backend().kind(), "counting");
    }

    #[tokio::test]
    async fn switch_backend_keeps_old_when_new_fails() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        assert_eq!(session.backend().kind(), "null");

        // Backend that fails start_session.
        struct FailingBackend;
        #[async_trait]
        impl crate::backend::KernelBackend for FailingBackend {
            fn kind(&self) -> &'static str {
                "failing"
            }
            async fn health(&self) -> anyhow::Result<HealthInfo> {
                Ok(HealthInfo {
                    name: "failing".into(),
                    ok: true,
                    detail: String::new(),
                    packages: vec![],
                })
            }
            async fn start_session(
                &self,
                _params: SessionParams,
            ) -> anyhow::Result<String> {
                anyhow::bail!("kernel spawn failed")
            }
            async fn execute(
                &self,
                _session_id: &str,
                _req: CodeRequest,
            ) -> anyhow::Result<ExecOutcome> {
                Ok(ExecOutcome::default())
            }
            async fn send_dataset(
                &self,
                _session_id: &str,
                _h: DatasetHeader,
                _c: Vec<Bytes>,
            ) -> anyhow::Result<DatasetAck> {
                Ok(DatasetAck::default())
            }
            async fn interrupt(
                &self,
                _session_id: &str,
                _request_id: &str,
            ) -> anyhow::Result<()> {
                Ok(())
            }
            async fn close_session(&self, _session_id: &str) -> anyhow::Result<()> {
                Ok(())
            }
            async fn shutdown(&self) -> anyhow::Result<()> {
                Ok(())
            }
        }
        let result = session
            .switch_backend(Arc::new(FailingBackend) as Arc<dyn crate::backend::KernelBackend>)
            .await;
        assert!(result.is_err());
        // old backend retained
        assert_eq!(session.backend().kind(), "null");
    }

    #[tokio::test]
    async fn execute_with_empty_inputs_works() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("create");
        let output = session.execute_with("1+1", HashMap::new()).await.expect("exec");
        // NullBackend returns ExecOutcome::default() which has no value, no error.
        assert!(output.ok());
    }

    #[tokio::test]
    async fn create_session_with_uses_provided_backend() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let backend: Arc<dyn crate::backend::KernelBackend> = Arc::new(NullBackend);
        let session = manager.create_session_with(backend).await.expect("create");
        assert!(session.is_active());
        assert_eq!(session.backend().kind(), "null");
    }
}
