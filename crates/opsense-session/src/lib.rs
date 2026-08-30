//! Session abstraction for interactive REPL and analysis runtime.
//!
//! Provides:
//! - Session lifecycle management (create/active/idle/close, idle sweep)
//! - Variable namespace with DataFrame/scalar/plot/model values
//! - Execution through a pluggable [`backend::KernelBackend`] (local IPC kernel
//!   process today; a gRPC runner client later) — Python/R is never embedded,
//!   it runs as its own process behind the framed protocol.
//! - Resource limits and cancellation tokens
//! - Session persistence (save/load/export)

pub mod backend;
pub mod cancellation;
pub mod grpc_backend;
pub mod lifecycle;
pub mod limits;
pub mod persistence;
pub mod state;

pub use backend::{
    EchoBackend, HealthInfo, KernelBackend, KernelConfig, KernelOutput, LocalIpcBackend,
};
pub use cancellation::CancellationToken;
pub use grpc_backend::GrpcRunnerBackend;
pub use lifecycle::{Session, SessionManager, SessionStatus};
pub use limits::{ResourceLimits, ResourceUsage};
pub use persistence::{export_session, import_session, load_session, save_session};
pub use state::{
    AnalysisResult, Artifact, ArtifactType, HistoryEntry, SessionState, SessionValue,
    SessionValueData, SessionValueType,
};

use std::sync::Arc;

use opsense_core::config::Config;

/// Process-global session manager (single-manager process: the REPL and MCP
/// tools both resolve work through this).
static SESSION_MANAGER: std::sync::OnceLock<Arc<SessionManager>> = std::sync::OnceLock::new();

/// Install the global session manager. The default backend is the in-process
/// `echo` backend (zero runtime dependencies — no interpreter or protobuf
/// needed, so the host always starts). An explicit `OPSENSE_KERNEL` selects an
/// external kernel binary instead; the opt-in `:py`/`:jl` paths still spawn
/// their own language kernels on demand. Later calls are no-ops and return the
/// first manager.
#[must_use]
pub fn init_session_manager(config: &Config) -> Arc<SessionManager> {
    let backend: Arc<dyn KernelBackend> = if std::env::var("OPSENSE_KERNEL").is_ok() {
        Arc::new(LocalIpcBackend::new(KernelConfig::default()))
    } else {
        Arc::new(EchoBackend)
    };
    let manager = Arc::new(SessionManager::new(config.clone(), backend));
    let _ = SESSION_MANAGER.set(manager.clone());
    manager
}

/// The global session manager, if [`init_session_manager`] ran.
#[must_use]
pub fn session_manager() -> Option<Arc<SessionManager>> {
    SESSION_MANAGER.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::HealthInfo;
    use async_trait::async_trait;
    use bytes::Bytes;
    use opsense_core::config::{
        Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
    };
    use opsense_proto::host::ExecOutcome;
    use opsense_proto::pb::{CodeRequest, DatasetAck, DatasetHeader, SessionParams};
    use std::collections::HashMap;

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

    /// In-process backend double: no kernel process, canned outcomes.
    struct NullBackend;

    #[async_trait]
    impl KernelBackend for NullBackend {
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
        async fn start_session(&self, params: SessionParams) -> anyhow::Result<String> {
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
        async fn interrupt(&self, _session_id: &str, _request_id: &str) -> anyhow::Result<()> {
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
    fn session_lifecycle_and_variables() {
        let config = test_config();
        let manager = SessionManager::new(config, Arc::new(NullBackend));
        let session = manager.create_session().expect("session");
        assert!(session.is_active());
        assert!(manager
            .list_sessions()
            .iter()
            .any(|(id, _, _)| *id == session.id()));

        let var_name = session.with_state(|s| {
            let name = s.next_var_name();
            s.set_variable(name.clone(), SessionValue::scalar(42.0f64));
            name
        });
        assert_eq!(var_name, "@1");
        assert_eq!(
            session
                .state()
                .lock()
                .unwrap()
                .get_variable("@1")
                .and_then(|v| v.as_scalar().and_then(|s| s.as_f64())),
            Some(42.0)
        );
        // Names stay unique after a delete.
        let next = session.with_state(|s| {
            s.variables.remove("@1");
            s.next_var_name()
        });
        assert_eq!(next, "@1");

        assert!(manager.close_session(session.id()));
        assert!(!manager.get_session(session.id()).is_some());
    }

    #[test]
    fn dataframe_value_roundtrips_through_serde() {
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("metric", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![Some(0.5), None, Some(2.5)])),
                Arc::new(StringArray::from(vec!["cpu", "cpu", "cpu"])),
            ],
        )
        .unwrap();

        let value = SessionValue::dataframe(batch);
        let json = serde_json::to_string(&value).unwrap();
        let back: SessionValue = serde_json::from_str(&json).unwrap();
        let rb = back.as_dataframe().expect("dataframe restored");
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 3);
    }

    #[test]
    fn idle_sweep_closes_expired_sessions() {
        let mut config = test_config();
        config.session.idle_timeout_secs = 0; // everything is immediately stale
        let manager = SessionManager::new(config, Arc::new(NullBackend));
        let session = manager.create_session().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        manager.sweep_idle_sessions();
        assert!(!manager.get_session(session.id()).is_some());
    }
}
