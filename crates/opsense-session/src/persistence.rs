//! Session persistence: snapshot to disk, portable JSON export/import.
//!
//! DataFrames survive the round-trip as Arrow IPC bytes inside
//! [`SessionValue]'s serde representation; live Python state does not.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lifecycle::{Session, SessionManager};
use crate::limits::ResourceLimits;
use crate::state::{AnalysisResult, Artifact, HistoryEntry, SessionValue};

/// Serializable session snapshot.
#[derive(Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub id: Uuid,
    pub variables: Vec<(String, SessionValue)>,
    pub current_station: Option<String>,
    pub history: Vec<HistoryEntry>,
    pub artifacts: Vec<Artifact>,
    pub analysis_results: Vec<AnalysisResult>,
    pub limits: ResourceLimits,
    pub created_at: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
}

fn snapshot_of(session: &Session) -> SessionSnapshot {
    let state = session.state.lock().unwrap();
    SessionSnapshot {
        id: state.id,
        variables: state
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        current_station: state.current_station.clone(),
        history: state.history.clone(),
        artifacts: state.artifacts.values().cloned().collect(),
        analysis_results: state.analysis_results.values().cloned().collect(),
        limits: state.limits.clone(),
        created_at: state.created_at,
        last_active: state.last_active,
    }
}

fn apply_snapshot(manager: &SessionManager, snapshot: SessionSnapshot) -> Result<Arc<Session>> {
    let session = manager.create_session()?;
    {
        let mut state = session.state.lock().unwrap();
        state.id = snapshot.id;
        state.variables = snapshot.variables.into_iter().collect();
        state.current_station = snapshot.current_station;
        state.history = snapshot.history;
        state.artifacts = snapshot
            .artifacts
            .into_iter()
            .map(|a| (a.id.clone(), a))
            .collect();
        state.analysis_results = snapshot
            .analysis_results
            .into_iter()
            .map(|r| (r.id.clone(), r))
            .collect();
        state.limits = snapshot.limits;
        state.created_at = snapshot.created_at;
        state.last_active = snapshot.last_active;
    }
    Ok(session)
}

/// Save a session to a JSON file (atomic temp+rename write).
///
/// # Errors
/// Serialization or filesystem failures.
pub fn save_session(session: &Session, path: &Path) -> Result<()> {
    let json = serde_json::to_vec_pretty(&snapshot_of(session)).context("serialize session")?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, json).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename into {}", path.display()))?;
    Ok(())
}

/// Load a session saved with [`save_session] into a fresh manager slot.
///
/// # Errors
/// File read or deserialization failures.
pub fn load_session(manager: &SessionManager, path: &Path) -> Result<Arc<Session>> {
    let json = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let snapshot: SessionSnapshot =
        serde_json::from_slice(&json).context("deserialize session snapshot")?;
    apply_snapshot(manager, snapshot)
}

/// Portable JSON export (same payload as [`save_session], returned inline).
///
/// # Errors
/// Serialization failures.
pub fn export_session(session: &Session) -> Result<String> {
    serde_json::to_string_pretty(&snapshot_of(session)).context("export session")
}

/// Import a session from [`export_session] JSON.
///
/// # Errors
/// Deserialization failures.
pub fn import_session(manager: &SessionManager, json: &str) -> Result<Arc<Session>> {
    let snapshot: SessionSnapshot = serde_json::from_str(json).context("import session")?;
    apply_snapshot(manager, snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::HealthInfo;
    use crate::lifecycle::SessionManager;
    use crate::state::{Artifact, ArtifactType, HistoryEntry, SessionValue};
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use bytes::Bytes;
    use opsense_core::config::{
        Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
    };
    use opsense_proto::host::ExecOutcome;
    use opsense_proto::pb::{CodeRequest, DatasetAck, DatasetHeader, SessionParams};
    use std::collections::HashMap;
    use std::sync::Arc;

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

    fn make_batch(rows: usize) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(
                    (0..rows).map(|i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn save_and_load_session_roundtrip() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("session");

        // populate state
        session.with_state(|s| {
            s.set_variable(
                "@1".into(),
                SessionValue::dataframe(make_batch(5)),
            );
            s.set_variable("scalar".into(), SessionValue::scalar(3.14f64));
            s.set_variable("plot".into(), SessionValue::plot(vec![1, 2, 3], "png"));
            s.current_station = Some("st-1".into());
            s.add_history(HistoryEntry {
                timestamp: chrono::Utc::now(),
                command: "test".into(),
                result_var: Some("@1".into()),
                success: true,
                error: None,
                duration_ms: 42,
            });
            s.add_artifact(Artifact {
                id: "a1".into(),
                name: "thing".into(),
                artifact_type: ArtifactType::Plot,
                data: vec![9, 9, 9],
                metadata: HashMap::new(),
                created_at: chrono::Utc::now(),
            });
        });

        let tmp = std::env::temp_dir().join(format!(
            "opsense-session-save-{}.json",
            std::process::id()
        ));
        save_session(&session, &tmp).expect("save");
        assert!(tmp.exists());

        // load into the same manager
        let loaded = load_session(&manager, &tmp).expect("load");
        let state = loaded.state.lock().unwrap();

        assert_eq!(state.variables.len(), 3);
        assert!(state.get_variable("@1").unwrap().as_dataframe().is_some());
        assert_eq!(
            state
                .get_variable("scalar")
                .unwrap()
                .as_scalar()
                .unwrap()
                .as_f64()
                .unwrap(),
            3.14
        );
        assert_eq!(state.get_variable("plot").unwrap().as_bytes().unwrap(), &[1u8, 2, 3]);
        assert_eq!(state.current_station.as_deref(), Some("st-1"));
        assert_eq!(state.history.len(), 1);
        assert_eq!(state.history[0].command, "test");
        assert_eq!(state.history[0].duration_ms, 42);
        assert_eq!(state.artifacts.len(), 1);
        assert_eq!(state.artifacts["a1"].name, "thing");

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn load_session_missing_file_returns_error() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let result = load_session(&manager, std::path::Path::new("/nonexistent/session.json"));
        assert!(result.is_err());
    }

    #[test]
    fn load_session_corrupted_json_returns_error() {
        let tmp = std::env::temp_dir().join(format!(
            "opsense-session-corrupt-{}.json",
            std::process::id()
        ));
        std::fs::write(&tmp, "not valid json {]").unwrap();

        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let result = load_session(&manager, &tmp);
        assert!(result.is_err());

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn export_and_import_session_roundtrip() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("session");

        session.with_state(|s| {
            s.set_variable("@1".into(), SessionValue::scalar(42.0f64));
            s.set_variable(
                "@2".into(),
                SessionValue::dataframe(make_batch(3)),
            );
            s.current_station = Some("st1".into());
        });

        let json = export_session(&session).expect("export");
        assert!(json.contains("@1"));
        assert!(json.contains("@2"));

        let imported = import_session(&manager, &json).expect("import");
        let state = imported.state.lock().unwrap();
        assert_eq!(state.variables.len(), 2);
        assert!(state.get_variable("@1").is_some());
        assert!(state.get_variable("@2").is_some());
        assert_eq!(state.current_station.as_deref(), Some("st1"));
    }

    #[test]
    fn import_session_invalid_json_returns_error() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let result = import_session(&manager, "not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn save_session_empty_state_writes_valid_file() {
        let manager = SessionManager::new(test_config(), Arc::new(NullBackend));
        let session = manager.create_session().expect("session");
        let tmp = std::env::temp_dir().join(format!(
            "opsense-session-empty-{}.json",
            std::process::id()
        ));
        save_session(&session, &tmp).expect("save");
        let contents = std::fs::read_to_string(&tmp).unwrap();
        assert!(contents.contains("\"id\""));
        assert!(contents.contains("\"variables\""));
        std::fs::remove_file(&tmp).ok();
    }
}
