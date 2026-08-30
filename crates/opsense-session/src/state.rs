//! Session state management: variable namespace, history, artifacts.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::limits::{ResourceLimits, ResourceUsage};

/// Type of value stored in session variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionValueType {
    /// Arrow RecordBatch (the DataFrame of the Rust side).
    DataFrame,
    /// Scalar value (number/string/bool/JSON).
    Scalar,
    /// Plot artifact (PNG/SVG bytes).
    Plot,
    /// ML model artifact (pickled bytes).
    Model,
    /// Raw bytes.
    Bytes,
}

/// Internal payload of a [`SessionValue`].
#[derive(Debug, Clone)]
pub enum SessionValueData {
    DataFrame(RecordBatch),
    Scalar(serde_json::Value),
    Plot(Vec<u8>),
    Model(Vec<u8>),
    Bytes(Vec<u8>),
}

impl SessionValueData {
    fn tag(&self) -> &'static str {
        match self {
            SessionValueData::DataFrame(_) => "dataframe",
            SessionValueData::Scalar(_) => "scalar",
            SessionValueData::Plot(_) => "plot",
            SessionValueData::Model(_) => "model",
            SessionValueData::Bytes(_) => "bytes",
        }
    }
}

impl Serialize for SessionValueData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use base64::Engine as _;
        match self {
            // DataFrames survive persistence through Arrow IPC bytes.
            SessionValueData::DataFrame(rb) => {
                let mut buf = Vec::new();
                {
                    let mut writer =
                        arrow::ipc::writer::StreamWriter::try_new(&mut buf, &rb.schema())
                            .map_err(serde::ser::Error::custom)?;
                    writer.write(rb).map_err(serde::ser::Error::custom)?;
                    writer.finish().map_err(serde::ser::Error::custom)?;
                }
                serializer.serialize_str(&format!(
                    "{}:{}",
                    self.tag(),
                    base64::engine::general_purpose::STANDARD.encode(buf)
                ))
            }
            SessionValueData::Scalar(v) => v.serialize(serializer),
            SessionValueData::Plot(b) | SessionValueData::Model(b) | SessionValueData::Bytes(b) => {
                serializer.serialize_str(&format!(
                    "{}:{}",
                    self.tag(),
                    base64::engine::general_purpose::STANDARD.encode(b)
                ))
            }
        }
    }
}

impl<'de> Deserialize<'de> for SessionValueData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use base64::Engine as _;
        let s = String::deserialize(deserializer)?;

        let decode = |payload: &str| -> Result<Vec<u8>, D::Error> {
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .map_err(serde::de::Error::custom)
        };

        let ipc_to_batch = |bytes: Vec<u8>| -> Result<RecordBatch, D::Error> {
            let reader = arrow::ipc::reader::StreamReader::try_new(Cursor(bytes), None)
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            let schema = reader.schema().clone();
            let mut batches = Vec::new();
            for batch in reader {
                batches.push(batch.map_err(|e| serde::de::Error::custom(e.to_string()))?);
            }
            match batches.len() {
                1 => Ok(batches.remove(0)),
                0 => Err(serde::de::Error::custom("empty dataframe snapshot")),
                _ => arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| serde::de::Error::custom(e.to_string())),
            }
        };
        struct Cursor(Vec<u8>);
        impl std::io::Read for Cursor {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = buf.len().min(self.0.len());
                buf[..n].copy_from_slice(&self.0[..n]);
                self.0.drain(..n);
                Ok(n)
            }
        }

        if let Some(payload) = s.strip_prefix("dataframe:") {
            let batch = ipc_to_batch(decode(payload)?)?;
            return Ok(SessionValueData::DataFrame(batch));
        }
        if let Some(payload) = s.strip_prefix("plot:") {
            return Ok(SessionValueData::Plot(decode(payload)?));
        }
        if let Some(payload) = s.strip_prefix("model:") {
            return Ok(SessionValueData::Model(decode(payload)?));
        }
        if let Some(payload) = s.strip_prefix("bytes:") {
            return Ok(SessionValueData::Bytes(decode(payload)?));
        }
        serde_json::from_str::<serde_json::Value>(&s)
            .map(SessionValueData::Scalar)
            .map_err(|_| serde::de::Error::custom("invalid session value format"))
    }
}

/// A value stored in the session namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionValue {
    pub value_type: SessionValueType,
    /// Encoded payload (tagged string; DataFrames ride as Arrow IPC base64).
    pub data: SessionValueData,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl SessionValue {
    #[must_use]
    pub fn dataframe(rb: RecordBatch) -> Self {
        Self {
            value_type: SessionValueType::DataFrame,
            data: SessionValueData::DataFrame(rb),
            created_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn scalar<T: Serialize>(value: T) -> Self {
        Self {
            value_type: SessionValueType::Scalar,
            data: SessionValueData::Scalar(
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
            ),
            created_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn plot(data: Vec<u8>, format: &str) -> Self {
        Self {
            value_type: SessionValueType::Plot,
            data: SessionValueData::Plot(data),
            created_at: Utc::now(),
            metadata: [("format".to_string(), format.to_string())]
                .into_iter()
                .collect(),
        }
    }

    pub fn model(data: Vec<u8>) -> Self {
        Self {
            value_type: SessionValueType::Model,
            data: SessionValueData::Model(data),
            created_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    #[must_use]
    pub fn as_dataframe(&self) -> Option<&RecordBatch> {
        match &self.data {
            SessionValueData::DataFrame(rb) => Some(rb),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_scalar(&self) -> Option<&serde_json::Value> {
        match &self.data {
            SessionValueData::Scalar(v) => Some(v),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.data {
            SessionValueData::Plot(v) | SessionValueData::Model(v) | SessionValueData::Bytes(v) => {
                Some(v)
            }
            _ => None,
        }
    }
}

/// History entry for one executed command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Utc>,
    pub command: String,
    pub result_var: Option<String>,
    pub success: bool,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// Artifact stored in a session (plots, models, exports).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub name: String,
    pub artifact_type: ArtifactType,
    pub data: Vec<u8>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactType {
    Plot,
    Model,
    Export,
    Other,
}

/// Result of an analysis execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub id: String,
    /// `"stats"`, `"ml"`, `"python"`, `"rhai"`, …
    pub analysis_type: String,
    pub code: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub artifacts: Vec<String>,
    pub duration_ms: u64,
    pub success: bool,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Full mutable session state.
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionState {
    pub id: Uuid,
    #[serde(skip)]
    pub variables: HashMap<String, SessionValue>,
    pub current_station: Option<String>,
    pub history: Vec<HistoryEntry>,
    #[serde(skip)]
    pub artifacts: HashMap<String, Artifact>,
    #[serde(skip)]
    pub analysis_results: HashMap<String, AnalysisResult>,
    pub limits: ResourceLimits,
    #[serde(skip)]
    pub usage: ResourceUsage,
    pub created_at: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            id: Uuid::now_v7(),
            variables: HashMap::new(),
            current_station: None,
            history: Vec::new(),
            artifacts: HashMap::new(),
            analysis_results: HashMap::new(),
            limits: ResourceLimits::default(),
            usage: ResourceUsage::default(),
            created_at: Utc::now(),
            last_active: Utc::now(),
        }
    }
}

impl SessionState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_variable(&mut self, name: String, value: SessionValue) {
        self.usage.result_rows = self
            .variables
            .values()
            .filter_map(|v| v.as_dataframe())
            .map(|rb| rb.num_rows() as u64)
            .sum();
        self.variables.insert(name, value);
        self.last_active = Utc::now();
    }

    #[must_use]
    pub fn get_variable(&self, name: &str) -> Option<&SessionValue> {
        self.variables.get(name)
    }

    /// Next auto variable name: `@1`, `@2`, … based on the highest existing
    /// suffix so names stay unique after deletions.
    #[must_use]
    pub fn next_var_name(&self) -> String {
        let max = self
            .variables
            .keys()
            .filter_map(|k| k.strip_prefix('@'))
            .filter_map(|n| n.parse::<usize>().ok())
            .max()
            .unwrap_or(0);
        format!("@{}", max + 1)
    }

    pub fn add_history(&mut self, entry: HistoryEntry) {
        self.history.push(entry);
        while self.history.len() > self.limits.max_history {
            self.history.remove(0);
        }
    }

    pub fn add_artifact(&mut self, artifact: Artifact) {
        self.artifacts.insert(artifact.id.clone(), artifact);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(
                    (0..rows).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn session_value_dataframe_factory() {
        let value = SessionValue::dataframe(make_batch(5));
        assert_eq!(value.value_type, SessionValueType::DataFrame);
        let df = value.as_dataframe().unwrap();
        assert_eq!(df.num_rows(), 5);
    }

    #[test]
    fn session_value_scalar_factory_handles_types() {
        let int_val = SessionValue::scalar(42i64);
        let float_val = SessionValue::scalar(3.14f64);
        let str_val = SessionValue::scalar("hello".to_string());
        let bool_val = SessionValue::scalar(true);

        assert_eq!(int_val.as_scalar().unwrap().as_i64(), Some(42));
        assert_eq!(
            float_val.as_scalar().unwrap().as_f64().unwrap() * 100.0,
            314.0
        );
        assert_eq!(str_val.as_scalar().unwrap().as_str(), Some("hello"));
        assert_eq!(bool_val.as_scalar().unwrap().as_bool(), Some(true));
    }

    #[test]
    fn session_value_plot_factory_stores_format_metadata() {
        let value = SessionValue::plot(vec![1, 2, 3, 4], "png");
        assert_eq!(value.value_type, SessionValueType::Plot);
        assert_eq!(value.metadata.get("format"), Some(&"png".to_string()));
        assert_eq!(value.as_bytes().unwrap(), &[1u8, 2, 3, 4]);
    }

    #[test]
    fn session_value_model_factory() {
        let value = SessionValue::model(vec![10, 20, 30]);
        assert_eq!(value.value_type, SessionValueType::Model);
        assert_eq!(value.as_bytes().unwrap(), &[10u8, 20, 30]);
    }

    #[test]
    fn session_value_accessors_return_none_for_wrong_type() {
        let scalar = SessionValue::scalar(1.0f64);
        assert!(scalar.as_dataframe().is_none());
        assert!(scalar.as_bytes().is_none());

        let plot = SessionValue::plot(vec![1u8], "png");
        assert!(plot.as_scalar().is_none());
        assert!(plot.as_dataframe().is_none());

        let df = SessionValue::dataframe(make_batch(1));
        assert!(df.as_scalar().is_none());
        assert!(df.as_bytes().is_none());
    }

    #[test]
    fn session_state_default_has_empty_collections() {
        let state = SessionState::default();
        assert!(state.variables.is_empty());
        assert!(state.history.is_empty());
        assert!(state.artifacts.is_empty());
        assert!(state.analysis_results.is_empty());
        assert!(state.current_station.is_none());
    }

    #[test]
    fn set_variable_stores_and_gets() {
        let mut state = SessionState::default();
        state.set_variable("@1".into(), SessionValue::scalar(1.0f64));
        let v = state.get_variable("@1").unwrap();
        assert_eq!(v.as_scalar().unwrap().as_f64(), Some(1.0));
    }

    #[test]
    fn set_variable_updates_result_rows_count() {
        let mut state = SessionState::default();
        state.set_variable(
            "a".into(),
            SessionValue::dataframe(make_batch(10)),
        );
        // add another variable: result_rows sums all dataframe row counts.
        state.set_variable(
            "b".into(),
            SessionValue::dataframe(make_batch(20)),
        );
        assert_eq!(state.usage.result_rows, 30);

        // overwrite existing with a smaller batch: still sums current state.
        state.set_variable("a".into(), SessionValue::dataframe(make_batch(5)));
        assert_eq!(state.usage.result_rows, 25);
    }

    #[test]
    fn set_variable_ignores_scalars_in_row_count() {
        let mut state = SessionState::default();
        state.set_variable("scalar".into(), SessionValue::scalar(42.0f64));
        state.set_variable("df".into(), SessionValue::dataframe(make_batch(7)));
        // only the dataframe contributes
        assert_eq!(state.usage.result_rows, 7);
    }

    #[test]
    fn next_var_name_starts_at_one() {
        let state = SessionState::default();
        assert_eq!(state.next_var_name(), "@1");
    }

    #[test]
    fn next_var_name_reuses_after_delete() {
        let mut state = SessionState::default();
        state.set_variable("@1".into(), SessionValue::scalar(1.0f64));
        state.set_variable("@2".into(), SessionValue::scalar(2.0f64));
        state.set_variable("@3".into(), SessionValue::scalar(3.0f64));
        state.variables.remove("@2");
        // Highest suffix is now 3; @2 was deleted, next is @4.
        assert_eq!(state.next_var_name(), "@4");
    }

    #[test]
    fn next_var_name_ignores_non_numeric_keys() {
        let mut state = SessionState::default();
        state.set_variable("foo".into(), SessionValue::scalar(1.0f64));
        state.set_variable("bar".into(), SessionValue::scalar(2.0f64));
        // No `@N` keys yet; max=0; next=@1.
        assert_eq!(state.next_var_name(), "@1");
    }

    #[test]
    fn add_history_appends_entries() {
        let mut state = SessionState::default();
        for i in 0..5 {
            state.add_history(HistoryEntry {
                timestamp: Utc::now(),
                command: format!("cmd{i}"),
                result_var: None,
                success: true,
                error: None,
                duration_ms: i as u64,
            });
        }
        assert_eq!(state.history.len(), 5);
        assert_eq!(state.history[2].command, "cmd2");
    }

    #[test]
    fn add_history_trims_to_max_history() {
        let mut state = SessionState::default();
        state.limits.max_history = 3;
        for i in 0..10 {
            state.add_history(HistoryEntry {
                timestamp: Utc::now(),
                command: format!("cmd{i}"),
                result_var: None,
                success: true,
                error: None,
                duration_ms: i as u64,
            });
        }
        assert_eq!(state.history.len(), 3);
        // FIFO trim: oldest two dropped.
        assert_eq!(state.history[0].command, "cmd7");
        assert_eq!(state.history[2].command, "cmd9");
    }

    #[test]
    fn add_history_exact_capacity_is_kept() {
        let mut state = SessionState::default();
        state.limits.max_history = 5;
        for i in 0..5 {
            state.add_history(HistoryEntry {
                timestamp: Utc::now(),
                command: format!("cmd{i}"),
                result_var: None,
                success: true,
                error: None,
                duration_ms: i as u64,
            });
        }
        assert_eq!(state.history.len(), 5);
    }

    #[test]
    fn add_artifact_inserts_and_can_be_retrieved() {
        let mut state = SessionState::default();
        let artifact = Artifact {
            id: "plot1".into(),
            name: "my plot".into(),
            artifact_type: ArtifactType::Plot,
            data: vec![0xFFu8; 16],
            metadata: HashMap::from([("k".to_string(), "v".to_string())]),
            created_at: Utc::now(),
        };
        state.add_artifact(artifact);
        assert_eq!(state.artifacts.len(), 1);
        assert_eq!(state.artifacts["plot1"].name, "my plot");
    }

    #[test]
    fn dataframe_value_serde_roundtrip_preserves_rows_and_cols() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![0.1, 0.2, 0.3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();
        let value = SessionValue::dataframe(batch);
        let json = serde_json::to_string(&value).unwrap();
        let back: SessionValue = serde_json::from_str(&json).unwrap();
        let rb = back.as_dataframe().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 3);
    }

    #[test]
    fn plot_value_serde_roundtrip_preserves_bytes() {
        let value = SessionValue::plot(vec![1, 2, 3, 4, 5], "svg");
        let json = serde_json::to_string(&value).unwrap();
        let back: SessionValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.as_bytes().unwrap(), &[1u8, 2, 3, 4, 5]);
        assert_eq!(back.value_type, SessionValueType::Plot);
    }

    #[test]
    fn scalar_value_serde_roundtrip() {
        let value = SessionValue::scalar(123.456f64);
        let json = serde_json::to_string(&value).unwrap();
        let back: SessionValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.value_type, SessionValueType::Scalar);
        assert_eq!(
            back.as_scalar().unwrap().as_f64().unwrap() * 1000.0,
            123456.0
        );
    }

    #[test]
    fn model_value_serde_roundtrip() {
        let value = SessionValue::model(vec![9, 8, 7, 6]);
        let json = serde_json::to_string(&value).unwrap();
        let back: SessionValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.value_type, SessionValueType::Model);
        assert_eq!(back.as_bytes().unwrap(), &[9u8, 8, 7, 6]);
    }

    #[test]
    fn deserialize_invalid_value_returns_error() {
        // Garbage without a recognized tag prefix or valid JSON.
        let result: Result<SessionValue, _> = serde_json::from_str(r#""not-a-valid-tag""#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_invalid_base64_returns_error() {
        // dataframe tag with bogus base64 payload.
        let result: Result<SessionValue, _> =
            serde_json::from_str(r#""dataframe:!!!not_base64!!""#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_empty_dataframe_returns_error() {
        // Valid base64 but decoding into Arrow yields zero batches.
        let valid_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"random bytes that are not arrow",
        );
        let payload = format!("dataframe:{valid_b64}");
        let result: Result<SessionValue, _> = serde_json::from_str(&payload);
        // Either the arrow decode fails or yields an empty stream error.
        assert!(result.is_err());
    }

    #[test]
    fn history_entry_serde_roundtrip() {
        let entry = HistoryEntry {
            timestamp: Utc::now(),
            command: "test".into(),
            result_var: Some("@1".into()),
            success: false,
            error: Some("boom".into()),
            duration_ms: 12345,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: HistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.command, "test");
        assert_eq!(back.result_var, Some("@1".to_string()));
        assert!(!back.success);
        assert_eq!(back.error, Some("boom".to_string()));
        assert_eq!(back.duration_ms, 12345);
    }

    #[test]
    fn artifact_type_serde_all_variants() {
        for (variant, expected) in [
            (ArtifactType::Plot, r#""Plot""#),
            (ArtifactType::Model, r#""Model""#),
            (ArtifactType::Export, r#""Export""#),
            (ArtifactType::Other, r#""Other""#),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected);
        }
    }
}
