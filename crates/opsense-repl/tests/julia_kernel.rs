//! End-to-end test against the real Julia kernel binary: push a dataset and
//! make sure `_df_1` is bound in the kernel scope.
//!
//! Regression coverage for two bugs hit while wiring `:query` → `:jl`:
//! 1. `Base.require(Main, :Arrow)` loads the module without binding it, so
//!    `Main.Arrow.read(...)` crashed the kernel on dataset push.
//! 2. The host never pushed `@N` for sub-REPL code referencing `_df_N`.
//!
//! Run with: `cargo test -p opsense-repl --test julia_kernel -- --ignored --nocapture`
//! Requires the kernel binary (`cargo build -p opsense-kernel-julia`) and
//! Arrow.jl in the default Julia depot.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use opsense_core::config::{
    Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
};
use opsense_core::{Observation, Signal, TelemetryKind};
use opsense_repl::commands::observations_to_record_batch;
use opsense_session::{KernelConfig, KernelOutput, LocalIpcBackend, Session, SessionManager, SessionValue};

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

fn julia_backend() -> LocalIpcBackend {
    let binary: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/opsense-kernel-julia");
    assert!(
        binary.exists(),
        "kernel binary missing — run `cargo build -p opsense-kernel-julia` first"
    );
    LocalIpcBackend::new(KernelConfig {
        command: binary,
        args: vec![],
        allow_fs: false,
        allow_net: false,
        max_memory_mb: 2048,
    })
}

fn one_row_batch() -> RecordBatch {
    observations_to_record_batch(&[Observation::new(
        1_788_049_704,
        "up".to_string(),
        TelemetryKind::Metric,
        Signal::Raw,
        1.0,
    )])
    .expect("batch")
}

#[test]
#[ignore = "spawns the real Julia kernel (needs Julia + Arrow.jl)"]
fn julia_kernel_binds_pushed_dataset_as_df() {
    let manager = Arc::new(SessionManager::new(
        test_config(),
        Arc::new(julia_backend()),
    ));
    let session: Arc<Session> = manager
        .block_on(manager.create_session_with(Arc::new(julia_backend())))
        .expect("julia session");

    session
        .state()
        .lock()
        .unwrap()
        .set_variable("@1".into(), SessionValue::dataframe(one_row_batch()));

    // Push the dataset the way `execute_kernel_code` does. First line mirrors
    // the real sub-REPL flow (`using …` imports exports like `nrow` into Main),
    // then reference `_df_1`.
    let mut inputs: HashMap<String, RecordBatch> = HashMap::new();
    inputs.insert("@1".into(), one_row_batch());
    let setup = manager
        .block_on(session.execute_with("using DataFrames, Statistics", HashMap::new()))
        .expect("execute using");
    assert!(
        setup.error.is_none(),
        "kernel error: {:?}",
        setup.error.as_deref().unwrap_or_default()
    );
    let out: KernelOutput = manager
        .block_on(session.execute_with("nrow(_df_1)", inputs))
        .expect("execute");

    assert!(
        out.error.is_none(),
        "kernel error: {:?}",
        out.error.as_deref().unwrap_or_default()
    );
    let text = out.text.as_deref().unwrap_or_default();
    assert!(text.contains('1'), "expected nrow(_df_1) == 1, got: {text}");
}
