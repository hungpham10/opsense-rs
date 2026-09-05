//! # Backtest job flow
//!
//! Tách làm 2 phần:
//! - [`protocol`]: contract chung (spec / status / event types, redis key layout,
//!   control-plane read/write) — dùng bởi cả gateway (MCP) lẫn runner.
//! - [`executor`]: `BacktestExecutor` (vector transform component) + vòng chạy
//!   backtest phía runner.

pub mod executor;
pub mod protocol;

pub use executor::BacktestExecutor;
pub use protocol::{
    BacktestJobSpec, JOB_STREAM_KEY, JobEvent, JobEventKind, JobStatus, job_cancel_key,
    job_events_key, job_status_key, read_events, read_status, request_cancel, user_guard_key,
    write_status,
};
