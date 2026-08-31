//! Opsense core: metric behavior and capacity analysis engine.
//!
//! This crate is intentionally free of I/O and transport concerns so it can be
//! unit-tested in isolation. The binary crate (`opsense`) wires these pieces
//! together behind an HTTP API and the collector/clock tasks.
//!
//! NOTE: the `config` module lives in `src/config.rs`. The module name
//! `config` would shadow the external `config` crate, so the dependency is
//! renamed to `config_crate` (`package = "config"`) in `Cargo.toml`.

pub mod collector;
pub mod config;
pub mod context;
pub mod script;
pub mod source;
pub mod station;
pub mod template;

pub use context::{Context, OpsenseContext, Stations, Watermarks};
pub use opsense_model::{LogLevel, Observation, Signal, TelemetryKind, TimeSeries};
pub use station::{Cursor, Stage, Station};
