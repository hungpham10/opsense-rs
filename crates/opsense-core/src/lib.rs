//! Opsense core: metric behavior and capacity analysis engine.
//!
//! This crate is intentionally free of I/O and transport concerns so it can be
//! unit-tested in isolation. The binary crate (`opsense`) wires these pieces
//! together behind an HTTP API and the collector/clock tasks.
//!
//! NOTE: the `config` module lives in `src/config.rs`. The module name
//! `config` would shadow the external `config` crate, so the dependency is
//! renamed to `config_crate` (`package = "config"`) in `Cargo.toml`.

mod config;
mod context;
mod station;

pub use config::Config;
pub use context::{Context, Stations};
pub use station::{CategoryStation, PatternStation, Station, StationKind, TimeseriesStation};

pub use opsense_model::events::{LogLevel, Observation, Signal, TelemetryKind, TimeSeries};
