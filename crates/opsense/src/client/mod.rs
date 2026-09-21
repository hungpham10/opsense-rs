//! Thin client to `opsense serve`.
//!
//! `graphql` speaks HTTP GraphQL to a running gateway; `grpc` wraps the
//! kernel-runner wire protocol ([`RunnerClient`]) used by the `repl`
//! subcommand's `--runner` mode and by `tests/integration_runner_grpc.rs`;
//! `auth` drives the RFC 8628 device-flow login used by the REPL `:login`
//! command. `session_api` is mid-refactor and is not exported yet.

pub mod auth;
pub mod graphql;
pub mod grpc;

pub use auth::{poll_token, request_device_code, save_token_to_disk};
pub use graphql::{
    ComponentInput, EditResult, NodeSummary, Observation, OpsenseClient, SetAttributeResult,
    StationSummary, Status,
};
pub use grpc::{ExecOutcome, RunnerClient};
