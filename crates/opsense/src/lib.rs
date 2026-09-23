//! Opsense gateway binary — library crate.
//!
//! Exported slices: `serve` (HTTP / Unix-socket gateway), `api` (admin /
//! oauth / repl GraphQL), `client` (GraphQL thin client + gRPC
//! [`client::RunnerClient`]), `repl` (CLI REPL, kernel `--runner` mode) and
//! `mcp` (MCP stdio server over the client). The kernel-runner (gRPC server),
//! session and token modules are being rebuilt on top of the refactored
//! `opsense-core` / `opsense-mlib` / `opsense-model` crates and are not
//! exported yet.

pub mod api;
pub mod client;
pub mod mcp;
pub mod repl;
pub mod runner;
pub mod serve;

/// Link the `opsense-qlib` crate so its typetag-registered pipeline
/// components join the component inventory deserialized from
/// pipeline TOML. Same mechanism as `opsense-components`.
/// The bare `use` is intentional (side-effect linking), so clippy's
/// `single_component_path_imports` is allowed here.
#[allow(unused_imports, clippy::single_component_path_imports)]
use opsense_qlib;
