//! Opsense gateway binary — library crate.
//!
//! Exported slices: `serve` (HTTP gateway + GraphQL under `/api/repl`), `api`
//! (repl GraphQL + admin + oauth), `client` (GraphQL thin client + gRPC
//! [`client::RunnerClient`]), `cli` (script-friendly subcommands, mỗi lệnh = 1
//! GraphQL round-trip), `init` (scaffold config), `repl` (interactive REPL),
//! `runner` (kernel-runner gRPC server), `mcp` (MCP stdio server over the
//! client), `tls` (process-wide rustls crypto provider) and `token`/`session`
//! (auth helpers).

pub mod api;
pub mod cli;
pub mod client;
pub mod init;
pub mod mcp;
pub mod repl;
pub mod runner;
pub mod serve;
pub mod tls;

/// Link the `opsense-qlib` crate so its typetag-registered pipeline
/// components join the component inventory deserialized from
/// pipeline TOML. Same mechanism as `opsense-components`.
/// The bare `use` is intentional (side-effect linking), so clippy's
/// `single_component_path_imports` is allowed here.
#[allow(unused_imports, clippy::single_component_path_imports)]
use opsense_qlib;
