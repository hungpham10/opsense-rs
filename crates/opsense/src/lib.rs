pub mod api;
pub mod client;
pub mod init;
pub mod mcp;
pub mod repl;
pub mod runner;
pub mod serve;
pub mod session;
pub mod token;

/// Link the `opsense-qlib` crate so its typetag-registered pipeline
/// components join the component inventory deserialized from
/// pipeline TOML. Same mechanism as `opsense-components`.
#[allow(unused_imports)]
use opsense_qlib as _qlib_inventory;
