//! opsense REPL — thin client that talks to `opsense serve` via GraphQL, or
//! directly to a runner via gRPC when `--runner` is set.
//!
//! Every GraphQL command is a single HTTP call. There is no local state to
//! sync or discard. Pipeline changes are applied immediately on the server.
//!
//! In kernel mode (gRPC), two execution modes are supported: inline and block.

pub mod commands;
pub mod display;
mod kernel;

use anyhow::Context as _;
use reedline::{DefaultPrompt, Reedline, Signal};

use crate::client::OpsenseClient;
use commands::dispatch;
use kernel::KernelRepl;

// Đường dẫn thật là `/api/repl/graphql` — `serve.rs` nest `repl::routes()`
// (route `/graphql`) dưới `/api/repl`. Thiếu `/api/repl` thì nginx trả
// **500**, không phải 401, nên trông như server chết thay vì thiếu auth.
// `localhost` chứ KHÔNG `127.0.0.1`: nginx tra tenant theo **Host header**
// (`04-api.conf:20` -> `/api/admin/v1/tenant/{host}/id`) va seed tao
// `host = 'localhost'` (`sql/postgres/dev/50-init-tenant.sql`). Gui
// `127.0.0.1` thi khong ra tenant => **500**, trong nhu server chet chu
// khong phai la thieu auth. Xem `crates/opsense/tests/common/mod.rs:38`.
const DEFAULT_ENDPOINT: &str = "http://localhost:8080/api/repl/graphql";

/// Run the interactive REPL.
///
/// - When `runner` is `Some(endpoint)`, switch to kernel gRPC mode.
/// - Otherwise, fall back to GraphQL HTTP against `endpoint`.
pub async fn run(endpoint: Option<String>, runner: Option<String>) -> anyhow::Result<()> {
    if let Some(runner_endpoint) = runner {
        return run_kernel(runner_endpoint).await;
    }

    let endpoint = endpoint
        .or_else(|| std::env::var("OPSENSE_GRAPHQL_URL").ok())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

    let client = OpsenseClient::new(&endpoint)
        .with_context(|| format!("failed to build HTTP client for '{endpoint}'"))?;

    // Quick sanity check: try to reach the server.
    match client.status().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "warning: could not reach {endpoint}: {e:#}\n\
                 The server may not be running — try `opsense serve` first.\n"
            );
        }
    }

    println!("opsense REPL  →  {endpoint}");
    println!("type :help or :h for commands, :quit or :q to exit\n");

    let mut rl = Reedline::create();
    let prompt = DefaultPrompt::default();

    loop {
        match rl.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                if line == ":quit" || line == ":q" || line == ":exit" {
                    println!("Goodbye.");
                    break;
                }

                match dispatch(line, &client).await {
                    Ok(Some(text)) => println!("{text}"),
                    Ok(None) => {}
                    Err(e) => eprintln!("error: {e:#}"),
                }
            }
            Ok(Signal::CtrlC) => continue,
            Ok(Signal::CtrlD) => {
                println!("Goodbye.");
                break;
            }
            Ok(Signal::ExternalBreak(_)) | Ok(Signal::HostCommand(_)) => continue,
            // `Signal` is #[non_exhaustive]; ignore any future variants.
            Ok(_) => continue,
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }

    Ok(())
}

/// Run the gRPC kernel REPL loop.
async fn run_kernel(endpoint: String) -> anyhow::Result<()> {
    let mut repl = KernelRepl::new(endpoint);
    repl.run().await
}
