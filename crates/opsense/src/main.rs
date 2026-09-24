use clap::{CommandFactory, Parser, Subcommand};

use std::net::SocketAddr;
use std::path::PathBuf;

use opsense::serve;

// Force `opsense-components` to be linked into the binary: its pipeline
// components (`timeseries_station_sink`, `http`, `telegram`, …) are
// registered with typetag via `#[used]`-style statics, which rustc/linker
// strips when no code path references the crate. Without this, configs that
// use those component types fail to deserialize at runtime:
//   unknown variant `timeseries_station_sink`, expected one of `clock`, ...
#[allow(unused_imports)]
use opsense_components as _;

// Same force-link for `opsense-rhai`: it registers `rhai_transform` via the
// same typetag/inventory mechanism, so configs like
// strategies/prometheus/config.toml (clock → http → rhai → tsdb) must be
// able to deserialize it in `opsense serve` / `opsense validate`.
#[allow(unused_imports)]
use opsense_rhai as _;

#[derive(Parser, Debug)]
#[command(
    name = "opsense",
    about = "One gateway for every site relabitity activities"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the Opsense service: pipeline runtime + REST/GraphQL API.
    ///
    /// Listener mode via `GATEWAY_LISTENER` (`http`=TCP, default `unix`),
    /// config path via `OPSENSE_CONFIG` (default `.opsense/config.toml`).
    Serve {},

    /// Run the MCP stdio server (Model Context Protocol client tooling).
    ///
    /// Thin client that speaks to a running `opsense serve` over GraphQL at
    /// `OPSENSE_GRAPHQL_URL` (default `http://127.0.0.1:8080/graphql`).
    Mcp {},

    /// Run the opsense REPL client.
    ///
    /// Without `--runner` this talks to a running gateway over GraphQL
    /// (`$OPSENSE_GRAPHQL_URL`, default `http://127.0.0.1:8080/graphql`);
    /// with `--runner` it connects directly to a kernel-runner gRPC endpoint
    /// (kernel mode, commands `:echo`/`:py`/`:jl`, `:inline`/`:block`).
    Repl {
        /// GraphQL endpoint to talk to (default `$OPSENSE_GRAPHQL_URL` or
        /// `http://127.0.0.1:8080/graphql`).
        #[arg(long)]
        endpoint: Option<String>,
        /// Kernel-runner gRPC endpoint (e.g. `http://opsense-runner:50051`);
        /// enables kernel mode.
        #[arg(long)]
        runner: Option<String>,
    },

    /// Validate config.toml without running the service.
    ///
    /// Reads config from `OPSENSE_CONFIG` (default `.opsense/config.toml`)
    /// or explicit `--config` path, parses and validates it.
    Validate {
        /// Explicit config file path (overrides OPSENSE_CONFIG).
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,
    },

    /// Run the opsense kernel runner: a standalone execution worker exposing
    /// the `KernelRunner` gRPC service, spawning one kernel process per
    /// session (echo / python / julia).
    ///
    /// Bind, kernel command and auth come from `~/.config/opsense/runner.json`
    /// (`OPSENSE_RUNNER_CONFIG`), then env (`OPSENSE_RUNNER_BIND`,
    /// `OPSENSE_KERNEL`, `OPSENSE_SERVE_URL`, `OPSENSE_ADMIN_TOKEN`), then
    /// CLI overrides below.
    Runner {
        /// gRPC bind address (default from config/env `OPSENSE_RUNNER_BIND`,
        /// fallback `0.0.0.0:50051`).
        bind: Option<SocketAddr>,
        /// Kernel binary to spawn per session; overrides config/env
        /// `OPSENSE_KERNEL`.
        #[arg(long = "kernel-command", value_name = "PATH")]
        kernel_command: Option<PathBuf>,
        /// Extra argument passed to the kernel binary (repeatable).
        #[arg(long = "kernel-arg", value_name = "ARG")]
        kernel_args: Vec<String>,
        /// Connect to `bind`, run one `KernelRunner::Health` RPC, then exit.
        /// Used by container healthchecks (`opsense runner --health-check`).
        #[arg(long)]
        health_check: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    // Register sqlx `any` drivers (mysql/postgres/sqlite) before the Resolver
    // builds its connection pools.
    sqlx::any::install_default_drivers();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            match Cli::parse().command {
                Some(Commands::Serve {}) => {
                    if let Err(e) = serve::run().await {
                        eprintln!("server error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Mcp {}) => {
                    if let Err(e) = opsense::mcp::run(None).await {
                        eprintln!("mcp error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Repl { endpoint, runner }) => {
                    if let Err(e) = opsense::repl::run(endpoint, runner).await {
                        eprintln!("repl error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Validate { config }) => {
                    if let Err(e) = opsense::serve::validate_config(config).await {
                        eprintln!("config validation failed: {e}");
                        std::process::exit(1);
                    } else {
                        println!("config validation passed");
                    }
                }
                Some(Commands::Runner {
                    bind,
                    kernel_command,
                    kernel_args,
                    health_check,
                }) => {
                    if health_check {
                        if let Err(e) = opsense::runner::health_check(bind).await {
                            eprintln!("runner error: {e}");
                            std::process::exit(1);
                        }
                    } else if let Err(e) =
                        opsense::runner::run(bind, kernel_command, kernel_args).await
                    {
                        eprintln!("runner error: {e}");
                        std::process::exit(1);
                    }
                }
                None => {
                    let _ = Cli::command().print_help();
                    println!();
                }
            }
        });
    Ok(())
}
