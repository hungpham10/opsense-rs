use clap::{CommandFactory, Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

use opsense::{mcp, repl, runner, serve};

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
    /// Run the Opsense service. Default: pipeline + REST API.
    ///
    /// Analysis/MCP/runner modes (all hosted by serve.rs):
    ///   --repl                        interactive REPL (kernel over IPC)
    ///   --mcp                         MCP server over stdio (owns stdin)
    ///   --mcp --http                  MCP server over Streamable HTTP
    ///   --runner-bind <host:port>     host the KernelRunner gRPC server
    /// Env overrides: OPSENSE_RUNNER_BIND, OPSENSE_MCP_PORT, GATEWAY_LISTENER
    Serve {},
    /// Standalone execution worker: gRPC KernelRunner service over local
    /// kernel IPC processes (`opsense runner` = execution layer of the
    /// serve <-> runner architecture).
    ///
    /// Env overrides: OPSENSE_RUNNER_BIND, OPSENSE_KERNEL,
    /// OPSENSE_RUNNER_KERNEL_ARGS
    Runner {
        #[arg(long)] bind: Option<SocketAddr>,
        #[arg(long)] kernel: Option<PathBuf>,
        #[arg(long, num_args = 1..)] kernel_args: Option<Vec<String>>,
    },
    /// Interactive thin REPL. Connects to `opsense serve` via GraphQL by
    /// default; pass `--runner` to talk directly to a gRPC runner instead.
    ///
    /// Env: OPSENSE_GRAPHQL_URL (default http://127.0.0.1:8080/graphql)
    Repl {
        #[arg(long, help = "GraphQL endpoint URL (overrides OPSENSE_GRAPHQL_URL)")]
        endpoint: Option<String>,
        /// gRPC endpoint of a running runner (e.g. "http://127.0.0.1:50051").
        /// When set, the REPL switches to kernel mode and ignores `endpoint`.
        #[arg(long, help = "gRPC runner endpoint for kernel REPL mode")]
        runner: Option<String>,
    },
    /// Alias of `serve --mcp [--http]`.
    Mcp {
        #[arg(long, help = "GraphQL endpoint URL (overrides OPSENSE_GRAPHQL_URL)")]
        endpoint: Option<String>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    opsense_libs::vector::components::used();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            match Cli::parse().command {
                Some(Commands::Runner { bind, kernel, kernel_args }) => {
                    let bind = bind.or_else(|| {
                        std::env::var("OPSENSE_RUNNER_BIND")
                            .ok()
                            .and_then(|s| s.parse().ok())
                    })
                    .unwrap_or_else(|| "0.0.0.0:50051".parse().unwrap());
                    let kernel = kernel.or_else(|| {
                        std::env::var("OPSENSE_KERNEL")
                            .ok()
                            .map(PathBuf::from)
                    })
                    .unwrap_or_else(|| {
                        opsense_runner::config::resolve_kernel_binary("opsense-kernel-echo")
                    });
                    let args = kernel_args.unwrap_or_default();
                    if let Err(e) = runner::run(bind, kernel, args).await {
                        eprintln!("runner error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Repl { endpoint, runner }) => {
                    if let Err(e) = repl::run(endpoint, runner).await {
                        eprintln!("repl error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Mcp { endpoint }) => {
                    if let Err(e) = mcp::run(endpoint).await {
                        eprintln!("mcp error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Serve {}) => {
                    if let Err(e) = serve::run().await {
                        eprintln!("server error: {e}");
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
