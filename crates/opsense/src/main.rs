use clap::{CommandFactory, Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

use opsense::{mcp, repl, runner, serve, session};

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
        /// Smoke check: kết nối tới chính `bind` qua gRPC Health RPC, exit 0 nếu
        /// OK, 1 nếu lỗi. Dùng cho docker `HEALTHCHECK` và integration test.
        #[arg(long)] health_check: bool,
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
    /// Service Account (long session) — Ed25519 keypair do serve mint,
    /// dùng để REPL ký request gRPC tới Runner.
    Session {
        #[command(subcommand)]
        action: SessionSubcmd,
    },
}

#[derive(Subcommand, Debug)]
enum SessionSubcmd {
    /// Mint Ed25519 keypair mới từ serve, lưu vào `~/.config/opsense/sessions/<id>.json`.
    Issue {
        #[arg(long, help = "Opsense host (vd https://opsense.example.com). Env: OPSENSE_HOST")]
        host: Option<String>,
    },
    /// Liệt kê session: remote (gọi serve) + local file.
    List {
        #[arg(long)] host: Option<String>,
    },
    /// Revoke session trên serve + xoá file local.
    Revoke {
        /// session_id (base64 public key) cần revoke.
        session_id: String,
        #[arg(long)] host: Option<String>,
    },
    /// In `private_key` ra stdout — dùng để copy sang máy khác.
    ///   opsense session resolve <id> | ssh runner 'opsense session import <id> $(cat) …'
    Resolve {
        session_id: String,
    },
    /// Import session từ `private_key` đã copy từ máy khác.
    Import {
        session_id: String,
        /// base64 private key (Ed25519 secret, 32 bytes raw).
        private_key: String,
        /// RFC 3339 timestamp; mặc định now + 8h.
        #[arg(long)] expires_at: Option<String>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    opsense_libs::vector::components::used();
    sqlx::any::install_default_drivers();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            match Cli::parse().command {
                Some(Commands::Runner { bind, kernel, kernel_args, health_check }) => {
                    if health_check {
                        // Smoke check: connect to bind via gRPC Health RPC, exit 0 if OK, 1 if error.
                        match runner::health_check(bind.unwrap_or_else(|| "127.0.0.1:50051".parse().unwrap())).await {
                            Ok(()) => std::process::exit(0),
                            Err(e) => {
                                eprintln!("runner health check failed: {e}");
                                std::process::exit(1);
                            }
                        }
                    }
                    
                    if let Err(e) = runner::run(bind, kernel, kernel_args.unwrap_or_default()).await {
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
                Some(Commands::Session { action }) => {
                    let cmd = match action {
                        SessionSubcmd::Issue { host } => session::cli::SessionCmd {
                            action: session::cli::SessionAction::Issue,
                            host,
                        },
                        SessionSubcmd::List { host } => session::cli::SessionCmd {
                            action: session::cli::SessionAction::List,
                            host,
                        },
                        SessionSubcmd::Revoke { session_id, host } => session::cli::SessionCmd {
                            action: session::cli::SessionAction::Revoke(session_id),
                            host,
                        },
                        SessionSubcmd::Resolve { session_id } => session::cli::SessionCmd {
                            action: session::cli::SessionAction::Resolve(session_id),
                            host: None,
                        },
                        SessionSubcmd::Import { session_id, private_key, expires_at } => {
                            session::cli::SessionCmd {
                                action: session::cli::SessionAction::Import {
                                    session_id,
                                    private_key,
                                    expires_at,
                                },
                                host: None,
                            }
                        }
                    };
                    if let Err(e) = session::cli::run(cmd) {
                        eprintln!("session error: {e}");
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
