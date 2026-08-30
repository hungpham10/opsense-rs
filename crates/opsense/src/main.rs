use clap::{CommandFactory, Parser, Subcommand};
use opsense::serve;
use std::path::PathBuf;

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
    Serve {
        /// Start the interactive analysis REPL (kernel process over IPC).
        #[arg(long)]
        repl: bool,

        /// Also run the MCP server alongside the pipeline API service.
        #[arg(long)]
        mcp: bool,

        /// With --mcp: serve Streamable HTTP on --port instead of stdio.
        #[arg(long, requires = "mcp")]
        http: bool,

        /// Port for `--mcp --http`.
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// Also host the runner gRPC server (KernelRunner) on this host:port.
        #[arg(long)]
        runner_bind: Option<String>,
    },
    /// Standalone execution worker: gRPC KernelRunner service over local
    /// kernel IPC processes (`opsense runner` = execution layer of the
    /// serve <-> runner architecture).
    Runner {
        /// Socket address to bind, e.g. 127.0.0.1:50051 (or OPSENSE_RUNNER_BIND).
        #[arg(default_value_t = default_runner_bind())]
        bind: String,
    },
    /// Alias of `serve --repl` (interactive analysis REPL over a kernel IPC session).
    Repl,
    /// Alias of `serve --mcp [--http]`.
    Mcp {
        /// Serve Streamable HTTP on --port instead of stdio.
        #[arg(long)]
        http: bool,
        /// Port for --http.
        #[arg(long, default_value_t = 8080)]
        port: u16,
    },
    /// Scaffold a sample config.toml to edit yourself (default .opsense/config.toml).
    Init {
        /// Where the config is written; defaults to .opsense/config.toml.
        path: Option<PathBuf>,
        /// Overwrite the file if it already exists.
        #[arg(long)]
        force: bool,
    },
}

/// REPL needs stdin; stdio MCP owns it too — the two cannot coexist.
fn validate_serve(repl: bool, mcp: bool, http: bool) -> Result<(), String> {
    if repl && mcp && !http {
        return Err("--repl conflicts with --mcp over stdio (both read stdin). \
             Use `--repl --mcp --http` to serve MCP on HTTP alongside the REPL."
            .into());
    }
    Ok(())
}

fn default_runner_bind() -> String {
    std::env::var("OPSENSE_RUNNER_BIND").unwrap_or_else(|_| "127.0.0.1:50051".into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    match Cli::parse().command {
        Some(Commands::Init { path, force }) => opsense::init::run(path.as_deref(), force)?,
        Some(Commands::Runner { bind }) => {
            runtime.block_on(opsense_runner::run(bind.parse()?))?;
        }
        Some(Commands::Repl) => {
            runtime.block_on(serve::run(serve::ServeModes {
                repl: true,
                ..Default::default()
            }))?;
        }
        Some(Commands::Mcp { http, port }) => {
            runtime.block_on(serve::run(serve::ServeModes {
                mcp_stdio: !http,
                mcp_http: http,
                port,
                ..Default::default()
            }))?;
        }
        Some(Commands::Serve {
            repl,
            mcp,
            http,
            port,
            runner_bind,
        }) => {
            if let Err(message) = validate_serve(repl, mcp, http) {
                eprintln!("error: {message}");
                Cli::command().print_help()?;
                std::process::exit(2);
            }
            // Env overrides let operators tune serve without touching code:
            // OPSENSE_RUNNER_BIND / OPSENSE_MCP_PORT.
            let runner_bind = runner_bind.or_else(|| std::env::var("OPSENSE_RUNNER_BIND").ok());
            let port = std::env::var("OPSENSE_MCP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(port);
            // serve.rs is the single serving entry: REST gateway + optional
            // runner gRPC + MCP transports (stdio/http/REPL terminal modes).
            runtime.block_on(serve::run(serve::ServeModes {
                repl,
                mcp_stdio: mcp && !http,
                mcp_http: mcp && http,
                port,
                runner_bind,
            }))?;
        }
        None => {
            Cli::command().print_help()?;
            println!();
        }
    }
    Ok(())
}
