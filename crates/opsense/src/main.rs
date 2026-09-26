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

// Same force-link for `opsense-mlib`'s declarative converter transforms
// (`websocket_2_json`, `json_2_json` — see strategies/binance/config.toml).
// They are typetag-registered inside the `converters` module, which nothing
// else in the binary constructs directly; without this `opsense serve` would
// reject configs using them (`unknown variant 'websocket_2_json'`).
#[allow(unused_imports)]
use opsense_mlib as _;

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
    /// Scaffold a ready-to-edit config file.
    ///
    /// Default `.opsense/config.toml` — the file `opsense serve` picks up.
    /// Refuses to overwrite an existing file unless `--force`.
    Init {
        /// Target path (default `.opsense/config.toml`).
        path: Option<PathBuf>,
        /// Overwrite if the file already exists.
        #[arg(long)]
        force: bool,
    },

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

    /// Print pipeline topology + registered stations (1 GraphQL round-trip).
    Status {
        /// GraphQL endpoint (default `$OPSENSE_GRAPHQL_URL` hoặc
        /// `http://127.0.0.1:8080/graphql`).
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// Print the **live** config of pipeline components (JSON).
    ///
    /// Reads before you edit: `params` của script Rhai, `script_path`, inputs…
    /// Cùng dữ liệu với MCP tool `opsense_get_config`.
    Components {
        /// Chỉ 1 node (vd `grid`); bỏ trống → toàn bộ pipeline.
        id: Option<String>,
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// Query observations of a `timeseries` station (bounded server-side).
    ///
    /// `opsense query grid --signal order`
    Query {
        /// Station id.
        node: String,
        /// From ts (unix seconds, inclusive). Mặc định = cửa sổ tối đa cho phép.
        #[arg(long)]
        from: Option<i64>,
        /// To ts (unix seconds, inclusive). Mặc định = now.
        #[arg(long)]
        to: Option<i64>,
        /// Số dòng tối đa (mặc định 1000, trần cứng 10000).
        #[arg(long)]
        limit: Option<i64>,
        /// Lọc theo signal: `order`, `summary`, `raw`, …
        #[arg(long)]
        signal: Option<String>,
        /// Lọc theo `labels.kind`: `trading_step`, `snapshot`, …
        #[arg(long)]
        label_kind: Option<String>,
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// Trading state stored in a station: orders (`open`/`closed`) — sống qua
    /// restart vì nằm trong station, không phải RAM của node.
    Orders {
        /// Station id, vd `grid`.
        node: String,
        /// `open` | `closed`; bỏ trống → cả hai.
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        from: Option<i64>,
        #[arg(long)]
        to: Option<i64>,
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// Read ONE field of a node's live config (JSON pointer).
    ///
    /// `opsense get-param grid /params/sl_pct`
    GetParam {
        /// Node id, vd `grid`.
        id: String,
        /// JSON pointer, vd `/params/sl_pct`.
        path: String,
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// Patch ONE field of a node's live config (JSON pointer + JSON literal).
    ///
    /// `opsense set-param grid /params/sl_pct 0.02`
    ///
    /// Ưu tiên hơn `reload`: không gửi lại cả danh sách node. Server validate
    /// trước khi reload, nên patch hỏng thì runtime giữ nguyên.
    SetParam {
        /// Node id, vd `grid`.
        id: String,
        /// JSON pointer, vd `/params/sl_pct`.
        path: String,
        /// JSON literal: `0.02`, `"trading"`, `true`, `[1,2]`.
        value: String,
        #[arg(long)]
        endpoint: Option<String>,
    },

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Lệnh CLI phải parse được mà không cần server. Nếu đổi tên/cờ mà quên đổi
    /// script/CI thì chết ở đây chứ không phải lúc chạy.
    #[test]
    fn cli_subcommands_parse() {
        for args in [
            vec!["opsense", "status"],
            vec!["opsense", "components"],
            vec!["opsense", "components", "grid"],
            vec!["opsense", "get-param", "grid", "/params/sl_pct"],
            vec!["opsense", "set-param", "grid", "/params/sl_pct", "0.02"],
            vec!["opsense", "query", "grid", "--signal", "order", "--limit", "10"],
            vec!["opsense", "orders", "grid", "--status", "open"],
        ] {
            Cli::try_parse_from(&args).unwrap_or_else(|e| panic!("{args:?} không parse: {e}"));
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {    dotenvy::dotenv().ok();
    // Register sqlx `any` drivers (mysql/postgres/sqlite) before the Resolver
    // builds its connection pools.
    sqlx::any::install_default_drivers();
    // Pin ONE rustls crypto provider before any TLS work — the build ends up
    // with both `ring` and `aws-lc-rs` features enabled, which makes rustls
    // panic at the first handshake. See `opsense::tls`.
    opsense::tls::install_default_provider();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            match Cli::parse().command {
                Some(Commands::Init { path, force }) => {
                    let p = path.as_deref();
                    if let Err(e) = opsense::init::run(p, force) {
                        eprintln!("init error: {e}");
                        std::process::exit(1);
                    }
                }
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
                Some(Commands::Status { endpoint }) => {
                    if let Err(e) = opsense::cli::status(endpoint).await {
                        eprintln!("status error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Components { id, endpoint }) => {
                    if let Err(e) = opsense::cli::components(endpoint, id).await {
                        eprintln!("components error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Query {
                    node,
                    from,
                    to,
                    limit,
                    signal,
                    label_kind,
                    endpoint,
                }) => {
                    if let Err(e) = opsense::cli::query(endpoint, &node, from, to, limit, signal, label_kind).await {
                        eprintln!("query error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Orders {
                    node,
                    status,
                    from,
                    to,
                    endpoint,
                }) => {
                    if let Err(e) = opsense::cli::orders(endpoint, &node, status, from, to).await {
                        eprintln!("orders error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::GetParam { id, path, endpoint }) => {
                    if let Err(e) = opsense::cli::get_param(endpoint, &id, &path).await {
                        eprintln!("get-param error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::SetParam {
                    id,
                    path,
                    value,
                    endpoint,
                }) => {
                    if let Err(e) = opsense::cli::set_param(endpoint, &id, &path, &value).await {
                        eprintln!("set-param error: {e}");
                        std::process::exit(1);
                    }
                }
                Some(Commands::Repl { endpoint, runner }) => {                    if let Err(e) = opsense::repl::run(endpoint, runner).await {
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
