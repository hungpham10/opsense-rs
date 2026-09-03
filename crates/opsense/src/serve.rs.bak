use axum::{body::Body, http::Request, routing::get, Router};

use axum_prometheus::PrometheusMetricLayer;
use tokio::net::{TcpListener, UnixListener};
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use std::fs;
use std::io::Error;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::api::{self, AppState};

use opsense_components::{pipeline_from_config, OpsenseContext};
use opsense_core::config::Config;
use opsense_core::Context;
use opsense_libs::vector::runtime::{Event, Runtime};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

fn init_telemetry() -> Option<(SdkTracerProvider, SdkMeterProvider)> {
    let agent_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());
    let use_alloy = std::env::var("USE_ALLOY").unwrap_or_else(|_| "false".to_string());

    if agent_endpoint == "http://127.0.0.1:4317" && use_alloy != "true" {
        return None;
    }

    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", "opsense"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new(
                "deployment.environment",
                std::env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
            ),
        ])
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&agent_endpoint)
        .build()
        .ok()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&agent_endpoint)
        .build()
        .ok()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let tracer = tracer_provider.tracer("opsense");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::new("debug"))
        .with(telemetry_layer)
        .init();

    Some((tracer_provider, meter_provider))
}

#[allow(clippy::too_many_arguments)]
pub async fn routes(
    state: AppState,
    enable_sentry: bool,
    serve_mcp: bool,
) -> Result<Router, Error> {
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    let environment = std::env::var("ENVIRONMENT").unwrap_or_else(|_| "dev".to_string());

    let mut router =
        api::router(state).route("/metrics", get(|| async move { metric_handle.render() }));

    // MCP transport mounts under the gateway when enabled; opsense-mcp only
    // provides the handler handle, this router owns all serving paths.
    if serve_mcp {
        println!("MCP Streamable HTTP mounted at <gateway>/mcp");
        router = router.route("/mcp", opsense_mcp::mcp_handler());
    }

    let router = router
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<Body>| {
                let headers = request.headers();

                // Nginx injects user identity headers after OIDC/JWT auth
                // (see 04-api.conf, 05-docs.conf in nginx/vhost/)
                let user_id = headers
                    .get("x-user-id")
                    .or_else(|| headers.get("x-auth-user-id"))
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("guest");
                let email = headers
                    .get("x-user-email")
                    .or_else(|| headers.get("x-auth-email"))
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let tenant_id = headers
                    .get("x-tenant-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown");
                let is_guest = headers
                    .get("x-is-guest")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("false");

                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri(),
                    version = ?request.version(),
                    user_id = %user_id,
                    email = %email,
                    tenant_id = %tenant_id,
                    is_guest = %is_guest,
                )
            }),
        )
        .layer(prometheus_layer);

    let final_router = if enable_sentry && environment == "prod" {
        router.layer(
            ServiceBuilder::new()
                .layer(sentry::integrations::tower::SentryHttpLayer::new().enable_transaction())
                .layer(
                    sentry::integrations::tower::NewSentryLayer::<Request<Body>>::new_from_top(),
                ),
        )
    } else {
        router
    };

    Ok(final_router)
}

/// Which auxiliary servers [`run`] hosts alongside the REST gateway.
///
/// `serve.rs` is the single serving entry of the binary: REST pipeline API,
/// runner gRPC (`KernelRunner`), MCP over Streamable HTTP, plus a blocking
/// terminal mode (analysis REPL or MCP stdio) that owns stdin.
///
/// In MCP stdio mode (`mcp_stdio`, no `repl`) a missing/unloadable config is
/// not fatal: the REST gateway and pipeline are skipped and only the MCP
/// stdio server runs — `opsense_init` loads its own config per session. Every
/// other mode fails hard when the config cannot be loaded.
#[derive(Debug, Default, Clone)]
pub struct ServeModes {
    /// Host the runner gRPC server on this `host:port`.
    pub runner_bind: Option<String>,

    /// Serve MCP over Streamable HTTP under the gateway's own `/mcp` route.
    pub mcp_http: bool,
    pub port: u16,

    /// Blocking terminal mode owning stdin: interactive REPL or MCP stdio.
    pub mcp_stdio: bool,
    pub repl: bool,
}

/// Load the collection config for the REST gateway and REPL (fails hard when
/// it cannot be loaded). MCP stdio never reaches here: it loads its own config
/// lazily via `opsense_init(path)`, so a missing or invalid config in the
/// spawn cwd cannot kill the process before the MCP handshake completes.
fn load_config() -> Result<Config, Error> {
    let config_path = config_path();
    Config::load(Path::new(&config_path)).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
    })
}

/// Config path for the REST gateway and REPL: `OPSENSE_CONFIG` env override,
/// then falls back to `.opsense/config.toml` (project-local), then the old
/// `conf/opsense.conf.toml` for backwards compatibility. All paths are
/// resolved against the process working directory.
fn config_path() -> String {
    if let Ok(p) = std::env::var("OPSENSE_CONFIG") {
        return p;
    }
    let dot = Path::new(".opsense/config.toml");
    if dot.exists() {
        return ".opsense/config.toml".to_string();
    }
    "conf/opsense.conf.toml".to_string()
}

/// Spawn the optional auxiliary servers (runner gRPC) shared by both the
/// full-gateway and degraded MCP-stdio paths.
async fn spawn_extras(runner_bind: &Option<String>) -> Vec<tokio::task::JoinHandle<()>> {
    let mut extras = Vec::new();
    if let Some(bind) = runner_bind.clone() {
        extras.push(tokio::spawn(async move {
            match bind.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    println!("Runner gRPC starting on {addr}");
                    if let Err(err) = opsense_runner::run(addr).await {
                        tracing::error!("runner server failed: {err:#}");
                    }
                }
                Err(err) => tracing::error!("invalid --runner-bind `{bind}`: {err}"),
            }
        }));
    }
    extras
}

pub async fn run(modes: ServeModes) -> std::io::Result<()> {
    let telemetry_guard = init_telemetry();

    opsense_rhai::register();
    let mcp_stdio_only = modes.mcp_stdio && !modes.repl;
    if mcp_stdio_only {
        // Pure MCP stdio, no gateway: `opsense_init` loads its own config
        // later. Never build the pipeline here even when a config exists in
        // the cwd — an invalid config would exit the process before the MCP
        // handshake, and hosts spawn the binary with arbitrary cwd (Claude
        // Desktop uses `/`, codex/zcode use the workspace directory).
        let mut extras = spawn_extras(&modes.runner_bind).await;
        let result = opsense_mcp::run()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()));
        for handle in extras.drain(..) {
            handle.abort();
        }
        if let Some((trace_provider, meter_provider)) = telemetry_guard {
            let _ = trace_provider.force_flush();
            let _ = meter_provider.force_flush();
        }
        return result;
    };
    let config = load_config()?;
    let config_path = config_path();

    // Shared station registry: `OpsenseContext` (transform publish) and
    // `AppState` (API / MCP / Rhai read) point at the same instance. The
    // storage/durability tier was removed — stations are the only cache.
    let stations = OpsenseContext::new_stations();

    let ctx = OpsenseContext::from_config(&config, stations.clone());
    let collector = ctx.collector().clone();
    let components = pipeline_from_config(&config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    runtime
        .reload(components)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    let vector_handler = runtime
        .start(|event: Event| async move {
            match event {
                Event::Minor((id, e)) => {
                    tracing::warn!("runtime minor event at node {id}: {e}")
                }
                Event::Major((id, e)) => {
                    tracing::error!("runtime major event at node {id}: {e}")
                }
                Event::Panic((id, e)) => {
                    tracing::error!("runtime panic at node {id}: {e}")
                }
            }
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let app_state = AppState::new(collector, Arc::new(RwLock::new(runtime)), stations);
    let enable_sentry = std::env::var("SENTRY_DSN").is_ok();
    let router = routes(app_state.clone(), enable_sentry, modes.mcp_http).await?;

    // Auxiliary servers: runner gRPC and MCP HTTP live next to the gateway.
    let extras = spawn_extras(&modes.runner_bind).await;
    let rest = async {
        let listener_mode =
            std::env::var("GATEWAY_LISTENER").unwrap_or_else(|_| "unix".to_string());
        match listener_mode.as_str() {
            "http" => {
                let addr =
                    std::env::var("GATEWAY_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
                let tcp = TcpListener::bind(&addr).await?;
                println!("Server starting on HTTP: {}", addr);
                axum::serve(tcp, router)
                    .with_graceful_shutdown(shutdown_signal())
                    .await
            }
            _ => {
                // Default: Unix socket mode
                let path = PathBuf::from("/var/run/axum");
                let _ = tokio::fs::remove_file(&path).await;
                tokio::fs::create_dir_all(path.parent().unwrap()).await?;

                let usx = UnixListener::bind(path.clone())?;

                fs::set_permissions(&path, fs::Permissions::from_mode(0o660))?;

                println!("Server starting on Unix Socket: {:?}", path);

                axum::serve(usx, router)
                    .with_graceful_shutdown(shutdown_signal())
                    .await
            }
        }
    };

    enum Terminal {
        Rest,
        Repl,
        McpStdio,
    }
    let terminal = if modes.repl {
        Terminal::Repl
    } else if modes.mcp_stdio {
        Terminal::McpStdio
    } else {
        Terminal::Rest
    };

    let outcome = match terminal {
        Terminal::Rest => rest.await.map(|_| ()),
        Terminal::Repl | Terminal::McpStdio => {
            // The gateway keeps running while the blocking mode owns stdin.
            let rest_task = tokio::spawn(rest);
            let result: std::io::Result<()> = if modes.repl {
                let config_path = config_path.clone();
                tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                    let config = Config::load(Path::new(&config_path))
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    let manager = opsense_session::init_session_manager(&config);
                    opsense_repl::run(manager).map_err(|e| std::io::Error::other(e.to_string()))
                })
                .await
                .map_err(|e| std::io::Error::other(format!("repl task: {e}")))
                .and_then(|inner| inner)
            } else {
                opsense_mcp::run()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
            };
            rest_task.abort();
            result
        }
    };

    app_state.stop().await?;
    app_state.wait_for_shutdown().await?;
    for handle in extras {
        handle.abort();
    }
    // The event-forwarding supervisor only exits when every `report_tx`
    // clone is dropped — including the one living in the Runtime that
    // `app_state` still holds — so awaiting it here deadlocks by
    // construction. It has no cleanup of its own; aborting is correct.
    vector_handler.abort();

    if let Some((trace_provider, meter_provider)) = telemetry_guard {
        let _ = trace_provider.force_flush();
        let _ = meter_provider.force_flush();
    }
    outcome
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
