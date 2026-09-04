use axum::{
    Router, body::Body, extract::connect_info, http::Request, routing::get, serve::IncomingStream,
};

use axum_prometheus::PrometheusMetricLayer;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use tokio::net::unix::UCred;
use tokio::net::{TcpListener, UnixListener};
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use std::fs;
use std::io::Error;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use opsense_core::Config;

use crate::api::{AppState, admin, health_check, oauth, repl};

fn init_telemetry() -> Option<(SdkTracerProvider, SdkMeterProvider)> {
    let agent_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());
    let use_alloy = std::env::var("USE_ALLOY").unwrap_or_else(|_| "false".to_string());

    if agent_endpoint == "http://127.0.0.1:4317" && use_alloy != "true" {
        return None;
    }

    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", "universal-gateway"),
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

    let tracer = tracer_provider.tracer("universal-gateway");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::new("debug"))
        .with(telemetry_layer)
        .init();

    Some((tracer_provider, meter_provider))
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct UdsConnectInfo {
    peer_addr: Arc<tokio::net::unix::SocketAddr>,
    peer_cred: UCred,
}

impl connect_info::Connected<IncomingStream<'_, UnixListener>> for UdsConnectInfo {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        let peer_addr = stream.io().peer_addr().unwrap();
        let peer_cred = stream.io().peer_cred().unwrap();
        Self {
            peer_addr: Arc::new(peer_addr),
            peer_cred,
        }
    }
}

pub async fn routes(app_state: AppState, enable_sentry: bool) -> Result<Router, Error> {
    let (prometheus_layer, _metric_handle) = PrometheusMetricLayer::pair();
    let environment = std::env::var("ENVIRONMENT").unwrap_or_else(|_| "dev".to_string());

    // TODO: xem thử có cách nào load cấu hình từ yaml bên ngoài luôn đươc không
    let router = Router::new()
        .route("/health", get(health_check))
        .nest("/api/repl", repl::routes(app_state.clone()))
        .nest("/api/admin", admin::routes())
        .nest("/api/oauth", oauth::routes());

    let router = router
        .with_state(app_state)
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
                .layer(SentryHttpLayer::new().enable_transaction())
                .layer(NewSentryLayer::<Request<Body>>::new_from_top()),
        )
    } else {
        router
    };

    Ok(final_router)
}

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

fn load_config() -> Result<Config, Error> {
    let config_path = config_path();
    Config::load(Path::new(&config_path))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

pub async fn run() -> std::io::Result<()> {
    let telemetry_guard = init_telemetry();

    let app_state = AppState::new(&load_config()?).await?;
    let router = routes(app_state.clone(), telemetry_guard.is_none()).await?;

    let listener_mode = std::env::var("GATEWAY_LISTENER").unwrap_or_else(|_| "unix".to_string());

    let serve_result = match listener_mode.as_str() {
        "http" => {
            let addr = std::env::var("GATEWAY_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
            let tcp = TcpListener::bind(&addr).await?;
            println!("Server starting on HTTP: {}", addr);
            axum::serve(tcp, router.into_make_service())
                .with_graceful_shutdown(shutdown_signal())
                .await
        }
        _ => {
            // Default: Unix socket mode
            let path = PathBuf::from("/var/run/axum");
            let _ = tokio::fs::remove_file(&path).await;
            tokio::fs::create_dir_all(path.parent().unwrap()).await?;

            let make_service = router.into_make_service_with_connect_info::<UdsConnectInfo>();
            let usx = UnixListener::bind(path.clone())?;

            fs::set_permissions(&path, fs::Permissions::from_mode(0o666))?;

            println!("Server starting on Unix Socket: {:?}", path);

            axum::serve(usx, make_service)
                .with_graceful_shutdown(shutdown_signal())
                .await
        }
    };

    app_state.stop().await?;
    app_state.wait_for_shutdown().await?;

    if let Some((trace_provider, meter_provider)) = telemetry_guard {
        let _ = trace_provider.force_flush();
        let _ = meter_provider.force_flush();
    }
    serve_result
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
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
        },
        _ = terminate => {
        },
    }
}
