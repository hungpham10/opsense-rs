use axum::{
    Router, body::Body, extract::connect_info, http::Request, routing::get, serve::IncomingStream,
};

use axum_prometheus::PrometheusMetricLayer;
use tokio::net::unix::UCred;
use tokio::net::{TcpListener, UnixListener};
use tokio::signal;
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
    // Log ra stdout **luôn**, kể cả khi không bật OTLP. Trước đây cả subscriber
    // nằm trong nhánh "có OTLP", nên `opsense serve` chạy với endpoint mặc định
    // là **không log gì cả** và `RUST_LOG` bị bỏ qua hoàn toàn — chẩn đoán
    // pipeline (component fail, kernel không đặt lệnh, lỗi TLS) bị mù.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    let agent_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string());
    let use_alloy = std::env::var("USE_ALLOY").unwrap_or_else(|_| "false".to_string());

    if agent_endpoint == "http://127.0.0.1:4317" && use_alloy != "true" {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
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
        .with(filter)
        .with(fmt_layer)
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

pub async fn routes(app_state: AppState) -> Result<Router, Error> {
    let (prometheus_layer, _metric_handle) = PrometheusMetricLayer::pair();

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

    Ok(router)
}

fn config_path() -> PathBuf {
    if let Ok(p) = std::env::var("OPSENSE_CONFIG") {
        return PathBuf::from(p);
    }
    let dot = Path::new(".opsense/config.toml");
    if dot.exists() {
        return PathBuf::from(".opsense/config.toml");
    }
    PathBuf::from("conf/opsense.conf.toml")
}

fn load_config() -> Result<Config, Error> {
    let config_path = config_path();
    Config::load(&config_path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

/// Validate config.toml from path or OPSENSE_CONFIG env.
///
/// Besides the config invariants from `cfg.validate()`, this also builds the
/// pipeline component graph — the same deserialization `serve` performs — so
/// configs referencing a component type that isn't compiled into the binary
/// (e.g. `unknown variant 'timeseries_station_sink'`) fail *here* instead of
/// crash-looping the container.
pub async fn validate_config(opt_path: Option<PathBuf>) -> Result<(), Error> {
    let path = opt_path.unwrap_or_else(config_path);
    let cfg = Config::load(&path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    cfg.validate()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    // Deserialize the pipeline components (typetag registry) exactly like
    // `AppState::new` does when serve starts.
    crate::api::pipeline_from_config(&cfg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(())
}

/// Dọn đường dẫn socket trước khi `UnixListener::bind`.
///
/// Phải xoá được **cả hai** dạng: socket cũ (file) và thư mục baked sẵn vào image.
/// `remove_dir_all` chỉ xoá thư mục — gặp socket nó trả `ENOTDIR`, và lỗi đó bị
/// `let _ =` bỏ qua nên `bind` sau đó **luôn** `EADDRINUSE`.
///
/// Đây là lý do `docker restart opsense-serve` làm app crash-loop rồi supervisor
/// bỏ (`FATAL` sau `startretries`), trong khi nginx vẫn sống và trả 500/502 nên
/// trông như server hỏng chứ không phải app chết. Đo được: restart lần đầu ra
/// `Address already in use (os error 98)`, `supervisorctl status` báo
/// `app FATAL`; recreate container mới lên.
async fn clear_socket_path(path: &std::path::Path) -> std::io::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(md) if md.is_dir() => tokio::fs::remove_dir_all(path).await,
        Ok(_) => tokio::fs::remove_file(path).await,
        // Không có gì để dọn — đúng như mong muốn.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn run() -> std::io::Result<()> {
    let telemetry_guard = init_telemetry();

    let app_state = AppState::new(&load_config()?).await?;
    let router = routes(app_state.clone()).await?;

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
            clear_socket_path(&path).await?;
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

#[cfg(test)]
mod socket_path_tests {
    use super::clear_socket_path;

    /// Socket **cũ** (file) phải bị xoá — `remove_dir_all` trả `ENOTDIR` nên
    /// nếu chỉ dùng nó thì `bind` sau đó luôn `EADDRINUSE`.
    #[test]
    fn clears_stale_socket_file() {
        let dir = std::env::temp_dir().join(format!("opsense-sock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("axum");

        // Tạo socket rồi drop listener: file socket còn lại trên đĩa, đúng trạng
        // thái sau khi container restart.
        {
            let l = std::os::unix::net::UnixListener::bind(&path).unwrap();
            drop(l);
        }
        assert!(path.exists(), "socket cũ phải còn trên đĩa trước khi dọn");

        // Chứng minh luôn **vì sao** cần `remove_file`: `remove_dir_all` trả
        // `ENOTDIR` trên socket, tức là code cũ bỏ sót socket rồi `bind` EADDRINUSE.
        let rt_probe = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt_probe.block_on(tokio::fs::remove_dir_all(&path)).unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::NotADirectory,
            "remove_dir_all phải thất bại trên socket — nếu test này đổi thì \
             `clear_socket_path` có thể gọn hơn, nhưng đừng bỏ nhánh socket"
        );
        assert!(path.exists(), "remove_dir_all phải để lại socket");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(clear_socket_path(&path)).expect("dọn socket cũ");
        assert!(!path.exists(), "socket cũ phải bị xoá");

        // Và bind lại được — đây mới là điều thực sự cần.
        std::os::unix::net::UnixListener::bind(&path).expect("bind lại phải được");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Socket cũ nằm trong thư mục **đã bị xoá** thì không phải lỗi.
    #[test]
    fn missing_path_is_ok() {
        let path = std::env::temp_dir().join("opsense-khong-ton-tai-abc/axum");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(clear_socket_path(&path)).expect("path không có là Ok");
    }

    /// Thư mục thật ở đúng vị trí socket (trường hợp baked vào image) vẫn phải xoá
    /// được, không thì `bind` cũng EADDRINUSE.
    #[test]
    fn clears_stale_directory() {
        let dir = std::env::temp_dir().join(format!("opsense-dir-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(clear_socket_path(&dir)).expect("dọn thư mục cũ");
        assert!(!dir.exists(), "thư mục cũ phải bị xoá");
    }
}
