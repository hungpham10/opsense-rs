//! SSE cho module — stream dữ liệu từ node `output` trong pipeline.
//!
//! Test tầng host: dựng `AppState` thật từ config có node `output`, đăng ký
//! module builtin khai route SSE, rồi mở kết nối HTTP và đọc event.
//!
//! Không cần infra: `Secret::new()` trả `Ok` khi thiếu env Infisical
//! (`opsense-model/src/secret.rs:169`), storage để `memory`. Dữ liệu đến từ
//! node `clock` tự phát tick — không cần inject tay.

use std::time::Duration;

use axum::http::StatusCode;
use opsense::api::AppState;
use opsense::api::modules::ApiModuleRegistry;
use opsense_core::Config;
use opsense_ffi::{
    BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest, ModuleError, OpsenseModule, RouteSpec,
    StreamHandle, TenantServices, Transport,
};
use tokio::net::TcpListener;

/// Node `clock` tự phát `{"event": "tick", "ts": N, "src": "clock"}` mỗi
/// `interval_secs` (`opsense-mlib/src/vector/components/clock.rs:35`). Test
/// lọc theo `src` để chứng minh `stream_push` thật sự quyết định nội dung.
const CONFIG: &str = r#"
[engine]
poll_interval_seconds = 1

[storage]
backend = "memory"

[[pipeline.components]]
type = "clock"
id = "clock"
interval_secs = 1

# Chỉ `type = "output"` mới có broadcast channel — `#[sink]` thì không.
[[pipeline.components]]
type = "output"
id = "market-out"
inputs = ["clock"]
"#;

fn rest_route(id: &str, path: &str) -> RouteSpec {
    RouteSpec {
        id: id.into(),
        method: HttpMethod::Get,
        path: path.into(),
        transport: Transport::Rest,
        source_node: None,
    }
}

fn sse_route(id: &str, path: &str, source: Option<&str>) -> RouteSpec {
    RouteSpec {
        id: id.into(),
        method: HttpMethod::Get,
        path: path.into(),
        transport: Transport::Sse,
        source_node: source.map(String::from),
    }
}

/// Module test: 1 route SSE + 1 route REST. `stream_push` chỉ phát message có
/// `src == "clock"` và bọc lại thành `{"seen": ...}` — chỗ "xào trộn" của plan.
struct SseTestModule;

impl OpsenseModule for SseTestModule {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "sse-test".into(),
            version: "0".into(),
            builtin: true,
            prefix: "/api/sse-test".into(),
            routes: vec![
                rest_route("ping", "/ping"),
                sse_route("ticks", "/ticks", Some("market-out")),
            ],
            tools: vec![],
        }
    }

    fn route(
        &self,
        _req: HttpRequest,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<HttpResponse, ModuleError>> {
        Box::pin(async {
            Ok(HttpResponse {
                status: 200,
                headers: vec![],
                body: b"pong".to_vec(),
            })
        })
    }

    fn tool(
        &self,
        name: String,
        _args: serde_json::Value,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<String, ModuleError>> {
        Box::pin(async move { Err(ModuleError::RouteNotFound(name)) })
    }

    fn stream(
        &self,
        _req: HttpRequest,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<StreamHandle, ModuleError>> {
        Box::pin(async {
            Ok(StreamHandle {
                stream_id: "t1".into(),
            })
        })
    }

    fn stream_push(
        &self,
        _stream_id: String,
        payload: String,
    ) -> BoxFut<'static, Result<String, ModuleError>> {
        Box::pin(async move {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&payload) else {
                return Ok(String::new());
            };
            // Lọc rớt → trả "" ⇒ host không yield event.
            if v.get("src").and_then(serde_json::Value::as_str) != Some("clock") {
                return Ok(String::new());
            }
            Ok(serde_json::json!({ "seen": v }).to_string())
        })
    }
}

/// Module khai `transport = "sse"` mà quên `source_node`.
struct NoSourceModule;

impl OpsenseModule for NoSourceModule {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "no-source".into(),
            version: "0".into(),
            builtin: true,
            prefix: "/api/no-source".into(),
            routes: vec![sse_route("ticks", "/ticks", None)],
            tools: vec![],
        }
    }

    fn route(
        &self,
        _req: HttpRequest,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<HttpResponse, ModuleError>> {
        Box::pin(async {
            Ok(HttpResponse {
                status: 404,
                headers: vec![],
                body: vec![],
            })
        })
    }

    fn tool(
        &self,
        name: String,
        _args: serde_json::Value,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<String, ModuleError>> {
        Box::pin(async move { Err(ModuleError::RouteNotFound(name)) })
    }
}

/// Khai route SSE nhưng không override `stream`/`stream_push` — dùng default impl.
struct NoStreamImpl;

impl OpsenseModule for NoStreamImpl {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "no-stream".into(),
            version: "0".into(),
            builtin: true,
            prefix: "/api/no-stream".into(),
            routes: vec![sse_route("ticks", "/ticks", Some("market-out"))],
            tools: vec![],
        }
    }

    fn route(
        &self,
        _req: HttpRequest,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<HttpResponse, ModuleError>> {
        Box::pin(async {
            Ok(HttpResponse {
                status: 404,
                headers: vec![],
                body: vec![],
            })
        })
    }

    fn tool(
        &self,
        name: String,
        _args: serde_json::Value,
        _svc: &TenantServices,
    ) -> BoxFut<'static, Result<String, ModuleError>> {
        Box::pin(async move { Err(ModuleError::RouteNotFound(name)) })
    }
}

/// `Config::load` chỉ đọc từ path — ghi file tạm rồi load.
fn load_config(dir: &std::path::Path) -> Config {
    let path = dir.join("config.toml");
    std::fs::write(&path, CONFIG).expect("ghi config tạm");
    Config::load(&path)
        .expect("config test phải parse + validate")
        .0
}

/// Đọc chunk SSE cho tới khi gặp `data:` chứa `needle`, hoặc hết giờ.
async fn read_until(body: reqwest::Response, needle: &str) -> String {
    let mut body = body;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut seen = String::new();

    while tokio::time::Instant::now() < deadline {
        let Ok(Some(chunk)) = body.chunk().await else {
            break; // stream kết thúc
        };
        let text = String::from_utf8_lossy(&chunk).to_string();
        for line in text.lines() {
            seen.push_str(line);
            seen.push('\n');
            if line.starts_with("data:") && line.contains(needle) {
                return seen;
            }
        }
    }
    panic!("timeout 15s chờ `{needle}`, đã thấy:\n{seen}");
}

/// Mở kết nối SSE và trả response (để đọc chunk).
async fn open_sse(base: &str, path: &str) -> reqwest::Response {
    let res = reqwest::get(format!("{base}{path}"))
        .await
        .expect("GET SSE");
    assert_eq!(res.status(), StatusCode::OK, "SSE phải trả 200");
    assert_eq!(
        res.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream"),
        "phải là text/event-stream — nếu không client EventSource sẽ không nhận"
    );
    res
}

/// Một `AppState` cho **cả** suite.
///
/// `AppState::new` gọi `axum_prometheus::PrometheusMetricLayer::pair()` — hàm
/// này `set_global_recorder` và panic nếu gọi lần 2 (`api/mod.rs:117-119`
/// ghi rõ điều này). Nên các case phải dùng chung state, không dựng lại.
struct Suite {
    addr: String,
    #[allow(dead_code)]
    state: AppState,
}

impl Suite {
    async fn build(dir: &std::path::Path) -> Self {
        // `Resolver::new` cần DB connect được — sqlite in-memory không cần
        // server. `REDIS_DSN` phải **rỗng**: resolver `split(",")` rồi bỏ qua
        // entry rỗng (`resolver.rs:180`) ⇒ không connect Redis thật.
        // SAFETY: env process-global; suite này là test duy nhất trong file.
        unsafe {
            std::env::set_var("DB_DSN", "sqlite::memory:");
            std::env::set_var("REDIS_DSN", "");
        }

        let cfg = load_config(dir);
        let state = AppState::new(&cfg).await.expect("AppState::new");

        let mut registry = ApiModuleRegistry::new();
        registry.register_builtin(std::sync::Arc::new(SseTestModule));
        registry.register_builtin(std::sync::Arc::new(NoSourceModule));
        registry.register_builtin(std::sync::Arc::new(NoStreamImpl));

        let router = registry
            .into_router(state.clone())
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Self {
            addr: format!("http://{addr}"),
            state,
        }
    }
}

/// Gộp mọi case vào **một** test: `AppState::new` chỉ gọi được một lần mỗi
/// process, nên tách test sẽ panic ở test thứ hai.
#[tokio::test(flavor = "multi_thread")]
async fn sse_module_suite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = Suite::build(dir.path()).await;

    // ── 1. Route SSE thật sự phát event từ node `output` ──────────────────
    let res = open_sse(&s.addr, "/api/sse-test/ticks").await;
    let seen = read_until(res, "seen").await;
    assert!(
        seen.contains("clock"),
        "event phải mang dữ liệu do module quyết định (module lọc `src == \"clock\"`),\n\
         thấy:\n{seen}"
    );

    // ── 2. Route REST của cùng module vẫn chạy ────────────────────────────
    let res = reqwest::get(format!("{}/api/sse-test/ping", s.addr))
        .await
        .expect("GET REST");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "pong");

    // ── 3. Thiếu `source_node` → stream rỗng, không 500, không panic ──────
    let mut res = open_sse(&s.addr, "/api/no-source/ticks").await;
    let got = read_for(&mut res, Duration::from_secs(2)).await;
    assert!(
        !got.contains("data:"),
        "thiếu `source_node` thì không được có event nào, thấy:\n{got}"
    );

    // ── 4. Module không override `stream` (default impl trả lỗi) ──────────
    //         → stream rỗng thay vì crash.
    let mut res = open_sse(&s.addr, "/api/no-stream/ticks").await;
    let got = read_for(&mut res, Duration::from_secs(2)).await;
    assert!(
        !got.contains("data:"),
        "module không hỗ trợ SSE không được phát event, thấy:\n{got}"
    );
}

/// Đọc SSE trong `window`, trả toàn bộ text đã nhận.
///
/// Timeout là kết quả **mong muốn**: stream đang giữ mở đúng như SSE, nên
/// `chunk()` không bao giờ trả `None`. Trả `""` khi hết cửa sổ.
async fn read_for(body: &mut reqwest::Response, window: Duration) -> String {
    tokio::time::timeout(window, async {
        let mut acc = String::new();
        while let Ok(Some(chunk)) = body.chunk().await {
            acc.push_str(&String::from_utf8_lossy(&chunk));
        }
        acc
    })
    .await
    .unwrap_or_default()
}
