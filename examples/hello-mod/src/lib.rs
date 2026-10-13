use opsense_ffi::{BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest,
                 ModuleError, OpsenseModule, RouteSpec, StreamHandle, TenantServices,
                 Transport};

thread_local! {
    static HOST: std::cell::RefCell<Option<*const opsense_ffi::ffi::HostV1>> = const { std::cell::RefCell::new(None) };
}

pub struct HelloModule;

impl OpsenseModule for HelloModule {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "hello".into(),
            version: "1.0.0".into(),
            builtin: false,
            prefix: "/api/hello".into(),
            routes: vec![
                RouteSpec {
                    id: "ping".into(),
                    method: HttpMethod::Get,
                    path: "/ping".into(),
                    transport: Transport::Rest,
                    source_node: None,
                },
                RouteSpec {
                    id: "host-status".into(),
                    method: HttpMethod::Get,
                    path: "/host-status".into(),
                    transport: Transport::Rest,
                    source_node: None,
                },
                // Fixture SSE: `source_node` trỏ tới node `type = "output"`
                // trong pipeline. `stream_push` bên dưới lọc chỉ message có
                // `tick = true` — đây là chỗ "xào trộn dữ liệu" sẽ nằm.
                RouteSpec {
                    id: "ticks".into(),
                    method: HttpMethod::Get,
                    path: "/ticks".into(),
                    transport: Transport::Sse,
                    source_node: Some("market-out".into()),
                },
            ],
            tools: vec![],
        }
    }

    fn route(&self, req: HttpRequest, _svc: &TenantServices)
        -> BoxFut<'static, Result<HttpResponse, ModuleError>>
    {
        let route_id = req.route_id.clone();
        Box::pin(async move {
            if route_id == "host-status" {
                let host = HOST.with(|h| *h.borrow()).expect("host not set");
                let json = unsafe {
                    opsense_ffi::ffi::host_call(&*host, "status", &serde_json::json!({}))
                }.map_err(|e| ModuleError::Internal(e.to_string()))?;
                return Ok(HttpResponse { status: 200, headers: vec![], body: json.into_bytes() });
            }
            let body = b"hello".to_vec();
            Ok(HttpResponse { status: 200, headers: vec![], body })
        })
    }

    fn tool(&self, name: String, _args: serde_json::Value, _svc: &TenantServices)
        -> BoxFut<'static, Result<String, ModuleError>>
    {
        Box::pin(async move { Err(ModuleError::RouteNotFound(name)) })
    }

    // ── SSE (ABI 4) ────────────────────────────────────────────────────────

    fn stream(&self, _req: HttpRequest, _svc: &TenantServices)
        -> BoxFut<'static, Result<StreamHandle, ModuleError>>
    {
        Box::pin(async { Ok(StreamHandle { stream_id: "hello-ticks".into() }) })
    }

    /// Lọc message: chỉ phát cái có `tick = true`, đổi shape thành
    /// `{"tick": <payload>}`. Trả chuỗi rỗng = bỏ qua.
    fn stream_push(&self, _stream_id: String, payload: String)
        -> BoxFut<'static, Result<String, ModuleError>>
    {
        Box::pin(async move {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&payload) else {
                return Ok(String::new());
            };
            if v.get("tick").and_then(serde_json::Value::as_bool) != Some(true) {
                return Ok(String::new());
            }
            // `to_string` trên `Value` không fail.
            Ok(serde_json::json!({ "tick": v }).to_string())
        })
    }

    fn stream_close(&self, _stream_id: String) {
        tracing_shim("stream closed");
    }
}

/// `hello-mod` không phụ thuộc `tracing`; dùng stderr để không cần dep.
fn tracing_shim(msg: &str) {
    eprintln!("[hello-mod] {msg}");
}

#[unsafe(no_mangle)]
pub extern "C" fn opsense_module_hello_init(
    host: *const opsense_ffi::ffi::HostV1,
) -> *mut opsense_ffi::ffi::ModuleV1 {
    use opsense_ffi::ffi::ModuleExports;
    HOST.with(|h| *h.borrow_mut() = Some(host));
    Box::into_raw(HelloModule.into_vtable())
}