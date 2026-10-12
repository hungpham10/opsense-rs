use opsense_mod::{BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest,
                 ModuleError, OpsenseModule, RouteSpec, TenantServices};

thread_local! {
    static HOST: std::cell::RefCell<Option<*const opsense_mod::ffi::HostV1>> = const { std::cell::RefCell::new(None) };
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
                RouteSpec { id: "ping".into(), method: HttpMethod::Get, path: "/ping".into() },
                RouteSpec { id: "host-status".into(), method: HttpMethod::Get, path: "/host-status".into() },
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
                    opsense_mod::ffi::host_call(&*host, "status", &serde_json::json!({}))
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
}

#[unsafe(no_mangle)]
pub extern "C" fn opsense_module_hello_init(
    host: *const opsense_mod::ffi::HostV1,
) -> *mut opsense_mod::ffi::ModuleV1 {
    use opsense_mod::ffi::ModuleExports;
    HOST.with(|h| *h.borrow_mut() = Some(host));
    Box::into_raw(HelloModule.into_vtable())
}