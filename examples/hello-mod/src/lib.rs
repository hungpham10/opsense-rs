use opsense_mod::{BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest,
                 ModuleError, OpsenseModule, RouteSpec, TenantServices};

pub struct HelloModule;

impl OpsenseModule for HelloModule {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "hello".into(),
            version: "1.0.0".into(),
            builtin: false,
            prefix: "/api/hello".into(),
            routes: vec![RouteSpec {
                id: "ping".into(), method: HttpMethod::Get, path: "/ping".into(),
            }],
            tools: vec![],
        }
    }

    fn route(&self, req: HttpRequest, svc: &TenantServices)
        -> BoxFut<'static, Result<HttpResponse, ModuleError>>
    {
        let _ = req;
        let body = format!("tenant={}", svc.tenant_id).into_bytes();
        Box::pin(async move { Ok(HttpResponse { status: 200, headers: vec![], body }) })
    }

    fn tool(&self, name: String, _args: serde_json::Value, _svc: &TenantServices)
        -> BoxFut<'static, Result<String, ModuleError>>
    {
        Box::pin(async move { Err(ModuleError::RouteNotFound(name)) })
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn opsense_module_hello_init(
    _host: *const opsense_mod::ffi::HostV1,
) -> *mut opsense_mod::ffi::ModuleV1 {
    use opsense_mod::ffi::ModuleExports;
    Box::into_raw(HelloModule.into_vtable())
}