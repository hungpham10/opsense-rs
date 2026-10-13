//! Built-in REPL module — wraps the existing `api/repl` as an `OpsenseModule`.

#[cfg(feature = "repl")]
use {
    crate::api::AppState,
    crate::api::repl::{schema, v1},
    opsense_ffi::{BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest, ModuleError, OpsenseModule, RouteSpec, TenantServices},
    async_graphql::{Schema, EmptySubscription},
    std::sync::Arc,
};

#[cfg(feature = "repl")]
pub struct ReplApiModule {
    state: AppState,
    schema: Arc<Schema<v1::QueryRoot, v1::MutationRoot, EmptySubscription>>,
}

#[cfg(feature = "repl")]
impl ReplApiModule {
    pub fn new(state: AppState) -> Self {
        let schema = Arc::new(schema());
        Self { state, schema }
    }
}

#[cfg(feature = "repl")]
impl OpsenseModule for ReplApiModule {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "repl".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            builtin: true,
            prefix: "/api/repl".into(),
            routes: vec![
                RouteSpec {
                    id: "graphql".into(),
                    method: HttpMethod::Post,
                    path: "/graphql".into(),
                },
            ],
            tools: vec![], // Tools are registered in MCP, not API
        }
    }

    fn route(&self, req: HttpRequest, _services: &TenantServices) -> BoxFut<'static, Result<HttpResponse, ModuleError>> {
        let schema = self.schema.clone();
        let state = self.state.clone();

        Box::pin(async move {
            if req.route_id != "graphql" || req.method != HttpMethod::Post {
                return Err(ModuleError::RouteNotFound(req.route_id));
            }

            // Parse GraphQL request
            let gql_req: async_graphql::Request = match serde_json::from_slice(&req.body) {
                Ok(r) => r,
                Err(e) => return Err(ModuleError::BadRequest(format!("GraphQL parse: {e}"))),
            };

            // Execute GraphQL
            let gql_req = gql_req.data(state.clone());
            let resp = schema.execute(gql_req).await;

            // Serialize response
            let resp_json = serde_json::to_vec(&resp).map_err(|e| ModuleError::Internal(format!("serialize: {e}")))?;
            Ok(HttpResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/json".into())],
                body: resp_json,
            })
        })
    }

    fn tool(&self, _name: String, _args: serde_json::Value, _services: &TenantServices) -> BoxFut<'static, Result<String, ModuleError>> {
        Box::pin(async { Err(ModuleError::RouteNotFound("no tools in API module".into())) })
    }
}