# Opsense — Module Framework

> Mô tả **đúng những gì code làm** (đối chiếu bằng `cargo check`, `grep`).
> Kiến trúc tổng thể: [`architecture.md`](./architecture.md).
> Hướng dẫn dùng: [`GUIDE.md`](./GUIDE.md).

---

## 1. Framework là gì — hai đường chạy module

Opsense gọi là **module** mọi thứ triển khai `OpsenseModule` (`opsense-mod/src/lib.rs:42`). Có hai cách nạp:

| Đường | Cách nạp | Ví dụ | File |
|---|---|---|---|
| **Builtin** | Link tĩnh vào binary, `ApiModuleRegistry::register_builtin` | `repl` | `serve.rs:136-139` |
| **Dynamic (.so)** | `dlopen` lúc boot, `load_dynamic` + `modules.toml` | `hello` | `modules.rs:62-117` |

Route đã là module (prefix `/api/repl`, `builtin=true`): REPL GraphQL (`repl/adapter.rs:35`).
Route **chưa phải** module: `/health`, `/api/admin/*`, `/api/oauth/*` — vẫn mount trực tiếp (`serve.rs:159-162`).

Bật dynamic module: compile với `--features opsense/dynamic-modules` (CI: `.github/workflows/ci.yml:58`).

---

## 2. Crate map

| Crate | Vai trò | Dependency |
|---|---|---|
| `opsense-mod` | **SDK nhẹ**: `OpsenseModule`, `CacheBackend`, `HostServices`, FFI vtable, `TenantServices` | `serde`, `serde_json`, `thiserror`, `tokio`, `axum`, `futures` |
| `opsense-mlib` | `RedisBackend` impl `CacheBackend` (feature `redis`), vector runtime, storage | `opsense-mod` (optional) |
| `opsense` | Binary: wiring, resolve tenant, `dlopen`, router | cả hai trên |

**Luồng depend**: `opsense` → `opsense-mod` & `opsense-mlib`; crate `opsense-mlib` **có** depend `opsense-mod` (optional, feature `redis`). Không crate nào khác depend `opsense-mod` (SDK độc lập).

---

## 3. Vòng đời module

```
dlopen(.so) → opsense_module_<id>_init(HostV1)
    → ModuleV1 { state, manifest, handle, free }
    → host gọi manifest() một lần
    → request → host Route → module.route(HttpRequest, TenantServices)
    → tool call → host Tool → module.tool(name, args, TenantServices)
unload → free string trả về
```

Symbol naming (`ffi.rs:88`, `modules.rs:84`):

```
opsense_module_<crate_name>_init   // '-' → '_'
```

Ví dụ: crate `xt-pnl` ⇒ symbol `opsense_module_xt_pnl_init`.

---

## 4. Hợp đồng FFI — `HostV1` / `ModuleV1`

`opsense-mod/src/ffi.rs`:

```rust
#[repr(C)]
pub struct HostV1 {
    pub abi_version: u32,
    /// Opaque context pointer — set by the host during `load_dynamic`.
    /// Each `HostV1` gets its own context so multiple `AppState` instances
    /// don't share the same global. The host passes this back to `call`/`log`.
    pub context: *mut std::ffi::c_void,
    pub call: unsafe extern "C" fn(*mut std::ffi::c_void, *const c_char, *const c_char) -> *mut c_char,
    pub log:  unsafe extern "C" fn(*mut std::ffi::c_void, u32, *const c_char, *const c_char),
    pub free: unsafe extern "C" fn(*mut c_char),
}
```

```rust
#[repr(C)]
pub struct ModuleV1 {
    pub abi_version: u32,
    pub state: ModuleHandle,
    pub manifest: unsafe extern "C" fn(ModuleHandle) -> *mut c_char,
    pub handle: unsafe extern "C" fn(ModuleHandle, *const TenantServices, *const c_char, *const c_char) -> *mut c_char,
    pub free: unsafe extern "C" fn(*mut c_char),
}
```

- **Mọi chuỗi là JSON UTF-8, NUL-terminated**.
- Host cấp bộ nhớ, host free (`free`). Module **không** free chuỗi nhận được.
- `ModuleHandle` = `#[repr(transparent)]` trên `*mut c_void` → `Send + Sync` an toàn.
- `ABI_VERSION` = 3 (`opsense-mod/src/lib.rs:33`). Tăng khi `HostV1`/`ModuleV1` đổi layout.

---

## 5. `OpsenseModule` — trait cốt lõi

`opsense-mod/src/lib.rs:42`:

```rust
pub trait OpsenseModule: Send + Sync + 'static {
    fn manifest(&self) -> Manifest;

    fn route(&self, req: HttpRequest, services: &TenantServices)
        -> BoxFut<'static, Result<HttpResponse, ModuleError>>;

    fn tool(&self, name: String, args: Value, services: &TenantServices)
        -> BoxFut<'static, Result<String, ModuleError>>;
}
```

- `BoxFut<'a, T>` = `Pin<Box<dyn Future<Output = T> + Send + 'a>>` (object-safe).
- `HttpRequest`/`HttpResponse` chỉ chứa `route_id`, `method`, `path`, `query`, `headers`, `body` — không dùng kiểu axum.

---

## 6. `TenantServices` — tenant per-request (an ninh)

`opsense-mod/src/cache.rs`:

```rust
pub struct TenantServices {
    pub tenant_id: i64,
    pub cache: Option<Arc<dyn CacheBackend>>,
}
```

Host dựng **mỗi request** trong `module_route_handler` (`crates/opsense/src/api/modules.rs`):

```rust
static MISSING_TENANT_WARNED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

let tenant_id = req
    .headers()
    .get("x-tenant-id")
    .and_then(|v| v.to_str().ok())
    .and_then(|s| s.parse::<i64>().ok())
    .unwrap_or_else(|| {
        if MISSING_TENANT_WARNED.set(()).is_ok() {
            tracing::warn!("thiếu/parse lỗi X-Tenant-Id → tenant_id=0 (chỉ log lần đầu)");
        }
        0
    });

let cache_backend = state
    .connector
    .cache(tenant_id)
    .map(opsense_mlib::cache::RedisBackend::new);
let services = TenantServices { tenant_id, cache: cache_backend };
module.route(http_req, &services).await
```

**Module không bao giờ tự chọn tenant** — header do host parse, backend đã bind đúng shard. Thiếu header ⇒ `tenant_id = 0` (có warn 1 lần, không im lặng — xem §14(f)).

---

## 7. `CacheBackend` / `Cache` / `Page`

`opsense-mod/src/cache.rs`:

```rust
pub trait CacheBackend: Send + Sync + 'static {
    fn get(&self, key: &str) -> BoxFuture<Result<Option<String>, CacheError>>;
    fn set_ex(&self, key: &str, value: &str, ttl: u64) -> BoxFuture<Result<(), CacheError>>;
    fn del(&self, key: &str) -> BoxFuture<Result<(), CacheError>>;
    fn exists(&self, key: &str) -> BoxFuture<Result<bool, CacheError>>;
}
```

```rust
pub enum CacheError { Miss, Backend(String) }
```

### `Cache` — key-value đơn giản

```rust
pub struct Cache { backend: Arc<dyn CacheBackend> }
impl Cache {
    pub fn new(backend: Arc<dyn CacheBackend>) -> Self
    pub async fn get(&self, key: &String) -> Result<String, CacheError>
    pub async fn set(&self, key: &String, value: &str, ttl: usize) -> Result<(), CacheError>
    pub async fn del(&self, key: &String) -> Result<(), CacheError>
}
```

Key format: `value[{key}]`.

### `Page` — paging có binary search

```rust
pub struct Page { backend: Arc<dyn CacheBackend> }
impl Page {
    pub fn new(backend: Arc<dyn CacheBackend>) -> Self
    pub async fn index(&self, key: &String, after: i64, limit: u64) -> Result<u64, CacheError>
    pub async fn get(&self, key: &String, after: i64, limit: u64) -> Result<String, CacheError>
    pub async fn set(&self, key: &String, value: &str, ttl: usize, after: i64, limit: u64) -> Result<(), CacheError>
}
```

Key format:
- `paging[{key}]:indices` → JSON `[ (after, limit, id), … ]`
- `paging[{key}]:data:{id}` → payload

Logic binary search (`lower_bound`/`upper_bound`) **giữ nguyên** từ `opsense-model/src/cache.rs` cũ, nhưng **đã copy vào `opsense-mod/src/cache.rs:9-48`** vì SDK không depend `mlib`.

---

## 8. `HostServices` / `HostOp` — đường `call()`

`opsense-mod/src/host.rs`: `HostServices::call(op, req_json) → res_json`.

`opsense/src/serve_host.rs`: impl cho gateway.

| Op | Mục đích | Trạng thái |
|---|---|---|
| `status` | topology + stations | ✅ |
| `components` | config đang chạy | ✅ |
| `attributes` | in-memory attributes | ✅ |
| `attr_set` / `attr_remove` | sửa attribute | ✅ |
| `query_station` | query station | ❌ `"not yet implemented"` |
| `orders` | trading orders | ❌ `"not yet implemented"` |
| `clear_station` | xoá 1 station | ❌ `"not yet implemented"` |
| `clear_all_stations` | xoá tất cả | ❌ `"not yet implemented"` |
| `config_read` | đọc pipeline config | ✅ |
| `audit_write` | ghi audit log | ✅ |
| `station_query` | raw station query | ❌ `"not yet implemented"` |

Đây là đường **duy nhất** module `.so` → host (status/attributes/audit...). An toàn nhờ `drive()` (`opsense-mod/src/ffi.rs:239`).

---

## 9. `Manifest` — mô tả module

`opsense-mod/src/manifest.rs`:

```rust
pub struct Manifest {
    pub id: String,           // unique: "repl", "xt-pnl"
    pub version: String,
    pub builtin: bool,
    pub prefix: String,       // "/api/repl"
    pub routes: Vec<RouteSpec>,
    pub tools: Vec<ToolSpec>,
}

pub struct RouteSpec { pub id: String, pub method: HttpMethod, pub path: String }
pub struct ToolSpec  { pub name: String, pub description: String, pub input_schema: Value }
```

**Luật khớp `route_id`** (`crates/opsense/src/api/modules.rs`):

1. Khớp cả `method` + `path` → trả `route_id`
2. Chỉ khớp `path` → trả `route_id`
3. Không khớp gì → trả `path` (để lỗi `RouteNotFound` mang tên route)

⚠️ Bẫy đã gây 404 toàn bộ `/api/repl/*`: `route_id` từng bị hardcode `"handler"`, `ReplApiModule` chỉ nhận `"graphql"`.

---

## 10. `ModuleError` → HTTP status

`opsense-mod/src/error.rs`:

| Error | Status | Code |
|---|---|---|
| `RouteNotFound` | 404 | `route_not_found` |
| `BadRequest` | 400 | `bad_request` |
| `Internal` | 500 | `internal_error` |
| `HostCallFailed` | 502 | `host_call_failed` |
| `AbiMismatch` | 500 | `abi_mismatch` |
| `NotLoaded` | 503 | `not_loaded` |
| `InitFailed` | 500 | `init_failed` |
| `Json` | 400 | `json_error` |

---

## 11. Cách viết module

### Builtin (link tĩnh)

```rust
// crates/opsense/src/api/my_mod.rs
use opsense_mod::{BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest, ModuleError, OpsenseModule, RouteSpec, TenantServices, Cache, CacheError};
use std::sync::Arc;

pub struct MyModule { /* state */ }

impl MyModule {
    pub fn new() -> Self { Self {} }
}

impl OpsenseModule for MyModule {
    fn manifest(&self) -> Manifest {
        Manifest {
            id: "my-mod".into(),
            version: "0.1.0".into(),
            builtin: true,
            prefix: "/api/my-mod".into(),
            routes: vec![RouteSpec { id: "hello".into(), method: HttpMethod::Get, path: "/hello".into() }],
            tools: vec![],
        }
    }

    fn route(&self, req: HttpRequest, services: &TenantServices) -> BoxFut<'static, Result<HttpResponse, ModuleError>> {
        Box::pin(async move {
            if req.route_id != "hello" { return Err(ModuleError::RouteNotFound(req.route_id)); }
            if let Some(cache) = services.cache.as_ref() {
                let c = Cache::new(cache.clone());
                let _ = c.get(&"test".into()).await;
            }
            Ok(HttpResponse { status: 200, headers: vec![], body: b"hello".to_vec() })
        })
    }

    fn tool(&self, name: String, args: Value, _services: &TenantServices) -> BoxFut<'static, Result<String, ModuleError>> {
        Box::pin(async move { Err(ModuleError::RouteNotFound(name)) })
    }
}
```

Đăng ký trong `serve.rs:136`:
```rust
registry.register_builtin(Arc::new(MyModule::new()));
```

### Dynamic (.so) — crate riêng

`Cargo.toml`:
```toml
[package]
name = "opsense-mod-hello"
crate-type = ["cdylib"]
[dependencies]
opsense-mod = { path = "../../../opsense-mod" }
serde_json = "1.0"
```

`src/lib.rs`:
```rust
use opsense_mod::{BoxFut, HttpMethod, HttpRequest, HttpResponse, Manifest, ModuleError, OpsenseModule, RouteSpec, TenantServices};
use std::sync::Arc;

pub struct HelloModule;

impl OpsenseModule for HelloModule {
    fn manifest(&self) -> Manifest { /* như trên */ }
    fn route(&self, req: HttpRequest, services: &TenantServices) -> BoxFut<...> { /* như trên */ }
    fn tool(&self, name: String, args: Value, services: &TenantServices) -> BoxFut<...> { /* như trên */ }
}

// FFI entry point — KHÔNG đổi tên crate
#[unsafe(no_mangle)]
pub extern "C" fn opsense_module_hello_init(host: *const opsense_mod::ffi::HostV1) -> *mut opsense_mod::ffi::ModuleV1 {
    use opsense_mod::ffi::ModuleExports;
    Box::into_raw(HelloModule.into_vtable())
}
```

`modules.toml`:
```toml
[[module]]
id = "hello"
builtin = false
enabled = true
path = "hello.so"
```

---

## 12. Build & cấu hình

| Việc | Cách |
|---|---|
| Build module | `cargo build --release -p opsense-mod-hello` (sinh `target/release/libhello.so`) |
| Đặt `.so` | Theo `path` trong `modules.toml`, relative to `OPSENSE_CONFIG_DIR` |
| Cấu hình | `modules.toml` — `id`, `builtin`, `enabled`, `path`, `config` (JSON tuỳ ý) |
| Bật feature | `opsense/dynamic-modules` ( kéo `libloading` ) |
| Symbol naming | `opsense_module_<id>_init` — `id` từ `modules.toml`, `-` → `_` |
| `builtin = true` | **Bỏ qua** bởi `load_dynamic` (`modules.rs:74`) — phải tự `register_builtin` |

> **Lưu ý:** Crate `hello-mod` là fixture test (`examples/hello-mod/`, `crate-type = ["cdylib"]`). Nó **không** nằm trong workspace `members` để tránh lỗi build musl (`cannot produce cdylib`). Build tay: `cargo build --manifest-path examples/hello-mod/Cargo.toml`.

---

## 13. Ràng buộc ABI — thay đổi gì phá vỡ `CacheBackend`

`CacheBackend` là trait object, vtable do host sinh. Module chỉ gọi qua vtable. Quy tắc:

| Thay đổi | Kết quả | Làm gì |
|---|---|---|
| Thêm method ở **cuối** | Module cũ gọi đúng slot cũ | bump `ABI_VERSION`, build module mới |
| Thêm/xoá ở **giữa** | Mọi slot sau bị lệch | bump `ABI_VERSION`, bắt buộc build module mới |
| Đổi/rename bất kỳ | Lệch | bump `ABI_VERSION` |
| Thêm method **có default body** ở cuối | An toàn | bump `ABI_VERSION` |
| Đổi `CacheError` enum | Layout enum thay đổi | bump `ABI_VERSION` |
| Đổi chữ ký `get`/`set_ex`/… | Lệch | bump `ABI_VERSION` |

**KHÔNG** được để `CacheBackend` hoặc `TenantServices` sau `#[cfg(feature = "redis")]` — module build không `redis` sẽ thấy struct/trait **khác** ⇒ layout lệch ⇒ UB im lặng. Chỉ `RedisBackend` mới gated.

---

## 13. Ràng buộc ABI cho FFI vtable

`HostV1`/`ModuleV1` thay đổi layout ⇒ bump `ABI_VERSION` (hiện = 3). Quy tắc giống §13.

---

## 14. Chỗ còn hở — nói thẳng

| Vấn đề | Nghiêm trọng | File | Mô tả |
|---|---|---|---|
| **(c) Op chưa implement** | 🟠 | `serve_host.rs:13` | `query_station`, `orders`, `clear_station`, `clear_all_stations`, `station_query` trả `"not yet implemented"`. |
| **(g) `drive()` fallback deadlock** | 🟠 | `ffi.rs:251` | `new_current_thread()` + `block_on(spawn())` → treo vĩnh viễn (nhánh fallback không chạy ở production). **Đã sửa** (`drive()` dùng `new_multi_thread().worker_threads(1)`); test `drive_works_inside_current_thread_runtime` làm bằng chứng. |
| **(h) nested `drive()` chưa từng test** | 🟠 | `ffi.rs:246` | Chuỗi `route()` → `host.call()` → `drive()` lồng nhau chưa từng chạy; `hello-mod` route `host-status` là bằng chứng đã test. |
| **(i) `hello-mod` phá build release musl** | 🟠 | `Cargo.toml:18` | cdylib trong workspace + cargo-dist musl ⇒ `cannot produce cdylib`. Đã tách workspace + `[workspace]` riêng. |

> **Chưa có module `.so` mẫu nào** trong repo ngoài `hello-mod` (fixture test). `repl` là builtin. Nên (g) chưa từng lộ ra thực tế.

---

## 15. Link chéo

- [`architecture.md`](./architecture.md) §2 — crate map có `opsense-mod`
- [`GUIDE.md`](./GUIDE.md) — dùng `opsense status/components/query` tương tác module
- `opsense-mod/src/ffi.rs` — chi tiết vtable, `ModuleHandle`, `ModuleExports::into_vtable`, `drive()`
- `opsense/src/api/modules.rs` — `module_route_handler`, `load_dynamic`, `DynamicModuleWrapper`
- `opsense/src/serve.rs` — wiring registry + router
- `opsense/src/serve_host.rs` — `HostServices` impl cho đường `call()`