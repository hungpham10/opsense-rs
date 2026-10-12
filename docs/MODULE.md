# Opsense — Module Framework

> Mô tả **đúng những gì code làm** (đối chiếu bằng `cargo check`, `grep`).
> Kiến trúc tổng thể: [`architecture.md`](./architecture.md).
> Hướng dẫn dùng: [`GUIDE.md`](./GUIDE.md).

---

## 1. Framework là gì — hai đường chạy module

Opsense gọi là **module** mọi thứ triển khai `OpsenseModule` (`opsense-mod/src/lib.rs:36`). Có hai cách nạp:

| Đường | Cách nạp | Ví dụ | File |
|---|---|---|---|
| **Builtin** | Link tĩnh vào binary, `ApiModuleRegistry::register_builtin` | `repl` | `serve.rs:136-139` |
| **Dynamic (.so)** | `dlopen` lúc boot, `load_dynamic` + `modules.toml` | (chưa có module mẫu) | `modules.rs:62-117` |

Route đã là module (prefix `/api/repl`, `builtin=true`): REPL GraphQL (`repl/adapter.rs:35`).
Route **chưa phải** module: `/health`, `/api/admin/*`, `/api/oauth/*` — vẫn mount trực tiếp (`serve.rs:159-162`).

Bật dynamic module: compile với `--features opsense/dynamic-modules` (CI: `.github/workflows/ci.yml:58`).

---

## 2. Crate map

| Crate | Vai trò | Dependency |
|---|---|---|
| `opsense-mod` | **SDK nhẹ**: `OpsenseModule`, `CacheBackend`, `HostServices`, FFI vtable, `TenantServices` | `serde`, `serde_json`, `thiserror`, `tokio`, `axum` |
| `opsense-mlib` | `RedisBackend` impl `CacheBackend` (feature `redis`), vector runtime, storage | `opsense-mod` (optional) |
| `opsense` | Binary: wiring, resolve tenant, `dlopen`, router | cả hai trên |

**Luồng depend**: `opsense` → `opsense-mod` & `opsense-mlib`; **không** crate nào khác depend `opsense-mod` (SDK độc lập).

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
    pub call: unsafe extern "C" fn(*const c_char, *const c_char) -> *mut c_char,
    pub log:  unsafe extern "C" fn(u32, *const c_char, *const c_char),
    pub free: unsafe extern "C" fn(*mut c_char),
}
```

```rust
#[repr(C)]
pub struct ModuleV1 {
    pub abi_version: u32,
    pub state: ModuleHandle,      // opaque, do module cấp
    pub manifest: unsafe extern "C" fn(ModuleHandle) -> *mut c_char,
    pub handle:   unsafe extern "C" fn(ModuleHandle, *const TenantServices, *const c_char, *const c_char) -> *mut c_char,
    pub free:     unsafe extern "C" fn(*mut c_char),
}
```

- **Mọi chuỗi là JSON UTF-8, NUL-terminated**.
- Host cấp bộ nhớ, host free (`free`). Module **không** free chuỗi nhận được.
- `ModuleHandle` = `#[repr(transparent)]` trên `*mut c_void` → `Send + Sync` an toàn.
- `ABI_VERSION` = 2 (`lib.rs:31`). Tăng khi `HostV1`/`ModuleV1` đổi layout.

---

## 5. `OpsenseModule` — trait cốt lõi

`opsense-mod/src/lib.rs:36`:

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

Host dựng **mỗi request** trong `module_route_handler` (`modules.rs:195-202`):

```rust
let tenant_id = req.headers().get("x-tenant-id")...parse().unwrap_or(0);
let cache_backend = state.connector.cache(tenant_id).map(RedisBackend::new);
let services = TenantServices { tenant_id, cache: cache_backend };
module.route(http_req, &services).await
```

**Module không bao giờ tự chọn tenant** — header do host parse, backend đã bind đúng shard. Thiếu header ⇒ `tenant_id = 0` (im lặng, **không 400** — xem §14(f)).

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
    #[cfg(feature = "redis")] pub fn redis(conn: MultiplexedConnection) -> Self
    pub async fn get(&self, key: &String) -> Result<String, CacheError>
    pub async fn set(&self, key: &String, value: &String, ttl: usize) -> Result<(), CacheError>
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
    pub async fn set(&self, key: &String, value: &String, ttl: usize, after: i64, limit: u64) -> Result<(), CacheError>
}
```

Key format:
- `paging[{key}]:indices` → JSON `[ (after, limit, id), … ]`
- `paging[{key}]:data:{id}` → payload

Logic binary search (`lower_bound`/`upper_bound` từ `mlib`) **giữ nguyên** từ `opsense-model/src/cache.rs` cũ.

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

Module `.so` **không dùng** đường này (dùng `TenantServices`). Điểm này là fallback cho module cũ/không dùng FFI backend.

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

**Luật khớp `route_id`** (`modules.rs:166-173`):
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
            // Dùng cache từ services
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
#[no_mangle]
pub extern "C" fn opsense_module_hello_init(host: *const opsense_mod::ffi::HostV1) -> *mut opsense_mod::ffi::ModuleV1 {
    let module = HelloModule;
    module.into_vtable() // impl ModuleExports
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

## 14. Chỗ còn hở — nói thẳng

| Vấn đề | Nghiêm trọng | File | Mô tả |
|---|---|---|---|
| **(a) block_on panic** | 🔴 | `ffi.rs:286,311` `modules.rs:351` | `Handle::try_current().block_on(...)` — `handle()` chạy *trong* async handler của axum ⇒ panic "runtime within a runtime". Module `.so` gọi `route` async là **chết ngay**. Cần `drive()` dùng `block_in_place` / `spawn_blocking`. |
| **(b) `HOST_SERVICES` global** | 🟠 | `modules.rs:308` | `OnceLock<Arc<dyn HostServices>>`. Nhiều `AppState` (test) → mọi `.so` trỏ state đầu tiên. Chưa sửa. |
| **(c) Op chưa implement** | 🟠 | `serve_host.rs:13` | `query_station`, `orders`, `clear_station`, `clear_all_stations`, `station_query` trả `"not yet implemented"`. |
| **(d) `module_id` ≠ `manifest.id`** | 🟡 | `modules.rs:84,111` | Symbol lấy theo `mcfg.id` (config), registry key theo `manifest().id` (code). Lệch ⇒ module đăng ký sai tên. |
| **(e) `builtin=true` bị bỏ qua** | 🟡 | `modules.rs:74` | `load_dynamic` skip `builtin` — phải tự `register_builtin`. |
| **(f) Thiếu `X-Tenant-Id` → 0** | 🟡 | `modules.rs:195` | `tenant_id = 0` im lặng. Entity dùng `dbt(0)` cho host lookup — rủi ro ghi nhầm tenant. |

> **Chưa có module `.so` mẫu nào** trong repo. `repl` là builtin. Nên (a) chưa từng lộ ra thực tế.

---

## Link chéo

- [`architecture.md`](./architecture.md) §2 — crate map có `opsense-mod`
- [`GUIDE.md`](./GUIDE.md) — dùng `opsense status/components/query` tương tác module
- `opsense-mod/src/ffi.rs` — chi tiết vtable, `ModuleHandle`, `ModuleExports::into_vtable`
- `opsense/src/api/modules.rs` — `module_route_handler`, `load_dynamic`, `DynamicModuleWrapper`
- `opsense/src/serve.rs` — wiring registry + router
- `opsense/src/serve_host.rs` — `HostServices` impl cho đường `call()`