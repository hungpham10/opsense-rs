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
| **Dynamic (.so)** | `dlopen` lúc boot, `load_dynamic` + `[[modules]]` | `hello` | `modules.rs` |

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

**Cast cho DB row**: engine `CastType` / `cast_value` nằm ở `opsense-mlib/src/cast.rs`. Module muốn đọc row đúng kiểu (xem §7) thì **tự depend `opsense-mlib`** — SDK `opsense-ffi` cố ý không kéo mlib vào.

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

    // ── SSE (ABI 4) ──
    pub stream_open:  Option<unsafe extern "C" fn(ModuleHandle, *const TenantServices, *const c_char, *const c_char) -> *mut c_char>,
    pub stream_push:  Option<unsafe extern "C" fn(ModuleHandle, *const c_char, *const c_char) -> *mut c_char>,
    pub stream_close: Option<unsafe extern "C" fn(ModuleHandle, *const c_char)>,
}
```

- **Mọi chuỗi là JSON UTF-8, NUL-terminated**.
- Host cấp bộ nhớ, host free (`free`). Module **không** free chuỗi nhận được.
- `ModuleHandle` = `#[repr(transparent)]` trên `*mut c_void` → `Send + Sync` an toàn.
- `ABI_VERSION` = **4** (`crates/opsense-ffi/src/lib.rs:33`). Tăng khi `HostV1`/`ModuleV1` đổi layout.
- 3 entry `stream_*` là `Option` → module build với ABI 3 (thiếu entry) **vẫn `dlopen` được**.
  Host check `is_some()` rồi trả lỗi rõ ràng, không gọi vào slot rỗng.

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

    // ── SSE — cả 3 đều có default impl ──
    fn stream(&self, req: HttpRequest, services: &TenantServices)
        -> BoxFut<'static, Result<StreamHandle, ModuleError>>;

    fn stream_push(&self, stream_id: String, payload: String)
        -> BoxFut<'static, Result<String, ModuleError>>;

    fn stream_close(&self, stream_id: String);
}
```

- `BoxFut<'a, T>` = `Pin<Box<dyn Future<Output = T> + Send + 'a>>` (object-safe).
- `HttpRequest`/`HttpResponse` chỉ chứa `route_id`, `method`, `path`, `query`, `headers`, `body` — không dùng kiểu axum.
- 3 method `stream*` **có default impl** → module không override vẫn compile, vẫn chạy REST bình thường.
  `stream` mặc định trả lỗi, `stream_push` mặc định trả `""` (bỏ qua), `stream_close` không làm gì.

---

## 6. `TenantServices` — tenant per-request (an ninh)

`opsense-mod/src/cache.rs`:

```rust
pub struct TenantServices {
    pub tenant_id: i64,
    pub cache: Option<Arc<dyn CacheBackend>>,
    pub db: Option<Arc<dyn DbBackend>>,
}
```

Host dựng **mỗi request** trong `module_route_handler` (`crates/opsense/src/api/modules.rs`):

```rust
let tenant_id = header_tenant_id(req.headers());

let cache = state.connector.cache(tenant_id)
    .map(opsense_mlib::cache::RedisBackend::new);
let db = state.connector.database(tenant_id).map(|pool| {
    crate::api::module_db::AnyDbBackend::arc(
        pool.clone(),
        state.connector.database_kind(tenant_id),
    )
});
let services = TenantServices { tenant_id, cache, db };
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

### `DbBackend` / `Db` — truy vấn DB

`opsense-ffi/src/db.rs`:

```rust
pub trait DbBackend: Send + Sync + 'static {
    fn query(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<Vec<Value>, DbError>>;
    fn execute(&self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<u64, DbError>>;
}

pub enum DbError { Backend(String), Query(String) }
```

`Db` là wrapper tiện dụng, thêm giải mã row → struct:

```rust
pub struct Db { backend: Arc<dyn DbBackend> }
impl Db {
    pub fn new(backend: Arc<dyn DbBackend>) -> Self
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, DbError>
    pub async fn query_as<T: DeserializeOwned>(&self, sql, params) -> Result<Vec<T>, DbError>
    pub async fn query_one<T: DeserializeOwned>(&self, sql, params) -> Result<Option<T>, DbError>
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError>
}
```

Contract **không** kéo `sqlx` vào SDK — module gửi SQL text + params JSON, host
chạy prepared statement trên pool của tenant.

- **Placeholder**: `$1..$n` (Postgres-native). Host tự đổi sang `?` cho MySQL
  (`rewrite_placeholders_mysql`, có nhân bản param nếu dùng lại `$1`).
- **Param JSON → SQL**: `null` → NULL, bool → BOOLEAN, số nguyên → BIGINT, số
  thực → DOUBLE, string → TEXT, mảng byte `0..=255` → BLOB, object → TEXT (JSON
  đã serialize).
- **Row → JSON**: object với key là tên cột; `BLOB` encode **base64** (JSON
  không có kiểu nhị phân).
- **`NULL` param mang kiểu TEXT** (`Option::<String>::None`); NULL cho cột số
  nên viết `CAST($n AS BIGINT)`.

> **Giới hạn `sqlx::Any`** (host dùng `AnyPool`): chỉ 9 kiểu được map —
> `BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, TEXT, BLOB, NULL`. Cột
> `TIMESTAMPTZ`/`NUMERIC`/`JSON`/`UUID` (Postgres) hay `BOOL` (SQLite) làm
> **hỏng cả row** ngay tại `AnyRow::map_from`. Module tự viết SQL nên hãy chủ
> động `CAST(col AS TEXT)` khi đọc, và tạo bảng bằng các kiểu nằm trong danh
> sách trên.

Host impl: `crates/opsense/src/api/module_db.rs` (`AnyDbBackend`).

### Cast kiểu khi đọc row — tái sử dụng `cast.rs` của mlib

Row trả về là JSON thô (`Vec<Value>`); host **không đoán "ý đồ" kiểu** của
module. Muốn đọc đúng kiểu (int, bool, array of int…) thì module **tự depend
`opsense-mlib`** và dùng engine cast sẵn có ở
`crates/opsense-mlib/src/cast.rs`:

```rust
use opsense_mlib::cast::{cast_value, CastType};

let rows = db
    .query("SELECT id, active, tags FROM trades WHERE id = $1", &[json!(1)])
    .await?;
let row = &rows[0];

let id = cast_value(row["id"].clone(), &CastType::I64); // Some(json!(1))
let active = cast_value(row["active"].clone(), &CastType::Bool); // 0/1 · "t"/"f" · "true" · "1" → bool
let qty = cast_value(row["qty"].clone(), &CastType::F64); // "1.5" → 1.5
```

`CastType`: `U64, U32, I64, I32, F64, F32, String, Bool`. `cast_value` trả
`Option<Value>` — `None` = không cast được; module tự quyết bỏ item hay lỗi cả
batch. **Đừng tự viết coercion trong module** — engine đã lo hết
string ⇄ number ⇄ bool.

Vì sao cần cast thay vì `row["x"].as_i64()`:

| Cần | SQL do module tự viết | Đọc bằng |
|---|---|---|
| bool | SQLite lưu `0/1`; PG trả JSON bool | `CastType::Bool` (nhận `0/1`, `"true"/"on"/"1"`, `true/false`) |
| array of int | `CAST(tags AS TEXT)` → PG `"{1,2,3}"`, SQLite `"1,2,3"` | tách chuỗi rồi `CastType::I64` từng phần tử |
| số từ TEXT | chính nó | `CastType::F64` / `I64` / `U32`… |
| BLOB | giữ nguyên | host đã encode **base64** — decode bên module |

Tách array text (PG `{1,2,3}` hoặc CSV `1,2,3`) rồi cast từng phần tử:

```rust
let tags: Vec<i64> = row["tags"]
    .as_str()
    .unwrap_or("")
    .trim_matches(|c| matches!(c, '{' | '}' | '[' | ']'))
    .split(',')
    .filter_map(|s| cast_value(Value::String(s.trim().to_string()), &CastType::I64))
    .filter_map(|v| v.as_i64())
    .collect();
```

**Lỗi điển hình nếu bỏ cast tay bằng `as_i64()`:** SQLite bool là số `0/1`,
PG `int[]` thành string `"1,2,3"`, `REAL` (f32) khác `DOUBLE` (f64).

> **Chốt lưu trữ:** đừng tạo cột `JSON`/`NUMERIC`/`TIMESTAMPTZ`/`UUID` (PG) hay
> `BOOL` (SQLite) — không map được qua `sqlx::Any`. Tạo bảng bằng
> `INTEGER/BIGINT/DOUBLE/TEXT/BLOB` rồi cast lúc đọc như trên.

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

pub struct RouteSpec {
    pub id: String,
    pub method: HttpMethod,
    pub path: String,
    pub transport: Transport,          // Rest (mặc định) | Sse
    pub source_node: Option<String>,  // node `output` làm nguồn, chỉ dùng khi Sse
}
pub struct ToolSpec { pub name: String, pub description: String, pub input_schema: Value }

pub enum Transport { Rest, Sse }   // serialize lowercase: "rest" / "sse"
```

**`transport` + `source_node` có `#[serde(default)]`** — manifest viết tay (không khai
2 field này) vẫn parse được và ra `Rest`. Nếu mất default thì **mọi** module
build trước SSE đều không load được.

**Luật `route_id`** (`crates/opsense/src/api/modules.rs`):

`route_id` là **`RouteSpec.id` khai báo trong manifest**, host gán lúc dựng router
(`into_router` capture `route.id` vào closure) — **không** phải suy ra từ URI.

Nhờ vậy:
- Route có path param (`/ohlc/candles/{broker}/{symbol}`) vẫn nhận đúng id;
- Request lệch method bị router chặn **405** trước khi tới handler (axum lọc method ở tầng `MethodRouter`).
- Module dispatch bằng `req.route_id` — ví dụ `ReplApiModule` chỉ nhận `"graphql"`.

⚠️ Bẫy cũ: `route_id` từng bị hardcode `"handler"`, `ReplApiModule` chỉ nhận `"graphql"` ⇒ 404 toàn bộ `/api/repl/*`.

**Validation lúc đăng ký** (`ApiModuleRegistry::validate_routes`):

- Trùng `(method, path)` exact → `InitFailed` (ngăn panic axum lúc boot).
- Trùng **shape** (`/x/{a}` + `/x/{b}`) → `InitFailed` (ngăn panic `matchit` insert trùng shape lúc boot).
- SSE + method ≠ `GET` → `InitFailed`.

> Shape = path thay mọi `{param}` thành `{}`. Chỉ dùng để so trùng, **không** dùng làm path mount
> (phải giữ nguyên tên param để `matchit` parse).

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

`opsense.conf.toml` — section `[[modules]]`:
```toml
[[modules]]
id = "hello"
builtin = false
enabled = true
path = "hello.so"
```

### Path params

Axum 0.8 dùng cú pháp `{param}` trong path (ví dụ `/ohlc/candles/{broker}/{symbol}`).
Router match thành công và truyền `route_id` là id khai báo trong manifest — **không phải** path đã điền.

Module nhận `HttpRequest.path` là full path (`/ohlc/candles/binance/BTCUSDT`) và tự tách
param. **Không có** `params: HashMap` trong `HttpRequest` — thêm field = đổi layout struct FFI
⇒ ABI bump. Khi nào cần param typed thì làm.

> **Lưu ý:** Crate `hello-mod` là fixture test (`examples/hello-mod/`, `crate-type = ["cdylib"]`). Nó **không** nằm trong workspace `members` để tránh lỗi build musl (`cannot produce cdylib`). Build tay: `cargo build --manifest-path examples/hello-mod/Cargo.toml`.

---

## 12. Build & cấu hình

Route SSE cần **hai phần**: một node `output` trong pipeline làm nguồn, và
module khai route trỏ tới node đó.

**1. Pipeline — node nguồn:**

```toml
[[pipeline.components]]
type = "clock"
id = "clock"
interval_secs = 1

# Chỉ `type = "output"` mới có broadcast channel.
[[pipeline.components]]
type = "output"
id = "market-out"
inputs = ["clock"]
```

**2. Manifest — khai route:**

```rust
RouteSpec {
    id: "ticks".into(),
    method: HttpMethod::Get,
    path: "/ticks".into(),
    transport: Transport::Sse,
    source_node: Some("market-out".into()),
}
```

**3. Module — 3 method:**

```rust
fn stream(&self, _req, _svc)
    -> BoxFut<'static, Result<StreamHandle, ModuleError>>
{
    Box::pin(async { Ok(StreamHandle { stream_id: "t1".into() }) })
}

/// Trả `""` = bỏ qua message này. Trả JSON = phát xuống client.
/// **Đây là chỗ lọc/xào trộn dữ liệu** — host không đổi khi bạn thêm logic.
fn stream_push(&self, _stream_id: String, payload: String)
    -> BoxFut<'static, Result<String, ModuleError>>
{
    Box::pin(async move {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&payload) else {
            return Ok(String::new());
        };
        if v.get("src").and_then(serde_json::Value::as_str) != Some("clock") {
            return Ok(String::new());      // lọc rớt
        }
        Ok(serde_json::json!({ "seen": v }).to_string())
    })
}

fn stream_close(&self, _stream_id: String) {
    tracing::info!("stream đóng");
}
```

**Vì sao vòng `recv()` nằm ở host chứ không ở module?**
`broadcast::Receiver` không FFI-safe — nó là handle của một task đang chạy.
Host chạy `rx.recv().await` rồi đẩy từng message qua `stream_push`. Nhờ vậy
module không cần thread riêng, không cần runtime riêng, và logic lọc nằm hoàn
toàn trong plugin.

**Client chậm hơn pipeline:** buffer `broadcast` là 1024 message. Client chậm sẽ
nhận event `error` báo `Data feed out of sync. Skipped N messages.` thay vì im
lặng mất dữ liệu.

**Qua nginx:** `conf/nginx/vhost/04-api.conf` (file image dùng) **chưa có** `proxy_buffering off`. Cần thêm dòng đó vào location `/api/` (kèm `proxy_http_version 1.1` đã có). File `packaging/nginx/opsense.conf` **không image nào dùng** — Earthfile:137 chỉ COPY `conf/nginx/vhost`. Kiểm trước khi deploy.

**Chưa làm:** WebSocket. `broadcast` là một chiều; WS hai chiều cần thêm
`socket_frame` vào vtable + registry `conn_id -> sender`.

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

`HostV1`/`ModuleV1` thay đổi layout ⇒ bump `ABI_VERSION` (hiện = **5**). Quy tắc giống §13.

Thêm 3 entry `stream_*` dạng `Option` ở **cuối** `ModuleV1` (ABI 4) là an toàn: module build
với ABI 3 không có entry này, host check `is_some()` và trả lỗi rõ ràng thay vì gọi
slot rỗng — không segfault. Đây là mẫu để thêm transport mới sau này.

ABI 5 = thêm field `db` vào **cuối** `TenantServices`. Module build với ABI 4 vẫn
`dlopen` được nhưng **phải** bump vì module nhận `*const TenantServices` từ host —
đọc struct cũ nghĩa là đọc sai offset (`cache` ↔ `db`).

---

## 14. Chỗ còn hở — nói thẳng

| Vấn đề | Nghiêm trọng | File | Mô tả |
|---|---|---|---|
| **(c) Op chưa implement** | 🟠 | `serve_host.rs:13` | `query_station`, `orders`, `clear_station`, `clear_all_stations`, `station_query` trả `"not yet implemented"`. |
| **(g) `drive()` fallback deadlock** | 🟠 | `ffi.rs:251` | `new_current_thread()` + `block_on(spawn())` → treo vĩnh viễn (nhánh fallback không chạy ở production). **Đã sửa** (`drive()` dùng `new_multi_thread().worker_threads(1)`); test `drive_works_inside_current_thread_runtime` làm bằng chứng. |
| **(h) nested `drive()` chưa từng test** | 🟠 | `ffi.rs:246` | Chuỗi `route()` → `host.call()` → `drive()` lồng nhau chưa từng chạy. `hello-mod` có route `host-status` **đã dựng sẵn** nhưng **chưa có test tự động** `dlopen` nó — chỉ mới compile được. Cần test tay: build `.so`, đặt vào thư mục chứa `opsense.conf.toml`, khai `[[modules]]` trong đó, chạy `opsense serve --features dynamic-modules`. |
| **(i) `hello-mod` phá build release musl** | 🟠 | `Cargo.toml:18` | cdylib trong workspace + cargo-dist musl ⇒ `cannot produce cdylib`. Đã tách workspace + `[workspace]` riêng. |

> **Chưa có module `.so` mẫu nào** trong repo. `repl` là builtin. `hello-mod` là **fixture thủ công** — có route `host-status` đã dựng sẵn, nhưng **không có test tự động** `dlopen` nó; chỉ mới compile được. Cần test tay: build `.so`, đặt vào thư mục chứa `opsense.conf.toml`, khai `[[modules]]` trong đó, chạy `opsense serve --features dynamic-modules`. Nên (g) chưa từng lộ ra thực tế.

---

## 15. Tham khảo & Fixture

| Crate / Path | Mô tả |
|---|---|
| `examples/hello-mod/` | **Fixture thủ công** — module mẫu để author tham khảo. **Không** có test tự động, **không** chạy trong CI. Build tay: `cargo build --manifest-path examples/hello-mod/Cargo.toml`. Dùng để test tay streaming/DB/nested-drive. |

---

## 16. Link chéo

- [`architecture.md`](./architecture.md) §2 — crate map có `opsense-mod`
- [`GUIDE.md`](./GUIDE.md) — dùng `opsense status/components/query` tương tác module
- `opsense-mod/src/ffi.rs` — chi tiết vtable, `ModuleHandle`, `ModuleExports::into_vtable`, `drive()`
- `opsense/src/api/modules.rs` — `module_route_handler`, `load_dynamic`, `DynamicModuleWrapper`
- `opsense/src/serve.rs` — wiring registry + router
- `opsense/src/serve_host.rs` — `HostServices` impl cho đường `call()`