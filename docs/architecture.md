# Opsense — Kiến trúc

> Tài liệu này mô tả **kiến trúc runtime** của `opsense-rs`. Hướng dẫn sử
> dụng: [`GUIDE.md`](./GUIDE.md); trạng thái thực thi:
> [`PLAN.MD`](./PLAN.MD); audit checklist: [`CHECKLIST.MD`](./CHECKLIST.MD).

## 1. Bức tranh tổng thể

Opsense = **2 tầng độc lập**, cùng share một process `opsense serve` qua
một endpoint GraphQL duy nhất (`POST /graphql`).

```
                  ┌────────────── opsense serve ──────────────┐
                  │  axum + async-graphql                     │
   curl / REPL  ──▶│  /graphql       /health                  │
   MCP client   ──▶│                                        │
                  │  Tầng 1: vector Runtime + stations       │
                  │  Tầng 2: RunnerClient → runner gRPC      │
                  └───────┬─────────────────┬────────────────┘
                          │                 │
            ┌─────────────▼──────┐  ┌───────▼──────────────┐
            │ TẦNG 1 — TELEMETRY │  │ TẦNG 2 — ANALYSIS   │
            │ vector Runtime     │  │ RunnerClient (gRPC)  │
            │ node-owned stations│  │ Ed25519 sig mỗi RPC  │
            │ (pipeline, Rhai)   │  │  → opsense-runner    │
            └────────────────────┘  └───────┬──────────────┘
                                           │ framed stdio IPC
                                           ▼
                                 ┌──────────────────────┐
                                 │ opsense-kernel-*      │
                                 │ (Python / Julia /     │
                                 │  echo)                │
                                 └──────────────────────┘
```

**REPL kernel mode** (Phase 2 revised 2026-09-03): `opsense repl --runner
<endpoint>` có thể nói thẳng với runner qua gRPC, bỏ qua host
(`opsense serve`):

```
   opsense repl ──gRPC + Ed25519 sig──▶ opsense-runner
   (--runner <ep>)   (KernelRepl +      (Phase 4)
                      RunnerClient)
```

Path này dùng cho low-latency interactive use; curl/GraphQL path dùng cho
shared telemetry + analysis trên cùng một host.

Hai tầng **không phụ thuộc lẫn nhau**: pipeline telemetry chạy được mà không
có session; analysis session chạy được mà không cần pipeline.

---

## 2. Tầng 1 — Telemetry pipeline

Engine dataflow kiểu vector (`opsense_libs::vector::Runtime`) điều phối các
component nối thành DAG. Config hoàn toàn bằng TOML
(`[[pipeline.components]]`), sửa nóng qua GraphQL `Mutation.reload` (runtime
tự diff, validate liên kết + chu trình DFS trước khi apply).

### 2.1 Crate bố trí

| Crate | Trách nhiệm |
|---|---|
| `opsense-libs` | LRU sharded, jq, Aho-Corasick, Radix + KMP, vector runtime subsystem |
| `opsense-core` | Domain: `Config`, `Context`, `Station` (3 loại) |
| `opsense-components` | Component chuẩn: clock, ingest, processor, http, collector, station |
| `opsense-rhai` | Rhai sandbox + `RhaiTransform` component + `register_ts_ops` |
| `opsense` (bin) | Serve endpoint GraphQL/HTTP, init, runner stub, repl stub, mcp stub |

### 2.2 Component & quy ước tên

Mọi `type` kết thúc bằng hậu tố vai trò (macro `#[source] / #[transform] /
#[sink]` trong `opsense-macros` sinh từ tên struct, không lệch được):

| Hậu tố | Type hiện có | Vai trò |
|---|---|---|
| `_source` | `clock_source`, `ingest_source` | Đưa tín hiệu/dữ liệu vào pipeline |
| `_transform` | `http_source`¹, `processor_transform`, `rhai_transform`, `pattern_station_transform`, `category_station_transform`, `timeseries_station_transform` | Xử lý observations |
| `_sink` | `collector_sink`, `timeseries_station_sink` | Đích cuối |

¹ `http_source` đứng sau `clock_source` trong graph, fetch dữ liệu từ URL
bên ngoài. Đuôi `_source` ở đây nói về **vai trò dữ liệu** (nguồn fetch
từ bên ngoài), không phải vị trí graph.

### 2.3 Mỗi node sở hữu station riêng

Mỗi node publish dữ liệu vào **station riêng** đăng ký theo `id` trong
`Context::stations` (xem `opsense-core/src/context.rs:13`):

```rust
pub type Stations = Arc<RwLock<HashMap<String, Station>>>;
```

`Station` enum (`opsense-core/src/station.rs:210`) có 3 biến thể:

- `Timeseries(TimeseriesStation)` — `LruCache<i64, Block, 32>` chia block theo
  thời gian. **1 station = 1 time series duy nhất**.
- `Category(CategoryStation)` — `Search` (Radix + KMP) cho substring + index key/value.
- `Pattern(PatternStation)` — Aho-Corasick multi-pattern matcher kèm bộ đếm hit/miss.

Consumer (Rhai, GraphQL resolvers) đọc qua `context.station::<T>(id)`.
`T::try_from(&Station)` trả `Error` nếu sai kind — resolver wrap thành
GraphQL error.

### 2.4 Tín hiệu

Các node trao đổi `Message` qua kênh mpsc bounded 1024 (backpressure).
Signal mang timestamp: `tick(ts) → data_ready(ts) → processed(ts)` (xem
`opsense-components::signal`).

---

## 3. Tầng 2 — Analysis session

> **Trạng thái** (2026-09-03): `opsense-runner` đã đầy đủ (auth, 3 backends,
> session registry, sweeper, 6 RPCs). `RunnerClient` (gRPC client) đã có ở
> `crates/opsense/src/client/grpc.rs`. REPL có thể nói thẳng runner qua
> `opsense repl --runner <endpoint>`. Còn lại: wrap RunnerClient qua 6
> GraphQL resolvers (Phase 2 bridge).

### 3.1 Hai layer tách biệt

```
┌────────────────────────────────────────────────────────────────────┐
│  opsense serve (Tầng 1 host process)                               │
│  - GraphQL endpoint                                                │
│  - Phase 2 sẽ bật: SessionManager → Backend (gRPC → runner)        │
└────────────────────┬───────────────────────────────────────────────┘
                     │ gRPC + Ed25519 sig (Phase 4)
                     ▼
┌────────────────────────────────────────────────────────────────────┐
│  opsense-runner (process độc lập)                                  │
│  - listen tonic, KernelRunner service                              │
│  - verify Ed25519 signature trên mỗi request                       │
│  - SessionRegistry + implicit keepalive sweeper                    │
│  - delegate xuống KernelBackend                                    │
└────────────────────┬───────────────────────────────────────────────┘
                     │ framed stdio IPC
                     ▼
┌────────────────────────────────────────────────────────────────────┐
│  opsense-kernel-{python,julia,echo}                                │
│  - process per session, exit khi session close                     │
│  - framed IPC: [tag u8][len u32 BE][payload protobuf]             │
└────────────────────────────────────────────────────────────────────┘
```

Lý do tách runner thành process riêng: kernel có thể crash → không kéo theo
host chết; runner có thể scale/restart độc lập với host; auth Ed25519 cần
verify tại biên, host không cần nhúng private key của mọi session.

### 3.2 Ed25519 auth scheme

Mỗi session có 1 Ed25519 keypair. **session_id = base64(public_key)**, và
đây chính là định danh host dùng trong mọi RPC.

Mỗi request mang 4 metadata header:

| Header | Nội dung |
|---|---|
| `x-session-id` | base64(public key) (= session_id) |
| `x-timestamp` | unix seconds, runner check ±30s |
| `x-nonce` | u64, chống replay |
| `x-signature` | base64(Ed25519.sign(`{ts}:{nonce}:{method}`)) |

`method` ∈ `{Start, Execute, Close, Interrupt, Ping}`. Runner:

1. `decode(session_id)` → 32 bytes → `VerifyingKey::from_bytes`.
2. Check `|now - timestamp| ≤ 30s` (rejects stale/replay windows).
3. `verify(format!("{ts}:{nonce}:{method}"), signature)`.
4. Nếu OK → forward xuống `KernelBackend`.

`Auth` trait trong `opsense_runner::auth` cho phép swap impl: `LocalAuth`
(in-process, dùng cho test) hoặc future HTTP server-side lookup
(`resolve_private_key` để mint short-lived signed tokens).

### 3.3 Implicit keepalive

Mỗi RPC `Start/Execute/Interrupt/Close/Ping` tự gọi `registry.touch(session_id)` —
host không cần explicit `:session ping`. Background sweeper (interval +
idle timeout từ `RunnerConfig`) close mọi session quá hạn.

### 3.4 Crate layout

```
crates/opsense-runner/
├── Cargo.toml          # tonic, prost, ed25519-dalek, base64, async-trait
├── src/
│   ├── lib.rs           # re-exports + run(bind, cfg, auth)
│   ├── auth.rs          # Auth trait + LocalAuth + AuthContext
│   │                    # AuthContext::TryFrom<&tonic::MetadataMap>
│   ├── backend/
│   │   ├── mod.rs       # KernelBackend trait, HealthInfo, EchoBackend, LocalBackend
│   │   ├── ipc.rs       # IpcKernelBackend: spawn kernel, drive framed stdio
│   │   └── arrow.rs     # Arrow IPC segment ↔ RecordBatch helpers
│   ├── config.rs        # RunnerConfig (kernel command, sweep interval, idle timeout)
│   ├── server.rs        # RunnerService (tonic), 6 RPCs, serve(bind, cfg, auth)
│   └── session.rs       # SessionRegistry + implicit-keepalive + idle sweeper
```

### 3.5 RPC surface (`KernelRunner` service)

| RPC | Stream | Mục đích |
|---|---|---|
| `Start(SessionParams) → SessionHandle` | unary | spawn kernel, mint session_id |
| `Execute(CodeRequest) → stream<ExecEvent>` | server-stream | chạy code, emit events |
| `Interrupt(InterruptRequest) → Ack` | unary | huỷ execution đang chạy |
| `Close(CloseRequest) → Ack` | unary | đóng session, kill kernel |
| `Ping(PingRequest) → Pong` | unary | touch session nếu sig OK |
| `Health(HealthRequest) → HealthStatus` | unary | thông tin backend |

### 3.6 Run entry

```rust
pub async fn run(
    bind: SocketAddr,
    cfg: RunnerConfig,
    auth: Option<SharedAuth>,
) -> Result<()>;
```

`auth = None` → open (no signature check, dev/test). `auth = Some(arc)` → mọi
RPC verify Ed25519. Cùng entry phụvụ cả production deployment và test.

### 3.7 Host-side RunnerClient (Phase 2)

Wrapper gRPC nằm ở `crates/opsense/src/client/grpc.rs` — đối xứng với
`RunnerService` server-side. Mỗi method tự build + ký metadata Ed25519 trước
khi gọi, vì vậy caller không bao giờ phải tự thao tác `MetadataMap`.

```rust
pub struct RunnerClient {
    inner: KernelRunnerClient<Channel>,
    session_id: String,        // = base64(public_key)
    signing_key: SigningKey,   // Ed25519 private key
}

impl RunnerClient {
    /// Connect + Start gộp trong 1 call. Nếu `require_challenge=true`
    /// trong `params`, caller gọi `verify()` tiếp theo.
    pub async fn connect(endpoint: &str, params: SessionParams) -> Result<Self>;

    /// Giải mã challenge bằng master_key rồi trả plaintext cho runner.
    pub async fn verify(&mut self, response: Vec<u8>) -> Result<VerifyResponse>;

    /// Gửi code (unbounded string), stream `ExecEvent` cho đến `Done(true)`.
    /// Multi-line tự nhiên: kernel Python/Julia xử lý qua `exec(compile)`
    /// hoặc `Meta.parseall()`.
    pub async fn execute(&mut self, code: &str) -> Result<ExecOutcome>;

    pub async fn interrupt(&mut self) -> Result<()>;
    pub async fn close(&mut self) -> Result<()>;
    pub async fn health(&mut self) -> Result<HealthStatus>;

    /// Helper: AES-256-GCM decrypt challenge ciphertext.
    pub fn decrypt_challenge(&self, ciphertext: &[u8], master_key: &[u8]) -> Result<Vec<u8>>;
}
```

`ExecOutcome` gom stream `ExecEvent` thành 1 view:

```rust
pub struct ExecOutcome {
    pub events: Vec<ExecEvent>,
    pub value: Option<Value>,
    pub error: Option<ErrorEvent>,
    pub timed_out: bool,
}

impl ExecOutcome {
    pub fn ok(&self) -> bool;
    pub fn text(&self) -> Option<&str>;       // extract Value::Text
    pub fn number(&self) -> Option<f64>;      // extract Value::Number
    pub fn stdout(&self) -> String;           // concat stdout_line events
    pub fn stderr(&self) -> String;           // concat stderr_line events
}
```

### 3.8 KernelRepl state machine

REPL layer nằm ở `crates/opsense/src/repl/kernel.rs` chỉ ngồi trên
`RunnerClient` + Reedline. Mode + chuyển trạng thái:

```
Idle    → :py / :jl / :echo    → Inline  (RunnerClient::connect + Start)
Inline  → :block               → Block
Block   → (empty line)         → Block   (join buffer → execute, stay)
Block   → :inline              → Inline
Any     → :send                → Block   (force-execute buffer)
Any     → :abort               → interrupt
Any     → :exit / :q           → close + return
Ctrl-C                        → clear buffer (no exit)
Ctrl-D                        → close + return
```

`opsense repl --runner http://127.0.0.1:50051` sẽ dispatch thẳng vào
`KernelRepl`. Khi không có `--runner`, REPL rơi về GraphQL mode (`repl/mod.rs`
tự check `runner.is_some()`).

### 3.9 Service Account Auth (Cross-Machine)

Khi REPL và Runner ở **khác máy**, cần thay `LocalAuth` (in-process) bằng
`RemoteAuth` (lookup public_key từ serve khi cache miss). Keypair Ed25519 do
**serve** mint (qua `POST /api/oauth/v1/session/issue`); REPL chỉ nhận và
lưu, Runner chỉ verify.

```
┌──────────┐                       ┌──────────┐                       ┌──────────┐
│   REPL   │                       │  Runner  │                       │  Serve   │
│  (local) │                       │ (remote) │                       │ (Nginx)  │
└────┬─────┘                       └────┬─────┘                       └────┬─────┘
     │ 1. opsense session issue         │                                  │
     │ ──────────────────────────────────────────────────────────────────>│
     │                                  │ <── mint Ed25519 keypair ────────│
     │ <────────────── { session_id, private_key, expires_in } ───────────│
     │                                  │                                  │
     │ 2. gRPC request (signed)         │                                  │
     │ ─────────────────────────────>  │                                  │
     │                                  │ 3. cache hit?                    │
     │                                  │    ├─ yes → verify               │
     │                                  │    └─ no  → POST /api/admin/v1/  │
     │                                  │         session/resolve ──────> │
     │                                  │         { private_key }          │
     │                                  │         ← derive public_key ──── │
     │                                  │         cache LRU                │
     │                                  │         verify                   │
```

**Hai luồng auth độc lập** — REPL/Runner **không** tự tạo keypair:

| Luồng | Hướng | Credential | Lưu trên |
|------|------|-----------|----------|
| gRPC verify | REPL → Runner | Ed25519 (session_id = public_key) | REPL: `~/.config/opsense/sessions/<id>.json` (mode 0600) |
| HTTP fetch | Runner → Serve | Admin Bearer (`abt_*`) | Runner: `~/.config/opsense/runner.json` (mode 0600) |

#### Code map

| File | Vai trò |
|------|---------|
| `crates/opsense/src/api/oauth/v1.rs` | `session_issue/revoke/list` handlers (đã có) |
| `crates/opsense/src/api/admin/v1.rs` | `session/resolve` handler (TODO: chưa implement) |
| `crates/opsense/src/client/session.rs` | REPL HTTP client: `issue/list/revoke` |
| `crates/opsense/src/cli/session.rs` | `opsense session {issue,list,revoke,resolve,import}` |
| `crates/opsense-runner/src/http_client.rs` | `ServeClient` (Bearer + timeout) |
| `crates/opsense-runner/src/auth.rs` | `verify_with_public_key()`, `LocalAuth`, `RemoteAuth` (LRU) |
| `crates/opsense-runner/src/config.rs` | `RunnerConfig::load()` đọc `runner.json` + env |
| `crates/opsense-runner/src/lib.rs` | `build_auth(&cfg)` factory |

#### Fail-closed

`RemoteAuth::verify_signature` nếu serve lookup lỗi (timeout, 5xx, network
down) → trả `Ok(false)`, **không** trả `Err`. gRPC layer coi `Ok(false)` =
"signature không hợp lệ" (reject) còn `Err` = "lỗi hệ thống" (500). Khi
serve down ta muốn reject hết (fail-closed) chứ không trả 500 lung tung.

#### Out of scope

- E2E test cần serve thật (chưa có serve instance trong CI)
- `POST /api/admin/v1/session/resolve` handler chưa implement (cần reuse
  `resolve_long_session` ở model layer)
- `RemoteAuth::create_challenge` chưa wire serve (cần master_key ở serve)

---

## 4. AppState (Tầng 1 — phase 1)

```rust
pub struct AppState {
    pub context: Arc<Context>,          // stations: Arc<RwLock<HashMap<String, Station>>>
    pub runtime: Arc<RwLock<Runtime>>,  // reload, send_tick, stop, wait_for_shutdown
}
```

Phase 1: siêu gọn. Process chạy 1 pipeline duy nhất, không có session map
hay event ring buffer.

Phase 2 REPL mode (đã xong 2026-09-03): REPL layer (`KernelRepl`) **không đi
qua host** — chạy thẳng `RunnerClient` tới runner gRPC. Có nghĩa là Tầng 2
đã thật sự expose cho người dùng cuối qua `opsense repl --runner`, mà không
cần chờ GraphQL bridge.

Phase 2 GraphQL bridge (chưa làm): thêm `runner: Option<Arc<RunnerClient>>`
khi cần expose Tầng 2 qua `/graphql` (cho curl, MCP, internal admin).
RunnerClient đã có ở `crates/opsense/src/client/grpc.rs` — chỉ cần wrap
qua async-graphql resolvers (6 RPCs: `kernelStart`/`kernelExecute`/...).
`Option<...>` vì host có thể chạy telemetry-only không cần runner.

---

## 5. GraphQL schema (Tầng 1 — phase 1)

Đặt tại `crates/opsense/src/api/repl/schema.rs`. Một schema duy nhất cho cả
REPL client, MCP client, curl, internal admin. Mỗi resolver delegate xuống
`AppState` (`Context`, `Runtime`).

### 5.1 SDL

```graphql
scalar JSON
scalar DateTime

enum TelemetryKind { metric log trace }
enum Signal        { utilization saturation rate errors duration raw }
enum LogLevel      { debug info warn error }
enum StationKind   { timeseries category pattern }

type NodeSummary {
  id: String!
  type: String!
  inputs: [String!]!
}

type StationSummary {
  id: String!
  kind: StationKind!
}

type Status {
  configPath: String!
  nodes: [NodeSummary!]!
  stations: [StationSummary!]!
}

type EditResult {
  reloaded: Boolean!
  nodes: [NodeSummary!]!
}

type Observation {
  ts: Int!
  metricId: String!
  kind: TelemetryKind!
  signal: Signal!
  value: Float!
  labels: JSON
  severity: LogLevel
}

type CatalogEntry { key: String! value: String! }
type CatalogPage  { total: Int! offset: Int! limit: Int! items: [CatalogEntry!]! }

type PatternResult {
  matched: Boolean!
  node: String!
}

input ComponentInput {
  type: String!
  id: String!
  config: JSON
  inputs: [String!]
}

type Query {
  status: Status!
  stations: [StationSummary!]!
  queryTimeseries(node: String!, fromTs: Int, toTs: Int): [Observation!]!
  queryCatalog(node: String!, pattern: String, limit: Int = 100, offset: Int = 0): CatalogPage!
  queryPattern(node: String!, text: String!): PatternResult!
}

type Mutation {
  init(path: String!): Status!
  reload(components: [ComponentInput!]!): EditResult!
}
```

### 5.2 Nhóm operation

| Nhóm | GraphQL operations | Ghi chú |
|---|---|---|
| Xem/sửa pipeline | `Query.status`, `Mutation.init`, `Mutation.reload` | `init` / `reload` chỉ chạm Tầng 1 |
| Danh sách station | `Query.stations` | trả id + kind; client chọn query theo kind |
| Truy vấn station | `Query.queryTimeseries` / `queryCatalog` / `queryPattern` | tách theo kind; sai kind → error |

### 5.3 Mapping resolver → `AppState`

| Resolver | Nguồn |
|---|---|
| `Query.status` | `runtime` topology + `context.stations` |
| `Query.stations` | `context.stations` → match `Station` variant |
| `Query.queryTimeseries(node, fromTs, toTs)` | `context.station::<TimeseriesStation>(node).query_range(from, to)` |
| `Query.queryCatalog(node, pattern, limit, offset)` | `context.station::<CategoryStation>(node).contains(pattern, offset, limit)` |
| `Query.queryPattern(node, text)` | `context.station::<PatternStation>(node).lookup(text)` |
| `Mutation.init(path)` | `Config::load` → `pipeline_from_config` → `runtime.reload` |
| `Mutation.reload(components)` | parse `ComponentInput` qua `typetag::serde` → `runtime.reload` |

### 5.4 Quyết định thiết kế chính

- **1 station = 1 time series**: `TimeseriesStation` lưu 1 chuỗi duy nhất
  (xem `station.rs:30-33`). Không có multi-metric → bỏ `metric: String` filter
  khỏi `queryTimeseries`.
- **Tên field `node` đồng nhất** trong 3 query — tránh nhầm `source` / `node`.
- **3 query tách theo kind**: client biết query nào cho station nào nhờ
  `stations[].kind`. Sai kind → GraphQL error rõ ràng.
- **Không có `Query.nodeTypes`**: `typetag::serde` ở
  `opsense-libs/src/vector/runtime/models.rs:133` tự fail khi deserialize
  `ComponentInput.type` không tồn tại → GraphQL trả error. Validate miễn phí.
- **AppState siêu gọn** (chỉ `context` + `runtime`): process chạy 1 pipeline
  duy nhất, không cần session map hay event ring buffer.

---

## 6. File layout

```
crates/
├── opsense/                  # Tầng 1 host (axum + GraphQL + pipeline runtime)
│   └── src/
│       ├── lib.rs            # pub mod api/init/mcp/repl/runner/serve
│       ├── main.rs           # clap dispatch: Serve / Runner / Repl / Mcp
│       ├── init.rs           # tạo config mẫu
│       ├── serve.rs          # axum + UDS/HTTP, mount /graphql (Phase 1)
│       ├── runner.rs         # bridge sang opsense-runner (Phase 4)
│       ├── template.toml     # config mẫu embed
│       ├── api/
│       │   ├── mod.rs        # AppState + health_check + routes(state)
│       │   ├── repl/
│       │   │   ├── mod.rs
│       │   │   ├── v1.rs
│       │   │   ├── schema.rs
│       │   │   └── resolvers.rs
│       │   └── admin/        # placeholder, Phase sau
│       ├── client/
│       │   ├── mod.rs        # pub use graphql + grpc
│       │   ├── graphql.rs    # OpsenseClient (reqwest → /graphql)
│       │   └── grpc.rs       # RunnerClient (tonic → runner, Ed25519 sig)
│       ├── repl/
│       │   ├── mod.rs        # dispatch GraphQL vs kernel mode (theo --runner)
│       │   ├── commands.rs   # GraphQL REPL command dispatch
│       │   ├── display.rs    # pretty printers
│       │   └── kernel.rs     # KernelRepl state machine (Idle/Inline/Block)
│       └── mcp/              # rmcp server (Phase 3)
│
├── opsense-runner/           # Tầng 2 runner (gRPC + Ed25519 + kernel IPC)
│   └── src/
│       ├── lib.rs            # re-exports + run(bind, cfg, auth)
│       ├── auth.rs           # Auth trait + LocalAuth + AuthContext
│       ├── backend/
│       │   ├── mod.rs        # KernelBackend trait + EchoBackend + LocalBackend
│       │   ├── ipc.rs        # IpcKernelBackend (spawn kernel, framed stdio)
│       │   └── arrow.rs      # Arrow IPC helpers
│       ├── config.rs         # RunnerConfig
│       ├── server.rs         # RunnerService (tonic), 6 RPCs
│       └── session.rs        # SessionRegistry + implicit-keepalive sweeper
│
├── opsense-kernel-echo/      # in-process echo kernel (test, dev)
├── opsense-kernel-python/    # Python kernel binary (subprocess)
└── opsense-kernel-julia/     # Julia kernel binary (subprocess)
```

---

## 7. Wire protocol

| Boundary | Wire | Phase |
|---|---|---|
| `serve ↔ curl / REPL / MCP` | GraphQL over HTTP (`POST /graphql`) | 1 |
| `serve ↔ runner` | gRPC (`KernelRunner` service trong `opsense-proto`) + Ed25519 sig trên metadata | 4 |
| `runner ↔ kernel` | Framed stdio IPC (`[tag u8][len u32 BE][payload protobuf]`) | 4 |
| Browser / HTTP client ↔ `serve` | REST cũ (`/health`, `/reload`, `/sources`) + `/graphql` mới | 1 |

Auth wire details:

```
metadata:
  x-session-id: <base64 ed25519 public key = session_id>
  x-timestamp:  <unix seconds, ±30s accepted>
  x-nonce:      <u64>
  x-signature:  <base64 ed25519.sign("{ts}:{nonce}:{method}")>
```

---

## 8. Tầng persistence (DB layer)

> Bổ sung 2026-09-03 sau khi gỡ bỏ sea-orm. DB hiện chỉ phục vụ **admin
> entity** (4 bảng) — không còn dính líu tới WMS/OHLC/chat/sitemap/gateway.

### 8.1 Bức tranh tổng thể

`Admin` (`opsense-model/src/entities/admin.rs`) dùng `sqlx::AnyPool` (1
pool/DSN, multi-dialect) do `Resolver` cấp. Mọi query của admin đều đi qua
4 bảng `sys_*`; mọi bảng khác trong `sql/` (mysql + postgres) đã được dọn
sạch ngày 2026-09-03 vì không còn code nào đụng tới.

```
                 ┌──────────────┐
                 │  Admin        │   sqlx::query(...)
                 │  (entity)     │   - MySQL: ON DUPLICATE KEY UPDATE
                 │               │   - Postgres/SQLite: ON CONFLICT (...) DO UPDATE
                 └──────┬────────┘
                        │
                        ▼
              ┌──────────────────┐
              │  Resolver        │   DbKind::from_dsn(DSN)
              │  (AnyPool)       │   - mysql  → DbKind::MySql
              │                  │   - postgres → DbKind::Postgres
              │                  │   - sqlite  → DbKind::Sqlite
              └──────┬───────────┘
                     │
              ┌──────┴──────────────────┐
              ▼                         ▼
   ┌─────────────────┐       ┌────────────────────┐
   │ MySQL 8 / Maria │       │ Postgres 16         │
   │ (default)       │       │ (alternative)       │
   └─────────────────┘       └────────────────────┘
```

### 8.2 4 bảng `sys_*` — schema + mục đích

Tất cả schema hiện hữu ở [`sql/mysql/03-create-tables-of-sys.sql`](../sql/mysql/03-create-tables-of-sys.sql)
và [`sql/postgres/03-create-tables-of-sys.sql`](../sql/postgres/03-create-tables-of-sys.sql).

#### `sys_tenant` — ánh xạ host → tenant_id

| Column | Type | Mục đích |
|---|---|---|
| `host` | `VARCHAR(200) PK` | domain/subdomain admin dùng để tra (`get_tenant_id`) |
| `id` | `BIGINT UNIQUE` | tenant_id nội bộ; hash cho cache/cache shard |
| `jwt_mode` | `VARCHAR(20)` | `null / hs256 / oidc` (chọn mode auth) |
| `jwt_secret` | `BIGINT` | FK → `sys_token_map.id` (token lưu HS256 secret, mã hoá) |
| `oidc_jwks_url` | `VARCHAR(500)` | URL JWKS của IdP |
| `oidc_issuer` | `VARCHAR(255)` | expected `iss` claim |
| `oidc_client_id` | `VARCHAR(255)` | OIDC client_id (chuỗi public) |
| `oidc_client_secret` | `BIGINT` | FK → `sys_token_map.id` |
| `oidc_expected_alg` | `VARCHAR(10)` | `RS256` / `ES256` / ... |
| `session_secret` | `BIGINT` | FK → `sys_token_map.id` (dùng cho cookie session) |

API dùng: `GET /tenant/{host}/id`, `GET /tenant/{host}/auth-config`.

#### `sys_oidc` — cấu hình OIDC cho từng tenant

Cùng shape với `sys_tenant` nhưng cho phép **nhiều** cấu hình OIDC/tenant
(nhiều IdP cho 1 tenant — vd phân biệt internal vs customer). Hiện
`Admin` chưa đụng tới — để dành cho phase auth (PLAN.MD phase 4).

#### `sys_token_map` — token mã hoá theo service

| Column | Type | Mục đích |
|---|---|---|
| `id` | `BIGINT PK` | tham chiếu từ `sys_user.token_id` và các FK secret ở `sys_tenant` |
| `tenant_id` | `BIGINT` | shard theo tenant |
| `service` | `VARCHAR(200)` | tên service: `admin_db_token`, `oidc_client_secret`, ... |
| `token` | `VARBINARY(1024)` / `BYTEA` | plaintext token **đã mã hoá** qua `opsense_libs::sops::encrypt` (master_key từ secret backend) |

Unique: `(tenant_id, service)`. API dùng: `GET /seo/tokens/{name}`,
`POST /seo/tokens/{name}`. Cache: in-process LRU 32 entries, key
`(tenant_id, service)` và `(tenant_id, id)` (`Admin::cache_unencrypted_tokens_by_*`).

#### `sys_user` — user token issued

| Column | Type | Mục đích |
|---|---|---|
| `id` | `BIGINT PK` | row id nội bộ |
| `tenant_id` | `BIGINT` | shard key |
| `user_id` | `VARCHAR(255)` | định danh user (do client cung cấp) |
| `token_hash` | `VARCHAR(64)` | sha256 hex của plaintext token — index UNIQUE để tra nhanh |
| `token_id` | `BIGINT` | FK → `sys_token_map.id` (lưu plaintext đã mã hoá) |
| `expires_at` | `TIMESTAMP NULL` | TTL; null = không hết hạn |
| `revoked_at` | `TIMESTAMP NULL` | set khi revoke; null = còn hiệu lực |
| `last_used_at` | `TIMESTAMP NULL` | update mỗi lần verify thành công |
| `created_at` / `updated_at` | `TIMESTAMP` | audit |

Unique: `(tenant_id, user_id)`, `token_hash` toàn cục.

API dùng: `POST /tokens/users` (issue), `GET /tokens/users`
(list), `GET /tokens/users/{user_id}` (reveal), `DELETE
/tokens/users/{user_id}` (revoke), `POST /token/introspect` (verify).

### 8.3 Multi-dialect: cách chọn UPSERT syntax

`Admin` gọi `Resolver::database_kind(tenant_id)` (xem
[`resolver.rs:14-36`](../crates/opsense-model/src/resolver.rs)) để lấy
`DbKind`. Hai syntax khác nhau cho cùng 1 ngữ nghĩa upsert:

| Dialect | INSERT ... ON CONFLICT / ON DUPLICATE KEY |
|---|---|
| `MySql` | `... VALUES (...) ON DUPLICATE KEY UPDATE token = VALUES(token), updated_at = CURRENT_TIMESTAMP` |
| `Postgres` / `Sqlite` | `... VALUES (...) ON CONFLICT (tenant_id, service) DO UPDATE SET token = EXCLUDED.token, updated_at = CURRENT_TIMESTAMP` |

Tương tự cho `sys_user` (xem `admin.rs:336-345` cho token,
`admin.rs:399-417` cho user). Mọi chỗ khác (SELECT, UPDATE đơn lẻ) đều
dialect-agnostic.

### 8.4 Quyết định thiết kế chính

- **Bỏ sea-orm hoàn toàn**: resolver chỉ trả `sqlx::AnyPool` — không có
  bridge nào sang sea-orm. Admin dùng raw `sqlx::query(...)`. Lý do:
  giảm compile time, giảm 1 tầng abstraction, multi-dialect mà không
  cần feature gating phức tạp.
- **Datetime qua TEXT**: `sqlx::Any` không có `chrono` impl, nên
  `expires_at` / `revoked_at` / `last_used_at` được SELECT qua
  `CAST(... AS TEXT)` rồi parse bằng `parse_dt` (hỗ trợ cả RFC3339 lẫn
  `YYYY-MM-DD HH:MM:SS`). Xem `admin.rs:parse_dt`.
- **Plaintext token không bao giờ ở DB**: chỉ lưu sha256 hex (index) +
  ciphertext trong `sys_token_map.token`. Verify = sha256(input) → tra
  `sys_user` → lấy `token_id` → đọc `sys_token_map` → decrypt → so sánh
  constant-time (`subtle::ConstantTimeEq`).
- **In-process cache, không Redis cache cho admin token**: cache chỉ
  phục vụ hot path `verify_user_token` (lookup theo `token_hash` hoặc
  `service+tenant_id`). Redis được `Resolver` setup riêng cho các
  pipeline use case khác.

### 8.5 Phạm vi cleanup (2026-09-03)

Đã xoá khỏi `sql/` (mysql + postgres):

- WMS (inventory + picking + zones): `wms_*` — không còn code nào đụng.
- OHLC (finance + brokers + gold stores + bank rates): `ohcl_*` — legacy.
- Chat: `chat_threads` — legacy.
- Sitemap / article / file management: `sys_sitemap`, `sys_articlemap`,
  `sys_filemap` — API đã bỏ.
- API gateway / table gateway: `sys_api_map`, `sys_table_map`,
  `sys_database_map` — API đã bỏ.
- Component management: `sys_streams`, `sys_sinks`,
  `sys_link_streams_to_sinks` — `into_components` đã bỏ.

Còn lại **chỉ 4 bảng** (`sys_tenant`, `sys_oidc`, `sys_token_map`,
`sys_user`) — khớp 1-1 với các method của `Admin` entity.

---

## 9. Roadmap

> Audit ngày 2026-09-04. Phase 5 build infra + CI unit test workflow đã xong;
> Phase 2 GraphQL bridge vẫn cần wrap RunnerClient qua resolvers.

- ✅ Phase scaffolding (Cargo, opsense-libs, opsense-core rewrite, opsense-components)
- ✅ `init` subcommand (tạo config mẫu)
- ✅ `serve` subcommand (axum + UDS/HTTP, GraphQL skeleton)
- 🟡 **Phase 1 (đang làm)**: GraphQL schema + resolvers cho Tầng 1
- 🟡 **Phase 1.5 (đang làm)**: OAuth2 Device Authorization Grant + Session Keys
  - ✅ Refactor `admin.rs` → 3 traits + helpers + device + session modules
  - ⏳ 2 bảng `sys_long_sessions` / `sys_short_sessions` (PR #3)
  - ⏳ 9 OAuth endpoints dưới `/api/oauth/v1/` (PR #4)
  - ⏳ Bearer middleware trong REPL/MCP (PR #5)
  - ⏳ Tests + Prometheus metrics (PR #6)
  - ⏳ Cleanup (serve.rs.bak, init.rs, docker-compose) (PR #7)
- ✅ **Phase 2 (revised 2026-09-03): REPL ↔ Runner gRPC integration**
  - ✅ `RunnerClient` ở `crates/opsense/src/client/grpc.rs` — Ed25519 sign mỗi
    request, `ExecOutcome` aggregator (`ok`/`text`/`number`/`stdout`/`stderr`)
  - ✅ `KernelRepl` ở `crates/opsense/src/repl/kernel.rs` — state machine
    (Idle/Inline/Block) + Reedline
  - ✅ CLI flag `--runner <endpoint>` ở `main.rs` (`Commands::Repl`)
  - ✅ 5 unit tests trong `grpc.rs` (multi-line, block, auth, interrupt, health)
  - ⏳ Phase 2 GraphQL bridge — RunnerClient sẵn sàng, wrap qua 6 resolvers
    `kernelStart`/`kernelExecute`/`kernelClose`/`kernelInterrupt`/`kernelHealth`/...
  - ⏳ Tests verify khi `opsense-kernel-echo` binary buildable
- ✅ **Phase 3: REPL + MCP client** — đã xong
  - `crates/opsense/src/repl/{mod,commands,display}.rs` + Reedline
  - `crates/opsense/src/mcp/{mod,server,tools}.rs` + rmcp 0.6
  - Shared `OpsenseClient` (`client/graphql.rs`) gọi `/graphql` qua reqwest
  - `main.rs`: subcommand `Repl` / `Mcp`
- 🟡 **Phase 4 (đang làm)**: gRPC runner + Ed25519 auth
  - ✅ `crates/opsense-runner/` skeleton: auth, 3 backends, SessionRegistry, RunnerService
  - ✅ `cargo check -p opsense-runner` clean (2 dead-code warnings on `cfg`)
  - ✅ `RunnerClient` (tonic) đã viết ở Phase 2 (`crates/opsense/src/client/grpc.rs`)
  - ✅ Tích hợp `Commands::Runner` vào `opsense` binary (`main.rs:34-38, 63-83`)
  - ⏳ `tests/grpc_e2e.rs` rewrite (cũ vẫn work với raw `KernelRunnerClient`)
- ✅ **Phase 5: Earthfile-based build & deployment (2026-09-04)**
  - `Earthfile` build 4 image (`opsense-serve`, `opsense-runner` echo,
    `opsense-runner-python`, `opsense-runner-julia`) qua `+builder`/`+recipe`/
    `+binaries` chia sẻ cargo-chef cache.
  - `docker-compose.yml` hiện có **5 services** (postgres, valkey,
    opsense-serve, opsense-runner-python, opsense-runner-julia). Echo
    runner build được nhưng chạy ngoài compose.
  - `Makefile` 12 target (`build-local`/`build-cloud`/`up`/`down`/...).
  - `.github/workflows/release.yml` push tag `v*` → `earthly --push +all`
    → 4 image lên `ghcr.io/lap02921/opsense-*:v*`.
  - Known gaps: vector service (open #4), E2E test, prod deploy, Phase 2
    GraphQL bridge.
- ✅ **CI: unit test workflow (2026-09-04)**
  - `.github/workflows/ci.yml` chạy `cargo test --workspace --all-targets`
    trên push/PR `main`.
  - Toolchain: Rust 1.94 + Python 3.12 + Julia 1.10 + redis 7 service +
    protoc.
  - Đã chạy ổn (user confirm 2026-09-04). Tách 2 workflow: CI =
    correctness gate, release = image publish.
- ⏳ Cleanup: `crates/opsense/src/serve.rs.bak` (backup Aug 30, không load)

### §9.2 OAuth2 Device Authorization Grant (RFC 8628)

**Mục đích:** Cho phép REPL/MCP client xác thực với `opsense-serve` mà không cần
password mỗi lần — cùng cơ chế như `gcloud auth login`.

**Hai loại session:**

| Loại | Bảng | TTL | Storage |
|---|---|---|---|
| Long session | `sys_long_sessions` | 8h | Ed25519 keypair, `private_key` mã hóa AES-256-GCM |
| Short session | `sys_short_sessions` | 5min | Hashed `access_token`, partition drop tự cleanup |

**Flow:**

```
REPL ──POST /api/oauth/v1/device/code──▶ Axum
   ◀─── { device_code, user_code, interval }

User ──GET /api/oauth/v1/device──▶ Axum (HTML form)
       Nhập user_code → POST /api/oauth/v1/device/verify
       (Bearer JWT → X-User-Id từ Nginx)

REPL ──POST /api/oauth/v1/device/token──▶ Axum
       Poll (interval) cho đến khi user duyệt
       ◀─── { access_token, refresh_token, session_id }
```

**9 endpoints:**

| Method | Path | Auth | Mô tả |
|---|---|---|---|
| POST | `/api/oauth/v1/device/code` | None | Tạo `device_code` + `user_code` |
| GET | `/api/oauth/v1/device` | None | HTML form |
| POST | `/api/oauth/v1/device/verify` | Bearer | User duyệt device code |
| POST | `/api/oauth/v1/device/token` | None | Poll lấy `access_token` |
| POST | `/api/oauth/v1/token/refresh` | None | Refresh `access_token` |
| POST | `/api/oauth/v1/token/revoke` | Bearer | Revoke refresh token |
| POST | `/api/oauth/v1/session/issue` | Bearer | Sinh Ed25519 keypair |
| POST | `/api/oauth/v1/session/revoke` | Bearer | Revoke long session |
| GET | `/api/oauth/v1/session/list` | Bearer | List long sessions |

**Bearer validation:** Nginx `conf/nginx/vhost/04-api.conf` dùng `jwt_required=1`,
validate HS256/JWKS, inject `X-User-Id` + `X-Tenant-Id` header.

### §9.3 Session Management (Long / Short)

**`sys_long_sessions`** — dùng cho MCP client dài hạn:

- `session_id` = `base64(public_key)` (Ed25519, 32 bytes)
- `private_key` = 32 bytes ngẫu nhiên, mã hóa AES-256-GCM bằng `MASTER_KEY`
- Mỗi gRPC request: ký Ed25519 signature → runner verify bằng `session_id` (public key)
- Lazy cleanup: khi lookup thấy `expires_at < now()` thì DELETE rồi return None

**`sys_short_sessions`** — dùng cho REPL access_token:

- 5 phút TTL, `token_hash` = `sha256(access_token)` để lookup
- Postgres: partition theo timestamp, procedure drop partitions > 7 ngày
- MySQL: `PARTITION BY RANGE` tương tự, EVENT scheduler gọi drop cũ

**Admin methods:**

- `issue_device_code`, `approve_device_code`, `poll_device_token`
- `issue_long_session`, `revoke_long_session`, `list_long_sessions`
- `insert_short_session`, `lookup_short_session`

---

## 10. Build & deployment (Earthfile-based)

> Bổ sung 2026-09-03. Thay thế 4 Dockerfile rời rạc bằng một `Earthfile`
> duy nhất, share cargo-chef cache giữa các binary build.

### 10.1 Bức tranh tổng thể

```
                       ┌────────────── opsense-serve ──────────────┐
                       │  OpenResty + alloy + opsense serve       │
   browser / curl ───▶ │  axum (UDS /var/run/axum + GraphQL)     │
                       └────────────────────┬────────────────────┘
                                            │ gRPC + Ed25519
                                            │ (OPSENSE_RUNNER_GRPC)
                                            ▼
       ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
       │ opsense-runner       │  │ opsense-runner-      │  │ opsense-runner-      │
       │ (debian-slim)        │  │ python (3.12-slim)   │  │ julia (1.10-bookworm)│
       │  + opsense-kernel-   │  │  + opsense-kernel-   │  │  + opsense-kernel-   │
       │  echo (default)      │  │  python              │  │  julia               │
       │  :50051              │  │  :50051              │  │  :50051              │
       └──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### 10.2 Earthfile target graph

```
+builder (rust:bookworm + pkg-config + protoc)  ← cache share
   ↓
+recipe (cargo chef prepare → recipe.json)  ← cache share
   ↓
   ├── +opsense  ──────────────┐
   ├── +kernel-echo  ──────────┤
   ├── +kernel-python ─────────┤── parallel, cùng đọc recipe.json
   └── +kernel-julia  ─────────┘
       ↓
       ├── +serve  (openresty base + lua modules + alloy + copy +opsense)
       ├── +runner (debian-slim + copy +opsense + copy +kernel-echo)
       ├── +runner-python (python:3.12-slim + copy +opsense + copy +kernel-python)
       └── +runner-julia (julia:1.10-bookworm + copy +opsense + copy +kernel-julia)
```

### 10.3 4 image output

| Image | Base | Binary | Port | Tag (default) |
|---|---|---|---|---|
| `opsense-serve` | `openresty/openresty:1.27.1.2-4-bookworm-fat` | `opsense serve` | `8080` | `local` (dev), `v*` (release) |
| `opsense-runner` | `debian:bookworm-slim` | `opsense runner` + echo kernel | `50051` | `local` / `v*` |
| `opsense-runner-python` | `python:3.12-slim` | `opsense runner` + python kernel | `50051` | `local` / `v*` |
| `opsense-runner-julia` | `julia:1.10-bookworm` | `opsense runner` + julia kernel | `50051` | `local` / `v*` |

Image name theo convention: `${REGISTRY}/${IMAGE_PREFIX}-<role>:${VERSION}`.
Mặc định `REGISTRY=ghcr.io`, `IMAGE_PREFIX=lap02921/opsense`, `VERSION=latest`.

### 10.4 Hai workflow tách biệt

**Local dev:**
```bash
earthly +all-local       # build 4 image với tag `local`
docker compose up -d      # chạy stack
curl http://localhost:8080/health
```

**Production (qua GH Action):**
- Push tag `v*` lên GitHub → trigger `.github/workflows/release.yml`
- Action: login ghcr.io → `earthly --push --build-arg VERSION=${{ github.ref_name }} +all`
- Image push lên `ghcr.io/lap02921/opsense-*:v*`
- Production server: `OPSENSE_TAG=v* docker compose pull && up -d`

### 10.5 Compose (5 services, hiện tại)

`docker-compose.yml` chỉ reference image (không có `build:` context).
Tag override qua biến `OPSENSE_TAG` (default `local`):

```yaml
services:
  opsense:
    image: opsense-serve:${OPSENSE_TAG:-local}
  opsense-runner-python:
    image: opsense-runner-python:${OPSENSE_TAG:-local}
  opsense-runner-julia:
    image: opsense-runner-julia:${OPSENSE_TAG:-local}
  postgres:    { image: postgres:16-alpine }
  valkey:      { image: valkey/valkey:9-alpine }
```

`opsense-runner` (echo) **không nằm trong compose** — Earthfile `+all-local`
vẫn build nó (artifact `+binaries/opsense-kernel-echo` + image
`opsense-runner:local`) nhưng nó là dev/test tool chạy ngoài stack
(REPL mode `--runner` cần endpoint của runner; user tự chạy `docker run
--rm -p 50051:50051 opsense-runner:local` cho local dev).

### 10.6 File-level thay đổi (Phase 5)

| File | Hành động |
|---|---|
| `Earthfile` | **Mới** — 1 file build 4 image, share cargo-chef cache |
| `.earthignore` | **Mới** — tương tự .dockerignore |
| `scripts/entrypoint.sh` | **Mới** — `mkdir /var/run/axum`, exec supervisord |
| `scripts/nginx.sh` | **Mới** — OpenResty foreground |
| `scripts/alloy.sh` | **Mới** — Alloy foreground |
| `conf/supervisor/opsense.conf` | **Mới** — 3 programs (app/nginx/alloy), NO tor |
| `conf/nginx/http.conf` | Sửa 1 dòng — comment out `load_module libproxy.so` |
| `docker-compose.yml` | Viết lại — 5 services, image reference only |
| `.github/workflows/release.yml` | **Mới** — trigger `v*` → `earthly --push +all` |
| `Makefile` | Viết lại — 12 target (`help`/`build-local`/`build-cloud`/`up`/`down`/`down-v`/`ps`/`logs`/`restart`/`shell`/`encrypt`/`decrypt`/`sql-clean`) |
| `Dockerfile` (cũ) | **Xoá** — thay bằng Earthfile |

### 10.7 Quyết định thiết kế chính

- **Một Earthfile thay 4 Dockerfile**: `+recipe` build 1 lần, cả 4 binary
  cùng đọc → giảm ~70% build time lần đầu. `SAVE IMAGE --cache-hint` ở
  `+builder` giúp Earthly reuse layer khi không đổi base.
- **Runner tách thành service riêng**: đúng theo kiến trúc 2 tầng
  (§3.1). Host `opsense serve` giao tiếp qua `OPSENSE_RUNNER_GRPC` env.
- **3 runner image riêng**: mỗi image chạy đúng 1 kernel backend, user
  tuỳ use case mà connect. Default là echo (test nhanh, không cần runtime
  ngoài).
- **`OPSENSE_TAG` override**: cùng `docker-compose.yml` phục vụ cả dev
  (`local`) lẫn prod (git tag), không cần maintain 2 file.
- **Không tor, không sops**: gỡ bỏ so với template. Supervisor cũng bỏ
  `[program:tor]`. Project này không cần.
- **`load_module libproxy.so` removed**: crate `proxy` (build NGINX C
  module) không tồn tại trong workspace. Lua-resty modules (JWT/OIDC/
  session/http/redis) đã xử lý đầy đủ.

### 10.8 Caveats

- `opsense-runner-julia` khá nặng (~1.5GB) vì kéo `julia:1.10-bookworm`.
  Có thể dùng `julia:1.10-alpine` để giảm, nhưng cần verify deps Linux.
- Khi Phase 2 GraphQL bridge bật, host sẽ tạo `RunnerClient` tới
  `opsense-runner-python:50051` hoặc `opsense-runner-julia:50051` (env
  `OPSENSE_RUNNER_GRPC`). Compose đã wire sẵn — chỉ cần thêm env khi
  triển khai thực tế.
- Open gap (`conf/opsense.conf.toml` reference `http://vector:8686` không
  có service vector trong compose): cần bổ sung vector service hoặc
  rewrite config trước khi `docker compose up opsense-serve` chạy
  end-to-end. **Không block CI** (CI chạy `cargo test` không cần compose).

### 10.9 CI workflow (Tầng 1 — unit test)

`.github/workflows/ci.yml` chạy trên push/PR `main`:

| Job | Runner | Mục đích |
|---|---|---|
| `test` (Setup & Unit Tests) | `ubuntu-latest` | Build + test workspace |

Service đi kèm: `redis:7` (port 6379, healthcheck) cho `opsense-libs`
test cần Redis backend.

Toolchains cài trong job:
- `rust: 1.94` (dtolnay)
- `python: 3.12` + numpy, pandas, pyarrow, protobuf
- `julia: 1.10` + Arrow, DataFrames, CSV, Plots
- `protoc` (`apt-get install protobuf-compiler`)

Cache:
- `Swatinem/rust-cache@v2` (key `ci`)
- Julia depot theo hash

Commands:
- `cargo build --workspace --all-targets`
- `cargo test --workspace --all-targets`

CI KHÔNG dùng Earthly — test trực tiếp source Rust. Earthly chỉ chạy
trong `.github/workflows/release.yml` (push tag `v*` → build/push 4 image
lên ghcr.io). Tách 2 workflow: CI = correctness gate, release = image
publish.

Trạng thái: ✅ chạy ổn từ 2026-09-04 (user confirm).
