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

## 8. Roadmap

> Audit ngày 2026-09-03. Phase 2 REPL mode (RunnerClient + KernelRepl) đã xong;
> Phase 2 GraphQL bridge chỉ còn wrap RunnerClient qua resolvers.

- ✅ Phase scaffolding (Cargo, opsense-libs, opsense-core rewrite, opsense-components)
- ✅ `init` subcommand (tạo config mẫu)
- ✅ `serve` subcommand (axum + UDS/HTTP, GraphQL skeleton)
- 🟡 **Phase 1 (đang làm)**: GraphQL schema + resolvers cho Tầng 1
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
- ⏳ Cleanup: `crates/opsense/src/serve.rs.bak` (backup Aug 30, không load)
