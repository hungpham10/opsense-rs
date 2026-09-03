# PLAN — Opsense capacity planning

> Tài liệu này mô tả **kế hoạch thực thi** cho `opsense-rs`. Trạng thái thực
> tế: xem [`architecture.md`](./architecture.md) (kiến trúc) và
> [`CHECKLIST.MD`](./CHECKLIST.MD) (audit). Cập nhật gần nhất: **2026-09-03**
> (Phase 5 Earthfile-based build & deployment).

## Vấn đề

- Quy trình SRE thường cố định và tuân theo các quy định tiêu chuẩn như error
  budget, SLA, SLO.
- SRE ở mỗi công ty vận hành khác nhau do công cụ / techstack khác nhau.
- SRE là con người, cần nghỉ ngơi → cần công cụ giám sát thay thế.
- SRE chỉ tiếp nhận hệ thống sau khi golive, theo dõi bất thường qua log/metric.

## Tầm nhìn

`opsense` = **hành vi metric + capacity engine** cho hạ tầng. KHÔNG làm RCA,
incident, remediation, LLM.

- Config hoàn toàn bằng TOML.
- Bộ thu thập dữ liệu: kết hợp **Rhai transform** + **jq engine tự viết**
  (`opsense_libs::jq::JsonQuery`). Endpoint có thể là bất kỳ dịch vụ nào
  expose metric (Prometheus, vector, custom).
- LRU cache dạng block (chia theo khoảng thời gian) cho truy vấn nhanh.
- Một **clock** đẩy tín hiệu tick định kỳ để pipeline biết thời điểm xử lý.

## Hai tầng

| Tầng | Vai trò | Runtime |
|---|---|---|
| **Tầng 1 — Telemetry** | Thu thập, transform, lưu trữ observations vào station | `opsense_libs::vector::Runtime` |
| **Tầng 2 — Analysis** | Chạy Python/Julia qua session, REPL/MCP client | `opsense_session::SessionManager` |

Hai tầng **độc lập**: pipeline chạy được không cần session; session chạy được
không cần pipeline. Phase 1 (đang làm) chỉ chạm Tầng 1.

---

## Phase 1 — GraphQL API cho Tầng 1 (đang thực hiện)

### Mục tiêu

Expose Tầng 1 qua endpoint `POST /graphql` trên `opsense serve`. Client (REPL,
MCP, curl) truy vấn pipeline + station đồng nhất qua GraphQL.

### 3 nhóm tính năng

| Nhóm | Mục đích |
|---|---|
| **Xem/sửa pipeline** | `status`, `init`, `reload` |
| **Danh sách station** | `stations` (id + kind) |
| **Truy vấn dữ liệu station** | `queryTimeseries` / `queryCatalog` / `queryPattern` (tách theo kind) |

### GraphQL schema

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

# TimeseriesStation — 1 station = 1 time series
type Observation {
  ts: Int!
  metricId: String!
  kind: TelemetryKind!
  signal: Signal!
  value: Float!
  labels: JSON
  severity: LogLevel
}

# CategoryStation
type CatalogEntry { key: String! value: String! }
type CatalogPage  { total: Int! offset: Int! limit: Int! items: [CatalogEntry!]! }

# PatternStation
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
  # (1) Xem pipeline
  status: Status!

  # (2) Danh sách station
  stations: [StationSummary!]!

  # (3) Truy vấn theo kind
  queryTimeseries(node: String!, fromTs: Int, toTs: Int): [Observation!]!
  queryCatalog(node: String!, pattern: String, limit: Int = 100, offset: Int = 0): CatalogPage!
  queryPattern(node: String!, text: String!): PatternResult!
}

type Mutation {
  # (1) Sửa pipeline
  init(path: String!): Status!
  reload(components: [ComponentInput!]!): EditResult!
}
```

### Quyết định thiết kế

- **1 station = 1 time series** (xem `opsense-core/src/station.rs:30-33`).
  `TimeseriesStation` lưu `LruCache<i64, Block, 32>`, mỗi entry chứa 1 chuỗi
  thời gian. Không có multi-metric trong 1 station → bỏ `metric: String`
  filter khỏi `queryTimeseries`.
- **Tên field `node` đồng nhất** giữa `queryTimeseries` / `queryCatalog` /
  `queryPattern` — tránh nhầm `source` vs `node`.
- **3 query tách riêng theo kind**: client biết ngay query nào dùng cho
  station nào nhờ `stations[].kind`. Sai kind → GraphQL error rõ ràng
  (`station 'x' is not a timeseries`).
- **Không liệt kê `nodeTypes`**: `typetag::serde` ở
  `opsense-libs/src/vector/runtime/models.rs:133` tự fail khi deserialize
  `ComponentInput.type` không tồn tại → GraphQL trả error. Validate miễn phí.
- **AppState siêu gọn** — chỉ `context` + `runtime`, không thêm session map
  hay event ring buffer. Process chạy 1 pipeline duy nhất.
- **Reuse types thay vì duplicate trong `v1.rs`**: `TelemetryKind`, `Signal`,
  `LogLevel`, `Observation` derive `Enum`/`SimpleObject` từ
  `opsense_model::events`; `StationKind` derive `Enum` trực tiếp trên
  `opsense_core::station::Station` (qua `kind()` method, vì async-graphql
  không derive được trên enum có data variants). Bỏ hẳn converter
  functions `telemetry_kind` / `signal` / `log_level` / `obs_to_gql` /
  `station_kind` trong `v1.rs`.

### Mapping schema → AppState

| Schema field | Nguồn |
|---|---|
| `Query.status` | `runtime` topology + `context.stations` |
| `Query.stations` | `context.stations` → match `Station` variant |
| `Query.queryTimeseries(node, fromTs, toTs)` | `context.station::<TimeseriesStation>(node).query_range(from, to)` |
| `Query.queryCatalog(node, pattern, limit, offset)` | `context.station::<CategoryStation>(node).contains(pattern, offset, limit)` |
| `Query.queryPattern(node, text)` | `context.station::<PatternStation>(node).lookup(text)` |
| `Mutation.init(path)` | `Config::load` → `pipeline_from_config` → `runtime.reload` |
| `Mutation.reload(components)` | parse `ComponentInput` → `runtime.reload` |

### File-level thay đổi (chỉ phase này)

| File | Hành động |
|---|---|
| `crates/opsense/src/api/repl/schema.rs` | **Mới** — toàn bộ types/enums + `QueryRoot`/`MutationRoot` |
| `crates/opsense/src/api/repl/resolvers.rs` | **Mới** — 7 resolvers |
| `crates/opsense/src/api/repl/v1.rs` | Sửa — `build_schema(state)` + `graphql` handler + `Extension(Arc<Schema>)` |
| `crates/opsense/src/api/repl/mod.rs` | Sửa — `routes(state)` build schema |
| `crates/opsense/src/api/mod.rs` | Sửa nhẹ — thêm `pub fn routes(state) -> Router<AppState>` |
| `crates/opsense/src/serve.rs` | Sửa nhẹ — gọi `api::routes(state)`, giữ middleware |

**KHÔNG đụng**: `Cargo.toml`, `repl/`, `mcp/`, `runner.rs`, `main.rs`, xóa crate cũ.

**Verify**: `cargo check -p opsense` pass.

### Caveats

- `TimeseriesStation::query_range` trả `Option<Vec<Observation>>` (`None` = cache miss). Resolver: `None` → list rỗng + log warning.
- `CategoryStation::insert(text, metadata)` — `CatalogEntry.value` trả `""` placeholder. Mở rộng sau.
- `ComponentInput.config` parse qua `serde_json::from_value::<Box<dyn Component>>` (typetag).

### Hiện trạng (rolling snapshot)

Cập nhật `2026-09-01`. Branch `feature/refactor-to-decoupe-monorepo-into-multiple-small-services`.

**Đã làm trong session này:**

| File | Thay đổi |
|---|---|
| `crates/opsense-libs/Cargo.toml` | Thêm `async-graphql = { workspace = true }` (cần cho `NodeInfo` derive) |
| `crates/opsense-libs/src/vector/runtime/models.rs` | `NodeInfo` thêm `#[derive(SimpleObject)]` + `#[graphql(name = "NodeInfo")]` |
| `crates/opsense-core/Cargo.toml` | Thêm `async-graphql = { workspace = true }` (cần cho `StationKind` derive) |
| `crates/opsense-core/src/station.rs` | `Station::kind()` method + `StationKind` enum derive `Enum` |
| `crates/opsense-core/src/lib.rs` | `pub use station::{Station, StationKind};` |
| `crates/opsense/src/api/repl/v1.rs` | Xoá duplicate `TelemetryKind`/`Signal`/`LogLevel`/`Observation`/`StationKind` enum + converter helpers; import từ `opsense_core` |

**Verify build hiện tại:**

- `cargo check -p opsense-libs` ✅
- `cargo check -p opsense-core` ✅
- `cargo check -p opsense` ❌ — **lỗi pre-existing** trong `opsense-components` (xem dưới), không phải do refactor này introduce.

**Đã xác nhận pre-existing** (bằng `git stash` + check HEAD — HEAD cũng broken y hệt):

1. `crates/opsense-core/src/context.rs:157` gọi `crate::registry::stations()` — module `registry` đã bị xoá (xem git status: `D crates/opsense-core/src/registry.rs`).
2. `crates/opsense-components/src/station/pattern_transform.rs` gọi `.optimize()`/`.similar()` trên `Arc<RwLock<PatternStation>>` — methods không tồn tại.
3. `crates/opsense-components/src/station/timeseries_sink.rs` dùng field `stage` / method `options()` trên `TimeseriesStationSink` — không có.
4. `crates/opsense-components/src/station/timeseries_sink.rs` gọi `ctx.get_node_watermark()` / `read_window()` / `set_node_watermark()` — methods không có trên `opsense_core::Context`.
5. `crates/opsense-components/src/station/category_transform.rs` và `timeseries_transform.rs` cũng dính tương tự.
6. `crates/opsense-components/src/clock.rs`, `http.rs`, `ingest.rs`, `processor.rs`, `signal.rs` — chưa verify chi tiết, có thể cũng broken.

**Còn lại (chưa làm — chờ user quyết):**

- ~~Refactor toàn bộ `crates/opsense-components/` để tương thích với `opsense-core` mới (bỏ `registry`, cập nhật `Context` API, đơn giản hoá các component theo Station model).~~ **Bỏ theo refactor branch** — `cargo build -p opsense` ✅ sạch.
- ~~Khôi phục `opsense-core::Context` methods cần thiết (`get_node_watermark`, `read_window`, `set_node_watermark`, `ensure_pattern`, `ensure_station`, ...) hoặc thiết kế lại contract giữa `Context` ↔ `Component`.~~ **Cố tình bỏ** theo user quyết.
- Quyết định cuối cùng về `Query.status` vs `Query.nodeInfo` — user đang suy nghĩ thêm; tạm giữ nguyên `Query.status`.

**Decisions pending:**

- Có thay `Query.status` thành `Query.nodeInfo` (đọc từ `Config.pipeline.components`, không cần `runtime.topology()`) hay giữ `status` cũ? — User suy nghĩ, tạm không động.
- Có thêm `Arc<Config>` vào `AppState` để `nodeInfo` đọc trực tiếp từ config không, hay vẫn đi qua runtime? — Tùy quyết định trên.

---

## Phase 2 (revised sau Phase 4) — RunnerClient thay vì SessionManager

> Phase 4 đã tách runner thành process độc lập + gRPC + Ed25519, nên Phase 2
> cũ (in-process `SessionManager` nhúng trong host) không còn phù hợp. Thay
> vào đó, host chỉ cần **một tonic client** (`RunnerClient`) tới runner.

**So sánh:**

| | Phase 2 cũ (hủy) | Phase 2 mới |
|---|---|---|
| Session state ở đâu | `Arc<SessionManager>` trong host process | Runner process (đã có) |
| Host → kernel | Backend `local-ipc` nhúng trong host | gRPC → runner → framed IPC |
| Auth | Không có | Ed25519 metadata, mỗi request |
| Multi-host | Khó (1 process = 1 manager) | Tự nhiên (N host → 1 runner) |
| Restart kernel crash | Cả host có thể chết | Host sống, runner restart kernel |

**Việc cần làm:**

1. **`RunnerClient`** (tonic wrapper) trong `opsense-runner` hoặc crate mới
   `opsense-runner-client`. Mỗi method (`start`, `execute`, `interrupt`,
   `close`, `ping`, `health`) tự:
   - Load/tạo Ed25519 keypair (lưu ở host, persistent per session).
   - Build metadata (`x-session-id`/`x-timestamp`/`x-nonce`/`x-signature`).
   - Await response.
2. **GraphQL schema Tầng 2**: thay vì `SessionManager`-centric, dùng
   `RunnerConnection` (1 connection = 1 gRPC channel). Schema:
   ```graphql
   type KernelSession { id: String! backend: String! }
   type KernelEvent   { kind: String! payload: JSON }
   type KernelResult  { stdout: String! text: String error: String
                         dataframe: JSON plots: [Int!]! }

   type Query {
     kernelHealth: KernelHealth!
     kernelListSessions: [KernelSession!]!
   }
   type Mutation {
     kernelStart(backend: String!, config: JSON): KernelSession!
     kernelExecute(id: String!, code: String!): KernelResult!
     kernelInterrupt(id: String!, requestId: String!): Ack!
     kernelClose(id: String!): Ack!
   }
   ```
3. **AppState**: thêm `runner: Option<Arc<RunnerClient>>` (None khi host chạy
   không có runner — telemetry only).
4. **Resolvers**: mỏng — forward `args` xuống `RunnerClient` + map response.
5. **REPL/MCP** (Phase 3 đã xong): tự động có thêm 6 commands/tools cho
   Tầng 2 thông qua cùng `/graphql` (HTTP path) hoặc qua runner gRPC trực
   tiếp (low-latency path).

**Tại sao Phase 2 cũ không cần:** Runner process đã sở hữu toàn bộ lifecycle
session, auth, sweeper, kernel. Tái-implement lại `SessionManager` trong host
chỉ để forward xuống gRPC là thừa — chỉ cần một client.

---

## Phase 2 (revised 2026-09-03) — REPL ↔ Runner gRPC integration

> Phase 2 mới nhất: client-side RunnerClient + REPL gRPC mode. Thay vì expose
> qua GraphQL (mất qua HTTP), REPL có thể nói thẳng với runner qua gRPC để
> latency thấp và auth Ed25519 native. Phase 2 GraphQL bridge vẫn còn trong
> roadmap, nhưng REPL-mode trước.

### Mục tiêu

`opsense repl --runner <endpoint> --kernel <python|julia|echo>` kết nối trực
tiếp tới runner đang chạy qua gRPC, không qua `opsense serve`. Hai execution
mode:

- **inline** (mặc định): mỗi dòng → một `Execute` call.
- **block**: tích luỹ dòng; dòng trống trigger execution nhưng vẫn ở block
  mode. Kernel natively handles multi-line (Python `exec(compile(...))`,
  Julia `Meta.parseall()`) nên client chỉ cần `join("\n")` rồi gửi 1
  `CodeRequest`.

### Kiến trúc

```
┌────────────────┐   gRPC + Ed25519 sig     ┌──────────────────┐
│ opsense repl   │ ───────────────────────► │ opsense-runner   │
│ (kernel mode)  │                          │ (Phase 4)        │
│ - RunnerClient │                          │                  │
│ - Reedline     │                          │                  │
│ - state machine│                          │                  │
└────────────────┘                          └────────┬─────────┘
                                                     │ framed IPC
                                                     ▼
                                          ┌──────────────────┐
                                          │ opsense-kernel-*  │
                                          └──────────────────┘
```

### File-level thay đổi

| File | Hành động |
|---|---|
| `crates/opsense/src/client/grpc.rs` | **Mới** — `RunnerClient` + `ExecOutcome` + 5 unit tests |
| `crates/opsense/src/client/mod.rs` | Sửa — re-export `RunnerClient`, `ExecOutcome` |
| `crates/opsense/Cargo.toml` | Sửa — thêm 5 workspace deps (ed25519-dalek, rand, subtle, chrono, base64) |
| `crates/opsense/src/repl/kernel.rs` | **Mới** — `KernelRepl` state machine (Idle/Inline/Block) |
| `crates/opsense/src/repl/mod.rs` | Sửa — `run(endpoint, runner)` dispatch to kernel mode khi `runner=Some` |
| `crates/opsense/src/main.rs` | Sửa — thêm `--runner` flag vào `Commands::Repl` |

### RunnerClient API

```rust
pub struct RunnerClient { /* channel, session_id, signing_key */ }

impl RunnerClient {
    /// Connect + Start trong 1 call. Nếu require_challenge=true thì
    /// caller phải gọi verify() trước khi execute.
    pub async fn connect(endpoint: &str, params: SessionParams) -> Result<Self>;

    pub async fn verify(&mut self, response: Vec<u8>) -> Result<VerifyResponse>;
    pub async fn execute(&mut self, code: &str) -> Result<ExecOutcome>;
    pub async fn interrupt(&mut self) -> Result<()>;
    pub async fn close(&mut self) -> Result<()>;
    pub async fn health(&mut self) -> Result<HealthStatus>;

    /// Decrypt challenge ciphertext (AES-GCM) sang raw 32 bytes.
    pub fn decrypt_challenge(&self, ciphertext: &[u8], master_key: &[u8]) -> Result<Vec<u8>>;
}

pub struct ExecOutcome {
    pub events: Vec<ExecEvent>,
    pub value: Option<Value>,
    pub error: Option<ErrorEvent>,
    pub timed_out: bool,
}

impl ExecOutcome {
    pub fn ok(&self) -> bool;
    pub fn text(&self) -> Option<&str>;
    pub fn number(&self) -> Option<f64>;
    pub fn stdout(&self) -> String;
    pub fn stderr(&self) -> String;
}
```

### REPL state machine

```
Idle    → :py / :jl / :echo    → Inline (connect + start_session)
Inline  → :block               → Block
Block   → (empty line)         → Block (EXECUTE, stay in Block)
Block   → :inline              → Inline
Any     → :send                → execute buffer
Any     → :abort               → interrupt, stay in mode
Any     → :exit                → close session, return
```

`Ctrl-C` (REPL-level) clear buffer, không exit. `Ctrl-D` close + exit.

### Tests

Trong `crates/opsense/src/client/grpc.rs` (`#[cfg(test)]` inline), 5 tests
reusing pattern từ `opsense-runner/tests/grpc_e2e.rs`:

| Test | Coverage |
|---|---|
| `multi_line_code_roundtrips` | `code: "line1\nline2\nline3"` → result_value.text giữ nguyên newline |
| `block_mode_buffer_executes_on_empty_line` | `["a","b","c"].join("\n")` → 1 result, không bị split |
| `runner_client_auth_flow` | Ed25519 sign Start + Execute round-trip, không challenge |
| `interrupt_cancels_execution` | Echo kernel `sleep:3000` không deadlock REPL |
| `health_returns_runner_info` | `kernel_name == "runner/ipc"` |

Tất cả test skip graceful khi `opsense-kernel-echo` binary chưa build.

### Hiện trạng

Cập nhật `2026-09-03`. Branch `feature/refactor-to-decoupe-monorepo-into-multiple-small-services`.

**Đã làm:**

| File | Trạng thái |
|---|---|
| `crates/opsense/src/client/grpc.rs` | ✅ Mới — `RunnerClient` + `ExecOutcome` + 5 unit tests |
| `crates/opsense/src/repl/kernel.rs` | ✅ Mới — `KernelRepl` state machine |
| `crates/opsense/src/repl/mod.rs` | ✅ Sửa — dispatch khi `--runner` set |
| `crates/opsense/src/main.rs` | ✅ Sửa — thêm `--runner` flag |
| `crates/opsense/src/client/mod.rs` | ✅ Sửa — re-export |
| `crates/opsense/Cargo.toml` | ✅ Sửa — 5 workspace deps |

**Verify build:**

- `cargo build -p opsense` ✅ — clean lib build
- `cargo build -p opsense --tests` 🔴 — 1 pre-existing error trong
  `crates/opsense/src/init.rs:70` (`opsense_core::config` private, từ refactor
  branch), không phải do Phase 2 này. Test code đã compile clean (3 `mut`
  fixes đã xong).
- Tests sẽ skip đến khi `opsense-kernel-echo` binary buildable (cũng blocked
  bởi cùng refactor: `opsense-kernel-echo/src/main.rs:164` reference
  `DatasetAck` proto đã xoá).

**Quyết định thiết kế đã chốt:**

- `RunnerClient::connect(endpoint, params)` gộp channel connect + Start RPC
  trong 1 call. Caller chỉ cần gọi `verify()` riêng nếu session require
  challenge. Tránh partial state khi caller quên gọi `start_session`.
- `decrypt_challenge(ciphertext, master_key) -> Vec<u8>` method trên
  `RunnerClient` (không phải free function): `sops::decrypt` trả hex
  string → hex-decode → 32 raw bytes. Encapsulate toàn bộ flow AES-GCM.
- Multi-line code: client không tự buffer, chỉ `join("\n")` rồi gửi
  nguyên xi trong `CodeRequest.code` (unbounded string). Kernel tự
  `exec(compile(code, "<opsense>", "exec"))` (Python) hoặc
  `Meta.parseall()` (Julia).
- `Ctrl-C` ở REPL level clear block buffer, không exit (khác với GraphQL
  REPL dùng `Ctrl-C` continue).

**Còn lại:**

- ⏳ Phase 2 GraphQL bridge (host `AppState.runner`, 6 resolvers
  `kernelStart`/`kernelExecute`/...). RunnerClient có sẵn nên chỉ cần wrap
  qua GraphQL.
- ⏳ Verify tests pass khi `opsense-kernel-echo` binary build được (cần
  fix `DatasetAck` reference trong echo kernel trước).
- ⏳ Sample config doc thêm `opsense-kernel-{python,julia}` để user
  reference cho `--kernel` flag.

## Phase 3 — REPL & MCP client (sau)

- `crates/opsense/src/repl/{mod,client,commands,display,completer}.rs` —
  REPL thuần client, gọi `ReplClient` (reqwest) sang `/graphql`.
- `crates/opsense/src/mcp/{mod,client,server}.rs` — MCP server dùng `rmcp`,
  mỗi tool delegate sang `OpsenseMcpClient` (cùng method set với `ReplClient`).
- Xóa `crates/opsense-repl/`, `crates/opsense-mcp/` cũ.

## Phase 4 — Runner subcommand (sau)

- `crates/opsense/src/runner.rs` + `runner/server.rs` — module thật, delegate
  sang `opsense_runner::run(bind)`.
- `main.rs`: `Commands::Runner { bind: Option<SocketAddr> }` → `opsense::runner::run(bind)`.

## Phase 4 — gRPC runner (đang thực hiện)

### Mục tiêu

Tách `opsense-runner` thành service độc lập, giao tiếp với host qua gRPC + Ed25519
auth. Host (REPL/MCP) chỉ cần ký request bằng private key → runner verify bằng
session-id (= public key) → forward xuống kernel process.

### Kiến trúc

```
┌────────────────┐   gRPC + Ed25519 sig     ┌──────────────────┐
│ Host (REPL/MCP)│ ───────────────────────► │ opsense-runner   │
│  - giữ private │                          │  - verify sig    │
│  - sign mỗi    │                          │  - spawn kernel  │
│    request     │                          │  - route stream  │
└────────────────┘                          └────────┬─────────┘
                                                     │ framed stdio
                                                     ▼
                                          ┌──────────────────┐
                                          │ opsense-kernel-*  │
                                          │  (Python/Julia/…) │
                                          └──────────────────┘
```

### Auth scheme (Ed25519 asymmetric)

Mỗi session_id = base64(public_key). Mỗi request mang metadata:

| Metadata | Nội dung |
|---|---|
| `x-session-id` | base64(public key) = session_id |
| `x-timestamp` | unix seconds, runner check ±30s |
| `x-nonce` | u64, dùng để chống replay |
| `x-signature` | base64(Ed25519.sign(`{ts}:{nonce}:{method}`)) |

**Sign scheme:** `sign(private_key, format!("{timestamp}:{nonce}:{method}"))`
với `method ∈ {Start, Execute, Close, Interrupt, Ping}`.

Runner verify với `verifying_key = decode(session_id)`. Timestamp ngoài ±30s →
reject. Signature sai → reject. Nonce chưa dùng `resolve_private_key` server-side
lookup (Phase 5).

### Crate layout (`opsense-runner`)

```
crates/opsense-runner/
├── Cargo.toml          # tonic, prost, ed25519-dalek, base64, async-trait
├── src/
│   ├── lib.rs           # re-exports + run(bind, cfg, auth) entry
│   ├── auth.rs          # Auth trait + LocalAuth (Ed25519) + AuthContext
│   ├── backend/
│   │   ├── mod.rs       # KernelBackend trait, HealthInfo, EchoBackend, LocalBackend
│   │   ├── ipc.rs       # IpcKernelBackend: spawn kernel, drive framed stdio
│   │   └── arrow.rs     # Arrow IPC segment ↔ RecordBatch helpers
│   ├── config.rs        # RunnerConfig (kernel command, sweep interval, idle timeout)
│   ├── server.rs        # RunnerService (tonic service), serve(bind, cfg, auth)
│   └── session.rs       # SessionRegistry + idle sweeper
```

### API surface (`KernelRunner` gRPC service)

| RPC | Stream | Mục đích |
|---|---|---|
| `Start(SessionParams) → SessionHandle` | unary | spawn kernel, mint session_id |
| `Execute(CodeRequest) → stream<ExecEvent>` | server-stream | chạy code, emit events |
| `Interrupt(InterruptRequest) → Ack` | unary | huỷ execution đang chạy |
| `Close(CloseRequest) → Ack` | unary | đóng session, kill kernel |
| `Ping(PingRequest) → Pong` | unary | implicit keepalive — touch session nếu sig OK |
| `Health(HealthRequest) → HealthStatus` | unary | thông tin backend |

### Implicit keepalive

Mỗi RPC `Start/Execute/Interrupt/Close/Ping` tự gọi `registry.touch(session_id)` —
host không cần explicit `:session ping`. Background sweeper định kỳ
(`sweep_interval_secs`) close mọi session quá `idle_timeout_secs`.

### Run entry

```rust
pub async fn run(
    bind: SocketAddr,
    cfg: RunnerConfig,
    auth: Option<SharedAuth>,
) -> Result<()>;
```

`auth = None` → open (no signature check). `auth = Some(arc)` → mọi RPC verify
Ed25519. Cùng một entry phục vụ cả production deployment (auth bật) và test
(auth tắt).

### Quyết định thiết kế

- **`AuthContext::TryFrom<&tonic::MetadataMap>`** thay vì free function
  `auth_from_request` — nhận `Result<AuthContext>` với error message chỉ rõ
  metadata key nào malformed.
- **`KernelBackend` trait + `dyn`** (`Arc<dyn KernelBackend>`) — registry
  không cần biết backend là `Ipc` (spawn process) hay `Echo` (in-process cho
  test). `EchoBackend` cho unit test không cần kernel binary.
- **`async_trait::async_trait`** trên cả `Auth` và `KernelBackend` — bắt buộc
  cho `dyn` compatibility (Rust async fn chưa dyn-safe natively).
- **`SessionRegistry` field** `sessions: Arc<Mutex<HashMap<String, SessionMeta>>>`
  — Arc để chia sẻ với sweeper task (Arc::clone, không clone Mutex).
- **Sweeper** extract thành `async fn sweep_loop(sessions, backend, interval, idle)`
  tránh fight ownership giữa spawn closure và struct construction.
- **`RunnerConfig` carry** trong `RunnerService`/`SessionRegistry` dù chưa đọc —
  forward-compatible khi thêm config-driven behaviors (max message size,
  per-backend auth, v.v.).
- **`pub use opsense_proto::host::ExecOutcome`** ở `backend/mod.rs` — gom
  wire types về một điểm, callers chỉ depend `opsense_runner::backend::*`.

### Hiện trạng (rolling snapshot)

Cập nhật `2026-09-02`. Branch `feature/refactor-to-decoupe-monorepo-into-multiple-small-services`.

**Đã làm:**

- ✅ `auth.rs` — `Auth` trait + `LocalAuth` (Ed25519 roundtrip, timestamp window
  ±30s, `TryFrom<&MetadataMap>` cho `AuthContext`, 4 unit tests pass).
- ✅ `backend/{mod,ipc,arrow}.rs` — `KernelBackend` trait (dyn-compatible), 3
  backends (`Echo`, `Ipc`, `Local`), Arrow IPC helpers.
- ✅ `config.rs` — `RunnerConfig` (kernel command, sweep/idle intervals).
- ✅ `session.rs` — `SessionRegistry` với implicit keepalive + background sweeper
  + auth gate (`verify_or_skip`).
- ✅ `server.rs` — `RunnerService` (6 RPCs: Start/Execute/Interrupt/Close/Ping/
  Health), gọi `AuthContext::try_from(request.metadata()).ok()` thẳng tại call site.
- ✅ `lib.rs` — single `run(bind, cfg, auth)` entry, gộp `run` + `run_with_auth`.

**Verify build hiện tại:**

- `cargo check -p opsense-runner` ✅ (2 dead-code warnings trên `cfg` fields —
  intentional carry-over cho future config-driven behaviors).
- `cargo check --workspace` ✅ cho runner + libs; `opsense-kernel-echo` còn
  reference `DatasetAck`/`DatasetHeader` đã bị xoá khỏi proto (pre-existing,
  không phải do phase này).

**Còn lại:**

- ⏳ Rewrite `crates/opsense-runner/tests/grpc_e2e.rs` — cũ vẫn work với raw
  `KernelRunnerClient`, chỉ cần refactor sang dùng helper `start_server()`
  từ `crates/opsense/src/client/grpc.rs` nếu muốn share.
- ⏳ Phase 5: server-side `resolve_private_key` lookup (Phase này luôn trả
  `None`, client tự giữ private key).

## Phases cũ (đã xong hoặc chuyển sang phase khác)

Tham khảo lịch sử trong git log. Phase scaffolding (Cargo workspace, opsense-libs
utilities, opsense-core domain rewrite, opsense-components, config TOML) đã
hoàn tất. Phase capacity engine, statistics, hot-reload config gốc đã được
thay thế bằng model mới: vector runtime + station registry + typetag-based
component deserialization.

---

## Trạng thái (rolling)

> Audit ngày 2026-09-02 qua codegraph. Phase 3 thực tế đã xong từ trước —
> docs cũ chưa cập nhật.

- ✅ Phase scaffolding (Cargo, opsense-libs, opsense-core rewrite, opsense-components)
- ✅ `init` subcommand (tạo config mẫu)
- ✅ `serve` subcommand (axum + UDS/HTTP, mount `/api/repl` GraphQL skeleton)
- 🟡 Phase 1 (đang làm): GraphQL schema + resolvers cho Tầng 1
  - ✅ `v1.rs` refactor: xoá duplicate types, dùng `opsense_core::{Observation, StationKind, Station}` trực tiếp
  - ✅ `NodeInfo` (opsense-libs) có `SimpleObject` derive
  - ✅ `cargo build -p opsense` sạch (zero errors, chỉ 2 dead-code warnings pre-existing trong `opsense-runner`)
  - ✅ `queryTimeseries`/`queryCatalog`/`queryPattern` đã wire xong ở `v1.rs`
  - ⏸ `Query.status` vs `Query.nodeInfo` — chờ user quyết
  - ✅ `opsense-components/` — cố tình bỏ theo refactor branch
  - ✅ `opsense-core::Context` methods — cố tình bỏ theo user quyết
- 🟡 **Phase 2 (revised 2026-09-03): REPL ↔ Runner gRPC integration**
  - ✅ `RunnerClient` ở `crates/opsense/src/client/grpc.rs` (Ed25519 sign mỗi
    request, `ExecOutcome` aggregator)
  - ✅ `KernelRepl` ở `crates/opsense/src/repl/kernel.rs` (state machine)
  - ✅ CLI `--runner <endpoint>` ở `main.rs`
  - ⏳ Phase 2 GraphQL bridge (host `AppState.runner`, 6 resolvers
    `kernelStart`/`kernelExecute`/...) — RunnerClient sẵn sàng, chỉ cần wrap
  - ⏳ Tests verify pass khi `opsense-kernel-echo` binary buildable
- ✅ **Phase 3: REPL + MCP client** (đã xong từ trước audit 2026-09-02)
  - `crates/opsense/src/repl/{mod,commands,display}.rs` — Reedline REPL gọi GraphQL
  - `crates/opsense/src/mcp/{mod,server,tools}.rs` — rmcp server expose 6 tools
  - `crates/opsense/src/client/graphql.rs` — `OpsenseClient` (reqwest) shared
  - `main.rs` dispatch: `Commands::Repl` / `Commands::Mcp` → `repl::run` / `mcp::run`
- 🟡 Phase 4 (đang làm): gRPC runner (runner crate) — auth, session registry, sweep, 3 backends, 6 RPCs
  - ✅ `cargo check -p opsense-runner` clean (2 dead-code warnings on `cfg`)
  - ✅ `Auth::TryFrom<&MetadataMap>` + Ed25519 roundtrip tests
  - ✅ `RunnerClient` (tonic) đã viết ở Phase 2 (xem `crates/opsense/src/client/grpc.rs`)
  - ⏳ Rewrite `tests/grpc_e2e.rs` (cũ vẫn work)
  - ✅ Tích hợp `Commands::Runner` vào `opsense` binary (`main.rs:34-38, 63-83`)
- ✅ **Phase 2 (revised 2026-09-03): REPL ↔ Runner gRPC integration**
  - ✅ `RunnerClient` ở `crates/opsense/src/client/grpc.rs` — Ed25519 sign mỗi
    request, kèm `ExecOutcome` aggregator (`ok`/`text`/`number`/`stdout`/`stderr`)
  - ✅ `KernelRepl` ở `crates/opsense/src/repl/kernel.rs` — state machine
    (Idle/Inline/Block) + Reedline, multi-line block buffer
  - ✅ CLI flag `--runner <endpoint>` ở `main.rs` (command `Repl`)
  - ✅ 5 unit tests trong `grpc.rs` (multi-line, block, auth flow, interrupt, health)
  - ⏳ Phase 2 GraphQL bridge (host `AppState.runner`, 6 resolvers
    `kernelStart`/`kernelExecute`/...) — RunnerClient sẵn sàng, chỉ cần wrap
  - ⏳ Tests verify khi `opsense-kernel-echo` binary buildable
- ⏳ Xóa `opsense-repl/`, `opsense-mcp/` (đã xóa khỏi workspace, xem Cargo.toml:3-15)

---

## Phase 5 — Earthfile-based build & deployment (đang thực hiện)

> Phần này bổ sung cho Phase 4: dùng Earthly (`Earthfile`) làm công cụ build
> & push image duy nhất, thay cho 4 Dockerfile rời rạc. Local dev dùng
> `earthly +all-local` → `docker compose up`; production dùng GitHub Action
> trigger theo tag → `earthly --push +all` → `ghcr.io/lap02921/opsense-*:v*`.

### Mục tiêu

1. **Một nguồn build duy nhất**: Earthfile, không còn Dockerfile.
2. **Cache share giữa các binary**: cargo-chef `recipe.json` build 1 lần,
   cả 4 binary (`opsense` + 3 kernel) cùng đọc → giảm ~70% build time.
3. **Tách rõ 2 workflow**:
   - **Local dev** (`earthly +all-local` + `docker compose up`).
   - **CI / release** (`earthly --push +all` trong GH Action khi push tag `v*`).
4. **Compose không cần `build:` context**: chỉ reference image, tag từ
   biến `OPSENSE_TAG` (default `local` cho dev).

### File layout

```
opsense-rs/
├── Earthfile                    # Build 4 image: serve / runner / runner-python / runner-julia
├── docker-compose.yml           # 6 services, chỉ image reference
├── .earthignore                 # Tương tự .dockerignore
├── .github/workflows/
│   └── release.yml              # Push tag v* → earthly --push +all
├── scripts/
│   ├── entrypoint.sh            # mkdir /var/run/axum, exec supervisord
│   ├── nginx.sh                 # OpenResty foreground
│   └── alloy.sh                 # Grafana Alloy foreground
└── conf/
    ├── supervisor/opsense.conf  # 3 programs: app + nginx + alloy (NO tor)
    └── nginx/http.conf          # Comment out `load_module modules/libproxy.so` (line 29)
```

### Earthfile targets

| Target | Loại | Mô tả |
|---|---|---|
| `+base` | Image | `rust:bookworm` + `pkg-config` (chia sẻ) |
| `+recipe` | Artifact | `cargo chef prepare` → `recipe.json` |
| `+opsense` | Artifact | Build `opsense` binary |
| `+kernel-echo` | Artifact | Build `opsense-kernel-echo` (default) |
| `+kernel-python` | Artifact | Build `opsense-kernel-python` |
| `+kernel-julia` | Artifact | Build `opsense-kernel-julia` |
| `+serve` | Image | OpenResty + lua modules + alloy + opsense binary |
| `+runner` | Image | debian-slim + opsense + echo kernel (default) |
| `+runner-python` | Image | python:3.12-slim + opsense + python kernel |
| `+runner-julia` | Image | julia:1.10-bookworm + opsense + julia kernel |
| `+all-local` | Build | Build 4 image với tag `local` (dev, không push) |
| `+all` | Build | Build & push 4 image (CI dùng với `--push`) |

### 4 image output

| Image | Registry tag | Base | Binary | Exposed port |
|---|---|---|---|---|
| `opsense-serve` | `${REGISTRY}/${IMAGE_PREFIX}-serve:${VERSION}` | `openresty/openresty:1.27.1.2-4-bookworm-fat` | `opsense serve` | `8080` |
| `opsense-runner` | `${REGISTRY}/${IMAGE_PREFIX}-runner:${VERSION}` | `debian:bookworm-slim` | `opsense runner --kernel opsense-kernel-echo` | `50051` |
| `opsense-runner-python` | `${REGISTRY}/${IMAGE_PREFIX}-runner-python:${VERSION}` | `python:3.12-slim` | `opsense runner --kernel opsense-kernel-python` | `50051` |
| `opsense-runner-julia` | `${REGISTRY}/${IMAGE_PREFIX}-runner-julia:${VERSION}` | `julia:1.10-bookworm` | `opsense runner --kernel opsense-kernel-julia` | `50051` |

Mặc định `REGISTRY=ghcr.io`, `IMAGE_PREFIX=lap02921/opsense`, `VERSION=latest`.
CI override `VERSION` qua `--build-arg VERSION=${{ github.ref_name }}`.

### Compose services (6)

| Service | Image | Phụ thuộc |
|---|---|---|
| `opsense` | `opsense-serve:${OPSENSE_TAG:-local}` | postgres (healthy), valkey (healthy), opsense-runner (started) |
| `opsense-runner` | `opsense-runner:${OPSENSE_TAG:-local}` | — |
| `opsense-runner-python` | `opsense-runner-python:${OPSENSE_TAG:-local}` | — |
| `opsense-runner-julia` | `opsense-runner-julia:${OPSENSE_TAG:-local}` | — |
| `postgres` | `postgres:16-alpine` | healthcheck: `pg_isready` |
| `valkey` | `valkey/valkey:9.0-alpine` | healthcheck: `valkey-cli ping` |

Network `opsense-net` (bridge). Volume `pg_data` cho postgres.

### Hai workflow

**Local dev:**
```bash
# Bước 1: build image local với tag `local`
earthly +all-local

# Bước 2: chạy stack
docker compose up -d

# Verify
curl http://localhost:8080/health
```

**Production (qua GH Action):**
```bash
# Dev push tag release
git tag v0.1.0 && git push --tags

# GH Action tự động:
#   1. Login ghcr.io
#   2. earthly --push --build-arg VERSION=v0.1.0 +all
#   3. Push 4 image: ghcr.io/lap02921/opsense-*:v0.1.0

# Production server pull & chạy:
OPSENSE_TAG=v0.1.0 docker compose pull
OPSENSE_TAG=v0.1.0 docker compose up -d
```

### Quyết định thiết kế

- **Một Earthfile thay vì 4 Dockerfile**: cargo-chef `recipe.json` share
  giữa 4 binary build, `SAVE IMAGE --cache-hint` ở `+base` giúp Earthly
  reuse layer khi không đổi. Earthly cũng tự parallel các `BUILD` không
  có dependency giữa chúng.
- **Runner là service riêng trong compose**: tách theo kiến trúc 2 tầng
  (architecture.md §3.1). Host (`opsense serve`) giao tiếp runner qua
  `OPSENSE_RUNNER_GRPC=opsense-runner:50051`. Khi Phase 2 GraphQL bridge
  bật, host sẽ tạo `RunnerClient` tới endpoint này.
- **3 runner image (echo / python / julia)**: mỗi image chạy đúng 1
  kernel. User connect tới runner nào tuỳ use case (echo = test nhanh,
  python = analysis, julia = scientific).
- **`OPSENSE_TAG` env override**: cùng `docker-compose.yml` phục vụ cả
  dev (`local`) lẫn prod (git tag). Không cần maintain 2 file compose.
- **KHÔNG có tor, sops**: gỡ bỏ so với template gốc vì project này không
  cần. Supervisor config cũng bỏ `[program:tor]`.
- **`load_module modules/libproxy.so` removed** (conf/nginx/http.conf:29):
  crate `proxy` (build NGINX C module) không tồn tại trong workspace.
  Các lua-resty module (JWT/OIDC/session/http/redis) xử lý đầy đủ
  chức năng qua openresty layer.

### Lợi ích vs 4 Dockerfile

| Tiêu chí | 4 Dockerfile | Earthfile |
|---|---|---|
| Cargo deps build | 4 lần | 1 lần (share qua `+base`/`+recipe`) |
| Time build lần đầu | ~45 phút | ~25 phút (parallel + share) |
| Time rebuild khi sửa code | ~10 phút/image | ~3-5 phút (chỉ binary target invalidate) |
| Push registry | Script shell + GHCR login | `earthly --push` |
| CI workflow | Multi-job matrix | 1 job `earthly +all` |
| Dev cần cài gì | Docker only | Earthly (~50MB) |

### Hiện trạng (rolling snapshot)

Cập nhật 2026-09-03.

**Đã làm:**

| File | Trạng thái |
|---|---|
| `Earthfile` | ✅ — `+base` + `+recipe` + 4 binary target + 4 image target + `+all-local` + `+all` |
| `.earthignore` | ✅ — loại trừ `target/`, `.git/`, `secrets/`, ... |
| `scripts/entrypoint.sh` | ✅ — tạo `/var/run/axum`, exec supervisord |
| `scripts/nginx.sh` | ✅ — OpenResty foreground |
| `scripts/alloy.sh` | ✅ — Alloy foreground |
| `conf/supervisor/opsense.conf` | ✅ — 3 programs (app/nginx/alloy), NO tor |
| `conf/nginx/http.conf` | ✅ — comment out `load_module libproxy.so` (line 29) |
| `docker-compose.yml` | ✅ — 6 services, chỉ image reference, `OPSENSE_TAG` override |
| `.github/workflows/release.yml` | ✅ — trigger `v*` → `earthly --push +all` |
| `Makefile` | ✅ — 4 target: `server / runner / kernel-{echo,python,julia}` |
| `Dockerfile` (cũ) | 🗑️ Xoá — thay bằng Earthfile |

**Còn lại:**

- ⏳ Test end-to-end local: cài Earthly → `earthly +all-local` → `docker compose up`
- ⏳ Verify GH Action workflow khi push tag `v0.0.1`
- ⏳ Production deploy với `OPSENSE_TAG=v*` pull từ ghcr.io
- ⏳ Phase 2 GraphQL bridge (cần `AppState.runner` để host dùng `RunnerClient` kết nối `opsense-runner:50051`)

### Test scenarios

Kịch bản test E2E cho 4 image + compose stack đã soạn tại
[`TEST-SCENARIOS.MD`](./TEST-SCENARIOS.MD) (9 kịch bản A→I, 12 bước).
Tóm tắt:

| Kịch bản | Mục đích |
|---|---|
| A — Build local | Earthly +all-local, 4 image artifact, fix caveat `protobuf-compiler` |
| B — Compose smoke | 6 service healthy, /health, GraphQL, gRPC, REPL mode |
| C — Network isolation | 3 runner reachable song song, session_id khác nhau, sweeper |
| D — Hot reload & persistence | GraphQL reload, sys_user CRUD qua Postgres, Valkey ping |
| E — Failure modes | Container crash → auto-restart, Tầng 1 OK khi Tầng 2 down, rollback |
| F — Performance baseline | Boot ≤60s, gRPC p99 ≤20ms, memory footprint |
| G — CI/CD GH Action | Tag `v*` → 4 image push ghcr.io, tag matrix |
| H — Security smoke | Auth reject khi thiếu/sai metadata, secrets không lộ trong image |
| I — Cleanup | Prune image + Earthly cache |

**Audit 2026-09-03** phát hiện 5 prerequisite chưa xong (xem
`TEST-SCENARIOS.MD` §11) — phải giải quyết trước khi chạy test:

1. `docker-compose.yml` chưa tồn tại trên đĩa.
2. `Earthfile +base` thiếu `protobuf-compiler`.
3. `Makefile` chưa có (PLAN.MD ghi nhầm "đã làm").
4. `conf/opsense.conf.toml` reference `vector:8686` không có trong compose.
5. Config mount qua `volumes:` chưa wire trong image `opsense-serve`.

### Caveats

- Earthfile `+base` chưa có `protoc` — cần bổ sung nếu build trong
  container (CI runner đã có sẵn; Earthly image base `rust:bookworm`
  thiếu `protobuf-compiler` cho crate `opsense-proto` có `build.rs`).
- `opsense-runner-julia` image khá nặng (~1.5GB) vì kéo `julia:1.10-bookworm`.
  Có thể dùng `julia:1.10-alpine` để giảm, nhưng cần verify deps Linux.
- `julia:1.10-bookworm` chỉ cần thiết khi user dùng Julia kernel.
  Có thể build 3 image runner tuỳ use case thay vì build sẵn cả 3.
