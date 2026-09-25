# Opsense — Kiến trúc

> Mô tả **kiến trúc đang chạy**. Mọi khẳng định kèm đường dẫn file để tự kiểm
> chứng. Hướng dẫn dùng: [`GUIDE.md`](./GUIDE.md). Tổng quan: [`../README.md`](../README.md).

## 1. Bức tranh tổng thể

Opsense là **gateway + runtime pipeline**, không phải monolith:

```
        ┌──────────────────────── opsense serve (tiến trình dài hạn) ───────────────────────┐
        │  axum router: /health · /api/repl/graphql · /api/admin/* · /api/oauth/*          │
        │                                                                                  │
        │  AppState ─┬─ Context  (stations, attributes, storage)                          │
        │            ├─ Runtime  (vector DAG: source → transform → sink)                  │
        │            ├─ Resolver (DB: entities/admin/oauth)                                │
        │            └─ S3 client / Secret / OAuth metrics                                  │
        └───────▲───────────────────────────────────────────────┬─────────────────────────┘
                │ 1 GraphQL round-trip / lệnh CLI                │ framed IPC (protobuf+Arrow)
   ┌────────────┴─────────────┐                      ┌───────────▼───────────────┐
   │ opsense mcp (stdio)      │                      │ opsense-kernel-*          │
   │ opsense status/components│                      │ echo / python / julia     │
   │ opsense query/orders …   │                      └───────────────────────────┘
   │ opsense repl (tương tác) │                      ┌───────────────────────────┐
   │ curl / script            │                      │ opsense-runner (gRPC)      │
   └──────────────────────────┘                      └───────────────────────────┘
```

Ba điểm cốt lõi:

1. **Station là nguồn sự thật của state.** Mọi thứ component ghi ra — metric, log,
   snapshot, **lệnh giao dịch**, **cursor T+N** — đều là `Observation` trong station
   của node (`opsense-core/src/station.rs`). Không có tầng lưu trữ song song.
2. **MCP và CLI là hai mặt của cùng một API.** Cả hai đều là client mỏng gọi
   `POST /api/repl/graphql` (`crates/opsense/src/mcp/mod.rs:1-12`,
   `crates/opsense/src/cli.rs:1-20`). Không có logic nghiệp vụ nào nằm trong adapter.
3. **Chiến lược là genome, không phải class.** Script `.rhai` định nghĩa
   `fn rebuild(candles, prev, params)`; hoặc DAG genome (`ops` + `nodes`) chạy qua
   ONNX (`crates/opsense-rhai/src/strategy.rs`, `crates/opsense-qlib/src/graph/`).

## 2. Crate map

| Crate | Trách nhiệm |
|---|---|
| `opsense` | Binary: `serve`, CLI dạng script, REPL, MCP (client), kernel runner, `init` |
| `opsense-core` | `Config`, `Context`, `Observation`, `Station` (3 loại), mở storage backend + mirror S3 |
| `opsense-components` | Component thu thập/giữ chỗ: `http_source`, `csv_source`, `telegram_sink`, station transforms |
| `opsense-rhai` | Runtime script: `RhaiTransform`, binding `station_*`/`attr`/`trigger`, `portfolio_feed`, `ScriptStrategy` |
| `opsense-qlib` | Kernel giao dịch: `Portfolio`, `Session`, `TradingGrid`, `plan` (contract với script), `Graph` (DAG→ONNX) |
| `opsense-mlib` | Nền tảng: vector runtime, storage backends (parquet/sqlite/redis/…), `AnalysisGrid`, `TransitionAnalysis`, SGD |
| `opsense-model` | Entities (sea-orm), `Resolver`, `Secret`, admin/OAuth |
| `opsense-proto` | Schema protobuf cho IPC + gRPC |
| `opsense-runner` | Service `KernelRunner`: gRPC ↔ kernel IPC |
| `opsense-kernel-echo` / `-python` / `-julia` | Kernel tham chiếu / sidecar phân tích |
| `opsense-macros` | Proc-macro: `#[source]`, `#[transform]`, `#[sink]`, `#[rhai_class]`, `#[rhai_func]` |

Dependency hướng: `opsense` → {`opsense-core`, `opsense-components`, `opsense-rhai`,
`opsense-qlib`, `opsense-runner`, `opsense-model`}; `opsense-rhai` → `opsense-qlib`
(để implement `Strategy`). **Không crate nào của runtime depend crate `rhai`** ngoài
`opsense-rhai` — nhờ vậy không có vòng phụ thuộc.

## 3. Tầng 1 — Telemetry pipeline

### 3.1 Vector runtime

`opsense-mlib/src/vector/runtime/engine.rs` điều phối component nối thành DAG.
Config khai bằng `[[pipeline.components]]` trong TOML. `Runtime::reload`
(`engine.rs:630`) **diff trước, validate sau, apply cuối**:

1. so sánh component mới với component đang chạy → `adds` / `diffs` / `dels`;
2. validate: node mới, node đổi, node bị xoá, và **chu trình** (`engine.rs:673-676`);
3. mới apply (`add_new_nodes` → `update_changing_nodes` → `remove_oudated_nodes`).

Nhờ thứ tự này, `Mutation.patchComponent` (đọc config hiện tại → patch 1 trường →
deserialize **toàn bộ** danh sách qua typetag → reload) không bao giờ để lại pipeline
nửa vời: lỗi chết ở bước 2.

Component type = tên struct ở dạng snake_case do macro sinh
(`opsense-macros/src/configurable_component.rs:16-41`). Danh sách đúng **18 loại**
(binary tự liệt kê khi parse sai — ví dụ `opsense validate` với type sai):

| Nhóm | Type |
|---|---|
| source | `clock`, `input`, `http_source`, `csv_source` |
| transform | `json_2_json`, `websocket_2_json`, `rhai_transform`, `processor_transform`, `timeseries_station_transform`, `category_station_transform`, `pattern_station_transform` |
| sink | `timeseries_station_sink`, `capture_sink`, `file_sink`, `print`, `telegram_sink`, `null` |
| khác | `output` (không dùng trong config pipeline) |

Component có `station = true` tự đăng ký station theo `id` → truy vấn được; node
transform không phải sink thì **bắt buộc** có consumer (thường là `null`).

### 3.2 Station

`Station` có 3 loại (`opsense-core/src/station.rs:675-679`): `Timeseries`,
`Category`, `Pattern`; mỗi loại bọc `Arc<RwLock<…>>` để tra cứu qua `Context` không
cần copy.

`TimeseriesStation` chia theo **block thời gian** (`block_duration_secs`), `query_range`
đọc từ cache block; `update_range` ghi một loạt observation vào các block tương ứng.
Đó là cơ chế mà cả `timeseries_station_sink` lẫn audit log đều dùng.

### 3.3 Storage

`[storage] backend` (`opsense-core/src/config.rs:197-201`): `memory` (mặc định),
`parquet`, `sqlite`; tên cũ `duckdb`/`s3`/`lakehouse` vẫn nhận nhưng deprecated
(`station.rs:121-191`).

- **parquet** (`opsense-mlib/src/storage/parquet.rs`): mỗi station một thư mục
  `<data_dir>/<id>-<kind>/` gồm `wal.log` (crash-safe), checkpoint state
  (`tables/*.parquet` + `_current`), và timeseries time-partitioned
  `ts/blk=<block_id>/batch-*.parquet` (Spark/DuckDB/Polars đọc thẳng, prune theo `blk=`).
- **Mirror S3-compatible**: `[storage.s3]` (bucket/prefix/endpoint/url_style) →
  `s3://<bucket>/<prefix>/<id>/ts/**`; creds không khai trong config thì fallback
  `OPSENSE_S3_ACCESS_KEY_ID` / `OPSENSE_S3_SECRET_ACCESS_KEY` / `OPSENSE_S3_REGION`
  rồi `AWS_*` (`station.rs:131-155`). Lịch sync là `s3_flush_interval_secs` /
  `s3_snapshot_interval_secs` ở `[storage]` (**không** phải trong `[storage.s3]`).
- Local dev: `docker compose up -d rustfs rustfs-bucket` (RustFS thay MinIO vì
  registry chặn image của org `minio`).

## 4. Script Rhai

`opsense-rhai` compile script 1 lần, cache theo fingerprint (content hash cho
inline, mtime cho file) — `runtime.rs:95-127`; mỗi blocking thread có engine riêng
(`thread_local`) nên không phải khóa toàn cục.

Script `process(observations)` nhận mảng map, trả mảng map. Binding mỗi call:

| Binding | Vai trò |
|---|---|
| `station_query` / `station_candles` | đọc station khác (đây là cách script giữ state qua các call) |
| `attr` / `attrs` | attribute của config |
| `trigger()` | `payload.trigger`/`src` để branch theo input edge |
| `param_<name>` | params của node, từ `[pipeline.components.params]` |
| `ts_*`, `grid_fit*`, `transition_analysis` | toán họp dùng sẵn (native, không tự viết lại trong script) |
| `portfolio_feed` | chạy kernel giao dịch cho 1 nến vừa đóng |

Ngân sách wall-clock mỗi lần chạy: `OPSENSE_RHAI_TIMEOUT_SECS` (mặc định 30),
cưỡng chế bằng `on_progress` + timeout bao ngoài.

### 4.1 Strategy = script (`fn rebuild`)

`strategy = "rhai"` ⇒ `ScriptStrategy` (`crates/opsense-rhai/src/strategy.rs`) là
một `Strategy` của qlib, gọi `fn rebuild(candles, prev, params)` trong **chính script
của node** (lấy qua `runtime::current_script()`, nên không phải khai đường dẫn thêm
ở params và không thể lệch hai bản).

Hai chi tiết kiến trúc đáng chú ý:

- **Engine thứ hai**: `portfolio_feed` chạy *bên trong* `Engine::call_fn` của
  `process`, mà engine chính đang `with_borrow_mut` → không thể reentrant. Nên
  strategy có `thread_local` engine riêng, chỉ đăng ký binding read-only
  (`tools::register_strategy_tools`, không có `portfolio_feed`) để không đệ quy.
  AST cache dùng chung nên không compile lại.
- **Plan là dữ liệu**: script không dựng được `TradingGrid` (struct Rust field
  private), nên trả JSON theo hợp đồng `opsense_qlib::plan::GridPlan`. Kernel dựng
  lại grid và **chép bộ đếm win/lost của plan cũ** (`GridPlan::to_grids`) — bộ đếm
  là trí nhớ kernel, không chép thì win-prob "học từ lịch sử" bị reset mỗi lần
  rebuild.

### 4.2 Strategy = DAG genome (`strategy = "dag"`)

`Graph` (`crates/opsense-qlib/src/graph/`) là `Strategy` thứ hai: `ops` + `nodes` +
`weights` trong JSON, tự emit **ONNX** rồi chạy bằng tract. Genome là nguồn sự thật;
file `.onnx` chỉ là artifact (`write_onnx` / `load_artifact` có fingerprint để lệch
bytes↔genome thì báo lỗi). Cầu nối cho model build ngoài (Python).

## 5. Kernel giao dịch (`opsense-qlib`)

| Kiểu | Vai trò |
|---|---|
| `Portfolio` | Thực thi: rebuild plan → check exit → evaluate entry → T+N → notify sự kiện. Cùng một kernel cho backtest và realtime. |
| `Session` | Run-state: orders/history/plan + cursor (`review_at`, `candle_seq`, `candle_id`, `candle_ts`) |
| `TradingGrid` | Một lưới lệnh (levels, sl, weights, win-prob, bộ đếm win/lost) |
| `plan` | Hợp đồng serde giữa script ↔ kernel |
| `Graph` | DAG genome → ONNX → win-prob |

`Portfolio::evaluate` nhận `FetchFn` (hỏi nến bất kỳ lúc nào/range bất kỳ) nên
backtest đọc loader/cache còn realtime đọc station — chỉ khác chỗ lấy nến. Realtime
chạy qua native `portfolio_feed` (`crates/opsense-rhai/src/orders.rs`): tái dựng
session từ observation, gọi evaluate cho **đúng 1 nến vừa đóng**, rồi trả về lệnh
mới + cursor.

Vòng đời lệnh (append-only, sống qua restart):

- mở: `signal = "order"`, `labels.status = "open"`, kèm `order_id`/`grid`/`level`/`size`/`sl`/`tp`;
- đóng: `labels.status = "closed"`, `pnl_pct`;
- cursor: `labels.kind = "trading_step"`, `candle_seq` đơn điệu để T+N chặn và
  idempotent sau restart.

Rebuild fail (thiếu nến) vẫn tiến `review_at` rồi trả lỗi để realtime không retry mỗi
nến; lần sau thử lại với nến mới.

## 6. HTTP surface

Router (`crates/opsense/src/serve.rs:107-115`):

| Đường dẫn | Nội dung |
|---|---|
| `GET /health` | liveness |
| `POST /api/repl/graphql` | GraphQL Tầng 1 |
| `/api/admin/*` | token/user management |
| `/api/oauth/*` | device flow, token, session, `/api/oauth/metrics/oauth` |

Không có `/reload`, `/sources`, `/metrics`, `/mcp` — đổi cấu hình qua GraphQL, MCP qua
stdio.

## 7. GraphQL (`crates/opsense/src/api/repl/v1.rs`)

```graphql
type Query {
  status: Status                    # nodes {id type inputs} + stations {id kind}
  components(id: String): [ComponentConfig!]   # cấu hình ĐANG CHẠY (typetag JSON)
  attributes: BTreeMap!             # attribute trong memory
  queryTimeseries(node: String!, fromTs: Int, toTs: Int,
                  limit: Int = 1000, signal: String, labelKind: String): QueryResult!
}

type Mutation {
  reload(components: [ComponentInput!]!): EditResult!   # thay TOÀN BỘ danh sách node
  patchComponent(id: String!, path: String!, value: String!): EditResult!  # sửa 1 trường
  setAttribute(name: String!, value: String!): SetAttributeResult!
  removeAttribute(name: String!): Boolean!
}

type QueryResult { observations: [Observation!]!, truncated: Boolean!, scanned: Int! }
```

Guard bắt buộc của `queryTimeseries`: `limit` mặc định 1000, trần 10 000; cửa sổ trần
30 ngày; tính độ rộng bằng `checked_sub` để `from=i64::MIN, to=i64::MAX` không **tràn
số** (build release wrap thành số âm ⇒ guard im lặng bị bỏ qua — đã có test). Vượt
trần thì **từ chối kèm gợi ý**, không clamp im lặng.

`patchComponent` nhận `path` là JSON pointer vào config đang chạy (`/params/sl_pct`):
đọc config hiện tại → patch → deserialize toàn bộ danh sách → reload. Sau khi thành
công, ghi 1 observation vào station `opsense-audit` (`labels.kind = "config_edit"`,
kèm `node`/`path`/`from`/`to`) để đọc lại bằng chính đường query:
`opsense query opsense-audit --label-kind config_edit`.

## 8. Client: MCP, CLI, REPL

| Công cụ | Bản chất |
|---|---|
| `opsense mcp` | rmcp stdio server; 9 tool, mỗi tool = 1 round-trip (`mcp/server.rs`) |
| `opsense status/components/query/orders/get-param/set-param` | clap subcommand, cùng `OpsenseClient` |
| `opsense repl` | REPL tương tác (`:status`, `:attr`, `:node`, `:query`, `:login`); `--runner` để nói thẳng với runner qua gRPC |

## 9. Kernel runner & IPC

```
opsense-runner (gRPC :50051) ──Ed25519 sig mỗi RPC──▶ session
        │ framed IPC (protobuf control + Arrow data)
        ▼
opsense-kernel-python | -julia | -echo
```

`opsense runner [bind] [--health-check]` chạy độc lập (container riêng); REPL có thể
bỏ qua host và nói thẳng runner bằng `--runner <endpoint>`. Cấu hình runner đọc từ
`~/.config/opsense/runner.json` (`OPSENSE_RUNNER_CONFIG`), rồi env
(`OPSENSE_RUNNER_BIND`, `OPSENSE_KERNEL`, `OPSENSE_SERVE_URL`, `OPSENSE_ADMIN_TOKEN`),
rồi cờ CLI. Python kernel cần `numpy pandas pyarrow scipy scikit-learn statsmodels
matplotlib protobuf`.

## 10. Luồng dữ liệu ví dụ

### Prometheus (`strategies/prometheus/config.toml`)

```
clock(10s) → http_source(prom-explore, station) → rhai_transform(stats-summary)
           → timeseries_station_sink(tsdb) → parquet local + mirror S3
```

### Binance trading (`strategies/binance/config.toml`)

```
clock ─┬→ http_source(klines) ──┐
       └→ websocket_2_json(aggTrade) → json_2_json ─┴→ rhai_transform(grid)
                                                        ├─ trigger()=="tick": gộp nến live
                                                        └─ mode=="trading": portfolio_feed(...)
                                                              → observation order + cursor
                                                                (trong station `grid`)
```

Chiến lược do `fn rebuild` trong `grid.rhai` dựng; `params.mode` chọn giữa phân tích
và đặt lệnh. Kernel chỉ thực thi, không giữ state: state nằm ở station.

## 11. Những thứ đã bị gỡ (đừng tìm trong code)

- `opsense-qlib/src/strategies/` (`GridStrategy`, `VolatilityAdaptiveGridStrategy`) —
  thay bằng `ScriptStrategy` + `Graph`.
- `streaming.rs`, `QlibEngine`, `StreamingPortfolio` — realtime dùng kernel `evaluate`.
- Tầng lưu trữ observation chung (`ObservationStore` riêng, `persist_sink`) — station
  là tầng duy nhất.
- `opsense-store`, `opsense-mcp`, `opsense-libs` (crate) — hợp nhất vào
  `opsense-core` / `opsense` / `opsense-mlib`.
- Cờ `serve --repl`, `serve --mcp`, `serve --runner-bind` — dùng subcommand.
- MCP server nhúng trong `serve`, `opsense_init/deinit/run/backfill/describe` — MCP giờ
  là client mỏng của GraphQL.
