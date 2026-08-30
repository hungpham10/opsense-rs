# Opsense — Kiến trúc

Tài liệu này mô tả hai tầng chính của hệ thống và đường ranh giới giữa chúng.
Hướng dẫn sử dụng: [`GUIDE.md`](./GUIDE.md); trạng thái thực thi:
[`PLAN.MD`](./PLAN.MD); audit checklist: [`CHECKLIST.MD`](./CHECKLIST.MD).

## 1. Bức tranh tổng thể

```
                        ┌────────────── opsense serve ───────────────┐
                        │ serve.rs sở hữu MỌI transport serving      │
 MCP client ──stdio────▶│ routes(): /health /reload /sources         │
 MCP client ──HTTP─────▶│   ▲ mcp_handler() từ opsense-mcp          │
 curl/monitor ─────────▶│   ▲ Repl:    opsense-repl (reedline)      │
                        │ extras : KernelRunner gRPC (--runner-bind) │
                        └───────┬─────────────────┬──────────────────┘
                                │                 │
              ┌─────────────────▼──────┐   ┌──────▼────────────────┐
              │ TẦNG 1 — TELEMETRY     │   │ TẦNG 2 — ANALYSIS     │
              │ vector Runtime +       │   │ SESSION               │
              │ node-owned stations    │   │ Session → KernelBackend│
              │ (pipeline, Rhai)       │   │ (framed IPC / gRPC)    │
              └────────────────────────┘   └────────────────────────┘
```

Hai tầng **không phụ thuộc lẫn nhau**: pipeline telemetry chạy được mà không có
session; analysis session chạy được mà không cần pipeline.

## 2. Tầng 1 — Telemetry pipeline

Engine dataflow kiểu vector (`opsense_libs::vector::Runtime`) điều phối các
component nối thành DAG. Config hoàn toàn bằng TOML
(`[[pipeline.components]]`), sửa nóng qua MCP `opsense_edit` (runtime tự diff,
validate liên kết + chu trình DFS trước khi apply).

### Crate bố trí

| Crate | Trách nhiệm |
|---|---|
| `opsense-libs` | LRU sharded, jq, Aho-Corasick, Radix + KMP, vector runtime (subsystem) |
| `opsense-core` | Domain: `Config`, `Station` (3 loại), `Watermarks`, `Collector`, `template`, `script` trait |
| `opsense-components` | Component chuẩn: clock, ingest, processor, http, collector, station (3 loại) |
| `opsense-rhai` | Rhai sandbox + `RhaiTransform` component + `register_ts_ops` |
| `opsense-store` | _Đã được gộp lại_: store Parquet/DuckDB/S3/LMDB + `CacheStore` nằm trong `opsense-core`/`opsense-libs`; không còn crate riêng |

### Component và quy ước tên

Mọi `type` kết thúc bằng hậu tố vai trò (macro `#[source]/#[transform]/#[sink]`
trong `opsense-macros` sinh từ tên struct, không lệch được):

| Hậu tố | Type hiện có | Vai trò |
|---|---|---|
| `_source` | `clock_source`, `ingest_source` | Đưa dữ liệu/tín hiệu vào pipeline |
| `_transform` | `http_source`¹, `processor_transform`, `rhai_transform`, `pattern_station_transform`, `category_station_transform`, `timeseries_station_transform` | Xử lý observations |
| `_sink` | `collector_sink`, `timeseries_station_sink` | Đích cuối: HTTP query / đẩy ra collector |

¹ `http_source` dù chứa `_source` về tên gốc, hiện được `#[transform]`-hoá —
đứng sau `clock_source` trong graph. Đuôi `_source` ở đây nói về **vai trò dữ
liệu** (nguồn fetch từ bên ngoài), không phải vị trí graph; `opsense_status`
vẫn báo đúng vị trí.

### Tín hiệu, cursor & watermark

Các node trao đổi `Message` qua kênh mpsc bounded 1024 (backpressure). Signal
mang timestamp: `tick(ts) → data_ready(ts) → processed(ts)` (xem
`opsense-components::signal`). Mỗi node giữ cursor riêng theo tên
(`Watermarks::get_node / set_node`, cửa sổ nửa mở `(from_ts, to_ts]`) — lỗi
fetch chỉ giữ cursor, cửa sổ retry ở tick sau (không mất, không nhân đôi).
Cursor sống sót qua restart qua journal `watermarks.json` (atomic
tmp+rename, ghi khi cursor tiến).

### Mỗi node sở hữu station riêng

**Tầng persistence chung đã bị bỏ.** Mỗi node publish dữ liệu vào **station
riêng** đăng ký theo `id` trong `OpsenseContext::stations` (registry toàn cục).
`Station` enum có ba biến thể (xem `opsense-core::station`):

- `Timeseries(TimeseriesStation)` — `LruCache<(Stage, String), BTreeMap<ts, Obs>, 16>`
  keyed theo `(stage, metric)`, value là `BTreeMap<ts, Observation>` để
  range-scan nhanh. Evict theo entry (nghĩa là theo `(stage, metric)`).
- `Category(CategoryStation)` — `Search<u8>` (Radix + KMP) + index key/value
  cho phép substring search + trả về `(key, value)`.
- `Pattern(PatternStation)` — Aho-Corasick multi-pattern matcher kèm bộ đếm
  hit/miss.

Consumer (Rhai, MCP, REPL, các transform khác) đọc qua registry: mọi
`ObservationStore` trait cũ gộp thành API async trực tiếp trên `Station` —
`append`/`query`/`query_all`. `ts_query(id, stage, metric, from, to)` của Rhai
chính là `station(id).read().await.query(stage, metric, from, to)`.

> Lịch sử: trước refactor 2026-08-26 còn `persist_sink` đẩy xuống
> `ObservationStore` (memory/DuckDB/LMDB/S3) dùng chung; sau refactor mỗi node
> giữ data của mình, working LRU dùng chung biến mất. `station_sink` cũ đổi
> tên thành `timeseries_station_sink` (chỉ giữ hình thái cache + endpoint HTTP,
> không còn cold LMDB tầng station — LMDB giờ chỉ làm store trong
> `[storage]` cũ đã dời sang phase khác).

### Scripting Rhai

`rhai_transform` chạy script Rhai sandboxed (max_ops, timeout riêng, không
fs/network). Hợp đồng: `fn process(observations)` nhận array map, trả
observation mới. Toán tử time-series có sẵn (`register_ts_ops`):
`ts_rate`, `ts_moving_avg`, `ts_resample`, `ts_quantile`, `ts_p95`/`ts_p99`,
`ts_delta`, `ts_pct_change`; đọc lịch sử qua `ts_query`/`ts_mean` (lookup
registry theo id).

### Cấu hình TOML (`opsense-core::config::Config`)

| Mục | Schema | Mục đích |
|---|---|---|
| `[engine]` | `poll_interval_seconds`, `cache_block_seconds`, `cache_max_blocks`, `python_path`, `python_packages` | Chu kỳ default + Python kernel |
| `[capacity]` | `metric_name = float` | max capacity cho từng metric (cores, GB, req/s…) |
| `[attributes]` | `key = "value"` | template variables cho `http_source` (override bằng `OPSENSE_ATTR_<NAME>`) |
| `[sources.vector]` | `url`, `jq_filter?`, `metrics?` | pull model legacy (qua `ingest_source` → `Collector`) |
| `[storage]` | `backend` (memory/duckdb/lmdb/s3), `data_dir`, `block_secs`, `retention_secs`, `parquet_compression`, `mirror?`, `s3?` | chỉ dùng cho tầng lakehouse nếu còn path ghi |
| `[session]` | `max_memory_mb`, `max_cpu_time_secs`, `max_execution_time_secs`, `max_result_rows`, `idle_timeout_secs`, `allow_fs`, `allow_net` | giới hạn analysis session |
| `[repl]` | `history_file`, `max_history`, `completion`, `default_station` | cấu hình REPL shell |
| `[[pipeline.components]]` | bảng typetag — `type`, `id`, các field tuỳ component | DAG node |

### Kiểm chứng runtime

- `Runtime::reload()` chạy **DFS ba-màu** phát hiện chu trình `a → b → a` (lỗi
  kèm chuỗi liên kết).
- Backpressure: kênh mpsc bound 1024, `.await` khi đầy; counter
  `channel_full_waits` (log `warn` nếu >100ms).
- Backfill: `opsense_backfill({node, from_ts, to_ts})` ép một node
  `http_source` re-fetch đúng cửa sổ `(F, T]`. Watermark KHÔNG lùi.

## 3. TẦNG 2 — Analysis session & runner

Nguyên tắc (từ checklist IPC+gRPC Runner): **IPC = giao tiếp execution,
gRPC = boundary serve↔runner. Kernel không biết gRPC/MCP tồn tại.**

### Một schema, mọi boundary (`opsense-proto`)

`proto/opsense.proto` là nguồn chân lý duy nhất: service gRPC `KernelRunner`
cho serve↔runner, message `Envelope` cho framed protocol host↔kernel.
Frame layout: `[tag u8][len u32 BE][payload]` — tag `CONTROL` (protobuf
Envelope) hoặc `ARROW` (một Arrow IPC stream segment). Dataset truyền dạng
chunk ~64k rows/frame, data đi trước header làm terminator.

```
REPL/MCP ──> Session ──> trait KernelBackend
                           ├── LocalIpcBackend ──(framed stdio)── kernel process
                           └── GrpcRunnerBackend ──(gRPC)── opsense-runner ──(cùng framed stdio)── kernel process
```

- **`KernelBackend` trait**: `start_session / execute / send_dataset /
  interrupt / close_session / shutdown / health`. Hai implementation hoán đổi
  cho nhau tại runtime (`:kernel local` / `:runner connect`).
- **`LocalIpcBackend`**: mỗi session = một kernel process (isolation mạnh);
  handshake version, timeout từng op host-side, kill-on-drop, crash của kernel
  chỉ hạ đúng session đó.
- **`GrpcRunnerBackend`**: client tonic drop-in; runner (`opsense-runner`)
  chỉ dịch gRPC ↔ cùng framed protocol — kernel phía sau không đổi gì.
  Message limit 256MB hai đầu cho dataset lớn.

### Kernel đa ngôn ngữ

Kernel chỉ cần nói framed protocol: `opsense-kernel-python` (CPython thật:
pandas/scipy/sklearn/matplotlib; interrupt bằng SIGINT; sandbox fs/net/rlimit;
pb2 vendored sinh từ chính proto), `opsense-kernel-julia` (Arrow.jl qua Julia
subprocess), `opsense-kernel-echo` (Rust, fixture test/dev). Chọn qua
`OPSENSE_KERNEL_BIN`. Variable namespace nằm **phía host** (`SessionState` lưu
Arrow base64) nên dataset được push mỗi lần execute — đổi kernel/backend giữa
chừng không mất state.

### `serve.rs` — entry phục vụ duy nhất

`opsense serve` chấp nhận cùng lúc nhiều transport: `--repl` (REPL tương
tác), `--mcp` (MCP stdio), `--mcp --http` (MCP Streamable HTTP), `--runner-bind
ADDR` (gRPC KernelRunner). `routes()` render mọi HTTP path; MCP chỉ cung cấp
handler handle (`opsense_mcp::mcp_handler()`), runner chỉ cung cấp service
handle (`kernel_runner_service(cfg)`). Terminal mode (REPL/MCP stdio) chiếm
stdin trên blocking thread trong khi gateway sống nền.

Env: `OPSENSE_RUNNER_BIND` (default `127.0.0.1:50051`), `OPSENSE_MCP_PORT`,
`OPSENSE_KERNEL_BIN`. Subcommand alias: `opsense repl` ≡ `opsense serve --repl`,
`opsense runner` chạy runner độc lập, `opsense mcp` ≡ `serve --mcp[--http]`.

### Resilience

Lỗi giữ nguyên root cause (taxonomy ipc/grpc/kernel/dataset/timeout/cancel);
timeout host-side bắn interrupt rồi báo `timed_out`; kernel bị kill -9 → EOF
sạch, host sống; benchmark roundtrip: local-IPC ~0.4ms, qua gRPC ~1.5ms
(`cargo test -p opsense-session --test latency_bench -- --ignored --nocapture`).

## 4. Luồng dữ liệu ví dụ

**Thu thập Prometheus** (xem `examples/prometheus-demo/`):
`clock_source(20s) → http_source(query_range, script map Rhai)` — mỗi chu kỳ
chỉ hỏi delta kể từ cursor; observations đổ vào station riêng của node
`http_source` (bật `station = true` thì có cả endpoint Prometheus-style ở
`bind`). Không còn `persist_sink` trong graph.

**Phân tích tương tác**: REPL `:query` lấy observation từ station → push
dataset vào kernel qua ARROW frames → `:py` chạy pandas → `result` DataFrame
quay về host dưới Arrow → `@N` trong namespace, vẽ biểu đồ bằng matplotlib
PNG artifact.

## 5. Giới hạn đã biết

- Remote runner qua mạng (TLS/auth/discovery/quota) chưa làm — design không
  chặn (`GrpcRunnerBackend` nhận addr từ config).
- Python kernel yêu cầu `protobuf` runtime (bắt buộc cho wire protocol).
- Windows chưa hỗ trợ (framed stdio + unix socket; named pipe là việc sau).
- Tầng persistence chung đã bị gỡ; lakehouse Parquet/S3 chỉ còn làm mirror
  qua `[storage].mirror` cho dòng dữ liệu cũ (post-2026-08-24 retention).
  Mọi đọc query hiện đi qua station registry.
