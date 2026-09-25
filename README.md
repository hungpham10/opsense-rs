# Opsense

**Opsense** là engine thu thập + phân tích metric cho SRE. Nó kéo dữ liệu từ nguồn
tuỳ ý (Prometheus, VictoriaMetrics, Binance, API nội bộ…), chạy qua pipeline
transform viết bằng **Rhai**, ghi kết quả vào **station riêng của từng node**, rồi
cho phép đọc lại và sửa cấu hình qua **CLI / REPL / MCP**.

Trên nền tảng đó là một **kernel giao dịch** (`opsense-qlib`): chiến lược được
khai báo bằng script Rhai (`fn rebuild`) hoặc bằng DAG genome (`ops` + `nodes`)
chạy qua ONNX; lệnh và cursor T+N đều là observation trong station nên **sống qua
restart**.

```text
nguồn (HTTP/WS/CSV/clock) ─▶ transform (Rhai) ─▶ station (nguồn sự thật state)
                                                     │
                          đọc: CLI / REPL / MCP ◀────┘      ghi: sink (station / parquet / S3)
```

- Kiến trúc chi tiết: [`docs/architecture.md`](docs/architecture.md)
- Hướng dẫn dùng: [`docs/GUIDE.md`](docs/GUIDE.md)
- Trading: [`docs/TRADING_CORE_ANALYSIS.md`](docs/TRADING_CORE_ANALYSIS.md)
- Script Rhai: [`docs/RHAI.md`](docs/RHAI.md)

## Build

```bash
cargo build --release          # binary: ./target/release/opsense
cargo test --workspace         # toàn bộ test suite
```

## Quickstart

```bash
# 1) Sinh config mẫu (mặc định .opsense/config.toml; không ghi đè nếu đã có)
./target/release/opsense init

# 2) Kiểm tra config trước khi chạy (parse + dựng graph, bắt lỗi component type)
./target/release/opsense validate

# 3) Chạy gateway
OPSENSE_CONFIG=.opsense/config.toml ./target/release/opsense serve
```

### Thử ngay với Prometheus demo (không cần Postgres/S3)

```bash
OPSENSE_CONFIG=examples/prometheus-demo/config.toml ./target/release/opsense serve
# → curl http://127.0.0.1:8080/health   → {"ok":true}
# → đọc dữ liệu:  ./target/release/opsense query prom-explore
```

> `serve` cần một Postgres (hoặc SQLite) cho lớp auth/admin — đặt `DB_DSN` trỏ
> vào Postgres của compose, hoặc `sqlite://opsense.db` cho thử local. Pipeline
> metric không phụ thuộc DB; chỉ phần token/session/OAuth cần.

### Trading realtime (Binance)

```bash
# config + script mẫu: strategies/binance/{config.toml,grid.rhai}
#   params.mode = "analysis"  → dựng grid + transition, phát snapshot
#   params.mode = "trading"   → thêm đặt lệnh qua kernel (T+N, cursor trong station)
OPSENSE_CONFIG=strategies/binance/config.toml ./target/release/opsense serve
./target/release/opsense orders grid --status open     # lệnh đang mở
./target/release/opsense query grid --label-kind trading_step   # cursor T+N
```

## CLI

| Lệnh | Việc |
|---|---|
| `opsense init [path] [--force]` | Sinh config mẫu (không ghi đè nếu đã tồn tại) |
| `opsense serve` | Gateway: pipeline runtime + HTTP/GraphQL |
| `opsense validate [--config P]` | Parse + dựng graph, không chạy |
| `opsense status` | Topology node + danh sách station (1 round-trip) |
| `opsense components [id]` | Cấu hình **đang chạy** của node (kể cả `params` của script) |
| `opsense get-param <node> <ptr>` | Đọc một trường (JSON pointer, vd `/params/sl_pct`) |
| `opsense set-param <node> <ptr> <json>` | **Sửa một trường** — không cần gửi lại cả pipeline |
| `opsense query <node> [--limit --signal --label-kind]` | Đọc observation của station (có guard, có filter server-side) |
| `opsense orders <node> [--status open\|closed]` | Lệnh giao dịch + cursor T+N trong station |
| `opsense repl [--runner URL]` | REPL tương tác (`:status`, `:query`, `:node`, `:attr`) |
| `opsense mcp` | MCP stdio server (client mỏng, nói chuyện với `serve` qua GraphQL) |
| `opsense runner [bind] [--health-check]` | Kernel runner gRPC độc lập (echo/python/julia) |

Cờ `serve --repl` / `serve --mcp` **không còn tồn tại** — dùng subcommand riêng.

## HTTP surface

| Đường dẫn | Method | Nội dung |
|---|---|---|
| `/health` | GET | liveness |
| `/api/repl/graphql` | POST | GraphQL: `status`, `components`, `attributes`, `queryTimeseries`; mutation `reload`, `patchComponent`, `setAttribute`, `removeAttribute` |
| `/api/admin/*` | — | token/user management (qua Nginx + OIDC) |
| `/api/oauth/*` | — | device flow, token, session, metrics OAuth |

Không có `/reload`, `/sources`, `/metrics` hay `/mcp`: đổi cấu hình qua GraphQL
(`patchComponent`/`reload`) hoặc `opsense set-param`; MCP chỉ qua stdio.

## MCP

`opsense mcp` là **client mỏng**: mỗi tool = 1 GraphQL round-trip tới `serve`
đang chạy (`OPSENSE_GRAPHQL_URL`, mặc định `http://127.0.0.1:8080/graphql`).

| Tool | API |
|---|---|
| `opsense_status()` | `Query.status` |
| `opsense_get_config({id?})` | `Query.components` — **đọc trước khi sửa** |
| `opsense_attributes()` / `opsense_set_attribute` / `opsense_remove_attribute` | attributes |
| `opsense_query_timeseries({node, from_ts?, to_ts?, limit?, signal?, label_kind?})` | `Query.queryTimeseries` (có guard) |
| `opsense_orders({node, status?})` | state giao dịch: lệnh + cursor T+N |
| `opsense_set_param({id, path, value})` | `Mutation.patchComponent` — sửa 1 param |
| `opsense_reload({components_json})` | `Mutation.reload` — thay **toàn bộ** danh sách node |

## Lưu trữ

`[storage] backend = "memory" | "parquet" | "sqlite"` (tên cũ `duckdb`/`s3`/
`lakehouse` vẫn nhận nhưng đã deprecated).

- `memory` — thuần RAM, không tốn disk; mặc định, hợp test/demo.
- `parquet` — lake time-partitioned: `<data_dir>/<id>-<kind>/` gồm `wal.log`
  (crash-safe), checkpoint state (`tables/*.parquet` + `_current`) và timeseries
  `ts/blk=<block_id>/batch-*.parquet` (Spark/DuckDB/Polars đọc thẳng).
- Mirror S3-compatible qua `[storage.s3]` (bucket/prefix/endpoint/url_style) →
  `s3://<bucket>/<prefix>/<id>/ts/**`; lịch sync là
  `s3_flush_interval_secs` / `s3_snapshot_interval_secs` ở `[storage]`; creds
  fallback `OPSENSE_S3_*` rồi `AWS_*`.
- Object store cho local dev: **RustFS** (`docker compose up -d rustfs rustfs-bucket`).

## Biến môi trường

| Biến | Ý nghĩa |
|---|---|
| `OPSENSE_CONFIG` | Đường dẫn config (mặc định `.opsense/config.toml`, rồi `conf/opsense.conf.toml`) |
| `GATEWAY_LISTENER`, `GATEWAY_ADDR` | `unix` (mặc định) hoặc `http` + địa chỉ |
| `OPSENSE_GRAPHQL_URL` | Endpoint mà CLI/MCP/REPL dùng |
| `OPSENSE_RHAI_TIMEOUT_SECS` | Ngân sách wall-clock cho mỗi lần chạy script (mặc định 30) |
| `OPSENSE_ATTR_<NAME>` | Ghi đè/inject attribute từ môi trường |
| `DB_DSN`, `MASTER_KEY`, `REDIS_HOST`, `REDIS_PORT` | Auth/admin/OAuth (cần cho `serve`) |
| `OPSENSE_S3_ACCESS_KEY_ID`, `OPSENSE_S3_SECRET_ACCESS_KEY`, `OPSENSE_S3_REGION` | Creds cho mirror S3 |
| `OPSENSE_RUNNER_BIND`, `OPSENSE_KERNEL`, `OPSENSE_SERVE_URL`, `OPSENSE_ADMIN_TOKEN` | Kernel runner |

## Kernel & runner

Execution phân tích nặng (Python/Julia) chạy ở process riêng; gateway chỉ giữ
state.

```bash
export OPSENSE_KERNEL=./target/release/opsense-kernel-python      # hoặc -julia / -echo
opsense repl --runner 127.0.0.1:50051   # REPL nói chuyện runner qua gRPC
opsense runner 127.0.0.1:50051          # hoặc tự chạy runner
```

Python kernel cần: `numpy pandas pyarrow scipy scikit-learn statsmodels
matplotlib protobuf`.

## Repo layout

| Crate | Vai trò |
|---|---|
| `opsense` | Binary: `serve`, CLI dạng script, REPL, MCP (client), runner |
| `opsense-core` | `Config`, `Context`, `Observation`, station (memory/parquet/sqlite), S3 mirror |
| `opsense-components` | Component thu thập: `http_source`, `csv_source`, `telegram_sink`, … |
| `opsense-rhai` | Runtime script: `RhaiTransform`, `station_*`/`attr`/`trigger` binding, `portfolio_feed`, `ScriptStrategy` |
| `opsense-qlib` | Kernel giao dịch: `Portfolio`, `Session`, `TradingGrid`, `Graph` (DAG→ONNX), `plan` |
| `opsense-mlib` | Thư viện nền: vector runtime, storage backends, grid/transition, SGD |
| `opsense-model` | DB entities, resolver, secret, admin/OAuth |
| `opsense-proto` | Schema protobuf cho IPC/gRPC |
| `opsense-runner` | Service `KernelRunner` (gRPC ↔ kernel IPC) |
| `opsense-kernel-echo` / `-python` / `-julia` | Kernel tham chiếu / sidecar |
| `opsense-macros` | Proc-macro: `#[source]`, `#[transform]`, `#[rhai_class]`, … |

Thư mục khác: `strategies/` (config + script chạy thật), `examples/`,
`conf/`, `docs/`.
