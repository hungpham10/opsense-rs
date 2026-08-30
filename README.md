# Opsense

**Opsense** là engine phân tích hành vi metric & dung lượng cho SRE: kéo dữ liệu
từ nguồn HTTP tuỳ ý (Prometheus, VictoriaMetrics, API nội bộ…), xử lý qua
pipeline transform viết bằng Rhai, lưu vào lakehouse parquet, và chạy phân tích
nâng cao (pandas/scipy/sklearn) trong **kernel process riêng** nói framed IPC —
với runner gRPC khi cần tách execution khỏi gateway.

- Kiến trúc chi tiết: [`docs/architecture.md`](docs/architecture.md)
- Hướng dẫn sử dụng đầy đủ: [`docs/GUIDE.md`](docs/GUIDE.md)
- Ví dụ chạy với Prometheus demo công khai:
  [`examples/prometheus-demo/`](examples/prometheus-demo/config.toml)

## Build

```bash
cargo build --release      # binary: ./target/release/opsense
cargo test                 # toàn bộ test suite
```

## Quickstart

```bash
# 1) Sinh config mẫu (mặc định .opsense/config.toml)
./target/release/opsense init

# 2) Sửa config: [attributes] + bật một khối [pipeline]
$EDITOR .opsense/config.toml

# 3a) Chạy gateway thường trực (REST + tuỳ chọn MCP/runner gRPC)
OPSENSE_CONFIG=.opsense/config.toml ./target/release/opsense serve

# 3b) Hoặc điều khiển qua MCP từ client của bạn
./target/release/opsense mcp
```

### Thử ngay với Prometheus demo (không cần cài gì)

```bash
OPSENSE_CONFIG=examples/prometheus-demo/config.toml \
GATEWAY_LISTENER=http GATEWAY_ADDR=127.0.0.1:8123 \
./target/release/opsense serve
# → curl http://127.0.0.1:8123/health  → OK
# → parquet đổ vào .opsense/demo-lakehouse/ mỗi 20 giây
```

## `opsense serve` — một entry, mọi transport

| Cờ | Vai trò |
|---|---|
| *(mặc định)* | Pipeline thu thập + REST gateway (`/health`, `/reload`, `/sources`, `/metrics`) |
| `--repl` | REPL phân tích tương tác (chiếm stdin; gateway vẫn chạy nền) |
| `--mcp` | MCP server qua stdio (cho client MCP headless) |
| `--mcp --http` | MCP Streamable HTTP mount tại `<gateway>/mcp` |
| `--runner-bind <host:port>` | Host thêm server gRPC `KernelRunner` |

Env cấu hình: `OPSENSE_CONFIG`, `GATEWAY_LISTENER=unix\|http`,
`GATEWAY_ADDR`, `OPSENSE_RUNNER_BIND`, `OPSENSE_MCP_PORT`,
`OPSENSE_KERNEL_BIN`, `OPSENSE_QUERY_TIMEOUT_SECS`, `OPSENSE_RHAI_TIMEOUT_SECS`.

Các subcommand khác: `opsense runner [bind]` (execution worker độc lập),
`opsense repl`, `opsense mcp [--http]`, `opsense init [path] [--force]`.

## REPL phân tích

```text
opsense> :query up --from 1h          # lấy dữ liệu station -> @1
opsense> :py result = @1['value'].mean()
opsense> :stats describe @1
opsense> :plot @1 --type line --out up.png
opsense> :runner connect 127.0.0.1:50051   # chuyển execution sang runner gRPC
opsense> :kernel local                     # … và quay lại local IPC
```

Toàn bộ lệnh: `:help`. State (`@var`, history) nằm phía host nên **đổi kernel
hay runner không mất dữ liệu phiên**.

## Kernel & runner

Execution chạy trong **process riêng** qua framed IPC (protobuf control + Arrow
data); Python không hề biết gRPC tồn tại.

```bash
export OPSENSE_KERNEL_BIN=./target/release/opsense-kernel-python   # hoặc -kernel-julia (chỉ cần Arrow.jl cho dataset)
# hoặc kernel echo để test không cần Python:
export OPSENSE_KERNEL_BIN=./target/debug/opsense-kernel-echo

./target/release/opsense serve --repl --runner-bind 127.0.0.1:50051
# máy khác:
./target/release/opsense runner 127.0.0.1:50051
```

Python kernel cần: `numpy pandas pyarrow scipy scikit-learn statsmodels
matplotlib protobuf` (`protobuf` bắt buộc cho wire protocol).

## MCP tools

Pipeline: `opsense_init` · `opsense_status` · `opsense_edit` · `opsense_run` ·
`opsense_query` · `opsense_list_stations` · `opsense_describe` ·
`opsense_deinit`.
Kernel: `opsense_kernel_run({code})` · `opsense_kernel_health()`.
Chi tiết: [`docs/GUIDE.md`](docs/GUIDE.md).

## Lưu trữ

`[storage] backend = "duckdb" | "lmdb" | "memory" | "s3"` — lakehouse parquet
chia block (`blk=<start>/batch_NNN.parquet`) có block-pruning khi query;
retention `[storage] retention_secs`; mirror double-write tuỳ chọn.
Trạm timeseries nội bộ (`station_sink`) mở endpoint tương thích Prometheus.

## Repo layout

| Thư mục / crate | Nội dung |
|---|---|
| `crates/opsense` | Binary chính: `serve.rs` sở hữu mọi transport serving |
| `crates/opsense-proto` | `opsense.proto` + frame codec — schema duy nhất cho IPC và gRPC |
| `crates/opsense-session` | `Session`/`SessionManager`, trait `KernelBackend`, backends local-IPC & gRPC |
| `crates/opsense-runner` | Service handle `KernelRunner` (gRPC ↔ kernel IPC) |
| `crates/opsense-kernel-python` / `-julia` / `-echo` | Kernel sidecar Python / Julia (codec protobuf tự viết, không cần package) / kernel tham chiếu Rust |
| `crates/opsense-core` · `-components` · `-rhai` · `-store` · `-mcp` · `-libs` | Pipeline telemetry: sources/transforms/sinks, stores, scripting, MCP tools |
| `examples/` | Config demo chạy thật (Prometheus demo công khai) |
| `scripts/` | Script Rhai mẫu + hướng dẫn |
