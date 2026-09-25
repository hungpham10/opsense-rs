# Checklist — Opsense

Đối chiếu hiện trạng Opsense với hai bộ checklist:

- **Part A — Audit kiến trúc (17 mục):** rà cốt lõi Station / Store / DAG /
  Rhai / MCP / Parquet. Cập nhật sau đợt đóng 6 gap backend (2026-08-24).
- **Part B — Interactive REPL & Data Analysis (23 mục):** checklist hướng
  người dùng cuối.

Chú giải trạng thái: ✅ xong · ⚠️ một phần / thiếu góc · ❌ chưa làm.

> **Cập nhật 2026-09-25.** File này là **ảnh chụp audit có mốc** (2026-08-24/25),
> không phải trạng thái hiện tại — mọi dòng ghi "chưa làm" bên dưới đã được kiểm
> lại và ghi chú lại. Nơi mô tả đang chạy: [`architecture.md`](./architecture.md) +
> [`GUIDE.md`](./GUIDE.md).
>
> Những thay đổi lớn làm nhiều dòng cũ lỗi thời:
> - **REPL đã có** (`opsense repl`, có cả kernel mode `--runner` + test PTY) ⇒ các
>   dòng "UI REPL ❌" không còn đúng.
> - **Python + Julia runner đã có** (compose + `integration_runner_grpc`) ⇒ các
>   dòng "sidecar còn thiếu / Julia sau" không còn đúng.
> - **MCP không còn là server nhúng trong `serve`**: nó là client stdio mỏng của
>   GraphQL, 9 tool (`status`, `get_config`, `attributes`, `set/remove_attribute`,
>   `query_timeseries`, `orders`, `set_param`, `reload`) ⇒ các dòng mô tả
>   `opsense_init` / `opsense_deinit` / `/mcp` HTTP đã bị gỡ.
> - **Đã thêm tầng trading** (`opsense-qlib`): chiến lược là script Rhai
>   (`fn rebuild`) hoặc DAG genome ONNX; lệnh + cursor T+N nằm trong station.
> - **Đọc/sửa cấu hình**: `Query.components` + `Mutation.patchComponent`, tức sửa
>   một param không cần gửi lại cả danh sách node (kèm audit log trong station
>   `opsense-audit`).
> - `ObservationStore` chung + `persist_sink` đã bị gỡ: **station là tầng lưu duy
>   nhất** (các dòng 2, 14–16 mô tả tầng lưu đó).

---

## Part A — Audit kiến trúc (17 mục)

| # | Mục | Trạng thái | Ghi chú |
|---|---|---|---|
| 1 | Station registry toàn cục (OnceLock + dependencies) | ✅ | `register_station` first-wins; `set_station_dependencies(cfg.inputs)`; `describe_station` cộng `latest_raw/processed`. |
| 2 | Hợp đồng `ObservationStore` | ⚠️ | Tầng store chung đã bị **gỡ** (refactor 2026-08-26): mỗi node tự ghi vào station riêng qua `timeseries_station_sink`. Phần còn lại: `opsense-mlib/src/storage/` cho backend bền của station. |
| 3 | Backend lưu trữ: memory / parquet / sqlite | ✅ | `open_backend(id, cfg, kind)`; `"parquet"` canonical (alias cũ `duckdb`/`s3`/`lakehouse`); per-station dir `<data_dir>/<id>-<kind>/`. |
| 4 | Parquet lake time-partitioned + block-pruning khi query | ✅ | `ts/blk=<block_id>/batch-<millis>.parquet` (Hive partition `blk=`); prune qua partition khi query; Spark/DuckDB/Polars đọc thẳng. |
| 5 | Lakehouse layout trên S3 | ✅ | `flush_timeseries` → `s3://bucket/prefix/<id>/ts/**`; `snapshot` → `<id>/state/`; restore khi local trống; manifest `ts/manifest.json`. |
| 6 | Retention / xoá block cũ | ✅ | `retain_block_partitions` (trait `retain_older_than`); xoá nguyên `blk=` partition (local + S3) qua background task 60s. |
| 7 | Watermarks journal + sống qua restart | ✅ | `<data_dir>/watermarks.json` tmp+rename; file hỏng → sạch. |
| 8 | Vector runtime DAG + chặn cycle | ✅ | `validate_no_cycles` (DFS 3 màu) trong `reload()`. |
| 9 | Backpressure trên kênh node | ✅ | mpsc bound 1024 + `.await`; counter `channel_full_waits`. |
| 10 | Config TOML + attributes + override env | ✅ | `OPSENSE_ATTR_*`, template `{{...}}`. |
| 11 | Rhai engine + sandbox (max_ops, timeout) | ✅ | `engine()` max_ops 1M; `OPSENSE_RHAI_TIMEOUT_SECS`. |
| 12 | Hàm đọc lịch sử trong Rhai (`ts_query`/`ts_mean`) | ✅ | handle trạm publish vào registry theo id. |
| 13 | Thư viện toán tử time-series Rhai (`ts_*`) | ✅ | rate/moving_avg/resample/quantile/p95/p99/delta/pct_change. |
| 14 | Schema / metadata discovery (`describe`) | ⚠️ | Đã **gỡ** khỏi bề mặt: không còn `opsense_describe`/`opsense_list_stations`/`GET /describe`. Tương đương gần nhất: `opsense_status` (node + station) và `opsense components` (config đang chạy). |
| 15 | Metrics nội bộ của store (`StoreMetrics`) | ⚠️ | Counter của tầng store chung đã bị gỡ cùng tầng đó. Còn lại: metric nội bộ của axum (`PrometheusMetricLayer`) nhưng **chưa expose route** `/metrics`. |
| 16 | Lineage / invalidation tức thì (`invalidate_from`) | ⚠️ | API `invalidate_from` của store chung đã bị gỡ; hiện chỉ còn xoá theo `retention_secs` cho backend station. |
| 17 | Capacity engine (ngưỡng dung lượng) | ✅ | `capacity` map + `CapacityMap`; breach ngưỡng được đánh giá trong `anomaly_station_transform` (2026-09-12). |

Kết luận Part A (tính lại 2026-09-25): 13/17 ✅, 4 ⚠️. 4 ⚠️ đều do **tầng `ObservationStore` chung bị gỡ** (mục 2, 15, 16) và bề mặt `describe` bị thay bằng `opsense_status`/`opsense components` (mục 14) — không phải capability mới mất.

---

## Part B — Interactive REPL & Data Analysis (23 mục)

> **Cập nhật 2026-09-25:** REPL đã có (`opsense repl`), gồm cả kernel mode
> (`--runner`) có test PTY. MCP là client stdio mỏng của GraphQL với 9 tool; các
> tool `opsense_init`/`opsense_run`/`opsense_describe` **không còn tồn tại**.

| # | Mục | Trạng thái | Ghi chú |
|---|---|---|---|
| 1 | REPL vòng lặp tương tác (prompt/lệnh) | ✅ | `opsense repl`: `:status`, `:attr get/set/rm`, `:node list/get/add`, `:query`, `:login`, `:help`; kernel mode có `:py`/`:jl`/`:block`. Test: `integration_repl_pty`. |
| 2 | Nạp dữ liệu từ nhiều nguồn (HTTP/Prom/Vector) | ✅ | `http_source` / `csv_source` / `websocket_2_json` generic + `clock`/`input`; template `{{...}}` + `items`/`fields` mapping. |
| 3 | Inspect schema & metadata | ⚠️ | `opsense_describe`/`opsense_list_stations` đã bị gỡ; dùng `opsense_status` + `opsense_get_config`. |
| 4 | Query observations (range + metric) | ✅ | `opsense_query_timeseries({node, from_ts, to_ts, limit, signal, label_kind})` — có guard, trả `truncated`/`scanned`. |
| 5 | Tìm kiếm / filter linh hoạt | ⚠️ | Có filter server-side theo `signal` + `label_kind` + cửa sổ; chưa có full label-selector. |
| 6 | Toán tử time-series (rate/MA/resample/quantile…) | ✅ | `ts_*`. |
| 7 | Aggregation (avg/min/max/sum/count) | ✅ | `ts_resample(..., "avg"\|"min"\|…)`. |
| 8 | Downsampling / `step` | ⚠️ | `query_range` có `step` nhưng chưa downsample server-side. |
| 9 | Windowing / resampling | ✅ | `ts_resample(points, bucket_secs, agg)`. |
| 10 | Join giữa các series | ❌ | |
| 11 | Baseline so sánh hiện tại / lịch sử | ✅ | `ts_mean` baseline trong `fn process`. |
| 12 | Anomaly / threshold (capacity engine) | ✅ | `anomaly_station_transform` (RCF + capacity breach, 2026-09-12); RCF scoring còn 1 test parked — xem PLAN.MD Session 2026-09-12. |
| 13 | Scripting (Rhai) | ✅ | `register_fn` + `fn process(...)`. |
| 14 | Sandbox an toàn (max ops / timeout) | ✅ | max_ops 1M + `OPSENSE_RHAI_TIMEOUT_SECS`. |
| 15 | Visualization (biểu đồ) | ❌ | |
| 16 | Export (CSV/Parquet/JSON) | ⚠️ | ghi parquet sẵn; chưa có export CLI tuỳ ý. |
| 17 | Macros / saved queries | ❌ | |
| 18 | Pipeline realtime (edit / reload) | ✅ | `opsense_set_param` (`Mutation.patchComponent`, sửa 1 trường, validate trước apply) + `opsense_reload` (thay cả danh sách). Có audit trong station `opsense-audit`. |
| 19 | Persistence / parquet | ✅ | parquet (canonical) + s3 (data_dir `s3://`) + sqlite + memory. |
| 20 | Backpressure / streaming | ✅ | mpsc bound 1024. |
| 21 | Timeout / cancellation | ✅ | Query **bị chặn bằng guard** (trần 10 000 dòng / 30 ngày, dùng `checked_sub`) thay vì timeout; script có `OPSENSE_RHAI_TIMEOUT_SECS`. Biến `OPSENSE_QUERY_TIMEOUT_SECS` **không tồn tại**. |
| 22 | Lineage / invalidation | ⚠️ | `invalidate_from` của store chung đã bị gỡ; còn retention theo `retention_secs`. |
| 23 | Multi-station / federation | ⚠️ | registry trạm có; chưa federation port chung / cross-query. |

Kết luận Part B (tính lại 2026-09-25): 16 ✅ · 6 ⚠️ · 3 ❌. Đã đóng so với bản
gốc: **mục 1 (REPL tương tác)** — trước đây là ❌ lớn nhất. Vẫn ❌: join giữa các
series (10), visualization (15), macros/saved queries (17). ⚠️ là các mục bị ảnh
hưởng bởi việc gỡ tầng `ObservationStore` chung và bề mặt `describe`, không phải
mất capability độc lập.

---

### Đối chiếu với sóng thực thi (2026-08-24)

- **Wave A** (#4 discovery + #2 metrics): mục 1,2,3,14,15.
- **Wave B** (#3 timeout + backpressure): mục 8,9,20,21.
- **Wave C** (#5 operators): mục 6,7,9,13,14.
- **Wave D** (#6a block-size + read-compat): mục 4,5.
- **Wave E** (#1 S3): mục 3,19.
- **#6b lineage**: mục 16,22.
- Mục 17 (capacity engine) và Part B UI (1,10,12,15,16,17,23) là sóng sau.

---

## Part C — Audit checklist "IPC + gRPC Runner Architecture" (2026-08-25)

| # | Mục | Trạng thái | Ghi chú |
|---|---|---|---|
| 0 | Nguyên tắc IPC=exec, gRPC=control | ✅ | Kernel chỉ nói framed stdio; gRPC chỉ ở serve↔runner |
| 1 | Local Mode — REPL → IPC → kernel process | ✅ | `LocalIpcBackend`, kill-on-drop, timeout/cancel/cleanup |
| 2 | Runner Mode — serve→gRPC→runner→IPC→kernel | ✅ | `opsense runner [bind] [--health-check]`; cờ `serve --runner-bind` **không tồn tại**. |
| 3 | KernelBackend abstraction | ✅ | Trait 7 ops; LocalIpc ⇄ GrpcRunner đổi lúc chạy, giữ @var |
| 4 | gRPC boundary chỉ serve↔runner | ✅ | Kernel không biết gRPC; runner = translate mỏng |
| 5 | Runner CLI | ✅ | `opsense runner [bind]`; env thật là `OPSENSE_KERNEL` (không phải `OPSENSE_KERNEL_BIN`), cộng `OPSENSE_RUNNER_BIND`, `OPSENSE_SERVE_URL`, `OPSENSE_ADMIN_TOKEN`. |
| 6 | Session routing/reconnect | ⚠️ | switch_backend + registry có; auto-reconnect sau crash runner chưa |
| 7 | Dataset transfer Arrow/chunked/streaming | ✅ | 64k rows/frame, 1M-row test 2 đường, gRPC limit 256MB |
| 8 | Analysis kernel đa ngôn ngữ | ✅ | Cả ba kernel đã có và được test: `opsense-kernel-echo`, `-python`, `-julia` (compose có 3 runner service, `integration_runner_grpc` chạy cả 3). |
| 9 | REPL chọn/attach backend | ✅ | Cờ CLI `opsense repl --runner <endpoint>` để nói thẳng runner qua gRPC (bỏ qua host); trong kernel REPL: `:py`/`:jl`/`:echo`, `:inline`/`:block`, `:abort`. |
| 10 | MCP độc lập transport | ⚠️ | MCP là **stdio server client mỏng** của GraphQL (`opsense mcp`), không phải server HTTP: không có route `/mcp`, không có `opsense_kernel_run/health`. |
| 11 | Error handling giữ root cause | ✅ | kind/message taxonomy; crash kernel → EOF sạch, host sống |
| 12 | Performance benchmark | ✅ | bench #[ignore]: local ~0.4ms, gRPC ~1.5ms |
| 13 | Future remote runner | ❌ | Ngoài MVP — design không chặn (addr từ config) |

Kết luận Part C (tính lại 2026-09-25): 12 ✅ · 1 ⚠️ · 1 ❌. DoD đã đóng: **Python
kernel sidecar và Julia runner đều tồn tại**. ⚠️ còn lại là mục 10 — MCP nay là
client stdio mỏng thay vì transport độc lập (đây là chủ ý kiến trúc, không phải gap).
❌ duy nhất là mục 13 (remote runner), ngoài MVP theo kế hoạch.
