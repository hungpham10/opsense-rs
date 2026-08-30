# Checklist — Opsense

Đối chiếu hiện trạng Opsense với hai bộ checklist:

- **Part A — Audit kiến trúc (17 mục):** rà cốt lõi Station / Store / DAG /
  Rhai / MCP / Lakehouse. Cập nhật sau đợt đóng 6 gap backend (2026-08-24).
- **Part B — Interactive REPL & Data Analysis (23 mục):** checklist hướng
  người dùng cuối. Phần lớn ❌ — nền REPL chưa làm, chỉ có nền tảng backend
  (MCP stdio + Rhai read/sandbox một phần) làm "bản xem trước" thay thế.

Chú giải trạng thái: ✅ xong · ⚠️ một phần / thiếu góc · ❌ chưa làm.

---

## Part A — Audit kiến trúc (17 mục)

| # | Mục | Trạng thái | Ghi chú |
|---|---|---|---|
| 1 | Station registry toàn cục (OnceLock + dependencies) | ✅ | `register_station` first-wins; `set_station_dependencies(cfg.inputs)`; `describe_station` cộng `latest_raw/processed`. |
| 2 | Hợp đồng `ObservationStore` (append/query/query_all/latest_ts/delete_before/invalidate_from/describe) | ✅ | Trait `Send+Sync`; `describe`/`invalidate_from` có default (`unknown` / `Err`). |
| 3 | Backend lưu trữ: memory / duckdb / lmdb / s3 | ✅ | `backend(name, data_dir, block_secs)`; mirror double-write. |
| 4 | Lakehouse block layout + block-pruning khi query | ✅ | `<stage>/blk=<start>/batch_NNN.parquet`; prune đúng block, fallback full-list nếu >10k block. |
| 5 | Read-compat layout cũ (`dt=/hour=`) | ✅ | `DuckdbStore::parquet_all` quét đệ quy, đọc cả thư mục cũ không migrate. |
| 6 | Retention / `delete_before` thật | ✅ | task nền 60s; xoá nguyên block/partition đã già. |
| 7 | Watermarks journal + sống qua restart | ✅ | `<data_dir>/watermarks.json` tmp+rename; file hỏng → sạch. |
| 8 | Vector runtime DAG + chặn cycle | ✅ | `validate_no_cycles` (DFS 3 màu) trong `reload()`. |
| 9 | Backpressure trên kênh node | ✅ | mpsc bound 1024 + `.await`; counter `channel_full_waits`. |
| 10 | Config TOML + attributes + override env | ✅ | `OPSENSE_ATTR_*`, template `{{...}}`. |
| 11 | Rhai engine + sandbox (max_ops, timeout) | ✅ | `engine()` max_ops 1M; `OPSENSE_RHAI_TIMEOUT_SECS`. |
| 12 | Hàm đọc lịch sử trong Rhai (`ts_query`/`ts_mean`) | ✅ | handle trạm publish vào registry theo id. |
| 13 | Thư viện toán tử time-series Rhai (`ts_*`) | ✅ | rate/moving_avg/resample/quantile/p95/p99/delta/pct_change. |
| 14 | Schema / metadata discovery (`describe`) | ✅ | `opsense_describe`/`opsense_list_stations` + `GET /describe`. |
| 15 | Metrics nội bộ của store (`StoreMetrics`) | ✅ | atomic counters, `snapshot()` skip số 0. |
| 16 | Lineage / invalidation tức thì (`invalidate_from`) | ✅ | RAM/LMDB ngay; duckdb/s3 trả `Err` (chỉ qua retention). |
| 17 | Capacity engine (ngưỡng dung lượng) | ⚠️ | `capacity` map + `CapacityMap` có, nhưng wiring đánh giá ngưỡng còn mỏng — việc của sóng sau. |

Kết luận Part A: 16/17 ✅, 1 ⚠️. 6 gap chặn DoD đã đóng.

---

## Part B — Interactive REPL & Data Analysis (23 mục)

> REPL chuyên biệt chưa có; thay thế tạm bằng vòng lặp MCP (`opsense mcp` →
> `opsense_init`/`opsense_run`/`opsense_query`/`opsense_status`) + script Rhai.
> Các mục đánh dấu ✅/⚠️ là nền tảng backend đã sẵn, không phải UI REPL.

| # | Mục | Trạng thái | Ghi chú |
|---|---|---|---|
| 1 | REPL vòng lặp tương tác (prompt/lệnh) | ❌ | thay bằng MCP client + `opsense mcp`. |
| 2 | Nạp dữ liệu từ nhiều nguồn (HTTP/Prom/Vector) | ✅ | `http_source` generic + `vector`/`ingest_source`; template + script mapping. |
| 3 | Inspect schema & metadata | ✅ | `opsense_describe` / `opsense_list_stations`. |
| 4 | Query observations (range + metric) | ✅ | `opsense_query({stage,from_ts,to_ts})`. |
| 5 | Tìm kiếm / filter linh hoạt | ⚠️ | metric + cửa sổ; chưa full label-selector trong query MCP. |
| 6 | Toán tử time-series (rate/MA/resample/quantile…) | ✅ | `ts_*`. |
| 7 | Aggregation (avg/min/max/sum/count) | ✅ | `ts_resample(..., "avg"\|"min"\|…)`. |
| 8 | Downsampling / `step` | ⚠️ | `query_range` có `step` nhưng chưa downsample server-side. |
| 9 | Windowing / resampling | ✅ | `ts_resample(points, bucket_secs, agg)`. |
| 10 | Join giữa các series | ❌ | |
| 11 | Baseline so sánh hiện tại / lịch sử | ✅ | `ts_mean` baseline trong `fn process`. |
| 12 | Anomaly / threshold (capacity engine) | ⚠️ | capacity map sẵn; logic đánh giá ngưỡng thiếu (xem #17 Part A). |
| 13 | Scripting (Rhai) | ✅ | `register_fn` + `fn process(...)`. |
| 14 | Sandbox an toàn (max ops / timeout) | ✅ | max_ops 1M + `OPSENSE_RHAI_TIMEOUT_SECS`. |
| 15 | Visualization (biểu đồ) | ❌ | |
| 16 | Export (CSV/Parquet/JSON) | ⚠️ | ghi parquet (lakehouse) sẵn; chưa có export CLI tuỳ ý. |
| 17 | Macros / saved queries | ❌ | |
| 18 | Pipeline realtime (edit / reload) | ✅ | `opsense_edit` diff thêm/sửa/xoá node. |
| 19 | Persistence / lakehouse | ✅ | duckdb + s3 + lmdb + memory. |
| 20 | Backpressure / streaming | ✅ | mpsc bound 1024. |
| 21 | Timeout / cancellation | ✅ | `OPSENSE_QUERY_TIMEOUT_SECS` + Rhai timeout. |
| 22 | Lineage / invalidation | ✅ | tầng nóng `invalidate_from`. |
| 23 | Multi-station / federation | ⚠️ | registry trạm có; chưa federation port chung / cross-query. |

Kết luận Part B: backend foundation (mục 2,3,4,6,7,9,11,13,14,18,19,20,21,22)
đã sẵn sàng làm hạt nhân cho sóng REPL tiếp theo; UI REPL, join, visualize,
export CLI, macros, federation vẫn ❌.

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
| 2 | Runner Mode — serve→gRPC→runner→IPC→kernel | ✅ | `opsense runner` / `serve --runner-bind` |
| 3 | KernelBackend abstraction | ✅ | Trait 7 ops; LocalIpc ⇄ GrpcRunner đổi lúc chạy, giữ @var |
| 4 | gRPC boundary chỉ serve↔runner | ✅ | Kernel không biết gRPC; runner = translate mỏng |
| 5 | Runner CLI | ✅ | `opsense runner [bind]`, config env `OPSENSE_KERNEL_BIN` |
| 6 | Session routing/reconnect | ⚠️ | switch_backend + registry có; auto-reconnect sau crash runner chưa |
| 7 | Dataset transfer Arrow/chunked/streaming | ✅ | 64k rows/frame, 1M-row test 2 đường, gRPC limit 256MB |
| 8 | Analysis kernel đa ngôn ngữ | ⚠️ | Protocol đa-ngôn ngữ + echo kernel sẵn; **Python sidecar đang làm**, Julia/R sau |
| 9 | REPL chọn/attach backend | ✅ | `:runner connect`, `:kernel local/<name>` |
| 10 | MCP độc lập transport | ✅ | `opsense_kernel_run/health` qua Session API; `/mcp` mount trong routes() |
| 11 | Error handling giữ root cause | ✅ | kind/message taxonomy; crash kernel → EOF sạch, host sống |
| 12 | Performance benchmark | ✅ | bench #[ignore]: local ~0.4ms, gRPC ~1.5ms |
| 13 | Future remote runner | ❌ | Ngoài MVP — design không chặn (addr từ config) |

Kết luận Part C: 11 ✅ · 2 ⚠️ · 1 ❌(theo kế hoạch). DoD còn thiếu đúng một mảnh:
Python kernel sidecar.
