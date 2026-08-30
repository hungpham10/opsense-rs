#PLAN: Opsense capacity planing

## Vấn đề
- Quy trình SRE thường cố định và tuân theo các quy định tiêu chuẩn như error budget hay SLA và SLO
- SRE ở mỗi công ty sẽ hoạt động khác nhau do công cụ, techstack khác nhau dẫn tới việc vận hành sẽ khác nhau ở mỗi team
- SRE là con người cần có thời gian nghỉ ngơi nên cần một công cụ để có thể thay mặt SRE giám sát hệ thống và các bất thường của hệ thống
- SRE không hoàn toàn tham gia vào quy trình release hay review code, họ chỉ tiếp nhận hệ thống khi đã golive xong và bắt đầu theo dõi các      bất thường và tự điều tra hoặc điều chỉnh theo log và metric

## Plan chung
- Config hoàn toàn bằng toml
- Bộ thu thập dữ liệu metrics với thiết kế là sự hết hợp của RHAI + thư viện jq của tôi, đầu endpoint có thể là tất cả dịch vụ liên quan prometheus, timeseries data, ...
- LRU cache lưu dạng block để truy vấn theo khung thời gian thư viện có sẵn
- Cần chỉnh lại các thư viện vector để nó hoạt động được
- Cần một cơ chế đếm thời gian dạng như clock, cứ đến giờ là đẩy tín hiệu vào luồng ngầm chạy trong vector để nó xử lý, bạn có thể hiểu đây như một bộ xung thời gian để hệ thống cảm nhận được giờ là mấy giờ để xử lý gì

### sources.vector – Vector metric source

Vector là một công cụ pipeline dữ liệu, cho phép thu thập, chuyển đổi và xuất log/metric từ nhiều nguồn khác nhau (file, syslog, HTTP, …) và có thể đẩy chúng sang các đích như Prometheus, Loki, Elasticsearch, …

`[sources.vector]` trong file cấu hình TOML của Opsense mô tả một **nguồn metric kiểu Vector** mà Opsense sẽ đọc.

- **url** – endpoint HTTP mà Vector expose metric (ví dụ `http://vector:8686`).
- **jq_filter** (tùy chọn) – biểu thức jq để chọn/lặp payload JSON trả về từ Vector trước khi chuyển thành `Observation`. Engine `opsense_libs::jq::JsonQuery` **chỉ** hỗ trợ: truy cập trường (`.field`), lặp mảng (`[]`) và chọn trường (`select(a, b, ...)`). Nó **không** hỗ trợ pipe `|` hay object literal `{...}`, nên upstream phải trả về sẵn các object có dạng observation. Ví dụ:
  ```jq
  .data[]                      # lặp mảng các observation object dưới .data
  ```
  hoặc `select(ts, value, metric_id, kind, signal, severity, labels)` để chỉ lấy một số trường.
- **metrics** (tùy chọn) – danh sách `metric_id` cần giữ lại từ Vector.

Khi Opsense khởi động, collector đọc cấu hình này, thực hiện GET `url`, áp dụng `jq_filter` (qua `opsense_libs::jq::JsonQuery` – **không** dùng crate ngoài `jq-rs`), chuyển kết quả thành `Observation` rồi đưa vào cache.

**Ví dụ cấu hình**
```toml
[sources.vector]
url = "http://vector:8686"
jq_filter = ".data[]"
metrics = ["cpu_usage", "mem_usage"]
```

## Plan: Updated Opsense capacity engine incorporating PLAN.MD requirements

### Ghi chú (Cần chi tiết)
- Cần chi tiết hơn về cấu trúc `opsense-core` (các module và public API).
- Định dạng chính xác của file TOML cấu hình (các trường, kiểu dữ liệu, giá trị mặc định).
- Chi tiết tích hợp `jq` trong `VectorSource` (cách gọi `jq‑rs`, xử lý lỗi).
- Quy trình hot‑reload config và đồng bộ state của `CapacityEngine` khi config đổi.
- Các test case mẫu cho LRU cache, collector, và engine.

## Thiết kế chi tiết Capacity Engine

### Cấu trúc crate
- `opsense_libs` (crate tiện ích, đã có sẵn trong repo): `jq` (`JsonQuery` – engine jq tự viết), `lru` (`LruCache` – sharded, false-sharing-free), và subsystem `vector` (pipeline, chưa tích hợp).
- `opsense_core` (crate domain, mới tạo): chứa logic capacity engine.
  - `opsense_core::model` – các kiểu dữ liệu lõi (MetricId, RawSample, TimeSeries, MetricProfile, CapacityState, enums).
- `opsense_core::stats` – hàm thống kê thuần (trend, variance, distribution, seasonality, growth, saturation).
- `opsense_core::config` – cấu trúc TOML và loader (crate `config`).
- `opsense_core::source` – trait `MetricSource` + `PrometheusSource`, `VectorSource`.
- `opsense_core::cache` – dùng `opsense_libs::lru::LruCache` (sharded, false-sharing-free) để lưu block thời gian; **không** dùng crate ngoài `lru`.
- `opsense_core::engine` – `CapacityEngine` (profiles, states, capacities).
- `opsense_core::collector` – lập lịch kiểu RHAI + task `Clock` (`tokio::time::interval`).

### Schema TOML (`opsense.conf.toml`)
```toml
[engine]
poll_interval_seconds = 60
cache_block_seconds = 300
cache_max_blocks = 288

[capacity]
cpu_usage = 32.0
mem_usage = 64.0

[sources.prometheus]
url = "http://prometheus:9090"

[sources.vector]
url = "http://vector:8686"
jq_filter = ".data[]"
metrics = ["cpu_usage", "mem_usage"]
```

### Tích hợp `jq` trong `VectorSource`
- Dùng `opsense_libs::jq::JsonQuery` (thư viện jq tự viết trong `opsense-libs`, khớp với “thư viện jq của tôi” trong PLAN.MD): `JsonQuery::parse(filter)` rồi `execute(&payload)` trả `Vec<Value>`; **không** dùng crate ngoài `jq-rs`.
- Nếu `jq_filter` rỗng → parse JSON trực tiếp thành `TimeSeries`.
- Lỗi parse/thực thi → log cảnh báo và trả `Err` để collector retry.

### Hot‑reload & đồng bộ state
- `Config` lưu trong `Arc<RwLock<Config>>`; `GET /reload` đọc lại file và thay thế.
- `CapacityEngine` đọc `capacities` từ config mỗi lần `refresh`/`refresh_all`.
- Metric bị xoá khỏi `[capacity]` → xoá khỏi engine trong `refresh_all`.

### Test case mẫu
- `stats`: chuỗi hằng → trend ≈ 0; chuỗi tăng tuyến tính → slope > 0; chuỗi sin chu kỳ 24h → phát hiện seasonality.
- `cache`: chèn vượt `cache_max_blocks` → số block = giới hạn (evict đúng).
- `collector`: mock source trả 1 điểm → `CapacityState` đúng.
- `engine`: ingest chuỗi, `overview()` trả bảng capacity.

## Phân chia Phase (thực thi)

- **Phase 0 – Scaffold & Dependencies** ✅ (đã xong): `crates/opsense-libs` (util: `jq`, `lru`) có `Cargo.toml` + `lib.rs` và build xanh; `crates/opsense-core` (domain) tạo và phụ thuộc vào `opsense-libs` (bỏ `jq-rs`/`lru` ngoài); thêm `opsense-core` vào `workspace.members`; `cargo check` toàn workspace xanh.
- **Phase 1 – Cấu hình TOML** (`opsense_core::config`): định nghĩa `Config`, loader, validate, hot‑reload, file mẫu `conf/opsense.conf.toml`.
- **Phase 2 – Data model & Thống kê** (`model`, `stats`): kiểu dữ liệu + hàm thuần + unit test.
- **Phase 3 – Capacity Engine** (`engine`): `CapacityEngine` với `ingest/refresh/refresh_all/overview`, bọc `Arc<RwLock>`.
- **Phase 4 – Collector & Sources** (`source`, `collector`): trait `MetricSource`, `PrometheusSource`, `VectorSource` (jq), RHAI scheduler, `Clock` task.
- **Phase 5 – LRU Cache** (`cache`): `LruCache<(MetricId, block_start), TimeSeries>`, collector kiểm tra trước khi fetch.
- **Phase 6 – HTTP API** (`opsense::api`): `AppState` mang engine + config; route `/capacity`, `/capacity/:metric`, `/profile/:metric`; giữ `/health`, `/reload`.
- **Phase 7 – Telemetry & Observability**: đảm bảo engine metric export qua Prometheus layer (đã có sẵn trong `serve.rs`).
- **Phase 8 – Tests & CI**: unit test (stats, cache, config, jq); integration test mock Prometheus+Vector; CI fmt/clippy `-D warnings`/test/check.
- **Phase 9 – Documentation**: `docs/DESIGN.md` chi tiết module/TOML/jq/hot‑reload; cập nhật `README.md`, `PLAN.MD`.

---

## Trạng thái thực tế (2026-08-21) — ưu tiên TẦNG THU THẬP trước

Theo hướng bạn chốt: **làm xong vụ thu thập dữ liệu trước**, thống kê & Capacity Engine tính sau.
Scope giữ nguyên: Opsense = hành vi metric + capacity; KHÔNG RCA/incident/remediation/LLM.
Log/trace giảm về tín hiệu số tại source, không lưu raw.

### Đã xong
- **P1 – Config TOML** ✅ (`opsense_core::config`): `Config`, loader, validate, file mẫu `conf/opsense.conf.toml`.
- **B1 – Model** ✅ (`opsense_core::model`): `TelemetryKind{Metric,Log,Trace}`, `Signal{Utilization,Saturation,Rate,Errors,Duration,Raw}`, `Observation`, `MetricId`, `TimeSeries`.
- **B2 – Source** ✅ (`opsense_core::source`): trait `TelemetrySource` (async `fetch`), `VectorSource` (GET + `opsense_libs::jq::JsonQuery`), `PrometheusSource` (PromQL `/api/v1/query` → `Utilization`).
- **B3 – Cache** ✅ (`opsense_core::cache`): bọc `opsense_libs::lru::LruCache` theo block thời gian `(metric_id, block_start)`, evict theo `cache_max_blocks`/metric.
- **B2 – Collector + Clock** ✅ (`opsense_core::collector`): `from_config` tạo source từ config; `tick()` fetch → **reduce** (Metric pass-through; Log → `error_rate`+`volume`; Trace dropped) → cache.
- **B2 – Executor = vector `Runtime`** ✅ (crate riêng `opsense-components`): thu thập được điều khiển bởi `Runtime` của `opsense_libs::vector` (giống AppState tham khảo). Đồ thị 2 node: `ClockComponent` (`#[source]`, phát tick theo `interval`) → `CollectorSinkComponent` (`#[sink]`, gọi `collector.tick()` mỗi tick). Component tách thành crate riêng `crates/opsense-components/` (`clock.rs`, `collector_sink.rs`, `context.rs`, `lib.rs`) — `opsense-core` trở lại thuần domain, không phụ thuộc `opsense-macros`/`typetag`. Hai component dùng macro `opsense-macros` (`#[source]`/`#[sink]`) sinh sẵn `impl Identify`/`Component`, chỉ viết `run` qua macro `impl_clock_component!`/`impl_collector_sink_component!`. `Runtime` từ chối node đơn lẻ nên clock phải nối vào một sink. `Collector` (không serializable) được đẩy vào component qua `OpsenseContext` (`Runtime::set_context` → `Outbound.ctx`), vì trait `Component` bị `#[typetag::serde]` nên component chỉ giữ state serializable (`id`, `inputs`, `interval_secs`). `opsense-components` re-export `crate::vector::runtime` (alias `opsense_libs::vector::runtime`) để macro sinh code đúng path. `AppState` giờ mang `Arc<RwLock<Runtime>>` + `Arc<Collector>`; `stop()`/`wait_for_shutdown()` uỷ nhiệm cho runtime.
- **Sửa macro `opsense-macros` sea_orm** ✅: bỏ `cfg!(feature = "sea-orm")` (đọc feature của crate macro, không phải crate gọi → artifact cũ nướng cứng `sea_orm::FromQueryResult` phá build consumer); đổi thành opt-in qua attribute `#[sink(sea_orm)]`, xoá feature rỗng `sea-orm`.
- **MCP layer** ✅ (crate `opsense-mcp`, chạy bằng `opsense mcp` — dựng trên **SDK chính thức `rmcp` 3.x** stdio, đổi sang network transport sau này chỉ thay `stdio()` bằng streamable-http):
  - `opsense_init`: mở session từ `.opsense/config.toml` (mặc định; override qua `path`), deserialize `[pipeline].components` qua typetag (`type = "clock_component"` …), build `Runtime` + start. Thiếu `[pipeline]` thì dựng đồ thị mặc định `clock -> collector`. **Single-session**: khoá `<dir>/session.lock` (PID, tạo atomic `create_new`) — mỗi thư mục `.opsense` chỉ 1 session, chặn được cả 2 process khác nhau; lock bỏ quên (process crash) tự thu hồi qua kiểm tra PID sống (`kill(pid,0)`).
  - `opsense_deinit`: `Runtime::stop()` + `wait_for_shutdown()`, đóng session.
  - `opsense_status`: `Runtime::topology()` mới (thêm ở opsense-libs: `NodeInfo {id, component_type, inputs, outputs, running}`) + sự kiện runtime gần đây (ring 100).
  - `opsense_edit`: nhận **danh sách đầy đủ** component mong muốn; `Runtime::reload` tự diff thêm/sửa/xoá, validate liên kết & dead node, lỗi trả về client (isError).
  - Hỗ trợ: `Config` thêm `[pipeline]` (Vec component table); `EngineConfig` default từng field (`#[serde(default)]` struct-level) để config chỉ cần override từng phần. File mẫu: `.opsense/config.toml`.
  - Đã smoke-test end-to-end qua stdio: init → status → edit (kể cả reject đồ thị hỏng "requires input 'ghost'") → deinit.
- **B4 – Review/Monitor API** ✅ (`opsense::api`): `AppState` mang `Arc<Collector>` + `Arc<RwLock<Runtime>>`; route `GET /health`, `POST /reload`, `GET /metrics` (có sẵn) + `GET /observations/{metric}`, `GET /sources`.
- **Merge vector vào opsense-libs** ✅: subsystem `vector` (components + runtime) gộp thành module `opsense_libs::vector`; sửa macro `opsense-macros` (đường dẫn `vector_runtime` → `crate::vector::runtime`, ungate derive `Debug`/`PartialEq`) để build xanh.

### Chưa làm (hoãn theo ý bạn)
- **P5 – Stats** (`opsense_core::stats`): trend/variance/distribution/seasonality/growth/saturation; RED/USE lens.
- **P6 – Capacity Engine** (`opsense_core::engine`): `MetricProfile`, `CapacityState`, `CapacityEngine`.
- **P7 – Telemetry export**, **P8 – CI**, **P9 – `docs/DESIGN.md`**.

### Ghi chú quan trọng: hợp đồng VectorSource / jq
Engine `opsense_libs::jq::JsonQuery` **chỉ** hỗ trợ: truy cập trường (`.field`), lặp mảng (`[]`),
chọn trường (`select(a,b,...)`). Nó **không** hỗ trợ pipe `|` hay object literal `{...}`, nên
**upstream Vector phải trả về sẵn các object có dạng observation** (`ts`/`timestamp`, `metric_id`,
`kind`, `signal`, `value`, `labels`, `severity`). `jq_filter` chỉ dùng để lặp/chọn, ví dụ `.data[]`.
Việc mở rộng `JsonQuery` hỗ trợ `|` và `{...}` là hướng tương lai (để filter `.data[] | select(...) | {...}` gốc chạy được).

### Kiểm chứng
- `cargo test` ✅ (opsense-core: 9; opsense-components: 1; opsense-mcp: 2 — vòng đời init/status/edit/deinit + reject type lạ; opsense-libs: 22).
- `cargo clippy --workspace --all-targets` ✅ sạch (CI sẽ chạy `-D warnings`).
- Smoke test chạy binary HTTP: `/health`→OK, `/sources`→liệt kê 3 source + trạng thái fetch, `/observations/{m}`→trả mảng (rỗng khi chưa có backend), `POST /reload`→"reloaded".

---

## Trạng thái thực tế (2026-08-24) — Pipeline signal + Lakehouse storage

Theo plan đã chốt: vector chạy ngầm bắn **signal theo đồng hồ kèm timestamp**; mỗi công đoạn lưu vào LRU riêng rồi đổ xuống lakehouse parquet/duckdb (local có LMDB để test); pipeline **generic theo signal** — clock chỉ là một nguồn phát tín hiệu, sau này Kafka/Redis/RabbitMQ consumer cắm vào là chạy thay.

### Schema signal giữa các node (`payload` JSON của Message)
```json
{"event": "tick",       "ts": 1690003600}   // ClockComponent (hoặc Source khác sau này)
{"event": "data_ready", "ts": 1690003600}   // ingest đã fetch & lưu Raw tới mốc ts
{"event": "processed",  "ts": 1690003600}   // processor đã xử lý Raw→Processed tới mốc ts
```
Mỗi node giữ cursor riêng (`Watermarks`: `ingest_done`/`processed_done`/`persist_done`, AtomicI64 trong context). Cửa sổ đọc là nửa-mở `(from_ts, to_ts]` — node sau đọc đúng phần delta node trước vừa báo.

### Đã xong
- **opsense-core::store** ✅: `Stage{Raw,Processed}`, trait `ObservationStore` (`append`/`query`/`query_all`/`latest_ts`), `MemoryStore` (mỗi stage một LRU riêng — đúng yêu cầu "mỗi quá trình một LRU"), `Watermarks` + `Cursor`.
- **opsense-core::algorithm** ✅: trait `ProcessingAlgorithm` + `Passthrough` mặc định (thuật toán thật sẽ mô tả sau).
- **Config `[storage]`** ✅: `backend = "duckdb"|"lmdb"|"memory"` (mặc định memory), `data_dir`; `[storage.mirror]` tùy chọn ghi kép.
- **crate `opsense-store`** ✅:
  - `DuckdbStore` — lakehouse: **parquet là source of truth**, duckdb in-memory chỉ là engine truy vấn. Mỗi batch flush 1 file `<data_dir>/<stage>/dt=YYYY-MM-DD/hour=HH/batch_NNN.parquet` (hive-partition qua `COPY ... FORMAT PARQUET`); đọc bằng `read_parquet(glob, hive_partitioning=true)` → SQL thoải mái cho SaaS sau này. Row: `ts BIGINT, metric_id VARCHAR, data JSON`.
  - `LmdbStore` — heed/LMDB, key `[stage][ts big-endian sign-flip][seq]` để range-scan theo thời gian; dùng cho local/test.
  - `MirrorStore` — double-write, đọc từ backend chính. Factory `build(&storage_config)`.
- **Components mới** ✅ (`opsense-components`):
  - `signal.rs`: hằng/builder/parser cho schema signal trên.
  - `ClockComponent`: tick giờ mang `ts` hiện tại (unix secs).
  - `IngestComponent` (`#[transform]`, input `clock`): nhận `tick{ts}` → `collector.collect()` (fetch toàn bộ source + reduce) → `append(Raw)` vào working store → set `IngestDone` → phát `data_ready{ts}`.
  - `ProcessorComponent` (`#[transform]`, input `ingest`): nhận `data_ready{ts}` → query Raw `(processed_done, ts]` → `algorithm.process()` → `append(Processed)` → phát `processed{ts}`.
  - `PersistComponent` (`#[sink]`, input `processor`): nhận `processed{ts}` → query Processed `(persist_done, ts]` → ghi persistence (+mirror) → set `PersistDone`.
  - Đồ thị mặc định `default_pipeline(cfg)`: `clock → ingest → processor → persist`; `pipeline_from_config(cfg)` dùng `[pipeline].components` nếu cấu hình (override tay được), không thì dùng đồ thị mặc định — `serve` và MCP init dùng chung hàm này. Đồ thị cũ `clock → collector_sink` vẫn dùng song song được.
- **Wiring** ✅: `opsense serve` và MCP `opsense_init` cùng dựng `persistence = opsense_store::build(...)` → `OpsenseContext::from_config` (working = MemoryStore, algorithm = Passthrough) → reload pipeline → start. `opsense_status` trả thêm `watermarks {ingest_done, processed_done, persist_done}` để quan sát tiến độ từng node.
- File mẫu `.opsense/config.toml`: clock 1h (3600s), `[storage]` duckdb lakehouse, comment sẵn đồ thị 4 node + hướng dẫn thay source khác.

### Kiểm chứng
- Integration `hourly_pipeline_runs_end_to_end` ✅ (MockSource + memory stores): raw/processed/persisted đều có dữ liệu, cursors > 0.
- DuckDB/LMDB roundtrip tests ✅ (viết–đọc file thật trong temp dir).
- `cargo clippy --workspace --all-targets` ✅ sạch; `cargo test --workspace` ✅ xanh.

### Chưa làm (hướng tiếp)
- Thuật toán xử lý thật thay `Passthrough` (chờ mô tả).
- Source phát signal thay thế clock: Kafka/Redis/RabbitMQ consumer component.
- P5 Stats / P6 Capacity Engine đọc từ `ObservationStore` (query Processed), P7 telemetry export, P8 CI, P9 DESIGN.md.

---

## Trạng thái thực tế (2026-08-24) — Rhai scripting playground + MCP run/query

Quyết định cuối: **bỏ hẳn hướng FFI/dylib** (đã xoá sạch code libloading + thư mục `plugins/`), playground xử lý dữ liệu viết bằng **script Rhai** — an toàn (sandbox, không đụng host), không lỗi build/link đa ngôn ngữ, sửa script là chạy lại ngay. Rhai tách thành crate riêng `opsense-rhai` theo đúng pattern components/store.

### crate `opsense-rhai`
- **`runtime.rs`** — Script runtime:
  - `ScriptSource::{Inline, File}` — script nằm trong config hoặc file `.rhai`.
  - Cache AST toàn cục: key = content-hash (inline) / path+mtime (file) → **sửa file script là tự biên dịch lại ở batch kế tiếp**, không cần restart session.
  - Sandbox: không fs/network/host fn; giới hạn `max_operations = 1_000_000`, array/map 100_000, string 1_000_000, `set_max_expr_depths(256, 256)` (default 32/32 bị "Expression exceeds maximum complexity" với map literal + interpolation bình thường).
  - `call_process()` chạy trong `spawn_blocking`, JSON in/JSON out.
- **`transform.rs`** — node `rhai_transform_component` (`#[transform]`, typetag):
  - Fields: `id`, `inputs`, `script` | `script_path` (đúng một trong hai), `input_stage` (mặc định raw), `output_stage` (mặc định processed), `write_lru` (true), `write_store` (false).
  - Luồng dữ liệu y hệt ProcessorComponent: signal mang `ts` → cửa sổ `(cursor_node, ts]` → `fn process(observations)` → ghi output theo flag → phát `processed(ts)`.
  - **Lỗi script không giết pipeline**: log warn, giữ cursor, cửa sổ được retry ở signal kế (sửa script xong là self-heal).
- Hợp đồng script: `fn process(observations)` nhận mảng observation map (`ts`, `metric_id`, `kind`, `signal`, `value`, `labels?`, `severity?`), trả mảng mới cùng dạng.

### Watermark cursor theo tên node (`opsense-core::store`)
- `Watermarks` thêm `named: Mutex<HashMap<String, AtomicI64>>` + `get_node()/set_node()/named_snapshot()` — mỗi node script/plugin dùng `id` của mình làm cursor độc lập, không xung đột 3 slot enum cũ.

### Store/LRU cấu hình được cho từng node
- `IngestComponent`: `store_raw` (default false) — bật thì raw ghi thêm persistence.
- `ProcessorComponent`: `store_processed` (default false) — tương tự với processed.
- Node rhai: `write_lru` / `write_store` chọn LRU, store, hoặc cả hai.
- Mặc định giữ hành vi cũ ⇒ config/test cũ không đổi.

### MCP: vòng lặp playground khép kín (6 tools)
- **`opsense_run({node?, ts?})`** — bơm `tick(now)` vào node (mặc định `ingest`) qua `Runtime::inject`: pipeline không cần clock vẫn chạy được — muốn retest thì gọi run rồi query.
- **`opsense_query({source, stage, metric?, from_ts?, to_ts?})`** — đọc `working|persistence` × `raw|processed` theo cửa sổ thời gian, xem kết quả script ngay.
- `opsense_status` giờ trả thêm `watermarks.named` (cursor từng node script).
- Force-link `use opsense_rhai as _;` trong opsense-mcp để typetag đăng ký vào binary cuối.

### Playground
- `scripts/moving_avg.rhai` — batch mean per metric → `<metric>_mean`; `scripts/README.md` hướng dẫn tiếng Việt (hợp đồng, config node, workflow MCP run→query→edit).
- `.opsense/config.toml`: comment sẵn đồ thị `[pipeline]` KHÔNG có clock — `ingest → rhai(mean) → persist`, điều khiển hoàn toàn bằng `opsense_run`.

### Kiểm chứng
- Integration `clock → ingest → mean(rhai) → persist` ✅ (MemoryStore): script output có trong working LRU + persistence, cursor node `mean` tiến; test deserialize inline/full/unknown-field; test thiếu script trả lỗi sạch.
- `cargo clippy --workspace --all-targets` ✅ sạch; `cargo test --workspace` ✅ xanh (**45 test**: core 13, libs 22, mcp 4, components 1, store 2, rhai 3).

---

## Trạng thái thực tế (2026-08-24) — Gỡ Prometheus, node HTTP generic (template + attributes + script mapping)

Quyết định: **xoá sạch adapter Prometheus riêng** (`PrometheusSource`, parser, `[sources.prometheus]`, heuristic kind trong `/sources`). Lý do: hướng công cụ chung — ít maintain, tuỳ biến tốt. Use-case Prometheus không mất: node HTTP generic + một script Rhai map kết quả là chạy được (xem `scripts/prom_query_range.rhai`).

### Gỡ Prometheus
- `opsense-core/source.rs`: bỏ `PrometheusSource` + structs parse; giữ `VectorSource`; export chung `observation_from_value()` và `observations_from_body()` (body JSON observation-shape) cho pipeline dùng lại.
- `config.rs`: bỏ `[sources.prometheus]`; **validate nới lỏng — không bắt buộc source nào** (pipeline tự fetch qua node http). Trait `TelemetrySource` thêm `kind()` mặc định `"generic"` (`/sources` hiện kind thật, hết heuristic tiền tố id).
- `Collector::from_config` chỉ còn nhánh vector.

### Template engine + attributes (`opsense-core`)
- Module mới `template.rs`: `render(text, vars, attrs)` với cú pháp `{{name}}`:
  - builtin `{{from_ts}}` / `{{to_ts}}` / `{{ts}}`;
  - `{{tên}}` tra bảng attribute;
  - `{{env.TÊN}}` đọc trực tiếp biến môi trường;
  - biến lạ → lỗi liệt kê tên khả dụng (fail fast), `{{` chưa đóng → lỗi.
- Config thêm bảng top-level `[attributes]`; `Config::resolved_attributes()` merge env: `OPSENSE_ATTR_<TÊN_HOA>` ghi đè giá trị toml, env-only entry cũng được nhận (inject secret không cần sửa file).
- `OpsenseContext` thêm `attributes: Arc<BTreeMap<String,String>>` — mọi component đọc được.

### `http_source_component` (`opsense-components/src/http.rs`)
- Request toàn template: `url`, giá trị `headers`, giá trị `params` (đi qua `.query()` nên tự percent-encode), `body`. Method tuỳ ý, `timeout_secs` 30, client pool chia sẻ theo timeout.
- Nhịp chạy chuẩn hệ thống: signal mang `ts` → cursor riêng theo `id` (`get_node/set_node`) → `from_ts = cursor` (lần đầu `ts − initial_lookback_secs`, mặc định 0 = fetch instant-style đầu tiên) → render → gọi → ghi `Stage::Raw` vào working (+persistence nếu `store_raw`) → set cursor → phát `data_ready(ts)`.
- Lỗi render/fetch/map: warn + **giữ cursor**, cửa sổ retry ở tick kế (cùng hợp đồng node rhai).
- Map response qua enum `format`: `"observations"` (body sẵn observation-shape JSON) hoặc `"script"`.

### Script engine pluggable (`opsense-core/script.rs` + `opsense-rhai`)
- Tránh vòng phụ thuộc components ↔ rhai: core định nghĩa trait `ScriptRunner` (async, JSON in/out) + registry process-wide (`set_script_runner`/`script_runner`, lần đầu thắng).
- `opsense-rhai` expose `RhaiScriptRunner` (dùng lại AST cache/sandbox của runtime rhai) + `register()` idempotent; `opsense serve` và MCP `opsense_init` gọi register lúc khởi động. Node config `format = "script"` + `script`|`script_path` → body thô đi qua `fn process(body)`.
- `scripts/prom_query_range.rhai`: map response `/api/v1/query` (vector) lẫn `/query_range` (matrix) → observations, `metric_id` từ `labels.__name__`, giữ label phụ, `ts` float→int, value string→float.
- `.opsense/config.toml`: ví dụ đầy đủ `[attributes]` + node http query_range commented; `scripts/README.md` có mục "Cào API ngoài".

### Kiểm chứng
- Integration mock TCP server (`http_pipeline.rs`, components): request render đúng template (`GET /metrics/hcm?start=…&end=…`), Raw vào working + persistence (`store_raw`), chuỗi processor→persist flush, cursor tiến; endpoint 500 → cursor giữ nguyên, store rỗng; deserialize defaults/full/unknown-field/format lạ bị từ chối.
- Integration rhai (`http_format.rs`): node `format="script"` + `prom_query_range.rhai` qua mock trả matrix Prometheus → observations đúng ts/value/labels (instance, job), `data_ready(ts)` downstream, cursor == tick; runner inline script OK.
- Unit: template 7 test (builtin/attr/env/unknown/unclosed), registry script, attributes env-override, parser observations.
- `cargo clippy --workspace --all-targets` ✅ sạch; `cargo test --workspace` ✅ xanh (**57 test**: core 20, libs 22, mcp 4, components 4, store 2, rhai 5).

### Chưa làm (hướng tiếp)
- Auth nâng cao cho node HTTP (OAuth flow, refresh token), retry/backoff chủ động, rate-limit per endpoint.
- P5 Stats / P6 Capacity Engine đọc từ `ObservationStore`, P7 telemetry export, P8 CI, P9 DESIGN.md.

---

## Trạng thái thực tế (2026-08-24) — `opsense init` + GUIDE

- Subcommand mới `opsense init [path] [--force]` (`crates/opsense/src/init.rs`): sinh file config mẫu từ template nhúng `init_template.toml` (`include_str!`) ra `path` (mặc định `.opsense/config.toml` — trùng đường dẫn MCP `opsense_init` tải mặc định), tự tạo thư mục cha. **File tồn tại → từ chối** (`AlreadyExists`, kèm gợi ý) trừ khi `--force`; ghi xong in các bước tiếp theo.
- Template chứa: `[engine]`/`[capacity]`/`[attributes]` (chỉ `prom_url` placeholder trỏ localhost, người dùng tự sửa thành endpoint của mình — không kèm config riêng của bên nào)/`[storage]` duckdb active; 3 khối commented bật nhanh: (A) playground không-clock cho MCP `opsense_run`, (B) clock + `http_source_component` query_range VictoriaMetrics, (C) `[sources.vector]` cũ.
- QoL CLI: chạy `opsense` không subcommand → in help thay vì panic.
- `docs/GUIDE.md` (tiếng Việt): vòng đời init→sửa config→MCP/serve; bảng biến template; attributes + override `OPSENSE_ATTR_*`; hai pipeline mẫu; hợp đồng `fn process(body)`; ngữ nghĩa lỗi/cursor; **công thức thí nghiệm ad-hoc** (copy harness `crates/opsense-rhai/tests/http_format.rs` thành test `#[ignore]`, chạy `cargo test -- --ignored --nocapture`); mục xử lý sự cố.
- Kiểm chứng: unit test ghi template ra temp rồi `Config::load` OK + có `prom_url`, từ chối ghi đè, `--force` cho phép; smoke test binary trong /tmp (`init` → chạy lại bị chặn → bare command in help).

---

## Trạng thái thực tế (2026-08-24) — Quy tắc đặt tên `type` cho component

Nhìn config phải biết ngay node là nguồn dữ liệu, xử lý hay đích. Vì typetag name được sinh cơ học từ tên struct (`to_snake_case` trong `opsense-macros/configurable_component.rs`), chỉ cần **đổi tên struct** — không sửa macro — là toàn hệ thống theo quy tắc: **mọi `type` kết thúc bằng `_source` / `_transform` / `_sink`**.

### Đổi tên (struct → type)

| Struct cũ | Struct mới | `type` mới | Vai trò |
|---|---|---|---|
| ClockComponent | ClockSource | `clock_source` | nguồn tín hiệu nhịp |
| IngestComponent | IngestSource | `ingest_source` | kéo dữ liệu collector |
| HttpSourceComponent | HttpSource | `http_source` | cào API ngoài (template) |
| ProcessorComponent | ProcessorTransform | `processor_transform` | tổng hợp window |
| RhaiTransformComponent | RhaiTransform | `rhai_transform` | script Rhai |
| PersistComponent | PersistSink | `persist_sink` | lưu store |
| CollectorSinkComponent | CollectorSink | `collector_sink` | đẩy ra collector |

Macro `impl_<snake>!` sinh kèm tự đổi theo (`impl_clock_source!`…); re-export, `default_pipeline()`, toàn bộ test fixture TOML/JSON, doc comment trong code (`config.rs`, `server.rs`, `source.rs`), `init_template.toml`, `.opsense/config.toml` (khối ví dụ — user duyệt), `scripts/prom_query_range.rhai` + `scripts/README.md`, `docs/GUIDE.md` đều đã cập nhật. **Legacy vector components trong opsense-libs giữ nguyên** (`json_2_json`, `input`, `output`, `print`, `null`, `file_sink`) — subsystem cũ. Các mục lịch sử trong PLAN.MD giữ nguyên tên cũ vì mô tả trạng thái thời điểm đó.

Lưu ý ngữ nghĩa: đuôi type = **vai trò dữ liệu**; `opsense_status` vẫn báo vị trí đồ thị (enum Source/Transform/Sink theo bơm tín hiệu — vd `http_source` sau clock → status `Transform`). GUIDE.md có mục 3 giải thích + bảng đối chiếu cũ→mới.

### Kiểm chứng
- grep toàn repo không còn tên cũ (trừ mục lịch sử PLAN.MD); `cargo test --workspace` ✅ xanh (58 test), clippy sạch; smoke `init` ra /tmp dùng tên mới.

---

## Trạng thái thực tế (2026-08-24) — Trạm timeseries nội bộ (`station_sink`)

"Trạm dữ liệu" nhúng trong pipeline: cache lịch sử để các node tổng hợp quá khứ so với hiện tại, và thay chỗ Prometheus cho `http_source`.

### `CacheStore` — `ObservationStore` dạng cache khối (`opsense-store/src/cache_store.rs`)
- Hot tier: block thời gian `block_secs` (mặc định 300s), evict theo **tuổi** (`hot_ttl_secs`) + **số block** (`max_hot_blocks`) + **dung lượng xấp xỉ** (`max_hot_mb`), cũ nhất trước; chạy khi append và khi `sweep(now)` nền.
- Cold tier tuỳ chọn: write-through xuống delegate (LMDB riêng `.opsense/stations/<id>/`); query ghép cold + overlay hot, dedup `(metric_id, ts)` ưu tiên hot.
- Retention cold qua hàm trait mới **`delete_before(stage, cutoff) -> usize`** (default no-op): LMDB range-delete thật (key sort theo ts), DuckDB/Parquet cố tình bỏ qua — file chia ngày/giờ sẵn, dọn giao lifecycle ngoài (S3).

### Registry toàn cục + Component
- `opsense-core/store.rs`: `register_station(id, store)` / `station(id)` / `station_ids()` — mẫu `ScriptRunner`, first-wins (đổi tham số trạm cần restart session).
- `station_sink` (`opsense-components/src/station.rs`, `#[sink]`): fields `id, inputs, stage(raw|processed), bind(127.0.0.1:9190), block_secs, hot_ttl_secs, max_hot_blocks, max_hot_mb, data_dir(""), cold_retention_secs(0)`. Vòng run như persist_sink (cursor `get_node/set_node`, nhận cả `data_ready` lẫn `processed`). Lần chạm đầu tiên: dựng CacheStore → đăng ký registry → spawn detached axum server + sweeper 60s.
- HTTP (axum 0.8 promote vào `[workspace.dependencies]`; components thêm dep `axum` + `opsense-store`): `/api/v1/query_range` + `/api/v1/query` trả envelope Prometheus (`metric.__name__`, values `[[ts_f64,"value_str"]]`, selector equality v1, `step` chưa downsample), `/observations` JSON gốc, `/health`. → `http_source` trỏ về trạm thay Prometheus, `prom_query_range.rhai` giữ nguyên.
- Rhai (`opsense-rhai/runtime.rs`): native fns **`ts_query(id, stage, metric, from, to)`** (array map / `()`) và **`ts_mean(...)`** (f64 trung bình / `()`) đọc thẳng registry — script so hiện tại vs quá khứ không cần HTTP.
- `template.toml` khối C commented (station + http_source đọc lại bằng URL template); GUIDE.md mục 6 mới (bảng field/endpoint/hàm Rhai + cơ chế dọn RAM/LMDB/S3); mục sự cố thêm bind conflict.

### Kiểm chứng
- Unit CacheStore 4 test (bucketing/half-open, TTL sweep, count cap, cold survive + hot wins dup) + LMDB delete_before; rhai station_lookup 2 test (ts_mean/ts_query tính baseline từ registry; unknown id trả `()`).
- Integration `station_pipeline.rs`: seed working → bơm `processed(ts)` vào node → cursor tiến, handle nằm trong registry, `/health`, `/api/v1/query_range` (matrix đúng `__name__`/values), `/api/v1/query` (vector điểm mới nhất), `/observations` JSON gốc ✅.

---

## Trạng thái thực tế (2026-08-24) — Dọn bỏ `ProcessingAlgorithm`

Ý tưởng thừa từ thiết kế ban đầu: trait `ProcessingAlgorithm` + `Passthrough` chờ "thuật toán thật" cắm vào sau. Thực tế không có đường nào cho người dùng cắm (context dựng cứng Passthrough), nhu cầu biến đổi dữ liệu đã giải quyết bằng `rhai_transform`, tổng hợp quá khứ qua trạm `station_sink`.

### Đã xoá
- `opsense-core/src/algorithm.rs` (trait + `Passthrough`) và export trong `lib.rs`.
- Field `algorithm` khỏi `OpsenseContext` (+ tham số khỏi `new()`; bỏ luôn `#[allow(clippy::too_many_arguments)]`).
- `processor_transform`: dùng thẳng cửa sổ Raw ghi sang Processed (pass-through, có comment); `store_processed` cũng ghi `raw`.
- Dọn 5 call site dựng context (components lib + 4 file test) và các doc comment còn nhắc "processing algorithm" trong `store.rs`. Các mục lịch sử PLAN.MD phía trên giữ nguyên vì mô tả thời điểm đó.
- Giữ nguyên: node `processor_transform`, cặp stage Raw/Processed, cursor `processed_done`, luồng tín hiệu `data_ready → processed` — đó là xương sống pipeline, chỉ bỏ lớp gián tiếp.

### Kiểm chứng
- grep toàn repo sạch; `cargo clippy --workspace --all-targets` ✅; `cargo test --workspace` ✅ 66 test (mất đúng 1 test `passthrough_copies_input` theo kế hoạch).

---

## Trạng thái thực tế (2026-08-24) — Một module store + bỏ TTL RAM + bỏ cache Collector

Ba đợt dọn "ý tưởng thừa" gộp một: (1) **cache RAM không cần dọn theo thời gian** — trần số block/dung lượng đã chặn, retention theo thời gian là việc của tầng persistent (LMDB/S3); (2) **store nằm một nơi duy nhất** — trait, Stage, Watermarks, MemoryStore chuyển từ `opsense-core` sang `opsense-store`, core trở lại là crate domain thuần; (3) **Collector hết giữ bản sao dữ liệu** — `ObservationCache` nội bộ chép lại mọi observation chỉ để nuôi đúng một endpoint HTTP cũ, trong khi pipeline đã có working store.

### TTL khỏi hot tier của trạm
- `CacheConfig` mất field `hot_ttl_secs`; `Tier::evict` chỉ còn `max_hot_blocks` + `max_hot_mb` (oldest-first), chạy inline khi append — không còn sweep định kỳ cho RAM.
- `CacheStore::sweep(now)` giờ chỉ làm cold retention (`delete_before`); station_sink chỉ spawn sweeper khi có cold delegate && `cold_retention_secs > 0`.
- Breaking nhỏ: config `station_sink` còn khoá `hot_ttl_secs` sẽ lỗi deserialize — bỏ khoá đó đi.

### Gom store về `opsense-store`
- `opsense-store/src/store.rs` (mới): `Stage`, trait `ObservationStore` (+default `delete_before`), registry trạm (`register_station`/`station`), `Watermarks`+`Cursor`, `MemoryStore`(+`StageLru` dùng `opsense_libs::lru`). Unit test MemoryStore/Watermarks/registry chuyển cùng.
- Xoá `opsense-core/src/store.rs`; mọi crate đổi import `opsense_core::store::…` → `opsense_store::…` (components, rhai, mcp, tests). Deps: store += `serde`, `opsense-libs`; rhai += `opsense-store`.
- Giảm pub: bỏ re-export `DuckdbStore` (chỉ `build()` dùng), `MirrorStore` thành private, helper test `obs()` gộp một chỗ `#[cfg(test)] pub(crate) mod testing`.

### Bỏ cache Collector + endpoint cũ
- Xoá `opsense-core/src/cache.rs` (`ObservationCache`) và `Collector::series()`; `Collector::new(sources)` mất tham số capacity; `collect()` chỉ trả batch.
- Xoá route `GET /observations/{metric}` khỏi `opsense::api` — nhu cầu tra dữ liệu: MCP `opsense_query`, `/observations` của station_sink.

### Kiểm chứng
- grep sạch `opsense_core::store` / `ObservationCache` / `hot_ttl_secs` (mục lịch sử PLAN.MD giữ nguyên). `cargo clippy --workspace --all-targets` ✅; `cargo test --workspace` ✅ 65 test / 22 suite (bớt đúng 1 test TTL sweep); smoke `init` template mới ✅.

## Trạng thái thực tế (2026-08-24) — Audit checklist kiến trúc + đóng 3 gap ưu tiên

Rà toàn bộ checklist 17 mục qua codegraph: kiến trúc lõi Station / Rhai / DAG /
time-block khớp thiết kế (thậm chí thừa: OTel + Prometheus + Sentry, `opsense_edit`
realtime), nhưng có 3 lỗ hổng chặn Definition of Done — đã vá trong đợt này.

### 1. DuckDB/Parquet: block-pruning khi query + `delete_before` thật
- `query_all` trước đây glob toàn bộ `dt=*/hour=*` rồi mới filter ts sau. Giờ liệt kê
  đúng các partition giờ cắt ngang cửa sổ `(from_ts, to_ts]` (fallback full-glob khi
  >720 giờ); an toàn vì append luôn nhóm row theo giờ của chính nó.
- Override `delete_before` (default trait trả 0): xoá nguyên file parquet của
  partition đã già hoàn toàn, prune thư mục rỗng; giờ biên chờ già hết mới xoá.
- Wire-up: `[storage] retention_secs` (mặc định 0) + task nền 60s ở cả `serve` lẫn
  MCP init (`spawn_retention_sweeper`) — trước đây store chính không bao giờ được
  dọn, chỉ trạm station mới có sweeper.

### 2. Vector runtime: chặn cycle cho DAG
- `validate_no_cycles` chạy trong `reload()`: DFS ba-màu trên đồ thị id→inputs ghép
  từ node hiện hữu (bảng link) và node thêm/sửa; lỗi kèm chu trình `a -> b -> a`.
- Trước đây chỉ chặn self-loop; chu kỳ A→B→C→A pass validation và chỉ được dập tắt
  ngầm bởi cursor lúc chạy.

### 3. Watermarks sống qua restart
- `Watermarks` có journal JSON (`<data_dir>/watermarks.json`, tmp+rename atomic)
  tự ghi mỗi lần cursor tiến; `OpsenseContext::from_config` bật journal khi
  backend != memory. Restart nay tiếp tục đúng chỗ cũ thay vì backfill lại từ
  `initial_lookback_secs`; file hỏng thì log warn và khởi động sạch.

### Kiểm chứng
- `cargo clippy --workspace --all-targets` ✅; test mới: 2 journal watermarks +
  1 duckdb prune/retention + 2 find_cycle. Smoke `init` OK. Các mục lịch sử phía
  trên giữ nguyên. Gap còn lại lớn: S3-backed Parquet (chưa có dòng code nào).

## Trạng thái thực tế (2026-08-24) — Đóng 6 gap backend của checklist

Tiếp nối audit, đóng nốt 6 lỗ hổng chặn Definition of Done của checklist
"Interactive REPL & Data Analysis" (làm trước nền REPL). Mọi backend chia sẻ
một trait `ObservationStore` + registry trạm toàn cục; code mới nằm chủ yếu ở
`opsense-store`, `opsense-mcp`, `opsense-components`, `opsense-rhai`.

### A. Schema/metadata discovery (#4) + Metrics nội bộ (#2)
- Mọi backend implement `describe() -> StationDescribe` (backend, schema_version,
  params, metrics, dependencies) + trait `StoreMetrics` đếm atomic (appends,
  points_in/out, queries, errors, evictions, cold_reads/hot_hits,
  files_written/scanned, query_total_us).
- Helper `describe_station(id)` cộng `latest_raw/processed` động; registry thêm
  `set_station_dependencies` (từ `cfg.inputs`) + `station_ids`.

### B. Query timeout/cancellation + backpressure (#3)
- Query trạm + MCP bọc `spawn_blocking` + `tokio::time::timeout`
  (`OPSENSE_QUERY_TIMEOUT_SECS`, mặc định 30s; hết giờ trả "query timed out
  after Ns"). Script Rhai bọc `OPSENSE_RHAI_TIMEOUT_SECS` riêng.
- Backpressure: kênh mpsc bound 1024 + `.await` khi đầy; counter
  `channel_full_waits` (log `warn` >100ms).

### C. Thư viện toán tử time-series cho Rhai (#5)
- `register_ts_ops` đăng `ts_rate / ts_moving_avg / ts_resample / ts_quantile /
  ts_p95 / ts_p99 / ts_delta / ts_pct_change` (nhận array observation-map, trả
  `Dynamic`). Test `crates/opsense-rhai/tests/operators.rs`.

### D. Lakehouse block-size cấu hình được (#6a) + read-compat
- `[storage].block_secs` (mặc định 3600). Layout ghi mới `<stage>/blk=<start>/
  batch_NNN.parquet`; query prune đúng block, retention xoá nguyên block.
  Layout cũ `dt=/hour=` vẫn đọc được (read-compat, không migrate).

### E. S3-backed Parquet (#1)
- `S3Store` trên `Arc<dyn object_store::ObjectStore>`; `backend="s3"` parse
  `s3://bucket/prefix` qua `AmazonS3Builder::from_env()` + `OPSENSE_S3_ENDPOINT`.
  Ghi: encode parquet cục bộ (DuckDB in-memory) rồi PUT lên layout `blk=`; chỉ
  advance `latest_ts` khi upload thành công. Mirror nhận arm `"s3"`. Test
  `InMemory` (roundtrip/pruning/retention/invalid_block → Err).

### Lineage / invalidation tức thì (#6b)
- `invalidate_from(stage, from_ts)` override ở MemoryStore (xoá ≥ from),
  CacheStore (bỏ hot-block ≥ from), LmdbStore (range-delete). DuckDB/S3 trả
  `Err` — tầng bền chỉ qua retention (dọn theo dòng phá block pruning).

### Kiểm chứng
- `cargo clippy --workspace --all-targets` ✅ sạch; `cargo test --workspace` ✅
  (opsense-store 16 test incl. 2 S3 InMemory; opsense-rhai 24 incl. operators).
  Smoke `init /tmp` parse OK với khoá mới (`block_secs`, ví dụ `s3://…`).
  Breaking change duy nhất: layout lakehouse mới `blk=` khi ghi (đọc vẫn hiểu
  thư mục cũ) + 2 khoá config mới đều có default an toàn.
- Xem `docs/CHECKLIST.MD` (Part A: audit kiến trúc 17 mục sau 6 gap; Part B:
  checklist Interactive REPL 23 mục → hiện trạng, phần lớn chưa làm trừ MCP
  stdio + Rhai read/sandbox một phần).

---

## Trạng thái thực tế (2026-08-25) — Kiến trúc IPC + gRPC Runner

Hiện thực hoá checklist "Opsense — IPC + gRPC Runner Architecture": bỏ hẳn pyo3
embed, mọi execution chạy **kernel process riêng qua framed stdio IPC**; serve ↔
runner nói **gRPC**; kernel không biết gRPC/MCP.

### Crate mới / thay đổi
- `opsense-proto`: `opsense.proto` là schema duy nhất cho cả hai boundary
  (service `KernelRunner` cho gRPC; `Envelope` protobuf cho CONTROL frames).
  Frame = `[tag u8][len u32][payload]`, tag CONTROL/ARROW; ARROW frame = một
  Arrow IPC stream segment. Kèm `KernelConnection` — driver host-side dùng
  chung (handshake/execute/send_dataset/interrupt/close/shutdown + timeout).
- `opsense-kernel-echo`: kernel tham chiếu Rust khoá chặt wire contract
  (directives sleep/print/err/df). 6 e2e test lifecycle.
- `opsense-session`: xoá `python_bridge.rs` + pyo3. Thêm trait `KernelBackend`
  (`start_session/execute/send_dataset/interrupt/close/shutdown/health`),
  `LocalIpcBackend` (mỗi session một process, kill-on-drop),
  `GrpcRunnerBackend` (client gRPC drop-in), `Session::switch_backend`
  (đổi backend giữa chừng giữ nguyên state host-side vì dataset re-push mỗi
  execute), registry runner trong `SessionManager`.
- `opsense-runner`: service handle `kernel_runner_service(cfg)` +
  standalone bin qua `opsense runner`. Runner chỉ dịch gRPC ↔ cùng framed
  protocol.
- Dataset: `chunk_record_batch` chia 64k rows/frame; gRPC message limit nâng
  256MB hai đầu; đã test 1M rows qua cả local-IPC lẫn gRPC.
- serve.rs là entry phục vụ duy nhất: REST gateway + `/mcp` (handle
  `opsense_mcp::mcp_handler()` mount trong `routes()`) + runner gRPC
  (`--runner-bind`) + terminal mode REPL/MCP-stdio. Env:
  `OPSENSE_RUNNER_BIND`, `OPSENSE_MCP_PORT`, `OPSENSE_KERNEL_BIN`.
- MCP tools mới: `opsense_kernel_run({code})`, `opsense_kernel_health()` —
  chỉ gọi Session/Kernel API, không đụng runner IPC (checklist §10).
- REPL mới: `:runner list | :runner connect <addr> [name]`,
  `:kernel local | :kernel <name>`; alias subcommand `opsense repl|runner|mcp`.

### Số đo (bench #[ignore], echo kernel, 200 rounds)
- local-IPC execute roundtrip: ~0.40 ms
- grpc-runner execute roundtrip: ~1.52 ms (overhead gRPC ~1.1 ms)

### Chưa làm
- `opsense-kernel-python` sidecar (Phase cuối) — hiện dev dùng
  `OPSENSE_KERNEL_BIN=...opsense-kernel-echo`; Python modules `opsense_*.py`
  vẫn nằm ở `crates/opsense-session/python/` chờ chuyển sang sidecar.
- Remote runner network/TLS/auth/discovery (checklist §13 — ngoài MVP).

---

## Trạng thái thực tế (2026-08-26) — Gộp S3 vào DuckdbStore + nén Parquet

- **Một store duy nhất cho lakehouse**: xoá `s3_store.rs` + dep `object_store`.
  `DuckdbStore` giờ hỗ trợ cả local lẫn `s3://bucket/prefix` qua extension
  **httpfs** của DuckDB (`INSTALL httpfs; LOAD httpfs;` + `SET s3_*`). Config
  `[storage.s3]` (endpoint/region/keys/url_style) với env override
  `OPSENSE_S3_*`; mirror `"s3"` cũng đi qua cùng đường.
- **Parquet nén mặc định ZSTD** (`[storage] parquet_compression`,
  zstd|snappy|gzip|uncompressed) — giảm sizing/chi phí lưu trữ S3; áp cả local.
- Remote read: prune theo block vẫn hoạt động (URL suy từ cửa sổ, filename
  wildcard); lỗi block thiếu → fallback full-glob. Retention remote giao
  lifecycle policy (`delete_before` trả 0).
