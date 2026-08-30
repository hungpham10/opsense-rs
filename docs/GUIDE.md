# Opsense — Hướng dẫn sử dụng

Opsense là **engine phân tích hành vi metric và dung lượng**: nó kéo dữ liệu
metric từ các nguồn HTTP tuỳ ý (VictoriaMetrics, Prometheus, API nội bộ…),
chạy qua pipeline transform viết bằng Rhai, lưu vào **station riêng của từng
node** (in-memory LRU + cold tier tuỳ chọn), rồi để phân tích tương tác /
truy vấn lại từ REPL hoặc MCP.

Tài liệu kiến trúc: [`architecture.md`](./architecture.md). Tài liệu script
mẫu: [`../scripts/README.md`](../scripts/README.md). Trạng thái thực thi:
[`PLAN.MD`](./PLAN.MD).

---

## 1. Vòng đời cơ bản

```bash
# 1) Sinh file config mẫu (mặc định .opsense/config.toml, có sẵn thì KHÔNG ghi đè)
opsense init

# 2) Sửa config: đặt [attributes] và bỏ comment MỘT khối [pipeline]
$EDITOR .opsense/config.toml

# 3a) REPL tương tác (analysis session + Python sub-REPL)
opsense serve --repl
# hoặc alias: opsense repl

# 3b) MCP stdio cho client tích hợp (Claude Desktop, IDE…)
opsense serve --mcp
# hoặc alias: opsense mcp
# MCP Streamable HTTP: opsense serve --mcp --http --port 8123
```

`opsense init [path] [--force]`:
- Không truyền `path` → ghi `.opsense/config.toml` (đúng đường dẫn `opsense_init`
  của MCP mặc định tải lên).
- File đã tồn tại → **từ chối** với lỗi rõ ràng; chỉ ghi đè khi truyền `--force`.
- File sinh ra đã parse được ngay (`Config::load`) — bạn chỉ cần bật một khối
  `[pipeline]`.

### Các MCP tool

| Tool | Ý nghĩa |
|---|---|
| `opsense_init({config?})` | Mở session: load config, dựng runtime (mặc định `.opsense/config.toml`). |
| `opsense_status()` | Topology từng node + watermark/cursor + danh sách trạm. |
| `opsense_edit({nodes})` | Sửa pipeline realtime — truyền **danh sách đầy đủ** các node mong muốn. |
| `opsense_run({node, ts?})` | Trigger thủ công: bơm tín hiệu `tick(ts)` vào một node. |
| `opsense_query({source, stage?, metric?, from_ts?, to_ts?})` | Đọc observation từ **station** (id của node). Tầng persistence chung đã bị gỡ. |
| `opsense_backfill({node, from_ts, to_ts})` | Ép `http_source` re-fetch đúng cửa sổ `(F, T]`. Watermark KHÔNG lùi. |
| `opsense_list_stations()` | Liệt kê id mọi trạm đã đăng ký (process-global). |
| `opsense_describe({id?})` | Không truyền id → liệt kê id; truyền id → JSON mô tả station (backend, schema, params, metrics, dependencies). |
| `pattern_add({node, text})` / `pattern_get({node, text})` / `pattern_stats({node})` | Thao tác Aho-Corasick automaton của `pattern_station_transform`. |
| `catalog_search({node, pattern})` | Substring search trên `category_station_transform` (radix + KMP). |
| `opsense_kernel_run({code, path?})` | chạy code trong analysis kernel; `result` capture. |
| `opsense_kernel_health()` | health của backend hiện tại (`local` / runner). |
| `opsense_deinit()` | Đóng session, dừng pipeline. |

> Lưu ý giới hạn hiện tại: `opsense_run` (inject) **chỉ nhận node lá** (node
> không có node nào phía sau). Vì thế khi muốn trigger tay, hãy để node lá là
> điểm bơm — hoặc dùng `clock_source` như các mẫu dưới đây.

### Lưu trữ & con trỏ qua restart

- **Station là tầng lưu trữ DUY NHẤT** (in-memory, mỗi node một cái). Tầng
  `ObservationStore` chung (memory/duckdb/lmdb/s3) + `persist_sink` đã bị gỡ
  trong refactor 2026-08-26. Mỗi node tự ghi output vào station riêng theo
  `id`; downstream đọc cửa sổ `(cursor, ts]` qua `read_window` — merge cả hai
  stage raw+processed với dedup `(metric_id, ts)`.
- **Watermark sống qua restart**: `Watermarks` có journal JSON
  (`<data_dir>/watermarks.json`, tmp+rename atomic); restart tiếp tục đúng chỗ
  cũ thay vì backfill lại từ `initial_lookback_secs`. File hỏng thì log warn
  và khởi động sạch.

> `[storage]` trong config giờ **chỉ còn ý nghĩa** cho các thành phần
> `mirror` hoặc khi cần re-import lakehouse cũ; nếu không có thì để mặc định
> `backend = "memory"`, `data_dir = ".opsense/lakehouse"` — không tốn disk.

---

## 2. Biến template trong config

Mọi giá trị string của `http_source` (url/headers/params/body) đều đi qua
bộ render template với các biến sau:

| Biến | Nguồn | Ý nghĩa |
|---|---|---|
| `{{from_ts}}` | cursor của node | Đầu cửa sổ dữ liệu (giây). Chu kỳ đầu = `ts - initial_lookback_secs`. |
| `{{to_ts}}` | timestamp tín hiệu | Cuối cửa sổ dữ liệu (giây), cửa sổ nửa mở `(from_ts, to_ts]`. |
| `{{ts}}` | như `to_ts` | Alias cho tiện. |
| `{{tên}}` | `[attributes]` | Biến cấu hình, ví dụ `{{prom_url}}`. |
| `{{env.TÊN}}` | môi trường | Đọc trực tiếp biến môi trường `TÊN`. |

### Attributes và override bằng môi trường

```toml
[attributes]
prom_url = "http://127.0.0.1:9090"   # TODO: sửa thành URL Prometheus/VictoriaMetrics của bạn
site = "hcm"                         # tuỳ bạn đặt thêm — mọi entry đều thành {{...}}
```

- Biến môi trường `OPSENSE_ATTR_<TÊN_HOA>` **ghi đè** giá trị trong TOML:
  `OPSENSE_ATTR_PROM_URL=... opsense serve` (đừng commit secret vào file config —
  đưa token/API key vào env rồi tham chiếu `{{env.MY_TOKEN}}` trong headers).
- Chỉ có env, không khai báo trong TOML vẫn dùng được.

---

## 3. Quy tắc đặt tên `type`

Mọi component trong `[pipeline]` đều kết thúc bằng một hậu tố vai trò, nhìn
config là biết ngay node đó làm gì:

| Hậu tố | Vai trò | Type hiện có |
|---|---|---|
| `_source` | Đưa dữ liệu/tín hiệu vào pipeline | `clock_source`, `ingest_source` |
| `_transform` | Xử lý observations | `http_source`¹, `processor_transform`, `rhai_transform`, `pattern_station_transform`, `category_station_transform`, `timeseries_station_transform` |
| `_sink` | Đích cuối | `collector_sink`, `timeseries_station_sink` |

¹ `http_source` dù có đuôi `_source`, được khai báo `#[transform]` trong code
(`opsense-components/src/http.rs`) — đứng sau `clock_source` nên `opsense_status`
vẫn báo `Transform`. Đuôi `_source` ở đây nói về **vai trò dữ liệu** (nguồn
fetch từ bên ngoài), không phải vị trí graph.

Tên này được sinh tự động từ tên struct bởi macro `#[source]/#[transform]/#[sink]`
trong `opsense-macros`, nên component mới không thể lệch quy tắc.

Đối chiếu tên cũ → mới (cho config viết trước quy tắc):

| Cũ | Mới | Ghi chú |
|---|---|---|
| `clock_component` | `clock_source` | |
| `ingest_component` | `ingest_source` | |
| `http_source_component` | `http_source` | giờ là `#[transform]` |
| `processor_component` | `processor_transform` | |
| `rhai_transform_component` | `rhai_transform` | trong crate `opsense-rhai` |
| `persist_component` | _đã xoá_ | persistence chung bị gỡ, mỗi node có station riêng |
| `station_sink` | `timeseries_station_sink` | chỉ còn RAM hot + HTTP endpoint (không còn cold LMDB) |
| `ahocorasick_transform` | `pattern_station_transform` | Aho-Corasick, kèm hit/miss counter |
| `catalog_transform` | `category_station_transform` | Radix + KMP key/value index |

---

## 4. Pipeline mẫu A — playground không clock

Không có `clock_source`: graph chỉ chạy khi bạn chủ động bơm tín hiệu bằng
`opsense_run`. Phù hợp để thử script Rhai trên dữ liệu bơm tay:

```toml
[[pipeline.components]]
type = "ingest_source"
id = "ingest"

[[pipeline.components]]
type = "rhai_transform"
id = "mean"
inputs = ["ingest"]
script_path = "scripts/moving_avg.rhai"

[[pipeline.components]]
type = "timeseries_station_sink"
id = "mean-station"
inputs = ["mean"]
# bind = "127.0.0.1:9190"             # mở endpoint Prometheus-style
```

Workflow: `opsense_init` → `opsense_run({node: "mean-station", ts: <unix giây>})`
→ `opsense_query({source: "mean-station", stage: "processed", metric: "..."})`
xem kết quả → sửa script → `opsense_run` lại.

> Trước đây đuôi pipeline là `persist_sink` đẩy xuống store chung. Sau refactor
> 2026-08-26 node lá nên là `timeseries_station_sink` (lưu + query được) hoặc
> `collector_sink` (đẩy ra collector; dùng khi test nguồn cũ).

---

## 5. Pipeline mẫu B — kéo VictoriaMetrics/Prometheus bằng `http_source`

Node HTTP generic: mỗi chu kỳ nó render request từ template, gọi API, map
response thành observations, và **chỉ hỏi đúng phần dữ liệu mới** kể từ chu
kỳ trước.

```toml
[[pipeline.components]]
type = "clock_source"
id = "clock"
interval_secs = 60              # nhịp kéo: mỗi 60s một cửa sổ mới

[[pipeline.components]]
type = "http_source"
id = "prom"
inputs = ["clock"]
url = "{{prom_url}}/api/v1/query_range"
initial_lookback_secs = 900     # chu kỳ đầu backfill 15 phút
timeout_secs = 120
station = true                  # đăng ký station riêng: query được qua MCP/REPL/HTTP
# bind = "127.0.0.1:9290"       # mở endpoint Prometheus-style trên station này

# Mapping khai báo thuần qua opsense_libs::jq (không cần script):
# `items` chọn mảng phần tử cần phát observation; `fields` chỉ định với mỗi
# output field đường path lấy giá trị (+ cast_to); `constants` gắn giá trị
# tĩnh vào mọi observation.
items = "data.result[].values[]"

[pipeline.components.fields]
ts = { query = "0", cast_to = "i64" }
value = { query = "1", cast_to = "f64" }

[pipeline.components.constants]
metric_id = "up"
labels = { job = "node_exporter" }

[pipeline.components.params]    # key giữ nguyên, value là template
query = "up"                    # PromQL tuỳ ý, ví dụ: sum by (job) (rate(http_requests_total[5m]))
start = "{{from_ts}}"
end = "{{to_ts}}"
step = "120"                    # bước lấy mẫu (giây)

[[pipeline.components]]
type = "timeseries_station_sink"
id = "prom-station"
inputs = ["prom"]
```

Cách hoạt động theo thời gian:

1. Clock phát `tick(now)`; node `prom` đọc cursor riêng của mình (ban đầu = 0).
2. Chu kỳ đầu: `from_ts = now - initial_lookback_secs`; các chu kỳ sau: `from_ts = cursor`.
3. Request được render (`start={{from_ts}}`, `end={{to_ts}}`) rồi gọi endpoint.
4. Thành công → observations vào station riêng của node `prom`, **cursor tiến tới `now`**.
5. Thất bại (HTTP != 2xx, template/jq/cast lỗi, JSON sai schema) → log warn,
   **cursor giữ nguyên** → chu kỳ kế sẽ thử lại nguyên cửa sổ bị lỡ (không mất
   dữ liệu, không nhân đôi).

Chỉ cần đổi `query` để theo dõi metric khác. Muốn nhiều query song song: thêm
nhiều node `http_source` cùng inputs `["clock"]`, id khác nhau.

### Mapping response bằng `items` + `fields` (jq)

Body JSON bất kỳ được map thành observations bằng bộ khai báo (engine
`opsense_libs::jq::JsonQuery` — chỉ hỗ trợ truy cập trường `.field`, index số
`"0"`, lặp mảng `[]` và `select(a, b, ...)`; **không** có pipe `|` hay object
literal):

- `items` — path chọn **mảng phần tử** để phát mỗi phần tử một observation.
  Bỏ trống: body tự là mảng (hoặc một object) observation-shape, node parse
  thẳng qua `observations_from_body`.
- `fields` — mỗi entry là một output field (`ts`, `value`, `metric_id`,
  `kind`, `signal`, `labels`, `severity`…): `query` là path lấy giá trị từ
  từng item, `cast_to` (i64/f64/string/bool) ép kiểu khi API trả string/number
  lẫn lộn. Field không pick được thì bỏ qua (warn).
- `constants` — key/value tĩnh merge vào mọi observation (đặt `metric_id`,
  `labels`, `signal`… cố định).

Ví dụ với response Prometheus `/query_range` (matrix
`data.result[i].values[j] = [ts, "value"]`):

```toml
items = "data.result[].values[]"        # mỗi sample một observation
[pipeline.components.fields]
ts = { query = "0", cast_to = "i64" }     # phần tử đầu của pair
value = { query = "1", cast_to = "f64" }  # phần tử sau (string → f64)
[pipeline.components.constants]
metric_id = "up"
labels = { job = "node_exporter" }
```

Lưu ý: jq map gắn **cùng một bộ constants** cho mọi item — nếu query trả
nhiều series cần `metric_id`/`labels` khác nhau, hãy tách thành nhiều node
`http_source` (mỗi node một query lọc đúng một series), hoặc lọc ở phía API.

### Bật station cho `http_source` để query lại

```toml
[[pipeline.components]]
type = "http_source"
id = "prom"
inputs = ["clock"]
url = "{{prom_url}}/api/v1/query_range"
initial_lookback_secs = 900
timeout_secs = 30
station = true                 # đăng ký station với id = "prom"
bind = "127.0.0.1:9290"        # mở endpoint Prometheus-style (truy vấn lại history)
block_secs = 300
max_hot_blocks = 288
max_hot_mb = 256
data_dir = ""                  # để trống = chỉ RAM; đặt path để bật cold LMDB
cold_retention_secs = 0        # 0 = giữ vĩnh viễn
```

Trường hợp dùng `station = true` thì **không cần thêm `timeseries_station_sink`**
cho cùng dữ liệu — station của `http_source` đã đủ.

---

## 6. Station (trạm) — cache + index theo node

> Hướng dẫn viết script Rhai (query trạm, toán tử `ts_*`, grid analysis, ví dụ
> cảnh báo đĩa) đã tách thành tài liệu riêng: [`RHAI.md`](./RHAI.md).

### Mô hình lưu trữ: station là cache DUY NHẤT

Từ refactor 2026-08-26, **tầng persistence chung bị bỏ**. Mỗi node sinh dữ
liệu (`http_source`, `ingest_source`, `processor_transform`, `rhai_transform`,
…) tự ghi output vào **station riêng** đăng ký theo node id trong
`OpsenseContext.stations` (process-global). Downstream đọc cửa sổ
`(cursor, ts]` qua `read_window` — merge cả hai stage raw+processed với dedup
`(metric_id, ts)`, nên pass-through chain vẫn chạy đúng.

`Station` enum có **3 hình thái** (mỗi node publish một trong ba, hoặc nhiều
station cùng tồn tại trong registry):

| Hình thái | Cách bật | Dùng khi |
|---|---|---|
| **`Timeseries`** | `http_source` + `station = true` (+ `bind/block_secs/max_hot_blocks/max_hot_mb/data_dir/cold_retention_secs`) hoặc `timeseries_station_sink` | Mọi thứ cần cache time-series; mặc định cho mọi nguồn dữ liệu metric |
| **`Pattern`** | `pattern_station_transform` (Aho-Corasick multi-pattern) | Log/text stream: đánh dấu known/unknown theo pattern |
| **`Category`** | `category_station_transform` (Radix + KMP key/value index) | Catalog / từ điển / metadata, cho phép substring search |

### Endpoint HTTP của station (chỉ `Timeseries`)

`TimeseriesStationSink` (và `http_source` với `station = true`) có thể mở
endpoint HTTP nói tiếng Prometheus. Endpoint cố định:

| Route | Ý nghĩa |
|---|---|
| `GET /api/v1/query_range?query=&start=&end=` | Matrix kiểu Prometheus; selector v1: tên metric + `{label="v"}` equality. |
| `GET /api/v1/query?query=&time=` | Instant vector: điểm mới nhất mỗi series trong lookback (mặc định 300s). |
| `GET /observations?metric=&from_ts=&to_ts=&stage=` | JSON gốc `Vec<Observation>` để debug. |
| `GET /health` | Sống hay chưa. |

Hai cách tiêu thụ lịch sử:

1. **Qua HTTP** — một `http_source` khác trỏ URL về trạm thay cho
   Prometheus/VictoriaMetrics; `scripts/prom_query_range.rhai` chạy nguyên bản
   vì trạm trả đúng envelope `{"status":"success","data":{"result":[…]}}`.
2. **Trực tiếp trong script Rhai** — handle được publish vào registry toàn
   cục theo `id`, script gọi `ts_query()` / `ts_mean()` để so hiện tại với
   quá khứ.

Ví dụ cấu hình `timeseries_station_sink`:

```toml
[[pipeline.components]]
type = "timeseries_station_sink"
id = "tsdb"
inputs = ["processor"]
bind = "127.0.0.1:9190"
block_secs = 300                       # độ rộng block LRU
max_hot_blocks = 288                   # trần số entry
max_hot_mb = 256                       # trần dung lượng (xấp xỉ)
data_dir = ""                          # để trống = chỉ RAM
cold_retention_secs = 0                # 0 = giữ vĩnh viễn
```

Cơ chế dọn dẹp — **RAM chỉ dọn khi đầy**:

- **Hot tier (RAM)**: evict theo entry `(stage, metric)` ngay khi vượt
  `max_hot_blocks` / `max_hot_mb`, cũ nhất trước. Không có TTL: cache RAM không
  cần dọn theo thời gian, trần dung lượng đã đủ chặn.
- **Cold tier (LMDB, optional)**: khi `data_dir` khác rỗng, dữ liệu cũ evict
  khỏi RAM vẫn còn trong LMDB; truy vấn ghép cold + overlay hot, dedup
  `(metric_id, ts)`.
- **Retention theo thời gian**: `cold_retention_secs > 0` bật task nền range-
  delete key cũ hơn N giây (chỉ áp dụng khi có cold tier).

> Lịch sử: trước 2026-08-26 trạm còn field `hot_ttl_secs` (đã bỏ), layout
> `dt=/hour=` (đã thay bằng LRU `(stage, metric)`), `delete_before` qua LMDB
> cold riêng. Giờ chỉ còn LRU thuần sync + cold tier optional.

### Phục hồi dữ liệu bị evict (`opsense_backfill`)

Trạm RAM chỉ giữ theo cap (`max_hot_blocks` / `max_hot_mb`) — entry cũ nhất
bị đẩy ra trước. Khi cần lại dữ liệu cũ:

```json
opsense_backfill({ "node": "vms-disk-usage", "from_ts": <F>, "to_ts": <T> })
```

Node `http_source` sẽ re-fetch đúng cửa sổ `(F, T]` từ nguồn gốc (template
render lại bình thường), watermark KHÔNG lùi nên luồng thu thập thường không
ảnh hưởng.

### Hàm Rhai đọc lịch sử

| Hàm | Trả về |
|---|---|
| `ts_query(id, stage, metric, from_ts, to_ts)` | Array observation maps; `()` nếu không có station id đó. |
| `ts_mean(id, stage, metric, from_ts, to_ts)` | f64 trung bình cửa sổ; `()` nếu rỗng/không có station. |
| `ts_rate(points)` | (cuối − đầu) / Δt của một array observation map; `()` nếu rỗng / chia 0. |
| `ts_moving_avg(points, window_secs)` | Array `{ts, value}` trung bình trượt theo thời gian. |
| `ts_resample(points, bucket_secs, agg)` | Gom nhóm theo block thời gian; `agg` ∈ `avg\|min\|max\|sum\|count`, trả array `{ts, value}`. |
| `ts_quantile(points, q)` | Phân vị q∈[0,1]; có đường sugar `ts_p95` / `ts_p99`. |
| `ts_delta(points)` | Độ lệch điểm cuối so với điểm đầu. |
| `ts_pct_change(points)` | % thay đổi (cuối − đầu)/đầu. |

Các toán tử `ts_*` nhận array các observation-map (định dạng giống `ts_query`)
và trả lại `Dynamic` (`()` / số / array-of-map) để nối chuỗi trong Rhai.

```rhai
fn process(observations) {
    let base = ts_mean("tsdb", "processed", "cpu_usage", now_secs() - 3600, now_secs());
    // gắn baseline vào output để đánh giá giá trị hiện tại...
}
```

> Registry giữ handle **đầu tiên** đăng ký theo id (như `ScriptRunner`): sửa
> tham số trạm (block/bind/caps…) có hiệu lực sau khi restart session.

---

## 7. Thí nghiệm nhanh dạng test Rust (khi cần "cào thật rồi phân tích")

Khi muốn thử một ý tưởng phân tích trên **dữ liệu thật** mà chưa muốn chỉnh
config, hãy tự viết một test `#[ignore]` — đây là chỗ khai báo thí nghiệm
của bạn:

1. Copy harness từ
   [`crates/opsense-rhai/tests/http_format.rs`](../crates/opsense-rhai/tests/http_format.rs)
   thành file mới, ví dụ `crates/opsense-rhai/tests/my_experiment.rs`.
2. Trong test, thay mock server bằng endpoint thật của bạn (URL lấy từ
   `[attributes]`/`OPSENSE_ATTR_*`) và khai báo `items`/`fields`/`constants`
   jq mapping tuỳ ý.
3. Đánh dấu `#[ignore]` để nó không chạy trong CI, rồi chạy có chọn lọc:

```bash
cargo test -p opsense-rhai --test my_experiment -- --ignored --nocapture
```

`--nocapture` giúp in thẳng output ra console để bạn xem và tinh chỉnh
config. Khi ưng ý, chuyển nội dung đó thành `http_source` trong config
chính thức.

---

## 8. Xử lý sự cố

- **`unknown template variable ...`** — biến trong `{{...}}` không có trong
  attributes/env; kiểm tra chính tả và `OPSENSE_ATTR_*`.
- **`http <id> skipped window ...`** — endpoint lỗi; xem log warn, cursor
  không tiến, cửa sổ sẽ được thử lại ở tick sau. Kiểm tra url/params/auth.
- **`http <id> field \`x\` cannot cast to ...`** — `cast_to` không áp dụng
  được lên giá trị jq pick được; kiểm tra path `query` và kiểu trả về của API.
- **`http_source`: unknown field `store_raw`** — `store_raw` đã bị gỡ khi
  `persist_sink` biến mất. Mọi observation giờ mặc định vào station của node
  `http_source`; nếu muốn endpoint Prometheus-style, bật `station = true`.
- **Reload báo transform thiếu kết nối** — transform phải có input VÀ phải có
  node nào đó đứng sau; node cuối pipeline nên là `timeseries_station_sink`
  hoặc `collector_sink`.
- **`opsense_run` từ chối node** — node đó không phải node lá; bơm vào node
  cuối hoặc dùng clock.
- **`station <id> cannot bind ...`** — port trạm bị chiếm; đổi `bind` hoặc tắt
  tiến trình cũ. Trạm giữ handle đầu tiên đăng ký — sửa config trạm cần
  restart session để áp dụng.
- **`station_sink`: unknown field `hot_ttl_secs`** — khoá này đã bỏ từ
  2026-08-26; cache RAM chỉ dọn khi đầy (`max_hot_blocks` / `max_hot_mb`).
  `cold_retention_secs` (nếu có `data_dir` cold LMDB) cũng đã đổi tên từ
  `retention_secs`.
- **`pipeline graph contains a cycle: a -> b -> a`** — config hoặc
  `opsense_edit` vừa tạo vòng dependency giữa các node; bỏ một cạnh `inputs`
  hoặc tách đường vòng qua node trung gian rồi nạp lại.
- **`query timed out after Ns`** — query trạm/MCP vượt
  `OPSENSE_QUERY_TIMEOUT_SECS` (mặc định 30s). Nâng giới hạn hoặc thu hẹp
  cửa sổ `from_ts/to_ts`. Script Rhai riêng có `OPSENSE_RHAI_TIMEOUT_SECS`.
- **`opsense_query({source: "persistence"})` lỗi** — tầng persistence chung đã
  bị gỡ. Truyền `source` là **station/node id** (ví dụ `"prom"` nếu node
  `http_source` đó bật `station = true`).
- **Watermark không lùi khi đổi config** — đúng: cursor sống qua restart nhờ
  `<data_dir>/watermarks.json`. Muốn backfill lại từ đầu, xoá file này.

---

## 9. Execution kernel & runner (kiến trúc IPC + gRPC)

Từ 2026-08-25, analysis execution chạy **kernel process riêng** nói framed IPC
(protobuf control + Arrow data); không còn Python embed. serve ↔ runner nói gRPC.

```bash
# Kernel mặc định: opsense-kernel-python; dev/test có thể trỏ sang echo
export OPSENSE_KERNEL_BIN=target/debug/opsense-kernel-echo

# Một process, mọi transport:
opsense serve --repl --runner-bind 127.0.0.1:50051 --mcp --http --port 8123
#   ├── REST gateway (/health /reload /sources /metrics)
#   ├── /mcp                     MCP Streamable HTTP (mount trong routes())
#   ├── KernelRunner gRPC        tại --runner-bind
#   └── REPL terminal            (chiếm stdin)

# Runner độc lập (máy khác sau này):
opsense runner 127.0.0.1:50051
```

### REPL

| Lệnh | Ý nghĩa |
|---|---|
| `:runner list` | backend hiện có (`local` + các runner đã connect) |
| `:runner connect <host:port> [name]` | attach runner và chuyển session hiện tại sang nó |
| `:kernel local` / `:kernel <name>` | đổi backend giữa chừng — `@var`, history giữ nguyên |
| `:station [list\|describe <id>\|use <id>]` | quản lý station đang focus |
| `:query <metric> [--stage raw\|processed] [--from 24h] [--to 0]` | lấy observation từ station, lưu vào `@N` |
| `:stats` / `:plot` | thống kê & matplotlib |
| `:pattern <add\|get\|stats> <node> [text]` | Aho-Corasick trên pattern_station_transform |
| `:catalog <node> <pattern>` | substring search trên category_station_transform |
| `:new` / `:use` / `:close` / `:save` / `:load` | quản lý session |
| `:sessions` | liệt kê session |
| `:vars` / `:ls` | các biến Arrow trong namespace |

### Env cấu hình

`OPSENSE_RUNNER_BIND` (default bind của runner, mặc định `127.0.0.1:50051`),
`OPSENSE_MCP_PORT`, `OPSENSE_KERNEL_BIN` (chọn kernel binary),
`GATEWAY_LISTENER`/`GATEWAY_ADDR` (unix socket/http cho gateway),
`OPSENSE_QUERY_TIMEOUT_SECS` (timeout MCP query, mặc định 30s),
`OPSENSE_RHAI_TIMEOUT_SECS` (timeout script Rhai).

---

## 10. Python & Julia sub-REPL (`:py` / `:jl`)

Gõ `:py` **không có code** → chuyển sang Python sub-REPL. Tương tự `:jl` cho
Julia.

```text
opsense> :py
Python kernel [session abc] — :block=multi-line | :line=single | exit()=back

python> import numpy as np          ← Enter = chạy ngay (LINE mode)
python> x = np.array([1,2,3])
python> result = x.mean()
2.0
python> :block                      ← chuyển sang BLOCK mode
... def analyze(df):                ← Enter = thêm dòng (KHÔNG chạy)
...     return df.describe()
...                                 ← dòng RỖNG = gửi cả block xuống kernel
>>> exit()                          ← quay về opsense>
opsense>
```

| Lệnh trong sub-REPL | Ý nghĩa |
|---|---|
| `:block` | Chuyển sang multi-line: Enter thêm dòng, dòng rỗng chạy cả block |
| `:line` | Quay lại single-line: Enter chạy ngay |
| `exit()` / `:quit` / Ctrl-D | Thoát sub-REPL, về opsense |

### `_df_N` — truy cập station data trong Python

Khi bạn dùng `:query <metric> --from 1h`, kết quả lưu vào `@N`. Trong Python
sub-REPL, `@N` **tự động thành pandas DataFrame** với tên `_df_N`:

```text
opsense> :query cpu_usage --from 1h     # -> @1
100 rows [cpu_usage raw ...] -> @1
opsense> :py
python> print(_df_1.shape)              # @1 → _df_1
(100, 5)
python> _df_1['value'].describe()
count    100.000000
...
```

Quy tắc đặt tên: `@1` → `_df_1`, `@2` → `_df_2`, v.v.
Mọi DataFrame từng push vào session đều available cho đến khi session đóng.

### Julia kernel

```bash
# Cần Julia + Arrow.jl trong depot:
julia -e 'using Pkg; Pkg.add("Arrow")'

# Chạy REPL:
export OPSENSE_KERNEL_JULIA=target/debug/opsense-kernel-julia
opsense serve --repl
opsense> :jl
julia> using Statistics
julia> mean([1,2,3])
2.0
julia> exit()
```

Python kernel cần packages: `numpy pandas pyarrow scipy scikit-learn statsmodels matplotlib protobuf`.
Khuyến nghị dùng venv: `uv venv .venv && uv pip install … && export OPSENSE_PYTHON=$PWD/.venv/bin/python`.

---

## 11. Log pattern matching (`pattern_station_transform`)

Node `pattern_station_transform` duy trì một **Aho-Corasick automaton** chứa
các log pattern đã biết. Khi log stream qua, mỗi entry được đánh dấu:
`pattern_matched=true` nếu text match ít nhất một pattern, `false` nếu là log
mới (anomaly tiềm ẩn). Hit/miss được đếm atomic, query được qua MCP/REPL.

### Config mẫu

```toml
[[pipeline.components]]
type = "clock_source"
id = "clock"
interval_secs = 60

[[pipeline.components]]
type = "http_source"
id = "logs"
inputs = ["clock"]
url = "{{prom_url}}/api/v1/query?query={{log_query}}"
format = "script"
script_path = "scripts/log_format.rhai"

[[pipeline.components]]
type = "pattern_station_transform"
id = "log-matcher"
inputs = ["logs"]

[[pipeline.components]]
type = "timeseries_station_sink"
id = "logs-store"
inputs = ["log-matcher"]
```

### Thêm patterns + xem stats

**REPL:**
```text
opsense> :pattern add log-matcher OOM killed pod
Pattern added to `log-matcher`
opsense> :pattern get log-matcher "OOM killed pod nginx"
KNOWN → 2026-08-26 OOM killed pod nginx
```

**MCP:**
```json
{"tool": "pattern_add",   "args": {"node": "log-matcher", "text": "OOM killed"}}
{"tool": "pattern_get",   "args": {"node": "log-matcher", "text": "..."}}
{"tool": "pattern_stats", "args": {"node": "log-matcher"}}
```

**Rhai script:**
```rhai
fn process(observations) {
    observations.map(|o| #{
        ts: o.ts,
        metric_id: o.metric_id,
        kind: o.kind,
        signal: o.signal,
        value: o.value,
        labels: #{ known: pattern_is_known("log-matcher", o.metric_id) },
    })
}
```

> Đổi tên từ `ahocorasick_transform` (cũ) → `pattern_station_transform` (mới).

---

## 12. Key/value catalog (`category_station_transform`)

Node `category_station_transform` nhận observations và index vào một **Radix
trie + KMP** cho phép substring search nhanh trên keys, kèm bảng key/value
để trả về kết quả dạng `(key, value)`.

### Config mẫu

```toml
[[pipeline.components]]
type = "category_station_transform"
id = "svc-catalog"
inputs = ["prom"]           # index metrics từ http_source
key_field = "metric_id"    # field dùng làm key (mặc định: metric_id)
```

### Search từ REPL/MCP

```text
opsense> :catalog svc-catalog cpu
┌──────────────────┬─────────────────────┐
│ key              │ value               │
├──────────────────┼─────────────────────┤
│ cpu_usage        │ {"team":"sre",...}  │
│ cpu_pressure     │ {"team":"infra"...} │
```

**MCP:** `catalog_search({node: "svc-catalog", pattern: "cpu"})`

**Rhai script:**
```rhai
fn process(observations) {
    // Index entries into catalog
    catalog_insert("svc-catalog", "new_metric", "{\"desc\":\"test\"}");
    // Search
    let results = catalog_search("svc-catalog", "cpu");
    ...
}
```

> Đổi tên từ `catalog_transform` (cũ) → `category_station_transform` (mới)
> để thể hiện rõ nó là một `Station` thay vì transform thuần.
