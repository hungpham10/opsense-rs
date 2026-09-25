# Opsense — Hướng dẫn sử dụng

> Tài liệu này mô tả **đúng những gì code làm** (đối chiếu bằng `opsense --help`
> và `opsense validate`). Kiến trúc: [`architecture.md`](./architecture.md).

## 1. Vòng đời cơ bản

```bash
# 1) Sinh config mẫu (mặc định .opsense/config.toml; KHÔNG ghi đè nếu đã có)
opsense init
opsense init path/to/my.toml --force   # ghi đè có chủ đích

# 2) Sửa config: [attributes] + bỏ comment MỘT khối [[pipeline.components]]
$EDITOR .opsense/config.toml
opsense validate                        # parse + dựng graph, bắt lỗi type sai

# 3) Chạy
OPSENSE_CONFIG=.opsense/config.toml opsense serve

# 4) Xem nó hoạt động
opsense status                          # node + station
opsense components                      # cấu hình đang chạy (kể cả params)
opsense query <station> --limit 20      # đọc observation
```

Client khác:

```bash
opsense repl            # REPL tương tác (:status :attr :node :query :login)
opsense mcp             # MCP stdio (Claude Desktop, IDE…)
curl -XPOST localhost:8080/api/repl/graphql -d '{"query":"{ status { nodes { id } } }"}'
```

Cờ `serve --repl` / `--mcp` / `--runner-bind` **không còn** — dùng subcommand.

### Các MCP tool

`opsense mcp` là client mỏng của `opsense serve`: mỗi tool = 1 round-trip
`POST /api/repl/graphql`.

| Tool | API | Ý nghĩa |
|---|---|---|
| `opsense_status()` | `Query.status` | Topology node + danh sách station. |
| `opsense_get_config({id?})` | `Query.components` | Cấu hình **đang chạy** (kể cả `params` của script). Bỏ `id` → toàn bộ pipeline. **Đọc trước khi sửa.** |
| `opsense_attributes()` | `Query.attributes` | Attribute trong memory. |
| `opsense_set_attribute({name, value})` | `Mutation.setAttribute` | Set attribute (cảnh báo nếu `OPSENSE_ATTR_<NAME>` đang ghi đè). |
| `opsense_remove_attribute({name})` | `Mutation.removeAttribute` | Xoá attribute. |
| `opsense_query_timeseries({node, from_ts?, to_ts?, limit?, signal?, label_kind?})` | `Query.queryTimeseries` | Đọc station, **có guard**, filter server-side, trả `truncated`. |
| `opsense_orders({node, status?})` | `Query.queryTimeseries` | Lệnh (`signal=order`, `labels.status=open\|closed`) **+** cursor T+N (`labels.kind=trading_step`). |
| `opsense_set_param({id, path, value})` | `Mutation.patchComponent` | Sửa **một** trường (JSON pointer). Đường sửa mặc định. |
| `opsense_reload({components_json})` | `Mutation.reload` | Thay **toàn bộ** danh sách node. Thô — thiếu một node là mất node đó. |

### Sửa cấu hình: đọc → patch → audit

```text
opsense get-param grid /params/sl_pct        # 0.008
opsense set-param grid /params/sl_pct 0.02   # chỉ đổi 1 trường
opsense components grid | jq .config.params   # xác nhận
```

- Server đọc cấu hình hiện tại → patch → **deserialize toàn bộ danh sách qua
  typetag** → mới reload. Patch sai kiểu ⇒ lỗi và **runtime giữ nguyên**.
- Mọi lần sửa được ghi vào station `opsense-audit`:
  `opsense query opsense-audit --label-kind config_edit`
  (`labels`: `node`, `path`, `from`, `to`).
- Sửa chỉ có tác dụng trong RAM tiến trình đang chạy; `config.toml` không tự
  cập nhật (restart sẽ trở về file).

### Lưu trữ & con trỏ qua restart

- **Station là tầng lưu state DUY NHẤT**: mỗi node ghi vào station riêng theo
  `id`; metric, log, snapshot, **lệnh giao dịch** và **cursor T+N** đều là
  `Observation` trong đó. Tầng `ObservationStore` chung + `persist_sink` đã bị gỡ.
- `[storage] backend` quyết định tầng bền cho **mọi station**:
  - `memory` (mặc định) — thuần RAM;
  - `parquet` — `<data_dir>/<id>-<kind>/` gồm `wal.log` (crash-safe), checkpoint
    (`tables/*.parquet` + `_current`) và timeseries `ts/blk=<block_id>/batch-*.parquet`;
  - `sqlite` — file local.
- Mirror S3-compatible: `[storage.s3]` → `s3://<bucket>/<prefix>/<id>/ts/**`, lịch
  sync là `s3_flush_interval_secs` / `s3_snapshot_interval_secs` ở `[storage]`.
  Creds fallback `OPSENSE_S3_*` rồi `AWS_*`. Local dev: `docker compose up -d rustfs
  rustfs-bucket`.

---

## 2. Biến template trong config

Mọi giá trị string của `http_source` (url/headers/body) đi qua bộ render template:

| Biến | Nguồn | Ý nghĩa |
|---|---|---|
| `{{from_ts}}` | cursor của node | Đầu cửa sổ dữ liệu (giây) |
| `{{to_ts}}` / `{{ts}}` | timestamp tín hiệu | Cuối cửa sổ (nửa mở) |
| `{{tên}}` | `[attributes]` | Biến cấu hình |
| `{{env.TÊN}}` | môi trường | Đọc trực tiếp biến môi trường |

Ngoài ra `bindings` của `http_source` cho phép gọi jq và đưa kết quả vào bảng
`{{...}}`:

```toml
[[pipeline.components]]
type = "http_source"
id = "prom-explore"
inputs = ["clock"]
url = "{{prom_url}}/api/v1/query_range"
bindings = { q = "\"[{.metric}]{}\"" }

  [pipeline.components.params]
  query = "up"
```

### Attributes và override bằng môi trường

```toml
[attributes]
prom_url = "http://127.0.0.1:9090"
```

- `OPSENSE_ATTR_PROM_URL=...` **ghi đè** giá trị trong TOML (đừng commit secret vào
  file config).
- Biến môi trường không khai trong TOML vẫn dùng được.
- Xem/sửa lúc chạy: `opsense attributes` qua MCP, hoặc `:attr` trong REPL.

---

## 3. Quy tắc đặt tên `type`

Tên component = tên struct ở dạng snake_case, sinh tự động bởi macro
`#[source]/#[transform]/#[sink]` (`opsense-macros`) nên không thể lệch quy tắc.
Danh sách đúng **18 loại** (binary tự liệt kê khi bạn dùng sai):

| Nhóm | Type |
|---|---|
| source | `clock`, `input`, `http_source`, `csv_source` |
| transform | `json_2_json`, `websocket_2_json`, `rhai_transform`, `processor_transform`, `timeseries_station_transform`, `category_station_transform`, `pattern_station_transform` |
| sink | `timeseries_station_sink`, `capture_sink`, `file_sink`, `print`, `telegram_sink`, `null` |
| khác | `output` (không dùng trong pipeline) |

Lưu ý: `http_source`/`csv_source` tên mang hậu tố `_source` nhưng được khai
`#[transform]` vì chúng vừa là nguồn vừa có thể đẩy xuống node khác.

Component có `station = true` tự đăng ký station theo `id` → query được ngay.
Node transform không phải sink thì **bắt buộc** có consumer (thường là `null`).

---

## 4. Pipeline mẫu A — không cần network

```toml
[[pipeline.components]]
type = "clock"
id = "clock"
interval_secs = 10

[[pipeline.components]]
type = "rhai_transform"
id = "double"
inputs = ["clock"]
script_path = "scripts/doubler.rhai"

[[pipeline.components]]
type = "timeseries_station_sink"
id = "double-station"
inputs = ["double"]
```

Đọc: `opsense query double-station`.

## 5. Pipeline mẫu B — Prometheus/VictoriaMetrics bằng `http_source`

```toml
[[pipeline.components]]
type = "http_source"
id = "prom-explore"
inputs = ["clock"]
url = "{{prom_url}}/api/v1/query_range?query=up"
interval_secs = 10
timeout_secs = 5
station = true
```

`http_source` chạy được cả hai kiểu:

- **JSON thô** (mảng observation) — dùng `items` + `fields` (jq) + `constants` để
  ánh xạ sang shape chuẩn; đây là đường dùng cho mock/e2e.
- **Candle (OHLCV)**: thêm `candle_parse_mode = "array"` (mảng row) — đường dùng
  cho Binance `klines`.

---

## 6. Station

- `TimeseriesStation` chia theo **block thời gian**; đọc bằng `query_range`,
  ghi bằng `update_range`.
- Đọc từ script: `station_query("id", from, to)` / `station_candles("id", …)`;
  từ CLI/MCP/REPL: `opsense query <id>` / `opsense orders <id>`.
- **Không có endpoint HTTP riêng cho station** và **không có tool backfill**; muốn
  kéo lại dữ liệu thì chỉnh `initial_lookback_secs` (hoặc `lookback_secs` cho
  trading) rồi restart/reload.
- Query có guard: `limit` mặc định 1000 (trần 10 000), cửa sổ trần 30 ngày, vượt trần
  thì báo lỗi kèm gợi ý; kết quả có `truncated`.

### Key/value catalog (`category_station_transform`)

Index key/value (Radix + KMP) để tra cứu chuỗi:

```toml
[[pipeline.components]]
type = "category_station_transform"
id = "instance-catalog"
inputs = ["prom-explore"]
key_field = "instance"     # mặc định "key"
value_field = "value"      # mặc định "value"
```

### Log pattern matching (`pattern_station_transform`)

```toml
[[pipeline.components]]
type = "pattern_station_transform"
id = "log-pattern"
inputs = ["http"]
text_field = "text"           # mặc định "text"
matched_field = "matched"     # mặc định "matched"
patterns = ["timeout", "oom", "panic"]
```

Kết quả ghi `matched: true/false` vào payload trước khi forward; node có bộ đếm
hit/miss để đọc lại qua station.

---

## 7. Script Rhai

Script định nghĩa `fn process(observations)`; nhận/trả mảng map. Xem
[`RHAI.md`](./RHAI.md). Trong config:

```toml
[[pipeline.components]]
type = "rhai_transform"
id = "stats-summary"
inputs = ["prom-explore"]
script_path = "strategies/prometheus/script.rhai"   # hoặc `script = '''…'''`

  [pipeline.components.params]
  window = 5
```

- File `.rhai` compile 1 lần, cache theo mtime → sửa file là batch sau dùng bản mới.
- `params` vào script dưới tên `param_<tên>`.
- `trigger()` cho biết message đến từ input edge nào (`payload.trigger` rồi `payload.src`).
- `attr(name)` đọc attribute của config.
- Ngân sách wall-clock: `OPSENSE_RHAI_TIMEOUT_SECS` (mặc định 30).

---

## 8. Trading realtime

Config + script mẫu: `strategies/binance/{config.toml,grid.rhai}`.

```toml
[params của node rhai]
mode = "analysis"     # hoặc "trading" → thêm bước đặt lệnh
strategy = "rhai"     # plan do `fn rebuild` trong grid.rhai dựng
```

```bash
opsense orders grid --status open                    # lệnh đang mở
opsense query grid --label-kind trading_step         # cursor T+N
opsense set-param grid /params/sl_pct 0.01          # đổi tham số, không restart
```

- Lệnh là observation `signal = "order"` (`labels.status = open|closed`), cursor là
  observation `labels.kind = "trading_step"` — cả hai trong station nên **sống qua
  restart**; kernel không giữ state trong RAM.
- Kernel chỉ giao trên nến **đã đóng**; T+N chặn bằng `settlement_candles`
  (`0` = theo thị trường) và `candle_seq` trong cursor.
- `strategy = "dag"` dùng genome ONNX thay cho script (xem
  [`TRADING_CORE_ANALYSIS.md`](./TRADING_CORE_ANALYSIS.md)).

---

## 9. Kernel & runner

```bash
opsense runner 127.0.0.1:50051                     # worker gRPC độc lập
opsense runner --health-check                       # dùng cho container healthcheck
opsense repl --runner 127.0.0.1:50051              # REPL nói thẳng runner, bỏ qua serve
```

Cấu hình runner: `~/.config/opsense/runner.json` (`OPSENSE_RUNNER_CONFIG`) → env
(`OPSENSE_RUNNER_BIND`, `OPSENSE_KERNEL`, `OPSENSE_SERVE_URL`, `OPSENSE_ADMIN_TOKEN`)
→ cờ CLI. Kernel sidecar: `opsense-kernel-python` (cần `numpy pandas pyarrow scipy
scikit-learn statsmodels matplotlib protobuf`), `-julia`, `-echo`.

Trong `repl --runner` có các lệnh: `:py` / `:jl` / `:echo` (mở session kernel),
`:inline` / `:block` (chế độ dòng lệnh vs khối), `:send`, `:abort`, `:quit`.

---

## 10. Xử lý sự cố

| Triệu chứng | Nguyên nhân thường gặp |
|---|---|
| `unknown variant '<type>'` khi validate | Sai tên component — xem bảng ở §3 (18 loại) |
| `transform non-sink phải có consumer` | Thiếu node lá, thường thêm `null` |
| `query timed out` / treo | Query cửa sổ quá rộng: chia nhỏ `--from/--to`, tăng `--limit` (đã có guard, nhưng vòng lặp client vẫn cần kỷ luật) |
| MCP báo lỗi kết nối | `opsense serve` chưa chạy, hoặc sai `OPSENSE_GRAPHQL_URL` |
| `set-param` báo lỗi deserialize | Giá trị sai kiểu cho field đó — xem `opsense components <id>` |
| Script không thấy thay đổi | File `.rhai` cache theo mtime; sửa file là batch kế tiếp dùng bản mới |
| Station trống | Node chưa `station = true`, hoặc query ngoài cửa sổ có dữ liệu |
| `serve` không khởi động | Thiếu `DB_DSN` (auth/admin cần DB) — dùng Postgres của compose hoặc `sqlite://opsense.db` |

---

## 11. Docker compose (local dev)

```bash
docker compose up -d --wait --wait-timeout 240     # postgres, valkey, dex, runner×3, serve, rustfs
docker compose down -v                              # dọn kèm volume
```

Image runtime được build bằng Earthly (`earthly +integration-images`); `opsense` đổi
binary thì cần rebuild image trước khi `docker compose up` (compose mount config,
không mount binary).
