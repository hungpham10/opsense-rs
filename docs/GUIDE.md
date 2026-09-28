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

### Token (bắt buộc khi tenant bật OIDC)

Mọi lệnh trên đều là client của `opsense serve`, nên cần bearer token:

```bash
./scripts/mint-dex-token.py          # gia hạn nếu được, không thì đăng nhập; ghi ~/.config/opsense/token (mode 0600)
./scripts/mint-dex-token.py --login  # ép đăng nhập lại, bỏ qua refresh
./scripts/mint-dex-token.py --print  # chỉ in ra, để dán vào env của MCP client
```

Token đọc theo thứ tự (`client/graphql.rs:422`): `OPSENSE_ACCESS_TOKEN` **thắng**, kể cả khi sai (không fallback); không có thì đọc file. Env rỗng bị coi là "không khai" nên vẫn rơi về file.

Dex cấp token **24 giờ** — khoảng một lần/ngày. Triệu chứng hết hạn là mọi lệnh trả `401`. Khi mới lấy, script in sẵn thời hạn:

```
wrote ~/.config/opsense/token (780 chars, mode 0600)
  user=dev-user@example.com  còn 24.0 giờ (hết lúc 2026-09-29 15:21:41)
```

Script tự gia hạn bằng **refresh token** (`offline_access`, lưu ở `~/.config/opsense/refresh_token`): token còn hạn quá 5 phút thì không gọi mạng, sắp hết hạn thì 1 request, không có refresh token thì mới qua form đăng nhập. Dex **xoay** refresh token mỗi lần dùng nên script luôn lưu bản mới nhất; refresh token hỏng thì báo một dòng rồi rơi về đăng nhập.

Restart `opsense-dex` cũng làm token cũ hết giá trị (Dex giữ phiên trong RAM).

Nối vào MCP client (stdio) — endpoint mặc định là `http://localhost:8080/api/repl/graphql`, đổi bằng `OPSENSE_GRAPHQL_URL`:

```json
{
  "mcpServers": {
    "opsense": {
      "command": "/đường/dẫn/tới/opsense",
      "args": ["mcp"],
      "env": { "OPSENSE_GRAPHQL_URL": "http://localhost:8080/api/repl/graphql" }
    }
  }
}
```

### Ba loại token — đừng nhầm

Mọi client (`opsense status`, `mcp`, `repl`) đều là client của `opsense serve`, nên đều cần token. Nhưng có **ba** loại, và chúng khác nhau cả về hạn lẫn cách lấy:

| | Hình dạng | Hạn | Lấy bằng | Dùng để |
|---|---|---|---|---|
| `id_token` (Dex) | JWT 3 phần, `eyJ…` | 24h | `scripts/mint-dex-token.py` | Đăng nhập thường, không cần trình duyệt |
| `abt_…` (device flow) | chuỗi ngẫu nhiên, **1 phần** | 8h | `repl :login` (mở trình duyệt) | Token cho CLI/MCP, không phụ thuộc phiên Dex |
| refresh token (device flow) | chuỗi ngẫu nhiên | 8h, **cứng** | kèm `abt_` | Chỉ script dùng để gia hạn |

`~/.config/opsense/token` chứa token nào tuỳ lệnh bạn chạy gần nhất. Đọc dạng
thì biết ngay: 3 phần là `id_token`, 1 phần là `abt_`.

### `repl :login` — chạy trọn device flow trong một lệnh

```bash
opsense repl
:login
# → in URL + user_code → bạn mở trình duyệt, nhập code, bấm Authorize
# → ghi abt_ token vào ~/.config/opsense/token
```

**`:login` KHÔNG cần đăng nhập Dex trước.** Nó tự chạy cả flow. Lệnh `:login`
gọi `request_device_code` → in ra URL để bạn mở → `poll_token` → lưu
`access_token` (`repl/commands.rs:314`).

Hết hạn nó báo đúng: *"Restart REPL (`:quit` then `opsense repl`) to use it."*

Trường hợp này bắt buộc mở trình duyệt, vì bước `device/verify` cần người dùng
thật bấm nút duyệt. Đổi provider sang Auth0 thì `:login` vẫn chạy (nó đi
authorization_code flow chuẩn, provider lo phần đăng nhập), nhưng
`scripts/mint-dex-token.py` thì **không** — script giả lập POST form
`login`+`password`, còn Auth0 dùng Universal Login (Google/MFA/CAPTCHA), không
có form tĩnh để POST vào. Với provider khác Dex thì dùng `:login`.

### `opsense mcp` không tự đăng nhập được

9 tool của MCP (`mcp/server.rs`) không có tool auth nào. `login_and_save_token`
(`client/auth.rs:150`) là device flow dành cho CLI — nó in
`Open this URL in your browser` rồi poll — nhưng **không có caller nào** và MCP
là stdio (không có ai để bấm nút). Nên:

```bash
./scripts/mint-dex-token.py     # hoặc :login
# rồi RESTART MCP client — token được nạp MỘT LẨN lúc khởi động
```

Sửa file `token` khi MCP đang chạy **không có tác dụng**: `OpsenseClient::new`
đọc token một lần rồi giữ trong RAM (`client/graphql.rs:190`).

### Token dài hạn (vĩnh viễn) cho tự động hoá

Cả hai loại trên đều hạn ngắn (24h / 8h). Muốn token không hết hạn để chạy
script dài hạn, phát bằng API admin (một người một token, `UPSERT` theo
`tenant_id + user_id`):

```bash
# Lấy `sub` của chính mình từ id_token
SUB=$(python3 -c "
import base64,json,os
t=open(os.path.expanduser('~/.config/opsense/token')).read().strip()
d=t.split('.')[1]; d+='='*(-len(d)%4)
print(json.loads(base64.urlsafe_b64decode(d))['sub'])")

curl -s -XPOST http://localhost:8080/api/admin/v1/tokens/users \
  -H "Authorization: Bearer $(cat ~/.config/opsense/token)" \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\": \"$SUB\"}"        # bỏ expires_at ⇒ NULL ⇒ vĩnh viễn
# → {"token":"abt_…"}   dùng token này thay ~/.config/opsense/token
```

- Có `"expires_at": "2026-12-31T23:59:59Z"` thì đặt hạn cụ thể.
- `GET /api/admin/v1/tokens/users/{user_id}` trả lại plaintext.
- `DELETE /api/admin/v1/tokens/users/{user_id}` thu hồi (set `revoked_at`, token
  cũ chết ngay, hàng vẫn còn trong DB).

Token phát ra là **một lần duy nhất** — mất thì lấy lại bằng `GET` ở trên.

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
