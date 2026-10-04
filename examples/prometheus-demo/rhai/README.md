# Opsense playground — xử lý dữ liệu bằng script Rhai

Viết logic xử lý bằng **Rhai** (ngôn ngữ scripting nhúng, sandbox — không
filesystem/network, giới hạn số phép tính) rồi gắn vào pipeline như một node
`rhai_transform`. Đây là đường chính để thử nghiệm thuật toán trước
khi viết thành code thật.

Vòng lặp playground:

```
sửa .rhai → opsense_run (MCP) → opsense_query xem kết quả
```

Không cần build hay restart session: script file được **compile lại tự động**
khi đổi trên đĩa (kiểm tra mtime trước mỗi batch); inline script thì sửa config
rồi `opsense_edit` reload là xong.

## Hợp đồng script

Script phải định nghĩa một hàm:

```rhai
fn process(observations) {
    // observations: mảng observation map:
    //   { ts: int, metric_id: string, kind: "metric", signal: "...",
    //     value: float, labels: map }
    ...
    // trả về mảng observation mới (cùng schema)
}
```

Luồng dữ liệu của node (y hệt processor mặc định):

1. Nhận **bất kỳ signal mang `ts`** (`tick` / `data_ready` / `processed`).
2. Đọc cửa sổ `(cursor, ts]` từ LRU stage `input_stage`.
3. Gọi `process()` trong sandbox.
4. Kết quả ghi vào stage `output_stage` — vào LRU (`write_lru`, mặc định
   true), và/hoặc thẳng xuống store (`write_store`).
5. Phát `processed(ts)` downstream; cursor riêng của node (tên = `id`) tăng
   lên `ts`.

Script lỗi → batch bị bỏ qua, **cursor KHÔNG tiến**: cửa sổ sẽ được thử lại ở
tín hiệu kế tiếp, sửa xong script là tự phục hồi không mất dữ liệu.

## Cấu hình node

Dùng file script:

```toml
[[pipeline.components]]
type = "rhai_transform"
id = "mean"                      # cũng là tên watermark cursor riêng của node
inputs = ["ingest"]
script_path = "scripts/moving_avg.rhai"
# input_stage  = "raw"           # raw | processed
# output_stage = "processed"
# write_lru    = true            # ghi vào working LRU
# write_store  = false           # ghi thêm xuống persistence (parquet)
```

Hoặc script inline ngay trong TOML (tiện thử nhanh):

```toml
[[pipeline.components]]
type = "rhai_transform"
id = "double"
inputs = ["ingest"]
script = '''
fn process(observations) {
    let out = [];
    for obs in observations {
        obs.value *= 2.0;
        out.push(obs);
    }
    out
}
'''
```

## Pipeline mẫu — trigger thủ công hoàn toàn qua MCP

Khai báo pipeline **không có clock** để toàn quyền điều khiển bằng tay:

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
type = "persist_sink"
id = "persist"
inputs = ["mean"]
```

Rồi thao tác:

1. `opsense_init` — mở session.
2. `opsense_run {"ts": <unix_secs>}` — bơm `tick(ts)` vào node `ingest`
   (tham số `node` đổi được nếu muốn trigger từ node khác).
3. `opsense_status` — xem watermark (kèm cursor riêng `mean`).
4. `opsense_query {"stage": "processed", "source": "working"}` — xem kết quả.
5. Sửa script → `opsense_run` lại → so sánh kết quả.

Xem thêm ví dụ: [`scripts/moving_avg.rhai`](moving_avg.rhai) — batch mean theo
metric, phát `<metric>_mean`.

## Truyền cấu hình vào script (`params` + `[attributes]`)

Node `rhai_transform` nhận cấu hình từ config.toml qua hai kênh:

- `params` của node — mỗi cặp key/value thành biến toàn cục `param_<tên>`
  (vd `params.factor = 3` → `param_factor`). Tên chỉ gồm `[A-Za-z0-9_]`.
- `[attributes]` của config (override bằng `OPSENSE_ATTR_<TÊN_HOA>`) — đọc
  bằng `attr("tên")` (trả `()` nếu không có) hoặc `attrs()` (cả map).

```toml
[[pipeline.components]]
type = "rhai_transform"
id = "disk-alert"
inputs = ["disk-usage"]
script_path = "scripts/disk_spike_check.rhai"
[pipeline.components.params]
saturated = 0.9
```

```rhai
fn process(observations) {
    let saturated = 0.9;
    try { saturated = param_saturated } catch { 0.9 };  // fallback mặc định
    let env = attr("env");
    // ...
}
```

Xem ví dụ đầy đủ: [`scripts/disk_spike_check.rhai`](disk_spike_check.rhai),
[`scripts/disk_usage_grid.rhai`](disk_usage_grid.rhai),
[`scripts/disk_capacity_forecast.rhai`](disk_capacity_forecast.rhai).

## Dự đoán capacity disk

[`disk_capacity_forecast.rhai`](disk_capacity_forecast.rhai) dùng
`capacity_forecast` — lớp ghép `TrendAnalysis` + `AnalysisGrid` +
`TransitionAnalysis` — để trả lời **hướng đi** và **biên đường chéo** của mỗi
đĩa: nó đang đi lên hay đi ngang, dao động quanh xu hướng cỡ nào, và còn bao
lâu nữa thì đầy.

Mỗi đĩa cho ra 3 observation:

| `metric_id` | value | dùng để |
|---|---|---|
| `disk_capacity_hours:<mp>` | giờ tới khi **mép trên** chạm 100% | **cảnh báo** (tính cả dao động) |
| `disk_capacity_trend:<mp>` | giờ tới khi **đường xu hướng** chạm 100% | lập kế hoạch (bỏ dao động) |
| `disk_capacity_band:<mp>` | biên độ tính bằng **ô lưới** | độ rộng dải dao động |

Hai mốc đầu lệch nhau đúng `amplitude / slope` — tức "dao động cắt ngang bao
nhiêu phần thời gian còn lại". Nếu đĩa đã ổn định, hai số gần bằng nhau và
`disk_capacity_band` nhỏ; nếu đĩa dao động mạnh, hai số tách xa và band lớn —
lúc đó đừng lập kế hoạch theo số nhỏ.

```toml
[pipeline.components.params]
cells       = 12     # max_bit của grid (2^cells ô tối đa)
recent_secs = 86400  # cửa sổ quan sát
min_samples = 12     # dưới ngưỡng thì bỏ qua đĩa đó
```

Script chạy trên **phần trăm** đã dùng (`capacity = 100`). Với input tính bằng
byte thì sửa tham số `capacity_forecast(..., 100.0, ...)` cho khớp tổng dung
lượng. Chi tiết về toán thuật: [`docs/RHAI.md`](../../../docs/RHAI.md) mục 5.

## Cào API ngoài bằng `http_source`

Node HTTP generic khai báo request dạng **template** — URL, header, query param
và body đều được render trước mỗi lần gọi với 3 nhóm biến:

- `{{from_ts}}` / `{{to_ts}}` / `{{ts}}` — cửa sổ watermark của chu kỳ hiện
  tại (lần đầu tiên chạy lùi về `initial_lookback_secs`);
- `{{tên}}` — một attribute trong `[attributes]` của config (override bằng
  biến môi trường `OPSENSE_ATTR_<TÊN_HOA>`);
- `{{env.TÊN}}` — biến môi trường đọc trực tiếp (token/secret).

```toml
[[pipeline.components]]
type = "http_source"
id = "prom"
inputs = ["clock"]
url = "{{prom_url}}/api/v1/query_range"
items = "data.result[].values[]"        # mỗi sample một observation
[pipeline.components.fields]
ts = { query = "0", cast_to = "i64" }   # jq path trong từng item
value = { query = "1", cast_to = "f64" }
[pipeline.components.constants]
metric_id = "up"
[pipeline.components.params]
query = "rate(node_cpu_seconds_total[5m])"
start = "{{from_ts}}"                   # chỉ hỏi đúng phần delta kể từ cursor
end   = "{{to_ts}}"
step  = "60"
```

Response map thành observations bằng bộ khai báo `items` + `fields` +
`constants` (engine jq của `opsense-mlib`) — không cần viết script hay code
Rust: thêm node config là xong. Body observation-shape sẵn thì bỏ trống
`items`/`fields`, node parse thẳng. Lỗi fetch/map → cửa sổ giữ cursor, tự
retry ở tick kế. Chi tiết: [`docs/GUIDE.md`](../docs/GUIDE.md) mục 5.

## Giới hạn sandbox

- Không truy cập filesystem, network hay thư viện ngoài.
- Tối đa 1.000.000 phép tính / lần gọi; mảng/map/string tối đa 100.000 phần tử.
- Mỗi lần gọi chạy trong thread riêng (`spawn_blocking`) — script treo hoặc
  panic chỉ làm bỏ qua batch đó, pipeline vẫn sống.
