# Viết script Rhai trong Opsense

Script Rhai là cách xử lý dữ liệu không cần build lại binary: sửa file là chạy
lại ở batch kế tiếp. Script chạy trong sandbox (không fs/network/host fn, xem
[§5](#5-sandbox)). Có hai chỗ script được dùng:

| Node | Hợp đồng | Dữ liệu vào |
|---|---|---|
| `rhai_transform` | `fn process(observations)` | array observation-map từ cửa sổ cursor |

> `http_source` không dùng script Rhai nữa: response API map thành observations
> bằng bộ khai báo `items` + `fields` + `constants` (jq, xem `docs/GUIDE.md`
> mục 5). Rhai chỉ còn vai trò transform giữa các node.

Script mẫu kèm repo: [`scripts/`](../scripts/README.md) — đặc biệt
[`disk_spike_check.rhai`](../scripts/disk_spike_check.rhai)
(so hiện tại với baseline trạm).

---

## 1. Observation map

Đơn vị dữ liệu chuẩn khắp hệ thống:

```rhai
#{
    ts: 1787669686,              // unix giây (i64)
    metric_id: "disk_usage_ratio",
    kind: "metric",              // metric | log | trace
    signal: "utilization",       // utilization | saturation | rate | errors | duration | raw
    value: 0.3568,               // f64
    labels: #{ mountpoint: "/", device: "/dev/sda1" },   // tuỳ ý
}
```

`rhai_transform` nhận mảng các map này và **phải trả về mảng mới cùng dạng**
(output ghi vào stage cấu hình).

## 2. Query dữ liệu từ trạm (`ts_query` / `ts_mean`)

MỌI node sinh dữ liệu (`http_source`, `ingest_source`, processor/rhai
transform, `timeseries_station_transform`…) đều tự đăng ký một trạm vào
registry toàn cục theo **node id** (first-wins: sửa tham số trạm cần restart
session). Mọi script đều gọi được — ví dụ node `http_source` có `id =
"vms-disk-usage"` (bật `station = true`) thì script đọc thẳng dữ liệu nó vừa
fetch:

```rhai
// Trả ARRAY observation-map; () nếu không có trạm id đó.
let points = ts_query("tsdb", "processed", "disk_usage_ratio", now_secs() - 3600, now_secs());

// Sugar: trung bình value của cửa sổ; () nếu rỗng/không có trạm.
let base = ts_mean("tsdb", "processed", "disk_usage_ratio", now_secs() - 3600, now_secs());
```

- Tham số: `(station_id, stage /* raw|processed */, metric_id, from_ts, to_ts)`
- Kiểm tra `!= ()` trước khi dùng — trạm chưa đăng ký hoặc cửa sổ trống trả `()`.
- Muốn query được thì node sinh dữ liệu phải publish trạm: `http_source` bật
  `station = true`, hoặc thêm `timeseries_station_sink`/`timeseries_station_transform`
  đứng sau node đó (xem `examples/prometheus-demo/config.toml`, khối commented).

## 3. Toán tử time-series (`ts_*`)

Nhận array observation-map (đúng định dạng `ts_query` trả về):

| Hàm | Trả về |
|---|---|
| `ts_rate(points)` | (cuối − đầu) / Δt; `()` nếu rỗng/chia 0 |
| `ts_moving_avg(points, window_secs)` | array `{ts, value}` trung bình trượt |
| `ts_resample(points, bucket_secs, agg)` | gom bucket; `agg` ∈ `avg\|min\|max\|sum\|count` |
| `ts_quantile(points, q)` | phân vị q∈[0,1] |
| `ts_p95(points)` / `ts_p99(points)` | sugar của quantile |
| `ts_delta(points)` | điểm cuối − điểm đầu |
| `ts_pct_change(points)` | % thay đổi |

Hàm thời gian: `now_secs()` → unix giây hiện tại.

## 4. Ví dụ end-to-end — cảnh báo đĩa theo baseline

Pipeline (bổ sung vào `examples/prometheus-demo/config.toml`):

```toml
[[pipeline.components]]
type = "timeseries_station_sink"
id = "tsdb"
inputs = ["disk-usage"]
bind = "127.0.0.1:9190"

[[pipeline.components]]
type = "rhai_transform"
id = "disk-spike"
inputs = ["disk-usage"]
script_path = "scripts/disk_spike_check.rhai"

[[pipeline.components]]
type = "timeseries_station_sink"
id = "checked-store"
inputs = ["disk-spike"]
```

`scripts/disk_spike_check.rhai`: mỗi điểm usage mới được gắn baseline 1 giờ,
delta và nhãn cảnh báo — xem file để biết chi tiết.

## 5. Phân tích lưới capacity (`grid_*`)

Chia khoảng capacity `[min, max]` thành các dải đều; chuỗi usage "đi" trên lưới.
Thuật toán **sàng phân cấp** (`opsense_libs::grid`) tìm số dải sao cho tỉ lệ cắt
biên giữa hai điểm liên tiếp thấp nhất trong khi lưới vẫn mịn nhất — dừng khi
delta crossings tăng đột biến (overfitting).

| Hàm | Ý nghĩa |
|---|---|
| `grid_fit(points, min, max, max_bit)` | Fit lưới từ array observation-map; trả object `AnalysisGrid` (hoặc `()`) |
| `grid_fit_values(values, min, max, max_bit)` | Như trên nhưng nhận array số thuần |
| `num_cells(g)` / `num_lines(g)` / `grid_step(g)` | Số dải / số đường lưới / độ rộng dải |
| `grid_cell(g, y)` | Chỉ số dải chứa giá trị `y` |
| `grid_crossings(g, points)` | Số lần cắt biên của chuỗi |
| `grid_occupancy(g, points, interval_secs)` | Histogram `result[bucket][cell]` theo thời gian |
| `grid_ranges(g)` | Array `#{index, low, high}` — biên từng dải |

Ví dụ với demo đĩa (biên vật lý `[0, disk_capacity]`):

```rhai
fn process(points) {
    let g = grid_fit(points, 0.0, 52591026176.0, 12);   // capacity ~49GB
    [
        #{ bands: num_cells(g), step: grid_step(g) },
        grid_occupancy(g, points, 3600),   // phân bố điểm theo giờ × dải
        grid_ranges(g),                    // liệt kê các dải [low, high)
    ]
}
```

`max_bit` là trần tinh xoáy của sieve (lưới mịn nhất = 2^max_bit dải);
đặt 10–14 là hợp lý. Script tham khảo: `scripts/disk_spike_check.rhai`
(cùng pipeline demo).

## 6. Pattern matching & catalog (`pattern_*` / `catalog_*`)

### Pattern (Aho-Corasick log matcher)

| Hàm | Trả về | Mô tả |
|---|---|---|
| `pattern_is_known(node_id, text)` | `bool` | text có match pattern nào không |
| `pattern_add(node_id, pattern)` | `()` | thêm pattern mới vào automaton |
| `pattern_stats(node_id)` | map `{total_patterns, hits, misses}` | thống kê |

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

### Catalog (Radix substring search)

| Hàm | Trả về |
|---|---|
| `catalog_insert(node_id, key, value)` | `()` — index key/value pair |
| `catalog_search(node_id, pattern)` | array of `{key, value}` maps |

```rhai
fn process(observations) {
    for o in observations {
        catalog_insert("svc-catalog", o.metric_id, serde_json::to_string(o));
    }
    let hits = catalog_search("svc-catalog", "cpu");
}
```

Cả hai dùng chung registry first-wins per node id.

## 7. Sandbox

- Không filesystem/network/host function; chỉ toán tử Rhai + hàm `ts_*`/`now_secs`.
- Giới hạn: 1_000_000 operations, array/map 100_000, string 1_000_000.
- Timeout riêng: env `OPSENSE_RHAI_TIMEOUT_SECS`.
- Lỗi script **không giết pipeline**: log warn, cursor giữ nguyên, cửa sổ được
  retry ở tick kế — sửa script xong tự heal.

## 8. Test nhanh

1. Pipeline playground không clock (mẫu A trong `.opsense/config.toml`) →
   MCP `opsense_run({node})` bơm tay → `opsense_query` xem kết quả.
2. Hoặc harness Rust thật theo [`GUIDE.md` §7](./GUIDE.md): copy
   `crates/opsense-rhai/tests/http_format.rs`, mock server, đánh dấu
   `#[ignore]`, chạy `cargo test -- --ignored --nocapture`.
