# Viết script Rhai trong Opsense

> **Trạng thái:** engine Rhai + bindings grid/transition tích hợp thẳng vào
> `opsense-components` (crate đã nối sẵn serve qua `use opsense_components as _;`
> và healthy CI). Script chạy qua node `rhai_transform` khai trong `config.toml`
> — **không cần build lại binary**: sửa file script là batch kế tiếp tự chạy.
> Tài liệu này mô tả **hợp đồng script** — toàn bộ hàm script gọi được. Mọi
> binding dưới đây được khai **một block duy nhất** bởi macro `#[rhai]` trong
> `opsense-macros` (kiểu typetag, y hệt `#[transform]`); **không có "dead
> binding"**: nếu một hàm trong bảng trả lỗi "hàm không tồn tại" thì đó là bug
> — báo ngay (xem §8).

Script chạy trong sandbox (không filesystem/network/host function — xem §7).
Có **một** chỗ script được dùng trong pipeline:

| Node | Hợp đồng | Dữ liệu vào |
|---|---|---|
| `rhai_transform` | `fn process(observations)` → array observation-map mới | array observation-map từ cửa sổ cursor |

> `http_source`/`csv_source` không dùng script Rhai: response API map thành
> observations bằng bộ khai báo `items` + `fields` + `constants` (jq, xem
> `docs/GUIDE.md` §5). Rhai giữ vai trò `rhai_transform` giữa các node, và
> `fn rebuild` cho chiến lược trading (§9).

Script mẫu kèm repo: [`scripts/`](../scripts/README.md) — đặc biệt
[`disk_spike_check.rhai`](../scripts/disk_spike_check.rhai) (so hiện tại với
baseline). Mọi function dưới đây có ví dụ dùng thật trong script đó.

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
(output ghi vào stage cấu hình). Ví dụ script đầy đủ:
[`scripts/disk_spike_check.rhai`](../scripts/disk_spike_check.rhai).

## 2. Query dữ liệu từ trạm (`station_query` / `station_candles`)

Mọi node sinh dữ liệu (`http_source`, `rhai_transform`,
`timeseries_station_transform`, …) tự đăng ký một **trạm** theo node id
(first-wins). Script đọc được mọi trạm qua hai hàm:

```rhai
// station_query(station, from_ts, to_ts) -> array observation-map
let obs = station_query("tsdb", now_secs() - 3600, now_secs());

// Lọc server-side (3.3) — LUÔN lọc khi trạm chứa nhiều dữ liệu
let orders = station_query("grid", from, now, "order");
let cursor = station_query("grid", from, now, "summary", "trading_step");

// station_candles(station, from_ts, to_ts, resolution) -> array candle map
// map dạng #{ t, o, h, l, c, v }; resolution lọc theo label
let candles = station_candles("grid", from, to, "60");
```

- Cả hai trả `()` nếu **không có** trạm id đó → kiểm tra `!= ()` trước khi dùng.
- `station_query` dùng `query_recent`: trả mọi observation **thực sự có** trong
  cửa sổ, kể cả khi coverage hổng (khác `query_range` vốn trả rỗng nếu bất kỳ
  block nào chưa cover trọn — vô dụng cho script muốn "cho tôi dữ liệu gần now").
- `station_candles` đọc qua `DataLoader` của qlib — **cùng đường đọc với kernel
  trading**, và window âm được clamp về 0 (không tràn thành cửa sổ khổng lồ).
- Muốn query được thì node sinh dữ liệu phải publish trạm: bật `station = true`,
  hoặc thêm `timeseries_station_transform` đứng sau node.

### 2.1 Script được nạp lại từ đĩa mỗi lần eval

`script_path` được đọc lại **mọi lần** script chạy, nên sửa file `.rhai` không
cần restart — node nhận script mới ở message kế tiếp.

Hệ quả phải biết: **đổi bề mặt API (số tham số của một hàm) sẽ làm hỏng pipeline
đang chạy**, vì engine đã đăng ký binding cũ nhưng script mới gọi arity mới:

```text
WARN rhai grid skipped batch at ts …: script error:
  Function not found: station_query (&str, i64, i64, &str) (line 107, position 14)
```

Script mới lỗi ⇒ cả batch bị bỏ (`transform.rs`) ⇒ node im bặt, nhưng tiến trình
vẫn sống. Khi thấy lỗi này thì **restart `opsense serve`**, đừng chỉ sửa script.

### 2.2 Lọc server-side là bắt buộc, không phải tuỳ chọn

`max_map_size` của Rhai là trần **engine-wide** (100.000, xem `runtime.rs`), tính
trên mọi map sống cùng lúc. Một observation là một map lồng `labels` map, nên đọc
trạm chứa nhiều tick sẽ vượt trần và **cả batch script chết**:

```text
script error: Size of object map too large
```

Đo thật trên `strategies/binance`: 26.680 observation trong 1 giờ là đủ vỡ. Vì vậy
khi chỉ cần một loại dữ liệu (lệnh, cursor, cảnh báo) thì **luôn truyền bộ lọc**,
đừng lọc tay trong script — lọc tay thì cái map vẫn đã được dựng.

### 2.3 Ghi: `station_write` + `params.write_stations`

Mặc định script vẫn ghi **ngầm** vào trạm của chính nó (cái mà `fn process` trả
về). Muốn tự quyết định gì gì đi đâu:

```rhai
station_write("order-history", obs);   // phải khai trước trong params
out += obs;                            // forward xuống sink
```

```toml
[pipeline.components.params]
write_stations = ["order-history"]          # trạm được phép ghi (ngoài own)
implicit_station_write = false              # tắt ghi ngầm (mặc định true)
```

Ghi vào trạm **không** khai trong `write_stations` sẽ **báo lỗi** chứ không im
lặng bỏ qua — ghi nhầm chỗ là lỗi cấu hình, im lặng sẽ biến nó thành "dữ liệu mất
không rõ ở đâu".

> Hàm `ts_query`/`ts_mean(station, stage, metric, from, to)` của bản tài liệu cũ
> **đã bị gỡ** khi bỏ tầng store chung. Thay bằng `station_query` + lọc `metric_id`
> trong script nếu cần.

## 3. Toán tử time-series (`ts_*`)

Nhận array observation-map (định dạng `ts_query` trả về):

| Hàm | Trả về |
|---|---|
| `ts_rate(points)` | (value cuối − value đầu)/Δt; `()` nếu rỗng/chia 0 |
| `ts_moving_avg(points, window_secs)` | array `{ts, value}` trung bình trượt |
| `ts_resample(points, bucket_secs, agg)` | gom bucket; `agg` ∈ `avg\|min\|max\|sum\|count` |
| `ts_quantile(points, q)` | phân vị q∈[0,1] |
| `ts_p95(points)` / `ts_p99(points)` | sugar của quantile |
| `ts_delta(points)` | điểm cuối − điểm đầu |
| `ts_pct_change(points)` | % thay đổi |

Hàm thời gian: `now_secs()` → unix giây hiện tại.

## 4. Ví dụ end-to-end — cảnh báo baseline

Pipeline (bổ sung vào `config.toml` của strategy, node sinh dữ liệu trước,
node check sau — khớp pattern CI hiện có):

```toml
[[pipeline.components]]
type = "http_source"
id = "disk-usage"
station = true          # publish trạm để script đọc được

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

## 5. Phân tích chuỗi (`grid_*`, `transition_*`, `trend_*`, `capacity_*`)

> Cả hai type + toàn bộ accessor được đăng ký **một block duy nhất** bởi macro
> `#[rhai]` — script **không cần khai constructor tay**: macro validate
> constructor ở compile-time → script **luôn tạo được instance**.

### AnalysisGrid (`grid_*`)

Chia khoảng capacity `[min, max]` thành các dải đều; chuỗi usage "đi" trên
lưới. Thuật toán **sàng phân cấp** tìm số dải sao cho tỉ lệ cắt biên giữa hai
điểm liên tiếp thấp nhất trong khi lưới vẫn mịn nhất (dừng khi delta crossings
tăng đột biến — overfitting).

| Hàm | Ý nghĩa |
|---|---|
| `grid_fit(points, min, max, max_bits)` | Fit lưới; trả `AnalysisGrid` (hoặc `()` nếu không fit được) |
| `grid_fit_values(values, min, max, max_bits)` | Như trên, nhận array số thuần |
| `num_cells(g)` / `num_lines(g)` / `grid_step(g)` | Số dải / số đường lưới / độ rộng dải |
| `grid_cell(g, y)` | Chỉ số dải chứa giá trị `y` |
| `grid_crossings(g, points)` | Số lần cắt biên của chuỗi |
| `grid_occupancy(g, points, interval_secs)` | Histogram `result[bucket][cell]` theo thời gian |
| `grid_ranges(g)` | Array `#{low, high}` — biên từng dải |

### TransitionAnalysis (`transition_*`)

Xây trên một `AnalysisGrid` đã fit: chia cửa sổ dữ liệu thành các **bucket**
liên tiếp, đếm lần di chuyển giữa các dải và thời gian lưu từng dải — phát
hiện "trạng thái" bất thường (tỉ lệ đi xuống/tăng/đứng yên từ một dải với
xác suất). **Constructor bắt buộc** (macro validate compile-time):

| Hàm | Ý nghĩa |
|---|---|
| `transition_analysis(grid, points, interval_secs)` | **constructor** — instance `TransitionAnalysis` (hoặc `()` nếu không fit được) |
| `num_buckets(ta)` / `num_cells(ta)` / `interval_secs(ta)` | số bucket / số dải / độ rộng bucket |
| `grid(ta)` | AnalysisGrid cơ sở |
| `transitions(ta)` | tổng số lần di chuyển |
| `has_transitions_from(ta, cell)` / `total_from(ta, cell)` | có transition từ dải / tổng đi ra khỏi dải `cell` |
| `down_probability(ta, cell)` / `up_probability(ta, cell)` / `stay_probability(ta, cell)` | xác suất đi xuống / đi lên / đứng yên từ dải |
| `down_probabilities(ta)` / `up_probabilities(ta)` / `stay_probabilities(ta)` | vector xác suất cho mọi dải |

Script điển hình — fit lưới → dựng transition → đọc accessor:

```rhai
fn process(points) {
    let g = grid_fit(points, 0.0, 52591026176.0, 12);
    if g == () { return (); }

    let ta = transition_analysis(g, points, 300);   // bucket 5 phút
    if ta == () { return (); }

    [       // mọi accessor gọi được ngay — không "miss"
        #{ buckets: num_buckets(ta), cells: num_cells(ta), interval: interval_secs(ta) },
        down_probabilities(ta),
        up_probabilities(ta),
        stay_probabilities(ta),
        grid_ranges(grid(ta)),
    ]
}
```

### TrendAnalysis (`trend_*`)

Hồi quy tuyến tính trên `(t, value)` của một chuỗi bất kỳ: **đi đâu**, **dao
động cỡ nào** quanh xu hướng, và **bao lâu nữa chạm** một mốc ngang. Không cần
biên vật lý — xem `opsense-mlib/src/trend.rs`.

Biên độ (`trend_amplitude`) là nửa bề rộng của **biên đường chéo**: dữ liệu thật
đi thành dải dốc `trend(h) ± amplitude`, không phải một đường thẳng.

| Hàm | Ý nghĩa |
|---|---|
| `trend_fit(points, min_samples, amplitude_quantile, significance, min_amplitude)` | **constructor** — instance `TrendAnalysis` (hoặc `()` nếu không đủ dữ liệu) |
| `trend_direction(t)` | `"rising"` / `"falling"` / `"flat"` |
| `trend_current(t)` / `trend_samples(t)` / `trend_span_secs(t)` | quan sát cuối / số điểm / bề rộng cửa sổ |
| `trend_origin_ts(t)` / `trend_anchor_ts(t)` | mốc gốc đường hồi quy / mốc neo của phép chiếu |
| `trend_slope_per_sec(t)` / `..._per_hour(t)` / `..._per_day(t)` | độ dốc, 3 thang đọc được |
| `trend_r2(t)` / `trend_residual_std(t)` | chất lượng fit và độ lệch của phần dư |
| `trend_amplitude(t)` / `trend_amplitude_rel(t)` | biên độ tuyệt đối / theo tỉ lệ `max − min` của dữ liệu |
| `trend_offset(t)` | quan sát cuối lệch bao nhiêu so với đường hồi quy (chẩn đoán) |
| `trend_value_at(t, ts)` | giá trị **đường hồi quy** tại `ts` |
| `trend_project(t, hours)` | `#{hours, ts, trend, low, high}` — chiếu và hai mép biên |
| `trend_hours_to(t, target)` | giờ tới khi **đường xu hướng** chạm `target` (hoặc `()`) |
| `trend_hours_to_upper_envelope(t, target)` | giờ tới khi **mép trên** chạm `target` — luôn ≤ trên |

Hai ngưỡng cần hiểu:

- `amplitude_quantile` (mặc định `0.95`) — dao động thật thường **nhọn** (đỉnh
  rồi đáy), nên biên độ lấy phân vị chứ không lấy độ lệch chuẩn.
- `significance` (mặc định `2.0`) — chỉ gọi là `Rising`/`Falling` khi **tổng
  độ dốc cả cửa sổ** vượt `significance × amplitude`. Chuỗi đi ngang nhưng dao
  động mạnh vẫn là `flat`: đường hồi quy lúc đó chỉ là nhiễu.

### CapacityForecast (`capacity_*`)

Lớp ghép: `TrendAnalysis` + `AnalysisGrid` + `TransitionAnalysis`, trả lời
"bao nhiêu thì đáng lo so với capacity". Xem `opsense-rhai/src/capacity.rs`.

| Hàm | Ý nghĩa |
|---|---|
| `capacity_forecast(points, capacity, interval_secs, max_bit, min_samples)` | **constructor** (hoặc `()`). `capacity` = biên vật lý **cùng đơn vị** `value`: `100.0` cho phần trăm, `52591026176.0` cho byte |
| `capacity(f)` / `capacity_current(f)` | biên vật lý / quan sát cuối |
| `capacity_headroom(f)` / `capacity_headroom_rel(f)` | còn trống, tuyệt đối / theo tỉ lệ |
| `capacity_direction(f)` / `capacity_oscillating(f)` | hướng đi / có dao động đáng kể **so với capacity** không |
| `capacity_amplitude(f)` / `capacity_amplitude_rel(f)` | biên độ, tuyệt đối / theo tỉ lệ `capacity` |
| `capacity_envelope_cells(f)` | **biên độ tính bằng ô lưới** — `0.4` nghĩa là mép trên/dưới lệch nhau chưa tới một dải |
| `capacity_current_cell(f)` / `capacity_top_cell(f)` | đang ở dải nào / dải "sắp đầy" |
| `capacity_drift(f)` | `P(lên) − P(xuống)` theo transition, trong `[-1, 1]` |
| `capacity_samples(f)` / `capacity_span_secs(f)` / `capacity_interval_secs(f)` | kích thước cửa sổ và nhịp bucket |
| `capacity_trend(f)` | trả về `TrendAnalysis` bên trong — đọc tiếp bằng `trend_*` |
| `capacity_grid(f)` / `capacity_transition(f)` | trả về `AnalysisGrid` / `TransitionAnalysis` — đọc tiếp bằng `grid_*` / `transition_*` |
| `capacity_hours_to_full(f)` | mép trên chạm `capacity` — **dùng để cảnh báo** (hoặc `()`) |
| `capacity_hours_to_trend_full(f)` | đường xu hướng chạm `capacity` — **dùng để lập kế hoạch** |
| `capacity_hours_to_top_cell(f)` | mép trên chạm biên dưới dải trên cùng |
| `capacity_project(f, hours)` | `#{hours, ts, trend, low, high}` |

Ba mốc "còn bao lâu nữa" xếp đúng thứ tự:

```
hours_to_top_cell  ≤  hours_to_full  ≤  hours_to_trend_full
```

Chênh lệch giữa hai mốc cuối chính là `amplitude / slope` — tức **"dao động cắt
ngang bao nhiêu phần thời gian còn lại"**. Chỉ nhìn đường hồi quy thì không thấy
con số này, và đó là lý do cả ba cùng tồn tại.

Script điển hình:

```rhai
fn process(observations) {
    let f = capacity_forecast(observations, 100.0, 0, 12, 12);
    if type_of(f) == "() { return []; }

    let h_full   = capacity_hours_to_full(f);
    let h_trend  = capacity_hours_to_trend_full(f);
    // () = chỉ số không đi lên → không có mốc chạm trần. Ép -1 để series không
    // biến mất (thiếu series thì alert không bắt được).
    let hours    = if type_of(h_full)  == "()" { -1.0 } else { h_full };
    let hours_tr = if type_of(h_trend) == "()" { -1.0 } else { h_trend };

    [ #{
        ts: now_secs(),
        metric_id: "disk_capacity_hours",
        kind: "metric", signal: "raw",
        value: hours,
        labels: #{
            direction: capacity_direction(f),
            oscillating: if capacity_oscillating(f) { "yes" } else { "no" },
            hours_trend: hours_tr.to_string(),
            band_cells: capacity_envelope_cells(f).to_string(),
            drift: capacity_drift(f).to_string(),
        },
    } ]
}
```

Xem script chạy thật, một observation cho mỗi chỉ số:
[`examples/prometheus-demo/rhai/disk_capacity_forecast.rhai`](../examples/prometheus-demo/rhai/disk_capacity_forecast.rhai).

## 6. Pattern matching & catalog (`pattern_*` / `catalog_*`)

### Pattern (Aho-Corasick log matcher)

| Hàm | Trả về |
|---|---|
| `pattern_is_known(node_id, text)` | `bool` — text có match pattern nào không |
| `pattern_add(node_id, pattern)` | `()` — thêm pattern vào automaton |
| `pattern_stats(node_id)` | map `{total_patterns, hits, misses}` |

### Catalog (Radix substring search)

| Hàm | Trả về |
|---|---|
| `catalog_insert(node_id, key, value)` | `()` — index key/value |
| `catalog_search(node_id, pattern)` | array `{key, value}` maps |

Cả hai dùng chung registry first-wins per node id (macro `#[rhai]` sinh, nếu
script dùng `pattern_*`/`catalog_*` thì cũng là một block duy nhất — §8).

## 7. Sandbox

- Không filesystem/network/host function; chỉ toán tử Rhai + các hàm đăng ký:
  `ts_*`, `grid_*`, `transition_*`, `pattern_*`, `catalog_*`, `station_query`,
  `station_candles`, `attr`/`attrs`, `trigger`, `now_secs`, và (chỉ ở
  `rhai_transform` của node trading) `portfolio_feed`.
- Giới hạn: 1_000_000 operations, array/map 100_000, string 1_000_000.
- Timeout riêng: env `OPSENSE_RHAI_TIMEOUT_SECS`.
- Lỗi script **không giết pipeline**: log warn, cursor giữ nguyên, cửa sổ được
  retry ở tick kế — sửa script xong tự heal.

## 8. Test nhanh

Quy trình chuẩn — **run → chờ → đọc dữ liệu → timeout:**

1. Khai node `rhai_transform` + script trong `config.toml` của một strategy
   (xem §4). Chạy `opsense serve` với config đó.
2. **Chờ 1 khoảng thời gian** đủ cho pipeline chạy vài tick (vd 15–30s).
3. **Đọc dữ liệu qua GraphQL** (`queryTimeseries`) với `station_id` = trạm của
   node script — xem metric do script sinh ra đã vào trạm chưa.
4. **Không có dữ liệu → báo timeout + xử lý:** kiểm tra server còn healthy
   không, log warn lỗi script (script bị retry tự heal), station đã đăng ký
   chưa; sau đó chạy lại ở batch kế. **Không panic ngầm** — timeout là hành vi
   kỳ vọng khi pipeline chưa đủ dữ liệu.

---

## 9. Chiến lược trading: `fn rebuild`

Ngoài `fn process`, script của node `rhai_transform` có thể khai thêm
`fn rebuild(candles, prev, params)` để làm **chiến lược** (genome khai báo được,
không phải class Rust phải sửa rồi compile):

```rhai
fn rebuild(candles, prev, params) {
    // candles = [#{ t, o, h, l, c, v }, …]  — nến kernel fetch trong lookback
    // prev    = [#{ long_win, long_lost, short_win, short_lost }, …]
    //            thống kê lệnh đã đóng của plan cũ (kernel giữ, không mất khi
    //            script dựng lại plan)
    // params  = map knob từ [pipeline.components.params]
    // trả: một plan cho MỖI ô lưới (kernel ghép mỗi plan thành một lưới lệnh
    //      riêng trong khoảng giá của ô đó)
}
```

- Kernel (`Strategy::rebuild`) gọi hàm này mỗi `review_interval_secs`.
- Hợp đồng dữ liệu của plan: `opsense_qlib::plan::GridPlan` / `CellStats` (serde).
  Script **không dựng được** `TradingGrid` (field Rust private) nên trả JSON; kernel
  dựng lại grid và chép bộ đếm win/lost từ `prev` — bộ đếm là trí nhớ kernel.
- Script chịu thiếu knob bằng helper tự viết (tên `fallback`, vì `default` là
  keyword Rhai):

  ```rhai
  fn knob(params, name, fallback) {
      if params.contains(name) { params[name] } else { fallback }
  }
  ```

- Script nguồn là **chính script của node** (đọc qua `runtime::current_script()`) →
  không khai đường dẫn chiến lược ở `params`, không thể lệch hai bản.
- `portfolio_feed(candles, obs, candle, cfg, symbol)` là hàm duy nhất được phép
  gọi kernel: nó chạy trên **engine Rhai thứ hai** (engine chính đang
  `with_borrow_mut` nên không reentrant được), chỉ đăng ký binding read-only.

Script mẫu đầy đủ: [`strategies/binance/grid.rhai`](../strategies/binance/grid.rhai)
— sieve tìm số ô (`grid_fit`) → xác suất chuyển trạng thái
(`transition_analysis`) → win-prob theo ô → nếu đủ lệnh đã đóng thì tin tỉ lệ
thắng thực tế.

> **Hợp đồng lỗi:** nếu script đúng (đã khớp bảng §2–§6) mà một hàm báo "hàm
> không tồn tại" → đó là bug binding (macro sinh thiếu/trùng) — báo ngay. Ví
> dụ hiện tại nếu bảng §5 thiếu constructor thì là **bug** chứ không phải hành
> vi kỳ vọng.
