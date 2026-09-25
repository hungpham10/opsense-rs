# Prometheus data collection pipeline

> Cập nhật: **2026-09-25** — mô tả lại theo `examples/prometheus-demo/config.toml`
> hiện tại (bản cũ mô tả config đã bị thay, trong đó có type `clock_source` không
> tồn tại). Bản đầy đủ chạy trong CI (Parquet + mirror S3) là
> `strategies/prometheus/config.toml`.

## 0. Cấu hình mẫu

```toml
[engine]
poll_interval_seconds = 10

[storage]
backend = "memory"          # bản mẫu dùng memory để thử không cần Postgres/S3

[attributes]
prom_url = "https://prometheus.demo.prometheus.io"

[capacity]
up = 1.0
node_memory_MemAvailable_bytes = 16.0
```

Chạy:

```bash
opsense validate --config examples/prometheus-demo/config.toml
OPSENSE_CONFIG=examples/prometheus-demo/config.toml opsense serve
opsense query prom-explore --limit 20
```

> Endpoint demo của Prometheus trả **Prometheus JSON**, không phải mảng
> observations, nên `prom-explore` log warn khi parse — đó là hành vi đã biết của
> endpoint công khai, không phải lỗi config. Bản CI
> (`strategies/prometheus/config.toml`) re-point `url` về mock observations để
> assert end-to-end.

## 1. Các node

| # | `type` | `id` | Vai trò |
|---|---|---|---|
| 1 | `clock` | `clock` | nhịp 10s đẩy tick xuống toàn pipeline |
| 2 | `http_source` | `prom-explore` | kéo metric từ Prometheus, `station = true` |
| 3 | `rhai_transform` | `stats-summary` | script `strategies/prometheus/script.rhai` |
| 4 | `timeseries_station_sink` | `tsdb` | ghi ra station đọc lại được |
| 5 | `rhai_transform` | `inline-example` | script inline `'''…'''`, minh hoạ `params` |
| 6 | `null` | `null` | node lá cho các transform không phải sink |

Node `transform` không phải sink thì **bắt buộc** phải có consumer — đó là lý do
node `null` nhận input từ cả ba nhánh.

## 2. `http_source` (`prom-explore`)

| Field | Ý nghĩa |
|---|---|
| `url` | URL có template: `{{prom_url}}`, `{{from_ts}}`, `{{to_ts}}` |
| `interval_secs` | chu kỳ khi tick không mang `interval` |
| `timeout_secs` | timeout HTTP (giây) |
| `station = true` | tự đăng ký station theo `id` → query được ngay |
| `bindings` | jq expression → đưa kết quả vào bảng `{{...}}` |
| `items` / `fields` / `constants` | map response thô → shape observation chuẩn (jq) |

Hai kiểu response:

- **JSON thô** (mảng observation) — dùng `items` + `fields` + `constants`; đường
  dùng cho mock/e2e.
- **Candle (OHLCV)** — thêm `candle_parse_mode = "array"`; đường dùng cho Binance
  `klines`.

## 3. Mapping observation

Chuẩn mà script và station dùng:

```rhai
#{
    ts: 1787669686,          // unix giây
    metric_id: "up",
    kind: "metric",          // metric | log | trace
    signal: "utilization",
    value: 0.3568,
    labels: #{ source: "prometheus-demo" },
}
```

## 4. Đọc lại dữ liệu

| Cách | Lệnh |
|---|---|
| CLI | `opsense query prom-explore --limit 20` |
| MCP | `opsense_query_timeseries({node: "prom-explore", limit: 20})` |
| REPL | `:query prom-explore` |
| GraphQL | `POST /api/repl/graphql` → `Query.queryTimeseries` |
| Từ script khác | `station_query("prom-explore", from, to)` |

Không có endpoint HTTP riêng cho station và không có tool backfill: muốn kéo lại
lịch sử thì chỉnh `initial_lookback_secs` rồi restart/reload.

## 5. Watermark

`<data_dir>/watermarks.json` (ghi tạm + rename atomic) nhớ mốc sớm nhất của mỗi
node để không xử lại dữ liệu cũ sau restart. File hỏng → log warn và khởi động
sạch.

## 6. Mở rộng

1. Thêm node mới với `id` duy nhất.
2. Nối `inputs = ["<id node nguồn>"]`.
3. Bật `station = true` nếu muốn query trực tiếp từ node đó.
4. Dùng `items`/`fields`/`constants` để map response về shape chuẩn.
5. Đấu nhánh transform vào `null` (hoặc một sink thật).

Script mẫu trong `examples/prometheus-demo/rhai/` được test bởi
`crates/opsense-rhai/tests/{pipeline,disk_grid_report,disk_spike,e2e_disk_grid}.rs`
— sửa script thì chạy các test đó thay vì chỉ chạy tay.
