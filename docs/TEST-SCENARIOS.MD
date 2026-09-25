# Test scenarios

> Tài liệu này mô tả **cách chạy test thật** của repo: test suite offline, test
> strategy E2E, test integration cần compose, và smoke check container.
> Nơi mô tả kiến trúc: [`architecture.md`](./architecture.md).
>
> Cập nhật: **2026-09-25** — viết lại từ `.github/workflows/ci.yml` và từ trạng
> thái code hiện tại. Bản trước (2026-09-03) là draft Phase 5 và đã lệch nặng:
> nó mô tả `docker-compose.yml` là chưa tồn tại, hard-code 6 service, và trộn lẫn
> lệnh của workflow release cũ.

## 0. Tổng quan: 4 tầng test

| Tầng | Lệnh | Cần network/compose? |
|---|---|---|
| Unit + integration offline | `cargo test --workspace` | Không |
| Strategy E2E (Rhai grid trading) | xem §2 | Không (mock HTTP/WS nội bộ) |
| Live advisory (Binance/OKX thật) | xem §3 | Có Internet; `#[ignore]`, CI `continue-on-error` |
| Integration + container E2E | xem §4 | Có (docker compose 9 service) |

```bash
cargo check --workspace --all-targets   # gate nhanh
cargo test --workspace                  # 53 binary test
```

Lưu ý khi chạy local: đặt `CARGO_INCREMENTAL=0` để tránh cache artifact hỏng sau
đợt refactor lớn; test có dùng clock/station thật nên **không chạy song song các
target trading** (xem `--test-threads=1` ở §2).

## 1. Tầng 1 — Unit + integration offline

```bash
cargo test --workspace
```

Test file hiện có:

| Thư mục | Nội dung |
|---|---|
| `crates/opsense/tests/integration_health.rs` | `/health` trả đúng shape |
| `crates/opsense/tests/integration_repl_pty.rs` | REPL tương tác qua PTY |
| `crates/opsense-rhai/tests/operators.rs`, `pipeline.rs` | toán tử `ts_*`, `fn process` |
| `crates/opsense-rhai/tests/rhai_api_surface.rs` | đủ 25 accessor mà script được phép gọi |
| `crates/opsense-rhai/tests/station_lookup.rs`, `grid_ops.rs` | đọc station, thao tác grid |
| `crates/opsense-rhai/tests/test_binance_grid_strategy.rs` | `fn rebuild` → `GridPlan` → kernel |
| `crates/opsense-rhai/tests/test_prometheus_strategy.rs`, `test_predict_strategy.rs` | script prometheus/predict |
| `crates/opsense-rhai/tests/disk_*.rs`, `e2e_disk_grid.rs` | script mẫu trong `examples/prometheus-demo/rhai/` |
| `crates/opsense-qlib/src/**` (unit) | `Portfolio`, `Session`, T+N, `Graph` DAG→ONNX, `plan` serde |

Các test mà **cần** integration (`OPSENSE_INTEGRATION=true`) sẽ skip âm thầm khi
thiếu env, nên `cargo test --workspace` vẫn xanh khi không có compose.

## 2. Tầng 2 — Strategy E2E (Rhai grid trading)

Không cần compose. Đây là tầng hỏng nhiều nhất (hợp đồng `fn rebuild` giữa
`grid.rhai` và kernel, cursor T+N, idempotency sau restart) nên CI tách riêng để
đọc được log.

```bash
cargo test --workspace --release \
  --test e2e_binance_config \
  --test trading_pipeline \
  --test rhai_api_surface \
  --test test_binance_grid_strategy \
  --test integration_mcp_config \
  -- --test-threads=1 --nocapture
```

`--test-threads=1` là bắt buộc: các test này dùng station/clock thật, chạy song
song dễ đẩy nhau qua mốc phút (đã từng fail phụ thuộc pha phút, không phải logic).

Ba tầng trong `e2e_binance_config.rs`:

1. **Contract** — `strategies/binance/config.toml` phải khai `strategy = "rhai"`,
   đủ 5 knob của `fn rebuild`, và `grid.rhai` phải định nghĩa `fn rebuild`.
2. **Pipeline** — chạy config thật với HTTP klines + WebSocket aggTrade **mock**
   (test tự dựng server), kiểm tra nến gộp đúng và script không lỗi.
3. **Trading** — `mode = "trading"`: `portfolio_feed` → plan từ script → lệnh +
   cursor `labels.kind = "trading_step"` trong station. Assert: phải có ≥1 lệnh
   `open`, lệnh có thể `open|closed` tuỳ lúc test đọc station, cursor đơn điệu.

## 3. Tầng 3 — Live advisory (không chặn PR)

```bash
cargo test -p opsense-rhai --release --test grid_live_binance -- --ignored --nocapture
```

Cùng `fn rebuild` nhưng trên **dữ liệu thật**: 500 nến BTCUSDT/1m → plan →
kernel đặt lệnh. Test thử nhiều sàn theo thứ tự:

1. **OKX** (`www.okx.com`) — sàn chính, trả số là **string**, thứ tự **mới-trước**,
   cột `confirm` → phải reverse + lọc nến đã đóng.
2. `data-api.binance.vision` — endpoint data Binance.
3. `api.binance.com` — thường bị **HTTP 451** theo IP của GitHub runner.

Override endpoint: `OPSENSE_BINANCE_KLINES_URL=...`.

Vì sao `continue-on-error` trong CI: API bên ngoài không thuộc quyền kiểm soát
của repo (rate-limit, geo-block, sự cố). Fail ở đây không có nghĩa code sai, nhưng
bỏ hẳn thì mất đúng thứ cần xem khi sửa script — log vẫn in ra plan (số ô,
levels, win-prob).

## 4. Tầng 4 — Integration + container E2E

### 4.1 Dựng stack

```bash
earthly +integration-images          # build 4 image runtime
docker compose up -d --wait --wait-timeout 240
```

Compose hiện có **9 service** (không phải 5/6 như bản cũ của tài liệu này):

| Service | Vai trò |
|---|---|
| `postgres` | DB cho auth/admin/OAuth (`DB_DSN`) |
| `valkey` | cache, publish `6380` ra host cho test |
| `opsense-dex` | OIDC provider cho `integration_oauth` |
| `opsense-runner` | runner echo, host port `50051` |
| `opsense-runner-python` | host port `50052` |
| `opsense-runner-julia` | host port `50053` |
| `opsense-serve` | gateway, `8080` |
| `rustfs` | object store S3-compatible (thay MinIO), `9000` |
| `rustfs-bucket` | tạo bucket `opsense-lake` một lần |

Lưu ý: 3 runner dùng **3 port host khác nhau** (bản cũ của tài liệu này ghi cả 3
là `50051` → lệnh `grpcurl` sẽ luôn trỏ nhầm runner).

### 4.2 Smoke test

```bash
curl -fsS http://localhost:8080/health                       # {"ok":true}

curl -fsS -XPOST http://localhost:8080/api/repl/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ status { nodes { id } stations { id kind } } }"}'

for p in 50051 50052 50053; do
  grpcurl -plaintext 127.0.0.1:$p opsense.proto.KernelRunner/Health
done

docker compose exec -T postgres pg_isready -U opsense
docker compose exec -T valkey valkey-cli ping             # PONG
```

Lưu ý: **không có** route `/metrics` và **không có** `/mcp`. Đổi cấu hình qua
GraphQL `Mutation.patchComponent`; MCP chỉ qua stdio (`opsense mcp`).

### 4.3 Chạy integration tests

```bash
OPSENSE_SERVE_URL=http://localhost:8080 \
OPSENSE_DEX_ISSUER=http://localhost:5556/dex \
OPSENSE_RUNNER_ECHO=127.0.0.1:50051 \
OPSENSE_RUNNER_PYTHON=127.0.0.1:50052 \
OPSENSE_RUNNER_JULIA=127.0.0.1:50053 \
DB_DSN=postgres://opsense:opsense123@127.0.0.1:5432/opsense \
OPSENSE_S3_ACCESS_KEY_ID=opsense \
OPSENSE_S3_SECRET_ACCESS_KEY=opsense123 \
OPSENSE_INTEGRATION=true \
cargo test --workspace --release \
  --test integration_health \
  --test integration_oauth \
  --test integration_runner_grpc \
  --test integration_repl_pty \
  --test integration_storage \
  --test integration_s3_lake \
  --test integration_mcp_config \
  -- --test-threads=1 --nocapture
```

`OPSENSE_INTEGRATION=true` là chốt an toàn: test có env nhưng thiếu hạ tầng sẽ
panic kèm message rõ thay vì skip im lặng (xem `common/mod.rs`).

### 4.4 E2E config từng chiến lược

Chạy tuần tự từng config trong `strategies/`, mỗi cái một compose lifecycle:

```bash
# prometheus
OPSENSE_S3_ENDPOINT=http://127.0.0.1:9000 OPSENSE_INTEGRATION=true \
  cargo test --workspace --release --test e2e_prometheus_config -- --test-threads=1

# predict
OPSENSE_S3_ENDPOINT=http://127.0.0.1:9000 OPSENSE_INTEGRATION=true \
  cargo test --workspace --release --test e2e_predict_config -- --test-threads=1

# binance (cần mock WS nội bộ, không cần sàn thật)
OPSENSE_INTEGRATION=true \
  cargo test --workspace --release --test e2e_binance_config -- --test-threads=1
```

`OPSENSE_S3_ENDPOINT` ở đây là biến **của test harness** (runtime đọc endpoint từ
`[storage.s3].endpoint` trong config), dùng để trỏ từ `rustfs:9000` (hostname
trong compose network, không resolve được từ host) sang `127.0.0.1:9000`.

Các test này chờ flush theo `s3_flush_interval_secs = 15` rồi khẳng định parquet
local **và** mirror S3 tại `opsense-lake/<prefix>/**` — nên chậm hơn unit test.

### 4.5 Dọn dẹp

```bash
docker compose down -v
```

## 5. Build & release

- **Build image**: `earthly +integration-images` (target `+integration-images` là
  thứ CI gọi; `+all` dùng cho release image đầy đủ).
- **CI**: `.github/workflows/ci.yml` — job `Build Release Binaries` → `Unit Tests`
  (`cargo nextest run --workspace --locked --cargo-profile release`) →
  `Strategy E2E (Rhai grid trading)` → `Prepare Integration Environment` →
  `Integration Tests` (matrix theo chiến lược).
- **Release**: `.github/workflows/release.yml` là workflow **cargo-dist** (job
  `plan`, `build-local-artifacts`, `build-global-artifacts`), trigger tag dạng
  `X.Y.Z`, `IMAGE_PREFIX: hungpham10/opsense`. **Không** phải job `build-push`
  Earthly và **không** phải tag `v*`.

## 6. Khi test đỏ — đọc log theo thứ tự

1. **Fail ở tầng nào?** Tầng 1/2 không cần compose → lỗi logic hoặc contract.
   Tầng 4 đỏ thường là hạ tầng (service chưa healthy, port lệch).
2. **Bật log đầy đủ cho tầng strategy**:
   `RUST_LOG=warn,opsense_components=info,opsense_rhai=debug,opsense_mlib=info`.
3. **Compose**: `docker compose ps --format json | jq -r '.[] | "\(.Service): \(.Health)"'`
   rồi `docker compose logs --tail 200 opsense-serve` / `opsense-runner-python`.
4. **Phụ thuộc tốc độ máy**: không khoá cứng số observation/cursor trong assert —
   số nến đã xử lý phụ thuộc tốc độ chạy. Guard nhận input từ agent thì test cả
   giá trị biên `MIN`/`MAX`, không chỉ giá trị thường gặp (đã có case
   `i64::MIN..i64::MAX` cho `queryTimeseries`).
