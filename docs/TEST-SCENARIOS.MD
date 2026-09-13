# Test scenarios — Container image (Phase 5)

> Kịch bản test cho Phase 5: Earthfile-based build & deployment.
> Mục tiêu: xác nhận 4 image (`opsense-serve`, `opsense-runner`,
> `opsense-runner-python`, `opsense-runner-julia`) build đúng, compose
> chạy đủ service, network kết nối host↔runner qua gRPC, health check
> pass, không có regression so với Phase 1-4.
>
> Cập nhật: 2026-09-03 — Kịch bản sinh sau khi audit PLAN.MD + architecture.md.
> Trạng thái file: **DRAFT** — chờ user approve rồi mới chạy.

## 0. Tiền điều kiện (preflight)

### 0.1 Công cụ bắt buộc

| Tool | Version | Check |
|---|---|---|
| Docker | ≥ 24.0 (buildkit) | `docker --version` |
| Earthly | ≥ 0.8 (khớp Earthfile VERSION) | `earthly --version` |
| docker compose plugin | ≥ v2.20 | `docker compose version` |
| curl | any | `curl --version` |
| grpcurl | ≥ 1.8 (debug gRPC runner) | `grpcurl --version` |
| jq | any | `jq --version` |

Cài Earthly:
```bash
# macOS
brew install earthly/tap/earthly
# Linux
sudo /bin/sh -c 'wget -O- https://get.earthly.dev | bash'
earthly bootstrap  # tải base image lần đầu
```

### 0.2 File pre-flight check (đảm bảo tồn tại trước khi build)

```bash
[ -f Earthfile ] && echo "✓ Earthfile" || echo "✗ MISSING Earthfile"
[ -f .earthignore ] && echo "✓ .earthignore" || echo "✗ MISSING .earthignore"
[ -f docker-compose.yml ] && echo "✓ docker-compose.yml" || echo "✗ MISSING docker-compose.yml"
[ -f scripts/entrypoint.sh ] && echo "✓ entrypoint" || echo "✗ MISSING entrypoint"
[ -f scripts/nginx.sh ] && echo "✓ nginx" || echo "✗ MISSING nginx"
[ -f scripts/alloy.sh ] && echo "✓ alloy" || echo "✗ MISSING alloy"
[ -f conf/supervisor/opsense.conf ] && echo "✓ supervisor" || echo "✗ MISSING supervisor"
[ -f conf/nginx/http.conf ] && echo "✓ nginx http.conf" || echo "✗ MISSING"
[ -f conf/nginx/www.conf ] && echo "✓ nginx www.conf" || echo "✗ MISSING"
[ -f conf/config.alloy ] && echo "✓ alloy config" || echo "✗ MISSING"
[ -f .github/workflows/release.yml ] && echo "✓ release.yml" || echo "✗ MISSING release.yml"
```

**Phát hiện từ audit 2026-09-03**: `docker-compose.yml` chưa tồn tại trên
đĩa dù PLAN.MD ghi "đã làm" — cần tạo file này (xem §7 prerequisite).

## 1. Kịch bản A — Build image local (Earthly only, không compose)

**Mục đích**: xác nhận Earthfile đúng cú pháp, build được 4 image dạng
artifact trước khi chạy compose.

**Thời gian dự kiến**: lần đầu 25-40 phút (do cargo-chef build 1 lần);
lần sau 3-5 phút (cache hit).

### Bước A1 — Smoke build binary target

```bash
earthly +opsense
earthly +kernel-echo
earthly +kernel-python
earthly +kernel-julia
```

**Verify**:
- Không có error `error: recipe ... not found`.
- Artifact `target/release/opsense` (~30MB) sinh ra.
- Earthly in `--> exporting ... ` rồi `Earthfile target completed`.

**Expected fail & fix** (đã biết từ PLAN.MD §10.8):
```
error: failed to run build script for `opsense-proto`
   = note: Please check that protobuf compiler is installed
```
**Nguyên nhân**: `+base` chưa cài `protobuf-compiler`. **Fix trước khi
test**: thêm `protobuf-compiler` vào apt-get ở `+base`:

```diff
 RUN apt-get update && \
     DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
-        pkg-config ca-certificates && \
+        pkg-config ca-certificates protobuf-compiler && \
     apt-get clean && rm -rf /var/lib/apt/lists/*
```

### Bước A2 — Build 4 image với tag `local`

```bash
earthly +all-local
```

**Verify**:
- 4 image xuất hiện trong `docker images`:
  ```bash
  docker images | grep opsense
  # opsense-serve          local
  # opsense-runner         local
  # opsense-runner-python  local
  # opsense-runner-julia   local
  ```
- Image size hợp lý:
  - `opsense-serve`: 500MB-1GB (openresty + alloy + lua modules)
  - `opsense-runner`: 200-300MB (debian-slim + binary)
  - `opsense-runner-python`: 800MB-1.2GB (python 3.12 + numpy/pandas/arrow)
  - `opsense-runner-julia`: 1.5-2GB (julia 1.10 bookworm)
- Tag = `local`, **không** phải `latest` (đã set `--build-arg VERSION=local`).

### Bước A3 — Inspect image metadata

```bash
docker inspect opsense-serve:local | jq '.[0] | {
  Cmd, Entrypoint, ExposedPorts, Env,
  Size: .Size,
  Layers: (.RootFS.Layers | length)
}'
```

**Verify**:
- `Entrypoint`: `["/app/entrypoint.sh", "/usr/bin/supervisord", "-n"]`
- `ExposedPorts`: `{"8080/tcp": {}}`
- `Cmd`: null (entrypoint chạy trực tiếp).
- Env có `OPSENSE_RUNNER_GRPC=opsense-runner:50051` (set qua supervisor, **không** phải image env).

Lặp lại cho 3 runner image — `ExposedPorts` phải là `{"50051/tcp": {}}`,
`Env` có `OPSENSE_RUNNER_BIND=0.0.0.0:50051` và `OPSENSE_KERNEL=...`.

## 2. Kịch bản B — Compose stack lên & health check (smoke E2E)

**Mục đích**: chạy đủ 6 service, kiểm tra dependency order, verify
health endpoint + GraphQL + gRPC.

### Bước B0 — Tạo docker-compose.yml (nếu chưa có)

> File này chưa tồn tại trên đĩa — đây là blocker của Phase 5. Cần tạo
> theo template:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: opsense
      POSTGRES_PASSWORD: opsense
      POSTGRES_DB: opsense
    volumes:
      - pg_data:/var/lib/postgresql/data
      - ./sql/postgres:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U opsense"]
      interval: 5s
      timeout: 3s
      retries: 10
    networks: [opsense-net]

  valkey:
    image: valkey/valkey:9.0-alpine
    command: ["valkey-server", "--appendonly", "yes"]
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10
    networks: [opsense-net]

  opsense-runner:
    image: opsense-runner:${OPSENSE_TAG:-local}
    networks: [opsense-net]

  opsense-runner-python:
    image: opsense-runner-python:${OPSENSE_TAG:-local}
    networks: [opsense-net]

  opsense-runner-julia:
    image: opsense-runner-julia:${OPSENSE_TAG:-local}
    networks: [opsense-net]

  opsense:
    image: opsense-serve:${OPSENSE_TAG:-local}
    ports:
      - "8080:8080"
    volumes:
      - ./conf/opsense.conf.toml:/app/opsense.conf.toml:ro
    depends_on:
      postgres: { condition: service_healthy }
      valkey:   { condition: service_healthy }
      opsense-runner: { condition: service_started }
    networks: [opsense-net]

networks:
  opsense-net:
    driver: bridge

volumes:
  pg_data:
```

### Bước B1 — Bring up stack

```bash
docker compose up -d
```

**Verify**:
- `docker compose ps` — 6 service đều `running` hoặc `healthy`.
- `docker compose logs opsense | head -50` — không có panic/error trong 10s đầu.

### Bước B2 — Health check HTTP

```bash
curl -fsS http://localhost:8080/health
# Expected: {"status":"ok",...}  (tùy impl AppState::health_check)
curl -fsSI http://localhost:8080/health
# Expected: HTTP/1.1 200 OK
```

**Verify fail mode** (cố ý):
```bash
# Stop opsense, restart 3 lần — supervisor phải auto-restart
docker compose restart opsense
sleep 10
curl -fsS http://localhost:8080/health  # vẫn 200
docker compose logs opsense | grep -E "started|supervisord"
```

### Bước B3 — GraphQL introspection

```bash
curl -fsS -X POST http://localhost:8080/graphql \
    -H 'Content-Type: application/json' \
    -d '{"query":"{ status { nodes { id type inputs } stations { id kind } } }"}' \
    | jq '.data.status'
```

**Verify**:
- `nodes[]` chứa ít nhất 1 entry (clock_source từ opsense.conf.toml).
- `stations[]` liệt kê đúng station từ config.

### Bước B4 — gRPC runner reachability (debug)

```bash
# List services của runner echo
grpcurl -plaintext opsense-runner:50051 list
# Expected: grpc.health.v1.Health, opsense.proto.KernelRunner

# Health
grpcurl -plaintext opsense-runner:50051 \
    opsense.proto.KernelRunner/Health
# Expected: {"kernelName":"runner/ipc",...}

# Repeat for python runner
grpcurl -plaintext opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Health
```

**Verify fail mode**:
```bash
# Stop 1 runner, verify host (opsense) vẫn serve telemetry (Tầng 1 độc lập)
docker compose stop opsense-runner-python
sleep 5
curl -fsS http://localhost:8080/health  # vẫn 200 — telemetry không phụ thuộc runner
docker compose start opsense-runner-python
```

### Bước B5 — Ed25519 auth E2E (host → runner)

Dùng `opsense repl --runner http://opsense-runner:50051` (Phase 2 revised):

```bash
docker run --rm --network opsense-rs_opsense-net \
    -v /var/run/docker.sock:/var/run/docker.sock \
    opsense-serve:local \
    opsense repl --runner http://opsense-runner:50051 <<EOF
:py
print(1+1)
:exit
EOF
```

> Lưu ý: image `opsense-serve` không có subcommand `repl` riêng — REPL
> build cùng host. Cần exec vào container chạy `opsense repl --runner
> http://opsense-runner:50051`.

**Cách thực tế hơn**:
```bash
docker compose exec opsense \
    /app/opsense repl --runner http://opsense-runner:50051
```

Trong REPL:
```
:py
print("hello from python kernel")
```
Ctrl-D để thoát.

**Verify**:
- REPL hiển thị `2` (echo) hoặc `hello from python kernel` (python kernel).
- Không có `auth failed` hoặc `signature mismatch`.

## 3. Kịch bản C — Network isolation & multi-runner

**Mục đích**: xác nhận host có thể kết nối đồng thời nhiều runner (echo,
python, julia), mỗi runner là session độc lập.

### Bước C1 — Test 3 runner từ host

```bash
# Echo runner
docker compose exec opsense \
    /app/opsense repl --runner http://opsense-runner:50051 <<< ':py
print("echo")'
# (echo kernel không support :py, dùng :echo)
docker compose exec opsense \
    /app/opsense repl --runner http://opsense-runner:50051 <<< ':echo
echo hello'

# Python runner
docker compose exec opsense \
    /app/opsense repl --runner http://opsense-runner-python:50051 <<< ':py
import sys
print(sys.version)'

# Julia runner
docker compose exec opsense \
    /app/opsense repl --runner http://opsense-runner-julia:50051 <<< ':jl
println(VERSION)'
```

**Verify**:
- Mỗi runner trả output riêng (echo trả nguyên xi, python trả version,
  julia trả VERSION string).
- Không có kernel crash (`docker compose ps` không `Exit 137`).

### Bước C2 — Session isolation

Mở 2 REPL song song tới cùng runner, kiểm tra session_id khác nhau:

```bash
# Session A
grpcurl -plaintext -d '{}' opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Start | jq .sessionId
# Lưu lại sessionId_A

# Session B
grpcurl -plaintext -d '{}' opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Start | jq .sessionId
# sessionId_B phải khác sessionId_A
```

**Verify**:
- Hai session_id là 2 base64 string khác nhau (= 2 Ed25519 keypair).
- Execute trên A không ảnh hưởng B (vd A set `x=1`, B đọc `x` → NameError).

### Bước C3 — Implicit keepalive & sweeper

```bash
# Mở session, chờ > idle_timeout (config RunnerConfig, default 300s)
grpcurl -plaintext -d '{}' opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Start | jq .sessionId > /tmp/sid

SID=$(cat /tmp/sid)

# Wait > 300s
sleep 320

# Execute lại — expect "session not found" hoặc auto-restart
grpcurl -plaintext -d "{\"sessionId\":\"$SID\",\"code\":\"1+1\"}" \
    opsense-runner-python:50051 opsense.proto.KernelRunner/Execute 2>&1 | head -5
```

**Verify**: response báo session hết hạn hoặc tự reopen. Tuỳ impl Phase 4
mà runner xử lý (xem `crates/opsense-runner/src/session.rs`).

## 4. Kịch bản D — Hot reload & persistence

### Bước D1 — Reload pipeline qua GraphQL

```bash
# Trước: status có 1 station
curl -sS -X POST http://localhost:8080/graphql \
    -H 'Content-Type: application/json' \
    -d '{"query":"{ stations { id kind } }"}'

# Reload thêm 1 node clock_source
curl -sS -X POST http://localhost:8080/graphql \
    -H 'Content-Type: application/json' \
    -d '{
      "query": "mutation Reload($c: [ComponentInput!]!) { reload(components: $c) { reloaded nodes { id } } }",
      "variables": {
        "c": [{
          "type": "clock_source",
          "id": "extra-clock",
          "inputs": []
        }]
      }
    }'

# Sau: status có 2 station
curl -sS -X POST http://localhost:8080/graphql \
    -H 'Content-Type: application/json' \
    -d '{"query":"{ stations { id kind } }"}'
```

**Verify**:
- Reload trả `reloaded: true`.
- Status sau có node mới.

### Bước D2 — Postgres persistence (sys_user CRUD)

```bash
# Issue token
curl -sS -X POST http://localhost:8080/api/admin/v1/tokens/users \
    -H 'Content-Type: application/json' \
    -d '{"tenant_id":1,"user_id":"alice"}' | jq

# Verify token
curl -sS -X POST http://localhost:8080/api/admin/v1/token/introspect \
    -H 'Content-Type: application/json' \
    -d '{"token":"<token_from_above>"}' | jq

# Restart postgres — token vẫn còn
docker compose restart postgres
sleep 10
curl -sS -X POST http://localhost:8080/api/admin/v1/token/introspect \
    -H 'Content-Type: application/json' \
    -d '{"token":"<token_from_above>"}' | jq
```

**Verify**: cùng token, cùng `expires_at` — không bị mất.

### Bước D3 — Valkey cache

```bash
docker compose exec valkey valkey-cli ping
# PONG
docker compose exec valkey valkey-cli dbsize
# (số key — phụ thuộc traffic)
```

## 5. Kịch bản E — Failure modes & rollback

### Bước E1 — Container crash → supervisor restart

```bash
# Trước
docker compose ps opsense  # Up X minutes (healthy)

# Kill -9 (SIGKILL, không graceful)
docker compose exec opsense kill -9 $(pgrep opsense)

# Đợi supervisor phát hiện & restart
sleep 30
docker compose ps opsense  # Up X seconds (restarting → healthy)
docker compose logs opsense | tail -20
```

**Verify**:
- `docker compose ps` cho thấy container restart (status `Up X seconds`).
- `supervisord` log: `app entered RUNNING state`, sau đó lại EXITED → RUNNING.
- `curl /health` 200 lại sau ≤30s.

### Bước E2 — Postgres outage → host vẫn serve telemetry

```bash
docker compose stop postgres
sleep 15

# Tầng 1 (telemetry) vẫn hoạt động
curl -fsS http://localhost:8080/health   # 200
curl -sS -X POST http://localhost:8080/graphql \
    -H 'Content-Type: application/json' \
    -d '{"query":"{ stations { id } }"}'

# Tầng 2 (admin API / token) fail
curl -sS -X POST http://localhost:8080/api/admin/v1/tokens/users \
    -H 'Content-Type: application/json' \
    -d '{}'
# Expected: 500 hoặc 503 (DB connection error)

docker compose start postgres
sleep 10
curl -fsS -X POST http://localhost:8080/api/admin/v1/tokens/users \
    -H 'Content-Type: application/json' -d '{}'
# Expected: 200
```

**Verify**: Tầng 1 và Tầng 2 độc lập như architecture.md §1 đã cam kết.

### Bước E3 — Runner crash → host vẫn serve telemetry

```bash
docker compose stop opsense-runner
sleep 5

curl -fsS http://localhost:8080/health  # 200
docker compose start opsense-runner
sleep 5
grpcurl -plaintext opsense-runner:50051 \
    opsense.proto.KernelRunner/Health
```

### Bước E4 — Rollback về tag trước (giả lập release v0.0.1 → v0.0.0)

> Cần có 2 tag pre-built trên local. Test scenario:

```bash
# Save state
docker compose down -v  # giữ image, xóa container+volume

# Pull tag cũ
docker compose pull  # không có remote registry local, nên test thủ công:
OPSENSE_TAG=v0.0.0 docker compose up -d
# Nếu image v0.0.0 không có local, cần pre-build trước:
#   earthly --push --build-arg VERSION=v0.0.0 +all  (hoặc)
#   earthly --build-arg VERSION=v0.0.0 +all-local

# Verify v0.0.0 chạy
curl -fsS http://localhost:8080/health
```

## 6. Kịch bản F — Resource & performance baseline

### Bước F1 — Boot time

```bash
# Cold start
docker compose down
time docker compose up -d
# Đo từ "up -d" đến khi tất cả healthy
```

**Baseline kỳ vọng**:
- 6 service cold start: ≤ 60s
- Just opsense-serve: ≤ 30s
- Earthly rebuild incremental: ≤ 5 phút

### Bước F2 — Memory footprint

```bash
docker stats --no-stream opsense opsense-runner opsense-runner-python opsense-runner-julia postgres valkey
```

**Baseline kỳ vọng** (RSS, MB):
| Service | Min | Max |
|---|---|---|
| opsense (openresty + alloy + app) | 150 | 350 |
| opsense-runner (debian + echo) | 30 | 80 |
| opsense-runner-python | 100 | 250 |
| opsense-runner-julia | 200 | 500 |
| postgres | 50 | 200 |
| valkey | 20 | 60 |

### Bước F3 — gRPC latency

```bash
# 100 round-trip health check
time for i in $(seq 1 100); do
    grpcurl -plaintext -d '{}' opsense-runner:50051 \
        opsense.proto.KernelRunner/Health > /dev/null
done
```

**Baseline kỳ vọng**:
- p50 ≤ 5ms (in-network)
- p99 ≤ 20ms
- Không có request nào `UNAVAILABLE` hoặc `DEADLINE_EXCEEDED`.

## 7. Kịch bản G — CI/CD pipeline (GH Action)

> Chạy khi push tag `v0.0.1` đầu tiên. Yêu cầu đã có workflow
> `.github/workflows/release.yml`.

### Bước G1 — Trigger thử (workflow_dispatch)

Trên GitHub UI → Actions → Release Images → Run workflow → tag `v0.0.0-rc1`.

**Verify**:
- Job `build-push` chạy ≤ 40 phút.
- 4 image push lên `ghcr.io/lap02921/opsense-*:v0.0.0-rc1`:
  ```bash
  docker pull ghcr.io/lap02921/opsense-serve:v0.0.0-rc1
  docker pull ghcr.io/lap02921/opsense-runner:v0.0.0-rc1
  ```
- GH Action summary list 4 image URL.

### Bước G2 — Production deploy với tag release

```bash
# Local: pull + chạy
export OPSENSE_TAG=v0.0.0-rc1
docker compose pull
OPSENSE_TAG=$OPSENSE_TAG docker compose up -d

# Verify
curl -fsS http://localhost:8080/health
docker compose ps  # 6 service healthy
```

### Bước G3 — Tag matrix

Test các tag format khác nhau:

| Tag | Expected | Note |
|---|---|---|
| `v0.0.0-rc1` | ✓ image tag `v0.0.0-rc1` | pre-release |
| `v1.0.0` | ✓ image tag `v1.0.0` | semver |
| `v1.0.0-alpha` | ✓ image tag `v1.0.0-alpha` | semver pre-release |
| `release-test` | ✗ workflow không trigger (cần `v*`) | test workflow_dispatch |
| `latest` | ✗ workflow không trigger | không hợp lệ |

> Lưu ý: PLAN.MD §10.8 đã ghi caveat về Earthfile `+base` thiếu
> `protobuf-compiler`. Build trong CI dùng base `ubuntu-latest` đã có
> sẵn protoc trong cache Earthly, nhưng build lần đầu có thể fail. Cần
> fix trước khi trigger workflow lần đầu.

## 8. Kịch bản H — Security smoke

### Bước H1 — Auth bắt buộc trên runner

```bash
# Gửi request KHÔNG có metadata → expect UNAUTHENTICATED
grpcurl -plaintext -d '{}' opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Start 2>&1 | head -5

# Gửi metadata sai signature → expect UNAUTHENTICATED
grpcurl -plaintext \
    -H 'x-session-id: invalid' \
    -H 'x-timestamp: 9999999999' \
    -H 'x-nonce: 1' \
    -H 'x-signature: invalid' \
    -d '{}' opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Start 2>&1 | head -5
```

**Verify**: cả 2 trả `Code = Unauthenticated`.

### Bước H2 — Timestamp window

```bash
# Timestamp ngoài ±30s → reject
grpcurl -plaintext \
    -H "x-session-id: $(openssl rand -base64 32)" \
    -H 'x-timestamp: 1000000' \
    -H 'x-nonce: 1' \
    -H 'x-signature: dummy' \
    -d '{}' opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Start 2>&1 | head -5
```

### Bước H3 — Secrets không lộ

```bash
# Build context KHÔNG chứa secrets/
docker run --rm opsense-serve:local find / -name "secrets" 2>/dev/null
# Expected: không có output
docker run --rm opsense-serve:local cat /app/.env 2>/dev/null
# Expected: không có file
```

**Verify**: `.earthignore` loại trừ `secrets/` và `.env` (đã thấy
trong audit). Image clean.

## 9. Kịch bản I — Cleanup & tear down

```bash
# Stop & xóa containers + network
docker compose down

# Xóa cả volumes (reset DB)
docker compose down -v

# Xóa image (giải phóng disk)
docker image prune -af --filter "label=earthly"
docker rmi opsense-serve:local opsense-runner:local \
    opsense-runner-python:local opsense-runner-julia:local 2>/dev/null

# Xóa Earthly cache (~5-10GB)
earthly prune
```

## 10. Tiêu chí pass/fail tổng hợp

| Kịch bản | Pass | Fail | Action khi fail |
|---|---|---|---|
| A — Build local | 4 image build OK | Earthly error | Xem log, fix Earthfile |
| B — Compose smoke | 6 service healthy, /health 200 | Service không healthy | `compose logs <svc>` |
| C — Network isolation | 3 runner reachable | Connection refused | Check `depends_on` + DNS |
| D — Persistence | Token survive restart | Token mất | Check Postgres health |
| E — Failure modes | Restart tự động, Tầng 1 OK khi Tầng 2 down | Crash loop | Fix supervisor config |
| F — Performance | Boot ≤60s, latency p99 ≤20ms | Vượt baseline | Profile, optimize |
| G — CI/CD | 4 image push lên ghcr.io | Workflow fail | Xem GH Action log |
| H — Security | Auth reject, secrets clean | Auth bypass | Critical — block release |

**Khi fail**:
1. Capture log: `docker compose logs --no-color > /tmp/opsense-fail.log`.
2. Save state: `docker compose down` (không `-v` để giữ DB để debug).
3. Tạo issue checklist với: step fail, expected, actual, log excerpt.
4. **KHÔNG** tự động retry — fix root cause trước.

## 11. Prerequisite chưa xong (cần hoàn thành trước khi chạy test)

> Tổng hợp từ audit 2026-09-03. Phase 5 PLAN.MD ghi "đã xong" nhưng
> chưa verify trên đĩa:

- [ ] **`docker-compose.yml`** — file chưa tồn tại. Cần tạo theo
      template §B0.
- [ ] **`Earthfile +base`** — thiếu `protobuf-compiler`. Build sẽ fail
      ở `opsense-proto` `build.rs`. Cần patch §A1.
- [ ] **`Makefile`** — PLAN.MD ghi "đã làm" nhưng `ls` không thấy.
      Không blocker cho test, nhưng cần update doc cho đúng.
- [ ] **`Vector source`** — `conf/opsense.conf.toml` reference
      `http://vector:8686` nhưng compose chưa có service `vector`.
      Capacity telemetry path sẽ fail khi smoke test. Cần 1 trong:
      (a) thêm `vector` service vào compose, hoặc (b) sửa config sang
      dummy URL + verify graceful degradation.
- [ ] **Config mount** — `opsense-serve` image không có volume
      `conf/opsense.conf.toml` baked-in. Cần mount qua `volumes:` trong
      compose (xem template §B0).

## 12. Sau khi pass — bước tiếp theo

1. Cập nhật `CHECKLIST.MD` (nếu có) ghi Phase 5 done.
2. Cập nhật PLAN.MD "Còn lại" → "Đã xong".
3. Cập nhật architecture.md §10 nếu có thay đổi số image hoặc port.
4. Tag release đầu tiên `v0.0.1` trên GitHub.
5. Setup monitoring (Grafana dashboard cho 4 image health).

---

**Phụ lục A — Shell helper chạy tất cả smoke test**

```bash
#!/usr/bin/env bash
# scripts/test-smoke.sh
set -euo pipefail

pass() { echo "✓ $1"; }
fail() { echo "✗ $1"; exit 1; }

[ "$(docker compose ps --services | wc -l)" = "6" ] || fail "compose service count != 6"

curl -fsS http://localhost:8080/health >/dev/null && pass "/health 200" || fail "/health"

grpcurl -plaintext opsense-runner:50051 \
    opsense.proto.KernelRunner/Health >/dev/null && pass "runner echo health" || fail "runner echo"

grpcurl -plaintext opsense-runner-python:50051 \
    opsense.proto.KernelRunner/Health >/dev/null && pass "runner python health" || fail "runner python"

grpcurl -plaintext opsense-runner-julia:50051 \
    opsense.proto.KernelRunner/Health >/dev/null && pass "runner julia health" || fail "runner julia"

docker compose exec -T postgres pg_isready -U opsense >/dev/null \
    && pass "postgres ready" || fail "postgres"

docker compose exec -T valkey valkey-cli ping | grep -q PONG \
    && pass "valkey PONG" || fail "valkey"

echo "=== ALL SMOKE TESTS PASSED ==="
```

**Phụ lục B — Quick rollback script**

```bash
#!/usr/bin/env bash
# scripts/test-rollback.sh
set -euo pipefail

TAG=${OPSENSE_TAG:-local}

# Save current state
docker compose down || true

# Rollback to previous tag (giả định OPSENSE_PREV_TAG set)
if [ -n "${OPSENSE_PREV_TAG:-}" ]; then
    OPSENSE_TAG=$OPSENSE_PREV_TAG docker compose up -d
    echo "Rolled back to $OPSENSE_PREV_TAG"
else
    echo "OPSENSE_PREV_TAG not set; nothing to rollback"
fi
```