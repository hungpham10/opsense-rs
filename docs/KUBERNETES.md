# Go-live Opsense lên Kubernetes — v1.0.10

Hướng dẫn chuẩn bị và thực thi go-live **Opsense v1.0.10** lên Kubernetes
(cluster-generic: EKS / GKE / K3s / RKE / Talos…). Đọc song song:
[`docs/architecture.md`](architecture.md) (kiến trúc) và
[`docs/GUIDE.md`](GUIDE.md) (usage/API).

> Trạng thái: **PREP** — kế hoạch chuẩn bị go-live cho v1.0.10 ("building playground"
> → hướng tới production). Mọi giá trị `<<PLACEHOLDER>>` phải thay trước khi
> `kubectl apply`.

---

## 1. Mục tiêu

- Chạy **4 image chính thức** `v1.0.10` từ GHCR dưới dạng workload Kubernetes:
  `opsense-serve`, `opsense-runner`, `opsense-runner-python`,
  `opsense-runner-julia`.
- Phụ thuộc: PostgreSQL 16, Valkey/Redis, Dex (OIDC), (tuỳ chọn) Alloy/OTLP
  collector.
- APP_ENV = `prod`, nạp seed `sql/postgres/prod/*.sql` đúng một lần.
- HTTPS qua Ingress + cert-manager; expose REST `:8080` (nginx front trong
  image) làm cổng duy nhất ra ngoài.

---

## 2. Kiến trúc deployment

```
                        ┌─────────────────────────────┐
 Internet ──Ingress────▶│  Service: opsense-serve:8080│──▶ Pod opsense-serve
 (TLS, path /)          └─────────────────────────────┘        │
                                                                │ nginx (in image)
                        ┌──────────────────────────────────────┘ supervisord
                        │    supervisord (in image)
                        │      ├─ nginx.sh        ──▶ http :8080
                        │      └─ /app/opsense serve        │
                        │           GATEWAY_LISTENER=unix    │
                        │           (nginx ↔ UDS /var/run/axum)
   opsense-runner ──────┘ gRPC :50051 (echo kernel, default)
   opsense-runner-python ── gRPC :50051
   opsense-runner-julia  ── gRPC :50051

   opsense-postgres ── Service :5432   (schema + sys_token_map/secrets)
   opsense-valkey   ── Service :6379   (nginx session cache)
   opsense-dex      ── Service :5556   (OIDC, issuer https://<<DOMAIN>>/dex)

   storage: PVC /app/.opsense  (lakehouse parquet duckdb/lmdb)
            hoặc [storage] backend="s3" → object store
```

Quan trọng — các giá trị được **ép cứng trong image**:

| Ép cứng bởi | Giá trị | Hệ quả ở k8s |
|---|---|---|
| `conf/supervisor/opsense.conf` | `OPSENSE_RUNNER_GRPC=opsense-runner:50051` | Service runner mặc định **bắt buộc đặt tên `opsense-runner`** cùng namespace |
| `conf/supervisor/opsense.conf` | `GATEWAY_LISTENER=unix` | Listener UDS chỉ nội bộ container; nginx là front HTTP duy nhất (`:8080`) |
| `conf/supervisor/opsense.conf` | `HOME=/app` | Đường dẫn lakehouse mặc định = `/app/.opsense/lakehouse` → PVC mount tại `/app/.opsense` |
| `Earthfile serve` | `ENTRYPOINT entrypoint.sh` (supervisord -n) | Pod **không** nên override `command`; chỉ truyền env |

---

## 3. Image & registry

| Image (GHCR) | Base | Entrypoint | Port | Health |
|---|---|---|---|---|
| `ghcr.io/hungpham10/opsense-serve:v1.0.10` | OpenResty bookworm + Alloy + supervisor | `/app/entrypoint.sh /usr/bin/supervisord -n` | `8080` | `curl :8080/health` |
| `ghcr.io/hungpham10/opsense-runner:v1.0.10` | debian:bookworm-slim | `/app/opsense runner` (`OPSENSE_RUNNER_BIND=0.0.0.0:50051`) | `50051` | `/app/opsense runner --health-check` |
| `ghcr.io/hungpham10/opsense-runner-python:v1.0.10` | python:3.12-slim (+numpy pandas pyarrow protobuf) | giống runner | `50051` | giống runner |
| `ghcr.io/hungpham10/opsense-runner-julia:v1.0.10` | julia:1.10-bookworm (+Arrow/DataFrames/CSV/Plots) | giống runner | `50051` | giống runner |

### ✅ Đã sửa: build 1 tag multi-arch

Trước đây workflow chạy `earthly --push` **2 lần** (amd64 → arm64) cùng một tag
→ lần sau ghi đè lần trước; tag `v1.0.10` hiện chỉ còn `linux/arm64`. Đã sửa
`image.yml` + `Earthfile` để **một tag, một lần build**:

1. CI stage binaries thành `binaries/<arch>/` (`x86_64/`, `aarch64/`);
2. `+build-binaries` tự chọn đúng bộ binaries theo `uname -m` (Earthly build
   `linux/arm64` chạy dưới QEMU nên trả `aarch64`);
3. Workflow chạy **một lần** `earthly --push ... +multi` (target `multi` trong
   Earthfile dùng `BUILD --platform linux/amd64 --platform linux/arm64 +all`)
   → Earthly tự push manifest list multi-arch cho **đúng tag `v1.0.10`** của cả 4
   image (không tag `-amd64`/`-arm64`, không bước ghép tay);
4. Step cuối tự verify bằng `docker manifest inspect`.

**Còn một việc bắt buộc trước go-live:** chạy lại workflow `push-image` cho tag
`v1.0.10` (workflow_dispatch với `version=v1.0.10`, hoặc push lại tag `v1.0.10`)
để các tag hiện tại được rebuild. Xác minh sau khi chạy:

```bash
docker manifest inspect ghcr.io/hungpham10/opsense-serve:v1.0.10 \
  | jq -r '.manifests[].platform.os + "/" + .platform.architecture'   # phải ra cả amd64 & arm64
```

---

## 4. Tiền đề

- `kubectl` + quyền `cluster-admin` (hoặc namespace `opsense`).
- Ingress controller (khuyến nghị `ingress-nginx`) + `cert-manager`.
- Docker image ở GHCR **cần đăng nhập**: tạo PAT có scope `read:packages`
  (hoặc chuyển package sang public).
- (tuỳ chọn) `kustomize` để sinh ConfigMap từ `sql/`.

```bash
# Namespace
kubectl create namespace opsense

# Pull secret cho GHCR (thay <<PAT>>)
kubectl -n opsense create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=hungpham10 \
  --docker-password=<<PAT>>

# Xác minh image pull được trên cả amd64/arm64:
kubectl -n opsense run probe --image=ghcr.io/hungpham10/opsense-runner:v1.0.10 \
  --restart=Never --command -- /app/opsense runner --health-check; kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/probe
kubectl -n opsense delete pod probe
```

---

## 5. ConfigMap — cấu hình ứng dụng

### 5.1 `opsense.conf.toml` (Config `OPSENSE_CONFIG`)

`conf/opsense.conf.toml` trong repo là pipeline **minimal** (clock → null),
chỉ nên dùng để smoke-test. Với prod, tạo config riêng có pipeline thật +
`[storage]` backend phù hợp:

```toml
# docs/KUBERNETES.md — mẫu prod
[engine]
poll_interval_seconds = 60
cache_block_seconds = 300
cache_max_blocks = 288

# Lakehouse: duckdb (PVC) hoặc s3 (object store). data_dir mặc định
# ".opsense/lakehouse" — tương đối theo HOME=/app.
[storage]
backend = "duckdb"          # duckdb | lmdb | s3 | memory
data_dir = "/app/.opsense/lakehouse"
block_secs = 600
retention_secs = 0          # 0 = giữ mãi

[capacity]
# cpu_usage = 32.0
# mem_usage = 64.0

[pipeline]
[[pipeline.components]]
type = "clock_source"
id = "clock"
interval_secs = 60
# ... bổ sung ingest / http_source / rhai_transform / persist_sink /
#     timeseries_station_sink cho từng use-case prod
```

Tạo:

```bash
kubectl -n opsense create configmap opsense-config --from-file=opsense.conf.toml=conf/opsense.conf.toml
# hoặc với kustomize: configMapGenerator từ file prod
```

### 5.2 Dex config (OIDC)

Prod cần config Dex riêng — issuer là URL HTTPS public, redirectURI là API host:

```yaml
# dex-config-prod.yaml — issuer phải giống URL ngoài thực tế
issuer: https://<<DOMAIN>>/dex
storage: { type: kubernetes }        # hoặc postgres; memory = mất session khi restart
web: { http: 0.0.0.0:5556 }
staticClients:
  - id: opsense
    name: Opsense
    secret: <<OIDC_CLIENT_SECRET>>   # phải khớp sys_oidc trong sql seed
    redirectURIs: ["https://<<DOMAIN>>/callback", "https://<<DOMAIN>>/mcp/callback"]
    grantTypes: [authorization_code, refresh_token, "urn:ietf:params:oauth:grant-type:device_code"]
    responseTypes: [code]
staticPasswords:
  - email: <<ADMIN_EMAIL>>
    hash: <<BCRYPT_HASH_OF_PASSWORD>>
    username: <<ADMIN_USER>>
    userID: <<ADMIN_USER>>
```

> Nếu dùng `storage: type: memory` (như config.dev.yaml), đánh dấu **không dùng
> cho prod** — Dex sẽ mất clients/session khi pod restart.

### 5.3 Alloy (tuỳ chọn, observability)

Image serve đã nhúng Alloy (`/etc/alloy/config.alloy`, mẫu ở `conf/config.alloy`).
Nếu bật `USE_ALLOY=true`, app export OpenTelemetry tới
`OTEL_EXPORTER_OTLP_ENDPOINT` (mặc định `http://127.0.0.1:4317`) — cần Alloy
nhận OTLP và forward tới Grafana/Tempo/Mimir. Muốn scrape `:8080/metrics`
của từng pod qua Prometheus → tạo `ServiceMonitor`/annotations tương ứng.

---

## 6. Secrets

Dùng **Kubernetes Secret** (literals). Không nên đặt plaintext trong ConfigMap.

```bash
# MASTER_KEY: 32 bytes ASCII — PHẢI khớp với key đã dùng khi mã hoá sys_token_map
# trong sql seed (compose dev dùng "0123456789abcdef0123456789abcdef").
# Đổi key → phải mã hoá lại mọi token (xem crates/opsense/src/token.rs).
kubectl -n opsense create secret generic opsense-secrets \
  --from-literal=MASTER_KEY='0123456789abcdef0123456789abcdef' \
  --from-literal=RUST_LOG=info \
  --from-literal=ENVIRONMENT=prod \
  --from-literal=JWT_MODE=HS256 \
  --from-literal=JWT_SECRET='<<JWT_SECRET>>' \
  --from-literal=OIDC_ISSUER='https://<<DOMAIN>>/dex' \
  --from-literal=OIDC_CLIENT_ID=opsense \
  --from-literal=OIDC_CLIENT_SECRET='<<OIDC_CLIENT_SECRET>>' \
  --from-literal=OIDC_SESSION_SECRET='<<32_BYTES>>'

kubectl -n opsense create secret generic opsense-db \
  --from-literal=POSTGRES_USER=opsense \
  --from-literal=POSTGRES_PASSWORD='<<STRONG_PASSWORD>>' \
  --from-literal=POSTGRES_DATABASE=opsense \
  --from-literal=DB_DSN='postgres://opsense:<<STRONG_PASSWORD>>@opsense-postgres:5432/opsense'

kubectl -n opsense create secret generic opsense-redis \
  --from-literal=REDIS_HOST=opsense-valkey \
  --from-literal=REDIS_PORT=6379
```

### Secrets sops/age (apps secrets file)

`entrypoint.sh` tự decrypt file `ENCRYPTED_FILE` nếu có cả
`SOPS_AGE_KEY_CONTENT` và file đó (xem `scripts/release.sh`). Không bắt buộc —
nếu thiếu, serve dùng env mặc định. Để dùng:

```bash
kubectl -n opsense create secret generic opsense-sops \
  --from-literal=SOPS_AGE_KEY_CONTENT='<<AGE_PRIVATE_KEY>>' \
  --from-file=secrets.prod.enc.yaml=env/secrets.prod.enc.yaml   # key và tên file phải khớp ENCRYPTED_FILE (§9)
```

---

## 7. Phụ thuộc

### 7.1 PostgreSQL 16

Khuyến nghị **managed** (RDS/CloudSQL/Neon) rồi trỏ `DB_DSN`/`POSTGRES_*`.
Nếu in-cluster — StatefulSet đơn giản (production thật nên dùng
`cloudnative-pg` hoặc helm `bitnami/postgresql`; mẫu dưới chỉ để test):

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: opsense-postgres, namespace: opsense }
spec:
  serviceName: opsense-postgres
  replicas: 1
  selector: { matchLabels: { app: opsense-postgres } }
  template:
    metadata: { labels: { app: opsense-postgres } }
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          ports: [{ containerPort: 5432 }]
          env:
            - { name: POSTGRES_DB, valueFrom: { secretKeyRef: { name: opsense-db, key: POSTGRES_DATABASE } } }
            - { name: POSTGRES_USER, valueFrom: { secretKeyRef: { name: opsense-db, key: POSTGRES_USER } } }
            - { name: POSTGRES_PASSWORD, valueFrom: { secretKeyRef: { name: opsense-db, key: POSTGRES_PASSWORD } } }
          volumeMounts: [{ name: data, mountPath: /var/lib/postgresql/data }]
          readinessProbe:
            exec: { command: ["pg_isready", "-U", "opsense", "-d", "opsense"] }
      volumes:
        - name: data
          persistentVolumeClaim: { claimName: opsense-postgres-pvc }
---
apiVersion: v1
kind: Service
metadata: { name: opsense-postgres, namespace: opsense }
spec:
  selector: { app: opsense-postgres }
  ports: [{ port: 5432, targetPort: 5432 }]
```

### 7.2 Init DB — schema + seed (chạy ĐÚNG MỘT LẦN)

Layout SQL (xem `scripts/release.sh prepare`):
`sql/postgres/*.sql` (schema, depth 1) → `sql/postgres/prod/*.sql` (seed, depth 2).

Khuyến nghị: **Job init một-shot**, đừng để serve auto-init (tránh race). Trong
serve deployment đặt `DISABLE_AUTO_INIT_DATABASE=true`.

Dùng `kustomize` sinh ConfigMap từ cây `sql/`, hoặc mount repo vào Job:

```yaml
# init-db.yaml — psql chạy lần lượt schema rồi seed prod
apiVersion: batch/v1
kind: Job
metadata: { name: opsense-init-db, namespace: opsense }
spec:
  backoffLimit: 3
  ttlSecondsAfterFinished: 86400
  template:
    spec:
      restartPolicy: Never
      initContainers:
        - name: fetch-sql
          image: alpine/git:latest
          command: ["sh", "-c", "git clone --depth=1 -b v1.0.10 https://github.com/hungpham10/opsense-rs /sqlrepo"]
          volumeMounts: [{ name: sqlrepo, mountPath: /sqlrepo }]
      containers:
        - name: psql
          image: postgres:16-alpine
          command: ["sh", "-c"]
          args:
            - |
              set -e
              export PGPASSWORD="$(cat /etc/pg/POSTGRES_PASSWORD)"
              U="-h opsense-postgres -U $(cat /etc/pg/POSTGRES_USER) -d $(cat /etc/pg/POSTGRES_DATABASE)"
              echo "== depth1 schema =="
              for f in /sqlrepo/sql/postgres/*.sql; do psql $U -v ON_ERROR_STOP=1 -f "$f"; done
              echo "== depth2 seed prod =="
              for f in /sqlrepo/sql/postgres/prod/*.sql; do psql $U -v ON_ERROR_STOP=1 -f "$f"; done
          envFrom:
            - secretRef: { name: opsense-db }
          volumeMounts:
            - { name: sqlrepo, mountPath: /sqlrepo }
            - { name: pgsecret, mountPath: /etc/pg, readOnly: true }
      volumes:
        - name: sqlrepo
          emptyDir: {}
        - name: pgsecret
          secret: { secretName: opsense-db }
---
# nếu dùng StatefulSet in-cluster, đợi nó Ready trước khi apply Job
```

> Lưu ý migration: lần nâng phiên bản sau, tạo Job migration kế tiếp chứa
> **diff SQL**, không re-run toàn bộ seed (idempotency của
> `50-init-tenant.sql` dựa vào `setval`/`ON CONFLICT` — kiểm tra từng script).

### 7.3 Valkey (Redis)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: opsense-valkey, namespace: opsense }
spec:
  replicas: 1
  selector: { matchLabels: { app: opsense-valkey } }
  template:
    metadata: { labels: { app: opsense-valkey } }
    spec:
      containers:
        - name: valkey
          image: valkey/valkey:9-alpine
          args: ["valkey-server", "--appendonly", "yes"]
          ports: [{ containerPort: 6379 }]
          volumeMounts: [{ name: data, mountPath: /data }]
          readinessProbe:
            exec: { command: ["valkey-cli", "ping"] }
      volumes:
        - name: data
          persistentVolumeClaim: { claimName: opsense-valkey-pvc }
---
apiVersion: v1
kind: Service
metadata: { name: opsense-valkey, namespace: opsense }
spec:
  selector: { app: opsense-valkey }
  ports: [{ port: 6379, targetPort: 6379 }]
```

### 7.4 Dex (OIDC)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: opsense-dex, namespace: opsense }
spec:
  replicas: 2
  selector: { matchLabels: { app: opsense-dex } }
  template:
    metadata: { labels: { app: opsense-dex } }
    spec:
      containers:
        - name: dex
          image: ghcr.io/dexidp/dex:v2.41.1
          args: ["dex", "serve", "/etc/dex/config.yaml"]
          ports: [{ containerPort: 5556 }]
          volumeMounts:
            - { name: config, mountPath: /etc/dex/config.yaml, subPath: config.yaml, readOnly: true }
          readinessProbe:
            exec:
              command: ["wget", "-qO-", "http://127.0.0.1:5556/dex/.well-known/openid-configuration"]
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { cpu: 500m, memory: 512Mi }
      volumes:
        - name: config
          configMap: { name: dex-config }
---
apiVersion: v1
kind: Service
metadata: { name: opsense-dex, namespace: opsense }
spec:
  selector: { app: opsense-dex }
  ports: [{ port: 5556, targetPort: 5556 }]
```

Ingress cho Dex (path `/dex` → `opsense-dex:5556`) — xem §9.

---

## 8. Runners (3 Deployment)

Mẫu chung; lặp 3 lần với tên/image khác nhau.

```yaml
# opsense-runner.yaml (echo) — python/julia chỉ đổi tên + image
apiVersion: apps/v1
kind: Deployment
metadata: { name: opsense-runner, namespace: opsense }
spec:
  replicas: 2
  selector: { matchLabels: { app: opsense-runner } }
  template:
    metadata: { labels: { app: opsense-runner } }
    spec:
      imagePullSecrets: [{ name: ghcr-pull }]
      containers:
        - name: runner
          image: ghcr.io/hungpham10/opsense-runner:v1.0.10
          ports: [{ containerPort: 50051, protocol: TCP }]
          # ENV OPSENSE_RUNNER_BIND / OPSENSE_KERNEL đã có sẵn trong image
          readinessProbe:
            exec: { command: ["/app/opsense", "runner", "--health-check"] }
            periodSeconds: 10
            failureThreshold: 6
          livenessProbe:
            exec: { command: ["/app/opsense", "runner", "--health-check"] }
            periodSeconds: 30
          resources:
            requests: { cpu: 500m, memory: 512Mi }
            limits: { cpu: "2", memory: 2Gi }
---
apiVersion: v1
kind: Service
metadata: { name: opsense-runner, namespace: opsense }
spec:
  selector: { app: opsense-runner }
  ports: [{ port: 50051, targetPort: 50051 }]
---
# … lặp lại cho opsense-runner-python & opsense-runner-julia
# Service tương ứng: opsense-runner-python:50051, opsense-runner-julia:50051
```

- **Tên Service `opsense-runner` bắt buộc** (supervisor trong image hardcode
  `OPSENSE_RUNNER_GRPC=opsense-runner:50051` cho process serve).
- Python/Julia runner tiêu tốn RAM nhiều hơn (Python stack / Julia runtime) —
  chỉnh resources khi benchmark.
- Muốn adhoc đổi runner mà serve kết nối tới → override bằng cách mount
  ConfigMap chứa `opsense.conf` (supervisor) hoặc chỉnh `command` (không khuyến
  nghị).

---

## 9. opsense-serve

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: opsense-lakehouse-pvc, namespace: opsense }
spec:
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 100Gi } }
  # storageClassName: <<gp3|standard|longhorn>> — lưu trữ block
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opsense-serve
  namespace: opsense
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  selector: { matchLabels: { app: opsense-serve } }
  template:
    metadata:
      labels: { app: opsense-serve }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: /metrics
    spec:
      imagePullSecrets: [{ name: ghcr-pull }]
      nodeSelector:                      # chỉ cần nếu cluster hỗn hợp arch + tag chưa được rebuild multi-arch
        kubernetes.io/arch: arm64
      containers:
        - name: serve
          image: ghcr.io/hungpham10/opsense-serve:v1.0.10
          ports: [{ containerPort: 8080, protocol: TCP }]
          env:
            # App
            - { name: APP_ENV, value: prod }
            - { name: OPSENSE_CONFIG, value: /app/opsense.conf.toml }
            - { name: DISABLE_AUTO_INIT_DATABASE, value: "true" }
            - { name: ENCRYPTED_FILE, value: /app/secrets/secrets.prod.enc.yaml }
            - { name: MASTER_KEY, valueFrom: { secretKeyRef: { name: opsense-secrets, key: MASTER_KEY } } }
            - { name: RUST_LOG, valueFrom: { secretKeyRef: { name: opsense-secrets, key: RUST_LOG } } }
            - { name: ENVIRONMENT, valueFrom: { secretKeyRef: { name: opsense-secrets, key: ENVIRONMENT } } }
            - { name: DB_DSN, valueFrom: { secretKeyRef: { name: opsense-db, key: DB_DSN } } }
            - { name: POSTGRES_HOST, value: opsense-postgres }
            - { name: POSTGRES_PORT, value: "5432" }
            - { name: POSTGRES_USER, valueFrom: { secretKeyRef: { name: opsense-db, key: POSTGRES_USER } } }
            - { name: POSTGRES_PASSWORD, valueFrom: { secretKeyRef: { name: opsense-db, key: POSTGRES_PASSWORD } } }
            - { name: POSTGRES_DATABASE, valueFrom: { secretKeyRef: { name: opsense-db, key: POSTGRES_DATABASE } } }
            # Runner + cache
            - { name: OPSENSE_RUNNER_GRPC, value: opsense-runner:50051 }
            - { name: REDIS_HOST, valueFrom: { secretKeyRef: { name: opsense-redis, key: REDIS_HOST } } }
            - { name: REDIS_PORT, valueFrom: { secretKeyRef: { name: opsense-redis, key: REDIS_PORT } } }
            # Reverse proxy / OIDC (nginx trong image)
            - { name: HTTP_PROTOCOL, value: http }
            - { name: HTTP_SERVER, value: localhost }
            - { name: NGINX_DIR, value: /usr/local/openresty/nginx/conf }
            - { name: NGINX_LOG, value: main }
            - { name: USE_TOR, value: "false" }
            - { name: USE_ALLOY, value: "false" }
            # OIDC nginx (đọc qua `env` directive của nginx trong image)
            - { name: OIDC_ISSUER, valueFrom: { secretKeyRef: { name: opsense-secrets, key: OIDC_ISSUER } } }
            - { name: OIDC_CLIENT_ID, valueFrom: { secretKeyRef: { name: opsense-secrets, key: OIDC_CLIENT_ID } } }
            - { name: OIDC_CLIENT_SECRET, valueFrom: { secretKeyRef: { name: opsense-secrets, key: OIDC_CLIENT_SECRET } } }
            - { name: OIDC_SESSION_SECRET, valueFrom: { secretKeyRef: { name: opsense-secrets, key: OIDC_SESSION_SECRET } } }
            - { name: JWT_MODE, valueFrom: { secretKeyRef: { name: opsense-secrets, key: JWT_MODE } } }
            - { name: JWT_SECRET, valueFrom: { secretKeyRef: { name: opsense-secrets, key: JWT_SECRET } } }
          volumeMounts:
            - { name: config, mountPath: /app/opsense.conf.toml, subPath: opsense.conf.toml, readOnly: true }
            - { name: lakehouse, mountPath: /app/.opsense }
            - { name: secrets, mountPath: /app/secrets, readOnly: true }   # (tuỳ chọn) file sops
          readinessProbe:
            httpGet: { path: /health, port: 8080 }
            periodSeconds: 10
            failureThreshold: 12
          livenessProbe:
            httpGet: { path: /health, port: 8080 }
            periodSeconds: 30
          resources:
            requests: { cpu: 500m, memory: 1Gi }
            limits: { cpu: "2", memory: 4Gi }
      volumes:
        - name: config
          configMap: { name: opsense-config }
        - name: lakehouse
          persistentVolumeClaim: { claimName: opsense-lakehouse-pvc }
        - name: secrets
          secret:
            secretName: opsense-sops     # (tuỳ chọn) chỉ khi dùng sops; optional=true → thiếu secret vẫn chạy
---
apiVersion: v1
kind: Service
metadata: { name: opsense-serve, namespace: opsense }
spec:
  selector: { app: opsense-serve }
  ports: [{ port: 8080, targetPort: 8080 }]
---
# Ingress + TLS
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: opsense
  namespace: opsense
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/proxy-read-timeout: "600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "600"
spec:
  tls:
    - hosts: [<<DOMAIN>>]
      secretName: opsense-tls
  rules:
    - host: <<DOMAIN>>
      http:
        paths:
          - path: /mcp
            pathType: Prefix
            backend: { service: { name: opsense-serve, port: { number: 8080 } } }
          - path: /api
            pathType: Prefix
            backend: { service: { name: opsense-serve, port: { number: 8080 } } }
          - path: /health
            pathType: Exact
            backend: { service: { name: opsense-serve, port: { number: 8080 } } }
          - path: /metrics
            pathType: Exact
            backend: { service: { name: opsense-serve, port: { number: 8080 } } }
          - path: /dex
            pathType: Prefix
            backend: { service: { name: opsense-dex, port: { number: 5556 } } }
---
# PDB — đảm bảo ít nhất 1 replica khi bảo trì node
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: opsense-serve, namespace: opsense }
spec:
  minAvailable: 1
  selector: { matchLabels: { app: opsense-serve } }
```

> ghi chú: `OPSENSE_RUNNER_GRPC` trong Deployment sẽ bị supervisor trong image
> **ghi đè** bằng `opsense-runner:50051` — vì vậy giữ Service tên `opsense-runner`.
> Muốn nhiều đội (multi-tenant) trỏ serve tới runner khác → phải mount
> ConfigMap override `/etc/supervisor/conf.d/opsense.conf`, đây là thay đổi cần
> làm ở wave sau.

---

## 10. Trình tự deploy

```bash
# 0. (một lần) namespace + pull secret
kubectl create namespace opsense
kubectl -n opsense create secret docker-registry ghcr-pull ...

# 1. ConfigMap + Secrets
kubectl -n opsense create configmap opsense-config --from-file=conf/opsense.conf.toml
kubectl -n opsense create configmap dex-config --from-file=config.yaml=docs/dex-config-prod.yaml   # theo §5.2
kubectl -n opsense apply -f manifests/secrets.yaml       # theo §6

# 2. Dependencies trước
kubectl -n opsense apply -f manifests/postgres.yaml       # §7.1
kubectl -n opsense wait --for=jsonpath='{.status.phase}'=Running pod -l app=opsense-postgres --timeout=300s
kubectl -n opsense apply -f manifests/init-db.yaml        # §7.2  (đợi Job Succeeded)
kubectl -n opsense apply -f manifests/valkey.yaml          # §7.3
kubectl -n opsense apply -f manifests/dex.yaml             # §7.4

# 3. Runners
kubectl -n opsense apply -f manifests/runner.yaml
kubectl -n opsense apply -f manifests/runner-python.yaml
kubectl -n opsense apply -f manifests/runner-julia.yaml

# 4. Serve
kubectl -n opsense apply -f manifests/serve.yaml          # §9 (Deployment/Service/PVC/Ingress/PDB)

# 5. Theo dõi
kubectl -n opsense get pods -w
kubectl -n opsense logs -f deploy/opsense-serve --all-containers
```

### Smoke test go-live

```bash
# HTTP OK từ ngoài (sau khi DNS/TLS trỏ về Ingress)
curl -fsS https://<<DOMAIN>>/health

# Station registry trong pod đang chạy (metrics của app qua nginx)
curl -fsS http://127.0.0.1:8080/health   # trong pod

# MCP loop (opsense mcp qua stdio trong pod hoặc /mcp HTTP)
curl -X POST https://<<DOMAIN>>/mcp -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"opsense_init","params":{}}'

# Lakehouse có dữ liệu (sau ≥1 tick pipeline + persist)
kubectl -n opsense exec deploy/opsense-serve -- ls -R /app/.opsense/lakehouse | head

# Runner health
kubectl -n opsense get pods -l app=opsense-runner -o wide
```

---

## 11. Observability

| Nguồn | Endpoint | Chú thích |
|---|---|---|
| App metrics | `GET /metrics` (axum PrometheusMetricLayer) | scrape qua Prometheus/ServiceMonitor, path `/metrics` |
| App health | `GET /health` | probe + uptime |
| gRPC runner | health check riêng (exec) | preflight trước khi serve gọi |
| Logs | stdout (`supervisord` → `/dev/stdout`) | `RUST_LOG=info`; nginx log mức `NGINX_LOG` |
| Otel (Alloy) | `OTEL_EXPORTER_OTLP_ENDPOINT` (`http://127.0.0.1:4317` default) | bật `USE_ALLOY=true` + Alloy collector |

Cảnh báo cần thiết (PrometheusRule / Grafana):
- `opsense-serve` Ready < 2, PDB `minAvailable` vi phạm.
- Runner down > 2 phút (health check).
- `DB_DSN` kết nối fail (serve log error, `sys_token_map` không resolve được).
- Disk lakehouse PVC đầy (PVC usage > 80%).
- SSL expiry (cert-manager `Certificate` `Ready=false`).

---

## 12. Upgrade / Rollback

**Upgrade** (v1.0.10 → v1.0.11):
1. Bump `Cargo.toml` + `version.txt` + `dist-workspace.toml` (nếu có), tạo tag
   `vX.Y.Z` → workflow `push-image` phát hành image.
2. **Verify multi-arch** (xem §3) — tag `v1.0.10` phải có cả `amd64` & `arm64`
   trước khi trỏ deployment (không còn bị ghi đè tag).
3. Migration DB: tạo Job migration chỉ chạy **diff SQL**; duyệt seed mới.
4. Cập nhật image tag trong manifest serve + runner, apply.
5. Rollout: `kubectl -n opsense rollout status deploy/opsense-serve --timeout=600s`.

**Rollback**:
```bash
kubectl -n opsense rollout undo deploy/opsense-serve     # về revision trước
kubectl -n opsense rollout undo deploy/opsense-runner
kubectl -n opsense exec deploy/opsense-serve -- /app/opsense runner --health-check || true
```
Chú ý: lakehouse/DB **không tự rollback schema** → giữ migration theo hướng
forward-only; dự phòng bằng backup trước nâng.

**Backup trước go-live**:
- `pg_dump` database `opsense` (chứa `sys_token_map`, `sys_oidc`, sessions).
- Snapshot PVC lakehouse hoặc xoá vòng đời object store.
- Lưu `MASTER_KEY` + sops age key ở nơi an toàn (vault/secret manager) —
  **mất MASTER_KEY = toàn bộ token đã mã hoá trong DB không decrypt được.**

---

## 13. Checklist go-live

- [ ] Image `v1.0.10` xác minh đa nền tảng (hoặc cluster chỉ có arm64).
- [ ] Pull secret `ghcr-pull` sẵn sàng; test pull trên node thật.
- [ ] `MASTER_KEY` khớp key mã hoá của seed; age keys lưu vault.
- [ ] Dex issuer = URL HTTPS thật; client/secret khớp `sys_oidc`.
- [ ] Job init DB chạy Succeeded; seed `prod` đúng (tenant/token).
- [ ] `DISABLE_AUTO_INIT_DATABASE=true` trên Deployment serve.
- [ ] PVC lakehouse mount `/app/.opsense` lưu trữ được bật retention.
- [ ] Ingress + cert-manager `Certificate` `Ready=true`; path `/dex`,`/mcp`,`/api`,`/health`,`/metrics` đúng.
- [ ] Smoke test (mục 10, "Smoke test go-live") pass trên môi trường UAT trước prod.
- [ ] PDB + resource limits + HPA (tuỳ chọn) đã apply; liveness/readiness ok.
- [ ] Backup DB + lakehouse + backup script cho migration.

---

## 14. Known gaps cần xử lý trong sprint tới

1. **Multi-arch tag** — **đã sửa** trong `image.yml` + `Earthfile`: target
   `+multi` build 1 lần (`BUILD --platform linux/amd64 --platform linux/arm64
   +all`), static binaries theo `binaries/<arch>/` và chọn theo `uname -m` →
   Earthly push 1 tag duy nhất là manifest list (xem §3). Còn lại: chạy lại
   workflow `push-image` cho tag `v1.0.10`.
2. **Supervisor hardcode `OPSENSE_RUNNER_GRPC=opsense-runner:50051`** — không
   cho phép serve chọn runner theo env. Fix: config supervisor qua ConfigMap
   (mẫu `conf/supervisor/opsense.conf`) hoặc `serve` đọc env thay vì ép cứng.
3. **Auto-init DB trong image** (`prepare`) — nguy hiểm khi replica > 1; đã né
   bằng Job init + flag `DISABLE_AUTO_INIT_DATABASE`. Nên tách init khỏi image
   entrypoint ở wave sau.
4. **Dex `storage: memory`** ở config.dev — làm config prod dùng
   `storage: kubernetes` hoặc PostgreSQL để clients/sessions bền.
5. **`conf/dex/config.dev.yaml` bị bake vào image serve** (`/etc/dex/config.dev.yaml`)
   — ở k8s, Dex chạy deployment riêng nên chỗ này thừa; xoá khỏi image để tránh
   nhầm lẫn.
6. **REPL/export CLI, join multi-series** (xem `docs/CHECKLIST.MD` Part B) —
   ngoài phạm vi v1.0.10 go-live.