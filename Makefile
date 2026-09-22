SHELL := /bin/bash
APP_ENV ?= dev
VERSION ?= local
OPSENSE_TAG ?= $(VERSION)
REGISTRY ?= ghcr.io
IMAGE_PREFIX ?= lap02921/opsense
TARGET ?= x86_64-unknown-linux-gnu
PLATFORM := $(shell echo $(TARGET) | sed 's/x86_64-/linux\/amd64/; s/aarch64-/linux\/arm64/')
COMPOSE := docker compose
EARTHLY := earthly

# VERSION is the image tag shared by all four Opsense images.
IMG_SERVE      := $(REGISTRY)/$(IMAGE_PREFIX)-serve:$(VERSION)
IMG_RUNNER     := $(REGISTRY)/$(IMAGE_PREFIX)-runner:$(VERSION)
IMG_RUNNER_PY  := $(REGISTRY)/$(IMAGE_PREFIX)-runner-python:$(VERSION)
IMG_RUNNER_JL  := $(REGISTRY)/$(IMAGE_PREFIX)-runner-julia:$(VERSION)

.PHONY: help build-local release build-cloud build-multiarch up up-cloud down down-v logs ps restart shell encrypt decrypt sql-clean test-integration test-integration-down

help:
	@echo "Opsense dev shortcuts:"
	@echo "  make build-local           - Build all 4 images locally with tag VERSION (default: local)."
	@echo "  make release               - Build & push all 4 images with VERSION as their tag (host platform)."
	@echo "  make build-cloud           - Alias for release."
	@echo "  make build-multiarch       - Build & push all 4 images for multiple platforms (TARGET required)."
	@echo "  make up                    - docker compose up -d using local image tag VERSION."
	@echo "  make up-cloud              - docker compose up -d using released registry images."
	@echo "  make down                  - docker compose down (KHÔNG xoá volume; dùng 'down-v' để xoá)"
	@echo "  make down-v                - docker compose down -v (DESTRUCTIVE: xoá volume)"
	@echo "  make ps                    - docker compose ps"
	@echo "  make logs                  - docker compose logs -f --tail=100"
	@echo "  make restart SVC=x         - restart một service (vd: make restart SVC=opsense-serve)"
	@echo "  make shell SVC=x           - bash vào service (vd: make shell SVC=opsense-serve)"
	@echo "  make encrypt F=env/secrets.dev.yaml      - sops -e một secrets file"
	@echo "  make decrypt F=env/secrets.dev.enc.yaml  - sops -d một secrets file"
	@echo "  make sql-clean             - xoá *.sql files sinh ra sau init"
	@echo "  make test-integration      - Build + compose up + run full integration suite (Nginx + UDS + Dex)"
	@echo "  make test-integration-down - Cleanup compose (down -v)"
	@echo ""
	@echo "Biến override:"
	@echo "  VERSION=v1.0.0 make build-local          (build local tags)"
	@echo "  VERSION=v1.0.0 make release              (build + push registry tags)"
	@echo "  VERSION=v1.0.0 make build-multiarch TARGET=aarch64-unknown-linux-musl  (multi-arch push)"
	@echo "  VERSION=v1.0.0 make up-cloud             (run released registry images)"
	@echo "  APP_ENV=uat make up"

# Build all 4 local images. Compose uses the same image names and VERSION tag.
build-local:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required." && exit 1)
	$(EARTHLY) --build-arg VERSION="$(VERSION)" +integration-images
	@echo "Built local tags: opsense-serve:$(VERSION), opsense-runner:$(VERSION), opsense-runner-python:$(VERSION), opsense-runner-julia:$(VERSION)"

# Build & push all 4 registry images for host platform.
release:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required (vd: VERSION=v1.0.0)." && exit 1)
	@if [ "$(VERSION)" = "local" ]; then \
		echo "ERROR: VERSION=local không được push lên registry."; exit 1; \
	fi
	$(EARTHLY) --push \
		--build-arg VERSION="$(VERSION)" \
		--build-arg REGISTRY="$(REGISTRY)" \
		--build-arg IMAGE_PREFIX="$(IMAGE_PREFIX)" \
		+all
	@echo "Pushed $(IMG_SERVE), $(IMG_RUNNER), $(IMG_RUNNER_PY), $(IMG_RUNNER_JL)"

PLATFORM := $(shell echo $(TARGET) | sed 's/x86_64-/linux\/amd64/; s/aarch64-/linux\/arm64/')

# Build & push all 4 registry images for multiple platforms.
# Targets: aarch64-unknown-linux-musl, x86_64-unknown-linux-musl, etc.
build-multiarch:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required (vd: VERSION=v1.0.0)." && exit 1)
	@if [ "$(VERSION)" = "local" ]; then \
		echo "ERROR: VERSION=local không được push lên registry."; exit 1; \
	fi
	@if [ "$(TARGET)" = "x86_64-unknown-linux-gnu" ]; then \
		echo "WARNING: Default TARGET used. Specify TARGET for cross-compilation."; \
	fi
	$(EARTHLY) --push \
		--platform $(PLATFORM) \
		--build-arg VERSION="$(VERSION)" \
		--build-arg REGISTRY="$(REGISTRY)" \
		--build-arg IMAGE_PREFIX="$(IMAGE_PREFIX)" \
		--build-arg TARGET="$(TARGET)" \
		+all
	@echo "Pushed $(IMG_SERVE), $(IMG_RUNNER), $(IMG_RUNNER_PY), $(IMG_RUNNER_JL) for $(TARGET)"

build-cloud: release

up:
	APP_ENV=$(APP_ENV) OPSENSE_TAG=$(OPSENSE_TAG) $(COMPOSE) up -d

up-cloud:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required (vd: VERSION=v1.0.0)." && exit 1)
	@if [ "$(VERSION)" = "local" ]; then \
		echo "ERROR: VERSION=local không được dùng cho up-cloud."; exit 1; \
	fi
	APP_ENV=$(APP_ENV) OPSENSE_TAG=$(VERSION) \
		OPSENSE_SERVE_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-serve \
		OPSENSE_RUNNER_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-runner \
		OPSENSE_RUNNER_PY_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-runner-python \
		OPSENSE_RUNNER_JL_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-runner-julia \
		$(COMPOSE) pull
	APP_ENV=$(APP_ENV) OPSENSE_TAG=$(VERSION) \
		OPSENSE_SERVE_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-serve \
		OPSENSE_RUNNER_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-runner \
		OPSENSE_RUNNER_PY_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-runner-python \
		OPSENSE_RUNNER_JL_IMAGE=$(REGISTRY)/$(IMAGE_PREFIX)-runner-julia \
		$(COMPOSE) up -d

down:
	$(COMPOSE) down

down-v:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

restart:
	@test -n "$(SVC)" || (echo "Usage: make restart SVC=opsense-serve" && exit 1)
	$(COMPOSE) restart $(SVC)

shell:
	@test -n "$(SVC)" || (echo "Usage: make shell SVC=opsense-serve" && exit 1)
	$(COMPOSE) exec $(SVC) bash

encrypt:
	@test -n "$(F)" || (echo "Usage: make encrypt F=env/secrets.dev.yaml" && exit 1)
	sops --age $$(cat ~/.config/sops/age/keys.txt.pub 2>/dev/null | head -1) -e $(F) > $(F:.yaml=.enc.yaml)

decrypt:
	@test -n "$(F)" || (echo "Usage: make decrypt F=env/secrets.dev.enc.yaml" && exit 1)
	sops -d $(F)

sql-clean:
	find ./sql -name '*.sql' -newer ./Makefile -print -delete

# Integration tests use local image names so they never depend on a registry.
test-integration:
	@echo ">>> Building 4 images (serve + runner + python + julia) via Earthly"
	$(EARTHLY) --build-arg VERSION="$(OPSENSE_TAG)" +integration-images
	@echo ">>> Compose up + wait for healthy"
	APP_ENV=$(APP_ENV) OPSENSE_TAG=$(OPSENSE_TAG) $(COMPOSE) up -d --wait --wait-timeout 180
	@echo ">>> Run integration test suite (Nginx + UDS + Dex + Axum)"
	APP_ENV=$(APP_ENV) OPSENSE_SERVE_URL=http://127.0.0.1:8080 \
	OPSENSE_DEX_ISSUER=http://127.0.0.1:5556/dex \
	OPSENSE_RUNNER_ECHO=127.0.0.1:50051 \
	OPSENSE_RUNNER_PYTHON=127.0.0.1:50052 \
	OPSENSE_RUNNER_JULIA=127.0.0.1:50053 \
	DB_DSN=postgres://opsense:opsense123@127.0.0.1:5432/opsense \
	cargo test --workspace --test integration_health --test integration_oauth \
		--test integration_runner_grpc --test integration_repl_pty \
		-- --test-threads=1

# Cleanup after integration test.
test-integration-down:
	$(COMPOSE) down -v

# Storage backend end-to-end tests — 5 cases.
#
# Case 1 (local):    parquet local, data từ local-metrics server.
# Case 2 (memory):   storage = "memory" — thuần RAM, không persistence.
# Case 3 (sqlite):   storage = "sqlite" — persistence qua 1 file .sqlite.
# Case 4 (s3):       parquet + mirror MinIO S3.
# Case 5 (prom):     full Prometheus (real) → metrics-adapter → S3.
#
# Chạy:
#   make test-storage-{local,memory,sqlite,s3,prometheus}-up
#   make test-storage-wait
#   make test-storage-validate
#   make test-storage-down
#   make test-storage-run-all     # chạy tuần tự cả 5 case
test-storage-local-up:
	@echo ">>> Case LOCAL: parquet local"
	cp tests/storages/configs/local.toml conf/opsense.conf.toml
	APP_ENV=dev OPSENSE_TAG=local $(COMPOSE) -f docker-compose.yml -f tests/storages/docker-compose.local.yml --profile local up -d --build local-metrics opsense-serve opsense-runner opsense-runner-python

test-storage-memory-up:
	@echo ">>> Case MEMORY: storage = memory"
	cp tests/storages/configs/memory.toml conf/opsense.conf.toml
	APP_ENV=dev OPSENSE_TAG=local $(COMPOSE) -f docker-compose.yml -f tests/storages/docker-compose.local.yml --profile local up -d --build local-metrics opsense-serve opsense-runner opsense-runner-python

test-storage-sqlite-up:
	@echo ">>> Case SQLITE: storage = sqlite"
	cp tests/storages/configs/sqlite.toml conf/opsense.conf.toml
	APP_ENV=dev OPSENSE_TAG=local $(COMPOSE) -f docker-compose.yml -f tests/storages/docker-compose.local.yml --profile local up -d --build local-metrics opsense-serve opsense-runner opsense-runner-python

test-storage-s3-up:
	@echo ">>> Case S3-MINIO: parquet + S3"
	cp tests/storages/configs/s3.toml conf/opsense.conf.toml
	APP_ENV=dev OPSENSE_TAG=local $(COMPOSE) -f docker-compose.yml -f tests/storages/docker-compose.s3.yml --profile s3 up -d --build metrics-adapter minio minio-bucket opsense-serve opsense-runner opsense-runner-python

test-storage-prometheus-up:
	@echo ">>> Case PROMETHEUS: full real Prometheus → S3"
	cp tests/storages/configs/prometheus.toml conf/opsense.conf.toml
	APP_ENV=dev OPSENSE_TAG=local $(COMPOSE) -f docker-compose.yml -f tests/storages/docker-compose.s3.yml --profile s3 --profile prometheus up -d --build metrics-adapter minio minio-bucket opsense-serve opsense-runner opsense-runner-python

test-storage-wait:
	@echo ">>> Đợi stack healthy..."
	APP_ENV=dev $(COMPOSE) exec opsense-serve sh -c 'for i in 1 2 3 4 5 6 7 8 9 10; do curl -fsS http://127.0.0.1:8080/health && break; sleep 3; done'
	@echo ">>> Đợi Prometheus scrape (nếu có)..."
	@for i in $$(seq 1 20); do curl -fsS http://localhost:9090/api/v1/query?query=up 2>/dev/null | grep -q '"status":"success"' && break; sleep 2; done || true

test-storage-validate:
	@echo ">>> Validate storage case"
	CONFIG_BACKEND=$$(grep '^backend' conf/opsense.conf.toml | head -1 | sed 's/.*backend = "\([^"]*\)".*/\1/')
	@echo "backend=$$CONFIG_BACKEND"
	@case "$$CONFIG_BACKEND" in \
	  memory) \
	    echo "memory: verify opsense-serve healthy, không tạo file storage" ; \
	    docker compose exec opsense-serve sh -c 'ls -la /app/.opsense/ 2>/dev/null || echo "no .opsense dir (OK - memory only)"' ; \
	    ;; \
	  sqlite) \
	    echo "sqlite: verify .sqlite file exists" ; \
	    docker compose exec opsense-serve sh -c 'find /app/.opsense -name "*.sqlite" -o -name "*.db" 2>/dev/null | head -5' ; \
	    ;; \
	  parquet|*) \
	    echo "parquet: verify ts/blk=*/*.parquet" ; \
	    docker compose exec opsense-serve sh -c 'find /app/.opsense -path "*/ts/blk=*/*.parquet" 2>/dev/null | head -5' ; \
	    which duckdb >/dev/null 2>&1 && { \
	      tmp=$$(mktemp -d); \
	      docker compose exec minio-bucket mc cp --recursive myminio/opsense-lake/$$(grep prefix conf/opsense.conf.toml | head -1 | sed 's/.*prefix = "\([^"]*\)"/\1/')/station-0-timeseries/ts/ "$${tmp}/" 2>/dev/null || true; \
	      find "$${tmp}" -name '*.parquet' | head -5; \
	      duckdb -c "SELECT decode(series) AS block_id, ts, convert_from(value, 'utf8') AS value FROM read_parquet('$${tmp}/**/*.parquet', union_by_name = true, hive_partitioning = true) LIMIT 5;" || true; \
	      rm -rf "$${tmp}"; \
	    } || echo "skip: duckdb không cài" ; \
	    ;; \
	esac

test-storage-down:
	$(COMPOSE) down -v
	@echo ">>> Restore original config"
	git checkout -- conf/opsense.conf.toml 2>/dev/null || true

# Chạy tuần tự tất cả cases.
test-storage-run-all:
	@for case in local memory sqlite s3 prometheus; do \
	  echo "=========================================="; \
	  echo "CASE: $$case"; \
	  echo "=========================================="; \
	  make "test-storage-$${case}-up"; \
	  make test-storage-wait; \
	  make test-storage-validate; \
	  make test-storage-down; \
	done
	@echo ">>> Tất cả storage cases hoàn thành."
	@echo ">>> Restore original config"
	git checkout -- conf/opsense.conf.toml 2>/dev/null || true
