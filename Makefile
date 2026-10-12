SHELL := /bin/bash
APP_ENV ?= dev
VERSION ?= local
OPSENSE_TAG ?= $(VERSION)
REGISTRY ?= ghcr.io
# Image thật nằm dưới tài khoản `hungpham10` (khớp `docs/KUBERNETES.md` §3).
# Trước đây là `lap02921/opsense` ⇒ `make up-cloud` pull trúng `denied` vì
# package đó không tồn tại.
IMAGE_PREFIX ?= hungpham10/opsense
TARGET ?= x86_64-unknown-linux-gnu
PLATFORM := $(shell echo $(TARGET) | sed 's/x86_64-/linux\/amd64/; s/aarch64-/linux\/arm64/')
COMPOSE := docker compose
EARTHLY := earthly
# Age private key dùng cho SOPS (`make encrypt`/`decrypt`). Override được:
#   SOPS_AGE_KEY=/path/to/keys.txt make encrypt F=env/render.env
SOPS_AGE_KEY ?= $(HOME)/.config/sops/age/keys.txt

# VERSION is the image tag shared by all four Opsense images.
IMG_SERVE      := $(REGISTRY)/$(IMAGE_PREFIX)-serve:$(VERSION)
IMG_RUNNER     := $(REGISTRY)/$(IMAGE_PREFIX)-runner:$(VERSION)
IMG_RUNNER_PY  := $(REGISTRY)/$(IMAGE_PREFIX)-runner-python:$(VERSION)
IMG_RUNNER_JL  := $(REGISTRY)/$(IMAGE_PREFIX)-runner-julia:$(VERSION)

.PHONY: help build-local release build-cloud build-multiarch up up-cloud down down-v logs ps restart shell encrypt decrypt sops-check sql-clean test-integration test-integration-down

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
	@echo "  make encrypt F=env/render.env          - sops -e một file env (xem docs/SOPS.md)"
	@echo "  make decrypt F=env/render.env.enc      - sops -d ra stdout, dạng KEY=VALUE"
	@echo "  make sops-check F=env/render.env.enc   - verify file SOPS giải mã được TRƯỚC KHI deploy"
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

# `--age` cần PUBLIC key, và public key nằm ở dòng COMMENT thứ hai của file
# `age-keygen` sinh ra (`# public key: age1…`) — KHÔNG phải dòng đầu. Bản cũ
# đọc `keys.txt.pub` (file này không tồn tại; key nằm ở `keys.txt`) lấy `head -1`
# ⇒ luôn ra dòng `# created: …` và SOPS fail "invalid recipient".
#
# Dùng `.sops.yaml` ở gốc repo thì không cần `--age` ở đây: SOPS tự khớp
# `path_regex` → recipient. Cờ `--age` chỉ là fallback khi chạy tay ngoài repo.
encrypt:
	@test -n "$(F)" || (echo "Usage: make encrypt F=env/render.env" && exit 1)
	@test -f $(SOPS_AGE_KEY) || (echo "ERROR: không thấy $(SOPS_AGE_KEY) — chạy: age-keygen -o $(SOPS_AGE_KEY)" && exit 1)
	sops --age $$(sed -n 's/^# public key: //p' $(SOPS_AGE_KEY)) -e $(F) > $(F).enc

# Ngược lại: giải mã ra stdout. `--output-type dotenv` vì `scripts/release.sh`
# đọc đúng chuẩn đó (`sops -d --output-type dotenv`) rồi `export` từng dòng —
# file mã hoá KHÔNG ra được dotenv thì container boot sẽ im lặng thiếu env.
decrypt:
	@test -n "$(F)" || (echo "Usage: make decrypt F=env/render.env.enc" && exit 1)
	sops -d --output-type dotenv $(F)

# Kiểm tra nhanh file SOPS có giải mã bằng key hiện tại không, và có ra đúng
# dạng `KEY=VALUE` không — chạy TRƯỚC khi deploy.
sops-check:
	@test -n "$(F)" || (echo "Usage: make sops-check F=env/render.env.enc" && exit 1)
	sops -d --output-type dotenv $(F) | grep -qE '^[A-Z_]+=' || (echo "ERROR: output không phải KEY=VALUE" && exit 1)
	@echo "OK — file giải mã được và ra đúng dạng KEY=VALUE"

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

