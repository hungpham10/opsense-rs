SHELL := /bin/bash
APP_ENV ?= dev
VERSION ?= local
REGISTRY ?= ghcr.io
IMAGE_PREFIX ?= lap02921/opsense
COMPOSE := docker compose
EARTHLY := earthly

# Tên image đầy đủ (registry + prefix + suffix). Dùng chung cho cả build và push.
IMG_SERVE      := $(REGISTRY)/$(IMAGE_PREFIX)-serve:$(VERSION)
IMG_RUNNER     := $(REGISTRY)/$(IMAGE_PREFIX)-runner:$(VERSION)
IMG_RUNNER_PY  := $(REGISTRY)/$(IMAGE_PREFIX)-runner-python:$(VERSION)
IMG_RUNNER_JL  := $(REGISTRY)/$(IMAGE_PREFIX)-runner-julia:$(VERSION)

# Alias :local để docker-compose tham chiếu (chỉ cần khi VERSION=local).
ifeq ($(VERSION),local)
  ALIAS_SERVE      := opsense-serve:local
  ALIAS_RUNNER     := opsense-runner:local
  ALIAS_RUNNER_PY  := opsense-runner-python:local
  ALIAS_RUNNER_JL  := opsense-runner-julia:local
endif

.PHONY: help build-local build-cloud up down logs ps restart shell encrypt decrypt sql-clean

help:
	@echo "Opsense dev shortcuts:"
	@echo "  make build-local   - Build + tag :local aliases (dùng cho docker-compose). VERSION mặc định = local."
	@echo "  make build-cloud  - Build & push lên cloud registry. Bắt buộc VERSION=... (vd: 1.2.3)."
	@echo "  make up            - docker compose up -d (APP_ENV=$(APP_ENV))"
	@echo "  make down          - docker compose down (KHÔNG xoá volume; dùng 'down-v' để xoá)"
	@echo "  make down-v        - docker compose down -v (DESTRUCTIVE: xoá volume)"
	@echo "  make ps            - docker compose ps"
	@echo "  make logs          - docker compose logs -f --tail=100"
	@echo "  make restart SVC=x - restart một service (vd: make restart SVC=opsense-serve)"
	@echo "  make shell SVC=x   - bash vào service (vd: make shell SVC=opsense-serve)"
	@echo "  make encrypt F=env/secrets.dev.yaml      - sops -e một secrets file"
	@echo "  make decrypt F=env/secrets.dev.enc.yaml  - sops -d một secrets file"
	@echo "  make sql-clean     - xoá *.sql files sinh ra sau init"
	@echo ""
	@echo "Biến override:"
	@echo "  VERSION=1.2.3 REGISTRY=... IMAGE_PREFIX=... make build-local  (build cloud tag)"
	@echo "  VERSION=1.2.3 make build-cloud                             (build + push)"
	@echo "  APP_ENV=uat make up"

# Build 4 images cho local docker daemon + tag :local aliases
# để docker-compose có thể tham chiếu.
build-local:
	$(EARTHLY) +all-local
	@if [ "$(VERSION)" != "local" ]; then \
		echo "NOTE: VERSION=$(VERSION) khác 'local' — bỏ qua tag :local aliases."; \
	else \
		docker tag $(IMG_SERVE)     $(ALIAS_SERVE); \
		docker tag $(IMG_RUNNER)    $(ALIAS_RUNNER); \
		docker tag $(IMG_RUNNER_PY) $(ALIAS_RUNNER_PY); \
		docker tag $(IMG_RUNNER_JL) $(ALIAS_RUNNER_JL); \
		echo "Tagged: $(ALIAS_SERVE), $(ALIAS_RUNNER), $(ALIAS_RUNNER_PY), $(ALIAS_RUNNER_JL)"; \
	fi

# Build & push 4 images lên cloud registry.
# Ví dụ: VERSION=1.2.3 make build-cloud
#         VERSION=$$(git rev-parse --short HEAD) make build-cloud
build-cloud:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required (vd: VERSION=1.2.3)" && exit 1)
	@if [ "$(VERSION)" = "local" ]; then \
		echo "ERROR: VERSION=local không được push lên registry."; exit 1; \
	fi
	$(EARTHLY) --push +all

up:
	APP_ENV=$(APP_ENV) $(COMPOSE) up -d

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
