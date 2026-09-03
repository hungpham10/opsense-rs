VERSION 0.8

# -----------------------------------------------------------------------
# Global config — override via --build-arg or env when needed
# -----------------------------------------------------------------------
ARG --global REGISTRY=ghcr.io
ARG --global IMAGE_PREFIX=lap02921/opsense
ARG --global VERSION=latest

# -----------------------------------------------------------------------
# builder — shared Rust toolchain layer (apt deps only, no source yet)
# -----------------------------------------------------------------------
builder:
    FROM rust:bookworm
    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            pkg-config ca-certificates protobuf-compiler && \
        apt-get clean && rm -rf /var/lib/apt/lists/*
    WORKDIR /app
    SAVE IMAGE --cache-hint

# -----------------------------------------------------------------------
# recipe — cargo-chef recipe.json. Built once, shared by every binary.
# -----------------------------------------------------------------------
recipe:
    FROM +builder
    RUN cargo install cargo-chef --locked
    COPY . .
    RUN cargo chef prepare --recipe-path recipe.json
    SAVE ARTIFACT recipe.json

# -----------------------------------------------------------------------
# Binary artifacts — each crate's release binary saved as a local artifact
# so the image targets can pick them up without rebuilding.
# -----------------------------------------------------------------------
opsense:
    FROM +recipe
    RUN cargo chef cook --release --recipe-path recipe.json -p opsense
    COPY . .
    RUN cargo build --release -p opsense
    SAVE ARTIFACT target/release/opsense AS LOCAL opsense

kernel-echo:
    FROM +recipe
    RUN cargo chef cook --release --recipe-path recipe.json -p opsense-kernel-echo
    COPY . .
    RUN cargo build --release -p opsense-kernel-echo && \
        mkdir -p /out/kernel-echo && \
        cp target/release/opsense-kernel-echo /out/kernel-echo/
    SAVE ARTIFACT /out/kernel-echo AS LOCAL kernel-echo

kernel-python:
    FROM +recipe
    RUN cargo chef cook --release --recipe-path recipe.json -p opsense-kernel-python
    COPY . .
    RUN cargo build --release -p opsense-kernel-python && \
        mkdir -p /out/kernel-python && \
        cp target/release/opsense-kernel-python /out/kernel-python/
    SAVE ARTIFACT /out/kernel-python AS LOCAL kernel-python

kernel-julia:
    FROM +recipe
    RUN cargo chef cook --release --recipe-path recipe.json -p opsense-kernel-julia
    COPY . .
    RUN cargo build --release -p opsense-kernel-julia && \
        mkdir -p /out/kernel-julia && \
        cp target/release/opsense-kernel-julia /out/kernel-julia/
    SAVE ARTIFACT /out/kernel-julia AS LOCAL kernel-julia

# -----------------------------------------------------------------------
# serve — Tầng 1 host: OpenResty reverse proxy + opsense + alloy
# -----------------------------------------------------------------------
serve:
    FROM openresty/openresty:1.27.1.2-4-bookworm-fat
    ARG VERSION

    # Runtime deps: supervisor, alloy (Grafana), curl, etc. NO tor, NO sops.
    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            supervisor curl git gettext-base postgresql-client gnupg2 ca-certificates && \
        mkdir -p /etc/apt/keyrings && \
        curl -fsSL https://apt.grafana.com/gpg.key | gpg --dearmor -o /etc/apt/keyrings/grafana.gpg && \
        echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
            > /etc/apt/sources.list.d/grafana.list && \
        apt-get update && apt-get install -y alloy && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    # Lua-resty modules bundled in OpenResty image
    RUN cd /tmp && \
        git clone --depth 1 https://github.com/zmartzone/lua-resty-openidc.git && \
        cp -av lua-resty-openidc/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-openidc && \
        git clone --depth 1 https://github.com/fffonion/lua-resty-openssl.git && \
        cp -av lua-resty-openssl/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-openssl && \
        git clone --depth 1 https://github.com/anvouk/lua-resty-jwt-verification.git && \
        cp -av lua-resty-jwt-verification/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-jwt-verification && \
        git clone --depth 1 https://github.com/jkeys089/lua-resty-hmac.git && \
        cp -av lua-resty-hmac/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-hmac && \
        git clone --depth 1 https://github.com/cdbattags/lua-resty-jwt.git && \
        cp -av lua-resty-jwt/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-jwt && \
        git clone --depth 1 https://github.com/bungle/lua-resty-session.git && \
        cp -av lua-resty-session/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-session && \
        git clone --depth 1 https://github.com/ledgetech/lua-resty-http.git && \
        cp -av lua-resty-http/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-http && \
        git clone --depth 1 https://github.com/hamishforbes/lua-ffi-zlib.git && \
        cp -av lua-ffi-zlib/lib/ffi-zlib.lua /usr/local/openresty/lualib/ && \
        rm -rf lua-ffi-zlib && \
        git clone --depth 1 https://github.com/openresty/lua-resty-redis.git && \
        cp -av lua-resty-redis/lib/resty/* /usr/local/openresty/lualib/resty/ && \
        rm -rf lua-resty-redis

    RUN useradd nginx && \
        mkdir -p /var/log/nginx /var/run/axum /app/secrets && \
        chown -R nginx:nginx /var/log/nginx && \
        chmod 755 /var/run/axum

    # Supervisor + nginx + alloy configs
    RUN mkdir -p /etc/supervisor/conf.d
    COPY conf/supervisor/opsense.conf /etc/supervisor/conf.d/opsense.conf
    COPY conf/nginx/http.conf   /usr/local/openresty/nginx/conf/nginx.conf
    COPY conf/nginx/www.conf    /usr/local/openresty/nginx/conf/http.d/default.conf
    COPY conf/nginx/map         /usr/local/openresty/nginx/conf/map.d
    COPY conf/nginx/vhost       /usr/local/openresty/nginx/conf/http.d/vhost
    COPY conf/config.alloy      /etc/alloy/config.alloy

    # Helper scripts + entrypoint
    COPY scripts/nginx.sh      /app/nginx.sh
    COPY scripts/alloy.sh      /app/alloy.sh
    COPY scripts/release.sh    /app/entrypoint.sh

    # Backend binary
    COPY (+opsense/opsense) /app/opsense
    RUN chmod +x /app/*.sh

    ENTRYPOINT ["/app/entrypoint.sh", "/usr/bin/supervisord", "-n"]
    EXPOSE 8080
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-serve:${VERSION}

# -----------------------------------------------------------------------
# runner — Tầng 2: opsense runner subcommand + default echo kernel
# -----------------------------------------------------------------------
runner:
    FROM +recipe
    COPY . .
    RUN cargo build --release -p opsense && \
        cargo build --release -p opsense-kernel-echo

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-echo

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner:${VERSION}

# -----------------------------------------------------------------------
# runner-python — runner with Python 3.12 + opsense-kernel-python
# -----------------------------------------------------------------------
runner-python:
    FROM python:3.12-slim
    ARG VERSION

    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            ca-certificates libssl3 && \
        pip install --no-cache-dir numpy pandas pyarrow protobuf && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    COPY (+opsense/opsense)                       /app/opsense
    COPY (+kernel-python/kernel-python/opsense-kernel-python) /app/opsense-kernel-python

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-python

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner-python:${VERSION}

# -----------------------------------------------------------------------
# runner-julia — runner with Julia 1.10 + opsense-kernel-julia
# -----------------------------------------------------------------------
runner-julia:
    FROM julia:1.10-bookworm
    ARG VERSION

    COPY (+opsense/opsense)                       /app/opsense
    COPY (+kernel-julia/kernel-julia/opsense-kernel-julia) /app/opsense-kernel-julia

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-julia

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner-julia:${VERSION}

# -----------------------------------------------------------------------
# all-local — build all 4 images with tag `local` (no push). Dev workflow.
#   earthly +all-local && docker compose up -d
# -----------------------------------------------------------------------
all-local:
    BUILD --build-arg VERSION=local +serve
    BUILD --build-arg VERSION=local +runner
    BUILD --build-arg VERSION=local +runner-python
    BUILD --build-arg VERSION=local +runner-julia

# -----------------------------------------------------------------------
# all — build & push all 4 images. CI workflow (`earthly --push +all`).
# Tag comes from --build-arg VERSION (CI passes ${{ github.ref_name }}).
# -----------------------------------------------------------------------
all:
    BUILD +serve
    BUILD +runner
    BUILD +runner-python
    BUILD +runner-julia
