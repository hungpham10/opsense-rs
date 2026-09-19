VERSION 0.8

# -----------------------------------------------------------------------
# Global config — override after the target, e.g. `earthly +all --VERSION=v1.0.0`
# -----------------------------------------------------------------------
ARG --global REGISTRY=ghcr.io
ARG --global IMAGE_PREFIX=hungpham10/opsense
ARG --global VERSION=latest
ARG --global TARGET=x86_64-unknown-linux-gnu

# -----------------------------------------------------------------------
# builder — shared Rust toolchain layer (apt deps only, no source yet)
# -----------------------------------------------------------------------
builder:
    FROM debian:bookworm-slim
    ARG TOOLCHAIN_VERSION=1.94
    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            pkg-config ca-certificates protobuf-compiler curl build-essential && \
        apt-get clean && rm -rf /var/lib/apt/lists/* && \
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${TOOLCHAIN_VERSION} && \
        mv /root/.cargo/bin/* /usr/local/bin/ && \
        rustc --version && cargo --version
    WORKDIR /app
    SAVE IMAGE --cache-hint

# -----------------------------------------------------------------------
# chef — install cargo-chef + cargo-zigbuild; reused by every recipe.
# -----------------------------------------------------------------------
chef:
    FROM +builder
    ARG ZIG_VERSION=0.13.0
    RUN curl -fsSL https://ziglang.org/download/${ZIG_VERSION}/zig-linux-x86_64-${ZIG_VERSION}.tar.xz | tar -xJ -C /usr/local/lib/ && \
        ln -s /usr/local/lib/zig-linux-x86_64-${ZIG_VERSION}/zig /usr/local/bin/zig && \
        zig version && \
        cargo install cargo-chef cargo-zigbuild --locked
    WORKDIR /app
    SAVE IMAGE --cache-hint

# -----------------------------------------------------------------------
# recipe — cargo-chef recipe.json
# -----------------------------------------------------------------------
recipe:
    FROM +chef
    ARG TARGET=$TARGET
    COPY . .
    RUN cargo chef prepare --recipe-path recipe.json
    SAVE ARTIFACT recipe.json

# -----------------------------------------------------------------------
# binaries — build release binaries
# -----------------------------------------------------------------------
binaries:
    FROM +recipe
    ARG TARGET=$TARGET
    RUN cargo chef cook --release --recipe-path recipe.json 
    COPY . .
    RUN cargo zigbuild --release --locked --target ${TARGET}
    SAVE ARTIFACT target/${TARGET}/release/opsense opsense
    SAVE ARTIFACT target/${TARGET}/release/opsense-kernel-echo opsense-kernel-echo
    SAVE ARTIFACT target/${TARGET}/release/opsense-kernel-python opsense-kernel-python
    SAVE ARTIFACT target/${TARGET}/release/opsense-kernel-julia opsense-kernel-julia

# -----------------------------------------------------------------------
# serve — Tầng 1 host: OpenResty reverse proxy + opsense + alloy
# -----------------------------------------------------------------------
serve:
    FROM openresty/openresty:1.27.1.2-4-bookworm-fat
    ARG VERSION=$VERSION
    ARG TARGET=$TARGET

    # Runtime deps: supervisor, alloy (Grafana), curl, etc.
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

    # Dex OIDC config
    COPY conf/dex/config.dev.yaml /etc/dex/config.dev.yaml

    # Helper scripts + entrypoint
    COPY scripts/nginx.sh      /app/nginx.sh
    COPY scripts/alloy.sh      /app/alloy.sh
    COPY scripts/release.sh    /app/entrypoint.sh

    # Backend binary (Chú ý khoảng trắng giữa --TARGET=$TARGET và /opsense)
    COPY (+binaries/opsense --TARGET=$TARGET) /app/opsense
    RUN chmod +x /app/*.sh

    ENTRYPOINT ["/app/entrypoint.sh", "/usr/bin/supervisord", "-n"]
    EXPOSE 8080
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-serve:${VERSION}
    SAVE IMAGE opsense-serve:${VERSION}

# -----------------------------------------------------------------------
# runner — Tầng 2: opsense runner subcommand + default echo kernel
# -----------------------------------------------------------------------
runner:
    FROM debian:bookworm-slim
    ARG VERSION=$VERSION
    ARG TARGET=$TARGET

    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            ca-certificates libssl3 && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    COPY (+binaries/opsense --TARGET=$TARGET)             /app/opsense
    COPY (+binaries/opsense-kernel-echo --TARGET=$TARGET) /app/opsense-kernel-echo

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-echo

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner:${VERSION}
    SAVE IMAGE opsense-runner:${VERSION}

# -----------------------------------------------------------------------
# runner-python — runner with Python 3.12 + opsense-kernel-python
# -----------------------------------------------------------------------
runner-python:
    FROM python:3.12-slim
    ARG VERSION=$VERSION
    ARG TARGET=$TARGET

    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            ca-certificates libssl3 && \
        pip install --no-cache-dir numpy pandas pyarrow protobuf && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    COPY (+binaries/opsense --TARGET=$TARGET)               /app/opsense
    COPY (+binaries/opsense-kernel-python --TARGET=$TARGET) /app/opsense-kernel-python

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-python

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner-python:${VERSION}
    SAVE IMAGE opsense-runner-python:${VERSION}

# -----------------------------------------------------------------------
# runner-julia — runner with Julia 1.10 + opsense-kernel-julia
# -----------------------------------------------------------------------
runner-julia:
    FROM julia:1.10-bookworm
    ARG VERSION=$VERSION
    ARG TARGET=$TARGET

    COPY (+binaries/opsense --TARGET=$TARGET)              /app/opsense
    COPY (+binaries/opsense-kernel-julia --TARGET=$TARGET) /app/opsense-kernel-julia

    RUN julia -e 'import Pkg; Pkg.add(["Arrow", "DataFrames", "CSV", "Plots"])'

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-julia

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner-julia:${VERSION}
    SAVE IMAGE opsense-runner-julia:${VERSION}

# -----------------------------------------------------------------------
# all — build & push all 4 images.
# -----------------------------------------------------------------------
all:
    ARG TARGET=$TARGET
    BUILD --build-arg TARGET=${TARGET} +serve
    BUILD --build-arg TARGET=${TARGET} +runner
    BUILD --build-arg TARGET=${TARGET} +runner-python
    BUILD --build-arg TARGET=${TARGET} +runner-julia

# -----------------------------------------------------------------------
# all-multiarch — build and push amd64 and arm64 manifests in one export.
# -----------------------------------------------------------------------
all-multiarch:
    ARG VERSION=$VERSION
    ARG REGISTRY=$REGISTRY
    ARG IMAGE_PREFIX=$IMAGE_PREFIX

    BUILD --platform=linux/amd64 \
        --build-arg TARGET=x86_64-unknown-linux-gnu \
        --build-arg VERSION=${VERSION} \
        --build-arg REGISTRY=${REGISTRY} \
        --build-arg IMAGE_PREFIX=${IMAGE_PREFIX} \
        +all
    BUILD --platform=linux/arm64 \
        --build-arg TARGET=aarch64-unknown-linux-gnu \
        --build-arg VERSION=${VERSION} \
        --build-arg REGISTRY=${REGISTRY} \
        --build-arg IMAGE_PREFIX=${IMAGE_PREFIX} \
        +all

# -----------------------------------------------------------------------
# integration-images — build 4 images locally (no registry push).
# -----------------------------------------------------------------------
integration-images:
    ARG VERSION=local
    ARG TARGET=$TARGET
    BUILD --build-arg VERSION=${VERSION} --build-arg TARGET=${TARGET} +serve
    BUILD --build-arg VERSION=${VERSION} --build-arg TARGET=${TARGET} +runner
    BUILD --build-arg VERSION=${VERSION} --build-arg TARGET=${TARGET} +runner-python
    BUILD --build-arg VERSION=${VERSION} --build-arg TARGET=${TARGET} +runner-julia
