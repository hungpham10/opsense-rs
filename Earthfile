VERSION 0.8

# -----------------------------------------------------------------------
# Global config — override khi gọi, e.g. `earthly +all --TARGET=x86_64-unknown-linux-gnu`
# -----------------------------------------------------------------------
ARG --global REGISTRY=ghcr.io
ARG --global IMAGE_PREFIX=hungpham10/opsense
ARG --global VERSION=latest
ARG --global TARGET=x86_64-unknown-linux-gnu

# -----------------------------------------------------------------------
# build-binaries — Ưu tiên lấy binary sẵn từ host, fallback về Cargo build
# -----------------------------------------------------------------------
build-binaries:
    FROM rust:1.94-slim-bookworm
    ARG TARGET=$TARGET

    WORKDIR /src

    # 1. Cài đặt system dependencies để sẵn sàng cho fallback build nếu thiếu binary
    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            protobuf-compiler pkg-config libssl-dev build-essential mold clang && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    # 2. Copy binaries từ host nếu đã build sẵn ở CI runner (dùng --if-exists thay cho --ignore-missing)
    # Lưu ý: Earthly sẽ không fail nếu các đường dẫn dưới đây chưa tồn tại
    # KHÔNG dùng --dir để nội dung binaries/ được copy thẳng vào /src/bin/ (không tạo thư mục con binaries/)
    COPY --if-exists binaries/ /src/bin/
    # target/ bị .earthignore chặn; giữ lại dòng dưới làm fallback nhưng thực tế không bao giờ copy được
    COPY --dir --if-exists target/${TARGET}/release target/release /src/bin/

    # 3. Copy toàn bộ source code vào để phục vụ fallback build
    COPY . /src/code/

    # 4. Kiểm tra & log: Nếu có binary từ host thì copy ra /out, ngược lại tiến hành cargo build
    RUN sh -c '\
      mkdir -p /out && \
      echo "=== Verifying pre-built binaries in /src/bin ===" && \
      if [ -d "/src/bin" ]; then ls -la /src/bin/; else echo "/src/bin does not exist (no pre-built binaries copied)"; fi && \
      if [ -f "/src/bin/opsense" ]; then \
        echo "--> Found pre-built binaries from host!"; \
        cp /src/bin/opsense* /out/; \
      elif [ -f "/src/bin/release/opsense" ]; then \
        echo "--> Found pre-built binaries in release folder!"; \
        cp /src/bin/release/opsense* /out/; \
      else \
        echo "--> Pre-built binaries not found! Fallback to Cargo build inside Earthly..."; \
        cd /src/code && \
        RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --workspace --release --locked && \
        cp target/release/opsense* /out/; \
      fi && \
      echo "=== Final artifacts in /out ===" && \
      ls -la /out/'

    SAVE ARTIFACT /out/opsense /opsense
    SAVE ARTIFACT /out/opsense-kernel-echo /opsense-kernel-echo
    SAVE ARTIFACT /out/opsense-kernel-python /opsense-kernel-python
    SAVE ARTIFACT /out/opsense-kernel-julia /opsense-kernel-julia

# -----------------------------------------------------------------------
# serve — OpenResty reverse proxy + opsense + alloy
# -----------------------------------------------------------------------
serve:
    FROM openresty/openresty:1.27.1.2-4-bookworm-fat
    ARG VERSION=$VERSION

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

    # Lua-resty modules
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
    COPY conf/dex/config.dev.yaml /etc/dex/config.dev.yaml

    # Helper scripts + entrypoint
    COPY scripts/nginx.sh      /app/nginx.sh
    COPY scripts/alloy.sh      /app/alloy.sh
    COPY scripts/release.sh    /app/entrypoint.sh

    # Copy binary lấy từ artifact của build-binaries
    COPY +build-binaries/opsense /app/opsense
    RUN chmod +x /app/*.sh /app/opsense

    ENTRYPOINT ["/app/entrypoint.sh", "/usr/bin/supervisord", "-n"]
    EXPOSE 8080
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-serve:${VERSION}
    SAVE IMAGE opsense-serve:${VERSION}

# -----------------------------------------------------------------------
# runner — opsense runner subcommand + default echo kernel
# -----------------------------------------------------------------------
runner:
    FROM debian:bookworm-slim
    ARG VERSION=$VERSION

    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            ca-certificates libssl3 && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    COPY +build-binaries/opsense             /app/opsense
    COPY +build-binaries/opsense-kernel-echo /app/opsense-kernel-echo
    RUN chmod +x /app/opsense /app/opsense-kernel-echo

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

    RUN apt-get update && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            ca-certificates libssl3 && \
        pip install --no-cache-dir numpy pandas pyarrow protobuf && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

    COPY +build-binaries/opsense               /app/opsense
    COPY +build-binaries/opsense-kernel-python /app/opsense-kernel-python
    RUN chmod +x /app/opsense /app/opsense-kernel-python

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

    COPY +build-binaries/opsense              /app/opsense
    COPY +build-binaries/opsense-kernel-julia /app/opsense-kernel-julia
    RUN chmod +x /app/opsense /app/opsense-kernel-julia

    RUN julia -e 'import Pkg; Pkg.add(["Arrow", "DataFrames", "CSV", "Plots"])'

    ENV OPSENSE_RUNNER_BIND=0.0.0.0:50051
    ENV OPSENSE_KERNEL=/app/opsense-kernel-julia

    EXPOSE 50051
    ENTRYPOINT ["/app/opsense", "runner"]
    SAVE IMAGE --push ${REGISTRY}/${IMAGE_PREFIX}-runner-julia:${VERSION}
    SAVE IMAGE opsense-runner-julia:${VERSION}

# -----------------------------------------------------------------------
# all — build & push cả 4 images
# -----------------------------------------------------------------------
all:
    BUILD +serve
    BUILD +runner
    BUILD +runner-python
    BUILD +runner-julia

# -----------------------------------------------------------------------
# integration-images — build 4 images locally (no registry push)
# -----------------------------------------------------------------------
integration-images:
    ARG VERSION=local
    BUILD --build-arg VERSION=${VERSION} +serve
    BUILD --build-arg VERSION=${VERSION} +runner
    BUILD --build-arg VERSION=${VERSION} +runner-python
    BUILD --build-arg VERSION=${VERSION} +runner-julia
