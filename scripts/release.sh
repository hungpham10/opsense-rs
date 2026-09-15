#!/bin/bash

######################################################################
# @author      : Hung Nguyen Xuan Pham (hung0913208@gmail.com)
# @file        : release
# @created     : Tuesday Aug 13, 2024 22:19:39 +07
# @description : Entrypoint tích hợp SOPS giải mã đa môi trường
######################################################################

function decrypt_secrets() {
  local ENV=${APP_ENV:-dev}
  local ENCRYPTED_FILE="${ENCRYPTED_FILE:-/app/secrets/secrets.${ENV}.enc.yaml}"

  if [ -z "$SOPS_AGE_KEY_CONTENT" ]; then
    echo "--- [SOPS] Not found SOPS_AGE_KEY_CONTENT. Ignore decryption. ---"
    return
  fi

  if [ -f "$ENCRYPTED_FILE" ]; then
    local decrypted_content
    decrypted_content=$(SOPS_AGE_KEY_FILE=<(echo "$SOPS_AGE_KEY_CONTENT") sops -d --output-type dotenv "$ENCRYPTED_FILE" 2>/dev/null)

    if [ -n "$decrypted_content" ]; then
      while IFS= read -r line || [ -n "$line" ]; do
        [[ -z "$line" || "$line" =~ ^# ]] && continue

        if [ "${DEBUG}" = "true" ]; then
          echo "$line"
        fi

        export "$line"
      done <<< "$decrypted_content"
    else
      echo "Error: [SOPS] Decrypt failed." >&2
      exit 1
    fi

    unset SOPS_AGE_KEY_CONTENT
    export SOPS_AGE_KEY_CONTENT=""
  else
    echo "--- [SOPS] Not found $ENCRYPTED_FILE, will use default environment variables ---"
  fi
}

function prepare() {
  if [ -f /etc/grafana-agent/config.yaml.shenv ]; then
    if ! envsubst < /etc/grafana-agent/config.yaml.shenv > /etc/grafana-agent/config.yaml; then
      echo "Error: failed to generate /etc/grafana-agent/config.yaml" >&2
      exit 1
    fi
  fi

  if [[ ${DISABLE_AUTO_INIT_DATABASE} = "true" ]]; then
    return
  fi

  # SQL layout:
  #   ${SQL_BASE}/{postgres|mysql}/*.sql            -> schema (depth=1, runs first)
  #   ${SQL_BASE}/{postgres|mysql}/${APP_ENV}/*.sql  -> env-specific seed (depth=2, runs after)
  local ENV=${APP_ENV:-dev}
  local SQL_BASE=${SQL_BASE:-/app/sql}

  # --- THỬ KẾT NỐI MYSQL ---
  local use_mysql=0
  if [ -n "$MYSQL_HOST" ]; then
    local mysql_args="-h $MYSQL_HOST -u $MYSQL_USER -P ${MYSQL_PORT:-3306} --password=$MYSQL_PASSWORD"

    for i in {1..30}; do
      if mysqladmin ping $mysql_args --silent >/dev/null 2>&1; then
        use_mysql=1
        break
      fi
      echo "Waiting for MySQL to be ready ($i/30)..."
      sleep 1
    done
  fi

  if [ $use_mysql -eq 1 ]; then
    # Giai đoạn 1: schema (depth=1)
    if [ -d "${SQL_BASE}/mysql" ]; then
      for script_path in $(ls -1 "${SQL_BASE}"/mysql/*.sql 2>/dev/null | sort); do
        echo "[schema] Executing: $script_path"
        if ! mysql $mysql_args "$MYSQL_DATABASE" < "$script_path"; then
          echo "Error: Failed to execute $script_path" >&2
          exit 1
        fi
      done
    fi

    # Giai đoạn 2: env-specific seed (depth=2)
    if [ -d "${SQL_BASE}/mysql/${ENV}" ]; then
      for script_path in $(ls -1 "${SQL_BASE}/mysql/${ENV}"/*.sql 2>/dev/null | sort); do
        echo "[env=${ENV}] Executing: $script_path"
        if ! mysql $mysql_args "$MYSQL_DATABASE" < "$script_path"; then
          echo "Error: Failed to execute $script_path" >&2
          exit 1
        fi
      done
    fi
    return
  fi

  # --- THỬ KẾT NỐI POSTGRESQL ---
  local use_pg=0
  if [ -n "$POSTGRES_HOST" ]; then
    export PGPASSWORD="$POSTGRES_PASSWORD"

    for i in {1..30}; do
      if pg_isready -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" -U "$POSTGRES_USER" >/dev/null 2>&1; then
        use_pg=1
        break
      fi
      echo "Waiting for PostgreSQL to be ready ($i/30)..."
      sleep 1
    done
  fi

  if [ $use_pg -eq 1 ]; then
    local pg_args="-h $POSTGRES_HOST -p ${POSTGRES_PORT:-5432} -U $POSTGRES_USER -d $POSTGRES_DATABASE"

    # Giai đoạn 1: schema (depth=1)
    if [ -d "${SQL_BASE}/postgres" ]; then
      for script_path in $(ls -1 "${SQL_BASE}"/postgres/*.sql 2>/dev/null | sort); do
        echo "[schema] Executing: $script_path"
        if ! psql $pg_args -v ON_ERROR_STOP=1 -f "$script_path"; then
          echo "Error: Failed to execute $script_path" >&2
          unset PGPASSWORD
          exit 1
        fi
      done
    fi

    # Giai đoạn 2: env-specific seed (depth=2)
    if [ -d "${SQL_BASE}/postgres/${ENV}" ]; then
      for script_path in $(ls -1 "${SQL_BASE}/postgres/${ENV}"/*.sql 2>/dev/null | sort); do
        echo "[env=${ENV}] Executing: $script_path"
        if ! psql $pg_args -v ON_ERROR_STOP=1 -f "$script_path"; then
          echo "Error: Failed to execute $script_path" >&2
          unset PGPASSWORD
          exit 1
        fi
      done
    fi

    unset PGPASSWORD
    return
  fi

  # Nếu đi đến đây tức là cả 2 DB đều không kết nối được hoặc không cấu hình env
  echo "Error: No database (MySQL or PostgreSQL) is available or configured." >&2
  exit 1
}

function localtonet() {
  if [ -z "${DOTNET_SYSTEM_GLOBALIZATION_INVARIANT:-}" ]; then
    export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
  fi

  if [ -n "${LOCALTONET:-}" ]; then
    set -x
    screen -S "localtonet.pid" -dm localtonet authtoken "$LOCALTONET"
    set +x
  fi
}

function boot() {
  local cmd=$1
  local nginx_conf="${NGINX_DIR}/http.d/default.conf"

  if [ "${USE_TOR}" = "true" ]; then
    rm -f "${SUPERVISOR_DIR}/without-tor.conf"
  else
    rm -f "${SUPERVISOR_DIR}/with-tor.conf"
  fi

  if [ "${HTTP_PROTOCOL}" = "https" ]; then
    sed -i "s/%%FORCE_SSL%%/on/g" "$nginx_conf"
  else
    sed -i '/HTTPS/d; /HTTP_X_FORWARDED_PROTO/d; /HTTP_X_FORWARDED_PORT/d' "$nginx_conf"
    HTTP_PROTOCOL="http"
  fi

  sed -i "s/%%NGINX_LOG%%/$NGINX_LOG/g" "${NGINX_DIR}/nginx.conf"

  sed -i "s|%%CDN_ENDPOINT%%|$CDN_ENDPOINT|g; s|%%CDN_BUCKET%%|$CDN_BUCKET|g; s|%%AWS_ACCESS_KEY_ID%%|$AWS_ACCESS_KEY_ID|g; s|%%AWS_SECRET_ACCESS_KEY%%|$AWS_SECRET_ACCESS_KEY|g" "$nginx_conf"
  sed -i "s|%%SERVER_PORT%%|$SERVER_PORT|g" "$nginx_conf"
  sed -i "s|%%HTTP_SERVER%%|$HTTP_SERVER|g; s|%%HTTP_PORT%%|$HTTP_PORT|g" "$nginx_conf"
  sed -i "s|%%WOOCOMMERCE_PROTOCOL%%|$WOOCOMMERCE_PROTOCOL|g; s|%%WOOCOMMERCE_TOKEN%%|$WOOCOMMERCE_TOKEN|g; s|%%WOOCOMMERCE_SERVER%%|$WOOCOMMERCE_SERVER|g; s|%%WOOCOMMERCE_HOSTNAME%%|$WOOCOMMERCE_HOSTNAME|g" "$nginx_conf"

  if [ "${WOOCOMMERCE_PROTOCOL}" != "http" ]; then
    sed -i '/proxy_ssl_server_name/d; /proxy_ssl_verify/d' "$nginx_conf"
  fi

  if [ "${USE_TOR}" = "true" ]; then
    mkdir -p /var/lib/tor/hidden_service
    chmod 700 /var/lib/tor/hidden_service 2>/dev/null || true

    if [ -n "${TOR_SERVER}" ]; then
      echo "${TOR_SERVER}" > /var/lib/tor/hidden_service/hostname
      echo "${TOR_PUBLIC_KEY}" | base64 -d > /var/lib/tor/hidden_service/hs_ed25519_public_key
      echo "${TOR_SECRET_KEY}" | base64 -d > /var/lib/tor/hidden_service/hs_ed25519_secret_key
      chmod 600 /var/lib/tor/hidden_service/hs_ed25519_secret_key 2>/dev/null || true
    fi

    chown -R debian-tor:debian-tor /var/lib/tor
    sed -i "s/%%COMMAND%%/$COMMAND/g" "${SUPERVISOR_DIR}/with-tor.conf"
  else
    sed -i "s/%%COMMAND%%/$COMMAND/g" "${SUPERVISOR_DIR}/without-tor.conf"
  fi

  shift
  exec "$cmd" "$@"
}

CMD=$1
shift

decrypt_secrets

prepare

localtonet

boot "$CMD" "$@"
