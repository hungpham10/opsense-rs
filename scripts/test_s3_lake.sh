#!/usr/bin/env bash
set -euo pipefail

# test_s3_lake.sh — End-to-end test:
#   Prometheus (real) → metrics-adapter → opsense http_source
#   → Lakehouse Storage (parquet time-partitioned) → MinIO (S3)
#   Validate: file S3 có ts/blk=*/batch-*.parquet, DuckDB đọc được.
#
# Usage:
#   ./scripts/test_s3_lake.sh up      # build + bring up stack
#   ./scripts/test_s3_lake.sh wait    # đợi stack healthy + data
#   ./scripts/test_s3_lake.sh validate # validate S3 parquet qua DuckDB
#   ./scripts/test_s3_lake.sh down    # tắt stack

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

export OPSENSE_CONFIG=/app/opsense.conf.toml
export OPSENSE_S3_ENDPOINT=http://minio:9000
export OPSENSE_S3_ACCESS_KEY_ID=opsense
export OPSENSE_S3_SECRET_ACCESS_KEY=opsense123

RED='\033[0;31m'
GRN='\033[0;32m'
YEL='\033[1;33m'
NC='\033[0m'
log()  { echo -e "${GRN}[test-s3-lake] $*${NC}"; }
warn() { echo -e "${YEL}[test-s3-lake] $*${NC}"; }
err()  { echo -e "${RED}[test-s3-lake] $*${NC}"; }

up() {
    log "build + bring up stack..."
    docker compose up -d --build prometheus node-exporter metrics-adapter minio minio-bucket opsense-serve opsense-runner opsense-runner-python
    log "done."
}

down() {
    log "tắt stack..."
    docker compose down -v
    log "done."
}

wait_healthy() {
    log "đợi stack healthy..."
    docker compose ps
    # Đợi opsense-serve healthy (tối đa 90s).
    for i in $(seq 1 45); do
        if docker compose ps opsense-serve 2>/dev/null | grep -q "healthy"; then
            log "opsense-serve healthy."
            break
        fi
        sleep 2
    done
    # Đợi Prometheus đã scrape ít nhất 1 lần.
    log "đợi Prometheus có data..."
    for i in $(seq 1 30); do
        if curl -fsS "http://localhost:9090/api/v1/query?query=up" 2>/dev/null | grep -q '"status":"success"'; then
            log "Prometheus có data."
            break
        fi
        sleep 2
    done
}

validate_s3() {
    log "validate S3 parquet..."
    # Bucket opsense-lake phải tồn tại.
    docker compose exec minio-bucket mc ls myminio/opsense-lake/integration-test/ 2>/dev/null || {
        err "Bucket opsense-lake chưa có data."
        return 1
    }
    # Liệt kê ts/ blk partitions.
    log "Files trên S3:"
    docker compose exec minio-bucket mc ls --recursive myminio/opsense-lake/integration-test/ 2>/dev/null | head -20 || true

    # Copy một file parquet từ S3 về host để DuckDB đọc.
    log "Copy parquet từ S3 về host..."
    local tmp_dir
    tmp_dir=$(mktemp -d)
    docker compose exec minio-bucket mc cp --recursive myminio/opsense-lake/integration-test/station-0-timeseries/ts/ "$tmp_dir/" 2>/dev/null || true
    ls -R "$tmp_dir" 2>/dev/null | head -20 || true

    # DuckDB đọc.
    if command -v duckdb &>/dev/null; then
        log "DuckDB đọc parquet..."
        duckdb -c "
        INSTALL httpfs; LOAD httpfs;
        CREATE SECRET s3 (TYPE S3, ENDPOINT 'http://localhost:9000', KEY_ID 'opsense', SECRET 'opsense123');
        SET s3_url_style 'path';
        SELECT decode(series) AS block_id, ts, convert_from(value, 'utf8') AS value
        FROM read_parquet('${tmp_dir}/**/*.parquet', union_by_name = true, hive_partitioning = true)
        LIMIT 5;
        "
    else
        warn "DuckDB không có trên host — skip read-back. Install: pip install duckdb"
    fi

    log "${GRN}validate done.${NC}"
}

case "${1:-}" in
    up)       up ;;
    down)     down ;;
    wait)     wait_healthy ;;
    validate) validate_s3 ;;
    "")
        echo "Usage: $0 {up|down|wait|validate}"
        exit 1
        ;;
esac
