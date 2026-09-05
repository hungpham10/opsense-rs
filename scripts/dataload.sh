#!/usr/bin/env sh

######################################################################
# @author      : Hung Nguyen Xuan Pham (hung0913208@gmail.com)
# @file        : prepare-data
# @created     : Saturday Oct 04, 2025 18:30:54 +07
#
# @description :
######################################################################


prepare() {
    if [ "$1" = "mysql" ]; then
        i=0
        while [ $i -le 30 ]; do
            if mysqladmin ping -h "${MYSQL_HOST:-127.0.0.1}" -u "${MYSQL_USER:-root}" -P "${MYSQL_PORT:-3306}" --password="${MYSQL_PASSWORD:-rootroot}" --silent; then
                break
            else
                echo "Waiting for MySQL to be ready..."
                sleep 1
            fi
            i=$((i + 1))
        done
        if ! mysqladmin ping -h "${MYSQL_HOST:-127.0.0.1}" -u "${MYSQL_USER:-root}" -P "${MYSQL_PORT:-3306}" --password="${MYSQL_PASSWORD:-rootroot}" --silent; then
            echo "Error: MySQL is not ready" >&2
            mysqladmin ping -h "${MYSQL_HOST:-127.0.0.1}" -u "${MYSQL_USER:-root}" -P "${MYSQL_PORT:-3306}" --password="${MYSQL_PASSWORD:-rootroot}"
            exit 1
        fi
        for script_path in "$2"/*; do
            if [ -f "$script_path" ]; then  # POSIX: dùng [ ] thay [[ ]]
                echo "Executing SQL script: $script_path"
                if ! mysql -h "${MYSQL_HOST:-127.0.0.1}" -u "${MYSQL_USER:-root}" -P "${MYSQL_PORT:-3306}" --password="${MYSQL_PASSWORD:-rootroot}" -D "${MYSQL_DB:-test}" < "$script_path"; then
                    echo "Error: Failed to execute $script_path" >&2
                    exit $?
                fi
            fi
        done
    elif [ "$1" = "postgres" ]; then
        i=0
        PG_HOST="${PG_HOST:-127.0.0.1}"
        while [ $i -le 30 ]; do
            if ! pg_isready -h "$PG_HOST"; then
                sleep 1
            else
                break
            fi
            i=$((i + 1))
        done
        if ! pg_isready -h "$PG_HOST" >/dev/null 2>&1; then
            pg_isready -h "$PG_HOST"
            exit $?
        fi
        for script_path in "$2"/*; do
            if [ ! -f "$script_path" ]; then
                continue
            fi
            if ! PGPASSWORD="${PG_PASSWORD:-rootroot}" psql -h "${PG_HOST:-127.0.0.1}" \
                    -U "${PG_USERNAME:-postgres}" \
                    -d "${PG_DATABASE:-test}" \
                    -a -f "$script_path"; then
                exit $?
            fi
        done
    else
        echo "Error: Unsupported database type: $1 (use 'mysql' or 'postgres')" >&2
        exit 1
    fi
}

if [ $# -lt 2 ]; then
    echo "Usage: $0 <database> <sql_dir> [extra_args...]" >&2
    exit 1
fi

DATABASE="$1"
SQL="$2"
shift 2  # POSIX: shift 2 thay vì shift; shift
prepare "$DATABASE" "$SQL"

if [ $# -gt 0 ]; then  # Fix: >0 nếu muốn check extra args, thay vì >1
    echo "Generate data for performance testing"
fi
