
#!/bin/sh
set -e

echo "============================================="
echo "Nginx wrapper: Waiting for Tor to be ready..."
echo "============================================="

MAX_WAIT=180
WAIT_INTERVAL=5

if [ -d /var/lib/tor/hidden_service ]; then
    for i in $(seq 1 $((MAX_WAIT / WAIT_INTERVAL))); do
        if [ -f /var/lib/tor/hidden_service/hostname ]; then
            TOR_SERVER=$(cat /var/lib/tor/hidden_service/hostname | tr -d '\n\r')
            echo "✅ Tor onion address ready: ${TOR_SERVER}"
            break
        fi

        echo "Tor is not ready... wait (${WAIT_INTERVAL}s)"
        sleep $WAIT_INTERVAL
    done

    if [ ! -f /var/lib/tor/hidden_service/hostname ]; then
        echo "ERROR: Timeout waiting for onion address after ${MAX_WAIT} seconds"
        echo "Tor log:"
        tail -n 50 /var/log/tor/notices.log || echo "No log file found"
        exit 1
    fi

    export TOR_SERVER="${TOR_SERVER}"
fi

if grep -q "%%TOR_SERVER%%" "${NGINX_DIR}/http.d/default.conf" &> /dev/null; then
    sed -i "s/%%TOR_SERVER%%/$TOR_SERVER/g" ${NGINX_DIR}/http.d/default.conf
fi
echo "Nginx starting now..."
echo "============================================="

exec /usr/local/openresty/nginx/sbin/nginx -g "daemon off;"
