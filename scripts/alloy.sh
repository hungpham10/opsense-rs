#!/bin/sh
set -e

if [ "$USE_ALLOY" = "true" ]; then
    exec /usr/bin/alloy run /etc/alloy/config.alloy --storage.path=/tmp/alloy-data
else
    sleep 315576000
fi
