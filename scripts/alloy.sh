#!/bin/bash
exec /usr/bin/alloy run /etc/alloy/config.alloy \
    --server.http.listen-addr=127.0.0.1:12345 \
    --storage.path=/var/lib/alloy
