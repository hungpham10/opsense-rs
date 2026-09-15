#!/bin/bash
set -e

mkdir -p /var/run/axum
chmod 755 /var/run/axum

exec "$@"
