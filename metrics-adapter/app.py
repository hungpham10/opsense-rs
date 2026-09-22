"""Metrics adapter — Prometheus /api/v1/query_range → opsense observation JSON.

opsense `http_source` expects a JSON array of observation objects:
```json
[{"ts":1700000000,"metric_id":"cpu_usage","kind":"gauge","signal":"rate","value":0.42,"labels":{"instance":"server1"}}]
```

Prometheus `/api/v1/query_range` returns a different shape, so this adapter
queries Prometheus, converts each (metric, timestamp, value) pair into an
observation, and serves the observation-format JSON at GET /metrics.

Docker compose: `metrics-adapter` reachable at http://metrics-adapter:8080/metrics.
"""

import json
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs

import requests

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
DEFAULT_QUERIES = os.environ.get(
    "PROMETHEUS_QUERIES",
    "up{job=\"prometheus\"},node_cpu_seconds_total{mode=\"idle\"},node_memory_MemAvailable_bytes",
)
CACHE_TTL = float(os.environ.get("CACHE_TTL", "5"))

_cache: dict[str, tuple[float, str]] = {}


def _prom_range(query: str, step: str = "10s") -> list[dict]:
    """Query Prometheus /api/v1/query_range and return flat observation dicts."""
    now = time.time()
    params = {
        "query": query,
        "start": str(now - 120),
        "end": str(now),
        "step": step,
    }
    url = f"{PROMETHEUS_URL}/api/v1/query_range?{urlencode(params)}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "success":
        return []
    result = data.get("data", {}).get("result", [])
    obs: list[dict] = []
    for r in result:
        metric = r.get("metric", {})
        metric_id = metric.get("__name__", "unknown")
        labels = {k: v for k, v in metric.items() if k != "__name__"}
        for ts_val, val_val in r.get("values", []):
            try:
                value = float(val_val)
            except (ValueError, TypeError):
                continue
            obs.append({
                "ts": int(float(ts_val)),
                "metric_id": metric_id,
                "kind": "gauge",
                "signal": "gauge",
                "value": value,
                "labels": labels,
            })
    return obs


def _metrics_handler() -> str:
    now = time.time()
    cached, cached_at = _cache.get("data", (0.0, 0.0))
    if now - cached_at < CACHE_TTL:
        return cached
    all_obs: list[dict] = []
    for q in DEFAULT_QUERIES.split(","):
        q = q.strip()
        if not q:
            continue
        try:
            all_obs.extend(_prom_range(q))
        except Exception as e:
            print(f"metrics-adapter: query failed for {q}: {e}")
    body = json.dumps(all_obs)
    _cache["data"] = (body, now)
    return body


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/metrics":
            body = _metrics_handler()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode())
        elif parsed.path == "/-/healthy":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        print(f"metrics-adapter: {fmt % args}")


def main():
    port = int(os.environ.get("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"metrics-adapter listening on :{port} (Prometheus @ {PROMETHEUS_URL})")
    server.serve_forever()


if __name__ == "__main__":
    main()
