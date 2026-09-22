"""Local metrics server — serves static observation JSON for the local test case.

Returns a JSON array of observation objects that `opsense http_source` expects:
[{"ts":..., "metric_id":"cpu_usage", "kind":"gauge", "signal":"rate", "value":..., "labels":{...}}, ...]
"""

import json
import random
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

OBSERVATIONS = [
    {"metric_id": "cpu_usage", "kind": "gauge", "signal": "rate"},
    {"metric_id": "mem_usage", "kind": "gauge", "signal": "gauge"},
    {"metric_id": "disk_usage", "kind": "gauge", "signal": "rate"},
]


def generate_batch(now: float) -> list[dict]:
    out = []
    for obs in OBSERVATIONS:
        value = {
            "cpu_usage": random.uniform(0.1, 0.9),
            "mem_usage": random.uniform(0.3, 0.8),
            "disk_usage": random.uniform(0.4, 0.7),
        }[obs["metric_id"]]
        out.append({
            "ts": int(now),
            "metric_id": obs["metric_id"],
            "kind": obs["kind"],
            "signal": obs["signal"],
            "value": value,
            "labels": {"instance": "local-test"},
        })
        now += 10
    return out


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            batch = generate_batch(time.time())
            body = json.dumps(batch)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode())
        elif self.path == "/-/healthy":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        print(f"local-metrics: {fmt % args}")


def main():
    port = int(__import__("os").environ.get("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"local-metrics listening on :{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
