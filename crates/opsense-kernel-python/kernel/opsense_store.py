"""
opsense.store - Query observations from Opsense stations.

Lazy reads trực tiếp qua DuckDB trên Parquet lakehouse của station (S3) khi
session có env `OPSENSE_S3_BASE` (+ credentials `OPSENSE_S3_*`); ngược lại
fallback gọi GraphQL của serve (`OPSENSE_SERVE_URL`).

Layout parquet (time-partitioned lake, do `LakehouseStorage` flush):
`<base>/<station>/ts/blk=<block_id>/batch-<millis>.parquet` với cột
`id, series BLOB ('blk:<block_id>'), ts BIGINT, value BLOB (JSON của Block)`.
Partition `blk=` (block partition, `block_id = floor(ts / block_secs)`) nên
DuckDB/Spark/Polars đều prune được theo thời gian. Query lọc `blk` (partition
column) + `series` trong SQL → chỉ file bob liên quan được đọc.

Các hàm trả pandas DataFrame với cột: ts (int64 unix sec), metric_id, value,
labels (JSON string), kind, signal.
"""

import json
import os
import time
import urllib.request
from typing import Any, Dict, List, Optional

_DF_COLUMNS = ["ts", "metric_id", "value", "labels", "kind", "signal"]

# Env được host inject vào os.environ (Session.setup) — không cần session manager.


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)


def _serve_url() -> Optional[str]:
    return _env("OPSENSE_SERVE_URL")


def _block_secs() -> int:
    return int(_env("OPSENSE_BLOCK_SECS", "604800") or 604800)


def _graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = _serve_url()
    if not url:
        raise RuntimeError(
            "no store backend: set OPSENSE_S3_BASE (parquet lakehouse) "
            "or OPSENSE_SERVE_URL (GraphQL fallback)"
        )
    endpoint = url.rstrip("/")
    if not endpoint.endswith("/graphql"):
        endpoint += "/graphql"
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        endpoint, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read())
    if payload.get("errors"):
        raise RuntimeError(f"graphql error: {payload['errors']}")
    return payload.get("data", {})


def _station_glob(station: str) -> Optional[str]:
    base = _env("OPSENSE_S3_BASE")
    if not base:
        return None
    # Time-partitioned lake: `ts/blk=<block_id>/batch-*.parquet` (Hive partition).
    return f"{base.rstrip('/')}/{station}/ts/**/*.parquet"


def _configure_s3(con) -> None:
    endpoint = _env("OPSENSE_S3_ENDPOINT")
    key = _env("OPSENSE_S3_ACCESS_KEY_ID")
    secret = _env("OPSENSE_S3_SECRET_ACCESS_KEY")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if endpoint and key and secret:
        region = _env("OPSENSE_S3_REGION", "us-east-1")
        token = _env("OPSENSE_S3_SESSION_TOKEN")
        sql = (
            f"CREATE OR REPLACE SECRET opsense_s3 (TYPE S3, ENDPOINT '{endpoint}', "
            f"REGION '{region}', KEY_ID '{key}', SECRET '{secret}'"
        )
        if token:
            sql += f", SESSION_TOKEN '{token}'"
        sql += ")"
        con.execute(sql)
        con.execute("SET s3_url_style 'path';")


def _query_parquet(
    station: str, metric: str, from_ts: int, to_ts: int
):
    """Đọc observation từ Parquet lakehouse qua DuckDB, filter theo block."""
    import duckdb  # lazy: chỉ import khi thật sự query

    glob = _station_glob(station)
    if not glob:
        return None

    bs = _block_secs()
    first, last = from_ts // bs, to_ts // bs
    # Chặn số block query một lần để tránh SQL khổng lồ.
    if last - first > 10_000:
        raise ValueError("time range quá lớn cho một query (hơn 10k block)")
    block_list = ", ".join(f"'blk:{b}'" for b in range(first, last + 1))

    metric_clause = "AND metric_id = ?" if metric else ""
    con = duckdb.connect()
    try:
        _configure_s3(con)
        sql = f"""
        WITH blocks AS (
            SELECT convert_from(value, 'utf8') AS blk
            FROM read_parquet('{glob}', union_by_name = true, hive_partitioning = true)
            WHERE blk BETWEEN {first} AND {last}
              AND decode(series) IN ({block_list})
        ), items AS (
            SELECT unnest(CAST(json_extract(blk, '$.items') AS JSON[])) AS j
            FROM blocks
        )
        SELECT
            CAST(json_extract_string(j, '$.ts') AS BIGINT)   AS ts,
            json_extract_string(j, '$.metric_id')            AS metric_id,
            CAST(json_extract_string(j, '$.value') AS DOUBLE) AS value,
            json_extract(j, '$.labels')                      AS labels,
            json_extract_string(j, '$.kind')                 AS kind,
            json_extract_string(j, '$.signal')               AS signal
        FROM items
        WHERE CAST(json_extract_string(j, '$.ts') AS BIGINT) > ? 
          AND CAST(json_extract_string(j, '$.ts') AS BIGINT) <= ?
          {metric_clause}
        ORDER BY ts ASC
        """
        params = [from_ts, to_ts] + ([metric] if metric else [])
        return con.execute(sql, params).df()
    finally:
        con.close()


def _query_graphql(station: str, metric: str, from_ts: int, to_ts: int):
    data = _graphql(
        "query($node: String!, $from: BigInt, $to: BigInt) {"
        " queryTimeseries(node: $node, fromTs: $from, toTs: $to) {"
        " ts metric_id value labels kind signal } }",
        {"node": station, "from": from_ts, "to": to_ts},
    )
    rows = data.get("queryTimeseries") or []
    if metric:
        rows = [r for r in rows if r.get("metric_id") == metric]
    return rows


def _to_dataframe(rows) -> "pd.DataFrame":
    import pandas as pd

    if not rows:
        return pd.DataFrame(columns=_DF_COLUMNS)
    df = pd.DataFrame(rows)
    for col in _DF_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[_DF_COLUMNS]


def query(
    station: str,
    stage: str = "processed",
    metric: str = "",
    from_ts: int = 0,
    to_ts: Optional[int] = None,
):
    """
    Query observations từ một station.

    Args:
        station: Station ID (vd "prom-explore")
        stage: "raw" hoặc "processed" (mặc định; hiện chỉ processed có dữ liệu)
        metric: Lọc theo metric_id ("" = tất cả)
        from_ts: Unix seconds (exclusive)
        to_ts: Unix seconds (inclusive); None = hiện tại

    Returns:
        pandas DataFrame: ts, metric_id, value, labels, kind, signal
    """
    if to_ts is None:
        to_ts = now()
    df = _query_parquet(station, metric, from_ts, to_ts)
    if df is None:
        df = _to_dataframe(_query_graphql(station, metric, from_ts, to_ts))
    return df


def query_all(
    station: str,
    stage: str = "processed",
    from_ts: int = 0,
    to_ts: Optional[int] = None,
):
    """Query tất cả metrics của station."""
    return query(station, stage, "", from_ts, to_ts)


def scan(station: str):
    """
    Lazy relation của station (chỉ ở chế độ Parquet lakehouse). Trả về
    DuckDB relation — caller tự `.filter()` / `.df()` khi cần; DuckDB chỉ
    đọc row groups thật sự cần.
    """
    import duckdb

    glob = _station_glob(station)
    if not glob:
        raise RuntimeError("scan() requires OPSENSE_S3_BASE (parquet lakehouse)")
    con = duckdb.connect()
    _configure_s3(con)
    rel = con.sql(
        f"SELECT decode(series) AS block_id, ts, value "
        f"FROM read_parquet('{glob}', union_by_name = true, hive_partitioning = true)"
    )
    return rel


def latest(station: str, stage: str = "processed", metric: str = "") -> Optional[float]:
    """Giá trị mới nhất của một metric (hoặc None)."""
    if not metric:
        raise ValueError("metric is required for latest()")
    df = query(station, stage, metric, 0, now())
    if df.empty:
        return None
    return float(df.iloc[-1]["value"])


def list_metrics(station: str, stage: str = "processed") -> List[str]:
    """Danh sách metric_id có trong station."""
    df = query(station, stage, "", 0, now())
    return sorted(df["metric_id"].dropna().unique().tolist())


def station_ids() -> List[str]:
    """Danh sách station đã đăng ký (qua serve GraphQL)."""
    data = _graphql("{ status { stations { id } } }")
    stations = (data.get("status") or {}).get("stations") or []
    return [s["id"] for s in stations]


# Time range helpers
def now() -> int:
    """Current Unix timestamp in seconds."""
    return int(time.time())


def parse_duration(s: str) -> int:
    """
    Parse duration string to seconds.

    Examples: "1h", "30m", "7d", "2w", "1h30m"
    """
    import re

    pattern = r"(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
    match = re.fullmatch(pattern, s.strip())
    if not match:
        raise ValueError(f"Invalid duration string: {s}")

    weeks, days, hours, minutes, seconds = match.groups()
    total = 0
    if weeks:
        total += int(weeks) * 86400 * 7
    if days:
        total += int(days) * 86400
    if hours:
        total += int(hours) * 3600
    if minutes:
        total += int(minutes) * 60
    if seconds:
        total += int(seconds)
    return total


def time_range(preset: str) -> tuple:
    """
    Get (from_ts, to_ts) for common presets.

    Presets: "1h", "6h", "24h", "7d", "30d", "1h_ago", etc.
    """
    to_ts = now()

    if preset.endswith("_ago"):
        duration = preset[:-4]
        from_ts = to_ts - parse_duration(duration)
    else:
        from_ts = to_ts - parse_duration(preset)

    return (from_ts, to_ts)
