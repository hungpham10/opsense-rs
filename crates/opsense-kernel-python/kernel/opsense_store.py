"""
opsense.store - Query observations from Opsense stations.

This module provides the Python interface to query data from Opsense stores.
All functions return pandas DataFrames with a DatetimeIndex (UTC).
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
import pyarrow as pa

# These will be injected by the Rust host at runtime
_session_manager = None
_current_session_id = None


def _get_store():
    """Get the store from the current session."""
    global _session_manager, _current_session_id
    if _session_manager is None:
        raise RuntimeError("Session manager not initialized")
    if _current_session_id is None:
        raise RuntimeError("No active session")
    
    session = _session_manager.get_session(_current_session_id)
    if session is None:
        raise RuntimeError(f"Session {_current_session_id} not found")
    
    return session.store


def _resolve_station(station: Optional[str]) -> str:
    """Resolve station name, using current station if not specified."""
    if station is not None:
        return station
    global _session_manager, _current_session_id
    session = _session_manager.get_session(_current_session_id)
    if session and session.state.current_station:
        return session.state.current_station
    raise ValueError("No station specified and no current station set")


def query(
    station: str,
    stage: str = "processed",
    metric: str = "",
    from_ts: int = 0,
    to_ts: int = 2**63 - 1,
) -> pd.DataFrame:
    """
    Query observations from a station.
    
    Args:
        station: Station ID (e.g., "tsdb")
        stage: "raw" or "processed" (default: "processed")
        metric: Metric ID to filter (empty = all metrics)
        from_ts: Start timestamp (Unix seconds, exclusive)
        to_ts: End timestamp (Unix seconds, inclusive)
    
    Returns:
        DataFrame with columns: ts, metric_id, value, labels (JSON), kind, signal
        Index: DatetimeIndex (UTC)
    """
    store = _get_store()
    station_id = _resolve_station(station)
    
    # Call Rust store query
    if metric:
        observations = store.query(stage, metric, from_ts, to_ts)
    else:
        observations = store.query_all(stage, from_ts, to_ts)
    
    # Convert to DataFrame
    if not observations:
        return pd.DataFrame(
            columns=["ts", "metric_id", "value", "labels", "kind", "signal"]
        ).set_index(pd.DatetimeIndex([], tz="UTC", name="ts"))
    
    data = []
    for obs in observations:
        data.append({
            "ts": pd.Timestamp(obs.ts, unit="s", tz="UTC"),
            "metric_id": obs.metric_id,
            "value": obs.value,
            "labels": obs.labels,
            "kind": obs.kind,
            "signal": obs.signal,
        })
    
    df = pd.DataFrame(data)
    df = df.set_index("ts")
    df.index.name = "ts"
    return df


def query_all(
    station: str,
    stage: str = "processed",
    from_ts: int = 0,
    to_ts: int = 2**63 - 1,
) -> pd.DataFrame:
    """
    Query all observations from a station (all metrics).
    
    Args:
        station: Station ID
        stage: "raw" or "processed"
        from_ts: Start timestamp (Unix seconds)
        to_ts: End timestamp (Unix seconds)
    
    Returns:
        DataFrame with all metrics
    """
    return query(station, stage, "", from_ts, to_ts)


def latest(
    station: str,
    stage: str = "processed",
    metric: str = "",
) -> Optional[float]:
    """
    Get the latest value for a metric.
    
    Args:
        station: Station ID
        stage: "raw" or "processed"
        metric: Metric ID
    
    Returns:
        Latest value or None if no data
    """
    store = _get_store()
    station_id = _resolve_station(station)
    
    if not metric:
        raise ValueError("metric is required for latest()")
    
    obs_list = store.query(stage, metric, 0, 2**63 - 1)
    if not obs_list:
        return None
    
    return obs_list[-1].value


def list_metrics(station: str, stage: str = "processed") -> List[str]:
    """
    List all metric IDs in a station.
    
    Args:
        station: Station ID
        stage: "raw" or "processed"
    
    Returns:
        List of metric IDs
    """
    store = _get_store()
    station_id = _resolve_station(station)
    
    # Query all and extract unique metrics
    observations = store.query_all(stage, 0, 2**63 - 1)
    metrics = sorted(set(obs.metric_id for obs in observations))
    return metrics


# Time range helpers
def now() -> int:
    """Current Unix timestamp in seconds."""
    import time
    return int(time.time())


def parse_duration(s: str) -> int:
    """
    Parse duration string to seconds.
    
    Examples: "1h", "30m", "7d", "2w", "1h30m"
    """
    import re
    pattern = r'(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
    match = re.fullmatch(pattern, s.strip())
    if not match:
        raise ValueError(f"Invalid duration: {s}")
    
    weeks, days, hours, minutes, seconds = match.groups()
    total = 0
    if weeks: total += int(weeks) * 7 * 86400
    if days: total += int(days) * 86400
    if hours: total += int(hours) * 3600
    if minutes: total += int(minutes) * 60
    if seconds: total += int(seconds)
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