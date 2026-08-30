"""
opsense.station - Station management and metadata.

Provides functions to list, describe, and manage stations.
"""

from typing import Optional, List, Dict, Any
import pandas as pd

# Injected by Rust host
_session_manager = None
_current_session_id = None


def _get_session():
    """Get current session state."""
    global _session_manager, _current_session_id
    if _session_manager is None:
        raise RuntimeError("Session manager not initialized")
    session = _session_manager.get_session(_current_session_id)
    if session is None:
        raise RuntimeError(f"Session {_current_session_id} not found")
    return session


def list() -> List[str]:
    """
    List all registered station IDs.
    
    Returns:
        List of station IDs
    """
    session = _get_session()
    # Get from store registry
    from opsense_store import station_ids
    return station_ids()


def describe(station: str) -> Dict[str, Any]:
    """
    Get detailed metadata for a station.
    
    Args:
        station: Station ID
    
    Returns:
        Dictionary with station metadata:
        - id, backend, schema_version, params, metrics, dependencies
        - time_range: (min_ts, max_ts)
        - record_count, block_count
    """
    session = _get_session()
    from opsense_store import describe_station
    
    desc = describe_station(station)
    if desc is None:
        raise ValueError(f"Station '{station}' not found")
    
    return desc


def use(station: str) -> str:
    """
    Set the current station for the session.
    
    Args:
        station: Station ID
    
    Returns:
        The station ID that was set
    """
    session = _get_session()
    # Validate station exists
    stations = list()
    if station not in stations:
        raise ValueError(f"Station '{station}' not found. Available: {stations}")
    
    session.state.current_station = station
    return station


def current() -> Optional[str]:
    """Get the current station ID."""
    session = _get_session()
    return session.state.current_station


def invalidate(
    station: str,
    stage: str = "processed",
    from_ts: int = 0,
) -> bool:
    """
    Invalidate data from a timestamp onwards (hot tier only).
    
    Args:
        station: Station ID
        stage: "raw" or "processed"
        from_ts: Timestamp to invalidate from
    
    Returns:
        True if invalidation was performed
    """
    session = _get_session()
    from opsense_store import station as get_station, Stage
    
    store = get_station(station)
    if store is None:
        raise ValueError(f"Station '{station}' not found")
    
    stage_enum = Stage.Raw if stage == "raw" else Stage.Processed
    try:
        store.invalidate_from(stage_enum, from_ts)
        return True
    except Exception as e:
        print(f"Invalidation failed: {e}")
        return False


def get_backend(station: str) -> str:
    """Get the backend type for a station."""
    desc = describe(station)
    return desc.get("backend", "unknown")


def get_time_range(station: str, stage: str = "processed") -> tuple:
    """
    Get the time range of data in a station.
    
    Returns:
        (min_ts, max_ts) or (None, None) if empty
    """
    session = _get_session()
    from opsense_store import station as get_station, Stage
    
    store = get_station(station)
    if store is None:
        return (None, None)
    
    stage_enum = Stage.Raw if stage == "raw" else Stage.Processed
    latest = store.latest_ts(stage_enum)
    
    # For min, we'd need to query - simplified for now
    return (0, latest) if latest > 0 else (None, None)


def get_record_count(station: str, stage: str = "processed") -> int:
    """Get approximate record count."""
    # Would need store-specific implementation
    return 0