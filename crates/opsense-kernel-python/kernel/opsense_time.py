"""
opsense.time - Time utilities for analysis.

Provides time parsing, current time, and time range helpers.
"""

import time
import re
from datetime import datetime, timezone
from typing import Tuple, Optional


def now() -> int:
    """Current Unix timestamp in seconds (UTC)."""
    return int(time.time())


def now_ms() -> int:
    """Current Unix timestamp in milliseconds."""
    return int(time.time() * 1000)


def now_ns() -> int:
    """Current Unix timestamp in nanoseconds."""
    return int(time.time() * 1_000_000_000)


def utcnow() -> datetime:
    """Current UTC datetime."""
    return datetime.now(timezone.utc)


def parse_duration(s: str) -> int:
    """
    Parse duration string to seconds.
    
    Examples:
        "1h" -> 3600
        "30m" -> 1800
        "7d" -> 604800
        "2w" -> 1209600
        "1h30m" -> 5400
        "1d12h" -> 129600
    """
    s = s.strip().lower()
    if not s:
        raise ValueError("Empty duration string")
    
    pattern = r'(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
    match = re.fullmatch(pattern, s)
    if not match:
        raise ValueError(f"Invalid duration format: {s}")
    
    weeks, days, hours, minutes, seconds = match.groups()
    total = 0
    if weeks: total += int(weeks) * 7 * 86400
    if days: total += int(days) * 86400
    if hours: total += int(hours) * 3600
    if minutes: total += int(minutes) * 60
    if seconds: total += int(seconds)
    
    if total == 0:
        raise ValueError(f"Duration cannot be zero: {s}")
    
    return total


def parse_timestamp(s: str) -> int:
    """
    Parse timestamp string to Unix seconds.
    
    Formats:
        - Unix seconds: "1700000000"
        - ISO 8601: "2024-01-15T10:30:00Z", "2024-01-15"
        - Relative: "now", "now-1h", "1h ago"
    """
    s = s.strip()
    
    if s == "now":
        return now()
    
    # Relative: "1h ago" or "now-1h"
    if s.endswith(" ago"):
        duration = s[:-4].strip()
        return now() - parse_duration(duration)
    
    if s.startswith("now-"):
        duration = s[4:].strip()
        return now() - parse_duration(duration)
    
    if s.startswith("now+"):
        duration = s[4:].strip()
        return now() + parse_duration(duration)
    
    # Try Unix timestamp
    try:
        return int(s)
    except ValueError:
        pass
    
    # Try ISO 8601
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        pass
    
    raise ValueError(f"Cannot parse timestamp: {s}")


def format_timestamp(ts: int, fmt: str = "%Y-%m-%d %H:%M:%S UTC") -> str:
    """Format Unix timestamp as string."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime(fmt)


def time_range(preset: str) -> Tuple[int, int]:
    """
    Get (from_ts, to_ts) for common presets.
    
    Presets:
        "1h", "6h", "24h", "7d", "30d" - last N duration
        "1h_ago", "24h_ago" - same as above
        "today" - midnight UTC to now
        "yesterday" - previous day
        "this_week" - Monday to now
        "last_week" - previous week
        "this_month" - 1st to now
        "last_month" - previous month
    """
    to_ts = now()
    preset = preset.lower()
    
    if preset.endswith("_ago"):
        duration = preset[:-4]
        from_ts = to_ts - parse_duration(duration)
        return (from_ts, to_ts)
    
    # Direct duration
    try:
        duration = parse_duration(preset)
        from_ts = to_ts - duration
        return (from_ts, to_ts)
    except ValueError:
        pass
    
    # Named presets
    dt = datetime.fromtimestamp(to_ts, tz=timezone.utc)
    
    if preset == "today":
        from_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return (int(from_dt.timestamp()), to_ts)
    
    if preset == "yesterday":
        from_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        from_ts = int(from_dt.timestamp()) - 86400
        to_ts = int(from_dt.timestamp())
        return (from_ts, to_ts)
    
    if preset == "this_week":
        # Monday = 0
        days_since_monday = dt.weekday()
        from_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        from_ts = int(from_dt.timestamp()) - days_since_monday * 86400
        return (from_ts, to_ts)
    
    if preset == "last_week":
        days_since_monday = dt.weekday()
        from_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        this_monday = int(from_dt.timestamp()) - days_since_monday * 86400
        from_ts = this_monday - 7 * 86400
        return (from_ts, this_monday)
    
    if preset == "this_month":
        from_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return (int(from_dt.timestamp()), to_ts)
    
    if preset == "last_month":
        if dt.month == 1:
            from_dt = dt.replace(year=dt.year - 1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            from_dt = dt.replace(month=dt.month - 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        if dt.month == 1:
            to_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            to_dt = dt.replace(month=dt.month, day=1, hour=0, minute=0, second=0, microsecond=0)
        return (int(from_dt.timestamp()), int(to_dt.timestamp()))
    
    raise ValueError(f"Unknown time preset: {preset}")


def timestamp_to_iso(ts: int) -> str:
    """Convert Unix timestamp to ISO 8601 string."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat()


def iso_to_timestamp(s: str) -> int:
    """Convert ISO 8601 string to Unix timestamp."""
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())