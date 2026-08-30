"""
opsense.capacity - Capacity planning and resource analysis.

Provides functions for CPU/memory/disk/latency capacity analysis,
growth projections, and exhaustion probability.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from scipy import stats as sp_stats
from scipy.optimize import curve_fit
import warnings
warnings.filterwarnings("ignore")


def cpu_prob_exceed(
    series: pd.Series,
    threshold_pct: float = 80.0,
    window: str = "24h",
) -> Dict[str, Any]:
    """
    Probability that CPU exceeds threshold over a time window.
    
    Args:
        series: CPU usage series (percentage 0-100)
        threshold_pct: Threshold percentage
        window: Rolling window for probability calculation
    
    Returns:
        Dict with probability, threshold, window, recent_probability
    """
    series = series.dropna()
    if len(series) == 0:
        return {"error": "Empty series"}
    
    # Overall probability
    exceed = (series > threshold_pct).mean()
    
    # Rolling probability
    rolling_exceed = series.rolling(window).apply(
        lambda x: (x > threshold_pct).mean() if len(x) > 0 else 0
    )
    
    return {
        "probability": float(exceed),
        "threshold": float(threshold_pct),
        "window": window,
        "recent_probability": float(rolling_exceed.iloc[-1]) if len(rolling_exceed) > 0 else 0.0,
        "current_value": float(series.iloc[-1]) if len(series) > 0 else None,
        "is_exceeding": bool(series.iloc[-1] > threshold_pct) if len(series) > 0 else False,
    }


def cpu_peak_analysis(
    series: pd.Series,
    threshold_pct: float = 80.0,
) -> Dict[str, Any]:
    """
    Analyze CPU peaks - frequency, duration, distribution.
    
    Returns:
        Dict with peak statistics
    """
    series = series.dropna()
    if len(series) == 0:
        return {"error": "Empty series"}
    
    # Find peaks (exceedances)
    exceeding = series > threshold_pct
    
    # Group consecutive exceedances
    peak_groups = []
    in_peak = False
    peak_start = None
    peak_values = []
    
    for i, (ts, val) in enumerate(series.items()):
        if val > threshold_pct:
            if not in_peak:
                in_peak = True
                peak_start = ts
                peak_values = [val]
            else:
                peak_values.append(val)
        else:
            if in_peak:
                in_peak = False
                peak_groups.append({
                    "start": peak_start,
                    "end": ts,
                    "duration_sec": (ts - peak_start).total_seconds() if hasattr(ts, 'total_seconds') else 0,
                    "max_value": max(peak_values),
                    "mean_value": np.mean(peak_values),
                })
                peak_values = []
    
    # Handle case where series ends in a peak
    if in_peak and peak_values:
        peak_groups.append({
            "start": peak_start,
            "end": series.index[-1],
            "duration_sec": (series.index[-1] - peak_start).total_seconds() if hasattr(series.index[-1], 'total_seconds') else 0,
            "max_value": max(peak_values),
            "mean_value": np.mean(peak_values),
        })
    
    if not peak_groups:
        return {
            "peak_count": 0,
            "total_peak_duration": 0,
            "avg_peak_duration": 0,
            "max_peak_value": float(series.max()),
        }
    
    durations = [p["duration_sec"] for p in peak_groups]
    max_values = [p["max_value"] for p in peak_groups]
    
    return {
        "peak_count": len(peak_groups),
        "total_peak_duration_sec": float(np.sum(durations)),
        "avg_peak_duration_sec": float(np.mean(durations)),
        "max_peak_duration_sec": float(np.max(durations)),
        "max_peak_value": float(np.max(max_values)),
        "avg_peak_value": float(np.mean(max_values)),
        "peaks": peak_groups,
    }


def mem_growth_rate(
    series: pd.Series,
    method: str = "linear",
) -> Dict[str, Any]:
    """
    Memory growth rate analysis.
    
    Args:
        series: Memory usage series (bytes or percentage)
        method: "linear", "exponential", "polynomial"
    
    Returns:
        Dict with growth rate, projection, R-squared
    """
    series = series.dropna()
    if len(series) < 3:
        return {"error": "Insufficient data"}
    
    # Convert index to numeric (seconds since start)
    x = np.arange(len(series)).astype(float)
    y = series.values.astype(float)
    
    if method == "linear":
        slope, intercept, r_value, p_value, std_err = sp_stats.linregress(x, y)
        return {
            "method": "linear",
            "growth_per_step": float(slope),
            "growth_per_day": float(slope * 86400 / np.mean(np.diff(x))) if len(x) > 1 and np.mean(np.diff(x)) > 0 else float(slope * 86400),
            "intercept": float(intercept),
            "r_squared": float(r_value ** 2),
            "p_value": float(p_value),
            "current_value": float(y[-1]),
        }
    
    elif method == "exponential":
        # Fit log(y) = a*x + b
        log_y = np.log(y + 1e-10)
        slope, intercept, r_value, p_value, std_err = sp_stats.linregress(x, log_y)
        growth_rate = np.exp(slope) - 1  # per step
        return {
            "method": "exponential",
            "growth_rate_per_step": float(growth_rate),
            "growth_rate_per_day": float(growth_rate * 86400 / np.mean(np.diff(x))) if len(x) > 1 else float(growth_rate * 86400),
            "r_squared": float(r_value ** 2),
            "p_value": float(p_value),
            "current_value": float(y[-1]),
        }
    
    elif method == "polynomial":
        # Quadratic fit
        coeffs = np.polyfit(x, y, 2)
        poly = np.poly1d(coeffs)
        y_pred = poly(x)
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        
        return {
            "method": "polynomial",
            "coeffs": [float(c) for c in coeffs],
            "r_squared": float(r_squared),
            "current_value": float(y[-1]),
            "instantaneous_growth": float(2 * coeffs[0] * x[-1] + coeffs[1]),
        }
    
    else:
        raise ValueError(f"Unknown method: {method}")


def mem_time_to_exhaustion(
    series: pd.Series,
    capacity: float,
    method: str = "linear",
    confidence: float = 0.95,
) -> Dict[str, Any]:
    """
    Projected time until memory hits capacity.
    
    Args:
        series: Memory usage series
        capacity: Total capacity (same units as series)
        method: Growth model ("linear", "exponential")
        confidence: Confidence level for prediction interval
    
    Returns:
        Dict with time_to_exhaustion, confidence_interval, current_usage
    """
    growth = mem_growth_rate(series, method)
    
    if "error" in growth:
        return growth
    
    current = growth["current_value"]
    remaining = capacity - current
    
    if remaining <= 0:
        return {
            "time_to_exhaustion_sec": 0,
            "time_to_exhaustion_days": 0,
            "status": "ALREADY_EXCEEDED",
            "current_usage": float(current),
            "capacity": float(capacity),
        }
    
    if method == "linear":
        growth_per_sec = growth["growth_per_step"] / np.mean(np.diff(np.arange(len(series)))) if len(series) > 1 else growth["growth_per_day"] / 86400
        
        if growth_per_sec <= 0:
            return {
                "time_to_exhaustion_sec": float('inf'),
                "time_to_exhaustion_days": float('inf'),
                "status": "NOT_GROWING",
                "current_usage": float(current),
                "capacity": float(capacity),
            }
        
        tte_sec = remaining / growth_per_sec
        
        # Uncertainty (simplified)
        return {
            "time_to_exhaustion_sec": float(tte_sec),
            "time_to_exhaustion_days": float(tte_sec / 86400),
            "status": "PROJECTED",
            "current_usage": float(current),
            "capacity": float(capacity),
            "growth_per_day": float(growth.get("growth_per_day", 0)),
        }
    
    elif method == "exponential":
        growth_rate = growth["growth_rate_per_step"]
        if growth_rate <= 0:
            return {
                "time_to_exhaustion_sec": float('inf'),
                "status": "NOT_GROWING",
            }
        
        # Solve: current * (1 + r)^t = capacity
        # t = log(capacity/current) / log(1+r)
        tte_steps = np.log(capacity / current) / np.log(1 + growth_rate)
        
        # Convert steps to seconds
        step_sec = np.mean(np.diff(np.arange(len(series)))) if len(series) > 1 else 1
        tte_sec = tte_steps * step_sec
        
        return {
            "time_to_exhaustion_sec": float(tte_sec),
            "time_to_exhaustion_days": float(tte_sec / 86400),
            "status": "PROJECTED",
            "current_usage": float(current),
            "capacity": float(capacity),
            "growth_rate_per_day": float(growth.get("growth_rate_per_day", 0)),
        }
    
    return {"error": f"Unknown method: {method}"}


def disk_projection(
    series: pd.Series,
    horizon_days: int = 30,
    method: str = "linear",
) -> Dict[str, Any]:
    """
    Disk usage projection.
    
    Args:
        series: Disk usage series
        horizon_days: Projection horizon
        method: "linear", "exponential", "quadratic"
    
    Returns:
        Dict with projected values, growth rate
    """
    growth = mem_growth_rate(series, method)
    
    if "error" in growth:
        return growth
    
    current = growth["current_value"]
    
    if method == "linear":
        growth_per_day = growth.get("growth_per_day", 0)
        projected = current + growth_per_day * horizon_days
        
        return {
            "current_usage": float(current),
            "projected_usage": float(projected),
            "horizon_days": horizon_days,
            "growth_per_day": float(growth_per_day),
            "method": "linear",
        }
    
    elif method == "exponential":
        growth_rate_per_day = growth.get("growth_rate_per_day", 0)
        projected = current * (1 + growth_rate_per_day) ** horizon_days
        
        return {
            "current_usage": float(current),
            "projected_usage": float(projected),
            "horizon_days": horizon_days,
            "growth_rate_per_day": float(growth_rate_per_day),
            "method": "exponential",
        }
    
    elif method == "quadratic":
        coeffs = growth["coeffs"]
        # Project using polynomial
        last_x = len(series) - 1
        future_x = last_x + horizon_days * 86400 / np.mean(np.diff(np.arange(len(series)))) if len(series) > 1 else last_x + horizon_days
        projected = coeffs[0] * future_x**2 + coeffs[1] * future_x + coeffs[2]
        
        return {
            "current_usage": float(current),
            "projected_usage": float(projected),
            "horizon_days": horizon_days,
            "method": "quadratic",
            "coeffs": [float(c) for c in coeffs],
        }
    
    return {"error": f"Unknown method: {method}"}


def latency_percentiles(
    series: pd.Series,
    percentiles: List[float] = None,
) -> Dict[str, Any]:
    """
    Latency percentile analysis.
    
    Args:
        series: Latency values (ms or seconds)
        percentiles: List of percentiles to compute
    
    Returns:
        Dict with percentiles, statistics
    """
    if percentiles is None:
        percentiles = [50, 90, 95, 99, 99.9, 99.99]
    
    series = series.dropna()
    if len(series) == 0:
        return {"error": "Empty series"}
    
    values = np.percentile(series, percentiles)
    
    result = {
        f"p{p}": float(v) for p, v in zip(percentiles, values)
    }
    
    result.update({
        "mean": float(series.mean()),
        "std": float(series.std()),
        "min": float(series.min()),
        "max": float(series.max()),
        "count": int(len(series)),
    })
    
    return result


def latency_tail_analysis(
    series: pd.Series,
    threshold_percentile: float = 99.0,
) -> Dict[str, Any]:
    """
    Analyze latency tail behavior.
    
    Returns:
        Dict with tail statistics, Pareto fit
    """
    series = series.dropna()
    if len(series) == 0:
        return {"error": "Empty series"}
    
    threshold = np.percentile(series, threshold_percentile)
    tail = series[series > threshold]
    
    # Fit Pareto distribution to tail
    from scipy.stats import pareto
    
    if len(tail) > 10:
        try:
            # Shift to positive
            tail_shifted = tail - tail.min() + 1
            b, loc, scale = pareto.fit(tail_shifted)
            pareto_params = {"b": float(b), "loc": float(loc), "scale": float(scale)}
        except Exception:
            pareto_params = None
    else:
        pareto_params = None
    
    return {
        "threshold_percentile": threshold_percentile,
        "threshold_value": float(threshold),
        "tail_count": int(len(tail)),
        "tail_percentage": float(len(tail) / len(series) * 100),
        "tail_mean": float(tail.mean()) if len(tail) > 0 else None,
        "tail_max": float(tail.max()) if len(tail) > 0 else None,
        "pareto_fit": pareto_params,
    }


def deployment_comparison(
    series_dict: Dict[str, pd.Series],
    percentiles: List[float] = None,
) -> pd.DataFrame:
    """
    Compare latency distributions across deployments.
    
    Args:
        series_dict: {deployment_name: latency_series}
        percentiles: Percentiles to compare
    
    Returns:
        DataFrame with deployments as rows, percentiles as columns
    """
    if percentiles is None:
        percentiles = [50, 90, 95, 99, 99.9]
    
    results = {}
    for name, series in series_dict.items():
        series = series.dropna()
        if len(series) > 0:
            results[name] = {f"p{p}": float(np.percentile(series, p)) for p in percentiles}
            results[name].update({
                "mean": float(series.mean()),
                "std": float(series.std()),
                "count": int(len(series)),
            })
    
    return pd.DataFrame(results).T