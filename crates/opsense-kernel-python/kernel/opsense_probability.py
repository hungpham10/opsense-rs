"""
opsense.probability - Probability and threshold analysis.

Provides functions for exceedance probabilities, confidence intervals,
conditional probabilities, and risk assessments.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from scipy import stats as sp_stats
import warnings
warnings.filterwarnings("ignore")


def ecdf(series: pd.Series) -> Tuple[np.ndarray, np.ndarray]:
    """
    Empirical Cumulative Distribution Function.
    
    Returns:
        (x_values, y_values) where y = P(X <= x)
    """
    series = series.dropna().sort_values()
    n = len(series)
    x = series.values
    y = np.arange(1, n + 1) / n
    return x, y


def exceedance_probability(series: pd.Series, threshold: float) -> Dict[str, Any]:
    """
    Probability that value exceeds threshold.
    
    Args:
        series: Input data
        threshold: Threshold value
    
    Returns:
        Dict with probability, count, threshold
    """
    series = series.dropna()
    if len(series) == 0:
        return {"probability": 0.0, "count": 0, "threshold": threshold}
    
    exceed_count = (series > threshold).sum()
    prob = exceed_count / len(series)
    
    return {
        "probability": float(prob),
        "exceed_count": int(exceed_count),
        "total_count": int(len(series)),
        "threshold": float(threshold),
    }


def threshold_exceedance(
    series: pd.Series,
    thresholds: List[float],
) -> Dict[float, Dict[str, Any]]:
    """
    Exceedance probabilities for multiple thresholds.
    
    Args:
        series: Input data
        thresholds: List of threshold values
    
    Returns:
        Dict mapping threshold -> {probability, count, ...}
    """
    return {t: exceedance_probability(series, t) for t in thresholds}


def confidence_interval(
    series: pd.Series,
    confidence: float = 0.95,
    method: str = "bootstrap",
) -> Dict[str, float]:
    """
    Confidence interval for the mean.
    
    Args:
        series: Input data
        confidence: Confidence level (0-1)
        method: "bootstrap", "t", "normal", "percentile"
    
    Returns:
        Dict with lower, upper, mean, confidence
    """
    series = series.dropna()
    if len(series) < 2:
        return {"lower": np.nan, "upper": np.nan, "mean": np.nan}
    
    mean = series.mean()
    n = len(series)
    
    if method == "t":
        # Student's t-distribution
        se = series.std(ddof=1) / np.sqrt(n)
        alpha = (1 - confidence) / 2
        t_val = sp_stats.t.ppf(1 - alpha, n - 1)
        margin = t_val * se
    
    elif method == "normal":
        # Normal approximation
        se = series.std(ddof=1) / np.sqrt(n)
        alpha = (1 - confidence) / 2
        z_val = sp_stats.norm.ppf(1 - alpha)
        margin = z_val * se
    
    elif method == "percentile":
        # Percentile bootstrap (non-parametric)
        lower_p = (1 - confidence) / 2 * 100
        upper_p = (1 + confidence) / 2 * 100
        lower = np.percentile(series, lower_p)
        upper = np.percentile(series, upper_p)
        return {"lower": float(lower), "upper": float(upper), "mean": float(mean), "confidence": confidence}
    
    elif method == "bootstrap":
        # Bootstrap confidence interval
        n_bootstrap = 10000
        boot_means = []
        for _ in range(n_bootstrap):
            sample = series.sample(n=n, replace=True)
            boot_means.append(sample.mean())
        lower_p = (1 - confidence) / 2 * 100
        upper_p = (1 + confidence) / 2 * 100
        lower = np.percentile(boot_means, lower_p)
        upper = np.percentile(boot_means, upper_p)
        return {"lower": float(lower), "upper": float(upper), "mean": float(mean), "confidence": confidence}
    
    else:
        raise ValueError(f"Unknown method: {method}")
    
    return {
        "lower": float(mean - margin),
        "upper": float(mean + margin),
        "mean": float(mean),
        "confidence": confidence,
        "method": method,
    }


def conditional_probability(
    series: pd.Series,
    condition: pd.Series,
    event: pd.Series,
) -> Dict[str, float]:
    """
    Conditional probability P(event | condition).
    
    Args:
        series: Full data (for reference)
        condition: Boolean series for condition
        event: Boolean series for event
    
    Returns:
        Dict with conditional_prob, joint_prob, condition_prob
    """
    if len(condition) != len(event):
        raise ValueError("Condition and event must have same length")
    
    condition = condition.astype(bool)
    event = event.astype(bool)
    
    condition_count = condition.sum()
    if condition_count == 0:
        return {"conditional_prob": 0.0, "joint_prob": 0.0, "condition_prob": 0.0}
    
    joint = (condition & event).sum()
    cond_prob = joint / condition_count
    joint_prob = joint / len(condition)
    condition_prob = condition_count / len(condition)
    
    return {
        "conditional_prob": float(cond_prob),
        "joint_prob": float(joint_prob),
        "condition_prob": float(condition_prob),
    }


def tail_probability(
    series: pd.Series,
    quantile: float = 0.95,
) -> Dict[str, Any]:
    """
    Probability of exceeding a given quantile (tail risk).
    
    Args:
        series: Input data
        quantile: Quantile threshold (e.g., 0.95 for 95th percentile)
    
    Returns:
        Dict with threshold, tail_prob, expected_shortfall
    """
    series = series.dropna()
    if len(series) == 0:
        return {"error": "Empty series"}
    
    threshold = series.quantile(quantile)
    tail = series[series > threshold]
    
    return {
        "threshold": float(threshold),
        "quantile": quantile,
        "tail_probability": float(len(tail) / len(series)),
        "expected_shortfall": float(tail.mean()) if len(tail) > 0 else float(threshold),
        "tail_count": int(len(tail)),
    }


def value_at_risk(
    series: pd.Series,
    confidence: float = 0.95,
) -> Dict[str, float]:
    """
    Value at Risk (VaR) - threshold below which losses occur with given probability.
    
    Args:
        series: Returns/losses (negative = loss)
        confidence: Confidence level
    
    Returns:
        Dict with var, confidence
    """
    series = series.dropna()
    if len(series) == 0:
        return {"var": np.nan, "confidence": confidence}
    
    var = np.percentile(series, (1 - confidence) * 100)
    return {"var": float(var), "confidence": confidence}


def expected_shortfall(
    series: pd.Series,
    confidence: float = 0.95,
) -> Dict[str, float]:
    """
    Expected Shortfall (CVaR) - average loss beyond VaR.
    
    Args:
        series: Returns/losses
        confidence: Confidence level
    
    Returns:
        Dict with expected_shortfall, confidence, var
    """
    series = series.dropna()
    if len(series) == 0:
        return {"expected_shortfall": np.nan, "confidence": confidence, "var": np.nan}
    
    var = np.percentile(series, (1 - confidence) * 100)
    tail = series[series <= var]
    es = tail.mean() if len(tail) > 0 else var
    
    return {
        "expected_shortfall": float(es),
        "var": float(var),
        "confidence": confidence,
        "tail_count": int(len(tail)),
    }


def probability_density(
    series: pd.Series,
    bins: int = 50,
) -> Dict[str, Any]:
    """
    Probability density estimation (histogram + KDE).
    
    Returns:
        Dict with histogram bins, counts, density, kde_x, kde_y
    """
    series = series.dropna()
    if len(series) == 0:
        return {"error": "Empty series"}
    
    # Histogram
    counts, bin_edges = np.histogram(series, bins=bins, density=True)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    
    # KDE
    from scipy.stats import gaussian_kde
    kde = gaussian_kde(series)
    x_range = np.linspace(series.min(), series.max(), 200)
    kde_y = kde(x_range)
    
    return {
        "bin_centers": bin_centers.tolist(),
        "density": counts.tolist(),
        "bin_edges": bin_edges.tolist(),
        "kde_x": x_range.tolist(),
        "kde_y": kde_y.tolist(),
    }


def joint_probability(
    series1: pd.Series,
    series2: pd.Series,
    bins: int = 20,
) -> Dict[str, Any]:
    """
    2D joint probability (heatmap data).
    
    Returns:
        Dict with x_edges, y_edges, density_2d
    """
    df = pd.DataFrame({"x": series1, "y": series2}).dropna()
    if len(df) == 0:
        return {"error": "Empty data"}
    
    H, xedges, yedges = np.histogram2d(df["x"], df["y"], bins=bins, density=True)
    
    return {
        "x_edges": xedges.tolist(),
        "y_edges": yedges.tolist(),
        "density": H.T.tolist(),  # Transpose for typical orientation
        "x_centers": ((xedges[:-1] + xedges[1:]) / 2).tolist(),
        "y_centers": ((yedges[:-1] + yedges[1:]) / 2).tolist(),
    }