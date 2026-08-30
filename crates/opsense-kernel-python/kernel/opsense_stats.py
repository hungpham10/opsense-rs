"""
opsense.stats - Statistical analysis functions.

Provides descriptive statistics, rolling statistics, distribution fitting,
time-series analysis, and forecasting.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from scipy import stats as sp_stats
from statsmodels.tsa.stattools import acf, adfuller, pacf
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA
import warnings
warnings.filterwarnings("ignore")


def describe(df: pd.DataFrame, percentiles: Optional[List[float]] = None) -> Dict[str, Any]:
    """
    Comprehensive descriptive statistics.
    
    Args:
        df: DataFrame with numeric columns
        percentiles: Custom percentiles (default: [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    
    Returns:
        Dictionary with statistics per column
    """
    if percentiles is None:
        percentiles = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.empty:
        return {}
    
    result = numeric_df.describe(percentiles=percentiles).to_dict()
    
    # Add extra statistics
    for col in numeric_df.columns:
        series = numeric_df[col].dropna()
        if len(series) > 0:
            result[col]["skewness"] = float(series.skew())
            result[col]["kurtosis"] = float(series.kurtosis())
            result[col]["mad"] = float((series - series.median()).abs().median())  # Median Absolute Deviation
            result[col]["cv"] = float(series.std() / series.mean()) if series.mean() != 0 else np.inf  # Coefficient of Variation
    
    return result


def rolling(
    df: pd.DataFrame,
    window: str,
    functions: List[str] = None,
    center: bool = False,
) -> pd.DataFrame:
    """
    Rolling window statistics.
    
    Args:
        df: DataFrame with DatetimeIndex
        window: Window size as duration string (e.g., "1h", "30m", "7d")
        functions: List of functions ["mean", "std", "min", "max", "median", "quantile", "skew", "kurt"]
        center: Whether to center the window
    
    Returns:
        DataFrame with rolling statistics (MultiIndex columns: (column, function))
    """
    if functions is None:
        functions = ["mean", "std", "min", "max"]
    
    # Parse window to pandas offset
    window_map = {
        "s": "s", "sec": "s", "second": "s",
        "m": "min", "min": "min", "minute": "min",
        "h": "h", "hour": "h",
        "d": "D", "day": "D",
        "w": "W", "week": "W",
    }
    
    import re
    match = re.match(r'(\d+)(\w+)', window)
    if not match:
        raise ValueError(f"Invalid window format: {window}")
    
    num, unit = match.groups()
    pandas_unit = window_map.get(unit, unit)
    pandas_window = f"{num}{pandas_unit}"
    
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.empty:
        return pd.DataFrame()
    
    result = numeric_df.rolling(pandas_window, center=center).agg(functions)
    return result


def quantile(series: pd.Series, q: Union[float, List[float]]) -> Union[float, Dict[float, float]]:
    """
    Compute quantile(s) of a series.
    
    Args:
        series: Input series
        q: Quantile (0-1) or list of quantiles
    
    Returns:
        Quantile value(s)
    """
    series = series.dropna()
    if isinstance(q, list):
        return {qq: float(series.quantile(qq)) for qq in q}
    return float(series.quantile(q))


def acf(series: pd.Series, nlags: int = 40, alpha: float = 0.05) -> Dict[str, Any]:
    """
    Autocorrelation function.
    
    Args:
        series: Time series
        nlags: Number of lags
        alpha: Confidence level
    
    Returns:
        Dict with 'acf', 'confint', 'nlags'
    """
    series = series.dropna()
    if len(series) < 2:
        return {"acf": [], "confint": [], "nlags": 0}
    
    nlags = min(nlags, len(series) - 1)
    acf_vals, confint = acf(series, nlags=nlags, alpha=alpha)
    
    return {
        "acf": acf_vals.tolist(),
        "confint": confint.tolist(),
        "nlags": nlags,
    }


def pacf(series: pd.Series, nlags: int = 40, alpha: float = 0.05) -> Dict[str, Any]:
    """Partial autocorrelation function."""
    series = series.dropna()
    if len(series) < 2:
        return {"pacf": [], "confint": [], "nlags": 0}
    
    nlags = min(nlags, len(series) - 1)
    pacf_vals, confint = pacf(series, nlags=nlags, alpha=alpha)
    
    return {
        "pacf": pacf_vals.tolist(),
        "confint": confint.tolist(),
        "nlags": nlags,
    }


def adf_test(series: pd.Series) -> Dict[str, Any]:
    """
    Augmented Dickey-Fuller test for stationarity.
    
    Returns:
        Dict with statistic, p-value, critical values, is_stationary
    """
    series = series.dropna()
    if len(series) < 10:
        return {"error": "Insufficient data for ADF test"}
    
    result = adfuller(series, autolag="AIC")
    
    return {
        "statistic": float(result[0]),
        "pvalue": float(result[1]),
        "critical_values": {k: float(v) for k, v in result[4].items()},
        "is_stationary": result[1] < 0.05,
        "nobs": int(result[3]),
    }


def fit_distribution(series: pd.Series, dist: str = "norm") -> Dict[str, Any]:
    """
    Fit a probability distribution to data.
    
    Args:
        series: Input data
        dist: Distribution name ("norm", "expon", "gamma", "beta", "weibull_min", "lognorm")
    
    Returns:
        Dict with parameters, KS test statistic, p-value
    """
    series = series.dropna()
    if len(series) < 3:
        return {"error": "Insufficient data"}
    
    dist_map = {
        "norm": sp_stats.norm,
        "expon": sp_stats.expon,
        "gamma": sp_stats.gamma,
        "beta": sp_stats.beta,
        "weibull": sp_stats.weibull_min,
        "lognorm": sp_stats.lognorm,
    }
    
    if dist not in dist_map:
        raise ValueError(f"Unknown distribution: {dist}")
    
    dist_obj = dist_map[dist]
    params = dist_obj.fit(series)
    
    # KS test
    ks_stat, p_value = sp_stats.kstest(series, dist, args=params)
    
    return {
        "distribution": dist,
        "params": [float(p) for p in params],
        "ks_statistic": float(ks_stat),
        "pvalue": float(p_value),
        "fits_well": p_value > 0.05,
    }


def seasonal_decompose(
    series: pd.Series,
    period: int,
    model: str = "additive",
) -> Dict[str, pd.Series]:
    """
    Seasonal decomposition using STL or classical method.
    
    Args:
        series: Time series with DatetimeIndex
        period: Seasonal period (number of observations per cycle)
        model: "additive" or "multiplicative"
    
    Returns:
        Dict with 'trend', 'seasonal', 'resid', 'observed'
    """
    series = series.dropna()
    if len(series) < 2 * period:
        raise ValueError(f"Series too short for period {period}")
    
    # Ensure regular frequency
    series = series.asfreq(pd.infer_freq(series.index) or "H")
    
    result = seasonal_decompose(series, model=model, period=period)
    
    return {
        "trend": result.trend,
        "seasonal": result.seasonal,
        "resid": result.resid,
        "observed": result.observed,
    }


def holt_winters(
    series: pd.Series,
    seasonal_periods: int = None,
    trend: str = "add",
    seasonal: str = "add",
    damped_trend: bool = False,
    forecast_steps: int = 0,
) -> Dict[str, Any]:
    """
    Holt-Winters exponential smoothing.
    
    Args:
        series: Time series
        seasonal_periods: Number of periods per season (auto if None)
        trend: "add", "mul", or None
        seasonal: "add", "mul", or None
        damped_trend: Whether to dampen trend
        forecast_steps: Number of steps to forecast
    
    Returns:
        Dict with 'fitted', 'forecast', 'params', 'aic', 'bic'
    """
    series = series.dropna()
    
    if seasonal_periods is None:
        # Try to infer from frequency
        freq = pd.infer_freq(series.index)
        if freq == "H":
            seasonal_periods = 24
        elif freq == "D":
            seasonal_periods = 7
        elif freq == "M":
            seasonal_periods = 12
        else:
            seasonal_periods = 24
    
    model = ExponentialSmoothing(
        series,
        trend=trend if trend else None,
        seasonal=seasonal if seasonal else None,
        seasonal_periods=seasonal_periods,
        damped_trend=damped_trend,
    )
    
    fitted = model.fit()
    
    result = {
        "fitted": fitted.fittedvalues,
        "params": {k: float(v) for k, v in fitted.params.items()},
        "aic": float(fitted.aic),
        "bic": float(fitted.bic),
    }
    
    if forecast_steps > 0:
        forecast = fitted.forecast(forecast_steps)
        result["forecast"] = forecast
        # Prediction intervals
        pred_int = fitted.get_prediction_interval(forecast_steps)
        result["forecast_lower"] = pred_int[:, 0]
        result["forecast_upper"] = pred_int[:, 1]
    
    return result


def arima_forecast(
    series: pd.Series,
    order: tuple = (1, 1, 1),
    seasonal_order: tuple = (0, 0, 0, 0),
    steps: int = 24,
) -> Dict[str, Any]:
    """
    ARIMA/SARIMA forecasting.
    
    Args:
        series: Time series
        order: (p, d, q) for ARIMA
        seasonal_order: (P, D, Q, s) for SARIMA
        steps: Forecast horizon
    
    Returns:
        Dict with 'forecast', 'conf_int', 'aic', 'bic', 'params'
    """
    series = series.dropna()
    
    model = ARIMA(series, order=order, seasonal_order=seasonal_order)
    fitted = model.fit()
    
    forecast_result = fitted.get_forecast(steps=steps)
    
    return {
        "forecast": forecast_result.predicted_mean,
        "conf_int": forecast_result.conf_int(),
        "params": {k: float(v) for k, v in fitted.params.items()},
        "aic": float(fitted.aic),
        "bic": float(fitted.bic),
    }


def auto_arima(
    series: pd.Series,
    max_p: int = 3,
    max_d: int = 2,
    max_q: int = 3,
    seasonal: bool = False,
    m: int = 24,
    steps: int = 24,
) -> Dict[str, Any]:
    """
    Auto ARIMA model selection (simplified - uses AIC).
    
    Note: For full auto_arima, consider pmdarima package.
    This is a basic grid search.
    """
    series = series.dropna()
    best_aic = np.inf
    best_order = None
    best_model = None
    
    for p in range(max_p + 1):
        for d in range(max_d + 1):
            for q in range(max_q + 1):
                if p == 0 and d == 0 and q == 0:
                    continue
                try:
                    model = ARIMA(series, order=(p, d, q))
                    fitted = model.fit()
                    if fitted.aic < best_aic:
                        best_aic = fitted.aic
                        best_order = (p, d, q)
                        best_model = fitted
                except Exception:
                    continue
    
    if best_model is None:
        return {"error": "No valid model found"}
    
    forecast_result = best_model.get_forecast(steps=steps)
    
    return {
        "order": best_order,
        "forecast": forecast_result.predicted_mean,
        "conf_int": forecast_result.conf_int(),
        "aic": float(best_aic),
        "params": {k: float(v) for k, v in best_model.params.items()},
    }