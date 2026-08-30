"""
opsense.plots - Visualization functions.

Generates plots as bytes (PNG/SVG/HTML) for REPL display and export.
Uses matplotlib with non-interactive backend.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import io
import base64
import warnings
warnings.filterwarnings("ignore")

# Set style
plt.style.use("seaborn-v0_8-whitegrid")
sns.set_palette("husl")


def _save_figure(fmt: str = "png") -> bytes:
    """Save current figure to bytes."""
    buf = io.BytesIO()
    plt.savefig(buf, format=fmt, dpi=150, bbox_inches="tight")
    plt.close()
    return buf.getvalue()


def _to_base64(data: bytes, fmt: str) -> str:
    """Convert bytes to base64 data URL."""
    b64 = base64.b64encode(data).decode("ascii")
    mime = {"png": "image/png", "svg": "image/svg+xml", "html": "text/html"}.get(fmt, "application/octet-stream")
    return f"data:{mime};base64,{b64}"


def plot_line(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    title: str = "",
    xlabel: str = "Time",
    ylabel: str = "Value",
    figsize: tuple = (12, 6),
    fmt: str = "png",
) -> bytes:
    """
    Plot time series line chart.
    
    Args:
        df: DataFrame with DatetimeIndex
        columns: Columns to plot (default: all numeric)
        title: Plot title
        xlabel: X-axis label
        ylabel: Y-axis label
        figsize: Figure size
        fmt: Output format (png, svg)
    
    Returns:
        Image bytes
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    fig, ax = plt.subplots(figsize=figsize)
    
    for col in columns:
        if col in df.columns:
            ax.plot(df.index, df[col], label=col, linewidth=1)
    
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M\n%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate()
    
    return _save_figure(fmt)


def plot_hist(
    df: pd.DataFrame,
    column: str,
    bins: int = 50,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "Frequency",
    figsize: tuple = (10, 6),
    fmt: str = "png",
    kde: bool = True,
) -> bytes:
    """
    Plot histogram with optional KDE.
    
    Args:
        df: DataFrame
        column: Column to plot
        bins: Number of bins
        title: Plot title
        xlabel: X-axis label (default: column name)
        ylabel: Y-axis label
        figsize: Figure size
        fmt: Output format
        kde: Whether to overlay KDE
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found")
    
    data = df[column].dropna()
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # Histogram
    n, bins_edges, patches = ax.hist(data, bins=bins, density=True, alpha=0.7, edgecolor="white", linewidth=0.5)
    
    # KDE
    if kde:
        from scipy.stats import gaussian_kde
        kde_obj = gaussian_kde(data)
        x_range = np.linspace(data.min(), data.max(), 200)
        ax.plot(x_range, kde_obj(x_range), "r-", linewidth=2, label="KDE")
    
    ax.set_title(title or f"Distribution of {column}")
    ax.set_xlabel(xlabel or column)
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    return _save_figure(fmt)


def plot_dist(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    title: str = "",
    figsize: tuple = (12, 6),
    fmt: str = "png",
) -> bytes:
    """
    Plot multiple distributions (KDE) overlaid.
    
    Args:
        df: DataFrame
        columns: Columns to plot (default: all numeric)
        title: Plot title
        figsize: Figure size
        fmt: Output format
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    fig, ax = plt.subplots(figsize=figsize)
    
    from scipy.stats import gaussian_kde
    
    for col in columns:
        if col in df.columns:
            data = df[col].dropna()
            if len(data) > 1:
                kde_obj = gaussian_kde(data)
                x_range = np.linspace(data.min(), data.max(), 200)
                ax.plot(x_range, kde_obj(x_range), label=col, linewidth=2)
    
    ax.set_title(title or "Distribution Comparison")
    ax.set_xlabel("Value")
    ax.set_ylabel("Density")
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    return _save_figure(fmt)


def plot_scatter(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: Optional[str] = None,
    size: Optional[str] = None,
    title: str = "",
    figsize: tuple = (10, 8),
    fmt: str = "png",
    alpha: float = 0.6,
) -> bytes:
    """
    Scatter plot with optional color/size encoding.
    
    Args:
        df: DataFrame
        x: X column
        y: Y column
        color: Column for color encoding
        size: Column for size encoding
        title: Plot title
        figsize: Figure size
        fmt: Output format
        alpha: Point transparency
    """
    if x not in df.columns or y not in df.columns:
        raise ValueError(f"Columns '{x}' or '{y}' not found")
    
    fig, ax = plt.subplots(figsize=figsize)
    
    plot_data = df[[x, y]].dropna()
    
    if color and color in df.columns:
        c = df.loc[plot_data.index, color]
        scatter = ax.scatter(plot_data[x], plot_data[y], c=c, alpha=alpha, cmap="viridis")
        plt.colorbar(scatter, ax=ax, label=color)
    elif size and size in df.columns:
        s = df.loc[plot_data.index, size]
        # Normalize sizes
        s_norm = (s - s.min()) / (s.max() - s.min()) * 100 + 10
        ax.scatter(plot_data[x], plot_data[y], s=s_norm, alpha=alpha)
    else:
        ax.scatter(plot_data[x], plot_data[y], alpha=alpha)
    
    ax.set_title(title or f"{y} vs {x}")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.grid(True, alpha=0.3)
    
    return _save_figure(fmt)


def plot_corr(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    method: str = "pearson",
    title: str = "",
    figsize: tuple = (10, 8),
    fmt: str = "png",
    cmap: str = "RdBu_r",
    annot: bool = True,
) -> bytes:
    """
    Correlation heatmap.
    
    Args:
        df: DataFrame
        columns: Columns to include (default: all numeric)
        method: Correlation method (pearson, spearman, kendall)
        title: Plot title
        figsize: Figure size
        fmt: Output format
        cmap: Colormap
        annot: Whether to annotate cells
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    corr_matrix = df[columns].corr(method=method)
    
    fig, ax = plt.subplots(figsize=figsize)
    
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    
    sns.heatmap(
        corr_matrix,
        mask=mask,
        annot=annot,
        fmt=".2f",
        cmap=cmap,
        center=0,
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.8},
        ax=ax,
    )
    
    ax.set_title(title or f"Correlation Matrix ({method})")
    
    return _save_figure(fmt)


def plot_residual(
    df: pd.DataFrame,
    actual: str,
    predicted: str,
    title: str = "",
    figsize: tuple = (12, 5),
    fmt: str = "png",
) -> bytes:
    """
    Residual analysis plots (residuals vs fitted, Q-Q plot).
    
    Args:
        df: DataFrame
        actual: Actual values column
        predicted: Predicted values column
        title: Plot title
        figsize: Figure size
        fmt: Output format
    """
    if actual not in df.columns or predicted not in df.columns:
        raise ValueError("Columns not found")
    
    y_true = df[actual].dropna()
    y_pred = df.loc[y_true.index, predicted]
    
    residuals = y_true - y_pred
    
    fig, axes = plt.subplots(1, 2, figsize=figsize)
    
    # Residuals vs Fitted
    axes[0].scatter(y_pred, residuals, alpha=0.6)
    axes[0].axhline(y=0, color="r", linestyle="--")
    axes[0].set_xlabel("Fitted Values")
    axes[0].set_ylabel("Residuals")
    axes[0].set_title("Residuals vs Fitted")
    axes[0].grid(True, alpha=0.3)
    
    # Q-Q Plot
    from scipy import stats
    stats.probplot(residuals, dist="norm", plot=axes[1])
    axes[1].set_title("Q-Q Plot")
    axes[1].grid(True, alpha=0.3)
    
    fig.suptitle(title or "Residual Analysis")
    fig.tight_layout()
    
    return _save_figure(fmt)


def plot_forecast(
    df: pd.DataFrame,
    actual: str,
    forecast: str,
    lower: Optional[str] = None,
    upper: Optional[str] = None,
    title: str = "",
    figsize: tuple = (12, 6),
    fmt: str = "png",
) -> bytes:
    """
    Forecast vs actual plot with confidence intervals.
    
    Args:
        df: DataFrame with DatetimeIndex
        actual: Actual values column
        forecast: Forecast values column
        lower: Lower confidence bound column
        upper: Upper confidence bound column
        title: Plot title
        figsize: Figure size
        fmt: Output format
    """
    fig, ax = plt.subplots(figsize=figsize)
    
    # Actual
    if actual in df.columns:
        actual_data = df[actual].dropna()
        ax.plot(actual_data.index, actual_data, label="Actual", color="black", linewidth=1.5)
    
    # Forecast
    if forecast in df.columns:
        fc_data = df[forecast].dropna()
        ax.plot(fc_data.index, fc_data, label="Forecast", color="red", linewidth=1.5, linestyle="--")
    
    # Confidence interval
    if lower and upper and lower in df.columns and upper in df.columns:
        lower_data = df[lower].dropna()
        upper_data = df[upper].dropna()
        if len(lower_data) > 0 and len(upper_data) > 0:
            common_idx = lower_data.index.intersection(upper_data.index)
            ax.fill_between(
                common_idx,
                lower_data.loc[common_idx],
                upper_data.loc[common_idx],
                color="red",
                alpha=0.2,
                label="Confidence Interval",
            )
    
    ax.set_title(title or "Forecast vs Actual")
    ax.set_xlabel("Time")
    ax.set_ylabel("Value")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M\n%Y-%m-%d"))
    fig.autofmt_xdate()
    
    return _save_figure(fmt)


def plot_seasonal(
    df: pd.DataFrame,
    column: str,
    period: int = 24,
    title: str = "",
    figsize: tuple = (12, 8),
    fmt: str = "png",
) -> bytes:
    """
    Seasonal decomposition plot.
    
    Args:
        df: DataFrame with DatetimeIndex
        column: Column to decompose
        period: Seasonal period
        title: Plot title
        figsize: Figure size
        fmt: Output format
    """
    from statsmodels.tsa.seasonal import seasonal_decompose
    
    data = df[column].dropna()
    if len(data) < 2 * period:
        raise ValueError(f"Insufficient data for period {period}")
    
    # Ensure regular frequency
    freq = pd.infer_freq(data.index)
    if freq:
        data = data.asfreq(freq)
    
    result = seasonal_decompose(data, period=period, model="additive")
    
    fig, axes = plt.subplots(4, 1, figsize=figsize, sharex=True)
    
    axes[0].plot(result.observed.index, result.observed, color="black", linewidth=0.8)
    axes[0].set_ylabel("Observed")
    axes[0].grid(True, alpha=0.3)
    
    axes[1].plot(result.trend.index, result.trend, color="blue", linewidth=1)
    axes[1].set_ylabel("Trend")
    axes[1].grid(True, alpha=0.3)
    
    axes[2].plot(result.seasonal.index, result.seasonal, color="green", linewidth=0.8)
    axes[2].set_ylabel("Seasonal")
    axes[2].grid(True, alpha=0.3)
    
    axes[3].plot(result.resid.index, result.resid, color="red", linewidth=0.5)
    axes[3].set_ylabel("Residual")
    axes[3].grid(True, alpha=0.3)
    
    fig.suptitle(title or f"Seasonal Decomposition ({column}, period={period})")
    fig.tight_layout()
    
    return _save_figure(fmt)


def plot_acf_pacf(
    series: pd.Series,
    nlags: int = 40,
    title: str = "",
    figsize: tuple = (12, 6),
    fmt: str = "png",
) -> bytes:
    """
    ACF and PACF plots.
    
    Args:
        series: Time series
        nlags: Number of lags
        title: Plot title
        figsize: Figure size
        fmt: Output format
    """
    from statsmodels.tsa.stattools import acf, pacf
    
    series = series.dropna()
    nlags = min(nlags, len(series) - 1)
    
    acf_vals, acf_conf = acf(series, nlags=nlags, alpha=0.05)
    pacf_vals, pacf_conf = pacf(series, nlags=nlags, alpha=0.05)
    
    fig, axes = plt.subplots(1, 2, figsize=figsize)
    
    # ACF
    axes[0].bar(range(nlags + 1), acf_vals, width=0.4, color="steelblue", alpha=0.7)
    axes[0].axhline(y=0, color="black", linewidth=0.5)
    axes[0].axhline(y=acf_conf[0, 1], color="red", linestyle="--", alpha=0.7)
    axes[0].axhline(y=acf_conf[0, 0], color="red", linestyle="--", alpha=0.7)
    axes[0].set_xlabel("Lag")
    axes[0].set_ylabel("ACF")
    axes[0].set_title("Autocorrelation")
    axes[0].grid(True, alpha=0.3)
    
    # PACF
    axes[1].bar(range(nlags + 1), pacf_vals, width=0.4, color="coral", alpha=0.7)
    axes[1].axhline(y=0, color="black", linewidth=0.5)
    axes[1].axhline(y=pacf_conf[0, 1], color="red", linestyle="--", alpha=0.7)
    axes[1].axhline(y=pacf_conf[0, 0], color="red", linestyle="--", alpha=0.7)
    axes[1].set_xlabel("Lag")
    axes[1].set_ylabel("PACF")
    axes[1].set_title("Partial Autocorrelation")
    axes[1].grid(True, alpha=0.3)
    
    fig.suptitle(title or "ACF / PACF")
    fig.tight_layout()
    
    return _save_figure(fmt)


def plot_box(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    by: Optional[str] = None,
    title: str = "",
    figsize: tuple = (12, 6),
    fmt: str = "png",
) -> bytes:
    """
    Box plot for distribution comparison.
    
    Args:
        df: DataFrame
        columns: Columns to plot
        by: Group by column
        title: Plot title
        figsize: Figure size
        fmt: Output format
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    fig, ax = plt.subplots(figsize=figsize)
    
    if by and by in df.columns:
        # Grouped box plot
        data = [df[df[by] == group][col].dropna() for col in columns for group in df[by].unique()]
        labels = [f"{col}\n{group}" for col in columns for group in df[by].unique()]
        ax.boxplot(data, labels=labels)
        ax.set_xticklabels(labels, rotation=45, ha="right")
    else:
        data = [df[col].dropna() for col in columns]
        ax.boxplot(data, labels=columns)
    
    ax.set_title(title or "Box Plot")
    ax.set_ylabel("Value")
    ax.grid(True, alpha=0.3)
    
    return _save_figure(fmt)


def plot_violin(
    df: pd.DataFrame,
    column: str,
    by: Optional[str] = None,
    title: str = "",
    figsize: tuple = (10, 6),
    fmt: str = "png",
) -> bytes:
    """
    Violin plot for distribution comparison.
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found")
    
    fig, ax = plt.subplots(figsize=figsize)
    
    if by and by in df.columns:
        data = [df[df[by] == group][column].dropna() for group in df[by].unique()]
        labels = df[by].unique()
        ax.violinplot(data, showmeans=True, showmedians=True)
        ax.set_xticks(range(1, len(labels) + 1))
        ax.set_xticklabels(labels)
    else:
        ax.violinplot([df[column].dropna()], showmeans=True, showmedians=True)
        ax.set_xticks([1])
        ax.set_xticklabels([column])
    
    ax.set_title(title or f"Violin Plot: {column}")
    ax.set_ylabel("Value")
    ax.grid(True, alpha=0.3)
    
    return _save_figure(fmt)


def plot_heatmap(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    title: str = "",
    figsize: tuple = (10, 8),
    fmt: str = "png",
    cmap: str = "viridis",
) -> bytes:
    """
    Heatmap of DataFrame values (not correlation).
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    fig, ax = plt.subplots(figsize=figsize)
    
    sns.heatmap(
        df[columns].T,
        cmap=cmap,
        annot=False,
        fmt=".2f",
        linewidths=0.5,
        cbar_kws={"shrink": 0.8},
        ax=ax,
    )
    
    ax.set_title(title or "Heatmap")
    ax.set_ylabel("Features")
    ax.set_xlabel("Observations")
    
    return _save_figure(fmt)


def save_plot(
    df: pd.DataFrame,
    plot_type: str,
    path: str,
    **kwargs,
) -> str:
    """
    Save plot to file.
    
    Args:
        df: DataFrame
        plot_type: "line", "hist", "scatter", "corr", "forecast", "seasonal", "acf", "box", "violin", "heatmap"
        path: Output path
        **kwargs: Arguments passed to plot function
    
    Returns:
        Path where file was saved
    """
    plot_funcs = {
        "line": plot_line,
        "hist": plot_hist,
        "dist": plot_dist,
        "scatter": plot_scatter,
        "corr": plot_corr,
        "residual": plot_residual,
        "forecast": plot_forecast,
        "seasonal": plot_seasonal,
        "acf": plot_acf_pacf,
        "box": plot_box,
        "violin": plot_violin,
        "heatmap": plot_heatmap,
    }
    
    if plot_type not in plot_funcs:
        raise ValueError(f"Unknown plot type: {plot_type}")
    
    fmt = path.split(".")[-1].lower()
    if fmt not in ["png", "svg", "html"]:
        fmt = "png"
    
    data = plot_funcs[plot_type](df, fmt=fmt, **kwargs)
    
    with open(path, "wb") as f:
        f.write(data)
    
    return path


# HTML export for interactive plots (using matplotlib's HTML backend or plotly if available)
def plot_to_html(
    df: pd.DataFrame,
    plot_type: str,
    **kwargs,
) -> str:
    """Generate HTML representation of plot."""
    # For now, return base64 embedded PNG
    data = save_plot(df, plot_type, "/tmp/temp_plot.png", **kwargs)
    with open("/tmp/temp_plot.png", "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f'<img src="data:image/png;base64,{b64}" style="max-width:100%;">'