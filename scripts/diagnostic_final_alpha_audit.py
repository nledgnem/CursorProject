"""Final Alpha Audit: Decile EV curve and LOESS fit for F_tk.

This script:
- Reads MSM v0 gold-layer `msm_timeseries.csv`.
- Bins F_tk into 10 deciles.
- Computes expected weekly return y per decile.
- Fits a LOESS-like smoothed curve of E[y | F_tk] over the F_tk range.
- Estimates the F_tk value where the smoothed curve peaks.
- Plots:
  * Decile mean y vs. decile center.
  * Smoothed LOESS curve over the raw (F_tk, y) cloud.
  * Rolling correlation between mean and median basket returns over time.

Outputs:
- `scripts/chart_final_alpha_loess.png`
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess


def load_timeseries(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Timeseries CSV not found at {path}")
    df = pd.read_csv(path)
    if "decision_date" not in df.columns or "F_tk" not in df.columns or "y" not in df.columns:
        raise KeyError(
            "Expected columns 'decision_date', 'F_tk', and 'y' in msm_timeseries.csv."
        )
    df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
    df = df.dropna(subset=["decision_date", "F_tk", "y"]).copy()
    df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0  # Unit: APR % (DATA_DICTIONARY.md)
    df = df.sort_values("decision_date").reset_index(drop=True)
    return df


def compute_deciles(df: pd.DataFrame, n_deciles: int = 10) -> pd.DataFrame:
    """Bin F_tk into deciles and compute expected weekly return y per decile."""
    df = df.copy()
    df["F_decile"] = pd.qcut(df["F_tk"], q=n_deciles, labels=False, duplicates="drop")

    decile_stats = (
        df.groupby("F_decile", dropna=True)
        .agg(
            mean_F=("F_tk", "mean"),
            mean_y=("y", "mean"),
            count=("y", "size"),
        )
        .reset_index()
        .sort_values("F_decile")
    )
    return decile_stats


def fit_loess(df: pd.DataFrame, frac: float = 0.4) -> tuple[np.ndarray, np.ndarray]:
    """Fit LOESS E[y | F_tk] and return sorted x grid and smoothed y."""
    x = df["F_tk"].to_numpy()
    y = df["y"].to_numpy()

    # Guard against degenerate cases
    if len(df) < 5 or np.nanstd(x) == 0:
        grid = np.array([np.nan])
        smoothed = np.array([np.nan])
        return grid, smoothed

    # Sort by F_tk for a clean curve
    order = np.argsort(x)
    x_sorted = x[order]
    y_sorted = y[order]

    lo = lowess(y_sorted, x_sorted, frac=frac, return_sorted=True)
    grid = lo[:, 0]
    smoothed = lo[:, 1]
    return grid, smoothed


def estimate_loess_peak(grid: np.ndarray, smoothed: np.ndarray) -> tuple[float, float]:
    """Return (F_peak, y_peak) where LOESS curve achieves its maximum."""
    if grid.size == 0 or np.all(np.isnan(smoothed)):
        return float("nan"), float("nan")
    idx = int(np.nanargmax(smoothed))
    return float(grid[idx]), float(smoothed[idx])


def compute_mean_median_rolling(df: pd.DataFrame, window: int = 12) -> pd.DataFrame:
    """Compute rolling correlation between mean and median basket returns over time.

    Here we approximate by using:
    - mean series: rolling mean of y
    - median proxy: rolling median of y
    """
    df = df.copy()
    df = df.set_index("decision_date").sort_index()

    df["mean_y"] = df["y"].rolling(window=window, min_periods=max(4, window // 2)).mean()
    df["median_y"] = df["y"].rolling(window=window, min_periods=max(4, window // 2)).median()

    # Rolling correlation between mean_y and median_y
    df["corr_mean_median"] = (
        df[["mean_y", "median_y"]]
        .rolling(window=window, min_periods=max(4, window // 2))
        .corr()
        .unstack()
        .iloc[:, 1]
    )

    df = df.reset_index()
    return df


def plot_final_alpha(
    df: pd.DataFrame,
    decile_stats: pd.DataFrame,
    loess_grid: np.ndarray,
    loess_vals: np.ndarray,
    rolling_df: pd.DataFrame,
    out_path: Path,
) -> None:
    plt.style.use("seaborn-v0_8-darkgrid")
    fig, axes = plt.subplots(3, 1, figsize=(10, 12), dpi=150)

    # Panel 1: Decile EV vs F_tk (% per day)
    ax = axes[0]
    x_dec = decile_stats["mean_F"] * 100.0
    ax.bar(
        x_dec,
        decile_stats["mean_y"],
        width=(x_dec.max() - x_dec.min()) /
        max(10, len(decile_stats)),
        alpha=0.7,
        color="#1f77b4",
    )
    ax.set_title("Decile Expected Weekly Return vs F_tk")
    ax.set_xlabel("F_tk (% per day, decile mean)")
    ax.set_ylabel("E[y] per week")

    # Panel 2: LOESS curve over raw scatter
    ax = axes[1]
    x_scatter = df["F_tk"] * 100.0
    ax.scatter(x_scatter, df["y"], s=10, alpha=0.3, color="#999999", label="Weekly (F_tk, y)")
    if np.isfinite(loess_grid).any():
        ax.plot(
            loess_grid * 100.0,
            loess_vals,
            color="red",
            linewidth=2.0,
            label="LOESS E[y | F_tk]",
        )
    ax.set_title("LOESS Curve: Expected Weekly Return vs F_tk (% per day)")
    ax.set_xlabel("F_tk (% per day)")
    ax.set_ylabel("E[y | F_tk] (smoothed)")
    ax.legend()

    # Panel 3: Rolling correlation mean vs median
    ax = axes[2]
    ax.plot(
        rolling_df["decision_date"],
        rolling_df["corr_mean_median"],
        color="purple",
        linewidth=1.5,
    )
    ax.axhline(0.0, color="black", linestyle="--", linewidth=1.0)
    ax.set_title("Rolling Correlation: Mean vs Median Weekly Return")
    ax.set_xlabel("Decision Date")
    ax.set_ylabel("Corr(mean_y, median_y)")

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    ts_path = (
        repo_root
        / "reports"
        / "msm_funding_v0"
        / "silver_router_variance_shield"
        / "msm_timeseries.csv"
    )

    df = load_timeseries(ts_path)

    # Decile stats
    decile_stats = compute_deciles(df, n_deciles=10)

    # LOESS fit
    lo_grid, lo_vals = fit_loess(df, frac=0.4)
    F_peak, y_peak = estimate_loess_peak(lo_grid, lo_vals)

    # Rolling mean/median correlation
    rolling_df = compute_mean_median_rolling(df, window=12)

    # Plot
    out_path = Path(__file__).with_name("chart_final_alpha_loess.png")
    plot_final_alpha(df, decile_stats, lo_grid, lo_vals, rolling_df, out_path)

    # Terminal summary
    # Decile 1, 5, 10 expected weekly return
    d1 = decile_stats.loc[decile_stats["F_decile"] == decile_stats["F_decile"].min(), "mean_y"]
    d5 = decile_stats.loc[decile_stats["F_decile"] == 4, "mean_y"]  # zero-indexed deciles: 0..9
    d10 = decile_stats.loc[decile_stats["F_decile"] == decile_stats["F_decile"].max(), "mean_y"]

    dec1_ev = float(d1.iloc[0]) if not d1.empty else float("nan")
    dec5_ev = float(d5.iloc[0]) if not d5.empty else float("nan")
    dec10_ev = float(d10.iloc[0]) if not d10.empty else float("nan")

    print("\n" + "=" * 72)
    print("FINAL ALPHA AUDIT (DECILES & LOESS)")
    print("=" * 72)
    print(f"Decile 1 (lowest F_tk)   EV[y]: {dec1_ev:.6f}")
    print(f"Decile 5 (middle F_tk)   EV[y]: {dec5_ev:.6f}")
    print(f"Decile 10 (highest F_tk) EV[y]: {dec10_ev:.6f}")
    print(f"LOESS peak F_tk          : {F_peak:.6f}")
    print(f"LOESS peak E[y]          : {y_peak:.6f}")
    print("=" * 72 + "\n")
    print(f"Saved final alpha chart to: {out_path}")


if __name__ == "__main__":
    main()

