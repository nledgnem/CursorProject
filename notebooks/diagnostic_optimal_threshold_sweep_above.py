"""Optimal Funding Gate (above T): strategy ON when F_tk_apr >= T, OFF when F_tk_apr < T.

Loads msm_timeseries.csv, sweeps T_apr, computes cumulative return when only
trading weeks with F_tk_apr >= T_apr, plots and marks the optimal threshold.
Saves chart as chart_threshold_sweep_apr_above.png.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def get_timeseries_path(repo_root: Path) -> Path:
    return (
        repo_root
        / "reports"
        / "msm_funding_v0"
        / "silver_router_variance_shield"
        / "msm_timeseries.csv"
    )


def load_and_ensure_apr(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "F_tk" not in df.columns or "y" not in df.columns:
        raise KeyError("msm_timeseries must contain F_tk and y")
    if "F_tk_apr" not in df.columns:
        df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0
    df = df.dropna(subset=["F_tk_apr", "y", "decision_date"]).copy()
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df = df.sort_values("decision_date").reset_index(drop=True)
    return df


def cumulative_return_when_above(df: pd.DataFrame, t_apr: float) -> float:
    """Cumulative return (%) when we only trade weeks with F_tk_apr >= t_apr."""
    mask = df["F_tk_apr"] >= t_apr
    # y is log return; use exp(y)-1 for arithmetic then compound
    y = df["y"].to_numpy()
    r = np.where(mask, np.expm1(y), 0.0)
    cum = np.cumprod(1.0 + r) - 1.0
    return float(cum[-1]) * 100.0 if len(cum) else 0.0


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    ts_path = get_timeseries_path(repo_root)
    if not ts_path.exists():
        raise FileNotFoundError(f"Timeseries not found: {ts_path}")

    df = load_and_ensure_apr(ts_path)
    apr = df["F_tk_apr"].to_numpy()
    t_min, t_max = float(np.nanmin(apr)), float(np.nanmax(apr))
    t_grid = np.linspace(t_min - 0.5, t_max + 0.5, 200)
    cum_returns = np.array([cumulative_return_when_above(df, t) for t in t_grid])

    idx_peak = int(np.argmax(cum_returns))
    t_optimal = float(t_grid[idx_peak])
    peak_cum = float(cum_returns[idx_peak])

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    ax.plot(
        t_grid,
        cum_returns,
        color="steelblue",
        linewidth=2.0,
        label="Cumulative return (%)",
    )
    ax.scatter(
        [t_optimal],
        [peak_cum],
        color="red",
        s=120,
        zorder=5,
        label=f"Peak: {peak_cum:.2f}%\nT = {t_optimal:.2f}% APR",
    )
    ax.set_xlabel("Threshold T (Gate ON when F_tk_apr >= T, % APR)")
    ax.set_ylabel("Cumulative Return (%)")
    ax.set_title(
        "Optimal Funding Gate (Above T): Cumulative Return vs. Min APR Threshold\n"
        "Strategy ON when F_tk_apr >= T, OFF when F_tk_apr < T"
    )
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    chart_path = Path(__file__).resolve().parent / "chart_threshold_sweep_apr_above.png"
    fig.savefig(chart_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {chart_path}")
    print(
        f"Optimal T_apr (gate ON when F_tk_apr >= T) = {t_optimal:.2f}% APR, "
        f"Peak cumulative return = {peak_cum:.2f}%"
    )


if __name__ == "__main__":
    main()
