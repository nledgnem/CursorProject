#!/usr/bin/env python3
"""
Final 3-chart tearsheet: BTCDOM tracking, funding quartiles, threshold sweep.
Prints validation summary to terminal.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"
MSM_PATH = REPO_ROOT / "reports" / "msm_funding_v0" / "silver_router_variance_shield" / "msm_timeseries.csv"
OUT_DIR = REPO_ROOT / "scripts" / "out"


def chart_1_btcdom_tracking() -> None:
    """Official Binance vs Silver-reconstructed BTCDOM line chart."""
    binance = pd.read_csv(DATA_LAKE / "binance_btcdom.csv")
    recon = pd.read_csv(DATA_LAKE / "btcdom_reconstructed.csv")

    binance["date"] = pd.to_datetime(binance["timestamp"]).dt.date
    recon["date"] = pd.to_datetime(recon["date"]).dt.date

    merged = recon.merge(
        binance[["date", "close"]].rename(columns={"close": "binance"}),
        on="date",
        how="inner",
    )
    merged = merged.sort_values("date").reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        merged["date"],
        merged["binance"],
        color="black",
        linewidth=1.2,
        label="Official Binance",
    )
    ax.plot(
        merged["date"],
        merged["reconstructed_index_value"].astype(float),
        color="blue",
        linestyle="--",
        linewidth=1.2,
        label="Custom (Silver-Reconstructed)",
    )
    ax.set_title("Index Tracking Validation: Official Binance BTCDOM vs. Silver-Reconstructed BTCDOM")
    ax.set_xlabel("Date")
    ax.set_ylabel("Index Level")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    plt.tight_layout()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_DIR / "chart_1_btcdom_tracking.png", dpi=150)
    plt.close()


def chart_2_quartiles() -> tuple[pd.DataFrame, float]:
    """
    Bar chart: 2-year cumulative L/S return by F_tk quartile.
    Returns quartile stats and Q3 cumulative return (%) for validation.
    """
    df = pd.read_csv(MSM_PATH)
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0  # Unit: APR % (DATA_DICTIONARY.md)
    cutoff = df["decision_date"].max() - pd.DateOffset(years=2)
    df = df.loc[df["decision_date"] >= cutoff].copy()
    df = df.dropna(subset=["y"])

    df["F_tk_quartile"] = pd.qcut(df["F_tk_apr"], q=4, labels=["Q1 (Lowest)", "Q2", "Q3", "Q4 (Highest)"])

    quartile_returns = (
        df.groupby("F_tk_quartile", observed=True)["y"]
        .sum()
        .apply(lambda s: (np.exp(s) - 1.0) * 100)
    )
    quartile_returns = quartile_returns.reindex(["Q1 (Lowest)", "Q2", "Q3", "Q4 (Highest)"])

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = ["red", "green", "blue", "darkblue"]
    x = np.arange(len(quartile_returns))
    bars = ax.bar(x, quartile_returns.values, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(quartile_returns.index)
    for bar, val in zip(bars, quartile_returns.values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + (0.5 if val >= 0 else -1.2),
            f"{val:.2f}%",
            ha="center",
            va="bottom" if val >= 0 else "top",
            fontsize=10,
            fontweight="bold",
        )
    ax.set_title("Attribution by Regime: 2-Year Cumulative L/S Return vs. Funding Quartiles")
    ax.set_ylabel("Cumulative Return (%)")
    ax.axhline(0, color="gray", linestyle="-", linewidth=0.5)
    ax.margins(y=0.15)
    plt.tight_layout()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_DIR / "chart_2_quartiles.png", dpi=150)
    plt.close()

    q3_return = float(quartile_returns.loc["Q3"]) if "Q3" in quartile_returns.index else np.nan
    return quartile_returns, q3_return


def chart_3_threshold_sweep() -> tuple[float, float]:
    """
    Line chart: cumulative return vs. max F_tk threshold (only take trades when F_tk <= T).
    Returns (peak_cumulative_return_pct, optimal_threshold).
    """
    df = pd.read_csv(MSM_PATH)
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0  # Unit: APR % (DATA_DICTIONARY.md)
    cutoff = df["decision_date"].max() - pd.DateOffset(years=2)
    df = df.loc[df["decision_date"] >= cutoff].copy()
    df = df.dropna(subset=["y", "F_tk"])

    F_min, F_max = df["F_tk_apr"].min(), df["F_tk_apr"].max()
    thresholds = np.linspace(F_min, F_max, 100)

    cum_returns = []
    for T in thresholds:
        # Weekly return = y if F_tk_apr <= T else 0 (log return)
        log_ret = df["y"].where(df["F_tk_apr"] <= T, 0.0).sum()
        pct = (np.exp(log_ret) - 1.0) * 100
        cum_returns.append(pct)

    cum_returns = np.array(cum_returns)
    peak_idx = np.argmax(cum_returns)
    peak_return = float(cum_returns[peak_idx])
    optimal_T = float(thresholds[peak_idx])

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(thresholds, cum_returns, color="steelblue", linewidth=1.5)
    ax.scatter([optimal_T], [peak_return], color="red", s=80, zorder=5)
    ax.annotate(
        f"Peak: {peak_return:.2f}%\nT = {optimal_T:.6f}",
        xy=(optimal_T, peak_return),
        xytext=(optimal_T + (thresholds[-1] - thresholds[0]) * 0.15, peak_return),
        fontsize=10,
        fontweight="bold",
        arrowprops=dict(arrowstyle="->", color="red"),
    )
    ax.set_title("Optimal Funding Gate: Cumulative Return vs. Max Absolute Funding Threshold")
    ax.set_xlabel("Threshold T (F_tk_apr <= T, % APR)")
    ax.set_ylabel("Cumulative Return (%)")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_DIR / "chart_3_threshold_sweep.png", dpi=150)
    plt.close()

    return peak_return, optimal_T


def main() -> None:
    print("BTCDOM Recalculated using Silver Data.")

    chart_1_btcdom_tracking()
    print(f"  Chart 1 saved: {OUT_DIR / 'chart_1_btcdom_tracking.png'}")

    quartile_returns, q3_return = chart_2_quartiles()
    print(f"  Chart 2 saved: {OUT_DIR / 'chart_2_quartiles.png'}")
    print(f"  Q3 Quartile cumulative return (%): {q3_return:.4f}")

    peak_return, optimal_T = chart_3_threshold_sweep()
    print(f"  Chart 3 saved: {OUT_DIR / 'chart_3_threshold_sweep.png'}")
    print(f"  Peak cumulative return (%): {peak_return:.4f}")
    print(f"  Optimal threshold T: {optimal_T:.6f}")


if __name__ == "__main__":
    main()
