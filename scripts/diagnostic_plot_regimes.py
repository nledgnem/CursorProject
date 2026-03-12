#!/usr/bin/env python3
"""
Diagnostic regime plots for MSM v0 using clean Silver-layer timeseries.

Generates:
- chart_quartiles_clean.png: 2-year cumulative L/S return by funding quartile
- chart_threshold_sweep_clean.png: 2-year cumulative L/S return vs max funding gate
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def load_timeseries(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])
    if "y" not in df.columns or "F_tk" not in df.columns:
        raise ValueError("Input CSV must contain at least 'decision_date', 'F_tk', and 'y' columns.")
    # Drop rows with missing key fields
    df = df.dropna(subset=["decision_date", "F_tk", "y"]).copy()
    return df


def filter_last_2y(df: pd.DataFrame) -> pd.DataFrame:
    max_date = df["decision_date"].max()
    cutoff = max_date - pd.Timedelta(days=365 * 2)
    return df[df["decision_date"] >= cutoff].copy()


def plot_quartiles(df: pd.DataFrame, out_path: Path) -> None:
    """Task 1: Strategy Return by Funding Rate Quartile (Last 2 Years)."""
    df = df.copy()

    # Define quartile labels
    labels = ["Q1 (Lowest)", "Q2 (Low-Mid)", "Q3 (Mid-High)", "Q4 (Highest)"]
    df["funding_quartile"] = pd.qcut(df["F_tk"], q=4, labels=labels)

    results = []
    for label in labels:
        sub = df[df["funding_quartile"] == label]
        y_sum = sub["y"].sum()
        cumulative_pct = (np.exp(y_sum) - 1.0) * 100.0
        results.append((label, cumulative_pct))

    labels_out, vals = zip(*results)
    vals = list(vals)

    # Color by sign: red for negative or zero, green for positive
    colors = ["green" if v > 0 else "red" for v in vals]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(labels_out, vals, color=colors)

    # Add labels on top of bars
    for bar, v in zip(bars, vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            v,
            f"{v:.1f}%",
            ha="center",
            va="bottom" if v >= 0 else "top",
            fontsize=10,
        )

    ax.set_ylabel("Cumulative Return (%)")
    ax.set_title(
        "Attribution by Regime: 2-Year Cumulative L/S Basket Return\n"
        "vs. Average Altcoin Funding Quartiles (Clean Data)"
    )
    ax.axhline(0.0, color="black", linewidth=0.8)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_threshold_sweep(df: pd.DataFrame, out_path: Path) -> None:
    """Task 2: Continuous Threshold Sweep (Cumulative Return vs. Max Funding Gate)."""
    df = df.copy()

    f_min = df["F_tk"].min()
    f_max = df["F_tk"].max()
    thresholds = np.linspace(f_min, f_max, 100)

    cum_returns = []
    for T in thresholds:
        active = df[df["F_tk"] <= T]
        y_sum = active["y"].sum()
        cumulative_pct = (np.exp(y_sum) - 1.0) * 100.0
        cum_returns.append(cumulative_pct)

    cum_returns = np.array(cum_returns)
    idx_max = int(np.nanargmax(cum_returns))
    T_opt = thresholds[idx_max]
    ret_opt = cum_returns[idx_max]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(thresholds, cum_returns, label="Cumulative Return (%)")
    ax.set_xlabel("Max Funding Threshold F_tk")
    ax.set_ylabel("Cumulative Return (%)")
    ax.set_title(
        "Optimal Funding Gate: Cumulative Return vs. Max Funding Threshold (Clean Data)"
    )

    # Highlight peak
    ax.plot(T_opt, ret_opt, "o", color="red")
    ax.annotate(
        f"Max: {ret_opt:.1f}% at T={T_opt:.4f}",
        xy=(T_opt, ret_opt),
        xytext=(0.05, 0.95),
        textcoords="axes fraction",
        arrowprops=dict(arrowstyle="->", color="red"),
        ha="left",
        va="top",
        fontsize=10,
    )
    ax.axhline(0.0, color="black", linewidth=0.8)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate regime diagnostics plots from MSM v0 Silver timeseries."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"),
        help="Path to msm_timeseries.csv",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("."),
        help="Directory to save output charts",
    )
    args = parser.parse_args()

    df = load_timeseries(args.csv)
    df_2y = filter_last_2y(df)

    out_quartiles = args.out_dir / "chart_quartiles_clean.png"
    out_sweep = args.out_dir / "chart_threshold_sweep_clean.png"

    plot_quartiles(df_2y, out_quartiles)
    plot_threshold_sweep(df_2y, out_sweep)

    print(f"Saved {out_quartiles}")
    print(f"Saved {out_sweep}")


if __name__ == "__main__":
    main()

