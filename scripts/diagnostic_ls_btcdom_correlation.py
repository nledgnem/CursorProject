import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def run_ls_btcdom_correlation(csv_path: str):
    # Load data
    df = pd.read_csv(csv_path)

    # Ensure required columns exist
    required_cols = ["y", "btcd_index_decision"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    # --- Step 1: Data Coverage Audit ---
    # LS Strategy sensor coverage (7-day log return y)
    ls_coverage_count = df["y"].notna().sum()

    # Derive 7-day BTCDOM log return from index levels
    df["btcdom_7d_ret"] = np.log(df["btcd_index_decision"].shift(-1) / df["btcd_index_decision"])

    # BTCDOM coverage (valid derived 7-day log returns)
    btcdom_coverage_count = df["btcdom_7d_ret"].notna().sum()

    # --- Step 2: Overlap & Unit Conversion ---
    overlap_df = df.dropna(subset=["y", "btcdom_7d_ret"]).copy()

    # Convert to physical percentage returns
    overlap_df["y_pct"] = (np.exp(overlap_df["y"]) - 1.0) * 100.0
    overlap_df["btcdom_pct"] = (np.exp(overlap_df["btcdom_7d_ret"]) - 1.0) * 100.0

    overlap_n = len(overlap_df)
    if overlap_n == 0:
        raise ValueError("No overlapping non-NaN observations between y and btcdom_7d_ret.")

    # --- Step 3: Dual-Correlation ---
    pearson_corr = overlap_df["y_pct"].corr(overlap_df["btcdom_pct"], method="pearson")
    spearman_corr = overlap_df["y_pct"].corr(overlap_df["btcdom_pct"], method="spearman")

    # --- Step 4: Visualization ---
    sns.set(style="whitegrid")
    fig, ax = plt.subplots(figsize=(8, 6))

    sns.regplot(
        data=overlap_df,
        x="btcdom_pct",
        y="y_pct",
        ax=ax,
        scatter_kws={"alpha": 0.6, "s": 40},
        line_kws={"color": "red", "linewidth": 2},
    )

    ax.set_xlabel("BTCDOM 7-Day Return (%)", fontsize=12, fontweight="bold")
    ax.set_ylabel("LS Strategy 7-Day Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Sensor Verification: LS Strategy vs. BTCDOM Correlation",
        fontsize=14,
        fontweight="bold",
    )

    # Text box with correlation metrics
    text_str = (
        f"Overlap N: {overlap_n}\n"
        f"Pearson: {pearson_corr:.3f}\n"
        f"Spearman: {spearman_corr:.3f}"
    )
    ax.text(
        0.05,
        0.95,
        text_str,
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
    )

    fig.tight_layout()

    # Save chart
    output_path = Path("scripts/chart_ls_btcdom_correlation.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)

    # --- Step 5: Terminal Output Report ---
    print("=== LS Strategy vs. BTCDOM Sensor Coverage & Correlation ===")
    print(f"Data Coverage - LS Strategy (y): {ls_coverage_count} weeks")
    print(f"Data Coverage - BTCDOM: {btcdom_coverage_count} weeks")
    print(f"Overlap Population (N): {overlap_n} weeks")
    print(f"Pearson Correlation: {pearson_corr:.4f}")
    print(f"Spearman Correlation: {spearman_corr:.4f}")
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    run_ls_btcdom_correlation(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

