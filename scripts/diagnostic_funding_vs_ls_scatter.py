import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
except ImportError:  # Fallback if statsmodels is unavailable
    lowess = None


def run_funding_vs_ls_scatter(csv_path: str):
    # Load data
    df = pd.read_csv(csv_path)

    # Keep only needed columns and drop NaNs
    df = df[["F_tk_apr", "y"]].dropna(subset=["F_tk_apr", "y"]).copy()

    if df.empty:
        raise ValueError("No valid rows after filtering for 'F_tk_apr' and 'y'.")

    # Convert weekly log return y to simple percentage return
    df["y_pct"] = (np.exp(df["y"]) - 1.0) * 100.0

    x = df["F_tk_apr"].values
    y_pct = df["y_pct"].values
    n = len(df)

    # --- Step 2: Regression & Correlations ---
    # Linear fit (OLS via numpy polyfit)
    slope, intercept = np.polyfit(x, y_pct, 1)

    # Pearson (linear) correlation
    pearson_corr = df["F_tk_apr"].corr(df["y_pct"], method="pearson")

    # Spearman (rank) correlation
    spearman_corr = df["F_tk_apr"].corr(df["y_pct"], method="spearman")

    # Prepare linear fit line
    x_lin = np.linspace(x.min(), x.max(), 500)
    y_lin = slope * x_lin + intercept

    # Prepare LOWESS fit if available
    x_lowess = None
    y_lowess = None
    if lowess is not None:
        lowess_result = lowess(y_pct, x, frac=0.4, return_sorted=True)
        x_lowess = lowess_result[:, 0]
        y_lowess = lowess_result[:, 1]

    # --- Step 3: Visualization ---
    sns.set(style="whitegrid")
    fig, ax = plt.subplots(figsize=(10, 7))

    # Scatter of raw observations
    ax.scatter(x, y_pct, alpha=0.5, s=35, color="steelblue", edgecolor="none")

    # Linear fit line (red dashed)
    ax.plot(x_lin, y_lin, color="red", linestyle="--", linewidth=2, label="Linear Fit (OLS)")

    # LOWESS curve (green solid) if available
    if x_lowess is not None and y_lowess is not None:
        ax.plot(
            x_lowess,
            y_lowess,
            color="green",
            linestyle="-",
            linewidth=2,
            label="LOWESS Fit",
        )

    # Baseline at 0% return
    ax.axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)

    ax.set_xlabel("F_tk_apr (%)", fontsize=12, fontweight="bold")
    ax.set_ylabel("LS Strategy 7-Day Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Signal Physics: F_tk_apr vs. LS Strategy Returns (Linear vs. LOWESS Fit)",
        fontsize=14,
        fontweight="bold",
    )

    # Legend (only if LOWESS is available we have two lines)
    ax.legend()

    # Text box with correlation metrics and population size
    text_str = (
        f"Master N: {n}\n"
        f"Pearson: {pearson_corr:.3f}\n"
        f"Spearman: {spearman_corr:.3f}"
    )
    ax.text(
        0.03,
        0.97,
        text_str,
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85),
    )

    fig.tight_layout()

    # Save chart
    output_path = Path("scripts/chart_funding_vs_ls_scatter.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)

    # --- Step 4: Terminal Output Report ---
    print("=== Signal Physics: F_tk_apr vs. LS Strategy Returns ===")
    print(f"Master Population Size (N): {n} weeks")
    print(f"Linear Pearson Correlation: {pearson_corr:.4f}")
    print(f"Non-Linear Spearman Rank Correlation: {spearman_corr:.4f}")
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    run_funding_vs_ls_scatter(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

