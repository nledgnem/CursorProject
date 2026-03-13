"""
Non-linear diagnostics for funding vs L/S returns.
Uses deciles and LOESS/quadratic fits to map E[y | F_tk].
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMESERIES_PATH = (
    PROJECT_ROOT
    / "reports"
    / "msm_funding_v0"
    / "silver_router_variance_shield"
    / "msm_timeseries.csv"
)


def main() -> None:
    df = pd.read_csv(TIMESERIES_PATH)
    df = df[["decision_date", "F_tk", "y"]].copy()
    df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0  # Unit: APR % (DATA_DICTIONARY.md)
    df = df.dropna(subset=["F_tk", "y"])
    df["decision_date"] = pd.to_datetime(df["decision_date"]).dt.normalize()

    # Arithmetic weekly return from log y
    df["arith_ret"] = np.exp(df["y"].astype(float)) - 1.0

    # --- Chart 1: Decile factor spread ---
    df["F_decile"], bins = pd.qcut(
        df["F_tk"],
        q=10,
        labels=[f"D{i}" for i in range(1, 11)],
        retbins=True,
        duplicates="drop",
    )

    decile_stats = (
        df.groupby("F_decile")["arith_ret"]
        .mean()
        .reindex([f"D{i}" for i in range(1, 11)])
    )

    colors = []
    for decile in decile_stats.index:
        idx = int(decile[1:])
        if 1 <= idx <= 3:
            colors.append("#2980b9")  # blue (Cold)
        elif 4 <= idx <= 8:
            colors.append("#27ae60")  # green (Warm)
        else:
            colors.append("#c0392b")  # red (Danger)

    fig1, ax1 = plt.subplots(figsize=(9, 5))
    bars = ax1.bar(decile_stats.index, decile_stats.values * 100.0, color=colors)
    ax1.axhline(0.0, color="black", linewidth=1)
    ax1.set_ylabel("Average weekly return (%)")
    ax1.set_xlabel("Funding decile (F_tk)")
    ax1.set_title("Non-Linear Factor Spread: Average Weekly Return by Funding Decile")
    for bar, val in zip(bars, decile_stats.values * 100.0):
        ax1.text(
            bar.get_x() + bar.get_width() / 2.0,
            val + (0.05 if val >= 0 else -0.05),
            f"{val:.2f}",
            ha="center",
            va="bottom" if val >= 0 else "top",
            fontsize=9,
        )
    plt.tight_layout()
    fig1.savefig(
        PROJECT_ROOT / "scripts" / "chart_nonlinear_1_deciles.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close(fig1)

    # --- Chart 2: LOESS + quadratic fit ---
    x = df["F_tk"].astype(float).values
    y = df["y"].astype(float).values

    # LOWESS (LOESS) curve
    try:
        from statsmodels.nonparametric.smoothers_lowess import lowess

        loess_result = lowess(y, x, frac=0.3, return_sorted=True)
        x_loess = loess_result[:, 0]
        y_loess = loess_result[:, 1]
    except ImportError:
        x_loess = None
        y_loess = None

    # Quadratic polynomial fit
    coeffs = np.polyfit(x, y, deg=2)
    x_quad = np.linspace(x.min(), x.max(), 200)
    y_quad = np.polyval(coeffs, x_quad)

    fig2, ax2 = plt.subplots(figsize=(9, 6))
    ax2.scatter(x, y, alpha=0.35, s=25, color="gray", edgecolors="none", label="Weekly observations")
    if x_loess is not None:
        ax2.plot(x_loess, y_loess, color="#2980b9", linewidth=2.5, label="LOESS (frac=0.3)")
    ax2.plot(x_quad, y_quad, color="#c0392b", linewidth=2, linestyle="--", label="Quadratic fit (deg=2)")
    ax2.set_xlabel("F_tk (average funding rate)")
    ax2.set_ylabel("L/S Basket weekly log return (y)")
    ax2.set_title("Continuous Physics: LOESS and Quadratic Regression of Returns vs. Funding")
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    plt.tight_layout()
    fig2.savefig(
        PROJECT_ROOT / "scripts" / "chart_nonlinear_2_loess.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close(fig2)

    # --- Terminal outputs ---
    d1_ret = decile_stats.get("D1", np.nan)
    d8_ret = decile_stats.get("D8", np.nan)
    d10_ret = decile_stats.get("D10", np.nan)

    print(f"Expected weekly return (Decile 1): {d1_ret:.6f}")
    print(f"Expected weekly return (Decile 8): {d8_ret:.6f}")
    print(f"Expected weekly return (Decile 10): {d10_ret:.6f}")

    if x_loess is not None and len(x_loess) > 0:
        idx_peak = int(np.nanargmax(y_loess))
        ftk_peak = float(x_loess[idx_peak])
        print(f"F_tk value at LOESS peak: {ftk_peak:.6f}")
    else:
        print("F_tk value at LOESS peak: NaN (LOWESS not available)")


if __name__ == "__main__":
    main()

