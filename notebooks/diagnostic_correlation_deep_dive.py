"""
Correlation deep-dive: why L/S Basket (y) vs BTCDOM correlation is ~zero.
Maps time-varying correlation, quadrant hit rate, and volatility ratio.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMESERIES_PATH = (
    PROJECT_ROOT
    / "reports"
    / "msm_funding_v0"
    / "silver_router_variance_shield"
    / "msm_timeseries.csv"
)
BTCDOM_PATH = PROJECT_ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"
OUT_DIR = PROJECT_ROOT / "scripts"

REGIME_COLORS = {
    "Q1": ("#e74c3c", 0.2),
    "Q2": ("#f39c12", 0.2),
    "Q3": ("#2ecc71", 0.2),
    "Q4": ("#95a5a6", 0.2),
}


def main():
    # --- Data integration: strict 7-day alignment (decision_date + next_date -> btcdom_7d_ret) ---
    ts = pd.read_csv(TIMESERIES_PATH, usecols=["decision_date", "next_date", "y", "F_tk"])
    ts["decision_date"] = pd.to_datetime(ts["decision_date"]).dt.normalize()
    ts["next_date"] = pd.to_datetime(ts["next_date"]).dt.normalize()
    ts["F_tk_apr"] = ts["F_tk"] * 365.0 * 100.0

    btcdom = pd.read_csv(BTCDOM_PATH, usecols=["date", "reconstructed_index_value"])
    btcdom["date"] = pd.to_datetime(btcdom["date"]).dt.normalize()
    btcdom = btcdom.rename(columns={"reconstructed_index_value": "btcdom_price"})
    btc_start = btcdom.rename(columns={"date": "decision_date", "btcdom_price": "btcdom_price_start"})
    btc_end = btcdom.rename(columns={"date": "next_date", "btcdom_price": "btcdom_price_end"})
    df = ts.merge(btc_start, on="decision_date", how="left").merge(btc_end, on="next_date", how="left")
    df["btcdom_7d_ret"] = np.log(df["btcdom_price_end"].astype(float) / df["btcdom_price_start"].astype(float))
    df = df.dropna(subset=["y", "btcdom_7d_ret"]).copy()
    df = df.sort_values("decision_date").reset_index(drop=True)

    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(years=2)
    df = df.loc[df["decision_date"] >= cutoff].reset_index(drop=True)

    if df.empty:
        print("No data after 2-year filter. Check date overlap.")
        return

    df["F_tk_quartile"], _ = pd.qcut(
        df["F_tk_apr"],
        q=4,
        labels=["Q1", "Q2", "Q3", "Q4"],
        retbins=True,
        duplicates="drop",
    )

    y = df["y"].astype(float).values
    btcdom_ret = df["btcdom_7d_ret"].astype(float).values
    dates = df["decision_date"].values

    # --- 12-week rolling correlation ---
    roll = 12
    rolling_corr = pd.Series(y).rolling(roll, min_periods=roll).corr(pd.Series(btcdom_ret))
    rolling_corr = rolling_corr.values  # align with df index after dropna

    # --- Chart 1: 12-week rolling correlation vs regimes ---
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    for i in range(len(df)):
        left = pd.Timestamp(dates[i])
        right = pd.Timestamp(dates[i + 1]) if i + 1 < len(df) else left + pd.Timedelta(days=7)
        q = df["F_tk_quartile"].iloc[i]
        color, alpha = REGIME_COLORS.get(str(q), ("#bdc3c7", 0.2))
        ax1.axvspan(left, right, facecolor=color, alpha=alpha, zorder=0)
    ax1.plot(df["decision_date"], rolling_corr, color="steelblue", linewidth=2, zorder=2)
    ax1.axhline(0.0, color="black", linewidth=1, linestyle="-", zorder=1)
    ax1.set_title("Chart 1: 12-Week Rolling Correlation vs. Funding Regimes")
    ax1.set_ylabel("Rolling Pearson correlation (y vs btcdom_7d_ret)")
    ax1.set_xlabel("decision_date")
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()
    fig1.savefig(OUT_DIR / "chart_deep_1_rolling_corr.png", dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Chart 2: Quadrant hit rate ---
    x, y_vals = btcdom_ret, y
    top_right = np.sum((x > 0) & (y_vals > 0))
    top_left = np.sum((x < 0) & (y_vals > 0))
    bottom_left = np.sum((x < 0) & (y_vals < 0))
    bottom_right = np.sum((x > 0) & (y_vals < 0))
    n = len(x)
    p_tr = 100 * top_right / n
    p_tl = 100 * top_left / n
    p_bl = 100 * bottom_left / n
    p_br = 100 * bottom_right / n

    fig2, ax2 = plt.subplots(figsize=(7, 7))
    ax2.scatter(x, y_vals, alpha=0.6, s=40, color="steelblue", edgecolors="white")
    ax2.axhline(0, color="black", linewidth=2)
    ax2.axvline(0, color="black", linewidth=2)
    ax2.set_xlabel("BTCDOM weekly log return")
    ax2.set_ylabel("L/S Basket weekly return (y)")
    ax2.set_title("Chart 2: Directional Concordance (Quadrant Hit Rate)")
    ax2.annotate(f"Top Right\n{p_tr:.1f}%", xy=(0.95, 0.95), xycoords="axes fraction", ha="right", va="top", fontsize=11)
    ax2.annotate(f"Top Left\n{p_tl:.1f}%", xy=(0.05, 0.95), xycoords="axes fraction", ha="left", va="top", fontsize=11)
    ax2.annotate(f"Bottom Left\n{p_bl:.1f}%", xy=(0.05, 0.05), xycoords="axes fraction", ha="left", va="bottom", fontsize=11)
    ax2.annotate(f"Bottom Right\n{p_br:.1f}%", xy=(0.95, 0.05), xycoords="axes fraction", ha="right", va="bottom", fontsize=11)
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    fig2.savefig(OUT_DIR / "chart_deep_2_quadrants.png", dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Chart 3: 12-week rolling volatility ratio ---
    roll_vol_y = pd.Series(y).rolling(roll, min_periods=roll).std()
    roll_vol_btc = pd.Series(btcdom_ret).rolling(roll, min_periods=roll).std()
    vol_ratio = roll_vol_y / roll_vol_btc.replace(0, np.nan)

    fig3, ax3 = plt.subplots(figsize=(10, 5))
    ax3.plot(df["decision_date"], vol_ratio, color="steelblue", linewidth=2)
    ax3.set_title("Chart 3: 12-Week Rolling Volatility Ratio (L/S Basket vs BTCDOM)")
    ax3.set_ylabel("Vol ratio = rolling_vol(y) / rolling_vol(btcdom_7d_ret)")
    ax3.set_xlabel("decision_date")
    ax3.grid(True, alpha=0.3)
    plt.tight_layout()
    fig3.savefig(OUT_DIR / "chart_deep_3_vol_ratio.png", dpi=150, bbox_inches="tight")
    plt.close(fig3)

    # --- Validation: terminal output ---
    valid_corr = rolling_corr[np.isfinite(rolling_corr)]
    min_roll_corr = np.min(valid_corr) if len(valid_corr) else np.nan
    max_roll_corr = np.max(valid_corr) if len(valid_corr) else np.nan
    directional_concordance = p_tr + p_bl  # Top-Right % + Bottom-Left %

    print("Minimum 12-week rolling correlation:", round(min_roll_corr, 6))
    print("Maximum 12-week rolling correlation:", round(max_roll_corr, 6))
    print("Total Directional Concordance % (Top-Right + Bottom-Left):", round(directional_concordance, 2), "%")


if __name__ == "__main__":
    main()
