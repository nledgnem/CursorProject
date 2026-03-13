"""
L/S Basket vs. Raw BTCDOM — Head-to-head comparison and regime diagnostics.
Compares Top 30 L/S basket return (y) with raw BTCDOM weekly return over last 2 years.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats

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

# Regime colors for axvspan (Q1–Q4)
REGIME_COLORS = {
    "Q1": ("#e74c3c", 0.2),   # light red
    "Q2": ("#f39c12", 0.2),   # light orange
    "Q3": ("#2ecc71", 0.2),   # light green
    "Q4": ("#95a5a6", 0.2),   # light gray
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
        print("No data after 2-year filter. Check date overlap of timeseries and BTCDOM.")
        return

    # Bin F_tk_apr into 4 equal-sized quartiles (PM-facing: use APR)
    df["F_tk_quartile"], _ = pd.qcut(
        df["F_tk_apr"],
        q=4,
        labels=["Q1", "Q2", "Q3", "Q4"],
        retbins=True,
        duplicates="drop",
    )

    y_log = df["y"].astype(float)
    btcdom_log = df["btcdom_7d_ret"].astype(float)
    dates = df["decision_date"]

    # Cumulative arithmetic returns: (exp(cumsum) - 1) * 100
    cum_y = (np.exp(y_log.cumsum()) - 1.0) * 100
    cum_btcdom = (np.exp(btcdom_log.cumsum()) - 1.0) * 100

    # --- Chart 1: Head-to-head cumulative returns ---
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(dates, cum_y, color="blue", linestyle="-", linewidth=2, label="L/S Basket")
    ax1.plot(
        dates,
        cum_btcdom,
        color="black",
        linestyle="--",
        linewidth=2,
        label="BTCDOM (7d)",
    )
    ax1.set_title("Chart 1: Cumulative Return (L/S Basket vs. True 7-Day BTCDOM)")
    ax1.set_ylabel("Cumulative return (%)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.axhline(0, color="gray", linewidth=0.8)
    plt.tight_layout()
    fig1.savefig(OUT_DIR / "chart_1_ls_vs_btcdom.png", dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Chart 2: Correlation scatter & statistics ---
    pearson_r, pearson_p = stats.pearsonr(btcdom_log, y_log)
    spearman_r, spearman_p = stats.spearmanr(btcdom_log, y_log)

    # Line of best fit
    slope, intercept = np.polyfit(btcdom_log, y_log, 1)
    x_fit = np.linspace(btcdom_log.min(), btcdom_log.max(), 100)
    y_fit = slope * x_fit + intercept

    fig2, ax2 = plt.subplots(figsize=(7, 6))
    ax2.scatter(btcdom_log, y_log, alpha=0.6, s=40, color="steelblue", edgecolors="white")
    ax2.plot(x_fit, y_fit, color="darkred", linewidth=2, label="Linear fit")
    ax2.set_xlabel("BTCDOM 7-day log return (btcdom_7d_ret)")
    ax2.set_ylabel("L/S Basket weekly return (y)")
    ax2.set_title("Chart 2: Weekly Return Correlation (L/S Basket vs. BTCDOM)")
    textstr = f"Pearson r = {pearson_r:.4f}\nSpearman ρ = {spearman_r:.4f}"
    props = dict(boxstyle="round", facecolor="wheat", alpha=0.8)
    ax2.text(0.05, 0.95, textstr, transform=ax2.transAxes, fontsize=11, verticalalignment="top", bbox=props)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.axhline(0, color="gray", linewidth=0.5)
    ax2.axvline(0, color="gray", linewidth=0.5)
    plt.tight_layout()
    fig2.savefig(OUT_DIR / "chart_2_correlation.png", dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Chart 3: Cumulative returns with regime overlays ---
    fig3, ax3 = plt.subplots(figsize=(10, 5))
    # Draw axvspan for each week by quartile
    for i in range(len(df)):
        left = dates.iloc[i]
        right = dates.iloc[i + 1] if i + 1 < len(df) else left + pd.Timedelta(days=7)
        q = df["F_tk_quartile"].iloc[i]
        color, alpha = REGIME_COLORS.get(str(q), ("#bdc3c7", 0.2))
        ax3.axvspan(left, right, facecolor=color, alpha=alpha, zorder=0)
    ax3.plot(dates, cum_y, color="blue", linestyle="-", linewidth=2, label="L/S Basket", zorder=2)
    ax3.plot(
        dates,
        cum_btcdom,
        color="black",
        linestyle="--",
        linewidth=2,
        label="BTCDOM",
        zorder=2,
    )
    ax3.set_title("Chart 3: Cumulative Returns overlaid with F_tk_apr Regimes")
    ax3.set_ylabel("Cumulative return (%)")
    ax3.set_xlabel("decision_date")
    from matplotlib.patches import Patch
    legend_handles = [
        Patch(facecolor=REGIME_COLORS["Q1"][0], alpha=REGIME_COLORS["Q1"][1], label="Q1"),
        Patch(facecolor=REGIME_COLORS["Q2"][0], alpha=REGIME_COLORS["Q2"][1], label="Q2"),
        Patch(facecolor=REGIME_COLORS["Q3"][0], alpha=REGIME_COLORS["Q3"][1], label="Q3"),
        Patch(facecolor=REGIME_COLORS["Q4"][0], alpha=REGIME_COLORS["Q4"][1], label="Q4"),
    ]
    leg_regime = ax3.legend(handles=legend_handles, title="F_tk_apr quartile", loc="upper right")
    ax3.add_artist(leg_regime)
    ax3.legend(loc="upper left", title="Series")
    ax3.grid(True, alpha=0.3)
    ax3.axhline(0, color="gray", linewidth=0.8)
    plt.tight_layout()
    fig3.savefig(OUT_DIR / "chart_3_regime_overlay.png", dpi=150, bbox_inches="tight")
    plt.close(fig3)

    # --- Chart 4: Regime isolation (Cold vs Warm) ---
    is_cold = df["F_tk_quartile"].isin(["Q1", "Q2"])
    is_warm = df["F_tk_quartile"].isin(["Q3", "Q4"])
    y_cond_cold = np.where(is_cold, y_log, 0.0)
    btcdom_cond_cold = np.where(is_cold, btcdom_log, 0.0)
    y_cond_warm = np.where(is_warm, y_log, 0.0)
    btcdom_cond_warm = np.where(is_warm, btcdom_log, 0.0)

    cum_y_cold = (np.exp(np.cumsum(y_cond_cold)) - 1.0) * 100
    cum_btcdom_cold = (np.exp(np.cumsum(btcdom_cond_cold)) - 1.0) * 100
    cum_y_warm = (np.exp(np.cumsum(y_cond_warm)) - 1.0) * 100
    cum_btcdom_warm = (np.exp(np.cumsum(btcdom_cond_warm)) - 1.0) * 100

    fig4, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    ax_top.plot(dates, cum_y_cold, color="blue", linestyle="-", linewidth=2, label="L/S Basket")
    ax_top.plot(dates, cum_btcdom_cold, color="black", linestyle="--", linewidth=2, label="BTCDOM")
    ax_top.set_title("Q1 & Q2 — Cold Regimes (returns zeroed in Q3/Q4)")
    ax_top.set_ylabel("Cumulative return (%)")
    ax_top.legend()
    ax_top.grid(True, alpha=0.3)
    ax_top.axhline(0, color="gray", linewidth=0.8)

    ax_bot.plot(dates, cum_y_warm, color="blue", linestyle="-", linewidth=2, label="L/S Basket")
    ax_bot.plot(dates, cum_btcdom_warm, color="black", linestyle="--", linewidth=2, label="BTCDOM")
    ax_bot.set_title("Q3 & Q4 — Warm Regimes (returns zeroed in Q1/Q2)")
    ax_bot.set_ylabel("Cumulative return (%)")
    ax_bot.set_xlabel("decision_date")
    ax_bot.legend()
    ax_bot.grid(True, alpha=0.3)
    ax_bot.axhline(0, color="gray", linewidth=0.8)

    fig4.suptitle(
        "Chart 4: Regime Isolation - Conditional Equity Curves by Funding Temperature",
        fontsize=12,
        y=1.02,
    )
    plt.tight_layout()
    fig4.savefig(OUT_DIR / "chart_4_regime_split.png", dpi=150, bbox_inches="tight")
    plt.close(fig4)

    # --- Validation: print correlations to terminal ---
    print("Pearson correlation (L/S Basket vs BTCDOM):", round(pearson_r, 6))
    print("Spearman correlation (L/S Basket vs BTCDOM):", round(spearman_r, 6))


if __name__ == "__main__":
    main()
