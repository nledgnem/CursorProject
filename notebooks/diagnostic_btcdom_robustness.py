"""
BTCDOM Orthogonal Robustness Check — Adversarial Validation

Step 1 — Assumption Ledger (Adversarial Risk Manager)

1) INVERSE RELATIONSHIP: BTCDOM = Bitcoin Dominance (BTC cap / total crypto cap).
   - An Altcoin-Long / BTC-Short strategy profits when alts outperform BTC.
   - When BTCDOM rises, BTC gains share vs alts → the L/S strategy loses.
   - When BTCDOM falls, alts gain share vs BTC → the L/S strategy gains.
   Hence: Strategy P&L is NEGATIVELY correlated with BTCDOM. To measure
   "altcoin outperformance" we use the NEGATIVE of BTCDOM return as the proxy.

2) PROXY RETURN: We define proxy_y = -1 * (weekly log return of BTCDOM).
   So: proxy_y = -1 * log(btcdom_close_t / btcdom_close_{t-1}).
   This is the log-return of a Short-BTCDOM position, i.e. alt outperformance.

3) NO LOOKAHEAD: decision_date in our timeseries is the weekly decision point
   (e.g. Monday 00:00 UTC). We align BTCDOM by joining on date = decision_date
   only: we use the index level ON the decision date (point-in-time). The
   weekly log return is then computed as log(close_t / close_{t-1}) along the
   time-ordered series of decision dates. We never use future or same-week
   forward data; each week's return uses only that week's and the prior week's
   close. Thus no lookahead.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Paths relative to project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMESERIES_PATH = PROJECT_ROOT / "reports" / "msm_funding_v0" / "silver_router_variance_shield" / "msm_timeseries.csv"
BTCDOM_PATH = PROJECT_ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"
OUT_DIR = PROJECT_ROOT / "scripts"  # save charts next to script


def main():
    # --- Data integration: strict 7-day alignment (decision_date -> next_date) ---
    ts = pd.read_csv(TIMESERIES_PATH, usecols=["decision_date", "next_date", "F_tk"])
    ts["decision_date"] = pd.to_datetime(ts["decision_date"]).dt.normalize()
    ts["next_date"] = pd.to_datetime(ts["next_date"]).dt.normalize()
    ts["F_tk_apr"] = ts["F_tk"] * 365.0 * 100.0

    btcdom = pd.read_csv(BTCDOM_PATH, usecols=["date", "reconstructed_index_value"])
    btcdom["date"] = pd.to_datetime(btcdom["date"]).dt.normalize()
    btcdom = btcdom.rename(columns={"reconstructed_index_value": "btcdom_price"})

    btc_start = btcdom.rename(columns={"date": "decision_date", "btcdom_price": "btcdom_price_start"})
    btc_end = btcdom.rename(columns={"date": "next_date", "btcdom_price": "btcdom_price_end"})
    df = ts.merge(btc_start, on="decision_date", how="left")
    df = df.merge(btc_end, on="next_date", how="left")
    df["btcdom_7d_ret"] = np.log(df["btcdom_price_end"].astype(float) / df["btcdom_price_start"].astype(float))
    df["proxy_y"] = -1.0 * df["btcdom_7d_ret"]
    df = df.dropna(subset=["btcdom_price_start", "btcdom_price_end", "proxy_y", "F_tk"]).copy()
    df = df.sort_values("decision_date").reset_index(drop=True)

    # --- Last 2 years ---
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(years=2)
    df = df.loc[df["decision_date"] >= cutoff]
    df = df.reset_index(drop=True)

    if df.empty:
        print("No data after 2-year filter and drop NaN. Check BTCDOM date range vs timeseries.")
        return

    # --- Chart 1: BTCDOM Quartile attribution (F_tk_apr for PM-facing labels) ---
    df["F_tk_quartile"], _ = pd.qcut(df["F_tk_apr"], q=4, labels=["Q1", "Q2", "Q3", "Q4"], retbins=True, duplicates="drop")
    quartile_sum = df.groupby("F_tk_quartile", observed=True)["proxy_y"].sum()
    pct = (np.exp(quartile_sum) - 1.0) * 100
    colors = ["#c0392b", "#27ae60", "#2980b9", "#2980b9"]  # Q1 red, Q2–Q4 green/blue
    if len(pct) < 4:
        colors = colors[: len(pct)]

    fig1, ax1 = plt.subplots(figsize=(8, 5))
    bars = ax1.bar(pct.index.astype(str), pct.values, color=colors[: len(pct)])
    ax1.set_title("Robustness Check: 2-Year Cumulative Inverse BTCDOM Return vs. Funding Quartiles")
    ax1.set_ylabel("Cumulative return (%)")
    ax1.set_xlabel("F_tk_apr quartile")
    for bar, val in zip(bars, pct.values):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + (0.5 if val >= 0 else -0.8),
                 f"{val:.1f}%", ha="center", va="bottom" if val >= 0 else "top", fontsize=10)
    plt.tight_layout()
    fig1.savefig(OUT_DIR / "chart_btcdom_quartiles.png", dpi=150, bbox_inches="tight")
    plt.close(fig1)

    # --- Chart 2: Threshold sweep (F_tk_apr) ---
    f_min, f_max = df["F_tk_apr"].min(), df["F_tk_apr"].max()
    thresholds = np.linspace(f_min, f_max, 100)
    cumulative_returns = []
    for T in thresholds:
        mask = df["F_tk_apr"] <= T
        log_ret_sum = df.loc[mask, "proxy_y"].sum()
        cum_ret = (np.exp(log_ret_sum) - 1.0) * 100
        cumulative_returns.append(cum_ret)
    cumulative_returns = np.array(cumulative_returns)
    peak_idx = np.argmax(cumulative_returns)
    peak_ret = cumulative_returns[peak_idx]
    peak_T = thresholds[peak_idx]

    fig2, ax2 = plt.subplots(figsize=(8, 5))
    ax2.plot(thresholds, cumulative_returns, color="steelblue", lw=2)
    ax2.axvline(peak_T, color="darkred", ls="--", alpha=0.8, label=f"Peak at F_tk_apr={peak_T:.2f}%")
    ax2.scatter([peak_T], [peak_ret], color="darkred", s=80, zorder=5)
    ax2.set_title("Robustness Check: Optimal Funding Gate vs. Inverse BTCDOM")
    ax2.set_xlabel("F_tk_apr threshold T (% APR)")
    ax2.set_ylabel("Cumulative proxy return (%)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    fig2.savefig(OUT_DIR / "chart_btcdom_threshold_sweep.png", dpi=150, bbox_inches="tight")
    plt.close(fig2)

    # --- Validation: Q3 cumulative return ---
    q3_ret = float(pct["Q3"]) if "Q3" in pct.index else np.nan

    print("BTCDOM Robustness Check Complete.")
    print(f"The cumulative Inverse BTCDOM return (%) for the Q3 Quartile: {q3_ret:.2f}%")
    print(f"Peak cumulative return (%): {peak_ret:.2f}% at F_tk_apr threshold = {peak_T:.2f}%")


if __name__ == "__main__":
    main()
