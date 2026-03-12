"""
Generate equity_curve_comparison.png: raw + gated L/S and macro indices,
with light green background shading when Gate is ON.
Also prints Q3/Q4 diagnostic (Sharpe and MDD for Raw L/S in Q3/Q4 only).
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set(style="whitegrid")
ROOT = Path(__file__).resolve().parents[1]
MSM_PATH = ROOT / "reports" / "msm_funding_v0" / "msm_v0_full_2023_2026" / "msm_timeseries.csv"
RECON_PATH = ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"
BINANCE_PATH = ROOT / "data" / "curated" / "data_lake" / "binance_btcdom.csv"
OUT_DIR = ROOT / "notebooks"


def main():
    msm = pd.read_csv(MSM_PATH, parse_dates=["decision_date", "next_date"])
    msm = msm[["decision_date", "next_date", "F_tk", "y"]].copy().sort_values("decision_date").reset_index(drop=True)

    recon = pd.read_csv(RECON_PATH, parse_dates=["date"]).sort_values("date")
    rl = recon[["date", "reconstructed_index_value"]].rename(columns={"reconstructed_index_value": "btcd_index"})
    msm = msm.merge(rl.rename(columns={"date": "decision_date", "btcd_index": "btcd_index_decision"}), on="decision_date", how="left")
    msm = msm.merge(rl.rename(columns={"date": "next_date", "btcd_index": "btcd_index_next"}), on="next_date", how="left")
    msm["ret_btcdom_recon"] = msm["btcd_index_next"] / msm["btcd_index_decision"] - 1.0

    bdf = pd.read_csv(BINANCE_PATH, parse_dates=["timestamp"])
    ts = bdf["timestamp"]
    if getattr(ts.dt, "tz", None) is not None:
        bdf["timestamp"] = ts.dt.tz_convert(None)
    bdf["date"] = bdf["timestamp"].dt.normalize()
    bl = bdf[["date", "close"]].rename(columns={"close": "binance_index"})
    msm = msm.merge(bl.rename(columns={"date": "decision_date", "binance_index": "binance_decision"}), on="decision_date", how="left")
    msm = msm.merge(bl.rename(columns={"date": "next_date", "binance_index": "binance_next"}), on="next_date", how="left")
    msm["ret_btcdom_binance"] = msm["binance_next"] / msm["binance_decision"] - 1.0

    recon["sma_30"] = recon["reconstructed_index_value"].rolling(window=30, min_periods=30).mean()
    sma = recon[["date", "sma_30"]].rename(columns={"date": "decision_date", "sma_30": "sma_30_decision"})
    msm = msm.merge(sma, on="decision_date", how="left")
    msm = msm.sort_values("decision_date").reset_index(drop=True)
    msm["funding_pct_rank"] = msm["F_tk"].rolling(window=52, min_periods=26).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )
    msm["funding_regime"] = pd.cut(
        msm["funding_pct_rank"],
        bins=[0.0, 0.25, 0.50, 0.75, 1.0],
        labels=["Q1: Negative/Low", "Q2: Weak", "Q3: Neutral", "Q4: High"],
        include_lowest=True,
    )
    msm["BTCDOM_Trend"] = np.where(msm["btcd_index_decision"] > msm["sma_30_decision"], "Rising", "Falling")
    gate = (msm["funding_regime"] == "Q2: Weak") & (msm["BTCDOM_Trend"] == "Rising")
    msm["is_mrf_active"] = gate
    msm["y_filtered"] = np.where(gate, msm["y"], 0.0)
    msm["recon_filtered"] = np.where(gate, msm["ret_btcdom_recon"], 0.0)
    msm["binance_filtered"] = np.where(gate, msm["ret_btcdom_binance"], 0.0)
    msm["cum_raw_ls"] = (1 + msm["y"]).cumprod() - 1
    msm["cum_filtered_ls"] = (1 + msm["y_filtered"]).cumprod() - 1
    msm["cum_recon_btcdom"] = (1 + msm["ret_btcdom_recon"]).cumprod() - 1
    msm["cum_binance_btcdom"] = (1 + msm["ret_btcdom_binance"]).cumprod() - 1
    msm["cum_recon_filtered"] = (1 + msm["recon_filtered"]).cumprod() - 1
    msm["cum_binance_filtered"] = (1 + msm["binance_filtered"]).cumprod() - 1

    df = msm.dropna(subset=["y", "funding_regime", "BTCDOM_Trend", "ret_btcdom_recon", "ret_btcdom_binance"]).copy()
    if df.empty:
        print("No data for chart.")
        return

    # Chart
    sns.set_context("talk")
    fig, ax = plt.subplots(figsize=(14, 8))
    for i in range(len(df)):
        if df["is_mrf_active"].iloc[i]:
            ax.axvspan(df["decision_date"].iloc[i], df["next_date"].iloc[i], color="green", alpha=0.12, zorder=0)
    ax.set_facecolor("white")
    # Core four series (avoid clutter): Raw L/S, Gated L/S, Gated macro indices
    ax.plot(
        df["decision_date"],
        df["cum_raw_ls"] * 100,
        label="Raw L/S Basket",
        linewidth=2,
        color="gray",
        linestyle="--",
        drawstyle="steps-post",
        zorder=3,
    )
    ax.plot(
        df["decision_date"],
        df["cum_filtered_ls"] * 100,
        label="Gated L/S Basket",
        linewidth=2.5,
        color="darkgreen",
        linestyle="-",
        drawstyle="steps-post",
        zorder=4,
    )
    ax.plot(
        df["decision_date"],
        df["cum_recon_filtered"] * 100,
        label="Gated Reconstructed BTCDOM",
        linewidth=1.3,
        color="steelblue",
        linestyle="-.",
        alpha=0.9,
        drawstyle="steps-post",
        zorder=3,
    )
    ax.plot(
        df["decision_date"],
        df["cum_binance_filtered"] * 100,
        label="Gated Binance BTCDOM",
        linewidth=1.3,
        color="darkorange",
        linestyle="-.",
        alpha=0.9,
        drawstyle="steps-post",
        zorder=3,
    )
    ax.set_title("MSM v0: Raw vs Gated L/S + Gated Macro (shaded = Gate ON)", fontsize=22, pad=20)
    ax.set_xlabel("Decision Date", fontsize=18)
    ax.set_ylabel("Cumulative Return (%)", fontsize=18)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:.0f}%"))
    ax.legend(fontsize=11, loc="upper left", framealpha=0.95)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    path = OUT_DIR / "equity_curve_comparison.png"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")

    # Q3/Q4 diagnostic
    q34 = (df["funding_regime"] == "Q3: Neutral") | (df["funding_regime"] == "Q4: High")
    y_q34 = df.loc[q34, "y"]
    if len(y_q34) == 0:
        print("\nNo weeks in Q3 or Q4.")
        return
    m, s = y_q34.mean(), y_q34.std()
    sharpe = (m / s) * np.sqrt(52) if s and s != 0 and not np.isnan(s) else np.nan
    wealth = (1 + y_q34).cumprod()
    mdd = ((wealth - wealth.cummax()) / wealth.cummax()).min() * 100
    print("\n=== Raw L/S Basket: Q3/Q4 Funding Regimes Only (Diagnostic for PM) ===")
    print(f"Weeks in Q3 or Q4: {len(y_q34)}")
    print(f"Annualized Sharpe (RF=0, 52 periods/yr): {sharpe:.4f}")
    print(f"Max Drawdown (%): {mdd:.2f}%")


if __name__ == "__main__":
    main()
