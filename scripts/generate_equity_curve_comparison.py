"""
Generate equity_curve_comparison.png: raw + gated L/S and macro indices,
with light green background shading when Gate is ON.
Also prints Q3/Q4 diagnostic (Sharpe and MDD for Raw L/S in Q3/Q4 only).
"""
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

sns.set(style="whitegrid")
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.macro_regime.btcdom_trend import (  # noqa: E402
    apply_gate,
    compute_btcdom_trend,
    compute_mrf_gate,
)
MSM_PATH = ROOT / "reports" / "msm_funding_v0" / "msm_v0_full_2023_2026" / "msm_timeseries.csv"
RECON_PATH = ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"
BINANCE_PATH = ROOT / "data" / "curated" / "data_lake" / "binance_btcdom.csv"
OUT_DIR = ROOT / "notebooks"


def main():
    msm = pd.read_csv(MSM_PATH, parse_dates=["decision_date", "next_date"])
    msm = msm[["decision_date", "next_date", "F_tk", "y"]].copy().sort_values("decision_date").reset_index(drop=True)
    msm["F_tk_apr"] = msm["F_tk"] * 365.0 * 100.0  # Unit: APR % (DATA_DICTIONARY.md)

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
    msm["funding_pct_rank"] = msm["F_tk_apr"].rolling(window=52, min_periods=26).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )
    msm["funding_regime"] = pd.cut(
        msm["funding_pct_rank"],
        bins=[0.0, 0.25, 0.50, 0.75, 1.0],
        labels=["Q1: Negative/Low", "Q2: Weak", "Q3: Neutral", "Q4: High"],
        include_lowest=True,
    )
    msm["BTCDOM_Trend"] = compute_btcdom_trend(msm["btcd_index_decision"], msm["sma_30_decision"])
    # ADR 003 (2026-08-17): the MRF gate is funding-only; BTCDOM_Trend was
    # removed from it. This chart still requires BTCDOM_Trend below because the
    # chart itself compares BTCDOM reconstructions -- that is a data requirement
    # of this script, not a gate input.
    msm["is_mrf_active"] = compute_mrf_gate(msm["funding_regime"])

    # Drop un-evaluable rows BEFORE building any cumulative series. Previously the
    # trend was a bare np.where, so a missing BTCDOM index produced a fabricated
    # "Falling", the gate read False, and the row contributed a silent 0.0 to every
    # cumulative curve below. No silent truncation -- report what was dropped.
    required = ["y", "funding_regime", "BTCDOM_Trend", "ret_btcdom_recon", "ret_btcdom_binance"]
    n_before = len(msm)
    df = msm.dropna(subset=required).copy()
    n_dropped = n_before - len(df)
    if n_dropped:
        print(
            f"[macro] Dropped {n_dropped}/{n_before} rows with incomplete macro inputs "
            f"({', '.join(required)}) before computing cumulative curves."
        )
    if df.empty:
        print("No data for chart.")
        return

    gate = df["is_mrf_active"].fillna(False).astype(bool)
    df["y_filtered"] = apply_gate(df["y"], gate)
    df["recon_filtered"] = apply_gate(df["ret_btcdom_recon"], gate)
    df["binance_filtered"] = apply_gate(df["ret_btcdom_binance"], gate)
    df["cum_raw_ls"] = (1 + df["y"]).cumprod() - 1
    df["cum_filtered_ls"] = (1 + df["y_filtered"]).cumprod() - 1
    df["cum_recon_btcdom"] = (1 + df["ret_btcdom_recon"]).cumprod() - 1
    df["cum_binance_btcdom"] = (1 + df["ret_btcdom_binance"]).cumprod() - 1
    df["cum_recon_filtered"] = (1 + df["recon_filtered"]).cumprod() - 1
    df["cum_binance_filtered"] = (1 + df["binance_filtered"]).cumprod() - 1

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
