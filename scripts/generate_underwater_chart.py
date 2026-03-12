"""
Generate an underwater drawdown chart for MSM v0:
- Raw L/S Basket vs Gated L/S Basket.

Output: reports/underwater_drawdown.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MSM_PATH = ROOT / "reports" / "msm_funding_v0" / "msm_v0_full_2023_2026" / "msm_timeseries.csv"
RECON_PATH = ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"
OUT_PNG = ROOT / "reports" / "underwater_drawdown.png"


def build_gated_series(msm: pd.DataFrame) -> pd.DataFrame:
    """
    Rebuild the Market Regime Filter gate on top of msm_timeseries:
    - Funding regimes via 52-week rolling percentile of F_tk.
    - BTCDOM trend via 30d SMA of reconstructed BTCDOM.
    """
    recon = pd.read_csv(RECON_PATH, parse_dates=["date"]).sort_values("date")
    rl = recon[["date", "reconstructed_index_value"]].rename(
        columns={"reconstructed_index_value": "btcd_index"}
    )
    msm = msm.merge(
        rl.rename(columns={"date": "decision_date", "btcd_index": "btcd_index_decision"}),
        on="decision_date",
        how="left",
    )
    msm = msm.sort_values("decision_date").reset_index(drop=True)

    # Funding regime: 52-week rolling percentile of F_tk mapped to quartiles
    msm["funding_pct_rank"] = msm["F_tk"].rolling(window=52, min_periods=26).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )
    msm["funding_regime"] = pd.cut(
        msm["funding_pct_rank"],
        bins=[0.0, 0.25, 0.50, 0.75, 1.0],
        labels=["Q1: Negative/Low", "Q2: Weak", "Q3: Neutral", "Q4: High"],
        include_lowest=True,
    )

    # 30d SMA of reconstructed BTCDOM
    recon["sma_30"] = recon["reconstructed_index_value"].rolling(window=30, min_periods=30).mean()
    sma = recon[["date", "sma_30"]].rename(columns={"date": "decision_date", "sma_30": "sma_30_decision"})
    msm = msm.merge(sma, on="decision_date", how="left")
    msm["BTCDOM_Trend"] = np.where(
        msm["btcd_index_decision"] > msm["sma_30_decision"], "Rising", "Falling"
    )

    gate = (msm["funding_regime"] == "Q2: Weak") & (msm["BTCDOM_Trend"] == "Rising")
    msm["is_mrf_active"] = gate
    msm["y_gated"] = np.where(gate, msm["y"], 0.0)
    return msm


def main() -> None:
    if not MSM_PATH.exists():
        raise SystemExit(f"msm_timeseries.csv not found at {MSM_PATH}")

    msm = pd.read_csv(MSM_PATH, parse_dates=["decision_date", "next_date"])
    if not {"decision_date", "F_tk", "y"} <= set(msm.columns):
        raise SystemExit("msm_timeseries.csv missing required columns: decision_date, F_tk, y")

    msm = msm.sort_values("decision_date").reset_index(drop=True)
    msm = build_gated_series(msm)

    # Build equity curves (wealth, not return)
    df = msm[["decision_date", "y", "y_gated"]].dropna(subset=["y"]).copy()
    df["wealth_raw"] = (1 + df["y"]).cumprod()
    df["wealth_gated"] = (1 + df["y_gated"]).cumprod()

    # Underwater drawdowns
    df["peak_raw"] = df["wealth_raw"].cummax()
    df["peak_gated"] = df["wealth_gated"].cummax()
    df["dd_raw"] = df["wealth_raw"] / df["peak_raw"] - 1.0
    df["dd_gated"] = df["wealth_gated"] / df["peak_gated"] - 1.0

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.step(
        df["decision_date"],
        df["dd_raw"] * 100,
        where="post",
        label="Raw L/S Basket",
        linewidth=1.5,
        color="gray",
    )
    ax.step(
        df["decision_date"],
        df["dd_gated"] * 100,
        where="post",
        label="Gated L/S Basket",
        linewidth=2.0,
        color="red",
    )

    # Fill area under zero for gated strategy
    ax.fill_between(
        df["decision_date"],
        df["dd_gated"] * 100,
        0,
        where=(df["dd_gated"] < 0),
        step="post",
        color="red",
        alpha=0.15,
    )

    ax.set_title("Underwater Drawdown: Raw vs Gated L/S Basket", fontsize=18, pad=16)
    ax.set_xlabel("Decision Date", fontsize=14)
    ax.set_ylabel("Drawdown (%)", fontsize=14)
    ax.axhline(0.0, color="black", linewidth=1.0)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:.0f}%"))
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11, loc="lower right")
    fig.autofmt_xdate()
    plt.tight_layout()

    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PNG, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"Saved: {OUT_PNG}")


if __name__ == "__main__":
    main()

