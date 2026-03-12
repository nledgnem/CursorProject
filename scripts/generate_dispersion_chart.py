"""
Generate a worst-week cross-sectional dispersion chart for MSM v0:
- Identify the week with the most negative L/S spread y.
- For that week, compute individual arithmetic returns of the 30 ALT basket members.
- Plot a horizontal bar chart of those 30 returns.

Output: reports/worst_week_dispersion.png
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MSM_PATH = ROOT / "reports" / "msm_funding_v0" / "msm_v0_full_2023_2026" / "msm_timeseries.csv"
PRICE_PATH = ROOT / "data" / "curated" / "data_lake" / "fact_price.parquet"
OUT_PNG = ROOT / "reports" / "worst_week_dispersion.png"


def get_close_asof(prices: pd.DataFrame, asset_id: str, asof: date) -> float | None:
    sub = prices[prices["asset_id"] == asset_id]
    if sub.empty:
        return None
    sub = sub[sub["date"] <= np.datetime64(asof)]
    if sub.empty:
        return None
    row = sub.sort_values("date").iloc[-1]
    return float(row["close"])


def main() -> None:
    if not MSM_PATH.exists():
        raise SystemExit(f"msm_timeseries.csv not found at {MSM_PATH}")
    if not PRICE_PATH.exists():
        raise SystemExit(f"fact_price.parquet not found at {PRICE_PATH}")

    msm = pd.read_csv(MSM_PATH, parse_dates=["decision_date", "next_date"])
    if not {"decision_date", "next_date", "y", "basket_members"} <= set(msm.columns):
        raise SystemExit("msm_timeseries.csv missing required columns: decision_date, next_date, y, basket_members")

    # Find worst y week (most negative L/S spread)
    msmd = msm.dropna(subset=["y"]).copy()
    worst_idx = msmd["y"].idxmin()
    worst_row = msmd.loc[worst_idx]
    d0 = worst_row["decision_date"].date()
    d1 = worst_row["next_date"].date()
    basket_members = [s.strip() for s in str(worst_row["basket_members"]).split(",") if s.strip()]

    print(f"Worst week decision_date: {d0}, next_date: {d1}, y={worst_row['y']:.4f}")
    print(f"Basket size: {len(basket_members)}")

    prices = pd.read_parquet(PRICE_PATH)
    # Ensure date column is datetime64[ns]
    if not np.issubdtype(prices["date"].dtype, np.datetime64):
        prices["date"] = pd.to_datetime(prices["date"])

    records = []
    for aid in basket_members:
        prev_close = get_close_asof(prices, aid, d0)
        next_close = get_close_asof(prices, aid, d1)
        if prev_close is None or next_close is None or prev_close <= 0:
            ret = np.nan
        else:
            ret = next_close / prev_close - 1.0
        records.append((aid, ret))

    df_ret = pd.DataFrame(records, columns=["asset_id", "ret"])
    df_ret = df_ret.dropna(subset=["ret"])
    df_ret = df_ret.sort_values("ret")

    # Colors: red if alt dropped (ret < 0, good for short), green if rallied (ret > 0, bad for short)
    colors = ["red" if r < 0 else "green" for r in df_ret["ret"]]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(df_ret["asset_id"], df_ret["ret"] * 100, color=colors)
    ax.set_title(
        f"Worst Week Dispersion: Alt Returns in Basket (Short Side)\\nWeek {d0} → {d1}",
        fontsize=16,
        pad=14,
    )
    ax.set_xlabel("Arithmetic Return (%)", fontsize=14)
    ax.axvline(0.0, color="black", linewidth=1.0)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:.0f}%"))
    plt.tight_layout()

    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PNG, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"Saved: {OUT_PNG}")


if __name__ == "__main__":
    main()

