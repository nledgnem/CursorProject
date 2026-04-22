"""
Quick diagnostic: inspect BTC funding in Dec 2025 and compute an annualized APY from
the average daily funding rate over that month.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from repo_paths import data_lake_root


def main() -> None:
    data_lake = data_lake_root()
    path = data_lake / "silver_fact_funding.parquet"
    print(f"Loading {path} ...")
    df = pd.read_parquet(path)
    fr_col = "funding_rate_raw_pct" if "funding_rate_raw_pct" in df.columns else "funding_rate"
    keep = [c for c in ("asset_id", "instrument_id", "date", fr_col, "exchange") if c in df.columns]
    df = df[keep]

    df["date"] = pd.to_datetime(df["date"])
    mask = (df["asset_id"] == "BTC") & (df["date"] >= "2025-12-01") & (df["date"] <= "2025-12-31")
    btc = df.loc[mask].copy()
    print("BTC funding rows in Dec 2025:", len(btc))

    if btc.empty:
        print("No BTC funding data for Dec 2025.")
        return

    # Average across instruments/exchanges per day
    btc_daily = btc.groupby("date")[fr_col].mean().sort_index()
    print("Unique days with BTC funding in Dec 2025:", len(btc_daily))
    print("\nFirst few daily funding rates:")
    print(btc_daily.head())
    print("\nLast few daily funding rates:")
    print(btc_daily.tail())

    mean_daily_pct = float(btc_daily.mean())
    mean_daily_dec = mean_daily_pct / 100.0
    if mean_daily_dec <= -1.0:
        apy = np.nan
    else:
        apy = (1.0 + mean_daily_dec) ** 365 - 1.0

    print("\nDec 2025 BTC mean daily funding rate (% points):", mean_daily_pct)
    print("Implied APY from that daily mean:", apy)


if __name__ == "__main__":
    main()

