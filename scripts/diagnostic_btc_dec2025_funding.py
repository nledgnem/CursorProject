"""
Quick diagnostic: inspect BTC funding in Dec 2025 and compute an annualized APY from
the average daily funding rate over that month.
"""

from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    data_lake = Path("data/curated/data_lake")
    path = data_lake / "silver_fact_funding.parquet"
    print(f"Loading {path} ...")
    df = pd.read_parquet(path, columns=["asset_id", "instrument_id", "date", "funding_rate", "exchange"])

    df["date"] = pd.to_datetime(df["date"])
    mask = (df["asset_id"] == "BTC") & (df["date"] >= "2025-12-01") & (df["date"] <= "2025-12-31")
    btc = df.loc[mask].copy()
    print("BTC funding rows in Dec 2025:", len(btc))

    if btc.empty:
        print("No BTC funding data for Dec 2025.")
        return

    # Average across instruments/exchanges per day
    btc_daily = btc.groupby("date")["funding_rate"].mean().sort_index()
    print("Unique days with BTC funding in Dec 2025:", len(btc_daily))
    print("\nFirst few daily funding rates:")
    print(btc_daily.head())
    print("\nLast few daily funding rates:")
    print(btc_daily.tail())

    mean_daily = float(btc_daily.mean())
    if mean_daily <= -1.0:
        apy = np.nan
    else:
        apy = (1.0 + mean_daily) ** 365 - 1.0

    print("\nDec 2025 BTC mean daily funding rate:", mean_daily)
    print("Implied APY from that daily mean:", apy)


if __name__ == "__main__":
    main()

