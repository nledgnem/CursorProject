#!/usr/bin/env python3
"""
Quick comparison between prices_daily.parquet (wide) and fact_price.parquet (fact table).

Checks:
- Overall symbol/asset_id coverage.
- Per-asset alignment for a few symbols (SOL, TRX, ZEC, SKY):
  - number of dates in common
  - max absolute price difference
  - dates where only one of the two has data
"""

from pathlib import Path

import pandas as pd


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    prices_path = repo_root / "data" / "curated" / "prices_daily.parquet"
    fact_price_path = repo_root / "data" / "curated" / "data_lake" / "fact_price.parquet"

    df_pw = pd.read_parquet(prices_path)
    df_fp = pd.read_parquet(fact_price_path)

    # Normalize types
    df_pw.index = pd.to_datetime(df_pw.index)
    df_fp["date"] = pd.to_datetime(df_fp["date"])

    symbols = ["SOL", "TRX", "ZEC", "SKY"]

    all_symbols = [str(c) for c in df_pw.columns]
    mapped_asset_ids = {s.upper() for s in all_symbols}
    fact_asset_ids = set(df_fp["asset_id"].unique())

    print("=== Overall Coverage ===")
    print(f"prices_daily symbols: {len(all_symbols)}")
    print(f"fact_price asset_ids: {len(fact_asset_ids)}")
    missing_in_fact = sorted(mapped_asset_ids - fact_asset_ids)
    missing_in_prices = sorted(fact_asset_ids - mapped_asset_ids)
    print(f"symbols (uppercased) missing in fact_price (first 20): {missing_in_fact[:20]}")
    print(f"asset_ids present only in fact_price (first 20): {missing_in_prices[:20]}")
    print()

    print("=== Per-asset comparison (prices_daily vs fact_price) ===")
    for sym in symbols:
        col = sym
        aid = sym.upper()
        print(f"\n{sym}:")
        if col not in df_pw.columns:
            print("  - NOT present in prices_daily columns")
            continue
        if aid not in fact_asset_ids:
            print("  - NOT present in fact_price.asset_id")
            continue

        wide_series = df_pw[col].dropna()
        fact_series = (
            df_fp[df_fp["asset_id"] == aid]
            .set_index("date")["close"]
            .sort_index()
        )

        common_dates = wide_series.index.intersection(fact_series.index)
        only_wide = sorted(set(wide_series.index) - set(fact_series.index))
        only_fact = sorted(set(fact_series.index) - set(wide_series.index))

        if len(common_dates) == 0:
            print("  - No overlapping dates between prices_daily and fact_price")
            continue

        diffs = (wide_series.loc[common_dates] - fact_series.loc[common_dates]).abs()
        max_diff = float(diffs.max())

        print(f"  - dates in both: {len(common_dates)}")
        print(f"  - max abs(price_daily - fact_price): {max_diff:.10f}")
        print(f"  - dates only in prices_daily: {len(only_wide)}")
        print(f"  - dates only in fact_price: {len(only_fact)}")


if __name__ == "__main__":
    main()

