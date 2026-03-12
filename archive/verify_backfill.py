#!/usr/bin/env python3
"""Verify backfill results."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("BACKFILL VERIFICATION")
print("=" * 80)

# Check fact_price
print("\n=== FACT_PRICE ===")
price_df = pl.read_parquet(str(data_lake_dir / "fact_price.parquet"))
print(f"Total rows: {len(price_df):,}")
print(f"Date range: {price_df['date'].min()} to {price_df['date'].max()}")
print(f"Unique assets: {price_df['asset_id'].n_unique()}")

# Check BTC specifically
btc = price_df.filter(pl.col('asset_id') == 'BTC')
if len(btc) > 0:
    print(f"\nBTC rows: {len(btc):,}")
    print(f"BTC date range: {btc['date'].min()} to {btc['date'].max()}")
else:
    print("\nBTC: No data found")

# Check for historical data (before 2020)
early = price_df.filter(pl.col('date') < pl.date(2020, 1, 1))
print(f"\nRecords before 2020: {len(early):,}")

if len(early) > 0:
    print(f"Earliest date in dataset: {price_df['date'].min()}")
    print("\nTop 10 assets by historical coverage (pre-2020):")
    top = early.group_by('asset_id').agg(pl.len().alias('count')).sort('count', descending=True).head(10)
    # Convert to dicts to avoid Unicode issues when printing
    for row in top.to_dicts():
        print(f"  {row['asset_id']}: {row['count']} records")
else:
    print("WARNING: No historical data found before 2020!")

# Check fact_marketcap
print("\n=== FACT_MARKETCAP ===")
mcap_df = pl.read_parquet(str(data_lake_dir / "fact_marketcap.parquet"))
print(f"Total rows: {len(mcap_df):,}")
print(f"Date range: {mcap_df['date'].min()} to {mcap_df['date'].max()}")

# Check fact_volume
print("\n=== FACT_VOLUME ===")
vol_df = pl.read_parquet(str(data_lake_dir / "fact_volume.parquet"))
print(f"Total rows: {len(vol_df):,}")
print(f"Date range: {vol_df['date'].min()} to {vol_df['date'].max()}")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

earliest = min(price_df['date'].min(), mcap_df['date'].min(), vol_df['date'].min())
latest = max(price_df['date'].max(), mcap_df['date'].max(), vol_df['date'].max())

print(f"Overall date range: {earliest} to {latest}")
days = (latest - earliest).days + 1
print(f"Total days: {days:,}")

from datetime import date as date_type
if earliest < date_type(2020, 1, 1):
    print("OK: Historical backfill appears successful!")
    print(f"OK: Data extends back to {earliest} ({days:,} days of history)")
else:
    print("WARNING: Historical backfill may not have completed")
    print(f"WARNING: Earliest date is {earliest} (expected 2013-2014)")
