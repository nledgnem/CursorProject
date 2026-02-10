#!/usr/bin/env python3
"""Verify that the data has been updated correctly after backfill."""

import polars as pl
from datetime import date
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("DATA UPDATE VERIFICATION")
print("=" * 80)

# Load fact tables
price_df = pl.read_parquet(str(data_lake_dir / "fact_price.parquet"))
mcap_df = pl.read_parquet(str(data_lake_dir / "fact_marketcap.parquet"))
vol_df = pl.read_parquet(str(data_lake_dir / "fact_volume.parquet"))

print("\n=== FACT TABLE OVERVIEW ===")
print(f"Price table: {len(price_df):,} rows")
print(f"Marketcap table: {len(mcap_df):,} rows")
print(f"Volume table: {len(vol_df):,} rows")

print("\n=== DATE RANGES ===")
print(f"Price: {price_df['date'].min()} to {price_df['date'].max()}")
print(f"Marketcap: {mcap_df['date'].min()} to {mcap_df['date'].max()}")
print(f"Volume: {vol_df['date'].min()} to {vol_df['date'].max()}")

print("\n=== UNIQUE ASSETS ===")
print(f"Prices: {price_df['asset_id'].n_unique()}")
print(f"Marketcaps: {mcap_df['asset_id'].n_unique()}")
print(f"Volumes: {vol_df['asset_id'].n_unique()}")

# Check BTC and ETH specifically
print("\n=== BTC DETAILED CHECK ===")
btc_price = price_df.filter(pl.col('asset_id') == 'BTC')
print(f"BTC price records: {len(btc_price):,}")
print(f"BTC date range: {btc_price['date'].min()} to {btc_price['date'].max()}")
btc_early = btc_price.filter(pl.col('date') < pl.date(2020, 1, 1))
print(f"BTC records before 2020: {len(btc_early):,}")

print("\n=== ETH DETAILED CHECK ===")
eth_price = price_df.filter(pl.col('asset_id') == 'ETH')
print(f"ETH price records: {len(eth_price):,}")
print(f"ETH date range: {eth_price['date'].min()} to {eth_price['date'].max()}")
eth_early = eth_price.filter(pl.col('date') < pl.date(2020, 1, 1))
print(f"ETH records before 2020: {len(eth_early):,}")

# Historical data breakdown
print("\n=== HISTORICAL DATA BREAKDOWN (Pre-2020) ===")
early = price_df.filter(pl.col('date') < pl.date(2020, 1, 1))
print(f"Total records before 2020: {len(early):,}")

print("\nBy year:")
for year in range(2013, 2020):
    year_data = early.filter(
        (pl.col('date') >= pl.date(year, 1, 1)) & 
        (pl.col('date') < pl.date(year + 1, 1, 1))
    )
    print(f"  {year}: {len(year_data):,} records")

print("\nTop 15 assets by historical coverage (pre-2020):")
top = early.group_by('asset_id').agg(pl.len().alias('count')).sort('count', descending=True).head(15)
for row in top.to_dicts():
    print(f"  {row['asset_id']}: {row['count']:,} records")

# Check for data quality
print("\n=== DATA QUALITY CHECKS ===")
print("Checking for duplicates...")
dupe_check = price_df.group_by(['asset_id', 'date']).agg(pl.len().alias('count')).filter(pl.col('count') > 1)
if len(dupe_check) > 0:
    print(f"  WARNING: Found {len(dupe_check)} duplicate (asset_id, date) pairs in prices")
else:
    print("  OK: No duplicates found in prices")

print("Checking for null values...")
price_nulls = price_df.filter(pl.col('close').is_null())
if len(price_nulls) > 0:
    print(f"  WARNING: Found {len(price_nulls)} null prices")
else:
    print("  OK: No null prices found")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
earliest_date = price_df['date'].min()
total_days = (price_df['date'].max() - earliest_date).days + 1
print(f"Earliest date: {earliest_date}")
print(f"Total date coverage: {total_days:,} days")
print(f"Historical records (pre-2020): {len(early):,}")

if earliest_date <= date(2013, 4, 28):
    print("\n[OK] Historical backfill successful - data extends to 2013")
else:
    print(f"\n[WARNING] Earliest date is {earliest_date}, expected 2013-04-28 or earlier")

if len(early) > 7000:
    print(f"[OK] Found {len(early):,} historical records (pre-2020)")
else:
    print(f"[WARNING] Only found {len(early):,} historical records (expected > 7000)")

print()
