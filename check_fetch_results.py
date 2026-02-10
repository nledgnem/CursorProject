#!/usr/bin/env python3
"""Check fetch results and understand failures."""

import pandas as pd
from pathlib import Path
from datetime import date, timedelta

print("=" * 70)
print("FETCH RESULTS ANALYSIS")
print("=" * 70)

# Check funding data
funding_path = Path("data/curated/data_lake/fact_funding.parquet")
if funding_path.exists():
    df = pd.read_parquet(funding_path)
    print(f"\nFunding Data:")
    print(f"  Total records: {len(df):,}")
    print(f"  Assets: {df['asset_id'].nunique()}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Check latest dates
    latest_date = df['date'].max()
    print(f"\n  Latest date: {latest_date}")
    print(f"  Records on latest date: {len(df[df['date'] == latest_date])}")
    
    # Check how many symbols have data up to today
    today = date.today()
    df['date_dt'] = pd.to_datetime(df['date']).dt.date
    symbols_with_latest = df[df['date_dt'] >= (today - timedelta(days=1))]['asset_id'].nunique()
    print(f"  Symbols with data in last 1 day: {symbols_with_latest}")
    
    # Sample of latest data
    print(f"\n  Sample of latest records:")
    latest = df[df['date'] >= df['date'].max() - pd.Timedelta(days=2)].sort_values(['date', 'asset_id'], ascending=[False, True])
    print(latest[['asset_id', 'date', 'funding_rate']].head(10).to_string())
    
    # Check for gaps
    print(f"\n  Checking for date gaps...")
    for asset in df['asset_id'].unique()[:5]:  # Check first 5 symbols
        asset_data = df[df['asset_id'] == asset].sort_values('date')
        if len(asset_data) > 0:
            last_date = asset_data['date'].max().date()
            print(f"    {asset}: last date = {last_date}, records = {len(asset_data)}")

# Check OI data
oi_path = Path("data/curated/data_lake/fact_open_interest.parquet")
if oi_path.exists():
    df2 = pd.read_parquet(oi_path)
    print(f"\n\nOI Data:")
    print(f"  Total records: {len(df2):,}")
    print(f"  Assets: {df2['asset_id'].nunique()}")
    print(f"  Date range: {df2['date'].min()} to {df2['date'].max()}")
    
    latest_date = df2['date'].max()
    print(f"  Latest date: {latest_date}")
    print(f"  Records on latest date: {len(df2[df2['date'] == latest_date])}")

print("\n" + "=" * 70)
print("\nInterpretation:")
print("  - '0 successful' means no new records were fetched")
print("  - '507 skipped' means 507 symbols were already up to date (good!)")
print("  - '2210 failed' means those symbols failed to fetch (could be:")
print("    * Symbols don't exist on Binance")
print("    * API errors after 3 retries")
print("    * Date range issues")
print("    * Invalid symbol names")
