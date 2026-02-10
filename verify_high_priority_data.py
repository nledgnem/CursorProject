#!/usr/bin/env python3
"""Verify high-priority data fetch results."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("HIGH-PRIORITY DATA VERIFICATION")
print("=" * 80)
print()

files = {
    "fact_trending_searches.parquet": "Trending Searches",
    "fact_category_market.parquet": "Coin Categories",
    "fact_markets_snapshot.parquet": "All Markets Snapshot",
    "fact_exchange_volume_history.parquet": "Exchange Volume History",
}

for filename, description in files.items():
    filepath = data_lake_dir / filename
    if not filepath.exists():
        print(f"[ERROR] {filename} - NOT FOUND")
        continue
    
    try:
        df = pl.read_parquet(str(filepath))
        print(f"[OK] {filename} - {description}")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {', '.join(df.columns)}")
        
        if "date" in df.columns:
            print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
        
        if "asset_id" in df.columns:
            print(f"   Unique assets: {df['asset_id'].n_unique()}")
            print(f"   Sample asset_ids: {df['asset_id'].unique()[:5].to_list()}")
        
        if "exchange_id" in df.columns:
            print(f"   Unique exchanges: {df['exchange_id'].n_unique()}")
            print(f"   Exchanges: {df['exchange_id'].unique().to_list()}")
        
        if "category_id" in df.columns:
            print(f"   Unique categories: {df['category_id'].n_unique()}")
        
        print()

    except Exception as e:
        print(f"[ERROR] {filename} - ERROR: {e}")
        print()

print("=" * 80)
