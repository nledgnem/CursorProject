#!/usr/bin/env python3
"""Check progress of category mapping fetch."""

import polars as pl
from pathlib import Path
from datetime import datetime

data_lake_dir = Path("data/curated/data_lake")
mapping_file = data_lake_dir / "map_category_asset.parquet"

print("=" * 80)
print("CATEGORY MAPPING FETCH PROGRESS")
print("=" * 80)
print()

if mapping_file.exists():
    df = pl.read_parquet(str(mapping_file))
    
    # Get file modification time
    mod_time = datetime.fromtimestamp(mapping_file.stat().st_mtime)
    
    print(f"File last updated: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time since update: {datetime.now() - mod_time}")
    print()
    print(f"Current status:")
    print(f"  Total mappings: {len(df):,}")
    print(f"  Unique assets with categories: {df['asset_id'].n_unique():,}")
    print(f"  Unique categories: {df['category_id'].n_unique():,}")
    print()
    
    # Show sample
    print("Sample mappings:")
    for row in df.head(10).to_dicts():
        print(f"  {row['asset_id']} -> {row['category_id']}")
    print()
    
    # Check if BTC has categories
    btc_mappings = df.filter(pl.col('asset_id') == 'BTC')
    if len(btc_mappings) > 0:
        print("BTC categories found:")
        for row in btc_mappings.to_dicts():
            print(f"  - {row['category_id']} ({row['category_name']})")
    else:
        print("BTC categories: Not yet fetched")
    print()
    
    # Estimate progress (assuming ~2,717 total assets)
    total_assets = 2717
    assets_with_cats = df['asset_id'].n_unique()
    progress_pct = (assets_with_cats / total_assets) * 100
    
    print(f"Estimated progress: {assets_with_cats:,} / {total_assets:,} assets ({progress_pct:.1f}%)")
    
    if progress_pct < 100:
        print()
        print("Fetch is still in progress...")
        print("The script is making ~2,700 API calls (one per asset)")
        print("Estimated time remaining: Check again in a few minutes")
else:
    print("Mapping file not found yet.")
    print("The fetch script is still running...")

print()
print("=" * 80)
