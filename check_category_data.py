#!/usr/bin/env python3
"""Check category data structure and see if we have asset mappings."""

import polars as pl
from pathlib import Path
import json

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("CATEGORY DATA STRUCTURE CHECK")
print("=" * 80)
print()

# Check dim_categories
if (data_lake_dir / "dim_categories.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "dim_categories.parquet"))
    print("dim_categories.parquet:")
    print(f"  Columns: {df.columns}")
    print(f"  Sample rows:")
    for row in df.head(5).to_dicts():
        print(f"    - {row}")
    print()

# Check fact_category_market
if (data_lake_dir / "fact_category_market.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "fact_category_market.parquet"))
    print("fact_category_market.parquet:")
    print(f"  Columns: {df.columns}")
    if "top_3_coins" in df.columns:
        sample = df.filter(pl.col("top_3_coins").is_not_null()).head(1)
        if len(sample) > 0:
            top_coins = sample["top_3_coins"][0]
            print(f"  Sample top_3_coins: {top_coins}")
    print()

# Check if dim_asset has category info
if (data_lake_dir / "dim_asset.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "dim_asset.parquet"))
    print("dim_asset.parquet:")
    print(f"  Columns: {df.columns}")
    if "category" in df.columns or "categories" in df.columns:
        print("  [INFO] dim_asset has category column(s)")
    else:
        print("  [INFO] dim_asset does NOT have category column(s)")
    print()

# Check fact_markets_snapshot (might have category info)
if (data_lake_dir / "fact_markets_snapshot.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "fact_markets_snapshot.parquet"))
    print("fact_markets_snapshot.parquet:")
    print(f"  Columns: {df.columns}")
    if "category" in df.columns or "categories" in df.columns:
        print("  [INFO] fact_markets_snapshot has category column(s)")
        sample = df.filter(pl.col("category").is_not_null() if "category" in df.columns else pl.lit(False)).head(3)
        if len(sample) > 0:
            print(f"  Sample category data:")
            for row in sample.to_dicts():
                print(f"    {row.get('asset_id', 'N/A')}: {row.get('category', 'N/A')}")
    else:
        print("  [INFO] fact_markets_snapshot does NOT have category column(s)")
    print()

print("=" * 80)
print("CONCLUSION")
print("=" * 80)
print()
print("dim_categories only contains category metadata (ID, name).")
print("It does NOT contain which assets belong to each category.")
print()
print("To get asset-to-category mappings, we need to:")
print("  1. Fetch category info from /coins/{id} endpoint for each coin")
print("  2. Or fetch from /coins/markets which may include category info")
print("  3. Create a mapping table: map_category_asset or add to dim_asset")
