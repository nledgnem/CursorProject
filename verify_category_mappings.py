#!/usr/bin/env python3
"""Verify category mappings."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("CATEGORY MAPPING VERIFICATION")
print("=" * 80)
print()

if (data_lake_dir / "map_category_asset.parquet").exists():
    df = pl.read_parquet(str(data_lake_dir / "map_category_asset.parquet"))
    
    print(f"Total mappings: {len(df):,}")
    print(f"Unique assets: {df['asset_id'].n_unique()}")
    print(f"Unique categories: {df['category_id'].n_unique()}")
    print()
    
    print("Sample mappings:")
    for row in df.head(10).to_dicts():
        print(f"  {row['asset_id']} -> {row['category_id']} ({row['category_name']})")
    print()
    
    # Example: Which categories does 1INCH belong to?
    if '1INCH' in df['asset_id'].to_list():
        print("Example: Which categories does 1INCH belong to?")
        inch_cats = df.filter(pl.col('asset_id') == '1INCH')
        for row in inch_cats.to_dicts():
            print(f"  - {row['category_id']} ({row['category_name']})")
        print()
    
    # Example: Which assets are in a specific category?
    if len(df) > 0:
        sample_category = df['category_id'][0]
        print(f"Example: Which assets are in '{sample_category}' category?")
        cat_assets = df.filter(pl.col('category_id') == sample_category)
        for row in cat_assets.to_dicts():
            print(f"  - {row['asset_id']}")
        print()

print("=" * 80)
print("ANSWER TO YOUR QUESTION")
print("=" * 80)
print()
print("dim_categories.parquet:")
print("  - Contains: Category metadata (ID, name)")
print("  - Does NOT contain: Which assets belong to each category")
print()
print("map_category_asset.parquet:")
print("  - Contains: Asset-to-category mappings")
print("  - Example: BTC -> proof-of-work, BTC -> layer-1, etc.")
print()
print("To find which categories BTC belongs to:")
print("  map_category_asset.filter(asset_id == 'BTC')")
print()
print("To find which assets are in 'proof-of-work' category:")
print("  map_category_asset.filter(category_id == 'proof-of-work')")
