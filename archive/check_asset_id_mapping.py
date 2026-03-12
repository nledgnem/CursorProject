#!/usr/bin/env python3
"""Check if funding data asset IDs are correctly mapped to universal IDs."""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("ASSET ID MAPPING VERIFICATION")
print("=" * 70)

data_lake = Path("data/curated/data_lake")

# Load funding data
funding_path = data_lake / "fact_funding.parquet"
if funding_path.exists():
    funding_df = pd.read_parquet(funding_path)
    funding_asset_ids = set(funding_df["asset_id"].unique())
    print(f"\nFunding Data:")
    print(f"  Total records: {len(funding_df):,}")
    print(f"  Unique asset_ids: {len(funding_asset_ids)}")
    print(f"  Sample asset_ids: {sorted(list(funding_asset_ids))[:20]}")
else:
    print("\n[ERROR] fact_funding.parquet not found")
    funding_asset_ids = set()

# Load dim_asset (canonical asset IDs)
dim_asset_path = data_lake / "dim_asset.parquet"
if dim_asset_path.exists():
    dim_asset = pd.read_parquet(dim_asset_path)
    canonical_asset_ids = set(dim_asset["asset_id"].unique())
    print(f"\nDimension Asset Table (dim_asset.parquet):")
    print(f"  Total canonical asset_ids: {len(canonical_asset_ids)}")
    print(f"  Sample canonical asset_ids: {sorted(list(canonical_asset_ids))[:20]}")
    print(f"\n  Columns: {list(dim_asset.columns)}")
    if len(dim_asset) > 0:
        print(f"\n  Sample rows:")
        print(dim_asset.head(10).to_string())
else:
    print("\n[WARNING] dim_asset.parquet not found")
    canonical_asset_ids = set()

# Load mapping table
map_path = data_lake / "map_provider_asset.parquet"
if map_path.exists():
    map_df = pd.read_parquet(map_path)
    print(f"\nProvider Asset Mapping (map_provider_asset.parquet):")
    print(f"  Total mappings: {len(map_df)}")
    print(f"  Columns: {list(map_df.columns)}")
    if len(map_df) > 0:
        print(f"\n  Sample mappings:")
        print(map_df.head(20).to_string())
        
        # Check if funding asset_ids are in the mapping
        if "asset_id" in map_df.columns:
            mapped_asset_ids = set(map_df["asset_id"].unique())
            print(f"\n  Mapped asset_ids: {len(mapped_asset_ids)}")
else:
    print("\n[WARNING] map_provider_asset.parquet not found")
    map_df = None

# Compare funding asset_ids with canonical IDs
print("\n" + "=" * 70)
print("MAPPING VERIFICATION")
print("=" * 70)

if canonical_asset_ids:
    # Check which funding asset_ids are in canonical list
    matching = funding_asset_ids & canonical_asset_ids
    missing = funding_asset_ids - canonical_asset_ids
    extra = canonical_asset_ids - funding_asset_ids
    
    print(f"\nFunding asset_ids vs Canonical asset_ids:")
    print(f"  Matching (in both): {len(matching)} ({len(matching)/len(funding_asset_ids)*100:.1f}%)")
    print(f"  Missing from canonical: {len(missing)}")
    if missing:
        print(f"    Sample missing: {sorted(list(missing))[:20]}")
    print(f"  Extra in canonical (not in funding): {len(extra)}")
    
    if len(missing) > 0:
        print(f"\n  [WARNING] {len(missing)} funding asset_ids are NOT in dim_asset!")
        print(f"  These may need to be added to dim_asset or mapped correctly.")
    else:
        print(f"\n  [OK] All funding asset_ids are in dim_asset!")

# Check mapping table if available
if map_df is not None and "asset_id" in map_df.columns:
    mapped_asset_ids = set(map_df["asset_id"].unique())
    funding_mapped = funding_asset_ids & mapped_asset_ids
    funding_unmapped = funding_asset_ids - mapped_asset_ids
    
    print(f"\nFunding asset_ids vs Mapping table:")
    print(f"  Mapped: {len(funding_mapped)} ({len(funding_mapped)/len(funding_asset_ids)*100:.1f}%)")
    print(f"  Unmapped: {len(funding_unmapped)}")
    if funding_unmapped:
        print(f"    Sample unmapped: {sorted(list(funding_unmapped))[:20]}")

print("\n" + "=" * 70)
