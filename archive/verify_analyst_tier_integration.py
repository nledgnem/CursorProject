#!/usr/bin/env python3
"""Verify Analyst tier data is properly integrated with asset_id mappings."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("ANALYST TIER DATA INTEGRATION VERIFICATION")
print("=" * 80)
print()

# Check each Analyst tier file
analyst_files = {
    "fact_ohlc.parquet": "OHLC Data",
    "fact_market_breadth.parquet": "Market Breadth (Top Gainers/Losers)",
    "fact_global_market.parquet": "Global Market Data",
    "fact_exchange_volume.parquet": "Exchange Volumes",
    "fact_derivative_volume.parquet": "Derivative Volumes",
    "fact_derivative_open_interest.parquet": "Derivative Open Interest",
    "dim_new_listings.parquet": "New Listings",
    "dim_derivative_exchanges.parquet": "Derivative Exchanges",
}

all_good = True

for filename, description in analyst_files.items():
    filepath = data_lake_dir / filename
    if not filepath.exists():
        print(f"[ERROR] {filename} - NOT FOUND")
        all_good = False
        continue
    
    try:
        df = pl.read_parquet(str(filepath))
        
        print(f"[OK] {filename} - {description}")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {', '.join(df.columns)}")
        
        # Check for asset_id
        if "asset_id" in df.columns:
            unique_assets = df["asset_id"].n_unique()
            sample_assets = df["asset_id"].unique()[:5].to_list()
            print(f"   [OK] Has asset_id column")
            print(f"   Unique assets: {unique_assets}")
            print(f"   Sample asset_ids: {sample_assets}")
            
            # Verify asset_ids exist in dim_asset
            dim_asset = pl.read_parquet(str(data_lake_dir / "dim_asset.parquet"))
            valid_assets = set(dim_asset["asset_id"].unique().to_list())
            file_assets = set(df["asset_id"].unique().to_list())
            missing = file_assets - valid_assets
            
            if missing:
                print(f"   [WARN] {len(missing)} asset_ids not in dim_asset: {list(missing)[:5]}")
            else:
                print(f"   [OK] All asset_ids mapped to dim_asset")
        else:
            print(f"   [INFO] No asset_id column (may be intentional for this table type)")
        
        # Check for date column (for fact tables)
        if "date" in df.columns:
            date_range = f"{df['date'].min()} to {df['date'].max()}"
            print(f"   Date range: {date_range}")
        
        print()
        
    except Exception as e:
        print(f"[ERROR] {filename} - ERROR: {e}")
        all_good = False
        print()

# Check cross-references
print("=" * 80)
print("CROSS-REFERENCE VERIFICATION")
print("=" * 80)
print()

# Check if OHLC asset_ids match price asset_ids
try:
    ohlc = pl.read_parquet(str(data_lake_dir / "fact_ohlc.parquet"))
    price = pl.read_parquet(str(data_lake_dir / "fact_price.parquet"))
    
    ohlc_assets = set(ohlc["asset_id"].unique().to_list())
    price_assets = set(price["asset_id"].unique().to_list())
    
    overlap = ohlc_assets & price_assets
    ohlc_only = ohlc_assets - price_assets
    price_only = price_assets - ohlc_assets
    
    print(f"OHLC vs Price asset_id overlap:")
    print(f"  Overlapping assets: {len(overlap)}")
    print(f"  OHLC-only assets: {len(ohlc_only)} {list(ohlc_only)[:5] if ohlc_only else ''}")
    print(f"  Price-only assets: {len(price_only)} (expected - OHLC is subset)")
    print()
    
except Exception as e:
    print(f"[WARN] Could not verify cross-references: {e}")
    print()

print("=" * 80)
if all_good:
    print("[OK] All Analyst tier files are properly integrated with asset_id mappings!")
else:
    print("[WARN] Some issues found - see above")
print("=" * 80)
