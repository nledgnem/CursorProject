#!/usr/bin/env python3
"""
Add asset_id column to existing dim_instrument.parquet file.

This script updates existing dim_instrument tables to include asset_id
by linking base_asset_symbol to dim_asset.asset_id.
"""

import sys
import argparse
from pathlib import Path
import pandas as pd

# Windows encoding fix
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def add_asset_id_to_dim_instrument(data_lake_dir: Path) -> None:
    """Add asset_id column to dim_instrument by linking with dim_asset."""
    
    dim_instrument_path = data_lake_dir / "dim_instrument.parquet"
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    
    if not dim_instrument_path.exists():
        print(f"ERROR: dim_instrument.parquet not found: {dim_instrument_path}")
        sys.exit(1)
    
    if not dim_asset_path.exists():
        print(f"ERROR: dim_asset.parquet not found: {dim_asset_path}")
        print("Cannot link asset_id without dim_asset table.")
        sys.exit(1)
    
    print("=" * 70)
    print("ADDING asset_id TO dim_instrument")
    print("=" * 70)
    
    # Load tables
    print(f"\nLoading dim_instrument from {dim_instrument_path}...")
    dim_instrument = pd.read_parquet(dim_instrument_path)
    print(f"  Loaded {len(dim_instrument)} instruments")
    
    print(f"\nLoading dim_asset from {dim_asset_path}...")
    dim_asset = pd.read_parquet(dim_asset_path)
    print(f"  Loaded {len(dim_asset)} assets")
    
    # Create lookup: symbol -> asset_id
    # Try matching base_asset_symbol to asset_id or symbol in dim_asset
    symbol_to_asset_id = {}
    if "symbol" in dim_asset.columns and "asset_id" in dim_asset.columns:
        # Match by symbol
        symbol_to_asset_id = dict(zip(dim_asset["symbol"], dim_asset["asset_id"]))
    # Also match asset_id to itself (in case symbol doesn't match but asset_id does)
    if "asset_id" in dim_asset.columns:
        for asset_id in dim_asset["asset_id"]:
            if asset_id not in symbol_to_asset_id:
                symbol_to_asset_id[asset_id] = asset_id
    
    print(f"  Created lookup with {len(symbol_to_asset_id)} symbol->asset_id mappings")
    
    # Add asset_id column
    if "asset_id" in dim_instrument.columns:
        print("\n  [INFO] asset_id column already exists, updating...")
        before_linked = dim_instrument["asset_id"].notna().sum()
    else:
        print("\n  [INFO] Adding new asset_id column...")
        before_linked = 0
    
    # Map base_asset_symbol to asset_id
    dim_instrument["asset_id"] = dim_instrument["base_asset_symbol"].map(symbol_to_asset_id)
    
    after_linked = dim_instrument["asset_id"].notna().sum()
    
    print(f"\n  Linked {after_linked}/{len(dim_instrument)} instruments to asset_id")
    if before_linked < after_linked:
        print(f"  (Updated {after_linked - before_linked} new links)")
    
    # Show unmatched instruments
    unmatched = dim_instrument[dim_instrument["asset_id"].isna()]
    if len(unmatched) > 0:
        print(f"\n  [WARN] {len(unmatched)} instruments could not be linked to asset_id:")
        unmatched_symbols = unmatched["base_asset_symbol"].unique()[:10]
        for symbol in unmatched_symbols:
            print(f"    - {symbol}")
        if len(unmatched_symbols) < len(unmatched):
            print(f"    ... and {len(unmatched) - len(unmatched_symbols)} more")
    
    # Save updated dim_instrument
    print(f"\nSaving updated dim_instrument to {dim_instrument_path}...")
    dim_instrument.to_parquet(dim_instrument_path, index=False)
    print("  ✅ Done!")
    
    # Show sample of linked instruments
    print("\nSample of linked instruments:")
    sample = dim_instrument[dim_instrument["asset_id"].notna()].head(10)
    print(sample[["instrument_id", "base_asset_symbol", "asset_id"]].to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Add asset_id column to dim_instrument by linking with dim_asset"
    )
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        default=Path("data/curated/data_lake"),
        help="Data lake directory (default: data/curated/data_lake)",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = (repo_root / args.data_lake_dir).resolve()
    
    add_asset_id_to_dim_instrument(data_lake_dir)


if __name__ == "__main__":
    main()

