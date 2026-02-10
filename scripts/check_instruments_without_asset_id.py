#!/usr/bin/env python3
"""
Check which instruments do not have an asset_id linked.

Shows instruments that couldn't be matched to dim_asset.
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


def check_unlinked_instruments(data_lake_dir: Path, show_all: bool = False) -> None:
    """Show instruments without asset_id."""
    
    dim_instrument_path = data_lake_dir / "dim_instrument.parquet"
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    
    if not dim_instrument_path.exists():
        print(f"ERROR: dim_instrument.parquet not found: {dim_instrument_path}")
        sys.exit(1)
    
    print("=" * 70)
    print("INSTRUMENTS WITHOUT asset_id")
    print("=" * 70)
    
    # Load dim_instrument
    dim_instrument = pd.read_parquet(dim_instrument_path)
    
    # Check if asset_id column exists
    if "asset_id" not in dim_instrument.columns:
        print("\n❌ ERROR: asset_id column does not exist in dim_instrument!")
        print("Run: python scripts/add_asset_id_to_instruments.py")
        sys.exit(1)
    
    # Find instruments without asset_id
    unlinked = dim_instrument[dim_instrument["asset_id"].isna()].copy()
    
    total = len(dim_instrument)
    linked = dim_instrument["asset_id"].notna().sum()
    unlinked_count = len(unlinked)
    
    print(f"\nStatistics:")
    print(f"  Total instruments: {total}")
    print(f"  Linked to asset_id: {linked} ({100*linked/total:.1f}%)")
    print(f"  Not linked: {unlinked_count} ({100*unlinked_count/total:.1f}%)")
    
    if unlinked_count == 0:
        print("\n✅ All instruments are linked to asset_id!")
        return
    
    print(f"\n{'='*70}")
    print(f"INSTRUMENTS WITHOUT asset_id ({unlinked_count} total)")
    print("=" * 70)
    
    # Show summary by base_asset_symbol
    unlinked_summary = unlinked.groupby("base_asset_symbol").agg({
        "instrument_id": "count",
        "instrument_type": "first",
        "venue": "first",
    }).rename(columns={"instrument_id": "count"}).sort_values("count", ascending=False)
    
    print(f"\nSummary by base_asset_symbol:")
    print(unlinked_summary.to_string())
    
    # Check if these symbols exist in dim_asset
    if dim_asset_path.exists():
        dim_asset = pd.read_parquet(dim_asset_path)
        unlinked_symbols = set(unlinked["base_asset_symbol"].unique())
        asset_symbols = set(dim_asset["symbol"].unique()) if "symbol" in dim_asset.columns else set()
        asset_ids = set(dim_asset["asset_id"].unique()) if "asset_id" in dim_asset.columns else set()
        
        # Check which unlinked symbols exist in dim_asset
        in_symbols = unlinked_symbols.intersection(asset_symbols)
        in_asset_ids = unlinked_symbols.intersection(asset_ids)
        not_in_asset = unlinked_symbols - asset_symbols - asset_ids
        
        print(f"\n{'='*70}")
        print("ANALYSIS")
        print("=" * 70)
        print(f"\nUnlinked symbols: {len(unlinked_symbols)}")
        if in_symbols:
            print(f"  ⚠️  {len(in_symbols)} symbols exist in dim_asset.symbol but didn't match:")
            print(f"     {sorted(list(in_symbols))[:20]}{' ...' if len(in_symbols) > 20 else ''}")
        if in_asset_ids:
            print(f"  ⚠️  {len(in_asset_ids)} symbols exist in dim_asset.asset_id but didn't match:")
            print(f"     {sorted(list(in_asset_ids))[:20]}{' ...' if len(in_asset_ids) > 20 else ''}")
        if not_in_asset:
            print(f"  ✅ {len(not_in_asset)} symbols not in dim_asset (expected):")
            print(f"     {sorted(list(not_in_asset))[:30]}{' ...' if len(not_in_asset) > 30 else ''}")
    
    # Show detailed list if requested
    if show_all:
        print(f"\n{'='*70}")
        print("DETAILED LIST")
        print("=" * 70)
        display_cols = ["instrument_id", "base_asset_symbol", "instrument_type", "venue"]
        print(unlinked[display_cols].to_string(index=False))
    else:
        print(f"\n{'='*70}")
        print("SAMPLE (first 50)")
        print("=" * 70)
        display_cols = ["instrument_id", "base_asset_symbol", "instrument_type", "venue"]
        print(unlinked[display_cols].head(50).to_string(index=False))
        if unlinked_count > 50:
            print(f"\n... and {unlinked_count - 50} more")
            print(f"\nUse --show-all to see complete list")


def main():
    parser = argparse.ArgumentParser(
        description="Check which instruments do not have an asset_id"
    )
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        default=Path("data/curated/data_lake"),
        help="Data lake directory (default: data/curated/data_lake)",
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Show all unlinked instruments (default: show first 50)",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = (repo_root / args.data_lake_dir).resolve()
    
    check_unlinked_instruments(data_lake_dir, show_all=args.show_all)


if __name__ == "__main__":
    main()

