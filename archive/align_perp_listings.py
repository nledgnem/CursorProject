"""
Align perp_listings_binance.parquet to standardized data lake format.

This script:
1. Reads the raw perp_listings_binance.parquet
2. Joins with map_provider_instrument to get canonical instrument_id
3. Creates an aligned version using standardized IDs
"""

import pandas as pd
from pathlib import Path

def align_perp_listings(
    perp_listings_path: Path,
    map_provider_instrument_path: Path,
    dim_instrument_path: Path,
    output_path: Path
):
    """Align perp_listings to data lake format."""
    
    print("=" * 80)
    print("Aligning perp_listings_binance to Data Lake Format")
    print("=" * 80)
    print()
    
    # Load files
    print(f"Loading {perp_listings_path}...")
    perp_df = pd.read_parquet(perp_listings_path)
    print(f"  Loaded {len(perp_df)} rows")
    print(f"  Columns: {perp_df.columns.tolist()}")
    print()
    
    print(f"Loading {map_provider_instrument_path}...")
    map_df = pd.read_parquet(map_provider_instrument_path)
    print(f"  Loaded {len(map_df)} mappings")
    print(f"  Provider: {map_df['provider'].unique()}")
    print()
    
    print(f"Loading {dim_instrument_path}...")
    dim_instrument = pd.read_parquet(dim_instrument_path)
    print(f"  Loaded {len(dim_instrument)} instruments")
    print()
    
    # Check mapping coverage
    perp_symbols = set(perp_df['symbol'])
    mapped_symbols = set(map_df['provider_instrument_id'])
    missing = perp_symbols - mapped_symbols
    extra = mapped_symbols - perp_symbols
    
    print("Mapping Coverage:")
    print(f"  Perp listings symbols: {len(perp_symbols)}")
    print(f"  Mapped symbols: {len(mapped_symbols)}")
    print(f"  Missing mappings: {len(missing)}")
    print(f"  Extra mappings (not in perp_listings): {len(extra)}")
    if missing:
        print(f"  [WARN] Missing symbols: {sorted(list(missing))[:10]}")
        if len(missing) > 10:
            print(f"     ... and {len(missing) - 10} more")
    print()
    
    # Join to get instrument_id
    print("Joining perp_listings with mapping table...")
    aligned_df = perp_df.merge(
        map_df[['provider_instrument_id', 'instrument_id', 'valid_from', 'valid_to']],
        left_on='symbol',
        right_on='provider_instrument_id',
        how='left'
    )
    
    unmapped_count = aligned_df['instrument_id'].isna().sum()
    if unmapped_count > 0:
        print(f"  [WARN] Warning: {unmapped_count} rows could not be mapped to instrument_id")
        unmapped_symbols = aligned_df[aligned_df['instrument_id'].isna()]['symbol'].unique()
        print(f"       Unmapped symbols: {sorted(unmapped_symbols)[:10]}")
        if len(unmapped_symbols) > 10:
            print(f"     ... and {len(unmapped_symbols) - 10} more")
    else:
        print(f"  [OK] All {len(aligned_df)} rows successfully mapped")
    print()
    
    # Join with dim_instrument to get additional standardized fields
    print("Enriching with dim_instrument data...")
    aligned_df = aligned_df.merge(
        dim_instrument[['instrument_id', 'instrument_symbol', 'base_asset_symbol', 
                       'instrument_type', 'venue', 'quote']],
        on='instrument_id',
        how='left'
    )
    print(f"  [OK] Joined with dim_instrument")
    print()
    
    # Reorder and select columns for aligned version
    # Keep original data, add standardized IDs
    output_columns = [
        'instrument_id',           # Standardized canonical ID (NEW)
        'instrument_symbol',       # From dim_instrument (matches original 'symbol')
        'base_asset_symbol',       # From dim_instrument (NEW)
        'symbol',                  # Keep original for backward compatibility
        'onboard_date',
        'source',
        'proxy_version',
        'instrument_type',         # From dim_instrument (NEW)
        'venue',                   # From dim_instrument (NEW)
        'quote',                   # From dim_instrument (NEW)
        'valid_from',              # From mapping (NEW)
        'valid_to'                 # From mapping (NEW)
    ]
    
    # Only include columns that exist
    available_columns = [col for col in output_columns if col in aligned_df.columns]
    aligned_df = aligned_df[available_columns]
    
    # Rename 'symbol' to 'provider_instrument_id' for clarity (or keep both?)
    # Actually, let's keep both: symbol (original) and instrument_symbol (standardized)
    
    print("Final aligned schema:")
    print(f"  Columns: {aligned_df.columns.tolist()}")
    print(f"  Rows: {len(aligned_df)}")
    print(f"  Rows with instrument_id: {aligned_df['instrument_id'].notna().sum()}")
    print()
    
    # Save aligned version
    print(f"Saving aligned version to {output_path}...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    aligned_df.to_parquet(output_path, index=False)
    print(f"  [OK] Saved {len(aligned_df)} rows")
    print()
    
    # Create summary
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Input file: {perp_listings_path}")
    print(f"Output file: {output_path}")
    print(f"Rows processed: {len(perp_df)}")
    print(f"Rows with instrument_id: {aligned_df['instrument_id'].notna().sum()}")
    print(f"Mapping coverage: {(aligned_df['instrument_id'].notna().sum() / len(aligned_df) * 100):.1f}%")
    print()
    
    print("Key Changes:")
    print("  [OK] Added 'instrument_id' (canonical ID)")
    print("  [OK] Added 'instrument_symbol' (standardized from dim_instrument)")
    print("  [OK] Added 'base_asset_symbol' (extracted base asset)")
    print("  [OK] Added 'instrument_type', 'venue', 'quote' (from dim_instrument)")
    print("  [OK] Added 'valid_from', 'valid_to' (temporal validity)")
    print("  [NOTE] Kept original 'symbol' for backward compatibility")
    print()
    
    if unmapped_count > 0:
        print("[WARN] WARNING: Some rows could not be mapped.")
        print("       These may be new listings not yet in the data lake.")
        print("       Review the unmapped symbols above.")
    else:
        print("[OK] All rows successfully aligned to data lake format!")
    
    return aligned_df


def main():
    """Main entry point."""
    repo_root = Path(__file__).parent
    
    # File paths
    perp_listings_path = repo_root / "data" / "raw" / "perp_listings_binance.parquet"
    map_provider_instrument_path = repo_root / "data" / "curated" / "data_lake" / "map_provider_instrument.parquet"
    dim_instrument_path = repo_root / "data" / "curated" / "data_lake" / "dim_instrument.parquet"
    output_path = repo_root / "data" / "curated" / "perp_listings_binance_aligned.parquet"
    
    # Check inputs exist
    if not perp_listings_path.exists():
        print(f"[ERROR] Error: {perp_listings_path} not found")
        return 1
    
    if not map_provider_instrument_path.exists():
        print(f"❌ Error: {map_provider_instrument_path} not found")
        return 1
    
    if not dim_instrument_path.exists():
        print(f"❌ Error: {dim_instrument_path} not found")
        return 1
    
    # Align
    try:
        aligned_df = align_perp_listings(
            perp_listings_path,
            map_provider_instrument_path,
            dim_instrument_path,
            output_path
        )
        
        print(f"\n[OK] Success! Aligned file saved to: {output_path}")
        print("\nNext steps:")
        print("  1. Review the aligned file")
        print("  2. Update scripts to use 'instrument_id' instead of 'symbol'")
        print("  3. Consider replacing the original file if everything looks good")
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

