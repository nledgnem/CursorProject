"""
Add asset_id to output files that are missing it.

This script:
1. Loads universe_eligibility.parquet and universe_snapshots.parquet
2. Joins with dim_asset to add asset_id
3. Saves aligned versions
"""

import pandas as pd
from pathlib import Path

def align_output_file(
    output_file_path: Path,
    dim_asset_path: Path,
    output_aligned_path: Path,
    join_column: str = 'symbol'
):
    """Align an output file by adding asset_id."""
    
    print(f"\nProcessing: {output_file_path.name}")
    print("-" * 80)
    
    # Load files
    print(f"Loading {output_file_path}...")
    df = pd.read_parquet(output_file_path)
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Has asset_id: {'asset_id' in df.columns}")
    
    if 'asset_id' in df.columns:
        print(f"  [SKIP] File already has asset_id column")
        return df
    
    print(f"\nLoading {dim_asset_path.name}...")
    dim_asset = pd.read_parquet(dim_asset_path)
    print(f"  Rows: {len(dim_asset):,}")
    print(f"  Columns: {dim_asset.columns.tolist()}")
    
    # Check join column exists
    if join_column not in df.columns:
        print(f"  [ERROR] Join column '{join_column}' not found in output file")
        print(f"  Available columns: {df.columns.tolist()}")
        return None
    
    # Check for unique mapping
    symbol_counts = dim_asset[join_column].value_counts()
    duplicates = symbol_counts[symbol_counts > 1]
    if len(duplicates) > 0:
        print(f"  [WARN] {len(duplicates)} symbols have multiple asset_ids in dim_asset:")
        print(f"    Examples: {duplicates.head(5).to_dict()}")
        print(f"    Using first occurrence for each symbol")
        dim_asset = dim_asset.drop_duplicates(subset=[join_column], keep='first')
    
    # Perform join
    print(f"\nJoining on '{join_column}' to add asset_id...")
    df_before = len(df)
    df_aligned = df.merge(
        dim_asset[[join_column, 'asset_id']],
        on=join_column,
        how='left'
    )
    df_after = len(df_aligned)
    
    if df_before != df_after:
        print(f"  [WARN] Row count changed: {df_before} -> {df_after}")
    
    # Check mapping coverage
    unmapped = df_aligned['asset_id'].isna().sum()
    mapped = df_aligned['asset_id'].notna().sum()
    coverage_pct = (mapped / len(df_aligned) * 100) if len(df_aligned) > 0 else 0
    
    print(f"  Mapped rows: {mapped:,} ({coverage_pct:.1f}%)")
    print(f"  Unmapped rows: {unmapped:,} ({100 - coverage_pct:.1f}%)")
    
    if unmapped > 0:
        unmapped_symbols = df_aligned[df_aligned['asset_id'].isna()][join_column].unique()
        print(f"  Unmapped symbols: {sorted(unmapped_symbols)[:10]}")
        if len(unmapped_symbols) > 10:
            print(f"    ... and {len(unmapped_symbols) - 10} more")
    else:
        print(f"  [OK] All rows successfully mapped")
    
    # Reorder columns to put asset_id near the beginning
    cols = df_aligned.columns.tolist()
    if 'asset_id' in cols:
        # Move asset_id to be right after symbol or at the beginning
        cols.remove('asset_id')
        if join_column in cols:
            idx = cols.index(join_column)
            cols.insert(idx + 1, 'asset_id')
        else:
            cols.insert(0, 'asset_id')
        df_aligned = df_aligned[cols]
    
    # Save aligned version
    print(f"\nSaving aligned version to {output_aligned_path.name}...")
    output_aligned_path.parent.mkdir(parents=True, exist_ok=True)
    df_aligned.to_parquet(output_aligned_path, index=False)
    print(f"  [OK] Saved {len(df_aligned):,} rows")
    print(f"  Columns: {len(df_aligned.columns)}")
    print(f"  New columns: {list(set(df_aligned.columns) - set(df.columns))}")
    
    return df_aligned


def verify_alignment(file_path: Path):
    """Verify that a file has asset_id and show sample data."""
    df = pd.read_parquet(file_path)
    
    has_asset_id = 'asset_id' in df.columns
    if has_asset_id:
        non_null_count = df['asset_id'].notna().sum()
        total_count = len(df)
        print(f"\nVerification: {file_path.name}")
        print(f"  Has asset_id: YES")
        print(f"  Rows with asset_id: {non_null_count:,} / {total_count:,} ({non_null_count/total_count*100:.1f}%)")
        
        # Show sample
        print(f"\n  Sample rows:")
        sample_cols = ['asset_id', 'symbol'] if 'symbol' in df.columns else ['asset_id']
        if 'rebalance_date' in df.columns:
            sample_cols.insert(0, 'rebalance_date')
        sample_cols = [c for c in sample_cols if c in df.columns]
        print(df[sample_cols].head(10).to_string(index=False))
    else:
        print(f"\nVerification: {file_path.name}")
        print(f"  Has asset_id: NO")
    
    return has_asset_id


def main():
    """Main entry point."""
    repo_root = Path(__file__).parent
    
    print("=" * 80)
    print("Aligning Output Files to Data Lake Format")
    print("=" * 80)
    
    # File paths
    dim_asset_path = repo_root / "data" / "curated" / "data_lake" / "dim_asset.parquet"
    
    files_to_align = [
        (
            repo_root / "data" / "curated" / "universe_eligibility.parquet",
            repo_root / "data" / "curated" / "universe_eligibility_aligned.parquet"
        ),
        (
            repo_root / "data" / "curated" / "universe_snapshots.parquet",
            repo_root / "data" / "curated" / "universe_snapshots_aligned.parquet"
        ),
    ]
    
    # Check inputs exist
    if not dim_asset_path.exists():
        print(f"[ERROR] {dim_asset_path} not found")
        return 1
    
    # Align each file
    aligned_files = []
    for input_path, output_path in files_to_align:
        if not input_path.exists():
            print(f"[SKIP] {input_path} not found")
            continue
        
        try:
            df_aligned = align_output_file(
                input_path,
                dim_asset_path,
                output_path,
                join_column='symbol'
            )
            
            if df_aligned is not None:
                aligned_files.append((input_path, output_path))
        except Exception as e:
            print(f"\n[ERROR] Failed to align {input_path.name}: {e}")
            import traceback
            traceback.print_exc()
            return 1
    
    # Verify aligned files
    print("\n" + "=" * 80)
    print("Verification")
    print("=" * 80)
    
    all_verified = True
    for input_path, output_path in aligned_files:
        if verify_alignment(output_path):
            all_verified = all_verified and True
        else:
            all_verified = False
    
    # Summary
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    
    print(f"\nFiles processed: {len(aligned_files)}")
    for input_path, output_path in aligned_files:
        print(f"  {input_path.name}")
        print(f"    -> {output_path.name}")
    
    if all_verified:
        print("\n[OK] All files successfully aligned!")
        print("\nNext steps:")
        print("  1. Review the aligned files")
        print("  2. Compare with originals to verify correctness")
        print("  3. Replace original files if everything looks good:")
        for input_path, output_path in aligned_files:
            print(f"     - Replace {input_path.name} with {output_path.name}")
        print("\nOr use the aligned versions directly in your code.")
    else:
        print("\n[WARN] Some files may not be fully aligned. Please review.")
    
    return 0 if all_verified else 1


if __name__ == "__main__":
    exit(main())

