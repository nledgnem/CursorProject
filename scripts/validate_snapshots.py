#!/usr/bin/env python3
"""Validate universe and basket snapshots for sanity checks."""

import sys
import argparse
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def validate_snapshots(snapshots_path: Path, universe_eligibility_path: Path, 
                       top_n: int, expected_rebalance_dates: int = None) -> bool:
    """
    Validate snapshot files for sanity checks.
    
    Returns True if all checks pass, False otherwise.
    """
    if not snapshots_path.exists():
        print(f"[ERROR] Basket snapshots file not found: {snapshots_path}")
        return False
    
    if not universe_eligibility_path.exists():
        print(f"[ERROR] Universe eligibility file not found: {universe_eligibility_path}")
        return False
    
    print(f"\nValidating snapshots...")
    print(f"  Basket snapshots: {snapshots_path}")
    print(f"  Universe eligibility: {universe_eligibility_path}")
    
    # Load data
    try:
        basket_df = pd.read_parquet(snapshots_path)
        universe_df = pd.read_parquet(universe_eligibility_path)
    except Exception as e:
        print(f"[ERROR] Failed to load parquet files: {e}")
        return False
    
    all_passed = True
    
    # Check 1: Weights are non-negative and <= 1.0, and sum to 1.0 per rebalance_date
    print("\n[Check 1] Weight constraints (non-negative, <= 1.0, sum to 1.0)...")
    if len(basket_df) == 0:
        print("  [SKIP] No basket snapshots to validate")
    else:
        # Check individual weights are valid
        invalid_weights = basket_df[(basket_df["weight"] < 0) | (basket_df["weight"] > 1.0)]
        if len(invalid_weights) > 0:
            print(f"  [FAIL] Found {len(invalid_weights)} rows with invalid weights (< 0 or > 1.0):")
            for _, row in invalid_weights.head(10).iterrows():
                print(f"    {row['rebalance_date']} {row['symbol']}: weight = {row['weight']:.10f}")
            all_passed = False
        else:
            print(f"  [PASS] All {len(basket_df)} weights are in valid range [0, 1.0]")
        
        # Check weights sum to 1.0 per rebalance_date
        weight_sums = basket_df.groupby("rebalance_date")["weight"].sum()
        tolerance = 1e-6  # Floating point tolerance
        invalid_sums = weight_sums[(weight_sums < 1.0 - tolerance) | (weight_sums > 1.0 + tolerance)]
        
        if len(invalid_sums) > 0:
            print(f"  [FAIL] Found {len(invalid_sums)} rebalance dates with weights != 1.0:")
            for date, weight_sum in invalid_sums.items():
                print(f"    {date}: sum = {weight_sum:.10f}")
            all_passed = False
        else:
            print(f"  [PASS] All {len(weight_sums)} rebalance dates have weights summing to 1.0")
    
    # Check 1b: Rank uniqueness per rebalance_date
    print("\n[Check 1b] Rank uniqueness per rebalance_date...")
    if len(basket_df) == 0:
        print("  [SKIP] No basket snapshots to validate")
    else:
        duplicate_ranks = basket_df.groupby(["rebalance_date", "rank"]).size()
        duplicate_ranks = duplicate_ranks[duplicate_ranks > 1]
        if len(duplicate_ranks) > 0:
            print(f"  [FAIL] Found duplicate ranks:")
            for (rebal_date, rank), count in duplicate_ranks.items():
                print(f"    {rebal_date} rank {rank}: appears {count} times")
            all_passed = False
        else:
            print(f"  [PASS] All ranks are unique per rebalance_date")
    
    # Check 1c: No duplicate (rebalance_date, symbol) rows
    print("\n[Check 1c] No duplicate (rebalance_date, symbol) rows...")
    if len(basket_df) == 0:
        print("  [SKIP] No basket snapshots to validate")
    else:
        duplicates = basket_df.duplicated(subset=["rebalance_date", "symbol"], keep=False)
        if duplicates.any():
            duplicate_rows = basket_df[duplicates]
            print(f"  [FAIL] Found {len(duplicate_rows)} duplicate (rebalance_date, symbol) rows:")
            for _, row in duplicate_rows.head(10).iterrows():
                print(f"    {row['rebalance_date']} {row['symbol']}")
            all_passed = False
        else:
            print(f"  [PASS] No duplicate (rebalance_date, symbol) rows")
    
    # Check 2: Top-N count matches (unless eligible < top_n)
    print("\n[Check 2] Top-N count matches...")
    if len(basket_df) == 0:
        print("  [SKIP] No basket snapshots to validate")
        basket_counts = pd.Series(dtype=int)
    else:
        basket_counts = basket_df.groupby("rebalance_date").size()
    
    # For each rebalance date, check if count matches top_n (unless eligible < top_n)
    for rebal_date in basket_counts.index:
        basket_count = basket_counts[rebal_date]
        # Count eligible coins (exclusion_reason is NULL) for this date
        eligible_count = len(universe_df[
            (universe_df["rebalance_date"] == rebal_date) & 
            (universe_df["exclusion_reason"].isna())
        ])
        
        if eligible_count < top_n:
            # Fewer eligible than top_n, so basket should have eligible_count
            expected_count = eligible_count
            if basket_count != expected_count:
                print(f"  [FAIL] {rebal_date}: basket has {basket_count} coins, but only {eligible_count} eligible (expected {expected_count})")
                all_passed = False
        else:
            # Should have exactly top_n
            if basket_count != top_n:
                print(f"  [FAIL] {rebal_date}: basket has {basket_count} coins, expected {top_n}")
                all_passed = False
    
    if all_passed:
        print(f"  [PASS] All {len(basket_counts)} rebalance dates have correct basket size")
    
    # Check 3: Verify rebalance date alignment
    print("\n[Check 3] Rebalance date alignment...")
    basket_dates = set(basket_df["rebalance_date"].unique())
    universe_dates = set(universe_df["rebalance_date"].unique())
    
    if basket_dates != universe_dates:
        missing_in_basket = universe_dates - basket_dates
        missing_in_universe = basket_dates - universe_dates
        if missing_in_basket:
            print(f"  [WARN] Dates in universe but not in basket: {sorted(missing_in_basket)}")
        if missing_in_universe:
            print(f"  [WARN] Dates in basket but not in universe: {sorted(missing_in_universe)}")
    else:
        print(f"  [PASS] All {len(basket_dates)} rebalance dates align between tables")
    
    # Check 4: Basket symbols are subset of eligible universe
    print("\n[Check 4] Basket symbols are subset of eligible universe...")
    if len(basket_df) == 0:
        print("  [SKIP] No basket snapshots to validate")
    else:
        for rebal_date in basket_dates:
            basket_symbols = set(basket_df[basket_df["rebalance_date"] == rebal_date]["symbol"])
            eligible_symbols = set(universe_df[
                (universe_df["rebalance_date"] == rebal_date) & 
                (universe_df["exclusion_reason"].isna())
            ]["symbol"])
            
            invalid = basket_symbols - eligible_symbols
            if invalid:
                print(f"  [FAIL] {rebal_date}: basket contains ineligible symbols: {invalid}")
                all_passed = False
        
        if all_passed:
            print(f"  [PASS] All basket symbols are eligible")
    
    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Validate universe and basket snapshots",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--snapshots",
        type=Path,
        required=True,
        help="Path to universe_snapshots.parquet (basket snapshots)",
    )
    parser.add_argument(
        "--universe-eligibility",
        type=Path,
        required=True,
        help="Path to universe_eligibility.parquet",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        required=True,
        help="Expected top-N size",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    # Resolve paths
    snapshots_path = args.snapshots if args.snapshots.is_absolute() else repo_root / args.snapshots
    universe_path = args.universe_eligibility if args.universe_eligibility.is_absolute() else repo_root / args.universe_eligibility
    
    success = validate_snapshots(snapshots_path, universe_path, args.top_n)
    
    if not success:
        sys.exit(1)
    else:
        print("\n[SUCCESS] All validation checks passed!")


if __name__ == "__main__":
    main()
