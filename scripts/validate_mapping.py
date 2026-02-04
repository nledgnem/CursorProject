#!/usr/bin/env python3
"""Validate data lake mappings: coverage, uniqueness, join sanity, conflicts."""

import sys
import argparse
from pathlib import Path
from datetime import date
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_lake.mapping_validation import (
    run_full_mapping_validation,
    generate_sql_queries_for_duckdb,
)


def main():
    parser = argparse.ArgumentParser(
        description="Validate data lake mappings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all dates
  python scripts/validate_mapping.py --data-lake-dir data/curated/data_lake

  # Validate specific snapshot date
  python scripts/validate_mapping.py --data-lake-dir data/curated/data_lake --snapshot-date 2024-01-01

  # Stricter coverage threshold
  python scripts/validate_mapping.py --data-lake-dir data/curated/data_lake --min-coverage 90.0
        """
    )
    
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        required=True,
        help="Directory with data lake parquet files",
    )
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Specific date to validate (YYYY-MM-DD), or all dates if not provided",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=85.0,
        help="Minimum coverage percentage required (default: 85.0)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON file path (default: data_lake_dir/mapping_validation.json)",
    )
    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit with non-zero code if validation fails",
    )
    parser.add_argument(
        "--print-sql",
        action="store_true",
        help="Print SQL queries for manual DuckDB validation",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = (repo_root / args.data_lake_dir).resolve()
    
    if not data_lake_dir.exists():
        print(f"[ERROR] Data lake directory not found: {data_lake_dir}")
        sys.exit(1)
    
    # Parse snapshot date
    snapshot_date = None
    if args.snapshot_date:
        snapshot_date = date.fromisoformat(args.snapshot_date)
    
    # Determine output path
    if args.output:
        output_path = (repo_root / args.output).resolve()
    else:
        output_path = data_lake_dir / "mapping_validation.json"
    
    print("=" * 70)
    print("MAPPING VALIDATION")
    print("=" * 70)
    print(f"\nData lake directory: {data_lake_dir}")
    if snapshot_date:
        print(f"Snapshot date: {snapshot_date}")
    else:
        print("Snapshot date: All dates")
    print(f"Minimum coverage: {args.min_coverage}%")
    print()
    
    # Run validation
    report = run_full_mapping_validation(
        data_lake_dir=data_lake_dir,
        snapshot_date=snapshot_date,
        min_coverage_pct=args.min_coverage,
        output_path=output_path,
    )
    
    # Print summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    
    coverage = report["coverage"]
    print(f"\nCoverage:")
    print(f"  Total marketcap assets: {coverage['total_mcap_assets']}")
    print(f"  Marketcap with price: {coverage['mcap_with_price']} ({coverage['pct_mcap_with_price']:.1f}%)")
    print(f"  Marketcap with volume: {coverage['mcap_with_volume']} ({coverage['pct_mcap_with_volume']:.1f}%)")
    print(f"  Marketcap with both: {coverage['mcap_with_both']} ({coverage['pct_mcap_with_both']:.1f}%)")
    
    uniqueness = report["uniqueness"]
    print(f"\nUniqueness:")
    print(f"  Provider asset duplicates: {len(uniqueness['provider_asset_duplicates'])}")
    print(f"  Provider instrument duplicates: {len(uniqueness['provider_instrument_duplicates'])}")
    
    join_sanity = report["join_sanity"]
    print(f"\nJoin Sanity (sample of {join_sanity['sample_size']}):")
    print(f"  All joins successful: {join_sanity['all_join_count']} ({join_sanity['all_join_pct']:.1f}%)")
    
    conflict = report["conflict_report"]
    print(f"\nConflict Report:")
    print(f"  Missing price count: {conflict['missing_price_count']}")
    print(f"  Suspected duplicates: {conflict['duplicate_count']}")
    
    # Guardrail errors
    if report["guardrail_errors"]:
        print(f"\n[FAIL] Guardrail violations:")
        for error in report["guardrail_errors"]:
            print(f"  - {error}")
    else:
        print(f"\n[PASS] All guardrails passed")
    
    # Print SQL queries if requested
    if args.print_sql:
        print("\n" + "=" * 70)
        print("SQL QUERIES FOR DUCKDB")
        print("=" * 70)
        queries = generate_sql_queries_for_duckdb()
        for name, sql in queries.items():
            print(f"\n-- {name}")
            print(sql)
    
    # Exit with error code if validation failed
    if not report["is_valid"] and args.fail_on_errors:
        print("\n[ERROR] Mapping validation failed. Exiting with error code.")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
