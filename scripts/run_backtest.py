#!/usr/bin/env python3
"""Run backtest."""

import sys
import argparse
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.backtest.engine import run_backtest
from src.utils.metadata import create_run_metadata, save_run_metadata

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run backtest")
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to strategy config YAML",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Directory containing price data files (default: data/curated, fallback to data/raw)",
    )
    parser.add_argument(
        "--snapshots-path",
        type=Path,
        default=None,
        help="Path to universe snapshots parquet file (default: data/curated/universe_snapshots.parquet)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write backtest outputs (default: outputs/)",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    config_path = args.config
    
    # Determine data directory
    curated_dir = repo_root / "data" / "curated"
    raw_dir = repo_root / "data" / "raw"
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    
    if args.data_dir:
        data_dir = args.data_dir if args.data_dir.is_absolute() else repo_root / args.data_dir
    else:
        # Default to curated, fallback to raw if curated doesn't exist
        # Try data lake format first, fallback to wide format
        if data_lake_dir.exists() and (data_lake_dir / "fact_price.parquet").exists():
            data_dir = curated_dir  # Use curated_dir as data_dir when using data lake format
        elif (curated_dir / "prices_daily.parquet").exists():
            data_dir = curated_dir
        else:
            data_dir = raw_dir
    
    # Determine prices_path (try data lake format first, fallback to wide format)
    if data_lake_dir.exists() and (data_lake_dir / "fact_price.parquet").exists():
        prices_path = data_lake_dir / "fact_price.parquet"
        print(f"[INFO] Using data lake format from {data_lake_dir}")
    elif (data_dir / "prices_daily.parquet").exists():
        prices_path = data_dir / "prices_daily.parquet"
    else:
        # Final fallback to raw_dir
        prices_path = raw_dir / "prices_daily.parquet"
    
    # Determine snapshots path (with fallback)
    if args.snapshots_path:
        snapshots_path = args.snapshots_path if args.snapshots_path.is_absolute() else repo_root / args.snapshots_path
        if not snapshots_path.exists():
            raise FileNotFoundError(f"Snapshots file not found: {snapshots_path}")
    else:
        # Default to curated, fallback to raw if curated doesn't exist
        curated_snapshots = repo_root / "data" / "curated" / "universe_snapshots.parquet"
        raw_snapshots = repo_root / "data" / "raw" / "universe_snapshots.parquet"
        if curated_snapshots.exists():
            snapshots_path = curated_snapshots
        elif raw_snapshots.exists():
            snapshots_path = raw_snapshots
            print(f"[WARN] Using snapshots from raw directory: {snapshots_path}")
        else:
            raise FileNotFoundError(
                f"Snapshots file not found. Tried: {curated_snapshots}, {raw_snapshots}"
            )
    
    if args.output_dir:
        output_dir = args.output_dir if args.output_dir.is_absolute() else repo_root / args.output_dir
    else:
        output_dir = repo_root / "outputs"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("Running Backtest")
    print("=" * 60)
    print(f"Config: {config_path}")
    print(f"Data directory: {data_dir}")
    print(f"Prices: {prices_path}")
    print(f"Snapshots: {snapshots_path}")
    print(f"Output: {output_dir}")
    print("=" * 60)
    
    backtest_metadata = run_backtest(
        config_path,
        prices_path,
        snapshots_path,
        output_dir,
    )
    
    # Ensure backtest_metadata is a dict (defensive check)
    if not isinstance(backtest_metadata, dict):
        print("[WARN] run_backtest() returned non-dict, using empty dict")
        backtest_metadata = {}
    
    # Generate run metadata
    row_counts = {}
    results_path = output_dir / "backtest_results.csv"
    if results_path.exists():
        results_df = pd.read_csv(results_path)
        row_counts["backtest_results"] = len(results_df)
    
    metadata = create_run_metadata(
        script_name="run_backtest.py",
        config_path=config_path,
        data_paths={
            "backtest_results": results_path,
            "snapshots": snapshots_path,
            "prices": prices_path,
        },
        row_counts=row_counts,
        date_range=backtest_metadata.get("date_range", {}),
        repo_root=repo_root,
    )
    # Add data_dir info to metadata
    metadata["data_dir_used"] = str(data_dir.relative_to(repo_root)) if data_dir.is_relative_to(repo_root) else str(data_dir)
    
    metadata_path = output_dir / "run_metadata_backtest.json"
    save_run_metadata(metadata, metadata_path)

