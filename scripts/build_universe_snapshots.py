#!/usr/bin/env python3
"""Build point-in-time universe snapshots."""

import sys
import argparse
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.universe.snapshot import build_snapshots
from src.utils.metadata import create_run_metadata, save_run_metadata

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build universe snapshots")
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
        help="Directory containing data files (default: data/curated, fallback to data/raw)",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    config_path = args.config
    
    # Determine data directory
    if args.data_dir:
        data_dir = args.data_dir if args.data_dir.is_absolute() else repo_root / args.data_dir
    else:
        # Default to curated, fallback to raw if curated doesn't exist
        curated_dir = repo_root / "data" / "curated"
        raw_dir = repo_root / "data" / "raw"
        if (curated_dir / "prices_daily.parquet").exists():
            data_dir = curated_dir
        else:
            data_dir = raw_dir
    
    # Try data lake format first, fallback to wide format
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    
    if data_lake_dir.exists() and (data_lake_dir / "fact_price.parquet").exists():
        # Use data lake format
        print(f"Using data lake format from {data_lake_dir}")
        prices_path = data_lake_dir / "fact_price.parquet"
        mcaps_path = data_lake_dir / "fact_marketcap.parquet"
        volumes_path = data_lake_dir / "fact_volume.parquet"
        use_data_lake = True
    else:
        # Fallback to wide format
        prices_path = data_dir / "prices_daily.parquet"
        mcaps_path = data_dir / "marketcap_daily.parquet"
        volumes_path = data_dir / "volume_daily.parquet"
        use_data_lake = False
    allowlist_path = repo_root / "data" / "perp_allowlist.csv"
    output_path = repo_root / "data" / "curated" / "universe_snapshots.parquet"
    
    # Try to find Binance perp listings dataset
    perp_listings_path = None
    for default_path in [
        repo_root / "data" / "raw" / "perp_listings_binance.parquet",
        repo_root / "data" / "curated" / "perp_listings_binance.parquet",
        repo_root / "outputs" / "perp_listings_binance.parquet",
    ]:
        if default_path.exists():
            perp_listings_path = default_path
            break
    
    print("=" * 60)
    print("Building Universe Snapshots")
    print("=" * 60)
    print(f"Config: {config_path}")
    print(f"Data directory: {data_dir}")
    print(f"Prices: {prices_path}")
    print(f"Market caps: {mcaps_path}")
    print(f"Volumes: {volumes_path}")
    print(f"Allowlist: {allowlist_path}")
    if perp_listings_path:
        print(f"Perp listings: {perp_listings_path}")
    else:
        print(f"Perp listings: Not found (using allowlist-only check)")
    print(f"Output: {output_path}")
    print("=" * 60)
    
    blacklist_path = repo_root / "data" / "blacklist.csv"
    stablecoins_path = repo_root / "data" / "stablecoins.csv"
    wrapped_path = repo_root / "data" / "wrapped.csv"
    
    snapshot_metadata = build_snapshots(
        config_path,
        prices_path,
        mcaps_path,
        volumes_path,
        allowlist_path,
        output_path,
        blacklist_path=blacklist_path,
        stablecoins_path=stablecoins_path,
        wrapped_path=wrapped_path,
        perp_listings_path=perp_listings_path,
    )
    
    # build_snapshots() always returns a dict, but add defensive check for robustness
    if not isinstance(snapshot_metadata, dict):
        print("[WARN] build_snapshots() returned non-dict, using empty dict")
        snapshot_metadata = {}
    
    # Generate run metadata
    row_counts = {}
    if output_path.exists():
        snapshots_df = pd.read_parquet(output_path)
        row_counts["snapshots"] = len(snapshots_df)
        row_counts["snapshot_dates"] = len(snapshots_df["rebalance_date"].unique())
    
    metadata = create_run_metadata(
        script_name="build_universe_snapshots.py",
        config_path=config_path,
        data_paths={
            "snapshots": output_path,
            "prices": prices_path,
            "marketcaps": mcaps_path,
            "volumes": volumes_path,
            "perp_allowlist_proxy": allowlist_path,
        },
        row_counts=row_counts,
        filter_thresholds=snapshot_metadata.get("filter_thresholds", {}),
        date_range=snapshot_metadata.get("date_range", {}),
        repo_root=repo_root,
    )
    # Add data_dir info to metadata
    metadata["data_dir_used"] = str(data_dir.relative_to(repo_root)) if data_dir.is_relative_to(repo_root) else str(data_dir)
    
    metadata_path = repo_root / "data" / "curated" / "run_metadata_snapshots.json"
    save_run_metadata(metadata, metadata_path)

