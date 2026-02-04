#!/usr/bin/env python3
"""Download price, market cap, and volume data from CoinGecko."""

import sys
import argparse
from pathlib import Path
from datetime import date, timedelta
from typing import Optional
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko import download_all_coins
from src.utils.metadata import create_run_metadata, save_run_metadata


def find_last_date_in_data(output_dir: Path) -> Optional[date]:
    """Find the last date present in existing data files."""
    prices_path = output_dir / "prices_daily.parquet"
    if prices_path.exists():
        try:
            prices_df = pd.read_parquet(prices_path)
            if len(prices_df) > 0:
                # Convert index to date if needed
                if isinstance(prices_df.index, pd.DatetimeIndex):
                    last_date = prices_df.index.max().date()
                else:
                    last_date = pd.to_datetime(prices_df.index.max()).date()
                return last_date
        except Exception as e:
            print(f"[WARN] Could not read existing prices file: {e}")
    return None


def append_incremental_data(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    dedupe: bool = True,
) -> pd.DataFrame:
    """
    Append new data to existing DataFrame, removing duplicates.
    
    Args:
        existing_df: Existing DataFrame with date index
        new_df: New DataFrame to append
        dedupe: If True, remove duplicate dates (keep existing)
    
    Returns:
        Combined DataFrame sorted by date
    """
    if existing_df.empty:
        return new_df.sort_index()
    
    if new_df.empty:
        return existing_df.sort_index()
    
    # Ensure both have datetime index
    if not isinstance(existing_df.index, pd.DatetimeIndex):
        existing_df.index = pd.to_datetime(existing_df.index)
    if not isinstance(new_df.index, pd.DatetimeIndex):
        new_df.index = pd.to_datetime(new_df.index)
    
    # Combine: if dedupe, existing takes precedence
    if dedupe:
        # Only add dates that don't exist in existing_df
        new_dates = new_df.index.difference(existing_df.index)
        if len(new_dates) > 0:
            new_df_filtered = new_df.loc[new_dates]
            combined = pd.concat([existing_df, new_df_filtered])
        else:
            combined = existing_df.copy()
    else:
        combined = pd.concat([existing_df, new_df])
        # Remove duplicates (keep first occurrence)
        combined = combined[~combined.index.duplicated(keep='first')]
    
    return combined.sort_index()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download price, market cap, and volume data from CoinGecko",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full download (default: 2 years minus 3 days)
  python scripts/download_data.py

  # Incremental (auto-detect last date and append)
  python scripts/download_data.py --incremental

  # Specific date range
  python scripts/download_data.py --start 2024-01-01 --end 2024-12-31

  # Force full reload
  python scripts/download_data.py --full
        """,
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). If not provided and --incremental, auto-detect from existing data.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: only fetch dates after last date in existing data",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force full reload (overwrite existing data)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: data/curated)",
    )
    
    args = parser.parse_args()
    
    # Paths
    repo_root = Path(__file__).parent.parent
    allowlist_path = repo_root / "data" / "perp_allowlist.csv"
    output_dir = args.output_dir if args.output_dir else repo_root / "data" / "curated"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine date range
    end_date = date.today()
    if args.end:
        end_date = date.fromisoformat(args.end)
    
    # Default: 2 years minus 3 days (727 days) to stay within CoinGecko limits
    DEFAULT_HISTORICAL_DAYS = 727  # 2 years (730 days) - 3 days safety margin
    
    if args.full:
        # Full reload: use default range (2 years minus 3 days)
        start_date = end_date - timedelta(days=DEFAULT_HISTORICAL_DAYS)
        run_mode = "FULL"
    elif args.incremental:
        # Incremental: find last date in existing data
        last_date = find_last_date_in_data(output_dir)
        if last_date:
            # Start from day after last date
            start_date = last_date + timedelta(days=1)
            run_mode = "INCREMENTAL"
        else:
            # No existing data, use default range
            start_date = end_date - timedelta(days=DEFAULT_HISTORICAL_DAYS)
            run_mode = "FULL"  # First run is effectively full
    elif args.start:
        # Explicit start date provided
        start_date = date.fromisoformat(args.start)
        run_mode = "INCREMENTAL" if not args.full else "FULL"
    else:
        # Default: 2 years minus 3 days
        start_date = end_date - timedelta(days=DEFAULT_HISTORICAL_DAYS)
        run_mode = "FULL"
    
    # Check if we need to fetch anything
    if args.incremental and not args.full:
        last_date = find_last_date_in_data(output_dir)
        if last_date and start_date > end_date:
            print("[INFO] No new dates to fetch (data is up to date)")
            sys.exit(0)
    
    print("=" * 60)
    print("CoinGecko Data Download")
    print("=" * 60)
    print(f"Mode: {run_mode}")
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")
    print(f"Allowlist: {allowlist_path}")
    print(f"Output: {output_dir}")
    print("=" * 60)
    
    # Load existing data if incremental
    existing_prices = pd.DataFrame()
    existing_mcaps = pd.DataFrame()
    existing_volumes = pd.DataFrame()
    
    if run_mode == "INCREMENTAL" and not args.full:
        prices_path = output_dir / "prices_daily.parquet"
        mcaps_path = output_dir / "marketcap_daily.parquet"
        volumes_path = output_dir / "volume_daily.parquet"
        
        if prices_path.exists():
            try:
                existing_prices = pd.read_parquet(prices_path)
                if not isinstance(existing_prices.index, pd.DatetimeIndex):
                    existing_prices.index = pd.to_datetime(existing_prices.index)
                print(f"  Loaded existing prices: {len(existing_prices)} rows")
            except Exception as e:
                print(f"  [WARN] Could not load existing prices: {e}")
        
        if mcaps_path.exists():
            try:
                existing_mcaps = pd.read_parquet(mcaps_path)
                if not isinstance(existing_mcaps.index, pd.DatetimeIndex):
                    existing_mcaps.index = pd.to_datetime(existing_mcaps.index)
                print(f"  Loaded existing marketcaps: {len(existing_mcaps)} rows")
            except Exception as e:
                print(f"  [WARN] Could not load existing marketcaps: {e}")
        
        if volumes_path.exists():
            try:
                existing_volumes = pd.read_parquet(volumes_path)
                if not isinstance(existing_volumes.index, pd.DatetimeIndex):
                    existing_volumes.index = pd.to_datetime(existing_volumes.index)
                print(f"  Loaded existing volumes: {len(existing_volumes)} rows")
            except Exception as e:
                print(f"  [WARN] Could not load existing volumes: {e}")
    
    # Download new data to temporary directory
    temp_dir = output_dir / ".temp_download"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        download_all_coins(allowlist_path, start_date, end_date, temp_dir)
        
        # Load newly downloaded data
        new_prices_path = temp_dir / "prices_daily.parquet"
        new_mcaps_path = temp_dir / "marketcap_daily.parquet"
        new_volumes_path = temp_dir / "volume_daily.parquet"
        
        new_prices = pd.DataFrame()
        new_mcaps = pd.DataFrame()
        new_volumes = pd.DataFrame()
        
        if new_prices_path.exists():
            new_prices = pd.read_parquet(new_prices_path)
            if not isinstance(new_prices.index, pd.DatetimeIndex):
                new_prices.index = pd.to_datetime(new_prices.index)
        
        if new_mcaps_path.exists():
            new_mcaps = pd.read_parquet(new_mcaps_path)
            if not isinstance(new_mcaps.index, pd.DatetimeIndex):
                new_mcaps.index = pd.to_datetime(new_mcaps.index)
        
        if new_volumes_path.exists():
            new_volumes = pd.read_parquet(new_volumes_path)
            if not isinstance(new_volumes.index, pd.DatetimeIndex):
                new_volumes.index = pd.to_datetime(new_volumes.index)
        
        # Merge with existing data (idempotent: existing takes precedence)
        if run_mode == "INCREMENTAL" and not args.full:
            print("\nMerging with existing data...")
            prices_combined = append_incremental_data(existing_prices, new_prices, dedupe=True)
            mcaps_combined = append_incremental_data(existing_mcaps, new_mcaps, dedupe=True)
            volumes_combined = append_incremental_data(existing_volumes, new_volumes, dedupe=True)
            
            print(f"  Prices: {len(existing_prices)} existing + {len(new_prices)} new = {len(prices_combined)} total")
            print(f"  Marketcaps: {len(existing_mcaps)} existing + {len(new_mcaps)} new = {len(mcaps_combined)} total")
            print(f"  Volumes: {len(existing_volumes)} existing + {len(new_volumes)} new = {len(volumes_combined)} total")
        else:
            prices_combined = new_prices
            mcaps_combined = new_mcaps
            volumes_combined = new_volumes
        
        # Save combined data
        prices_path = output_dir / "prices_daily.parquet"
        mcaps_path = output_dir / "marketcap_daily.parquet"
        volumes_path = output_dir / "volume_daily.parquet"
        
        print(f"\nSaving to {output_dir}...")
        prices_combined.to_parquet(prices_path)
        mcaps_combined.to_parquet(mcaps_path)
        volumes_combined.to_parquet(volumes_path)
        
    finally:
        # Clean up temp directory
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
    
    # Generate run metadata
    row_counts = {}
    appended_dates = None
    
    if prices_path.exists():
        prices_df = pd.read_parquet(prices_path)
        row_counts["prices"] = len(prices_df)
        row_counts["prices_coins"] = len(prices_df.columns)
        if run_mode == "INCREMENTAL" and len(new_prices) > 0:
            appended_dates = {
                "start": str(new_prices.index.min().date()),
                "end": str(new_prices.index.max().date()),
            }
    
    if mcaps_path.exists():
        mcaps_df = pd.read_parquet(mcaps_path)
        row_counts["marketcaps"] = len(mcaps_df)
        row_counts["marketcaps_coins"] = len(mcaps_df.columns)
    
    if volumes_path.exists():
        volumes_df = pd.read_parquet(volumes_path)
        row_counts["volumes"] = len(volumes_df)
        row_counts["volumes_coins"] = len(volumes_df.columns)
    
    metadata = create_run_metadata(
        script_name="download_data.py",
        data_paths={
            "prices": prices_path,
            "marketcaps": mcaps_path,
            "volumes": volumes_path,
            "perp_allowlist_proxy": allowlist_path,
        },
        row_counts=row_counts,
        date_range={
            "start_date": str(start_date),
            "end_date": str(end_date),
        },
        repo_root=repo_root,
    )
    
    # Add incremental metadata
    metadata["run_mode"] = run_mode
    if appended_dates:
        metadata["appended_date_range"] = appended_dates
    
    metadata_path = output_dir / "run_metadata_download.json"
    save_run_metadata(metadata, metadata_path)
    
    print(f"\n[SUCCESS] Download complete!")
    print(f"  Mode: {run_mode}")
    if appended_dates:
        print(f"  Appended dates: {appended_dates['start']} to {appended_dates['end']}")

