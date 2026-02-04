#!/usr/bin/env python3
"""
Incremental update script: Only downloads and appends new data to existing fact tables.

This script:
1. Checks existing fact tables to find latest dates
2. Downloads only missing date ranges
3. Merges new data with existing (deduplicates)
4. Saves updated fact tables

Usage:
    python scripts/incremental_update.py
    python scripts/incremental_update.py --days-back 7  # Update last 7 days
"""

import sys
import argparse
from pathlib import Path
from datetime import date, timedelta
from typing import Tuple
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko import download_all_coins
from src.data_lake.schema import (
    FACT_PRICE_SCHEMA,
    FACT_MARKETCAP_SCHEMA,
    FACT_VOLUME_SCHEMA,
)
from src.data_lake.mapping import generate_asset_id

# Import convert function - need to define locally or import properly
def convert_wide_to_fact_table(
    wide_df: pd.DataFrame,
    fact_schema: dict,
    source: str,
    value_column_name: str,
) -> pd.DataFrame:
    """Convert wide DataFrame to normalized fact table."""
    rows = []
    
    # Normalize index to date objects
    if isinstance(wide_df.index, pd.DatetimeIndex):
        wide_df_index = wide_df.index.date
    else:
        wide_df_index = wide_df.index
    
    for date_val in wide_df_index:
        if isinstance(date_val, pd.Timestamp):
            date_obj = date_val.date()
        elif isinstance(date_val, date):
            date_obj = date_val
        else:
            date_obj = pd.to_datetime(date_val).date()
        
        for symbol in wide_df.columns:
            try:
                value = wide_df.loc[date_obj, symbol]
            except KeyError:
                if isinstance(wide_df.index, pd.DatetimeIndex):
                    value = wide_df.loc[pd.Timestamp(date_obj), symbol]
                else:
                    value = wide_df.loc[date_obj, symbol]
            
            if pd.isna(value):
                continue
            
            asset_id = generate_asset_id(symbol=str(symbol))
            
            row = {
                "asset_id": asset_id,
                "date": date_obj,
                value_column_name: float(value),
                "source": source,
            }
            rows.append(row)
    
    return pd.DataFrame(rows)


def get_latest_dates_from_fact_tables(data_lake_dir: Path) -> dict:
    """Get latest date per asset from existing fact tables."""
    latest_dates = {}
    
    for fact_name in ["fact_price", "fact_marketcap", "fact_volume"]:
        fact_path = data_lake_dir / f"{fact_name}.parquet"
        if fact_path.exists():
            try:
                df = pd.read_parquet(fact_path)
                if "date" in df.columns and "asset_id" in df.columns:
                    latest = df.groupby("asset_id")["date"].max()
                    latest_dates[fact_name] = latest.to_dict()
            except Exception as e:
                print(f"  [WARN] Could not read {fact_name}: {e}")
    
    return latest_dates


def get_overall_latest_date(latest_dates: dict) -> date:
    """Get the overall latest date across all fact tables."""
    all_dates = []
    for fact_name, dates_dict in latest_dates.items():
        if dates_dict:
            all_dates.extend(dates_dict.values())
    
    if all_dates:
        return max(all_dates)
    return None


def download_incremental_wide_format(
    allowlist_path: Path,
    start_date: date,
    end_date: date,
    output_dir: Path,
    existing_wide_prices: pd.DataFrame = None,
    existing_wide_mcaps: pd.DataFrame = None,
    existing_wide_volumes: pd.DataFrame = None,
) -> tuple:
    """
    Download new data and merge with existing wide format DataFrames.
    
    Returns:
        Tuple of (merged_prices, merged_mcaps, merged_volumes)
    """
    # Download new data (this will create new files, but we'll merge)
    print(f"  Downloading data from {start_date} to {end_date}...")
    download_all_coins(allowlist_path, start_date, end_date, output_dir)
    
    # Load newly downloaded data
    prices_path = output_dir / "prices_daily.parquet"
    mcaps_path = output_dir / "marketcap_daily.parquet"
    volumes_path = output_dir / "volume_daily.parquet"
    
    new_prices = pd.read_parquet(prices_path) if prices_path.exists() else pd.DataFrame()
    new_mcaps = pd.read_parquet(mcaps_path) if mcaps_path.exists() else pd.DataFrame()
    new_volumes = pd.read_parquet(volumes_path) if volumes_path.exists() else pd.DataFrame()
    
    # Merge with existing
    if existing_wide_prices is not None and not existing_wide_prices.empty:
        # Combine: use existing as base, update with new data
        # NOTE: combine_first prefers existing values over new values (stable history policy).
        # This means provider "corrections" won't overwrite historical data unless
        # an explicit overwrite mode is implemented. This is intentional for data stability.
        merged_prices = existing_wide_prices.combine_first(new_prices)
        # Sort by date
        merged_prices = merged_prices.sort_index()
    else:
        merged_prices = new_prices
    
    if existing_wide_mcaps is not None and not existing_wide_mcaps.empty:
        # NOTE: combine_first prefers existing values (stable history policy)
        merged_mcaps = existing_wide_mcaps.combine_first(new_mcaps)
        merged_mcaps = merged_mcaps.sort_index()
    else:
        merged_mcaps = new_mcaps
    
    if existing_wide_volumes is not None and not existing_wide_volumes.empty:
        # NOTE: combine_first prefers existing values (stable history policy)
        merged_volumes = existing_wide_volumes.combine_first(new_volumes)
        merged_volumes = merged_volumes.sort_index()
    else:
        merged_volumes = new_volumes
    
    return merged_prices, merged_mcaps, merged_volumes


def append_to_fact_table(
    existing_fact: pd.DataFrame,
    new_wide_df: pd.DataFrame,
    fact_schema: dict,
    source: str,
    value_column_name: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert new wide format data to fact table and append to existing.
    
    Fails fast on duplicates in NEW data (guardrail).
    Deduplicates against existing data (keeps last value for same (asset_id, date)).
    
    Returns:
        Tuple of (combined_fact_table, new_fact_table) where new_fact_table is the
        converted new data (useful for logging dates_fetched).
    """
    # Convert new wide data to fact format
    new_fact = convert_wide_to_fact_table(
        wide_df=new_wide_df,
        fact_schema=fact_schema,
        source=source,
        value_column_name=value_column_name,
    )
    
    # FAIL-FAST: Check for duplicates in NEW data before append
    if not new_fact.empty:
        duplicates_in_new = new_fact.duplicated(subset=["asset_id", "date"], keep=False)
        if duplicates_in_new.any():
            dup_rows = new_fact[duplicates_in_new]
            dup_pairs = dup_rows[["asset_id", "date"]].drop_duplicates()
            raise ValueError(
                f"FAIL: Duplicate (asset_id, date) pairs found in NEW data:\n"
                f"{dup_pairs.to_string()}\n"
                f"This indicates a data quality issue. Please investigate the source data."
            )
    
    if existing_fact.empty:
        return new_fact, new_fact
    
    # Combine: append new, deduplicate (this handles overlap with existing data)
    combined = pd.concat([existing_fact, new_fact], ignore_index=True)
    
    # Deduplicate: keep last (newest) value for same (asset_id, date)
    # This handles cases where new data overlaps with existing data
    combined = combined.drop_duplicates(
        subset=["asset_id", "date"],
        keep="last"
    ).sort_values(["date", "asset_id"])
    
    return combined, new_fact


def main():
    parser = argparse.ArgumentParser(
        description="Incrementally update data tables (only download and append new data)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=None,
        help="Number of days back to check/update (default: auto-detect from existing data)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for update (YYYY-MM-DD, default: auto-detect)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for update (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated data directory",
    )
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        default=Path("data/curated/data_lake"),
        help="Data lake directory",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="coingecko",
        help="Data source name",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    curated_dir = (repo_root / args.curated_dir).resolve()
    data_lake_dir = (repo_root / args.data_lake_dir).resolve()
    allowlist_path = repo_root / "data" / "perp_allowlist.csv"
    
    print("=" * 80)
    print("INCREMENTAL DATA UPDATE")
    print("=" * 80)
    
    # Determine date range
    end_date = date.today()
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    
    # Check existing fact tables to find latest dates
    print("\n[Step 1] Checking existing fact tables...")
    latest_dates = get_latest_dates_from_fact_tables(data_lake_dir)
    overall_latest = None
    
    if latest_dates:
        overall_latest = get_overall_latest_date(latest_dates)
        if overall_latest:
            print(f"  Latest date in fact tables: {overall_latest}")
            start_date = overall_latest + timedelta(days=1)
            print(f"  Will download from: {start_date} to {end_date}")
        else:
            start_date = end_date - timedelta(days=args.days_back or 7)
            print(f"  No existing data found, downloading last {args.days_back or 7} days")
    else:
        # No existing data, use days_back or default
        days = args.days_back or 30
        start_date = end_date - timedelta(days=days)
        print(f"  No existing fact tables found, downloading last {days} days")
    
    # Override with command-line args if provided
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    if args.days_back and not args.start_date:
        start_date = end_date - timedelta(days=args.days_back)
    
    # Check if update is needed
    if start_date > end_date:
        latest_str = str(overall_latest) if overall_latest else "N/A"
        print(f"\n[INFO] Data is already up to date! Latest: {latest_str}, Today: {end_date}")
        return
    
    print(f"\n[Step 2] Downloading new data ({start_date} to {end_date})...")
    
    # Load existing wide format files (if they exist)
    existing_prices = pd.DataFrame()
    existing_mcaps = pd.DataFrame()
    existing_volumes = pd.DataFrame()
    
    prices_path = curated_dir / "prices_daily.parquet"
    mcaps_path = curated_dir / "marketcap_daily.parquet"
    volumes_path = curated_dir / "volume_daily.parquet"
    
    if prices_path.exists():
        existing_prices = pd.read_parquet(prices_path)
        print(f"  Loaded existing prices: {len(existing_prices)} days")
    if mcaps_path.exists():
        existing_mcaps = pd.read_parquet(mcaps_path)
        print(f"  Loaded existing marketcaps: {len(existing_mcaps)} days")
    if volumes_path.exists():
        existing_volumes = pd.read_parquet(volumes_path)
        print(f"  Loaded existing volumes: {len(existing_volumes)} days")
    
    # Download and merge wide format
    merged_prices, merged_mcaps, merged_volumes = download_incremental_wide_format(
        allowlist_path=allowlist_path,
        start_date=start_date,
        end_date=end_date,
        output_dir=curated_dir,
        existing_wide_prices=existing_prices,
        existing_wide_mcaps=existing_mcaps,
        existing_wide_volumes=existing_volumes,
    )
    
    # Save merged wide format
    print(f"\n[Step 3] Saving merged wide format files...")
    merged_prices.to_parquet(prices_path)
    print(f"  Saved prices: {len(merged_prices)} days, {len(merged_prices.columns)} coins")
    merged_mcaps.to_parquet(mcaps_path)
    print(f"  Saved marketcaps: {len(merged_mcaps)} days, {len(merged_mcaps.columns)} coins")
    merged_volumes.to_parquet(volumes_path)
    print(f"  Saved volumes: {len(merged_volumes)} days, {len(merged_volumes.columns)} coins")
    
    # Convert to fact tables incrementally
    print(f"\n[Step 4] Updating fact tables incrementally...")
    
    # Load existing fact tables
    fact_price_path = data_lake_dir / "fact_price.parquet"
    fact_mcap_path = data_lake_dir / "fact_marketcap.parquet"
    fact_volume_path = data_lake_dir / "fact_volume.parquet"
    
    existing_fact_price = pd.read_parquet(fact_price_path) if fact_price_path.exists() else pd.DataFrame()
    existing_fact_mcap = pd.read_parquet(fact_mcap_path) if fact_mcap_path.exists() else pd.DataFrame()
    existing_fact_volume = pd.read_parquet(fact_volume_path) if fact_volume_path.exists() else pd.DataFrame()
    
    # Only convert new data (filter wide format to new dates)
    new_prices_wide = merged_prices[merged_prices.index >= pd.Timestamp(start_date)]
    new_mcaps_wide = merged_mcaps[merged_mcaps.index >= pd.Timestamp(start_date)]
    new_volumes_wide = merged_volumes[merged_volumes.index >= pd.Timestamp(start_date)]
    
    # Track incremental update metrics
    rows_appended_per_table = {}
    dates_fetched_per_table = {}
    
    # Append to fact tables
    if not new_prices_wide.empty:
        updated_fact_price, new_fact_price = append_to_fact_table(
            existing_fact_price,
            new_prices_wide,
            FACT_PRICE_SCHEMA,
            args.source,
            "close",
        )
        rows_appended = len(updated_fact_price) - len(existing_fact_price)
        rows_appended_per_table["fact_price"] = rows_appended
        dates_fetched_per_table["fact_price"] = sorted(new_fact_price["date"].unique().tolist()) if not new_fact_price.empty else []
        
        updated_fact_price.to_parquet(fact_price_path, index=False)
        print(f"  Updated fact_price: {rows_appended:,} rows appended, {len(dates_fetched_per_table['fact_price'])} dates fetched")
    
    if not new_mcaps_wide.empty:
        updated_fact_mcap, new_fact_mcap = append_to_fact_table(
            existing_fact_mcap,
            new_mcaps_wide,
            FACT_MARKETCAP_SCHEMA,
            args.source,
            "marketcap",
        )
        rows_appended = len(updated_fact_mcap) - len(existing_fact_mcap)
        rows_appended_per_table["fact_marketcap"] = rows_appended
        dates_fetched_per_table["fact_marketcap"] = sorted(new_fact_mcap["date"].unique().tolist()) if not new_fact_mcap.empty else []
        
        updated_fact_mcap.to_parquet(fact_mcap_path, index=False)
        print(f"  Updated fact_marketcap: {rows_appended:,} rows appended, {len(dates_fetched_per_table['fact_marketcap'])} dates fetched")
    
    if not new_volumes_wide.empty:
        updated_fact_volume, new_fact_volume = append_to_fact_table(
            existing_fact_volume,
            new_volumes_wide,
            FACT_VOLUME_SCHEMA,
            args.source,
            "volume",
        )
        rows_appended = len(updated_fact_volume) - len(existing_fact_volume)
        rows_appended_per_table["fact_volume"] = rows_appended
        dates_fetched_per_table["fact_volume"] = sorted(new_fact_volume["date"].unique().tolist()) if not new_fact_volume.empty else []
        
        updated_fact_volume.to_parquet(fact_volume_path, index=False)
        print(f"  Updated fact_volume: {rows_appended:,} rows appended, {len(dates_fetched_per_table['fact_volume'])} dates fetched")
    
    print("\n" + "=" * 80)
    print("INCREMENTAL UPDATE COMPLETE")
    print("=" * 80)
    print(f"\nUpdated date range: {start_date} to {end_date}")
    print(f"Fact tables saved to: {data_lake_dir}")
    
    # Print structured summary
    if rows_appended_per_table:
        print(f"\n[Summary] Rows appended per table:")
        for table_name, rows_appended in rows_appended_per_table.items():
            dates_count = len(dates_fetched_per_table.get(table_name, []))
            print(f"  {table_name}: {rows_appended:,} rows, {dates_count} dates")
        
        print(f"\n[Summary] Dates fetched:")
        all_dates = set()
        for dates_list in dates_fetched_per_table.values():
            all_dates.update(dates_list)
        if all_dates:
            print(f"  Unique dates: {len(all_dates)} ({min(all_dates)} to {max(all_dates)})")


if __name__ == "__main__":
    main()
