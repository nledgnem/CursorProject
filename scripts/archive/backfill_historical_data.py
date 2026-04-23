#!/usr/bin/env python3
"""
Backfill historical data for all assets from 2013 to start of existing data.

This script follows the same pattern as download_data.py but:
1. Works with fact tables (data lake format) instead of wide format
2. Fetches from 2013-01-01 to the earliest existing date
3. Uses allowlist for correct CoinGecko IDs (same as download_data.py)
"""

import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Optional, Dict, Tuple
import polars as pl
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko import fetch_price_history
from src.data_lake.mapping import generate_asset_id


def find_earliest_date_in_fact_table(data_lake_dir: Path, table_name: str) -> Optional[date]:
    """Find the earliest date in an existing fact table."""
    filepath = data_lake_dir / f"{table_name}.parquet"
    
    if not filepath.exists():
        return None
    
    try:
        df = pl.read_parquet(str(filepath))
        if "date" in df.columns and len(df) > 0:
            min_date = df.select(pl.col("date").min()).item()
            return min_date
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    
    return None


def safe_print(text: str) -> None:
    """Print text safely, handling Unicode encoding errors on Windows."""
    try:
        print(text, end="", flush=True)
    except UnicodeEncodeError:
        # Replace problematic characters with ASCII equivalents
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text, end="", flush=True)


def backfill_historical_data(
    data_lake_dir: Path,
    allowlist_path: Path,
    start_date: date = date(2013, 1, 1),
    max_assets: Optional[int] = None,
):
    """
    Backfill historical data for all assets in allowlist.
    
    Args:
        data_lake_dir: Path to data lake directory
        allowlist_path: Path to allowlist CSV with symbol and coingecko_id columns
        start_date: Start date for backfill (default: 2013-01-01)
        max_assets: Optional limit on number of assets (for testing)
    """
    print("=" * 80)
    print("HISTORICAL DATA BACKFILL")
    print("=" * 80)
    print(f"Data Lake Directory: {data_lake_dir}")
    print(f"Allowlist: {allowlist_path}")
    print(f"Start Date: {start_date}")
    print()
    
    # Find earliest date in existing data
    print("Checking existing data...")
    earliest_price_date = find_earliest_date_in_fact_table(data_lake_dir, "fact_price")
    earliest_mcap_date = find_earliest_date_in_fact_table(data_lake_dir, "fact_marketcap")
    earliest_vol_date = find_earliest_date_in_fact_table(data_lake_dir, "fact_volume")
    
    # Use the earliest date across all tables
    earliest_date = min(
        d for d in [earliest_price_date, earliest_mcap_date, earliest_vol_date] 
        if d is not None
    ) if any(d is not None for d in [earliest_price_date, earliest_mcap_date, earliest_vol_date]) else None
    
    if earliest_date is None:
        print("No existing data found. Will backfill to today.")
        end_date = date.today()
    else:
        print(f"Existing data starts at: {earliest_date}")
        end_date = earliest_date - timedelta(days=1)
        print(f"Will backfill from {start_date} to {end_date}")
    
    if end_date < start_date:
        print("No backfill needed - existing data already covers requested range.")
        return
    
    # Load allowlist (same as download_data.py)
    print(f"\nLoading allowlist from {allowlist_path}")
    allowlist_df = pd.read_csv(allowlist_path)
    
    # Limit assets if specified (for testing)
    if max_assets:
        allowlist_df = allowlist_df.head(max_assets)
        print(f"Limited to {max_assets} assets for testing")
    
    total_coins = len(allowlist_df)
    print(f"Downloading historical data for {total_coins} coins from {start_date} to {end_date}...")
    days_diff = (end_date - start_date).days + 1
    print(f"Date range: {days_diff} days")
    # Rate limit: 500 calls/min = 0.12s per call (Analyst tier)
    estimated_minutes = (total_coins * 0.12) / 60.0
    print(f"Estimated time: ~{estimated_minutes:.1f} minutes (500 calls/min rate limit)\n")
    
    # Prepare data structures (keyed by asset_id, date)
    all_prices: Dict[Tuple[str, date], float] = {}
    all_mcaps: Dict[Tuple[str, date], float] = {}
    all_volumes: Dict[Tuple[str, date], float] = {}
    
    successful = 0
    failed = 0
    skipped = 0
    
    # Load dim_asset to map symbol -> asset_id
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    symbol_to_asset_id = {}
    if dim_asset_path.exists():
        try:
            dim_asset = pl.read_parquet(str(dim_asset_path))
            if "asset_id" in dim_asset.columns and "symbol" in dim_asset.columns:
                for row in dim_asset.to_dicts():
                    symbol = str(row.get("symbol", "")).upper()
                    asset_id = row.get("asset_id")
                    if symbol and asset_id:
                        symbol_to_asset_id[symbol] = asset_id
            print(f"  Loaded {len(symbol_to_asset_id)} symbol->asset_id mappings from dim_asset")
        except Exception as e:
            print(f"  Warning: Could not load dim_asset: {e}")
    
    # Fetch data for each coin in allowlist (same pattern as download_all_coins)
    for idx, row in allowlist_df.iterrows():
        symbol = str(row["symbol"]).upper()
        cg_id = str(row["coingecko_id"]).lower().strip()
        
        # Get asset_id from dim_asset, or generate it
        asset_id = symbol_to_asset_id.get(symbol)
        if not asset_id:
            asset_id = generate_asset_id(symbol=symbol)
        
        progress_pct = (idx + 1) / total_coins * 100
        safe_print(f"[{idx+1}/{total_coins}] ({progress_pct:.1f}%) {symbol} ({cg_id})... ")
        
        try:
            prices, mcaps, vols = fetch_price_history(
                coingecko_id=cg_id,
                start_date=start_date,
                end_date=end_date,
                sleep_seconds=0.12,  # Analyst tier: 500 calls/min
            )
            
            if prices and len(prices) > 0:
                # Store data keyed by (asset_id, date)
                for d, price in prices.items():
                    all_prices[(asset_id, d)] = price
                for d, mcap in mcaps.items():
                    if mcap and mcap > 0:  # Only store valid marketcaps
                        all_mcaps[(asset_id, d)] = mcap
                for d, vol in vols.items():
                    if vol and vol > 0:  # Only store valid volumes
                        all_volumes[(asset_id, d)] = vol
                
                successful += 1
                safe_print(f"[OK] {len(prices)} days | Success: {successful}, Failed: {failed}, Skipped: {skipped}\n")
            else:
                skipped += 1
                safe_print(f"[SKIP] No data | Success: {successful}, Failed: {failed}, Skipped: {skipped}\n")
        
        except KeyboardInterrupt:
            print("\n\n[INTERRUPTED] Backfill interrupted by user")
            print(f"Progress saved: {successful} assets processed, {len(all_prices):,} records collected")
            break
        except Exception as e:
            failed += 1
            error_msg = str(e)[:50] if len(str(e)) > 50 else str(e)
            # Don't print full error for 404s (common for old/invalid coins)
            if "404" in str(e) or "not found" in str(e).lower():
                skipped += 1
                failed -= 1  # Count 404s as skipped, not failed
                safe_print(f"[SKIP] No data (404) | Success: {successful}, Failed: {failed}, Skipped: {skipped}\n")
            else:
                safe_print(f"[ERROR] {error_msg} | Success: {successful}, Failed: {failed}, Skipped: {skipped}\n")
        
        # Progress summary every 50 assets
        if (idx + 1) % 50 == 0:
            print(f"\n--- Progress: {idx+1}/{total_coins} ({progress_pct:.1f}%) | Success: {successful}, Failed: {failed}, Skipped: {skipped} ---\n")
    
    print(f"\n\nDownload Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")
    print(f"  Total price records: {len(all_prices):,}")
    print(f"  Total marketcap records: {len(all_mcaps):,}")
    print(f"  Total volume records: {len(all_volumes):,}")
    
    if len(all_prices) == 0:
        print("\nERROR: No data was downloaded. Cannot proceed with backfill.")
        return
    
    # Convert to fact table format
    print("\nConverting to fact table format...")
    
    price_rows = []
    for (asset_id, d), price in all_prices.items():
        price_rows.append({
            "asset_id": asset_id,
            "date": d,
            "close": float(price),
            "source": "coingecko",
        })
    
    mcap_rows = []
    for (asset_id, d), mcap in all_mcaps.items():
        mcap_rows.append({
            "asset_id": asset_id,
            "date": d,
            "marketcap": float(mcap),
            "source": "coingecko",
        })
    
    volume_rows = []
    for (asset_id, d), vol in all_volumes.items():
        volume_rows.append({
            "asset_id": asset_id,
            "date": d,
            "volume": float(vol),
            "source": "coingecko",
        })
    
    prices_new = pl.DataFrame(price_rows)
    mcaps_new = pl.DataFrame(mcap_rows)
    volumes_new = pl.DataFrame(volume_rows)
    
    print(f"  Created {len(prices_new):,} price records")
    print(f"  Created {len(mcaps_new):,} marketcap records")
    print(f"  Created {len(volumes_new):,} volume records")
    
    # Load existing fact tables
    print("\nLoading existing fact tables...")
    existing_prices = None
    existing_mcaps = None
    existing_volumes = None
    
    if (data_lake_dir / "fact_price.parquet").exists():
        existing_prices = pl.read_parquet(str(data_lake_dir / "fact_price.parquet"))
        print(f"  Loaded {len(existing_prices):,} existing price records")
    
    if (data_lake_dir / "fact_marketcap.parquet").exists():
        existing_mcaps = pl.read_parquet(str(data_lake_dir / "fact_marketcap.parquet"))
        print(f"  Loaded {len(existing_mcaps):,} existing marketcap records")
    
    if (data_lake_dir / "fact_volume.parquet").exists():
        existing_volumes = pl.read_parquet(str(data_lake_dir / "fact_volume.parquet"))
        print(f"  Loaded {len(existing_volumes):,} existing volume records")
    
    # Combine with existing data (deduplicate)
    print("\nMerging with existing data (deduplicating)...")
    
    if existing_prices is not None:
        prices_combined = pl.concat([prices_new, existing_prices]).unique(subset=["asset_id", "date"])
        print(f"  Combined prices: {len(prices_combined):,} records (removed duplicates)")
    else:
        prices_combined = prices_new
    
    if existing_mcaps is not None:
        mcaps_combined = pl.concat([mcaps_new, existing_mcaps]).unique(subset=["asset_id", "date"])
        print(f"  Combined marketcaps: {len(mcaps_combined):,} records (removed duplicates)")
    else:
        mcaps_combined = mcaps_new
    
    if existing_volumes is not None:
        volumes_combined = pl.concat([volumes_new, existing_volumes]).unique(subset=["asset_id", "date"])
        print(f"  Combined volumes: {len(volumes_combined):,} records (removed duplicates)")
    else:
        volumes_combined = volumes_new
    
    # Sort by asset_id and date
    print("\nSorting data...")
    prices_combined = prices_combined.sort(["asset_id", "date"])
    mcaps_combined = mcaps_combined.sort(["asset_id", "date"])
    volumes_combined = volumes_combined.sort(["asset_id", "date"])
    
    # Save updated fact tables
    print("\nSaving updated fact tables...")
    prices_combined.write_parquet(str(data_lake_dir / "fact_price.parquet"))
    mcaps_combined.write_parquet(str(data_lake_dir / "fact_marketcap.parquet"))
    volumes_combined.write_parquet(str(data_lake_dir / "fact_volume.parquet"))
    
    print("\n" + "=" * 80)
    print("BACKFILL COMPLETE!")
    print("=" * 80)
    print(f"  Prices: {len(prices_combined):,} records")
    print(f"  Market caps: {len(mcaps_combined):,} records")
    print(f"  Volumes: {len(volumes_combined):,} records")
    
    # Show date ranges
    if len(prices_combined) > 0:
        min_date = prices_combined.select(pl.col("date").min()).item()
        max_date = prices_combined.select(pl.col("date").max()).item()
        print(f"\n  Date range: {min_date} to {max_date}")
        print(f"  Total days: {(max_date - min_date).days + 1}")
        print(f"  Unique assets: {prices_combined.select(pl.col('asset_id').n_unique()).item()}")
    
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Backfill historical data from 2013")
    parser.add_argument(
        "--start-date",
        type=str,
        default="2013-01-01",
        help="Start date for backfill (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=None,
        help="Path to allowlist CSV (default: data/perp_allowlist.csv)"
    )
    parser.add_argument(
        "--max-assets",
        type=int,
        help="Limit number of assets (for testing)"
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    start_date = date.fromisoformat(args.start_date)
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    
    if args.allowlist:
        allowlist_path = args.allowlist
    else:
        allowlist_path = repo_root / "data" / "perp_allowlist.csv"
    
    if not allowlist_path.exists():
        print(f"ERROR: Allowlist not found: {allowlist_path}")
        sys.exit(1)
    
    backfill_historical_data(
        data_lake_dir=data_lake_dir,
        allowlist_path=allowlist_path,
        start_date=start_date,
        max_assets=args.max_assets,
    )
