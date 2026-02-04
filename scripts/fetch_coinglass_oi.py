#!/usr/bin/env python3
"""Fetch Coinglass Open Interest (OI) data and save to data lake format."""

import sys
import argparse
import requests
import os
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import json
import time
import io
from time import sleep

# Windows encoding fix
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.metadata import get_git_commit_hash


# Coinglass API v4 (latest)
COINGLASS_API_BASE = "https://open-api-v4.coinglass.com/api"


def fetch_oi_history(
    api_key: str,
    symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = "8h",  # OI data typically uses 8h intervals
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> Optional[pd.DataFrame]:
    """
    Fetch historical Open Interest from Coinglass API with retry logic.
    
    Uses the aggregated-history endpoint which returns OHLC data.
    We use the "close" value as the daily OI value.
    
    Args:
        api_key: Coinglass API key
        symbol: Symbol (e.g., "BTC", "ETH") - base symbol without quote
        start_date: Start date for historical data
        end_date: End date for historical data
        interval: Data interval (default: "8h" for 8-hourly candles)
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (seconds), will double on each retry
    
    Returns:
        DataFrame with columns: date, asset_id, open_interest_usd, source
    """
    # Coinglass API v4 endpoint for historical OI
    # Endpoint: /api/futures/open-interest/aggregated-history
    url = f"{COINGLASS_API_BASE}/futures/open-interest/aggregated-history"
    
    headers = {
        "CG-API-KEY": api_key,
        "accept": "application/json",
    }
    
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
    }
    
    if start_date:
        # Convert date to Unix timestamp in milliseconds
        params["startTime"] = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
    if end_date:
        params["endTime"] = int(datetime.combine(end_date, datetime.max.time()).timestamp() * 1000)
    
    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Check for rate limiting (429) or server errors (500)
            if response.status_code == 429:
                wait_time = max(60.0, retry_delay * (2 ** attempt) * 10)
                print(f"  [RATE LIMIT 429] Hit rate limit for {symbol}, waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                sleep(wait_time)
                continue
            
            if data.get("code") != "0":
                error_msg = data.get("msg", "Unknown error")
                
                # Check for rate limit in error message
                if "too many requests" in error_msg.lower() or "rate limit" in error_msg.lower():
                    if attempt < max_retries - 1:
                        wait_time = max(60.0, retry_delay * (2 ** attempt) * 10)
                        print(f"  [RATE LIMIT] {symbol}: {error_msg}, waiting {wait_time:.1f}s...")
                        sleep(wait_time)
                        continue
                    else:
                        print(f"  [ERROR] {symbol}: {error_msg} (max retries reached)")
                        return None
                
                # Some errors are permanent (e.g., symbol not found), don't retry
                if "not found" in error_msg.lower() or "invalid" in error_msg.lower():
                    print(f"  [SKIP] {symbol}: {error_msg}")
                    return None
                
                # For other errors, retry with exponential backoff
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"  [RETRY] {symbol}: {error_msg}, waiting {wait_time:.1f}s...")
                    sleep(wait_time)
                    continue
                else:
                    print(f"  [ERROR] {symbol}: {error_msg} (max retries reached)")
                    return None
            
            # Parse OHLC response - aggregate to daily using "close" value
            rows = []
            per_day_closes: Dict[date, List[float]] = {}
            
            for item in data.get("data", []):
                time_ms = item.get("time")
                close_val = item.get("close")  # OI value at close of candle
                
                if time_ms is not None and close_val is not None:
                    # Convert timestamp (milliseconds) to date
                    dt_obj = datetime.fromtimestamp(time_ms / 1000)
                    d = dt_obj.date()
                    
                    # Collect all closes for each day (in case of 8h intervals, we get 3 per day)
                    per_day_closes.setdefault(d, []).append(float(close_val))
            
            # Aggregate to daily: use the last close value of the day (or mean if multiple)
            for d, closes in per_day_closes.items():
                # Use the last value of the day (most recent)
                daily_oi = closes[-1] if closes else None
                if daily_oi is not None:
                    rows.append({
                        "date": d,
                        "asset_id": symbol.upper(),  # Use symbol as asset_id
                        "open_interest_usd": float(daily_oi),
                        "source": "coinglass",
                    })
            
            if rows:
                return pd.DataFrame(rows)
            return None
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                print(f"  [TIMEOUT] {symbol}, retrying in {wait_time:.1f}s...")
                sleep(wait_time)
                continue
            else:
                print(f"  [ERROR] {symbol}: Timeout after {max_retries} attempts")
                return None
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                print(f"  [RETRY] {symbol}: {e}, waiting {wait_time:.1f}s...")
                sleep(wait_time)
                continue
            else:
                print(f"  [ERROR] {symbol}: {e} (max retries reached)")
                return None
                
        except Exception as e:
            print(f"  [ERROR] {symbol}: Unexpected error: {e}")
            return None
    
    return None


def fetch_oi_for_symbols(
    api_key: str,
    symbols: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    rate_limit_per_min: int = 30,
) -> pd.DataFrame:
    """
    Fetch historical OI for multiple symbols with rate limiting.
    
    Args:
        api_key: Coinglass API key
        symbols: List of base symbols (e.g., ["BTC", "ETH"])
        start_date: Start date for historical data
        end_date: End date for historical data
        rate_limit_per_min: API rate limit (requests per minute)
    
    Returns:
        DataFrame with OI data
    """
    all_rows = []
    
    # Calculate delay between requests to respect rate limit
    min_delay_seconds = 60.0 / rate_limit_per_min
    delay_seconds = max(2.2, min_delay_seconds + 0.2)
    
    print(f"  Fetching OI data from CoinGlass...")
    print(f"  Rate limit: {rate_limit_per_min} requests/min ({delay_seconds:.1f}s between requests)")
    print(f"  Estimated time: ~{len(symbols) * delay_seconds / 60:.1f} minutes for {len(symbols)} symbols")
    
    successful = 0
    failed = 0
    
    for i, symbol in enumerate(symbols, 1):
        try:
            # Fetch historical OI (with retry logic built in)
            df = fetch_oi_history(
                api_key=api_key,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                interval="8h",  # 8-hourly candles, aggregated to daily
                max_retries=3,
                retry_delay=5.0,
            )
            
            if df is not None and len(df) > 0:
                all_rows.append(df)
                successful += 1
            else:
                failed += 1
            
            # Progress indicator
            if i % 5 == 0 or i == len(symbols):
                total_rows = sum(len(df) for df in all_rows) if all_rows else 0
                elapsed_est = i * delay_seconds / 60
                remaining_est = (len(symbols) - i) * delay_seconds / 60
                print(f"    Progress: {i}/{len(symbols)} symbols ({successful} success, {failed} failed), "
                      f"{total_rows:,} records, ~{remaining_est:.1f} min remaining...")
            
            # Rate limiting: wait before next request (except for last symbol)
            if i < len(symbols):
                sleep(delay_seconds)
            
        except Exception as e:
            print(f"  [ERROR] Unexpected error processing {symbol}: {e}")
            failed += 1
            # Still wait to respect rate limit even on error
            if i < len(symbols):
                sleep(delay_seconds)
            continue
    
    if all_rows:
        combined_df = pd.concat(all_rows, ignore_index=True)
        print(f"\n  Completed: {successful} successful, {failed} failed, {len(combined_df):,} total records")
        return combined_df
    
    print(f"\n  Completed: {successful} successful, {failed} failed, 0 total records")
    return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Coinglass Open Interest data and save to data lake format",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Coinglass API key (optional if COINGLASS_API_KEY env var is set)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/data_lake/fact_open_interest.parquet"),
        help="Output path for OI parquet file",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="List of symbols to fetch (default: BTC only, or from config)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for historical data (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for historical data (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=30,
        help="API rate limit (requests per minute, default: 30 for hobbyist plan)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: only fetch dates after last date in existing file",
    )
    
    args = parser.parse_args()

    # Resolve API key (prefer env var to avoid leaking key in shell history)
    if not args.api_key:
        args.api_key = os.environ.get("COINGLASS_API_KEY")
    if not args.api_key:
        print("ERROR: Missing Coinglass API key. Provide --api-key or set COINGLASS_API_KEY.")
        sys.exit(1)
    
    repo_root = Path(__file__).parent.parent
    output_path = (repo_root / args.output).resolve() if not args.output.is_absolute() else args.output
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("FETCHING COINGLASS OPEN INTEREST DATA")
    print("=" * 70)
    
    # Get symbols to fetch
    symbols = args.symbols
    if symbols is None:
        # Default: fetch BTC only (most important for regime monitor)
        # Can be extended to fetch ETH and other majors if needed
        symbols = ["BTC"]
        print(f"  No symbols provided, defaulting to BTC only")
        print(f"  (To fetch more symbols, use --symbols BTC ETH ...)")
    
    print(f"  Fetching OI for {len(symbols)} symbols: {symbols}")
    print(f"  API key: {args.api_key[:8]}...")
    
    # Parse dates if provided
    start_date = None
    end_date = None
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    
    # If incremental mode, find last date in existing file
    if args.incremental and output_path.exists():
        try:
            existing_oi = pd.read_parquet(output_path)
            if len(existing_oi) > 0 and "date" in existing_oi.columns:
                last_date = pd.to_datetime(existing_oi["date"]).max().date()
                if start_date is None or start_date <= last_date:
                    start_date = last_date + timedelta(days=1)
                    print(f"  [INCREMENTAL] Auto-detected start date: {start_date} (last date in file: {last_date})")
        except Exception as e:
            print(f"  [WARN] Could not auto-detect start date from existing file: {e}")
    
    # If no dates provided, try to infer from config or use reasonable defaults
    if start_date is None or end_date is None:
        # Try to load from config if available
        config_path = repo_root / "configs" / "golden.yaml"
        if config_path.exists():
            try:
                import yaml
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                    if start_date is None and "start_date" in config:
                        start_date = date.fromisoformat(config["start_date"])
                    if end_date is None and "end_date" in config:
                        end_date = date.fromisoformat(config["end_date"])
            except:
                pass
    
    # Fetch historical OI
    print(f"  Date range: {start_date or 'all available'} to {end_date or 'all available'}")
    
    start_time = time.time()
    oi_df = fetch_oi_for_symbols(
        api_key=args.api_key,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        rate_limit_per_min=args.rate_limit,
    )
    elapsed_time = time.time() - start_time
    print(f"\n  Total time: {elapsed_time / 60:.1f} minutes ({elapsed_time:.1f} seconds)")
    
    # If no data fetched, create empty structure with correct schema
    if oi_df.empty:
        print(f"  [WARN] No OI data fetched from API - creating empty structure")
        oi_df = pd.DataFrame(columns=[
            "asset_id", "date", "open_interest_usd", "source"
        ])
    else:
        # Remove duplicates and sort
        oi_df = oi_df.drop_duplicates(subset=["date", "asset_id"])
        oi_df = oi_df.sort_values(["date", "asset_id"])
        # Reorder columns to match schema
        oi_df = oi_df[["asset_id", "date", "open_interest_usd", "source"]]
    
    # Append to existing if incremental
    if args.incremental and output_path.exists():
        try:
            existing_oi = pd.read_parquet(output_path)
            # Remove duplicates (existing takes precedence)
            existing_keys = set(zip(
                existing_oi["asset_id"],
                existing_oi["date"]
            ))
            new_keys = set(zip(
                oi_df["asset_id"],
                oi_df["date"]
            ))
            keys_to_add = new_keys - existing_keys
            
            if keys_to_add:
                mask = oi_df.apply(
                    lambda row: (row["asset_id"], row["date"]) in keys_to_add,
                    axis=1
                )
                oi_to_append = oi_df[mask]
                oi_df = pd.concat([existing_oi, oi_to_append], ignore_index=True)
                print(f"  [INCREMENTAL] Appended {len(oi_to_append):,} new records to existing {len(existing_oi):,} records")
            else:
                oi_df = existing_oi
                print(f"  [INCREMENTAL] No new records to append (all dates already exist)")
        except Exception as e:
            print(f"  [WARN] Could not load existing OI data: {e}, creating new")
    
    # Save to parquet
    oi_df.to_parquet(output_path, index=False)
    if len(oi_df) > 0:
        print(f"\n[SUCCESS] Saved {len(oi_df):,} OI records to {output_path}")
        print(f"  Date range: {oi_df['date'].min()} to {oi_df['date'].max()}")
        print(f"  Assets: {oi_df['asset_id'].nunique()}")
    else:
        print(f"\n[WARN] Created empty fact_open_interest structure at {output_path}")
    
    # Generate metadata
    metadata = {
        "script": "fetch_coinglass_oi.py",
        "timestamp": datetime.now().isoformat(),
        "git_commit": get_git_commit_hash(repo_root),
        "api_key_prefix": args.api_key[:8],
        "output_path": str(output_path.relative_to(repo_root)) if output_path.is_relative_to(repo_root) else str(output_path),
        "row_count": len(oi_df),
        "source": "coinglass",
        "status": "empty_structure" if len(oi_df) == 0 else "populated",
    }
    
    if len(oi_df) > 0:
        if "date" in oi_df.columns and len(oi_df) > 0:
            metadata["date_range"] = {
                "start": str(oi_df["date"].min()),
                "end": str(oi_df["date"].max()),
            }
        metadata["asset_count"] = oi_df["asset_id"].nunique()
    
    metadata_path = output_path.parent / "oi_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Metadata saved to {metadata_path}")


if __name__ == "__main__":
    main()
