#!/usr/bin/env python3
"""Fetch Coinglass funding rates and save to data lake format."""

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


def fetch_funding_rate_history(
    api_key: str,
    symbol: str,
    exchange: str = "Binance",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = "1d",
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> Optional[pd.DataFrame]:
    """
    Fetch historical funding rates from Coinglass API with retry logic.
    
    Args:
        api_key: Coinglass API key
        symbol: Symbol (e.g., "BTCUSDT", "ETHUSDT") - use full symbol with quote
        exchange: Exchange name (default: "Binance")
        start_date: Start date for historical data
        end_date: End date for historical data
        interval: Data interval (default: "1d" for daily)
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (seconds), will double on each retry
    
    Returns:
        DataFrame with columns: date, symbol, exchange, funding_rate, source
    """
    # Coinglass API v4 endpoint for historical funding rates
    # Endpoint uses hyphen: /api/futures/funding-rate/history
    url = f"{COINGLASS_API_BASE}/futures/funding-rate/history"
    
    headers = {
        "CG-API-KEY": api_key,
        "accept": "application/json",
    }
    
    params = {
        "symbol": symbol,
        "exchange": exchange,
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
                # Rate limited - wait longer before retry (exponential backoff with longer base)
                # For 429, wait at least 60 seconds (1 minute) to reset the rate limit window
                wait_time = max(60.0, retry_delay * (2 ** attempt) * 10)
                print(f"  [RATE LIMIT 429] Hit rate limit for {symbol}, waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                sleep(wait_time)
                continue
            
            if data.get("code") != "0":
                error_msg = data.get("msg", "Unknown error")
                
                # Check for rate limit in error message
                if "too many requests" in error_msg.lower() or "rate limit" in error_msg.lower():
                    if attempt < max_retries - 1:
                        wait_time = max(60.0, retry_delay * (2 ** attempt) * 10)  # Wait at least 60s for rate limit
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
            
            # Parse OHLC response - use "close" as the funding rate for that day
            rows = []
            for item in data.get("data", []):
                time_ms = item.get("time")
                funding_rate_close = item.get("close")  # Use close value as daily funding rate
                
                if time_ms is not None and funding_rate_close is not None:
                    # Convert timestamp (milliseconds) to date
                    dt = datetime.fromtimestamp(time_ms / 1000)
                    
                    rows.append({
                        "date": dt.date(),
                        "symbol": symbol,
                        "exchange": exchange,
                        "funding_rate": float(funding_rate_close),
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


def fetch_funding_rates_for_symbols(
    api_key: str,
    symbols: List[str],
    exchange: str = "Binance",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    rate_limit_per_min: int = 30,
) -> pd.DataFrame:
    """
    Fetch historical funding rates for multiple symbols with rate limiting.
    
    Uses the funding-rate/history endpoint which requires full symbol (e.g., BTCUSDT).
    Implements rate limiting to respect API limits (default: 30 requests/min = 1 every 2 seconds).
    
    Args:
        api_key: Coinglass API key
        symbols: List of base symbols (e.g., ["BTC", "ETH"]) - will be converted to full symbols
        exchange: Exchange name (default: "Binance")
        start_date: Start date for historical data
        end_date: End date for historical data
        rate_limit_per_min: API rate limit (requests per minute)
    
    Returns:
        DataFrame with funding rate data
    """
    all_rows = []
    
    # Calculate delay between requests to respect rate limit
    # 30 requests/min = 1 request every 2 seconds (60/30 = 2)
    min_delay_seconds = 60.0 / rate_limit_per_min
    # Add buffer to be safe - use 2.2 seconds to stay well under limit
    delay_seconds = max(2.2, min_delay_seconds + 0.2)
    
    print(f"  Fetching funding rates from {exchange}...")
    print(f"  Rate limit: {rate_limit_per_min} requests/min ({delay_seconds:.1f}s between requests)")
    print(f"  Estimated time: ~{len(symbols) * delay_seconds / 60:.1f} minutes for {len(symbols)} symbols")
    
    successful = 0
    failed = 0
    
    for i, base_symbol in enumerate(symbols, 1):
        # Convert base symbol to full symbol (e.g., BTC -> BTCUSDT)
        full_symbol = f"{base_symbol}USDT"
        
        try:
            # Fetch historical funding rates (with retry logic built in)
            df = fetch_funding_rate_history(
                api_key=api_key,
                symbol=full_symbol,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                interval="1d",  # Daily interval
                max_retries=3,  # Retry up to 3 times
                retry_delay=5.0,  # Start with 5s delay, exponential backoff
            )
            
            if df is not None and len(df) > 0:
                # Normalize symbol back to base for consistency with our asset_id format
                df["symbol"] = base_symbol
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
            print(f"  [ERROR] Unexpected error processing {base_symbol}: {e}")
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


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol (e.g., BTCUSDT -> BTC)."""
    if symbol.endswith("USDT"):
        return symbol[:-4]
    elif symbol.endswith("USD"):
        return symbol[:-3]
    return symbol


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Coinglass funding rates and save to data lake format",
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
        default=Path("data/curated/data_lake/fact_funding.parquet"),
        help="Output path for funding rates parquet file",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="List of symbols to fetch (default: fetch from Binance perp listings)",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="Binance",
        help="Exchange name (default: Binance)",
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
    print("FETCHING COINGLASS FUNDING RATES")
    print("=" * 70)
    
    # Get symbols to fetch
    symbols = args.symbols
    if symbols is None:
        # Try to load from universe eligibility or basket snapshots first (more efficient)
        # This limits to symbols that are actually used in the strategy
        universe_path = repo_root / "data" / "curated" / "universe_eligibility.parquet"
        basket_path = repo_root / "data" / "curated" / "universe_snapshots.parquet"
        
        eligible_symbols = set()
        if universe_path.exists():
            try:
                universe_df = pd.read_parquet(universe_path)
                if "symbol" in universe_df.columns:
                    eligible_symbols.update(universe_df["symbol"].unique())
            except:
                pass
        
        if basket_path.exists():
            try:
                basket_df = pd.read_parquet(basket_path)
                if "symbol" in basket_df.columns:
                    eligible_symbols.update(basket_df["symbol"].unique())
            except:
                pass
        
        if eligible_symbols:
            symbols = sorted(list(eligible_symbols))
            print(f"  Loaded {len(symbols)} symbols from universe eligibility/basket snapshots")
        else:
            # Fallback to Binance perp listings
            perp_listings_path = None
            for default_path in [
                repo_root / "data" / "raw" / "perp_listings_binance.parquet",
                repo_root / "data" / "curated" / "perp_listings_binance.parquet",
                repo_root / "outputs" / "perp_listings_binance.parquet",
            ]:
                if default_path.exists():
                    perp_listings_path = default_path
                    break
            
            if perp_listings_path:
                perp_df = pd.read_parquet(perp_listings_path)
                # Normalize symbols (BTCUSDT -> BTC)
                symbols = [normalize_symbol(s) for s in perp_df["symbol"].unique() if s]
                print(f"  Loaded {len(symbols)} symbols from Binance perp listings")
            else:
                print("  [ERROR] No symbols provided and no data sources found")
                sys.exit(1)
    
    print(f"  Fetching funding rates for {len(symbols)} symbols from {args.exchange}...")
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
            existing_funding = pd.read_parquet(output_path)
            if len(existing_funding) > 0 and "date" in existing_funding.columns:
                last_date = pd.to_datetime(existing_funding["date"]).max().date()
                if start_date is None or start_date <= last_date:
                    start_date = last_date + timedelta(days=1)
                    print(f"  [INCREMENTAL] Auto-detected start date: {start_date} (last date in file: {last_date})")
        except Exception as e:
            print(f"  [WARN] Could not auto-detect start date from existing file: {e}")
    
    # If no dates provided, try to infer from config or use reasonable defaults
    # For backtesting, we typically want data from the strategy start date
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
    
    # Fetch historical funding rates
    print(f"  Date range: {start_date or 'all available'} to {end_date or 'all available'}")
    
    start_time = time.time()
    funding_df = fetch_funding_rates_for_symbols(
        api_key=args.api_key,
        symbols=symbols,
        exchange=args.exchange,
        start_date=start_date,
        end_date=end_date,
        rate_limit_per_min=args.rate_limit,
    )
    elapsed_time = time.time() - start_time
    print(f"\n  Total time: {elapsed_time / 60:.1f} minutes ({elapsed_time:.1f} seconds)")
    
    # If no data fetched, create empty structure with correct schema
    if funding_df.empty:
        print(f"  [WARN] No funding data fetched from API - creating empty structure")
        print(f"  [INFO] Coinglass API endpoints may need verification or different endpoint path")
        # Create empty DataFrame with correct schema for fact_funding
        funding_df = pd.DataFrame(columns=[
            "asset_id", "instrument_id", "date", "funding_rate", "exchange", "source"
        ])
        # Create minimal structure so pipeline can continue
        print(f"  [INFO] Created empty fact_funding structure - can be populated later when API is verified")
    else:
        # Normalize to asset_id format (symbol should already be normalized, but ensure consistency)
        # Map symbol to asset_id (for now, use symbol as asset_id)
        funding_df["asset_id"] = funding_df["symbol"]
    
    # Try to map to instrument_id if we have dim_instrument
    instrument_lookup = {}
    dim_instrument_path = output_path.parent / "dim_instrument.parquet"
    if dim_instrument_path.exists():
        try:
            dim_instrument = pd.read_parquet(dim_instrument_path)
            # Create lookup: base_asset_symbol -> instrument_id for Binance perps
            binance_perps = dim_instrument[
                (dim_instrument["venue"] == "binance") & 
                (dim_instrument["instrument_type"] == "perpetual")
            ]
            for _, row in binance_perps.iterrows():
                base_symbol = row["base_asset_symbol"]
                instrument_id = row["instrument_id"]
                if base_symbol not in instrument_lookup:
                    instrument_lookup[base_symbol] = instrument_id
        except Exception as e:
            print(f"  [WARN] Could not load instrument lookup: {e}")
    
    # Add instrument_id if available (only if we have data)
    if not funding_df.empty:
        funding_df["instrument_id"] = funding_df["symbol"].map(instrument_lookup)
        # Remove duplicates and sort
        funding_df = funding_df.drop_duplicates(subset=["date", "symbol", "exchange"])
        funding_df = funding_df.sort_values(["date", "symbol"])
        # Reorder columns to match schema
        funding_df = funding_df[["asset_id", "instrument_id", "date", "funding_rate", "exchange", "source"]]
    # If empty, columns are already in correct order from initialization above
    
    # Append to existing if incremental
    if args.incremental and output_path.exists():
        try:
            existing_funding = pd.read_parquet(output_path)
            # Remove duplicates (existing takes precedence)
            existing_keys = set(zip(
                existing_funding["asset_id"],
                existing_funding["date"],
                existing_funding["exchange"]
            ))
            new_keys = set(zip(
                funding_df["asset_id"],
                funding_df["date"],
                funding_df["exchange"]
            ))
            keys_to_add = new_keys - existing_keys
            
            if keys_to_add:
                mask = funding_df.apply(
                    lambda row: (row["asset_id"], row["date"], row["exchange"]) in keys_to_add,
                    axis=1
                )
                funding_to_append = funding_df[mask]
                funding_df = pd.concat([existing_funding, funding_to_append], ignore_index=True)
                print(f"  [INCREMENTAL] Appended {len(funding_to_append):,} new records to existing {len(existing_funding):,} records")
            else:
                funding_df = existing_funding
                print(f"  [INCREMENTAL] No new records to append (all dates already exist)")
        except Exception as e:
            print(f"  [WARN] Could not load existing funding data: {e}, creating new")
    
    # Save to parquet
    funding_df.to_parquet(output_path, index=False)
    if len(funding_df) > 0:
        print(f"\n[SUCCESS] Saved {len(funding_df):,} funding rate records to {output_path}")
        print(f"  Date range: {funding_df['date'].min()} to {funding_df['date'].max()}")
        print(f"  Assets: {funding_df['asset_id'].nunique()}")
        print(f"  Exchange: {funding_df['exchange'].unique()}")
        print(f"  Records with instrument_id: {funding_df['instrument_id'].notna().sum()}")
    else:
        print(f"\n[WARN] Created empty fact_funding structure at {output_path}")
        print(f"  [INFO] Coinglass API endpoints need verification - structure ready for data when API is fixed")
    
    # Generate metadata
    metadata = {
        "script": "fetch_coinglass_funding.py",
        "timestamp": datetime.now().isoformat(),
        "git_commit": get_git_commit_hash(repo_root),
        "api_key_prefix": args.api_key[:8],
        "output_path": str(output_path.relative_to(repo_root)) if output_path.is_relative_to(repo_root) else str(output_path),
        "row_count": len(funding_df),
        "exchange": args.exchange,
        "source": "coinglass",
        "status": "empty_structure" if len(funding_df) == 0 else "populated",
    }
    
    if len(funding_df) > 0:
        metadata["symbol_count"] = funding_df["symbol"].nunique() if "symbol" in funding_df.columns else 0
        if "date" in funding_df.columns and len(funding_df) > 0:
            metadata["date_range"] = {
                "start": str(funding_df["date"].min()),
                "end": str(funding_df["date"].max()),
            }
    
    metadata_path = output_path.parent / "funding_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Metadata saved to {metadata_path}")


if __name__ == "__main__":
    main()
