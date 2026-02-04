#!/usr/bin/env python3
"""Fetch Coinglass funding rates and Open Interest (OI) data and save to data lake format."""

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


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol (e.g., BTCUSDT -> BTC)."""
    if symbol.endswith("USDT"):
        return symbol[:-4]
    elif symbol.endswith("USD"):
        return symbol[:-3]
    return symbol


# ============================================================
# Funding Rate Functions
# ============================================================

def fetch_funding_rate_history(
    api_key: str,
    symbol: str,
    exchange: str = "Binance",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = "1d",
    max_retries: int = 3,
    retry_delay: float = 5.0,
    max_time_seconds: float = 600.0,  # 10 minutes max per symbol
) -> Optional[pd.DataFrame]:
    """Fetch historical funding rates from Coinglass API with retry logic.
    
    Maximum 3 attempts total. After 3 attempts, moves on to next symbol.
    For unrecoverable errors (not found, invalid), fails immediately.
    """
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
        params["startTime"] = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
    if end_date:
        params["endTime"] = int(datetime.combine(end_date, datetime.max.time()).timestamp() * 1000)
    
    max_total_attempts = 3  # Hard limit: max 3 attempts total
    attempt = 0
    
    while attempt < max_total_attempts:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Handle rate limits - retry with exponential backoff (up to max attempts)
            if response.status_code == 429:
                attempt += 1
                if attempt >= max_total_attempts:
                    print(f"  [SKIP] {symbol}: Rate limit after {max_total_attempts} attempts")
                    return None
                wait_time = max(60.0, min(300.0, retry_delay * (2 ** min(attempt - 1, 6))))  # Cap at 5 minutes
                print(f"  [RATE LIMIT 429] {symbol}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                sleep(wait_time)
                continue
            
            if data.get("code") != "0":
                error_msg = data.get("msg", "Unknown error")
                
                # Rate limit errors - retry (up to max attempts)
                if "too many requests" in error_msg.lower() or "rate limit" in error_msg.lower():
                    attempt += 1
                    if attempt >= max_total_attempts:
                        print(f"  [SKIP] {symbol}: Rate limit after {max_total_attempts} attempts")
                        return None
                    wait_time = max(60.0, min(300.0, retry_delay * (2 ** min(attempt - 1, 6))))  # Cap at 5 minutes
                    print(f"  [RATE LIMIT] {symbol}: {error_msg}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                    sleep(wait_time)
                    continue
                
                # Unrecoverable errors - fail immediately
                if "not found" in error_msg.lower() or "invalid" in error_msg.lower():
                    print(f"  [SKIP] {symbol}: {error_msg}")
                    return None
                
                # Other errors - retry (up to max attempts)
                attempt += 1
                if attempt >= max_total_attempts:
                    print(f"  [SKIP] {symbol}: {error_msg} after {max_total_attempts} attempts")
                    return None
                wait_time = retry_delay * (2 ** min(attempt - 1, 4))
                print(f"  [RETRY] {symbol}: {error_msg}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                sleep(wait_time)
                continue
            
            # Success - parse and return data
            rows = []
            for item in data.get("data", []):
                time_ms = item.get("time")
                funding_rate_close = item.get("close")
                
                if time_ms is not None and funding_rate_close is not None:
                    dt_obj = datetime.fromtimestamp(time_ms / 1000)
                    rows.append({
                        "date": dt_obj.date(),
                        "symbol": symbol,
                        "exchange": exchange,
                        "funding_rate": float(funding_rate_close),
                        "source": "coinglass",
                    })
            
            if rows:
                return pd.DataFrame(rows)
            return None
            
        except requests.exceptions.Timeout:
            # Timeouts - retry (up to max attempts)
            attempt += 1
            if attempt >= max_total_attempts:
                print(f"  [SKIP] {symbol}: Timeout after {max_total_attempts} attempts")
                return None
            wait_time = min(300.0, retry_delay * (2 ** min(attempt - 1, 6)))
            print(f"  [TIMEOUT] {symbol}, attempt {attempt}/{max_total_attempts}, retrying in {wait_time:.1f}s...")
            sleep(wait_time)
            continue
        except Exception as e:
            # Other exceptions - retry (up to max attempts)
            attempt += 1
            if attempt >= max_total_attempts:
                print(f"  [SKIP] {symbol}: {e} after {max_total_attempts} attempts")
                return None
            wait_time = min(300.0, retry_delay * (2 ** min(attempt - 1, 6)))
            print(f"  [RETRY] {symbol}: {e}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
            sleep(wait_time)
            continue
    
    # If we've exhausted all attempts, return None
    print(f"  [SKIP] {symbol}: Exceeded {max_total_attempts} attempts")
    return None


def fetch_funding_rates_for_symbols(
    api_key: str,
    symbols: List[str],
    exchange: str = "Binance",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    rate_limit_per_min: int = 30,
    max_time_per_symbol: float = 600.0,  # 10 minutes max per symbol
    existing_data: Optional[pd.DataFrame] = None,  # Existing data to check against
) -> pd.DataFrame:
    """Fetch historical funding rates for multiple symbols with rate limiting.
    
    Per-symbol incremental logic:
    - If symbol doesn't exist in existing_data: fetch all data
    - If symbol exists but missing dates: fetch only missing dates
    - If symbol exists and has all dates up to end_date: skip symbol
    """
    all_rows = []
    delay_seconds = max(2.2, 60.0 / rate_limit_per_min + 0.2)
    
    print(f"  Fetching funding rates from {exchange}...")
    print(f"  Total symbols: {len(symbols)}")
    print(f"  Rate limit: {rate_limit_per_min} requests/min ({delay_seconds:.1f}s between requests)")
    print(f"  Max time per symbol: {max_time_per_symbol/60:.1f} minutes")
    if existing_data is not None and len(existing_data) > 0:
        print(f"  Existing data: {len(existing_data):,} records for {existing_data['asset_id'].nunique()} symbols")
    print()
    
    # Build symbol date lookup from existing data
    symbol_dates = {}
    if existing_data is not None and len(existing_data) > 0:
        for asset_id in existing_data["asset_id"].unique():
            asset_data = existing_data[existing_data["asset_id"] == asset_id]
            dates = set(pd.to_datetime(asset_data["date"]).dt.date)
            symbol_dates[asset_id] = dates
    
    successful = 0
    failed = 0
    skipped = 0
    start_time = time.time()
    
    # Determine end date (default to today if not provided)
    if end_date is None:
        end_date = date.today()
    
    for i, base_symbol in enumerate(symbols, 1):
        full_symbol = f"{base_symbol}USDT"
        symbol_start_time = time.time()
        
        try:
            # Check if symbol exists and what dates we have
            symbol_start = start_date
            needs_fetch = True
            
            if base_symbol in symbol_dates:
                existing_dates = symbol_dates[base_symbol]
                if existing_dates:
                    last_existing_date = max(existing_dates)
                    # Check if we have all dates up to end_date
                    if last_existing_date >= end_date:
                        # Symbol is up to date, skip it
                        skipped += 1
                        pct = (i / len(symbols)) * 100
                        print(f"  [{i}/{len(symbols)}] {base_symbol}... [SKIP] Already up to date (last date: {last_existing_date}) | {pct:.1f}%")
                        if i < len(symbols):
                            sleep(delay_seconds)
                        continue
                    else:
                        # Missing dates, fetch from day after last existing date
                        symbol_start = last_existing_date + timedelta(days=1)
                        print(f"  [{i}/{len(symbols)}] Fetching {base_symbol} (incremental from {symbol_start})...", end=" ", flush=True)
                else:
                    # Symbol exists but no dates, fetch all
                    print(f"  [{i}/{len(symbols)}] Fetching {base_symbol} (no dates found, fetching all)...", end=" ", flush=True)
            else:
                # Symbol doesn't exist, fetch all
                print(f"  [{i}/{len(symbols)}] Fetching {base_symbol} (new symbol, fetching all)...", end=" ", flush=True)
            
            df = fetch_funding_rate_history(
                api_key=api_key,
                symbol=full_symbol,
                exchange=exchange,
                start_date=symbol_start,
                end_date=end_date,
                interval="1d",
                max_retries=3,
                retry_delay=5.0,
                max_time_seconds=max_time_per_symbol,
            )
            
            if df is not None and len(df) > 0:
                df["symbol"] = base_symbol
                all_rows.append(df)
                successful += 1
                elapsed = time.time() - start_time
                avg_time_per_symbol = elapsed / i
                remaining_symbols = len(symbols) - i
                eta_seconds = avg_time_per_symbol * remaining_symbols
                total_rows = sum(len(df) for df in all_rows) if all_rows else 0
                pct = (i / len(symbols)) * 100
                symbol_time = time.time() - symbol_start_time
                print(f"✓ ({len(df)} records, {symbol_time:.1f}s) | {pct:.1f}% | ETA: {eta_seconds/60:.1f}m | Total: {total_rows:,} records")
            else:
                failed += 1
                elapsed = time.time() - start_time
                avg_time_per_symbol = elapsed / i
                remaining_symbols = len(symbols) - i
                eta_seconds = avg_time_per_symbol * remaining_symbols
                pct = (i / len(symbols)) * 100
                symbol_time = time.time() - symbol_start_time
                print(f"✗ (no data, {symbol_time:.1f}s) | {pct:.1f}% | ETA: {eta_seconds/60:.1f}m")
            
            if i < len(symbols):
                sleep(delay_seconds)
        except KeyboardInterrupt:
            print(f"\n  [INTERRUPTED] Stopping at symbol {i}/{len(symbols)}")
            raise
        except Exception as e:
            print(f"✗ ERROR: {e}")
            failed += 1
            if i < len(symbols):
                sleep(delay_seconds)
            continue
    
    total_time = time.time() - start_time
    
    if all_rows:
        combined_df = pd.concat(all_rows, ignore_index=True)
        print()
        print(f"  Completed: {successful} successful, {failed} failed, {skipped} skipped, {len(combined_df):,} total records")
        print(f"  Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
        return combined_df
    
    print()
    print(f"  Completed: {successful} successful, {failed} failed, {skipped} skipped, 0 total records")
    print(f"  Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
    return pd.DataFrame()


# ============================================================
# Open Interest Functions
# ============================================================

def fetch_oi_history(
    api_key: str,
    symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = "8h",
    max_retries: int = 3,
    retry_delay: float = 5.0,
    max_time_seconds: float = 600.0,  # 10 minutes max per symbol
) -> Optional[pd.DataFrame]:
    """Fetch historical Open Interest from Coinglass API with retry logic.
    
    Maximum 3 attempts total. After 3 attempts, moves on to next symbol.
    For unrecoverable errors (not found, invalid), fails immediately.
    """
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
        params["startTime"] = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
    if end_date:
        params["endTime"] = int(datetime.combine(end_date, datetime.max.time()).timestamp() * 1000)
    
    max_total_attempts = 3  # Hard limit: max 3 attempts total
    attempt = 0
    
    while attempt < max_total_attempts:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Handle rate limits - retry with exponential backoff (up to max attempts)
            if response.status_code == 429:
                attempt += 1
                if attempt >= max_total_attempts:
                    print(f"  [SKIP] {symbol}: Rate limit after {max_total_attempts} attempts")
                    return None
                wait_time = max(60.0, min(300.0, retry_delay * (2 ** min(attempt - 1, 6))))  # Cap at 5 minutes
                print(f"  [RATE LIMIT 429] {symbol}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                sleep(wait_time)
                continue
            
            if data.get("code") != "0":
                error_msg = data.get("msg", "Unknown error")
                
                # Rate limit errors - retry (up to max attempts)
                if "too many requests" in error_msg.lower() or "rate limit" in error_msg.lower():
                    attempt += 1
                    if attempt >= max_total_attempts:
                        print(f"  [SKIP] {symbol}: Rate limit after {max_total_attempts} attempts")
                        return None
                    wait_time = max(60.0, min(300.0, retry_delay * (2 ** min(attempt - 1, 6))))  # Cap at 5 minutes
                    print(f"  [RATE LIMIT] {symbol}: {error_msg}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                    sleep(wait_time)
                    continue
                
                # Unrecoverable errors - fail immediately
                if "not found" in error_msg.lower() or "invalid" in error_msg.lower():
                    print(f"  [SKIP] {symbol}: {error_msg}")
                    return None
                
                # Other errors - retry (up to max attempts)
                attempt += 1
                if attempt >= max_total_attempts:
                    print(f"  [SKIP] {symbol}: {error_msg} after {max_total_attempts} attempts")
                    return None
                wait_time = retry_delay * (2 ** min(attempt - 1, 4))
                print(f"  [RETRY] {symbol}: {error_msg}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                sleep(wait_time)
                continue
            
            # Success - parse and return data
            per_day_closes: Dict[date, List[float]] = {}
            for item in data.get("data", []):
                time_ms = item.get("time")
                close_val = item.get("close")
                
                if time_ms is not None and close_val is not None:
                    dt_obj = datetime.fromtimestamp(time_ms / 1000)
                    d = dt_obj.date()
                    per_day_closes.setdefault(d, []).append(float(close_val))
            
            rows = []
            for d, closes in per_day_closes.items():
                daily_oi = closes[-1] if closes else None
                if daily_oi is not None:
                    rows.append({
                        "date": d,
                        "asset_id": symbol.upper(),
                        "open_interest_usd": float(daily_oi),
                        "source": "coinglass",
                    })
            
            if rows:
                return pd.DataFrame(rows)
            return None
            
        except requests.exceptions.Timeout:
            # Timeouts - retry (up to max attempts)
            attempt += 1
            if attempt >= max_total_attempts:
                print(f"  [SKIP] {symbol}: Timeout after {max_total_attempts} attempts")
                return None
            wait_time = min(300.0, retry_delay * (2 ** min(attempt - 1, 6)))
            print(f"  [TIMEOUT] {symbol}, attempt {attempt}/{max_total_attempts}, retrying in {wait_time:.1f}s...")
            sleep(wait_time)
            continue
        except Exception as e:
            # Other exceptions - retry (up to max attempts)
            attempt += 1
            if attempt >= max_total_attempts:
                print(f"  [SKIP] {symbol}: {e} after {max_total_attempts} attempts")
                return None
            wait_time = min(300.0, retry_delay * (2 ** min(attempt - 1, 6)))
            print(f"  [RETRY] {symbol}: {e}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
            sleep(wait_time)
            continue
    
    # If we've exhausted all attempts, return None
    print(f"  [SKIP] {symbol}: Exceeded {max_total_attempts} attempts")
    return None


def fetch_oi_for_symbols(
    api_key: str,
    symbols: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    rate_limit_per_min: int = 30,
    existing_data: Optional[pd.DataFrame] = None,  # Existing data to check against
) -> pd.DataFrame:
    """Fetch historical OI for multiple symbols with rate limiting.
    
    Per-symbol incremental logic:
    - If symbol doesn't exist in existing_data: fetch all data
    - If symbol exists but missing dates: fetch only missing dates
    - If symbol exists and has all dates up to end_date: skip symbol
    """
    all_rows = []
    delay_seconds = max(2.2, 60.0 / rate_limit_per_min + 0.2)
    
    print(f"  Fetching OI data from CoinGlass...")
    print(f"  Total symbols: {len(symbols)}")
    print(f"  Rate limit: {rate_limit_per_min} requests/min ({delay_seconds:.1f}s between requests)")
    if existing_data is not None and len(existing_data) > 0:
        print(f"  Existing data: {len(existing_data):,} records for {existing_data['asset_id'].nunique()} symbols")
    print()
    
    # Build symbol date lookup from existing data
    symbol_dates = {}
    if existing_data is not None and len(existing_data) > 0:
        for asset_id in existing_data["asset_id"].unique():
            asset_data = existing_data[existing_data["asset_id"] == asset_id]
            dates = set(pd.to_datetime(asset_data["date"]).dt.date)
            symbol_dates[asset_id] = dates
    
    successful = 0
    failed = 0
    skipped = 0
    start_time = time.time()
    
    # Determine end date (default to today if not provided)
    if end_date is None:
        end_date = date.today()
    
    for i, symbol in enumerate(symbols, 1):
        try:
            # Check if symbol exists and what dates we have
            symbol_start = start_date
            
            if symbol.upper() in symbol_dates:
                existing_dates = symbol_dates[symbol.upper()]
                if existing_dates:
                    last_existing_date = max(existing_dates)
                    # Check if we have all dates up to end_date
                    if last_existing_date >= end_date:
                        # Symbol is up to date, skip it
                        skipped += 1
                        pct = (i / len(symbols)) * 100
                        print(f"  [{i}/{len(symbols)}] {symbol}... [SKIP] Already up to date (last date: {last_existing_date}) | {pct:.1f}%")
                        if i < len(symbols):
                            sleep(delay_seconds)
                        continue
                    else:
                        # Missing dates, fetch from day after last existing date
                        symbol_start = last_existing_date + timedelta(days=1)
                        print(f"  [{i}/{len(symbols)}] Fetching {symbol} (incremental from {symbol_start})...", end=" ", flush=True)
                else:
                    # Symbol exists but no dates, fetch all
                    print(f"  [{i}/{len(symbols)}] Fetching {symbol} (no dates found, fetching all)...", end=" ", flush=True)
            else:
                # Symbol doesn't exist, fetch all
                print(f"  [{i}/{len(symbols)}] Fetching {symbol} (new symbol, fetching all)...", end=" ", flush=True)
            
            df = fetch_oi_history(
                api_key=api_key,
                symbol=symbol,
                start_date=symbol_start,
                end_date=end_date,
                interval="8h",
                max_retries=3,
                retry_delay=5.0,
                max_time_seconds=600.0,  # 10 minutes max per symbol
            )
            
            if df is not None and len(df) > 0:
                all_rows.append(df)
                successful += 1
                elapsed = time.time() - start_time
                avg_time_per_symbol = elapsed / i
                remaining_symbols = len(symbols) - i
                eta_seconds = avg_time_per_symbol * remaining_symbols
                total_rows = sum(len(df) for df in all_rows) if all_rows else 0
                pct = (i / len(symbols)) * 100
                symbol_time = time.time() - start_time
                print(f"✓ ({len(df)} records) | {pct:.1f}% | ETA: {eta_seconds/60:.1f}m | Total: {total_rows:,} records")
            else:
                failed += 1
                elapsed = time.time() - start_time
                avg_time_per_symbol = elapsed / i
                remaining_symbols = len(symbols) - i
                eta_seconds = avg_time_per_symbol * remaining_symbols
                pct = (i / len(symbols)) * 100
                symbol_time = time.time() - start_time
                print(f"✗ (no data) | {pct:.1f}% | ETA: {eta_seconds/60:.1f}m")
            
            if i < len(symbols):
                sleep(delay_seconds)
        except Exception as e:
            print(f"✗ ERROR: {e}")
            failed += 1
            if i < len(symbols):
                sleep(delay_seconds)
            continue
    
    total_time = time.time() - start_time
    
    if all_rows:
        combined_df = pd.concat(all_rows, ignore_index=True)
        print()
        print(f"  Completed: {successful} successful, {failed} failed, {skipped} skipped, {len(combined_df):,} total records")
        print(f"  Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
        return combined_df
    
    print()
    print(f"  Completed: {successful} successful, {failed} failed, {skipped} skipped, 0 total records")
    print(f"  Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
    return pd.DataFrame()


# ============================================================
# Main Function
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fetch Coinglass funding rates and/or Open Interest data and save to data lake format",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Coinglass API key (optional if COINGLASS_API_KEY env var is set)",
    )
    parser.add_argument(
        "--fetch-funding",
        action="store_true",
        help="Fetch funding rates",
    )
    parser.add_argument(
        "--fetch-oi",
        action="store_true",
        help="Fetch Open Interest data",
    )
    parser.add_argument(
        "--funding-output",
        type=Path,
        default=Path("data/curated/data_lake/fact_funding.parquet"),
        help="Output path for funding rates parquet file",
    )
    parser.add_argument(
        "--oi-output",
        type=Path,
        default=Path("data/curated/data_lake/fact_open_interest.parquet"),
        help="Output path for OI parquet file",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="List of symbols to fetch (default: auto-detect for funding, BTC only for OI)",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="Binance",
        help="Exchange name for funding (default: Binance)",
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
        help="API rate limit (requests per minute, default: 30)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: only fetch dates after last date in existing file",
    )
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="Merge fetched data with existing parquet (recommended for max-range backfills)",
    )
    
    args = parser.parse_args()

    # Resolve API key (prefer env var to avoid leaking key in shell history)
    if not args.api_key:
        args.api_key = os.environ.get("COINGLASS_API_KEY")
    if not args.api_key:
        raise SystemExit(
            "Missing Coinglass API key. Provide --api-key or set COINGLASS_API_KEY environment variable."
        )
    
    # If neither flag is set, fetch both
    if not args.fetch_funding and not args.fetch_oi:
        args.fetch_funding = True
        args.fetch_oi = True
    
    repo_root = Path(__file__).parent.parent
    
    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    
    # Try to load from config if dates not provided
    if start_date is None or end_date is None:
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
    
    print("=" * 70)
    print("FETCHING COINGLASS DATA")
    print("=" * 70)
    print(f"  API key: {args.api_key[:8]}...")
    print(f"  Fetch funding: {args.fetch_funding}")
    print(f"  Fetch OI: {args.fetch_oi}")
    print(f"  Date range: {start_date or 'all available'} to {end_date or 'all available'}")
    print(f"  Incremental: {args.incremental}")
    print(f"  Merge existing: {args.merge_existing}")
    print()
    
    # ============================================================
    # Fetch Funding Rates
    # ============================================================
    if args.fetch_funding:
        print("=" * 70)
        print("FETCHING FUNDING RATES")
        print("=" * 70)
        
        funding_output_path = (repo_root / args.funding_output).resolve() if not args.funding_output.is_absolute() else args.funding_output
        funding_output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get symbols
        symbols = args.symbols
        if symbols is None:
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
                    symbols = [normalize_symbol(s) for s in perp_df["symbol"].unique() if s]
                    print(f"  Loaded {len(symbols)} symbols from Binance perp listings")
                else:
                    print("  [ERROR] No symbols provided and no data sources found")
                    symbols = []
        
        if symbols:
            # Load existing data for per-symbol incremental checking
            existing_funding = None
            if (args.incremental or args.merge_existing) and funding_output_path.exists():
                try:
                    existing_funding = pd.read_parquet(funding_output_path)
                    if len(existing_funding) > 0:
                        # Ensure asset_id column exists (might be symbol in old format)
                        if "asset_id" not in existing_funding.columns and "symbol" in existing_funding.columns:
                            existing_funding["asset_id"] = existing_funding["symbol"]
                        mode = "INCREMENTAL" if args.incremental else "MERGE"
                        print(f"  [{mode}] Loaded {len(existing_funding):,} existing records for {existing_funding['asset_id'].nunique()} symbols")
                except Exception as e:
                    print(f"  [WARN] Could not load existing funding data: {e}")
            
            funding_df = fetch_funding_rates_for_symbols(
                api_key=args.api_key,
                symbols=symbols,
                exchange=args.exchange,
                start_date=start_date,
                end_date=end_date,
                rate_limit_per_min=args.rate_limit,
                existing_data=existing_funding,
            )
            
            if funding_df.empty:
                funding_df = pd.DataFrame(columns=[
                    "asset_id", "instrument_id", "date", "funding_rate", "exchange", "source"
                ])
            else:
                funding_df["asset_id"] = funding_df["symbol"]
                
                # Map to instrument_id
                instrument_lookup = {}
                dim_instrument_path = funding_output_path.parent / "dim_instrument.parquet"
                if dim_instrument_path.exists():
                    try:
                        dim_instrument = pd.read_parquet(dim_instrument_path)
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
                
                funding_df["instrument_id"] = funding_df["symbol"].map(instrument_lookup)
                funding_df = funding_df.drop_duplicates(subset=["date", "symbol", "exchange"])
                funding_df = funding_df.sort_values(["date", "symbol"])
                funding_df = funding_df[["asset_id", "instrument_id", "date", "funding_rate", "exchange", "source"]]
            
            # Append/merge with existing if requested
            if (args.incremental or args.merge_existing) and funding_output_path.exists():
                try:
                    existing_funding = pd.read_parquet(funding_output_path)
                    # Union + dedupe (prefer existing for duplicates to keep stable history)
                    combined = pd.concat([existing_funding, funding_df], ignore_index=True)
                    before = len(combined)
                    combined = combined.drop_duplicates(subset=["asset_id", "date", "exchange"], keep="first")
                    funding_df = combined.sort_values(["date", "asset_id", "exchange"])
                    mode = "INCREMENTAL" if args.incremental else "MERGE"
                    print(f"  [{mode}] Merged funding: {len(existing_funding):,} existing + {before - len(existing_funding):,} fetched -> {len(funding_df):,} after dedupe")
                except Exception as e:
                    print(f"  [WARN] Could not load existing funding data: {e}")
            
            funding_df.to_parquet(funding_output_path, index=False)
            if len(funding_df) > 0:
                print(f"\n[SUCCESS] Saved {len(funding_df):,} funding rate records to {funding_output_path}")
                print(f"  Date range: {funding_df['date'].min()} to {funding_df['date'].max()}")
                print(f"  Assets: {funding_df['asset_id'].nunique()}")
            else:
                print(f"\n[WARN] Created empty fact_funding structure")
        
        print()
    
    # ============================================================
    # Fetch Open Interest
    # ============================================================
    if args.fetch_oi:
        print("=" * 70)
        print("FETCHING OPEN INTEREST")
        print("=" * 70)
        
        oi_output_path = (repo_root / args.oi_output).resolve() if not args.oi_output.is_absolute() else args.oi_output
        oi_output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get symbols (default: BTC only for OI)
        oi_symbols = args.symbols if args.symbols else ["BTC"]
        print(f"  Fetching OI for {len(oi_symbols)} symbols: {oi_symbols}")
        
        # Load existing data for per-symbol incremental checking / merge
        existing_oi = None
        if (args.incremental or args.merge_existing) and oi_output_path.exists():
            try:
                existing_oi = pd.read_parquet(oi_output_path)
                if len(existing_oi) > 0:
                    # Ensure asset_id column exists
                    if "asset_id" not in existing_oi.columns:
                        if "symbol" in existing_oi.columns:
                            existing_oi["asset_id"] = existing_oi["symbol"]
                    mode = "INCREMENTAL" if args.incremental else "MERGE"
                    print(f"  [{mode}] Loaded {len(existing_oi):,} existing records for {existing_oi['asset_id'].nunique()} symbols")
            except Exception as e:
                print(f"  [WARN] Could not load existing OI data: {e}")
        
        oi_df = fetch_oi_for_symbols(
            api_key=args.api_key,
            symbols=oi_symbols,
            start_date=start_date,
            end_date=end_date,
            rate_limit_per_min=args.rate_limit,
            existing_data=existing_oi,
        )
        
        if oi_df.empty:
            oi_df = pd.DataFrame(columns=[
                "asset_id", "date", "open_interest_usd", "source"
            ])
        else:
            oi_df = oi_df.drop_duplicates(subset=["date", "asset_id"])
            oi_df = oi_df.sort_values(["date", "asset_id"])
            oi_df = oi_df[["asset_id", "date", "open_interest_usd", "source"]]
        
        # Append/merge with existing if requested
        if (args.incremental or args.merge_existing) and oi_output_path.exists():
            try:
                existing_oi = pd.read_parquet(oi_output_path)
                combined = pd.concat([existing_oi, oi_df], ignore_index=True)
                before = len(combined)
                combined = combined.drop_duplicates(subset=["asset_id", "date"], keep="first")
                oi_df = combined.sort_values(["date", "asset_id"])
                mode = "INCREMENTAL" if args.incremental else "MERGE"
                print(f"  [{mode}] Merged OI: {len(existing_oi):,} existing + {before - len(existing_oi):,} fetched -> {len(oi_df):,} after dedupe")
            except Exception as e:
                print(f"  [WARN] Could not load existing OI data: {e}")
        
        oi_df.to_parquet(oi_output_path, index=False)
        if len(oi_df) > 0:
            print(f"\n[SUCCESS] Saved {len(oi_df):,} OI records to {oi_output_path}")
            print(f"  Date range: {oi_df['date'].min()} to {oi_df['date'].max()}")
            print(f"  Assets: {oi_df['asset_id'].nunique()}")
        else:
            print(f"\n[WARN] Created empty fact_open_interest structure")
    
    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
