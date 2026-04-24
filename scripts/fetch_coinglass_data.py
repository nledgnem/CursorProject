#!/usr/bin/env python3
"""Fetch Coinglass funding rates, Open Interest (OI), and Liquidations data and save to data lake format."""

import sys
import argparse
import requests
import os
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
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

from repo_paths import data_lake_root

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


def _resolve_coinglass_symbol_universe(
    repo_root: Path,
    source: str = "auto",
    funding_output_path: Optional[Path] = None,
) -> List[str]:
    """Resolve the base-symbol universe for CoinGlass fetches (funding and OI).

    Returns a sorted list of base symbols (e.g. ["BTC", "ETH", ...]) without the
    "USDT" quote suffix. `source="auto"` tries, in order: dim_instrument ->
    existing fact_funding -> perp_listings_binance -> universe_eligibility.

    Strategy note (why funding AND OI both use this): we execute shorts on
    Binance perp, so symbols without a Binance perp listing are not tradable for
    this strategy and not worth CoinGlass API budget. Matching OI's universe to
    funding's is a *strategy choice*, not a CoinGlass API requirement --
    CoinGlass serves OI/funding for a much wider set. A future strategy on a
    different venue could legitimately want a different OI universe.
    """
    def _load_symbols_from_dim_instrument() -> list[str]:
        p = (data_lake_root() / "dim_instrument.parquet")
        if not p.exists():
            return []
        try:
            d = pd.read_parquet(p, columns=["venue", "instrument_type", "base_asset_symbol"])
            d = d[(d["venue"].astype(str).str.lower() == "binance") & (d["instrument_type"] == "perpetual")]
            out = sorted(set(d["base_asset_symbol"].astype(str).tolist()))
            return [s for s in out if s]
        except Exception:
            return []

    def _load_symbols_from_fact_funding() -> list[str]:
        if funding_output_path is None or not funding_output_path.exists():
            return []
        try:
            d = pd.read_parquet(funding_output_path, columns=["asset_id"])
            out = sorted(set(d["asset_id"].astype(str).tolist()))
            return [s for s in out if s]
        except Exception:
            return []

    def _load_symbols_from_universe() -> list[str]:
        universe_path = repo_root / "data" / "curated" / "universe_eligibility.parquet"
        basket_path = repo_root / "data" / "curated" / "universe_snapshots.parquet"
        eligible_symbols: set[str] = set()
        if universe_path.exists():
            try:
                universe_df = pd.read_parquet(universe_path)
                if "symbol" in universe_df.columns:
                    eligible_symbols.update(universe_df["symbol"].astype(str).unique())
            except Exception:
                pass
        if basket_path.exists():
            try:
                basket_df = pd.read_parquet(basket_path)
                if "symbol" in basket_df.columns:
                    eligible_symbols.update(basket_df["symbol"].astype(str).unique())
            except Exception:
                pass
        return sorted({s for s in eligible_symbols if s and s != "nan"})

    def _load_symbols_from_perp_listings() -> list[str]:
        perp_listings_path = None
        for default_path in [
            repo_root / "data" / "raw" / "perp_listings_binance.parquet",
            repo_root / "data" / "curated" / "perp_listings_binance.parquet",
            repo_root / "outputs" / "perp_listings_binance.parquet",
        ]:
            if default_path.exists():
                perp_listings_path = default_path
                break
        if not perp_listings_path:
            return []
        try:
            perp_df = pd.read_parquet(perp_listings_path)
            out = [normalize_symbol(s) for s in perp_df["symbol"].astype(str).unique().tolist() if s]
            return sorted(set(out))
        except Exception:
            return []

    if source == "dim_instrument":
        symbols = _load_symbols_from_dim_instrument()
        if symbols:
            print(f"  Loaded {len(symbols)} symbols from dim_instrument (Binance perpetuals)")
    elif source == "fact_funding":
        symbols = _load_symbols_from_fact_funding()
        if symbols:
            print(f"  Loaded {len(symbols)} symbols from existing fact_funding.parquet")
    elif source == "universe":
        symbols = _load_symbols_from_universe()
        if symbols:
            print(f"  Loaded {len(symbols)} symbols from universe eligibility/basket snapshots")
    elif source == "perp_listings":
        symbols = _load_symbols_from_perp_listings()
        if symbols:
            print(f"  Loaded {len(symbols)} symbols from Binance perp listings")
    else:
        # AUTO: dim_instrument -> fact_funding -> perp_listings -> universe
        symbols = _load_symbols_from_dim_instrument()
        if symbols:
            print(f"  Loaded {len(symbols)} symbols from dim_instrument (Binance perpetuals)")
        else:
            symbols = _load_symbols_from_fact_funding()
            if symbols:
                print(f"  Loaded {len(symbols)} symbols from existing fact_funding.parquet")
            else:
                symbols = _load_symbols_from_perp_listings()
                if symbols:
                    print(f"  Loaded {len(symbols)} symbols from Binance perp listings")
                else:
                    symbols = _load_symbols_from_universe()
                    if symbols:
                        print(f"  Loaded {len(symbols)} symbols from universe eligibility/basket snapshots")
                    else:
                        print("  [ERROR] No symbols provided and no data sources found")
                        symbols = []
    return symbols


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

            # ZERO-TRUST PATCH: Do NOT call raise_for_status() yet.
            # Parse the JSON first so we can evaluate 400/429 business logic.
            try:
                data = response.json()
            except ValueError:
                # If the response isn't JSON (e.g., 502 Bad Gateway HTML), raise the HTTP error
                response.raise_for_status()
                data = {}

            # 1. Handle Rate Limits explicitly
            if response.status_code == 429:
                attempt += 1
                wait_time = retry_delay * (2 ** (attempt - 1))
                print(f"  [RATE LIMIT] {symbol}: Sleeping {wait_time}s...")
                sleep(wait_time)
                continue

            # 2. Handle Fail-Fast (Invalid Pair) before any retries
            if response.status_code in [400, 404]:
                error_msg = data.get("msg", "").lower()
                if "does not exist" in error_msg or "not found" in error_msg or "supported exchange" in error_msg:
                    print(f"  [SKIP FAST] {symbol}: Pair invalid. Ejecting instantly.")
                    return None
                else:
                    # It's a 400 error we don't recognize, treat as generic API failure
                    response.raise_for_status()

            # 3. Handle 200 OK but API-level logical errors (CoinGlass custom codes)
            if data.get("code") != "0":
                error_msg = data.get("msg", "Unknown error").lower()
                if "too many requests" in error_msg or "rate limit" in error_msg:
                    attempt += 1
                    wait_time = retry_delay * (2 ** (attempt - 1))
                    sleep(wait_time)
                    continue
                if "not found" in error_msg or "invalid" in error_msg or "does not exist" in error_msg or "supported exchange" in error_msg:
                    print(f"  [SKIP FAST] {symbol}: Pair invalid. Ejecting instantly.")
                    return None

                # Fall through to retry for unknown logical errors
                raise requests.exceptions.HTTPError(f"API Code != 0: {error_msg}")

            # 4. Success - raise for any other unhandled 5xx errors just in case
            if response.status_code >= 500:
                response.raise_for_status()

            # Success - parse and return data
            rows = []
            for item in data.get("data", []):
                time_ms = item.get("time")
                funding_rate_close = item.get("close")
                
                if time_ms is not None and funding_rate_close is not None:
                    # ZERO-TRUST PATCH: Enforce strict UTC to prevent local server offset drift
                    dt_obj = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
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
                    # ZERO-TRUST PATCH: Enforce strict UTC to prevent local server offset drift
                    dt_obj = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
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
# Liquidations Functions
# ============================================================

# CoinGlass v4 liquidations-aggregated-history response keys seen in practice:
#   The live API returns `aggregated_long_liquidation_usd` / `aggregated_short_liquidation_usd`
#   (confirmed against `/futures/liquidation/aggregated-history` on 2026-04-23).
#   The snake/camel variants are kept as fallbacks in case CoinGlass changes shape.
_LIQ_LONG_KEY_CANDIDATES = ("aggregated_long_liquidation_usd", "long_liquidation_usd", "longLiquidationUsd")
_LIQ_SHORT_KEY_CANDIDATES = ("aggregated_short_liquidation_usd", "short_liquidation_usd", "shortLiquidationUsd")
_liq_field_names_logged = False

# Cross-venue default for the `exchange_list` param on the CoinGlass
# liquidations endpoint. These are the 10 major centralized perp venues
# CoinGlass tracks liquidations for. Chosen as default so fact_liquidations
# reflects market-wide liquidation pressure, not a single-venue slice.
#
# What this does NOT include: Hyperliquid and Variational. CoinGlass does
# not report liquidations for those venues in this feed, so strategies that
# execute on them (e.g. Apathy Bleed, which also trades Hyperliquid and
# Variational alongside Binance) will be reading centralized-venue
# liquidation pressure as a proxy -- not a complete execution-venue-matched
# signal. Document this in any downstream analysis.
#
# Override with --liquidations-exchange-list on the CLI if a different
# universe is needed (e.g. "Binance" alone for a Binance-specific slice).
_DEFAULT_LIQ_EXCHANGE_LIST = "Binance,OKX,Bybit,Bitget,HTX,Gate,MEXC,Bitmex,Deribit,Kraken"


def _pick_first_key(item: dict, candidates) -> Optional[str]:
    for k in candidates:
        if k in item:
            return k
    return None


def fetch_liquidation_history(
    api_key: str,
    symbol: str,
    exchange_list: str = _DEFAULT_LIQ_EXCHANGE_LIST,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = "1d",
    max_retries: int = 3,
    retry_delay: float = 5.0,
    max_time_seconds: float = 600.0,  # 10 minutes max per symbol
) -> Optional[pd.DataFrame]:
    """Fetch historical aggregated liquidations from Coinglass API with retry logic.

    Maximum 3 attempts total. After 3 attempts, moves on to next symbol.
    For unrecoverable errors (not found, invalid), fails immediately.

    Default `exchange_list` aggregates across 10 major centralized perp venues
    (see _DEFAULT_LIQ_EXCHANGE_LIST). Hyperliquid and Variational are NOT in
    the CoinGlass liquidations feed; callers whose strategies execute there
    should read this table as a cross-centralized-venue proxy rather than an
    execution-matched signal. Pass a custom exchange_list to narrow or widen.

    Units: long_liquidation_usd and short_liquidation_usd are USD flow quantities
    per time window (summed across the exchanges in exchange_list). We call with
    interval="1d" and expect one row per UTC day; if the endpoint returns
    sub-daily rows, the daily aggregation below SUMS them (liquidations are
    flows, not levels).
    """
    global _liq_field_names_logged

    url = f"{COINGLASS_API_BASE}/futures/liquidation/aggregated-history"

    headers = {
        "CG-API-KEY": api_key,
        "accept": "application/json",
    }

    params = {
        "symbol": symbol.upper(),
        "exchange_list": exchange_list,
        "interval": interval,
    }

    if start_date:
        params["start_time"] = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
    if end_date:
        params["end_time"] = int(datetime.combine(end_date, datetime.max.time()).timestamp() * 1000)

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
                wait_time = max(60.0, min(300.0, retry_delay * (2 ** min(attempt - 1, 6))))
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
                    wait_time = max(60.0, min(300.0, retry_delay * (2 ** min(attempt - 1, 6))))
                    print(f"  [RATE LIMIT] {symbol}: {error_msg}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
                    sleep(wait_time)
                    continue

                # Unrecoverable errors - fail immediately
                lower = error_msg.lower()
                if "not found" in lower or "invalid" in lower or "does not exist" in lower or "supported exchange" in lower:
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
            items = data.get("data", []) or []
            if not items:
                return None

            # One-time log of the actual response shape so Tier 1 can confirm field names
            if not _liq_field_names_logged:
                sample = items[0]
                long_key = _pick_first_key(sample, _LIQ_LONG_KEY_CANDIDATES)
                short_key = _pick_first_key(sample, _LIQ_SHORT_KEY_CANDIDATES)
                print(f"  [LIQ FIELDS] first response keys: {sorted(sample.keys())}")
                print(f"  [LIQ FIELDS] using long_key={long_key!r}, short_key={short_key!r}")
                _liq_field_names_logged = True

            # SUM-aggregate per UTC day (liquidations are flow quantities, not levels).
            # interval=1d should return pre-aggregated daily rows, but if sub-daily rows
            # come back we still produce one row per day by summing.
            per_day_long: Dict[date, float] = {}
            per_day_short: Dict[date, float] = {}
            for item in items:
                time_ms = item.get("time")
                if time_ms is None:
                    continue
                long_key = _pick_first_key(item, _LIQ_LONG_KEY_CANDIDATES)
                short_key = _pick_first_key(item, _LIQ_SHORT_KEY_CANDIDATES)
                long_val = item.get(long_key) if long_key else None
                short_val = item.get(short_key) if short_key else None
                if long_val is None and short_val is None:
                    continue
                dt_obj = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
                d = dt_obj.date()
                if long_val is not None:
                    per_day_long[d] = per_day_long.get(d, 0.0) + float(long_val)
                if short_val is not None:
                    per_day_short[d] = per_day_short.get(d, 0.0) + float(short_val)

            rows = []
            for d in sorted(set(per_day_long.keys()) | set(per_day_short.keys())):
                rows.append({
                    "date": d,
                    "asset_id": symbol.upper(),
                    "long_liquidation_usd": float(per_day_long.get(d, 0.0)),
                    "short_liquidation_usd": float(per_day_short.get(d, 0.0)),
                    "source": "coinglass",
                })

            if not rows:
                return None

            df = pd.DataFrame(rows)

            # Zero-pad trim: CoinGlass pads pre-listing / pre-coverage dates
            # with (0, 0) rows when the requested start_time is earlier than
            # the symbol's first real observation. We trim LEADING zeros only
            # -- internal zero-liquidation days (real quiet days on a listed
            # perp) are legitimate and must be preserved. "Trim until first
            # nonzero, then keep everything after."
            nonzero_mask = (df["long_liquidation_usd"] > 0) | (df["short_liquidation_usd"] > 0)
            if not nonzero_mask.any():
                # All rows are zero -- symbol has no real liquidation history
                # in the requested range. Treat as "no data" so the caller's
                # [SKIP] (no data) path handles it uniformly; do not emit a
                # synthetic-zero DataFrame into the lake.
                print(f"  [ZERO-PAD] {symbol}: all {len(df)} rows zero -- treating as no-data")
                return None

            first_nonzero_idx = int(nonzero_mask.values.argmax())
            trimmed_count = first_nonzero_idx  # default RangeIndex, position == label
            if trimmed_count > 0:
                first_real_date = df.iloc[first_nonzero_idx]["date"]
                print(f"  [ZERO-PAD] {symbol}: {trimmed_count} leading zero-pad rows trimmed (first real observation: {first_real_date})")
                df = df.iloc[first_nonzero_idx:].reset_index(drop=True)
            else:
                print(f"  [ZERO-PAD] {symbol}: 0 leading zero-pad rows trimmed")

            return df

        except requests.exceptions.Timeout:
            attempt += 1
            if attempt >= max_total_attempts:
                print(f"  [SKIP] {symbol}: Timeout after {max_total_attempts} attempts")
                return None
            wait_time = min(300.0, retry_delay * (2 ** min(attempt - 1, 6)))
            print(f"  [TIMEOUT] {symbol}, attempt {attempt}/{max_total_attempts}, retrying in {wait_time:.1f}s...")
            sleep(wait_time)
            continue
        except Exception as e:
            attempt += 1
            if attempt >= max_total_attempts:
                print(f"  [SKIP] {symbol}: {e} after {max_total_attempts} attempts")
                return None
            wait_time = min(300.0, retry_delay * (2 ** min(attempt - 1, 6)))
            print(f"  [RETRY] {symbol}: {e}, attempt {attempt}/{max_total_attempts}, waiting {wait_time:.1f}s...")
            sleep(wait_time)
            continue

    print(f"  [SKIP] {symbol}: Exceeded {max_total_attempts} attempts")
    return None


def fetch_liquidations_for_symbols(
    api_key: str,
    symbols: List[str],
    exchange_list: str = _DEFAULT_LIQ_EXCHANGE_LIST,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    rate_limit_per_min: int = 30,
    existing_data: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Fetch historical liquidations for multiple symbols with rate limiting.

    Per-symbol incremental logic:
    - If symbol doesn't exist in existing_data: fetch all data
    - If symbol exists but missing dates: fetch only missing dates
    - If symbol exists and has all dates up to end_date: skip symbol
    """
    all_rows = []
    delay_seconds = max(2.2, 60.0 / rate_limit_per_min + 0.2)

    print(f"  Fetching liquidations data from CoinGlass...")
    print(f"  Exchange list: {exchange_list}")
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

    if end_date is None:
        end_date = date.today()

    for i, symbol in enumerate(symbols, 1):
        try:
            symbol_start = start_date

            if symbol.upper() in symbol_dates:
                existing_dates = symbol_dates[symbol.upper()]
                if existing_dates:
                    last_existing_date = max(existing_dates)
                    if last_existing_date >= end_date:
                        skipped += 1
                        pct = (i / len(symbols)) * 100
                        print(f"  [{i}/{len(symbols)}] {symbol}... [SKIP] Already up to date (last date: {last_existing_date}) | {pct:.1f}%")
                        if i < len(symbols):
                            sleep(delay_seconds)
                        continue
                    else:
                        symbol_start = last_existing_date + timedelta(days=1)
                        print(f"  [{i}/{len(symbols)}] Fetching {symbol} (incremental from {symbol_start})...", end=" ", flush=True)
                else:
                    print(f"  [{i}/{len(symbols)}] Fetching {symbol} (no dates found, fetching all)...", end=" ", flush=True)
            else:
                print(f"  [{i}/{len(symbols)}] Fetching {symbol} (new symbol, fetching all)...", end=" ", flush=True)

            df = fetch_liquidation_history(
                api_key=api_key,
                symbol=symbol,
                exchange_list=exchange_list,
                start_date=symbol_start,
                end_date=end_date,
                interval="1d",
                max_retries=3,
                retry_delay=5.0,
                max_time_seconds=600.0,
            )

            if df is not None and len(df) > 0:
                all_rows.append(df)
                successful += 1
                elapsed = time.time() - start_time
                avg_time_per_symbol = elapsed / i
                remaining_symbols = len(symbols) - i
                eta_seconds = avg_time_per_symbol * remaining_symbols
                total_rows = sum(len(d) for d in all_rows) if all_rows else 0
                pct = (i / len(symbols)) * 100
                print(f"✓ ({len(df)} records) | {pct:.1f}% | ETA: {eta_seconds/60:.1f}m | Total: {total_rows:,} records")
            else:
                failed += 1
                elapsed = time.time() - start_time
                avg_time_per_symbol = elapsed / i
                remaining_symbols = len(symbols) - i
                eta_seconds = avg_time_per_symbol * remaining_symbols
                pct = (i / len(symbols)) * 100
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
        description="Fetch Coinglass funding rates, Open Interest, and/or Liquidations data and save to data lake format",
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
        "--fetch-liquidations",
        action="store_true",
        help="Fetch aggregated liquidations (long/short USD per day)",
    )
    parser.add_argument(
        "--funding-output",
        type=Path,
        default=(data_lake_root() / "fact_funding.parquet"),
        help="Output path for funding rates parquet file",
    )
    parser.add_argument(
        "--oi-output",
        type=Path,
        default=(data_lake_root() / "fact_open_interest.parquet"),
        help="Output path for OI parquet file",
    )
    parser.add_argument(
        "--liquidations-output",
        type=Path,
        default=(data_lake_root() / "fact_liquidations.parquet"),
        help="Output path for liquidations parquet file",
    )
    parser.add_argument(
        "--liquidations-exchange-list",
        type=str,
        default=_DEFAULT_LIQ_EXCHANGE_LIST,
        help=(
            "Comma-separated exchange list for the CoinGlass liquidations endpoint. "
            "Default is 10-venue cross-aggregation across major centralized perps "
            "(Binance, OKX, Bybit, Bitget, HTX, Gate, MEXC, Bitmex, Deribit, Kraken). "
            "Hyperliquid and Variational are NOT in the CoinGlass liquidations feed. "
            "Override with a narrower list (e.g. 'Binance') for a single-venue slice."
        ),
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="List of symbols to fetch (default: auto-detect Binance-perp universe for both funding and OI)",
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
        help="Start date for historical data (YYYY-MM-DD). Default: end-date minus 730 days.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for historical data (YYYY-MM-DD). Default: today (UTC).",
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
    parser.add_argument(
        "--symbols-source",
        type=str,
        default="auto",
        choices=["auto", "dim_instrument", "fact_funding", "universe", "perp_listings"],
        help="Where to auto-detect symbols from when --symbols is not provided (default: auto). Applies to both funding and OI branches.",
    )
    parser.add_argument(
        "--liquidity-gate",
        action="store_true",
        help="(Optional) Apply Top-N liquid perpetual filter when auto-detecting funding symbols. NOT applied to OI (flagged for follow-up).",
    )
    
    args = parser.parse_args()

    # Resolve API key (prefer env var to avoid leaking key in shell history)
    if not args.api_key:
        args.api_key = os.environ.get("COINGLASS_API_KEY")
    if not args.api_key:
        raise SystemExit(
            "Missing Coinglass API key. Provide --api-key or set COINGLASS_API_KEY environment variable."
        )
    
    # If no fetch flag is set, fetch all three
    if not args.fetch_funding and not args.fetch_oi and not args.fetch_liquidations:
        args.fetch_funding = True
        args.fetch_oi = True
        args.fetch_liquidations = True
    
    repo_root = Path(__file__).parent.parent
    
    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)

    # Defaults: recent 2 years through today (UTC) when not provided on CLI.
    # Deliberately does NOT read from configs/golden.yaml -- that config drives
    # the backtest strategy window, not ingestion. Letting a strategy config
    # cap ingestion caused the 2025-12-31 truncation bug discovered 2026-04-23.
    # If backfill-specific defaults are needed, a separate ingestion config is
    # the right place for them.
    if end_date is None:
        end_date = datetime.now(timezone.utc).date()
    if start_date is None:
        start_date = end_date - timedelta(days=730)
    
    print("=" * 70)
    print("FETCHING COINGLASS DATA")
    print("=" * 70)
    print(f"  API key: {args.api_key[:8]}...")
    print(f"  Fetch funding: {args.fetch_funding}")
    print(f"  Fetch OI: {args.fetch_oi}")
    print(f"  Fetch liquidations: {args.fetch_liquidations}")
    print(f"  Date range: {start_date or 'all available'} to {end_date or 'all available'}")
    print(f"  Incremental: {args.incremental}")
    print(f"  Merge existing: {args.merge_existing}")
    print()

    # Resolve funding_output_path unconditionally so the OI branch can use it as
    # a "known-valid perps" signal when auto-resolving its own universe.
    funding_output_path = (repo_root / args.funding_output).resolve() if not args.funding_output.is_absolute() else args.funding_output

    # ============================================================
    # Fetch Funding Rates
    # ============================================================
    if args.fetch_funding:
        print("=" * 70)
        print("FETCHING FUNDING RATES")
        print("=" * 70)

        funding_output_path.parent.mkdir(parents=True, exist_ok=True)

        # Get symbols
        symbols = args.symbols
        if symbols is None:
            symbols = _resolve_coinglass_symbol_universe(
                repo_root=repo_root,
                source=args.symbols_source,
                funding_output_path=funding_output_path,
            )

            if args.liquidity_gate:
                # LIQUIDITY GATE: Filter for valid perpetuals FIRST (or universe), then Top-N by market cap (avoid spot-only denominator trap)
                _top_n_fallback = 200
                try:
                    data_lake_dir = data_lake_root()
                    mcap_path = data_lake_dir / "fact_marketcap.parquet"
                    if mcap_path.exists():
                        mcap_df = pd.read_parquet(mcap_path)
                        if (
                            not mcap_df.empty
                            and "date" in mcap_df.columns
                            and "marketcap" in mcap_df.columns
                            and "asset_id" in mcap_df.columns
                        ):
                            max_date = mcap_df["date"].max()
                            latest = mcap_df[mcap_df["date"] == max_date].copy()
                            latest = latest.sort_values("marketcap", ascending=False)

                            # ZERO-TRUST PATCH: Filter for known valid symbols FIRST, then take Top N
                            symbols_set = set(s.upper() for s in symbols)
                            # Prefer symbols we already have funding for (valid perpetuals) when available
                            if funding_output_path.exists():
                                try:
                                    existing = pd.read_parquet(funding_output_path)
                                    col = "asset_id" if "asset_id" in existing.columns else "symbol"
                                    if len(existing) > 0 and col in existing.columns:
                                        valid_perps = set(existing[col].astype(str).str.upper().tolist())
                                        if len(valid_perps) >= 30:
                                            symbols_set = valid_perps
                                except Exception:
                                    pass
                            # 1. Get all market cap assets that exist in our valid symbols list
                            valid_mcap_assets = latest[
                                latest["asset_id"].astype(str).str.upper().isin(symbols_set)
                            ]
                            # 2. NOW take the Top 150 of that filtered list
                            _top_n_perps = 150
                            top_perps = set(
                                valid_mcap_assets.head(_top_n_perps)["asset_id"]
                                .astype(str)
                                .str.upper()
                                .tolist()
                            )
                            # 3. Preserve original casing from the current symbols list (universe)
                            symbols = sorted([s for s in symbols if s.upper() in top_perps])
                            print(
                                f"  [LIQUIDITY GATE] Reduced universe to {len(symbols)} Top-{_top_n_perps} Liquid Perpetuals (max_date={max_date})."
                            )
                        else:
                            symbols = symbols[:_top_n_fallback]
                            print(
                                f"  [LIQUIDITY GATE] fact_marketcap schema missing columns; applied fail-safe slice to {len(symbols)} symbols."
                            )
                    else:
                        symbols = symbols[:_top_n_fallback]
                        print(
                            f"  [LIQUIDITY GATE] fact_marketcap.parquet not found; applied fail-safe slice to {len(symbols)} symbols."
                        )
                except Exception as e:
                    symbols = symbols[:_top_n_fallback]
                    print(f"  [LIQUIDITY GATE] Fallback after error ({e}); applied fail-safe slice to {len(symbols)} symbols.")
            else:
                print(f"  [NO LIQUIDITY GATE] Using full symbol universe: {len(symbols)} symbols")
        
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
            
            # ZERO-TRUST PATCH: Coerce all mixed temporal objects into uniform datetime.date for PyArrow
            funding_df["date"] = pd.to_datetime(funding_df["date"], utc=True).dt.date

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

        # Get symbols. Default: auto-resolve the same Binance-perp universe that
        # funding uses (~510 altcoins + BTC). Previously defaulted to ["BTC"] only;
        # altcoin OI is now covered. --liquidity-gate is intentionally NOT honored
        # for OI on this pass -- flagged as a follow-up decision.
        oi_symbols = args.symbols
        if oi_symbols is None:
            oi_symbols = _resolve_coinglass_symbol_universe(
                repo_root=repo_root,
                source=args.symbols_source,
                funding_output_path=funding_output_path,
            )
        if len(oi_symbols) <= 5:
            print(f"  Fetching OI for {len(oi_symbols)} symbols: {oi_symbols}")
        else:
            print(f"  Fetching OI for {len(oi_symbols)} symbols (first 5: {oi_symbols[:5]})")
        
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

    # ============================================================
    # Fetch Liquidations
    # ============================================================
    if args.fetch_liquidations:
        print("=" * 70)
        print("FETCHING LIQUIDATIONS")
        print("=" * 70)

        liq_output_path = (repo_root / args.liquidations_output).resolve() if not args.liquidations_output.is_absolute() else args.liquidations_output
        liq_output_path.parent.mkdir(parents=True, exist_ok=True)

        # NOTE: default --liquidations-exchange-list is cross-venue (10 major
        # centralized perps; see _DEFAULT_LIQ_EXCHANGE_LIST). Earlier drafts of
        # this branch defaulted to Binance-only on the (incorrect) premise that
        # strategies are Binance-only. Apathy Bleed actually executes across
        # Hyperliquid + Binance + Variational, and Hyperliquid/Variational do
        # not report liquidations to CoinGlass at all -- so single-venue is the
        # wrong framing either way. Cross-venue aggregation of the centralized
        # venues CoinGlass DOES cover is the closest thing to market-wide
        # liquidation pressure available from this feed.
        #
        # Symbol universe still comes from the Binance-perp set (dim_instrument
        # -> fact_funding fallbacks) because that's the coin universe we trade
        # and research; it does not restrict which exchanges CoinGlass
        # aggregates liquidations across for each symbol.
        liq_symbols = args.symbols
        if liq_symbols is None:
            liq_symbols = _resolve_coinglass_symbol_universe(
                repo_root=repo_root,
                source=args.symbols_source,
                funding_output_path=funding_output_path,
            )
        if len(liq_symbols) <= 5:
            print(f"  Fetching liquidations for {len(liq_symbols)} symbols: {liq_symbols}")
        else:
            print(f"  Fetching liquidations for {len(liq_symbols)} symbols (first 5: {liq_symbols[:5]})")

        # Load existing data for per-symbol incremental checking / merge
        existing_liq = None
        if (args.incremental or args.merge_existing) and liq_output_path.exists():
            try:
                existing_liq = pd.read_parquet(liq_output_path)
                if len(existing_liq) > 0:
                    if "asset_id" not in existing_liq.columns and "symbol" in existing_liq.columns:
                        existing_liq["asset_id"] = existing_liq["symbol"]
                    mode = "INCREMENTAL" if args.incremental else "MERGE"
                    print(f"  [{mode}] Loaded {len(existing_liq):,} existing records for {existing_liq['asset_id'].nunique()} symbols")
            except Exception as e:
                print(f"  [WARN] Could not load existing liquidations data: {e}")

        liq_df = fetch_liquidations_for_symbols(
            api_key=args.api_key,
            symbols=liq_symbols,
            exchange_list=args.liquidations_exchange_list,
            start_date=start_date,
            end_date=end_date,
            rate_limit_per_min=args.rate_limit,
            existing_data=existing_liq,
        )

        if liq_df.empty:
            liq_df = pd.DataFrame(columns=[
                "asset_id", "date", "long_liquidation_usd", "short_liquidation_usd", "source"
            ])
        else:
            liq_df = liq_df.drop_duplicates(subset=["asset_id", "date"])
            liq_df = liq_df.sort_values(["date", "asset_id"])
            liq_df = liq_df[["asset_id", "date", "long_liquidation_usd", "short_liquidation_usd", "source"]]

        # Append/merge with existing if requested
        if (args.incremental or args.merge_existing) and liq_output_path.exists():
            try:
                existing_liq = pd.read_parquet(liq_output_path)
                combined = pd.concat([existing_liq, liq_df], ignore_index=True)
                before = len(combined)
                combined = combined.drop_duplicates(subset=["asset_id", "date"], keep="first")
                liq_df = combined.sort_values(["date", "asset_id"])
                mode = "INCREMENTAL" if args.incremental else "MERGE"
                print(f"  [{mode}] Merged liquidations: {len(existing_liq):,} existing + {before - len(existing_liq):,} fetched -> {len(liq_df):,} after dedupe")
            except Exception as e:
                print(f"  [WARN] Could not load existing liquidations data: {e}")

        liq_df.to_parquet(liq_output_path, index=False)
        if len(liq_df) > 0:
            print(f"\n[SUCCESS] Saved {len(liq_df):,} liquidation records to {liq_output_path}")
            print(f"  Date range: {liq_df['date'].min()} to {liq_df['date'].max()}")
            print(f"  Assets: {liq_df['asset_id'].nunique()}")
        else:
            print(f"\n[WARN] Created empty fact_liquidations structure")

    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
