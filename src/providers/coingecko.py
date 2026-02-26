"""CoinGecko API provider for price, market cap, and volume data."""

import os
import time
import requests
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")  # Set in env; never commit keys


def to_utc_ts(d: date, offset_days: int = 0) -> int:
    """Convert date to UTC timestamp."""
    dt_obj = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=offset_days)
    return int(dt_obj.timestamp())


def fetch_price_history(
    coingecko_id: str,
    start_date: date,
    end_date: date,
    sleep_seconds: float = 0.12,  # 500 calls/min = 0.12s between calls (Analyst tier)
    max_retries: int = 5,
) -> Tuple[Dict[date, float], Dict[date, float], Dict[date, float]]:
    """
    Fetch daily prices, market caps, and volumes for a coin.
    
    Returns:
        Tuple of (prices, market_caps, volumes) as dict[date -> float]
    """
    url = f"{COINGECKO_BASE}/coins/{coingecko_id}/market_chart/range"
    
    start_ts = to_utc_ts(start_date, offset_days=-2)
    end_ts = to_utc_ts(end_date, offset_days=1)
    
    params = {
        "vs_currency": "usd",
        "from": start_ts,
        "to": end_ts,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    delay = sleep_seconds
    for attempt in range(1, max_retries + 1):
        try:
            # Disable proxy to avoid connection issues
            resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
            
            if resp.status_code == 200:
                data = resp.json()
                prices_data = data.get("prices", [])
                market_caps_data = data.get("market_caps", [])
                volumes_data = data.get("total_volumes", [])
                
                # Convert to daily dicts (use last price point per day)
                prices: Dict[date, float] = {}
                market_caps: Dict[date, float] = {}
                volumes: Dict[date, float] = {}
                
                for ts_ms, price in prices_data:
                    d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                    prices[d] = float(price)
                
                for ts_ms, mcap in market_caps_data:
                    d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                    market_caps[d] = float(mcap)
                
                for ts_ms, vol in volumes_data:
                    d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                    volumes[d] = float(vol)
                
                time.sleep(sleep_seconds)
                return prices, market_caps, volumes
            
            elif resp.status_code == 404:
                print(f"[WARN] CoinGecko has no data for {coingecko_id} (404). Skipping.")
                return {}, {}, {}
            
            elif resp.status_code == 429:
                print(f"[WARN] Rate limited for {coingecko_id} (429). Backing off for {delay:.1f}s...")
                time.sleep(delay)
                delay *= 2.0
                continue
            
            elif resp.status_code == 401:
                print(f"[ERROR] Unauthorized (401) for {coingecko_id}. Check API key.")
                return {}, {}, {}
            
            else:
                print(f"[ERROR] CoinGecko error for {coingecko_id}: {resp.status_code} {resp.text[:200]}")
                time.sleep(sleep_seconds)
                return {}, {}, {}
                
        except Exception as e:
            print(f"[ERROR] Request error for {coingecko_id}: {e}")
            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2.0
            else:
                return {}, {}, {}
    
    return {}, {}, {}


def download_all_coins(
    allowlist_path: Path,
    start_date: date,
    end_date: date,
    output_dir: Path,
) -> None:
    """
    Download price, market cap, and volume data for all coins in allowlist.
    
    Saves to:
        - output_dir/prices_daily.parquet
        - output_dir/marketcap_daily.parquet
        - output_dir/volume_daily.parquet
    """
    print(f"Loading allowlist from {allowlist_path}")
    allowlist_df = pd.read_csv(allowlist_path)
    
    all_prices: Dict[str, Dict[date, float]] = {}
    all_mcaps: Dict[str, Dict[date, float]] = {}
    all_volumes: Dict[str, Dict[date, float]] = {}
    
    total_coins = len(allowlist_df)
    print(f"Downloading data for {total_coins} coins from {start_date} to {end_date}...")
    # Rate limit: 250 calls/min = ~0.25s per call
    estimated_minutes = (total_coins * 0.25) / 60.0
    print(f"Estimated time: ~{estimated_minutes:.1f} minutes (250 calls/min rate limit)\n")
    
    successful = 0
    failed = 0
    
    def safe_print(text: str) -> None:
        """Print text safely, handling Unicode encoding errors on Windows."""
        try:
            print(text, end="", flush=True)
        except UnicodeEncodeError:
            # Replace problematic characters with ASCII equivalents
            safe_text = text.encode('ascii', 'replace').decode('ascii')
            print(safe_text, end="", flush=True)
    
    for idx, row in allowlist_df.iterrows():
        symbol = row["symbol"]
        cg_id = row["coingecko_id"]
        
        progress_pct = (idx + 1) / total_coins * 100
        safe_print(f"[{idx+1}/{total_coins}] ({progress_pct:.1f}%) Fetching {symbol} ({cg_id})... ")
        
        prices, mcaps, vols = fetch_price_history(cg_id, start_date, end_date)
        
        if prices:
            all_prices[symbol] = prices
            all_mcaps[symbol] = mcaps
            all_volumes[symbol] = vols
            successful += 1
            safe_print(f"[OK] {len(prices)} days | Success: {successful}, Failed: {failed}\n")
        else:
            failed += 1
            safe_print(f"[SKIP] No data | Success: {successful}, Failed: {failed}\n")
        
        # Show summary every 50 coins
        if (idx + 1) % 50 == 0:
            print(f"\n--- Progress Summary: {idx+1}/{total_coins} ({progress_pct:.1f}%) | Success: {successful}, Failed: {failed} ---\n")
    
    # Convert to DataFrames
    print("\nConverting to DataFrames...")
    
    # Prices
    price_rows = []
    for symbol, price_dict in all_prices.items():
        for d, price in price_dict.items():
            price_rows.append({"date": d, "symbol": symbol, "price": price})
    prices_df = pd.DataFrame(price_rows)
    if not prices_df.empty:
        prices_df = prices_df.pivot(index="date", columns="symbol", values="price")
        prices_df.index = pd.to_datetime(prices_df.index)
        prices_df = prices_df.sort_index()
    
    # Market caps
    mcap_rows = []
    for symbol, mcap_dict in all_mcaps.items():
        for d, mcap in mcap_dict.items():
            mcap_rows.append({"date": d, "symbol": symbol, "marketcap": mcap})
    mcaps_df = pd.DataFrame(mcap_rows)
    if not mcaps_df.empty:
        mcaps_df = mcaps_df.pivot(index="date", columns="symbol", values="marketcap")
        mcaps_df.index = pd.to_datetime(mcaps_df.index)
        mcaps_df = mcaps_df.sort_index()
    
    # Volumes
    vol_rows = []
    for symbol, vol_dict in all_volumes.items():
        for d, vol in vol_dict.items():
            vol_rows.append({"date": d, "symbol": symbol, "volume": vol})
    volumes_df = pd.DataFrame(vol_rows)
    if not volumes_df.empty:
        volumes_df = volumes_df.pivot(index="date", columns="symbol", values="volume")
        volumes_df.index = pd.to_datetime(volumes_df.index)
        volumes_df = volumes_df.sort_index()
    
    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    
    prices_path = output_dir / "prices_daily.parquet"
    mcaps_path = output_dir / "marketcap_daily.parquet"
    volumes_path = output_dir / "volume_daily.parquet"
    
    print(f"\nSaving to {prices_path}...")
    prices_df.to_parquet(prices_path)
    
    print(f"Saving to {mcaps_path}...")
    mcaps_df.to_parquet(mcaps_path)
    
    print(f"Saving to {volumes_path}...")
    volumes_df.to_parquet(volumes_path)
    
    print(f"\n[SUCCESS] Download complete!")
    print(f"  Total processed: {total_coins} coins")
    print(f"  Successful: {successful} coins")
    print(f"  Failed/Skipped: {failed} coins")
    print(f"  Prices: {len(prices_df)} days, {len(prices_df.columns)} coins")
    print(f"  Market caps: {len(mcaps_df)} days, {len(mcaps_df.columns)} coins")
    print(f"  Volumes: {len(volumes_df)} days, {len(volumes_df.columns)} coins")

