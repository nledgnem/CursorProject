#!/usr/bin/env python3
"""
Fetch derivative data (futures/perpetuals) from CoinGecko.

This provides backup data for Funding Skew and OI Risk features in MSM v0,
complementing CoinGlass data.
"""

import os
import sys
from pathlib import Path
from datetime import date, datetime, timezone
from typing import Dict, List, Optional
import polars as pl
import requests
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")  # Set in env; never commit keys


def fetch_derivatives_list(sleep_seconds: float = 0.12) -> List[Dict]:
    """Fetch list of all derivatives."""
    url = f"{COINGECKO_BASE}/derivatives"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch derivatives list: {e}")
        return []


def fetch_derivatives_exchanges(sleep_seconds: float = 0.12) -> List[Dict]:
    """Fetch list of derivative exchanges."""
    url = f"{COINGECKO_BASE}/derivatives/exchanges"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch derivative exchanges: {e}")
        return []


def fetch_exchange_derivatives(exchange_id: str, sleep_seconds: float = 0.12) -> List[Dict]:
    """Fetch derivatives for a specific exchange."""
    url = f"{COINGECKO_BASE}/derivatives/exchanges/{exchange_id}"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch derivatives for {exchange_id}: {e}")
        return []


def save_derivative_volumes(data_lake_dir: Path):
    """Fetch and save derivative volume data."""
    print("=" * 80)
    print("FETCHING DERIVATIVE VOLUMES")
    print("=" * 80)
    
    derivatives = fetch_derivatives_list()
    
    if not derivatives:
        print("ERROR: No derivative data was downloaded.")
        return
    
    today = date.today()
    all_data = []
    
    for deriv in derivatives:
        # Market is a string (e.g., "Binance (Futures)")
        exchange = str(deriv.get("market", ""))
        
        # Extract base asset from index_id (e.g., "BTC") or symbol
        index_id = deriv.get("index_id", "")
        symbol = deriv.get("symbol", "")
        base_asset = index_id.upper() if index_id else ""
        
        # Extract target from symbol (e.g., "BTCUSDT" -> "USDT")
        target = ""
        if symbol and len(symbol) > 3:
            # Try to extract quote currency from symbol
            if symbol.endswith("USDT"):
                target = "USDT"
            elif symbol.endswith("USD"):
                target = "USD"
            elif symbol.endswith("BTC"):
                target = "BTC"
        
        # Volume and OI are direct floats, not dicts
        volume_24h = deriv.get("volume_24h", 0.0)
        volume_usd = float(volume_24h) if volume_24h else 0.0
        
        open_interest = deriv.get("open_interest", 0.0)
        oi_usd = float(open_interest) if open_interest else 0.0
        
        # Extract funding rate
        funding_rate = float(deriv.get("funding_rate", 0.0)) if deriv.get("funding_rate") else 0.0
        
        if volume_usd > 0 or oi_usd > 0:
            all_data.append({
                "date": today,
                "exchange": exchange,
                "base_asset": base_asset,
                "target": target,
                "volume_usd": volume_usd,
                "open_interest_usd": oi_usd,
                "funding_rate": funding_rate,
                "source": "coingecko",
            })
    
    if len(all_data) == 0:
        print("ERROR: No derivative volume data was collected.")
        return
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "fact_derivative_volume.parquet"
    
    # Merge with existing data
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data if it exists
        existing = existing.filter(pl.col("date") != today)
        df_combined = pl.concat([df, existing])
    else:
        df_combined = df
    
    df_combined = df_combined.sort(["date", "exchange", "base_asset"])
    df_combined.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved {len(df_combined):,} derivative volume records to {output_path}")
    print(f"  Exchanges: {df_combined['exchange'].n_unique()}")
    print(f"  Assets: {df_combined['base_asset'].n_unique()}")


def save_derivative_open_interest(data_lake_dir: Path):
    """Fetch and save derivative open interest data."""
    print("=" * 80)
    print("FETCHING DERIVATIVE OPEN INTEREST")
    print("=" * 80)
    
    derivatives = fetch_derivatives_list()
    
    if not derivatives:
        print("ERROR: No derivative data was downloaded.")
        return
    
    today = date.today()
    all_data = []
    
    for deriv in derivatives:
        # Market is a string (e.g., "Binance (Futures)")
        exchange = str(deriv.get("market", ""))
        
        # Extract base asset from index_id (e.g., "BTC") or symbol
        index_id = deriv.get("index_id", "")
        symbol = deriv.get("symbol", "")
        base_asset = index_id.upper() if index_id else ""
        
        # Extract target from symbol (e.g., "BTCUSDT" -> "USDT")
        target = ""
        if symbol and len(symbol) > 3:
            if symbol.endswith("USDT"):
                target = "USDT"
            elif symbol.endswith("USD"):
                target = "USD"
            elif symbol.endswith("BTC"):
                target = "BTC"
        
        # OI is a direct float, not a dict
        open_interest = deriv.get("open_interest", 0.0)
        oi_usd = float(open_interest) if open_interest else 0.0
        # Convert USD to BTC (approximate, using current BTC price ~89000)
        oi_btc = oi_usd / 89000.0 if oi_usd > 0 else 0.0
        
        # Extract funding rate
        funding_rate = float(deriv.get("funding_rate", 0.0)) if deriv.get("funding_rate") else 0.0
        
        if oi_usd > 0:
            all_data.append({
                "date": today,
                "exchange": exchange,
                "base_asset": base_asset.upper() if base_asset else "",
                "target": target.upper() if target else "",
                "open_interest_usd": oi_usd,
                "open_interest_btc": oi_btc,
                "funding_rate": funding_rate,
                "source": "coingecko",
            })
    
    if len(all_data) == 0:
        print("ERROR: No open interest data was collected.")
        return
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "fact_derivative_open_interest.parquet"
    
    # Merge with existing data
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data if it exists
        existing = existing.filter(pl.col("date") != today)
        df_combined = pl.concat([df, existing])
    else:
        df_combined = df
    
    df_combined = df_combined.sort(["date", "exchange", "base_asset"])
    df_combined.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved {len(df_combined):,} open interest records to {output_path}")
    print(f"  Exchanges: {df_combined['exchange'].n_unique()}")
    print(f"  Assets: {df_combined['base_asset'].n_unique()}")


def save_derivative_exchanges(data_lake_dir: Path):
    """Fetch and save derivative exchange metadata."""
    print("=" * 80)
    print("FETCHING DERIVATIVE EXCHANGES")
    print("=" * 80)
    
    exchanges = fetch_derivatives_exchanges()
    
    if not exchanges:
        print("ERROR: No derivative exchange data was downloaded.")
        return
    
    all_data = []
    
    for exch in exchanges:
        all_data.append({
            "exchange_id": exch.get("id", ""),
            "exchange_name": exch.get("name", ""),
            "open_interest_btc": exch.get("open_interest_btc", 0.0),
            "trade_volume_24h_btc": exch.get("trade_volume_24h_btc", 0.0),
            "number_of_perpetual_pairs": exch.get("number_of_perpetual_pairs", 0),
            "number_of_futures_pairs": exch.get("number_of_futures_pairs", 0),
            "source": "coingecko",
        })
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "dim_derivative_exchanges.parquet"
    
    # Overwrite (dimension table, not time-series)
    df.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved {len(df)} derivative exchanges to {output_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch derivative data")
    parser.add_argument(
        "--volumes",
        action="store_true",
        help="Fetch derivative volumes"
    )
    parser.add_argument(
        "--open-interest",
        action="store_true",
        help="Fetch open interest data"
    )
    parser.add_argument(
        "--exchanges",
        action="store_true",
        help="Fetch derivative exchange metadata"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all derivative data"
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    
    if args.all:
        args.volumes = True
        args.open_interest = True
        args.exchanges = True
    
    if not any([args.volumes, args.open_interest, args.exchanges]):
        print("No data type specified. Use --all or specify individual types.")
        print("Available: --volumes, --open-interest, --exchanges")
        sys.exit(1)
    
    if args.exchanges:
        save_derivative_exchanges(data_lake_dir)
    
    if args.volumes:
        save_derivative_volumes(data_lake_dir)
    
    if args.open_interest:
        save_derivative_open_interest(data_lake_dir)
    
    print("\n" + "=" * 80)
    print("COMPLETE!")
    print("=" * 80)
