#!/usr/bin/env python3
"""
Fetch global market data including BTC dominance for MSM v0.

This data directly feeds the BTC Dominance feature in the Market State Monitor.
"""

import os
import sys
from pathlib import Path
from datetime import date, datetime, timezone
from typing import Dict, List
import polars as pl
import requests
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")  # Set in env; never commit keys


def fetch_global_market(sleep_seconds: float = 0.12) -> Dict:
    """Fetch current global market data."""
    url = f"{COINGECKO_BASE}/global"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data.get("data", {})
    except Exception as e:
        print(f"[ERROR] Failed to fetch global market data: {e}")
        return {}


def fetch_global_market_cap_chart(days: int = 3650, sleep_seconds: float = 0.12, timeout: int = 120) -> List[tuple]:
    """
    Fetch historical global market cap chart data.
    
    Returns list of (date, market_cap_btc, market_cap_usd) tuples.
    Uses longer timeout to avoid failures on large range.
    """
    url = f"{COINGECKO_BASE}/global/market_cap_chart"
    
    params = {
        "days": days,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=timeout, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        chart_data = []
        # API returns: {"market_cap_chart": {"market_cap": [[ts_ms, value_usd], ...], "volume": [...]}}
        mcap_chart = data.get("market_cap_chart") or {}
        if isinstance(mcap_chart, dict):
            market_cap_series = mcap_chart.get("market_cap", [])
        else:
            # Legacy: list of [ts, mcap_btc, mcap_usd]
            market_cap_series = mcap_chart if isinstance(mcap_chart, list) else []
        
        for row in market_cap_series:
            if not row or len(row) < 2:
                continue
            try:
                ts_ms = int(row[0])
                # New format: [timestamp, value_usd]; legacy: [timestamp, mcap_btc, mcap_usd]
                if len(row) >= 3:
                    mcap_btc = float(row[1]) if row[1] is not None else 0.0
                    mcap_usd = float(row[2]) if row[2] is not None else 0.0
                else:
                    mcap_usd = float(row[1]) if row[1] is not None else 0.0
                    mcap_btc = 0.0
                d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                chart_data.append((d, mcap_btc, mcap_usd))
            except (TypeError, ValueError):
                continue
        
        time.sleep(sleep_seconds)
        return chart_data
    except Exception as e:
        print(f"[ERROR] Failed to fetch global market cap chart: {e}")
        return []


def save_global_market_data(data_lake_dir: Path):
    """Fetch and save current global market data."""
    print("=" * 80)
    print("FETCHING GLOBAL MARKET DATA")
    print("=" * 80)
    
    global_data = fetch_global_market()
    
    if not global_data:
        print("ERROR: No global market data was downloaded.")
        return
    
    today = date.today()
    
    # Extract key metrics
    market_data = global_data.get("market_data", {})
    total_market_cap = market_data.get("total_market_cap", {})
    total_volume = market_data.get("total_volume", {})
    
    # Calculate BTC dominance (if available)
    btc_dominance = global_data.get("market_cap_percentage", {}).get("btc", 0.0)
    
    # Create fact table row
    row = {
        "date": today,
        "total_market_cap_usd": total_market_cap.get("usd", 0.0),
        "total_market_cap_btc": total_market_cap.get("btc", 0.0),
        "total_volume_usd": total_volume.get("usd", 0.0),
        "total_volume_btc": total_volume.get("btc", 0.0),
        "btc_dominance": btc_dominance,
        "active_cryptocurrencies": global_data.get("active_cryptocurrencies", 0),
        "markets": global_data.get("markets", 0),
        "source": "coingecko",
    }
    
    df = pl.DataFrame([row])
    output_path = data_lake_dir / "fact_global_market.parquet"
    
    # Merge with existing data
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data if it exists
        existing = existing.filter(pl.col("date") != today)
        df_combined = pl.concat([df, existing])
    else:
        df_combined = df
    
    df_combined = df_combined.sort("date", descending=True)
    df_combined.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved global market data to {output_path}")
    print(f"  BTC Dominance: {btc_dominance:.2f}%")
    print(f"  Total Market Cap: ${total_market_cap.get('usd', 0):,.0f}")


def save_global_market_cap_history(data_lake_dir: Path, days: int = 3650, timeout: int = 120):
    """
    Fetch and save historical global market cap chart.
    Uses long timeout (120s). If full range fails, retries with 365 days to get at least 1 year.
    """
    print("=" * 80)
    print("FETCHING GLOBAL MARKET CAP HISTORY")
    print("=" * 80)
    print(f"Fetching {days} days of history (timeout={timeout}s)...")
    
    chart_data = fetch_global_market_cap_chart(days=days, timeout=timeout)
    
    if not chart_data and days > 365:
        print("Full range failed (timeout?), retrying with 365 days...")
        chart_data = fetch_global_market_cap_chart(days=365, timeout=90)
    
    if not chart_data:
        print("ERROR: No historical market cap data was downloaded.")
        return
    
    all_data = []
    for d, mcap_btc, mcap_usd in chart_data:
        all_data.append({
            "date": d,
            "market_cap_btc": mcap_btc,
            "market_cap_usd": mcap_usd,
            "source": "coingecko",
        })
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "fact_global_market_history.parquet"
    
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        df_combined = pl.concat([df, existing]).unique(subset=["date"]).sort("date")
    else:
        df_combined = df.sort("date")
    
    df_combined.write_parquet(str(output_path))
    print(f"\n[SUCCESS] Saved {len(df_combined):,} historical records to {output_path}")
    print(f"  Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch global market data")
    parser.add_argument(
        "--current",
        action="store_true",
        help="Fetch current global market data"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Fetch historical global market cap chart"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch both current and historical data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3650,
        help="Number of days for historical data (default: 3650 = 10 years)"
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    
    if args.all:
        args.current = True
        args.history = True
    
    if not any([args.current, args.history]):
        print("No data type specified. Use --all or specify --current and/or --history")
        sys.exit(1)
    
    if args.current:
        save_global_market_data(data_lake_dir)
    
    if args.history:
        save_global_market_cap_history(data_lake_dir, days=args.days)
    
    print("\n" + "=" * 80)
    print("COMPLETE!")
    print("=" * 80)
