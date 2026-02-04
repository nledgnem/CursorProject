#!/usr/bin/env python3
"""
Fetch high-priority CoinGecko data for MSM v0.

This script fetches:
1. Trending searches - Sentiment indicator
2. Coin categories - Sector analysis
3. All markets snapshot - Broader ALT Breadth coverage
4. Historical exchange volumes - Enhanced liquidity metrics
"""

import sys
from pathlib import Path
from datetime import date, datetime, timezone
from typing import Dict, List, Optional
import polars as pl
import pandas as pd
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko_analyst import (
    fetch_trending_searches,
    fetch_coins_categories,
    fetch_coins_markets,
    fetch_exchange_volume_chart_range,
    check_api_usage,
)
from src.data_lake.mapping import generate_asset_id

DATA_LAKE_DIR = Path("data/curated/data_lake")


def safe_print(text: str) -> None:
    """Print text safely, handling Unicode encoding errors on Windows."""
    try:
        print(text, end="", flush=True)
    except UnicodeEncodeError:
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text, end="", flush=True)


def fetch_and_save_trending_searches():
    """Fetch trending searches and save to data lake."""
    print("=" * 80)
    print("FETCHING TRENDING SEARCHES")
    print("=" * 80)
    
    data = fetch_trending_searches()
    if not data:
        print("[ERROR] No trending search data received")
        return
    
    today = date.today()
    all_records = []
    
    # Process coins
    coins = data.get("coins", [])
    for idx, coin in enumerate(coins[:30]):  # Top 30
        coin_data = coin.get("item", {})
        all_records.append({
            "date": today,
            "item_type": "coin",
            "item_id": coin_data.get("id", ""),
            "item_name": coin_data.get("name", ""),
            "item_symbol": coin_data.get("symbol", ""),
            "rank": idx + 1,
            "source": "coingecko",
        })
    
    # Process NFTs
    nfts = data.get("nfts", [])
    for idx, nft in enumerate(nfts[:30]):  # Top 30
        nft_data = nft.get("item", {})
        all_records.append({
            "date": today,
            "item_type": "nft",
            "item_id": nft_data.get("id", ""),
            "item_name": nft_data.get("name", ""),
            "item_symbol": None,
            "rank": idx + 1,
            "source": "coingecko",
        })
    
    # Process categories
    categories = data.get("categories", [])
    for idx, category in enumerate(categories[:30]):  # Top 30
        category_data = category.get("item", {})
        all_records.append({
            "date": today,
            "item_type": "category",
            "item_id": str(category_data.get("id", "")),
            "item_name": category_data.get("name", ""),
            "item_symbol": None,
            "rank": idx + 1,
            "source": "coingecko",
        })
    
    if not all_records:
        print("[ERROR] No trending search records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "fact_trending_searches.parquet"
    
    # Merge with existing data (deduplicate by date, item_type, item_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data if it exists
        existing = existing.filter(pl.col("date") != today)
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} trending search records to {output_path}")
    print()


def fetch_and_save_categories():
    """Fetch coin categories with market data and save to data lake."""
    print("=" * 80)
    print("FETCHING COIN CATEGORIES")
    print("=" * 80)
    
    categories = fetch_coins_categories()
    if not categories:
        print("[ERROR] No category data received")
        return
    
    today = date.today()
    all_records = []
    
    for cat in categories:
        top_3_coins = cat.get("top_3_coins", [])
        top_3_json = json.dumps(top_3_coins) if top_3_coins else None
        
        # Handle None values safely
        def safe_float(val, default=0.0):
            return float(val) if val is not None else default
        
        all_records.append({
            "date": today,
            "category_id": str(cat.get("category_id", "")),
            "category_name": cat.get("name", ""),
            "market_cap_usd": safe_float(cat.get("market_cap"), 0.0),
            "market_cap_btc": safe_float(cat.get("market_cap_btc"), 0.0),
            "volume_24h_usd": safe_float(cat.get("volume_24h"), 0.0),
            "volume_24h_btc": safe_float(cat.get("volume_24h_btc"), 0.0),
            "market_cap_change_24h": cat.get("market_cap_change_24h"),
            "top_3_coins": top_3_json,
            "source": "coingecko",
        })
    
    if not all_records:
        print("[ERROR] No category records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "fact_category_market.parquet"
    
    # Merge with existing data (deduplicate by date, category_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        existing = existing.filter(pl.col("date") != today)
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} category records to {output_path}")
    print()


def fetch_and_save_markets_snapshot(max_pages: int = 10):
    """Fetch all markets snapshot and save to data lake."""
    print("=" * 80)
    print("FETCHING ALL MARKETS SNAPSHOT")
    print("=" * 80)
    
    today = date.today()
    all_records = []
    
    # Load asset mapping for coingecko_id -> asset_id
    dim_asset_path = DATA_LAKE_DIR / "dim_asset.parquet"
    coingecko_to_asset_id = {}
    
    if dim_asset_path.exists():
        dim_asset = pl.read_parquet(str(dim_asset_path))
        for row in dim_asset.to_dicts():
            cg_id = row.get("coingecko_id")
            asset_id = row.get("asset_id")
            if cg_id and asset_id:
                coingecko_to_asset_id[cg_id.lower()] = asset_id
    
    # Fetch markets (paginated, 250 per page)
    total_fetched = 0
    for page in range(1, max_pages + 1):
        safe_print(f"Fetching page {page}... ")
        markets = fetch_coins_markets(
            vs_currency="usd",
            order="market_cap_desc",
            per_page=250,
            page=page,
            price_change_percentage="24h",
        )
        
        if not markets:
            safe_print("No more data\n")
            break
        
        for market in markets:
            cg_id = market.get("id", "")
            symbol = market.get("symbol", "").upper()
            
            # Get asset_id from mapping or generate
            asset_id = coingecko_to_asset_id.get(cg_id.lower())
            if not asset_id:
                asset_id = generate_asset_id(symbol=symbol)
            
            # Parse dates
            ath_date = None
            atl_date = None
            if market.get("ath_date"):
                try:
                    ath_date = datetime.fromisoformat(market["ath_date"].replace("Z", "+00:00")).date()
                except:
                    pass
            if market.get("atl_date"):
                try:
                    atl_date = datetime.fromisoformat(market["atl_date"].replace("Z", "+00:00")).date()
                except:
                    pass
            
            all_records.append({
                "date": today,
                "asset_id": asset_id,
                "coingecko_id": cg_id,
                "symbol": symbol,
                "name": market.get("name", ""),
                "current_price_usd": float(market.get("current_price", 0.0)),
                "market_cap_usd": market.get("market_cap"),
                "market_cap_rank": market.get("market_cap_rank"),
                "fully_diluted_valuation_usd": market.get("fully_diluted_valuation"),
                "total_volume_usd": market.get("total_volume"),
                "high_24h_usd": market.get("high_24h"),
                "low_24h_usd": market.get("low_24h"),
                "price_change_24h": market.get("price_change_24h"),
                "price_change_percentage_24h": market.get("price_change_percentage_24h"),
                "market_cap_change_24h": market.get("market_cap_change_24h"),
                "market_cap_change_percentage_24h": market.get("market_cap_change_percentage_24h"),
                "circulating_supply": market.get("circulating_supply"),
                "total_supply": market.get("total_supply"),
                "max_supply": market.get("max_supply"),
                "ath_usd": market.get("ath"),
                "ath_change_percentage": market.get("ath_change_percentage"),
                "ath_date": ath_date,
                "atl_usd": market.get("atl"),
                "atl_change_percentage": market.get("atl_change_percentage"),
                "atl_date": atl_date,
                "source": "coingecko",
            })
        
        total_fetched += len(markets)
        safe_print(f"{len(markets)} coins | Total: {total_fetched}\n")
        
        if len(markets) < 250:  # Last page
            break
    
    if not all_records:
        print("[ERROR] No market snapshot records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "fact_markets_snapshot.parquet"
    
    # Merge with existing data (deduplicate by date, asset_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        existing = existing.filter(pl.col("date") != today)
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} market snapshot records to {output_path}")
    print()


def fetch_and_save_exchange_volumes_history(exchange_ids: List[str], days: int = 90):
    """Fetch historical exchange volumes and save to data lake."""
    print("=" * 80)
    print("FETCHING HISTORICAL EXCHANGE VOLUMES")
    print("=" * 80)
    
    end_date = date.today()
    start_date = date(end_date.year, end_date.month, end_date.day) - pd.Timedelta(days=days)
    
    all_records = []
    
    for idx, exchange_id in enumerate(exchange_ids):
        safe_print(f"[{idx+1}/{len(exchange_ids)}] {exchange_id}... ")
        
        volume_data = fetch_exchange_volume_chart_range(exchange_id, start_date, end_date)
        
        if volume_data:
            for d, volume_btc, volume_usd in volume_data:
                all_records.append({
                    "date": d,
                    "exchange_id": exchange_id,
                    "volume_btc": volume_btc,
                    "volume_usd": volume_usd,
                    "source": "coingecko",
                })
            safe_print(f"[OK] {len(volume_data)} days\n")
        else:
            safe_print("[SKIP] No data\n")
    
    if not all_records:
        print("[ERROR] No exchange volume history records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "fact_exchange_volume_history.parquet"
    
    # Merge with existing data (deduplicate by date, exchange_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove overlapping dates
        existing = existing.filter(
            ~((pl.col("date") >= start_date) & (pl.col("date") <= end_date) & 
              pl.col("exchange_id").is_in(exchange_ids))
        )
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} exchange volume history records to {output_path}")
    print()


def main():
    """Main function to fetch all high-priority data."""
    print("=" * 80)
    print("HIGH-PRIORITY COINGECKO DATA FETCH")
    print("=" * 80)
    print()
    
    # Check API usage first
    usage = check_api_usage()
    if usage:
        print(f"API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        print()
    
    # 1. Trending searches (1 API call)
    fetch_and_save_trending_searches()
    
    # 2. Coin categories (1 API call)
    fetch_and_save_categories()
    
    # 3. All markets snapshot (multiple API calls, paginated)
    fetch_and_save_markets_snapshot(max_pages=10)  # Up to 2,500 coins
    
    # 4. Historical exchange volumes (multiple API calls)
    # Major exchanges
    major_exchanges = [
        "binance",
        "coinbase",
        "kraken",
        "bitfinex",
        "bitstamp",
        "gemini",
        "kucoin",
        "okx",
        "huobi",
        "bybit",
    ]
    fetch_and_save_exchange_volumes_history(major_exchanges, days=90)
    
    print("=" * 80)
    print("ALL HIGH-PRIORITY DATA FETCHES COMPLETE!")
    print("=" * 80)
    
    # Final API usage check
    usage = check_api_usage()
    if usage:
        print(f"\nFinal API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        remaining = usage.get('current_remaining_monthly_calls', 0)
        print(f"Remaining: {remaining:,} calls")


if __name__ == "__main__":
    main()
