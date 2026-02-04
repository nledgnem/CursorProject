#!/usr/bin/env python3
"""
Fetch Analyst tier exclusive data from CoinGecko and save to data lake.

This script fetches:
1. OHLC data (Open, High, Low, Close) - Historical backfill
2. Top Gainers/Losers - Market breadth data
3. New Listings - Recently added coins
4. Exchange Volume - Exchange-level volume data
"""

import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Optional
import polars as pl
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko_analyst import (
    fetch_ohlc_range,
    fetch_top_gainers_losers,
    fetch_new_listings,
    fetch_exchange_volume_chart,
    check_api_usage,
)
from src.data_lake.mapping import generate_asset_id


def safe_print(text: str) -> None:
    """Print text safely, handling Unicode encoding errors on Windows."""
    try:
        print(text, end="", flush=True)
    except UnicodeEncodeError:
        # Replace problematic characters with ASCII equivalents
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text, end="", flush=True)


def fetch_ohlc_backfill(
    data_lake_dir: Path,
    allowlist_path: Path,
    start_date: Optional[date] = None,
    max_assets: Optional[int] = None,
):
    """Backfill historical OHLC data for all assets."""
    print("=" * 80)
    print("OHLC HISTORICAL BACKFILL")
    print("=" * 80)
    
    # If start_date not provided, use earliest date from price data
    if start_date is None:
        price_path = data_lake_dir / "fact_price.parquet"
        if price_path.exists():
            existing_prices = pl.read_parquet(str(price_path))
            if len(existing_prices) > 0:
                earliest_price_date = existing_prices.select(pl.col("date").min()).item()
                start_date = earliest_price_date
                print(f"Using earliest price date as start: {start_date}")
            else:
                start_date = date(2013, 4, 28)  # BTC launch date
        else:
            start_date = date(2013, 4, 28)  # BTC launch date
    
    # Determine end date - use latest date from price data or today
    price_path = data_lake_dir / "fact_price.parquet"
    if price_path.exists():
        existing_prices = pl.read_parquet(str(price_path))
        if len(existing_prices) > 0:
            latest_price_date = existing_prices.select(pl.col("date").max()).item()
            end_date = latest_price_date
            print(f"Price data range: {start_date} to {latest_price_date}")
        else:
            end_date = date.today()
    else:
        end_date = date.today()
    
    # Check if OHLC already exists and what date range it covers
    ohlc_path = data_lake_dir / "fact_ohlc.parquet"
    if ohlc_path.exists():
        existing_ohlc = pl.read_parquet(str(ohlc_path))
        if len(existing_ohlc) > 0:
            ohlc_earliest = existing_ohlc.select(pl.col("date").min()).item()
            ohlc_latest = existing_ohlc.select(pl.col("date").max()).item()
            print(f"Existing OHLC data: {ohlc_earliest} to {ohlc_latest}")
            
            # Only backfill if we need earlier dates or later dates
            if start_date >= ohlc_earliest and end_date <= ohlc_latest:
                print("No backfill needed - existing OHLC data already covers requested range.")
                return
            elif start_date < ohlc_earliest:
                # Need to backfill earlier dates
                end_date = min(end_date, ohlc_earliest - timedelta(days=1))
                print(f"Will backfill earlier dates: {start_date} to {end_date}")
            elif end_date > ohlc_latest:
                # Need to backfill later dates
                start_date = max(start_date, ohlc_latest + timedelta(days=1))
                print(f"Will backfill later dates: {start_date} to {end_date}")
    
    if end_date < start_date:
        print("No backfill needed - date range is invalid.")
        return
    
    print(f"Will backfill OHLC from {start_date} to {end_date}")
    
    # Load allowlist
    allowlist_df = pd.read_csv(allowlist_path)
    if max_assets:
        allowlist_df = allowlist_df.head(max_assets)
    
    # Load dim_asset for symbol->asset_id mapping
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    symbol_to_asset_id = {}
    if dim_asset_path.exists():
        dim_asset = pl.read_parquet(str(dim_asset_path))
        for row in dim_asset.to_dicts():
            symbol = str(row.get("symbol", "")).upper()
            asset_id = row.get("asset_id")
            if symbol and asset_id:
                symbol_to_asset_id[symbol] = asset_id
    
    total_coins = len(allowlist_df)
    print(f"\nFetching OHLC for {total_coins} coins from {start_date} to {end_date}...")
    
    all_ohlc = []
    successful = 0
    failed = 0
    
    for idx, row in allowlist_df.iterrows():
        symbol = str(row["symbol"]).upper()
        cg_id = str(row["coingecko_id"]).lower().strip()
        
        asset_id = symbol_to_asset_id.get(symbol) or generate_asset_id(symbol=symbol)
        
        progress_pct = (idx + 1) / total_coins * 100
        safe_print(f"[{idx+1}/{total_coins}] ({progress_pct:.1f}%) {symbol} ({cg_id})... ")
        
        try:
            ohlc_data = fetch_ohlc_range(cg_id, start_date, end_date)
            
            if ohlc_data and len(ohlc_data) > 0:
                for d, open_price, high_price, low_price, close_price in ohlc_data:
                    all_ohlc.append({
                        "asset_id": asset_id,
                        "date": d,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "source": "coingecko",
                    })
                successful += 1
                safe_print(f"[OK] {len(ohlc_data)} days | Success: {successful}, Failed: {failed}\n")
            else:
                failed += 1
                safe_print(f"[SKIP] No data | Success: {successful}, Failed: {failed}\n")
        except Exception as e:
            failed += 1
            error_msg = str(e)[:50] if len(str(e)) > 50 else str(e)
            safe_print(f"[ERROR] {error_msg} | Success: {successful}, Failed: {failed}\n")
    
    if len(all_ohlc) == 0:
        print("\nERROR: No OHLC data was downloaded.")
        return
    
    # Convert to DataFrame
    ohlc_new = pl.DataFrame(all_ohlc)
    print(f"\nCreated {len(ohlc_new):,} OHLC records")
    
    # Merge with existing data
    if ohlc_path.exists():
        existing_ohlc = pl.read_parquet(str(ohlc_path))
        ohlc_combined = pl.concat([ohlc_new, existing_ohlc]).unique(subset=["asset_id", "date"])
        print(f"Combined: {len(ohlc_combined):,} records (removed duplicates)")
    else:
        ohlc_combined = ohlc_new
    
    # Sort and save
    ohlc_combined = ohlc_combined.sort(["asset_id", "date"])
    ohlc_combined.write_parquet(str(ohlc_path))
    
    print(f"\n[SUCCESS] Saved {len(ohlc_combined):,} OHLC records to {ohlc_path}")


def fetch_market_breadth(
    data_lake_dir: Path,
    durations: list = ["24h", "7d", "14d", "30d"],
):
    """Fetch top gainers/losers data."""
    print("=" * 80)
    print("FETCHING MARKET BREADTH (Top Gainers/Losers)")
    print("=" * 80)
    
    # Load dim_asset for symbol->asset_id mapping
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    symbol_to_asset_id = {}
    if dim_asset_path.exists():
        dim_asset = pl.read_parquet(str(dim_asset_path))
        for row in dim_asset.to_dicts():
            symbol = str(row.get("symbol", "")).upper()
            asset_id = row.get("asset_id")
            if symbol and asset_id:
                symbol_to_asset_id[symbol] = asset_id
    
    all_data = []
    today = date.today()
    
    for duration in durations:
        print(f"\nFetching {duration} gainers/losers...")
        result = fetch_top_gainers_losers(duration=duration)
        
        # Process gainers
        gainers = result.get("gainers", [])
        for idx, coin in enumerate(gainers):
            symbol = str(coin.get("symbol", "")).upper()
            asset_id = symbol_to_asset_id.get(symbol) or generate_asset_id(symbol=symbol)
            
            # API returns usd_24h_change for 24h duration
            price_change_24h = coin.get("usd_24h_change") if duration == "24h" else None
            
            all_data.append({
                "date": today,
                "asset_id": asset_id,
                "rank": idx + 1,  # Rank is position in list
                "price_change_24h": price_change_24h,
                "price_change_7d": None,  # Not available in this endpoint
                "price_change_14d": None,
                "price_change_30d": None,
                "category": "gainer",
                "duration": duration,
                "source": "coingecko",
            })
        
        # Process losers
        losers = result.get("losers", [])
        for idx, coin in enumerate(losers):
            symbol = str(coin.get("symbol", "")).upper()
            asset_id = symbol_to_asset_id.get(symbol) or generate_asset_id(symbol=symbol)
            
            # API returns usd_24h_change for 24h duration
            price_change_24h = coin.get("usd_24h_change") if duration == "24h" else None
            
            all_data.append({
                "date": today,
                "asset_id": asset_id,
                "rank": idx + 1,  # Rank is position in list
                "price_change_24h": price_change_24h,
                "price_change_7d": None,
                "price_change_14d": None,
                "price_change_30d": None,
                "category": "loser",
                "duration": duration,
                "source": "coingecko",
            })
    
    if len(all_data) == 0:
        print("\nERROR: No market breadth data was downloaded.")
        return
    
    # Convert to DataFrame and save
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "fact_market_breadth.parquet"
    
    # Merge with existing data
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data if it exists
        existing = existing.filter(pl.col("date") != today)
        df_combined = pl.concat([df, existing])
    else:
        df_combined = df
    
    df_combined = df_combined.sort(["date", "duration", "category", "rank"])
    df_combined.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved {len(df_combined):,} market breadth records to {output_path}")


def fetch_new_listings_data(data_lake_dir: Path):
    """Fetch newly listed coins."""
    print("=" * 80)
    print("FETCHING NEW LISTINGS")
    print("=" * 80)
    
    listings = fetch_new_listings()
    
    if not listings:
        print("ERROR: No new listings data was downloaded.")
        return
    
    all_data = []
    today = date.today()
    
    for coin in listings:
        symbol = str(coin.get("symbol", "")).upper()
        asset_id = generate_asset_id(symbol=symbol)
        
        all_data.append({
            "asset_id": asset_id,
            "symbol": symbol,
            "name": coin.get("name", ""),
            "listing_date": today,  # Approximate - CoinGecko doesn't provide exact date
            "coingecko_id": coin.get("id", ""),
            "source": "coingecko",
        })
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "dim_new_listings.parquet"
    
    # Merge with existing data (deduplicate by coingecko_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Only add new coins not in existing data
        existing_ids = set(existing["coingecko_id"].to_list())
        df_new = df.filter(~pl.col("coingecko_id").is_in(list(existing_ids)))
        if len(df_new) > 0:
            df_combined = pl.concat([existing, df_new])
        else:
            df_combined = existing
            print(f"  No new listings (all {len(df)} already in database)")
    else:
        df_combined = df
    
    df_combined = df_combined.sort(["listing_date", "symbol"])
    df_combined.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved {len(df_combined):,} new listings to {output_path}")


def fetch_exchange_volumes(
    data_lake_dir: Path,
    exchange_ids: list = ["binance", "coinbase", "kraken", "kucoin", "okx", "bybit", "gate", "bitget", "mexc", "bitfinex"],
    days: int = 90,
):
    """Fetch exchange volume data."""
    print("=" * 80)
    print("FETCHING EXCHANGE VOLUMES")
    print("=" * 80)
    
    all_data = []
    
    for exchange_id in exchange_ids:
        print(f"\nFetching {exchange_id}...", end="", flush=True)
        volume_data = fetch_exchange_volume_chart(exchange_id, days=days)
        
        if volume_data:
            for d, volume_btc, volume_usd in volume_data:
                all_data.append({
                    "exchange_id": exchange_id,
                    "date": d,
                    "volume_btc": volume_btc,
                    "volume_usd": volume_usd,
                    "source": "coingecko",
                })
            print(f" [OK] {len(volume_data)} days")
        else:
            print(" [SKIP] No data")
    
    if len(all_data) == 0:
        print("\nERROR: No exchange volume data was downloaded.")
        return
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "fact_exchange_volume.parquet"
    
    # Merge with existing data
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        df_combined = pl.concat([df, existing]).unique(subset=["exchange_id", "date"])
    else:
        df_combined = df
    
    df_combined = df_combined.sort(["exchange_id", "date"])
    df_combined.write_parquet(str(output_path))
    
    print(f"\n[SUCCESS] Saved {len(df_combined):,} exchange volume records to {output_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch Analyst tier exclusive data")
    parser.add_argument(
        "--ohlc",
        action="store_true",
        help="Fetch OHLC historical data"
    )
    parser.add_argument(
        "--market-breadth",
        action="store_true",
        help="Fetch top gainers/losers"
    )
    parser.add_argument(
        "--new-listings",
        action="store_true",
        help="Fetch new listings"
    )
    parser.add_argument(
        "--exchange-volumes",
        action="store_true",
        help="Fetch exchange volumes"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all data types"
    )
    parser.add_argument(
        "--max-assets",
        type=int,
        help="Limit number of assets for OHLC (for testing)"
    )
    parser.add_argument(
        "--check-usage",
        action="store_true",
        help="Check API usage and rate limits"
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    allowlist_path = repo_root / "data" / "perp_allowlist.csv"
    
    if args.check_usage:
        print("=" * 80)
        print("API USAGE CHECK")
        print("=" * 80)
        usage = check_api_usage()
        print(usage)
        sys.exit(0)
    
    if args.all:
        args.ohlc = True
        args.market_breadth = True
        args.new_listings = True
        args.exchange_volumes = True
    
    if not any([args.ohlc, args.market_breadth, args.new_listings, args.exchange_volumes]):
        print("No data type specified. Use --all or specify individual types.")
        print("Available: --ohlc, --market-breadth, --new-listings, --exchange-volumes")
        sys.exit(1)
    
    if args.ohlc:
        fetch_ohlc_backfill(data_lake_dir, allowlist_path, max_assets=args.max_assets)
    
    if args.market_breadth:
        fetch_market_breadth(data_lake_dir)
    
    if args.new_listings:
        fetch_new_listings_data(data_lake_dir)
    
    if args.exchange_volumes:
        fetch_exchange_volumes(data_lake_dir)
    
    print("\n" + "=" * 80)
    print("ALL FETCHES COMPLETE!")
    print("=" * 80)
