#!/usr/bin/env python3
"""
Fetch low-priority CoinGecko data for MSM v0.

This script fetches:
1. Categories list - All coin categories (metadata)
2. Exchange details - Exchange details with tickers (for major exchanges)
3. Derivative exchanges list - All derivative exchanges (metadata)
"""

import sys
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko_analyst import (
    fetch_categories_list,
    fetch_exchange_details,
    fetch_derivatives_exchanges_list,
    check_api_usage,
)

DATA_LAKE_DIR = Path("data/curated/data_lake")


def safe_print(text: str) -> None:
    """Print text safely, handling Unicode encoding errors on Windows."""
    try:
        print(text, end="", flush=True)
    except UnicodeEncodeError:
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text, end="", flush=True)


def safe_str(val) -> Optional[str]:
    """Safely convert value to string, handling None."""
    return str(val) if val is not None else None


def safe_float(val, default=0.0) -> float:
    """Safely convert value to float, handling None."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def fetch_and_save_categories_list():
    """Fetch all categories list and save to data lake."""
    print("=" * 80)
    print("FETCHING CATEGORIES LIST")
    print("=" * 80)
    
    categories = fetch_categories_list()
    if not categories:
        print("[ERROR] No categories data received")
        return
    
    all_records = []
    
    for category in categories:
        all_records.append({
            "category_id": category.get("category_id", ""),
            "category_name": category.get("name", ""),
            "source": "coingecko",
        })
    
    if not all_records:
        print("[ERROR] No category records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "dim_categories.parquet"
    
    # Merge with existing data (deduplicate by category_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove categories that are being updated
        existing_ids = set(df["category_id"].unique().to_list())
        existing = existing.filter(~pl.col("category_id").is_in(existing_ids))
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} category records to {output_path}")
    print()


def fetch_and_save_exchange_tickers(exchange_ids: List[str]):
    """Fetch exchange details with tickers and save to data lake."""
    print("=" * 80)
    print("FETCHING EXCHANGE DETAILS WITH TICKERS")
    print("=" * 80)
    
    today = date.today()
    all_records = []
    
    for idx, exchange_id in enumerate(exchange_ids):
        safe_print(f"[{idx+1}/{len(exchange_ids)}] {exchange_id}... ")
        
        details = fetch_exchange_details(exchange_id)
        
        if details and "tickers" in details:
            tickers = details.get("tickers", [])
            
            for ticker in tickers:
                base = ticker.get("base", "")
                target = ticker.get("target", "")
                pair = ticker.get("pair", "")
                
                # Extract last price
                last_price = None
                if "last" in ticker:
                    last_price = safe_float(ticker.get("last"))
                elif "converted_last" in ticker:
                    last_price = safe_float(ticker.get("converted_last", {}).get("usd"))
                
                # Extract volume
                volume = None
                if "volume" in ticker:
                    volume = safe_float(ticker.get("volume"))
                elif "converted_volume" in ticker:
                    volume = safe_float(ticker.get("converted_volume", {}).get("usd"))
                
                # Extract bid-ask spread
                spread = None
                if "bid_ask_spread_percentage" in ticker:
                    spread = safe_float(ticker.get("bid_ask_spread_percentage"))
                
                all_records.append({
                    "date": today,
                    "exchange_id": exchange_id,
                    "ticker_base": base.upper() if base else "",
                    "ticker_target": target.upper() if target else "",
                    "ticker_pair": pair.upper() if pair else "",
                    "last_price_usd": last_price,
                    "volume_usd": volume,
                    "bid_ask_spread_percentage": spread,
                    "trust_score": safe_str(ticker.get("trust_score")),
                    "source": "coingecko",
                })
            
            safe_print(f"[OK] {len(tickers)} tickers\n")
        else:
            safe_print("[SKIP] No tickers data\n")
    
    if not all_records:
        print("[ERROR] No exchange ticker records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "fact_exchange_tickers.parquet"
    
    # Merge with existing data (deduplicate by date, exchange_id, ticker_pair)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data for these exchanges
        existing = existing.filter(
            ~((pl.col("date") == today) & pl.col("exchange_id").is_in(exchange_ids))
        )
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} exchange ticker records to {output_path}")
    print()


def fetch_and_save_derivatives_exchanges_list():
    """Fetch derivatives exchanges list and save to data lake."""
    print("=" * 80)
    print("FETCHING DERIVATIVES EXCHANGES LIST")
    print("=" * 80)
    
    exchanges = fetch_derivatives_exchanges_list()
    if not exchanges:
        print("[ERROR] No derivatives exchanges data received")
        return
    
    # This is just a list of IDs, so we'll update dim_derivative_exchanges
    # or create a simple reference table
    print(f"[INFO] Found {len(exchanges)} derivative exchanges in list")
    print(f"[INFO] Sample IDs: {[e.get('id', 'N/A') for e in exchanges[:10]]}")
    print()
    print("[NOTE] This endpoint returns just IDs. Use /derivatives/exchanges for full data.")
    print()


def main():
    """Main function to fetch all low-priority data."""
    print("=" * 80)
    print("LOW-PRIORITY COINGECKO DATA FETCH")
    print("=" * 80)
    print()
    
    # Check API usage first
    usage = check_api_usage()
    if usage:
        print(f"API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        print()
    
    # 1. Categories list (1 API call)
    fetch_and_save_categories_list()
    
    # 2. Exchange details with tickers (multiple API calls, for major exchanges only)
    # Limit to top 10 exchanges to keep API usage reasonable
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
        "bybit_spot",
    ]
    fetch_and_save_exchange_tickers(major_exchanges)
    
    # 3. Derivative exchanges list (1 API call)
    fetch_and_save_derivatives_exchanges_list()
    
    print("=" * 80)
    print("ALL LOW-PRIORITY DATA FETCHES COMPLETE!")
    print("=" * 80)
    
    # Final API usage check
    usage = check_api_usage()
    if usage:
        print(f"\nFinal API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        remaining = usage.get('current_remaining_monthly_calls', 0)
        print(f"Remaining: {remaining:,} calls")


if __name__ == "__main__":
    main()
