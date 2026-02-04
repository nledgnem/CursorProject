#!/usr/bin/env python3
"""
Fetch medium-priority CoinGecko data for MSM v0.

This script fetches:
1. All exchanges - Exchange rankings and trading volumes
2. Derivative exchange details - Exchange-specific derivative metrics
"""

import sys
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko_analyst import (
    fetch_exchanges_list,
    fetch_derivative_exchange_details,
    check_api_usage,
)
from scripts.fetch_derivative_data import fetch_derivatives_exchanges

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


def safe_int(val, default=None) -> Optional[int]:
    """Safely convert value to int, handling None."""
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def safe_float(val, default=0.0) -> float:
    """Safely convert value to float, handling None."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def safe_bool(val) -> Optional[bool]:
    """Safely convert value to bool, handling None."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def fetch_and_save_exchanges():
    """Fetch all exchanges and save to data lake."""
    print("=" * 80)
    print("FETCHING ALL EXCHANGES")
    print("=" * 80)
    
    exchanges = fetch_exchanges_list()
    if not exchanges:
        print("[ERROR] No exchange data received")
        return
    
    all_records = []
    
    for exchange in exchanges:
        all_records.append({
            "exchange_id": exchange.get("id", ""),
            "exchange_name": exchange.get("name", ""),
            "country": safe_str(exchange.get("country")),
            "year_established": safe_int(exchange.get("year_established")),
            "description": safe_str(exchange.get("description")),
            "url": safe_str(exchange.get("url")),
            "image": safe_str(exchange.get("image")),
            "has_trading_incentive": safe_bool(exchange.get("has_trading_incentive")),
            "trust_score": safe_int(exchange.get("trust_score")),
            "trust_score_rank": safe_int(exchange.get("trust_score_rank")),
            "trade_volume_24h_btc": safe_float(exchange.get("trade_volume_24h_btc")),
            "trade_volume_24h_btc_normalized": safe_float(exchange.get("trade_volume_24h_btc_normalized")),
            "tickers": safe_int(exchange.get("tickers")),
            "source": "coingecko",
        })
    
    if not all_records:
        print("[ERROR] No exchange records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "dim_exchanges.parquet"
    
    # Merge with existing data (deduplicate by exchange_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove exchanges that are being updated
        existing_ids = set(df["exchange_id"].unique().to_list())
        existing = existing.filter(~pl.col("exchange_id").is_in(existing_ids))
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} exchange records to {output_path}")
    print()


def fetch_and_save_derivative_exchange_details():
    """Fetch derivative exchange details and save to data lake."""
    print("=" * 80)
    print("FETCHING DERIVATIVE EXCHANGE DETAILS")
    print("=" * 80)
    
    # First, get list of derivative exchanges
    derivative_exchanges = fetch_derivatives_exchanges()
    if not derivative_exchanges:
        print("[ERROR] No derivative exchanges list received")
        return
    
    today = date.today()
    all_records = []
    
    # Extract exchange IDs from the list
    exchange_ids = []
    for exchange in derivative_exchanges:
        exchange_id = exchange.get("id") or exchange.get("exchange_id")
        if exchange_id:
            exchange_ids.append(exchange_id)
    
    print(f"Found {len(exchange_ids)} derivative exchanges")
    print()
    
    for idx, exchange_id in enumerate(exchange_ids):
        safe_print(f"[{idx+1}/{len(exchange_ids)}] {exchange_id}... ")
        
        details = fetch_derivative_exchange_details(exchange_id)
        
        if details:
            all_records.append({
                "date": today,
                "exchange_id": exchange_id,
                "exchange_name": safe_str(details.get("name")),
                "open_interest_btc": safe_float(details.get("open_interest_btc")),
                "trade_volume_24h_btc": safe_float(details.get("trade_volume_24h_btc")),
                "number_of_perpetual_pairs": safe_int(details.get("number_of_perpetual_pairs"), 0),
                "number_of_futures_pairs": safe_int(details.get("number_of_futures_pairs"), 0),
                "number_of_derivatives": safe_int(details.get("number_of_derivatives")),
                "source": "coingecko",
            })
            safe_print("[OK]\n")
        else:
            safe_print("[SKIP] No data\n")
    
    if not all_records:
        print("[ERROR] No derivative exchange detail records created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_records)
    output_path = DATA_LAKE_DIR / "fact_derivative_exchange_details.parquet"
    
    # Merge with existing data (deduplicate by date, exchange_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove today's data for these exchanges
        existing = existing.filter(
            ~((pl.col("date") == today) & pl.col("exchange_id").is_in(exchange_ids))
        )
        df = pl.concat([existing, df])
    
    df.write_parquet(str(output_path))
    print(f"[SUCCESS] Saved {len(all_records)} derivative exchange detail records to {output_path}")
    print()


def main():
    """Main function to fetch all medium-priority data."""
    print("=" * 80)
    print("MEDIUM-PRIORITY COINGECKO DATA FETCH")
    print("=" * 80)
    print()
    
    # Check API usage first
    usage = check_api_usage()
    if usage:
        print(f"API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        print()
    
    # 1. All exchanges (1 API call)
    fetch_and_save_exchanges()
    
    # 2. Derivative exchange details (multiple API calls, one per exchange)
    fetch_and_save_derivative_exchange_details()
    
    print("=" * 80)
    print("ALL MEDIUM-PRIORITY DATA FETCHES COMPLETE!")
    print("=" * 80)
    
    # Final API usage check
    usage = check_api_usage()
    if usage:
        print(f"\nFinal API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        remaining = usage.get('current_remaining_monthly_calls', 0)
        print(f"Remaining: {remaining:,} calls")


if __name__ == "__main__":
    main()
