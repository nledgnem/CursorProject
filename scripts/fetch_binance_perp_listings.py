#!/usr/bin/env python3
"""Fetch Binance USD-M Futures perpetual listings and save as parquet dataset."""

import sys
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.metadata import get_git_commit_hash


BINANCE_API_BASE = "https://fapi.binance.com"


def fetch_binance_exchange_info() -> Optional[Dict]:
    """Fetch Binance USD-M Futures exchangeInfo."""
    url = f"{BINANCE_API_BASE}/fapi/v1/exchangeInfo"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch Binance exchangeInfo: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON response: {e}")
        return None


def parse_perp_listings(exchange_info: Dict) -> pd.DataFrame:
    """
    Parse exchangeInfo and extract perpetual futures listings.
    
    Filters for:
    - contractType == "PERPETUAL"
    - quoteAsset == "USDT" (or other quote assets if needed)
    
    Returns DataFrame with columns: symbol, onboard_date, source, proxy_version
    """
    if "symbols" not in exchange_info:
        print("[ERROR] No 'symbols' field in exchangeInfo")
        return pd.DataFrame()
    
    rows = []
    for symbol_info in exchange_info["symbols"]:
        contract_type = symbol_info.get("contractType")
        quote_asset = symbol_info.get("quoteAsset")
        
        # Filter for perpetual USDT contracts
        if contract_type != "PERPETUAL":
            continue
        if quote_asset != "USDT":
            continue  # Can extend later to support other quote assets
        
        symbol = symbol_info.get("symbol")
        onboard_time = symbol_info.get("onboardDate")
        
        if not symbol or not onboard_time:
            continue
        
        # Convert onboardDate (milliseconds timestamp) to date
        try:
            onboard_dt = datetime.fromtimestamp(onboard_time / 1000, tz=timezone.utc)
            onboard_date = onboard_dt.date()
        except (ValueError, TypeError, OSError) as e:
            print(f"[WARN] Failed to parse onboardDate for {symbol}: {e}")
            continue
        
        rows.append({
            "symbol": symbol,
            "onboard_date": onboard_date,
            "source": "binance_exchangeInfo",
            "proxy_version": "v0",
        })
    
    if not rows:
        print("[WARN] No perpetual USDT contracts found")
        return pd.DataFrame(columns=["symbol", "onboard_date", "source", "proxy_version"])
    
    df = pd.DataFrame(rows)
    df = df.sort_values("onboard_date").reset_index(drop=True)
    
    return df


def save_dataset(df: pd.DataFrame, output_path: Path, repo_root: Path) -> None:
    """Save dataset to parquet and write metadata."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"  Saved {len(df)} perpetual listings to {output_path}")
    
    # Write metadata JSON
    metadata = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "script_name": "fetch_binance_perp_listings.py",
        "git_commit_hash": get_git_commit_hash(repo_root),
        "source": "binance_exchangeInfo",
        "proxy_version": "v0",
        "num_symbols": len(df),
        "date_range": {
            "earliest_onboard": str(df["onboard_date"].min()) if len(df) > 0 else None,
            "latest_onboard": str(df["onboard_date"].max()) if len(df) > 0 else None,
        },
        "output_file": str(output_path.relative_to(repo_root)) if output_path.is_relative_to(repo_root) else str(output_path),
    }
    
    metadata_path = output_path.parent / f"{output_path.stem}_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata to {metadata_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Binance perpetual futures listings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Save to default location (data/raw/perp_listings_binance.parquet)
  python scripts/fetch_binance_perp_listings.py

  # Save to custom location
  python scripts/fetch_binance_perp_listings.py --output outputs/perp_listings_binance.parquet
        """,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output parquet file path (default: data/raw/perp_listings_binance.parquet)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: append new listings to existing file",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    # Determine output path
    if args.output:
        output_path = args.output if args.output.is_absolute() else repo_root / args.output
    else:
        output_path = repo_root / "data" / "raw" / "perp_listings_binance.parquet"
    
    print("=" * 60)
    print("Fetching Binance Perpetual Futures Listings")
    print("=" * 60)
    print(f"Output: {output_path}")
    print("-" * 60)
    
    # Fetch exchangeInfo
    print("Fetching exchangeInfo from Binance...")
    exchange_info = fetch_binance_exchange_info()
    if not exchange_info:
        print("[ERROR] Failed to fetch exchangeInfo")
        sys.exit(1)
    
    # Parse perpetual listings
    print("Parsing perpetual listings...")
    df_new = parse_perp_listings(exchange_info)
    if df_new.empty:
        print("[ERROR] No perpetual listings found")
        sys.exit(1)
    
    print(f"  Found {len(df_new)} perpetual USDT contracts")
    print(f"  Date range: {df_new['onboard_date'].min()} to {df_new['onboard_date'].max()}")
    
    # Append to existing if incremental
    if args.incremental and output_path.exists():
        try:
            existing_df = pd.read_parquet(output_path)
            # Remove duplicates (existing takes precedence)
            existing_symbols = set(existing_df["symbol"])
            new_symbols = set(df_new["symbol"])
            symbols_to_add = new_symbols - existing_symbols
            
            if symbols_to_add:
                df_to_append = df_new[df_new["symbol"].isin(symbols_to_add)]
                df = pd.concat([existing_df, df_to_append], ignore_index=True)
                print(f"  [INCREMENTAL] Appended {len(df_to_append)} new listings to existing {len(existing_df)} listings")
            else:
                df = existing_df
                print(f"  [INCREMENTAL] No new listings to append (all symbols already exist)")
        except Exception as e:
            print(f"  [WARN] Could not load existing listings: {e}, creating new")
            df = df_new
    else:
        df = df_new
    
    # Save dataset
    print("\nSaving dataset...")
    save_dataset(df, output_path, repo_root)
    
    print("\n" + "=" * 60)
    print("Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
