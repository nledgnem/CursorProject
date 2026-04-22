#!/usr/bin/env python3
"""Expand allowlist by fetching top coins from CoinGecko."""

import sys
import requests
import time
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko import COINGECKO_BASE, COINGECKO_API_KEY

def fetch_top_coins(n: int = 1000, min_mcap: int = 1000000) -> pd.DataFrame:
    """
    Fetch top N coins from CoinGecko by market cap.
    
    Args:
        n: Number of coins to fetch
        min_mcap: Minimum market cap in USD
    """
    print(f"Fetching top {n} coins from CoinGecko (min mcap: ${min_mcap:,})...")
    
    all_coins = []
    page = 1
    per_page = 250  # Max per page
    
    while len(all_coins) < n:
        url = f"{COINGECKO_BASE}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
            "x_cg_pro_api_key": COINGECKO_API_KEY,
        }
        
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"[ERROR] API error: {resp.status_code} {resp.text[:200]}")
                break
            
            data = resp.json()
            if not data or len(data) == 0:
                break
            
            for coin in data:
                mcap = coin.get("market_cap", 0) or 0
                if mcap < min_mcap:
                    # Since sorted by market cap, we can stop here
                    print(f"Reached minimum market cap threshold at coin {len(all_coins)}")
                    break
                
                all_coins.append({
                    "symbol": coin["symbol"].upper(),
                    "coingecko_id": coin["id"],
                    "name": coin["name"],
                    "market_cap": mcap,
                })
            
            if len(data) < per_page:
                break
            
            page += 1
            time.sleep(1)  # Rate limiting
            
            if len(all_coins) >= n:
                break
                
        except Exception as e:
            print(f"[ERROR] Request failed: {e}")
            break
    
    df = pd.DataFrame(all_coins)
    if len(df) > n:
        df = df.head(n)
    
    print(f"Fetched {len(df)} coins")
    return df


def expand_allowlist(output_path: Path, n: int = 1000, min_mcap: int = 1000000):
    """Expand allowlist with top coins from CoinGecko."""
    # Fetch coins
    coins_df = fetch_top_coins(n=n, min_mcap=min_mcap)
    
    if coins_df.empty:
        print("[ERROR] No coins fetched")
        return
    
    # Create allowlist format (symbol, coingecko_id, venue)
    # For now, we'll use "BINANCE" as venue (you can update this later based on actual perp availability)
    allowlist_df = pd.DataFrame({
        "symbol": coins_df["symbol"],
        "coingecko_id": coins_df["coingecko_id"],
        "venue": "BINANCE",  # Placeholder - update based on actual perp availability
    })
    
    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    allowlist_df.to_csv(output_path, index=False)
    
    print(f"\n[SUCCESS] Allowlist expanded to {len(allowlist_df)} coins")
    print(f"Saved to {output_path}")
    print(f"\nTop 20 coins by market cap:")
    print(coins_df.head(20)[["symbol", "name", "market_cap"]].to_string(index=False))
    
    print(f"\nNote: The 'venue' column is set to 'BINANCE' as placeholder.")
    print("You may want to update this based on actual perpetual futures availability.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Expand allowlist with CoinGecko coins")
    parser.add_argument(
        "--n",
        type=int,
        default=1000,
        help="Number of coins to fetch (default: 1000)",
    )
    parser.add_argument(
        "--min-mcap",
        type=int,
        default=1000000,
        help="Minimum market cap in USD (default: 1,000,000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "perp_allowlist.csv",
        help="Output path for allowlist CSV",
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Expanding Allowlist")
    print("=" * 60)
    print(f"Target: {args.n} coins")
    print(f"Min market cap: ${args.min_mcap:,}")
    print(f"Output: {args.output}")
    print("=" * 60)
    
    expand_allowlist(args.output, n=args.n, min_mcap=args.min_mcap)

