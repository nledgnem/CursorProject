#!/usr/bin/env python3
"""
Analyze funding rate data length for each coin.
Shows top 10 coins with longest history and bottom 10 coins with shortest history.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Optional
import requests
from collections import defaultdict

# CoinGlass API config
COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")  # Set in env; never commit keys
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
API_SLEEP_SECONDS = 2  # Reduced from 3 to speed up

# Symbols to check (from regime_monitor.py)
FUNDING_SYMBOLS = [
    "BTC", "ETH", "XRP", "BNB", "SOL", "TRX", "ADA", "DOGE", "BCH", "LINK", "AVAX",
    "ARB", "OP", "TIA", "INJ", "NEAR", "APT", "SUI", "JUP", "PYTH", "SEI",
    "MATIC", "DOT", "LTC", "UNI", "ATOM", "ETC", "XLM", "FIL", "ICP", "ALGO",
    "VET", "THETA", "EOS", "AAVE", "MKR", "GRT", "SNX", "COMP", "YFI", "SUSHI",
    "CRV", "1INCH", "BAL", "REN", "ZRX", "BAT", "ZEC", "DASH", "XMR", "ENJ",
    "MANA", "SAND", "AXS", "GALA", "CHZ", "FLOW", "HBAR", "EGLD", "FTM",
    "ONE", "WAVES", "KSM", "ROSE", "CELO", "IOTA", "QTUM", "NEO", "ONT", "ZIL",
    "SC", "STORJ", "ANKR", "RUNE", "OCEAN", "ALPHA", "KAVA", "BAND", "CTSI", "OMG",
    "SKL", "LRC", "ZEN", "COTI", "FET", "RLC", "PERP", "UMA", "BADGER", "FIS",
    "BONK", "WIF", "PEPE", "FLOKI", "SHIB", "LUNC", "1000SATS", "ORDI", "RATS",
    "BOME", "MYRO", "POPCAT", "MEW", "GME", "TRUMP", "BIDEN", "AMP", "KITE",
    "KAITO", "LAYER", "ALT", "GMT", "ZETA", "MANTA", "DYM"
]

def coinglass_get(url: str, params: Optional[Dict] = None, timeout: int = 10) -> Optional[Dict]:
    """Make a GET request to CoinGlass API with rate limiting."""
    time.sleep(API_SLEEP_SECONDS)
    
    if url.startswith("http"):
        full_url = url
    else:
        full_url = f"{COINGLASS_BASE}{url}"
    
    headers = {
        "CG-API-KEY": COINGLASS_API_KEY
    }
    
    try:
        response = requests.get(full_url, headers=headers, params=params, timeout=timeout)
        
        if response.status_code == 429:
            print(f"Warning: Rate limited (429), retrying after longer sleep...")
            time.sleep(API_SLEEP_SECONDS * 3)
            response = requests.get(full_url, headers=headers, params=params, timeout=timeout)
        
        response.raise_for_status()
        return response.json()
        
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def fetch_funding_history_length(symbol: str) -> Optional[Dict]:
    """
    Fetch funding rate history for a symbol and return:
    - data_length: number of days with data
    - first_date: earliest date with data
    - last_date: latest date with data
    """
    url = f"{COINGLASS_BASE}/api/futures/funding-rate/oi-weight-history"
    
    # Try to fetch a large range (2 years back)
    end_ms = int(datetime.now().timestamp() * 1000)
    start_ms = int((datetime.now() - timedelta(days=730)).timestamp() * 1000)
    
    params = {
        "symbol": symbol.upper(),
        "interval": "8h",
        "startTime": start_ms,
        "endTime": end_ms,
    }
    
    data = coinglass_get(url, params=params)
    if data is None:
        return None
    
    if data.get("code") != "0":
        return None
    
    rows = data.get("data", [])
    if not isinstance(rows, list) or not rows:
        return {
            "data_length": 0,
            "first_date": None,
            "last_date": None
        }
    
    # Extract unique dates
    dates = set()
    for row in rows:
        try:
            t_ms = int(row.get("time"))
            d = datetime.fromtimestamp(t_ms / 1000.0).date()
            dates.add(d)
        except Exception:
            continue
    
    if not dates:
        return {
            "data_length": 0,
            "first_date": None,
            "last_date": None
        }
    
    sorted_dates = sorted(dates)
    return {
        "data_length": len(sorted_dates),
        "first_date": sorted_dates[0],
        "last_date": sorted_dates[-1]
    }


def main():
    import json
    
    print("=" * 80)
    print("Funding Rate Data Length Analysis")
    print("=" * 80)
    print(f"\nAnalyzing {len(FUNDING_SYMBOLS)} coins...\n")
    
    results = []
    results_file = "funding_data_length_results.json"
    
    # Try to load existing results
    try:
        with open(results_file, 'r') as f:
            existing = json.load(f)
            existing_symbols = {r['symbol'] for r in existing}
            print(f"Loaded {len(existing)} existing results from {results_file}\n")
    except:
        existing = []
        existing_symbols = set()
    
    for i, symbol in enumerate(FUNDING_SYMBOLS, 1):
        if symbol in existing_symbols:
            # Skip if we already have this symbol
            existing_result = next(r for r in existing if r['symbol'] == symbol)
            results.append(existing_result)
            length = existing_result["data_length"]
            if length > 0:
                print(f"[{i}/{len(FUNDING_SYMBOLS)}] {symbol} (cached) - {length} days")
            else:
                print(f"[{i}/{len(FUNDING_SYMBOLS)}] {symbol} (cached) - NO DATA")
            continue
            
        print(f"[{i}/{len(FUNDING_SYMBOLS)}] Fetching {symbol}...", end=" ", flush=True)
        result = fetch_funding_history_length(symbol)
        
        if result is None:
            print("ERROR")
            results.append({
                "symbol": symbol,
                "data_length": 0,
                "first_date": None,
                "last_date": None
            })
        else:
            length = result["data_length"]
            first = result["first_date"]
            last = result["last_date"]
            
            if length > 0:
                print(f"OK - {length} days ({first} to {last})")
            else:
                print("NO DATA")
            
            results.append({
                "symbol": symbol,
                "data_length": length,
                "first_date": str(first) if first else None,
                "last_date": str(last) if last else None
            })
        
        # Save progress after each coin
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
    
    # Sort by data length
    results_sorted = sorted(results, key=lambda x: x["data_length"], reverse=True)
    
    # Display results
    print("\n" + "=" * 80)
    print("TOP 10 COINS BY DATA LENGTH (Longest History)")
    print("=" * 80)
    print(f"{'Rank':<6} {'Symbol':<10} {'Days':<8} {'First Date':<12} {'Last Date':<12}")
    print("-" * 80)
    
    for i, r in enumerate(results_sorted[:10], 1):
        symbol = r["symbol"]
        length = r["data_length"]
        first = r["first_date"] if r["first_date"] else "N/A"
        last = r["last_date"] if r["last_date"] else "N/A"
        # Convert string dates back if needed
        if isinstance(first, str) and first != "N/A":
            first = datetime.strptime(first, "%Y-%m-%d").date()
        if isinstance(last, str) and last != "N/A":
            last = datetime.strptime(last, "%Y-%m-%d").date()
        print(f"{i:<6} {symbol:<10} {length:<8} {str(first):<12} {str(last):<12}")
    
    print("\n" + "=" * 80)
    print("BOTTOM 10 COINS BY DATA LENGTH (Shortest History)")
    print("=" * 80)
    print(f"{'Rank':<6} {'Symbol':<10} {'Days':<8} {'First Date':<12} {'Last Date':<12}")
    print("-" * 80)
    
    # Filter out zero-length results for bottom 10
    non_zero = [r for r in results_sorted if r["data_length"] > 0]
    if len(non_zero) >= 10:
        bottom_10 = non_zero[-10:]
    else:
        bottom_10 = non_zero
    
    for i, r in enumerate(reversed(bottom_10), 1):
        symbol = r["symbol"]
        length = r["data_length"]
        first = r["first_date"] if r["first_date"] else "N/A"
        last = r["last_date"] if r["last_date"] else "N/A"
        # Convert string dates back if needed
        if isinstance(first, str) and first != "N/A":
            first = datetime.strptime(first, "%Y-%m-%d").date()
        if isinstance(last, str) and last != "N/A":
            last = datetime.strptime(last, "%Y-%m-%d").date()
        print(f"{i:<6} {symbol:<10} {length:<8} {str(first):<12} {str(last):<12}")
    
    # Summary stats
    zero_data = sum(1 for r in results if r["data_length"] == 0)
    avg_length = sum(r["data_length"] for r in results) / len(results) if results else 0
    
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total coins analyzed: {len(results)}")
    print(f"Coins with data: {len(results) - zero_data}")
    print(f"Coins with no data: {zero_data}")
    print(f"Average data length: {avg_length:.1f} days")
    print(f"Max data length: {results_sorted[0]['data_length']} days ({results_sorted[0]['symbol']})")
    if non_zero:
        print(f"Min data length (non-zero): {non_zero[-1]['data_length']} days ({non_zero[-1]['symbol']})")


if __name__ == "__main__":
    main()
