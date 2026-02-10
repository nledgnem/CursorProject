#!/usr/bin/env python3
"""Test OHLC fetch with different date ranges."""

import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent))

from src.providers.coingecko_analyst import fetch_ohlc_range

# Test with recent dates (we know price data exists)
print("Testing BTC OHLC with recent dates (2024-01-01 to 2024-01-10)...")
ohlc_recent = fetch_ohlc_range('bitcoin', date(2024, 1, 1), date(2024, 1, 10))
print(f"  Result: {len(ohlc_recent)} days")
if ohlc_recent:
    print(f"  Sample: {ohlc_recent[:2]}")

# Test with old dates (2013)
print("\nTesting BTC OHLC with old dates (2013-04-28 to 2013-05-05)...")
ohlc_old = fetch_ohlc_range('bitcoin', date(2013, 4, 28), date(2013, 5, 5))
print(f"  Result: {len(ohlc_old)} days")
if ohlc_old:
    print(f"  Sample: {ohlc_old[:2]}")

# Test with date range that matches existing price data
print("\nTesting BTC OHLC with date range matching price data (2013-04-28 to 2018-10-17)...")
ohlc_range = fetch_ohlc_range('bitcoin', date(2013, 4, 28), date(2018, 10, 17))
print(f"  Result: {len(ohlc_range)} days")
if ohlc_range:
    print(f"  First: {ohlc_range[0]}")
    print(f"  Last: {ohlc_range[-1]}")
