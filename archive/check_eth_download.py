"""
Check if we can manually download ETH data for 2024.
"""
import sys
from pathlib import Path
from datetime import date
sys.path.insert(0, str(Path(__file__).parent))

from src.providers.coingecko import fetch_price_history

# Try to fetch ETH data for 2024
print("Attempting to fetch ETH data for 2024 from CoinGecko...")
prices, mcaps, volumes = fetch_price_history(
    coingecko_id="ethereum",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
)

print(f"\nETH 2024 data from CoinGecko:")
print(f"  Prices: {len(prices)} days")
print(f"  Market caps: {len(mcaps)} days")
print(f"  Volumes: {len(volumes)} days")

if len(prices) > 0:
    print(f"\nETH price date range: {min(prices.keys())} to {max(prices.keys())}")
    print(f"Sample prices (first 5):")
    for i, (d, p) in enumerate(sorted(prices.items())[:5]):
        print(f"  {d}: ${p:,.2f}")
else:
    print("\nWARNING: No ETH price data returned from CoinGecko for 2024!")
