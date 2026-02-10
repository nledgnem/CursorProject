#!/usr/bin/env python3
"""Test if CoinGecko API returns category info for individual coins."""

import requests
import json
from src.providers.coingecko_analyst import COINGECKO_BASE, COINGECKO_API_KEY

# Test with Bitcoin
coin_id = "bitcoin"
url = f"{COINGECKO_BASE}/coins/{coin_id}"

params = {
    "x_cg_pro_api_key": COINGECKO_API_KEY,
    "localization": "false",
    "tickers": "false",
    "market_data": "false",
    "community_data": "false",
    "developer_data": "false",
    "sparkline": "false",
}

try:
    resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
    resp.raise_for_status()
    data = resp.json()
    
    print("=" * 80)
    print(f"CoinGecko API Response for {coin_id}")
    print("=" * 80)
    print()
    
    # Check for category fields
    if "categories" in data:
        print(f"Categories field found: {data['categories']}")
    else:
        print("No 'categories' field in response")
    
    if "category" in data:
        print(f"Category field found: {data['category']}")
    else:
        print("No 'category' field in response")
    
    # Show all top-level keys
    print()
    print("Top-level keys in response:")
    for key in sorted(data.keys()):
        print(f"  - {key}")
    
    # If categories exist, show them
    if "categories" in data and data["categories"]:
        print()
        print("Categories for this coin:")
        for cat in data["categories"]:
            print(f"  - {cat}")
    
except Exception as e:
    print(f"Error: {e}")
