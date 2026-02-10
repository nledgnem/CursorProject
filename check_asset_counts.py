#!/usr/bin/env python3
"""Check asset counts for CoinGecko and CoinGlass."""

import sys
import io
import pandas as pd
from pathlib import Path

# Windows encoding fix
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# CoinGecko
allowlist = pd.read_csv(Path("data/perp_allowlist.csv"))
fact_price = pd.read_parquet(Path("data/curated/data_lake/fact_price.parquet"))

# CoinGlass
dim_instrument = pd.read_parquet(Path("data/curated/data_lake/dim_instrument.parquet"))
fact_funding = pd.read_parquet(Path("data/curated/data_lake/fact_funding.parquet"))

print("=" * 70)
print("ASSET COUNT SUMMARY")
print("=" * 70)

print("\nCoinGecko:")
print(f"  Allowlist: {len(allowlist)} assets")
print(f"  fact_price: {fact_price['asset_id'].nunique()} unique assets")
print(f"  fact_price rows: {len(fact_price):,} total rows")

print("\nCoinGlass:")
print(f"  dim_instrument: {len(dim_instrument)} instruments")
print(f"  Unique base assets: {dim_instrument['base_asset_symbol'].nunique()}")
print(f"  Linked to asset_id: {dim_instrument['asset_id'].notna().sum()} instruments")
print(f"  fact_funding: {fact_funding['asset_id'].nunique()} unique assets")
print(f"  fact_funding rows: {len(fact_funding):,} total rows")

print("\n" + "=" * 70)

