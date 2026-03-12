#!/usr/bin/env python3
"""Verify the newly fetched data."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("NEW DATA VERIFICATION")
print("=" * 80)

# Check global market data
gm_path = data_lake_dir / "fact_global_market.parquet"
if gm_path.exists():
    gm = pl.read_parquet(str(gm_path))
    print(f"\n=== GLOBAL MARKET DATA ===")
    print(f"Records: {len(gm):,}")
    print(f"Date range: {gm['date'].min()} to {gm['date'].max()}")
    if len(gm) > 0:
        latest = gm.sort('date', descending=True).head(1)
        print(f"\nLatest data:")
        print(f"  BTC Dominance: {latest['btc_dominance'].item():.2f}%")
        print(f"  Total Market Cap: ${latest['total_market_cap_usd'].item():,.0f}")
        print(f"  Active Cryptocurrencies: {latest['active_cryptocurrencies'].item():,}")
else:
    print("\n[WARNING] fact_global_market.parquet not found")

# Check derivative exchanges
dex_path = data_lake_dir / "dim_derivative_exchanges.parquet"
if dex_path.exists():
    dex = pl.read_parquet(str(dex_path))
    print(f"\n=== DERIVATIVE EXCHANGES ===")
    print(f"Exchanges: {len(dex):,}")
    if len(dex) > 0:
        print(f"\nTop 5 by OI:")
        top = dex.sort('open_interest_btc', descending=True).head(5)
        for row in top.to_dicts():
            print(f"  {row['exchange_name']}: {row['open_interest_btc']:,.2f} BTC OI")
else:
    print("\n[WARNING] dim_derivative_exchanges.parquet not found")

# Check derivative volumes
dv_path = data_lake_dir / "fact_derivative_volume.parquet"
if dv_path.exists():
    dv = pl.read_parquet(str(dv_path))
    print(f"\n=== DERIVATIVE VOLUMES ===")
    print(f"Records: {len(dv):,}")
    if len(dv) > 0:
        print(f"  Exchanges: {dv['exchange'].n_unique()}")
        print(f"  Assets: {dv['base_asset'].n_unique()}")
        print(f"  Date range: {dv['date'].min()} to {dv['date'].max()}")
else:
    print("\n[WARNING] fact_derivative_volume.parquet not found")

# Check derivative OI
doi_path = data_lake_dir / "fact_derivative_open_interest.parquet"
if doi_path.exists():
    doi = pl.read_parquet(str(doi_path))
    print(f"\n=== DERIVATIVE OPEN INTEREST ===")
    print(f"Records: {len(doi):,}")
    if len(doi) > 0:
        print(f"  Exchanges: {doi['exchange'].n_unique()}")
        print(f"  Assets: {doi['base_asset'].n_unique()}")
        print(f"  Date range: {doi['date'].min()} to {doi['date'].max()}")
        total_oi = doi['open_interest_usd'].sum()
        print(f"  Total OI: ${total_oi:,.0f}")
else:
    print("\n[WARNING] fact_derivative_open_interest.parquet not found")

print("\n" + "=" * 80)
