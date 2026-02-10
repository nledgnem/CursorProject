#!/usr/bin/env python3
"""Verify all newly fetched data."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("COMPLETE DATA VERIFICATION")
print("=" * 80)

# Check market breadth
mb_path = data_lake_dir / "fact_market_breadth.parquet"
if mb_path.exists():
    mb = pl.read_parquet(str(mb_path))
    print(f"\n=== MARKET BREADTH ===")
    print(f"Records: {len(mb):,}")
    print(f"Date: {mb['date'].unique().to_list()}")
    print(f"Durations: {mb['duration'].unique().to_list()}")
    print(f"Categories: {mb['category'].unique().to_list()}")
    
    if len(mb) > 0:
        print(f"\nTop 10 Gainers (24h):")
        top = mb.filter((pl.col('duration') == '24h') & (pl.col('category') == 'gainer')).sort('rank').head(10)
        for r in top.to_dicts():
            change = r.get('price_change_24h', 0.0)
            print(f"  {r['rank']}. {r['asset_id']}: {change:.2f}%")
        
        print(f"\nTop 10 Losers (24h):")
        losers = mb.filter((pl.col('duration') == '24h') & (pl.col('category') == 'loser')).sort('rank').head(10)
        for r in losers.to_dicts():
            change = r.get('price_change_24h', 0.0)
            print(f"  {r['rank']}. {r['asset_id']}: {change:.2f}%")
else:
    print("\n[WARNING] fact_market_breadth.parquet not found")

# Check new listings
nl_path = data_lake_dir / "dim_new_listings.parquet"
if nl_path.exists():
    nl = pl.read_parquet(str(nl_path))
    print(f"\n=== NEW LISTINGS ===")
    print(f"Records: {len(nl):,}")
    if len(nl) > 0:
        print(f"\nSample listings:")
        for r in nl.head(10).to_dicts():
            print(f"  {r['asset_id']} ({r['symbol']}): {r['name']}")
else:
    print("\n[WARNING] dim_new_listings.parquet not found")

# Check exchange volumes
ev_path = data_lake_dir / "fact_exchange_volume.parquet"
if ev_path.exists():
    ev = pl.read_parquet(str(ev_path))
    print(f"\n=== EXCHANGE VOLUMES ===")
    print(f"Records: {len(ev):,}")
    print(f"Exchanges: {ev['exchange_id'].n_unique()}")
    print(f"Date range: {ev['date'].min()} to {ev['date'].max()}")
    if len(ev) > 0:
        print(f"\nLatest volumes by exchange:")
        latest = ev.filter(pl.col('date') == ev['date'].max())
        for r in latest.sort('volume_usd', descending=True).head(10).to_dicts():
            print(f"  {r['exchange_id']}: ${r['volume_usd']:,.0f}")
else:
    print("\n[WARNING] fact_exchange_volume.parquet not found")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

files_to_check = [
    'fact_global_market.parquet',
    'fact_derivative_volume.parquet',
    'fact_derivative_open_interest.parquet',
    'dim_derivative_exchanges.parquet',
    'dim_new_listings.parquet',
    'fact_exchange_volume.parquet',
    'fact_market_breadth.parquet',
]

success_count = 0
for f in files_to_check:
    if (data_lake_dir / f).exists():
        success_count += 1
        print(f"[OK] {f}")
    else:
        print(f"[MISSING] {f}")

print(f"\nSuccessfully fetched: {success_count}/{len(files_to_check)} data types")
print()
