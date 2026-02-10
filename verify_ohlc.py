#!/usr/bin/env python3
"""Verify OHLC data."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")
ohlc_path = data_lake_dir / "fact_ohlc.parquet"

if ohlc_path.exists():
    ohlc = pl.read_parquet(str(ohlc_path))
    print("=" * 80)
    print("OHLC DATA VERIFICATION")
    print("=" * 80)
    print(f"Total records: {len(ohlc):,}")
    print(f"Date range: {ohlc['date'].min()} to {ohlc['date'].max()}")
    print(f"Unique assets: {ohlc['asset_id'].n_unique()}")
    
    btc = ohlc.filter(pl.col('asset_id') == 'BTC')
    if len(btc) > 0:
        print(f"\nBTC OHLC:")
        print(f"  Records: {len(btc):,}")
        print(f"  Date range: {btc['date'].min()} to {btc['date'].max()}")
        sample = btc.head(1)
        print(f"  Sample (first record):")
        print(f"    Date: {sample['date'].item()}")
        print(f"    Open: ${sample['open'].item():,.2f}")
        print(f"    High: ${sample['high'].item():,.2f}")
        print(f"    Low: ${sample['low'].item():,.2f}")
        print(f"    Close: ${sample['close'].item():,.2f}")
    
    print(f"\nTop 10 assets by OHLC records:")
    top = ohlc.group_by('asset_id').agg(pl.len().alias('count')).sort('count', descending=True).head(10)
    for r in top.to_dicts():
        print(f"  {r['asset_id']}: {r['count']:,} records")
else:
    print("OHLC file not found yet")
