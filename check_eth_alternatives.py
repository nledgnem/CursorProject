import polars as pl
from datetime import date
import sys
import io

# Fix encoding for Windows console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Load price data
prices = pl.read_parquet('data/curated/data_lake/fact_price.parquet')

# Check dim_asset for ETH
dim_asset = pl.read_parquet('data/curated/data_lake/dim_asset.parquet')
eth_in_dim = dim_asset.filter(
    (pl.col('asset_id') == 'ETH') | 
    (pl.col('symbol') == 'ETH') |
    (pl.col('symbol').str.contains('ETH', literal=True))
)
print('ETH in dim_asset:')
print(eth_in_dim.select(['asset_id', 'symbol']).head(10))

# Check all ETH-like assets in 2024
eth_like_2024 = prices.filter(
    (pl.col('asset_id').str.contains('ETH', literal=True)) &
    (pl.col('date') >= pl.date(2024, 1, 1)) & 
    (pl.col('date') <= pl.date(2024, 12, 31))
)
print(f'\nETH-like assets in 2024: {eth_like_2024["asset_id"].unique().to_list()}')

# Check earliest date for any ETH-like asset
eth_like_all = prices.filter(pl.col('asset_id').str.contains('ETH', literal=True))
if len(eth_like_all) > 0:
    print(f'\nEarliest date for any ETH-like asset: {eth_like_all["date"].min()}')
    print(f'Latest date for any ETH-like asset: {eth_like_all["date"].max()}')
    
    # Group by asset_id and show date ranges
    print('\nETH-like assets date ranges:')
    for asset_id in eth_like_all["asset_id"].unique().to_list()[:10]:
        asset_data = eth_like_all.filter(pl.col('asset_id') == asset_id)
        if len(asset_data) > 0:
            print(f'  {asset_id}: {asset_data["date"].min()} to {asset_data["date"].max()} ({len(asset_data)} rows)')
