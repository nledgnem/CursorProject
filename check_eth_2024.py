import polars as pl
from datetime import date

# Load price data WITHOUT date filtering
prices_full = pl.read_parquet('data/curated/data_lake/fact_price.parquet')

# Check ETH in 2024
eth_2024 = prices_full.filter(
    (pl.col('asset_id') == 'ETH') & 
    (pl.col('date') >= pl.date(2024, 1, 1)) & 
    (pl.col('date') <= pl.date(2024, 12, 31))
)
print(f'ETH rows in 2024: {len(eth_2024)}')

if len(eth_2024) > 0:
    print(f'ETH 2024 date range: {eth_2024["date"].min()} to {eth_2024["date"].max()}')
    print(f'Sample ETH 2024 data:')
    print(eth_2024.head(10))
else:
    print('No ETH data in 2024!')
    
# Check what happens when we filter by date range like the loader does
prices_filtered = prices_full.filter(
    (pl.col('date') >= pl.date(2024, 1, 1)) & 
    (pl.col('date') <= pl.date(2024, 12, 31))
)
eth_in_filtered = prices_filtered.filter(pl.col('asset_id') == 'ETH')
print(f'\nETH rows in date-filtered dataset (2024-01-01 to 2024-12-31): {len(eth_in_filtered)}')

# Check BTC for comparison
btc_2024 = prices_full.filter(
    (pl.col('asset_id') == 'BTC') & 
    (pl.col('date') >= pl.date(2024, 1, 1)) & 
    (pl.col('date') <= pl.date(2024, 12, 31))
)
print(f'\nBTC rows in 2024: {len(btc_2024)}')
if len(btc_2024) > 0:
    print(f'BTC 2024 date range: {btc_2024["date"].min()} to {btc_2024["date"].max()}')
