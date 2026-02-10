import polars as pl
from datetime import date

# Load price data
prices = pl.read_parquet('data/curated/data_lake/fact_price.parquet')

# Check ETH
eth_data = prices.filter(pl.col('asset_id') == 'ETH')
print(f'ETH rows: {len(eth_data)}')

if len(eth_data) > 0:
    print(f'ETH date range: {eth_data["date"].min()} to {eth_data["date"].max()}')
    print(f'ETH sample dates (first 10): {eth_data["date"].unique().sort().head(10).to_list()}')
    
    # Check specific date range
    eth_jan = prices.filter(
        (pl.col('asset_id') == 'ETH') & 
        (pl.col('date') >= pl.date(2024, 1, 8)) & 
        (pl.col('date') <= pl.date(2024, 1, 15))
    )
    print(f'\nETH data for 2024-01-08 to 2024-01-15: {len(eth_jan)} rows')
    if len(eth_jan) > 0:
        print(eth_jan)
else:
    print('No ETH data found!')
    
# Check all asset_ids containing ETH
eth_like = prices.filter(pl.col('asset_id').str.contains('ETH', literal=True))
if len(eth_like) > 0:
    print(f'\nAll asset_ids containing ETH: {eth_like["asset_id"].unique().to_list()}')

# Check BTC for comparison
btc_data = prices.filter(pl.col('asset_id') == 'BTC')
print(f'\nBTC rows: {len(btc_data)}')
if len(btc_data) > 0:
    print(f'BTC date range: {btc_data["date"].min()} to {btc_data["date"].max()}')
