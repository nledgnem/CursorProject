import polars as pl

# Check dim_asset
dim = pl.read_parquet('data/curated/data_lake/dim_asset.parquet')
eth_dim = dim.filter(pl.col('asset_id') == 'ETH')
print(f'ETH in dim_asset: {len(eth_dim)} rows')
if len(eth_dim) > 0:
    print(eth_dim.select(['asset_id', 'symbol']))

# Check prices - all dates
prices = pl.read_parquet('data/curated/data_lake/fact_price.parquet')
eth_prices = prices.filter(pl.col('asset_id') == 'ETH')
print(f'\nETH in fact_price (all dates): {len(eth_prices)} rows')
if len(eth_prices) > 0:
    print(f'ETH price date range: {eth_prices["date"].min()} to {eth_prices["date"].max()}')
