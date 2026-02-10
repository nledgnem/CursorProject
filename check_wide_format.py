import pandas as pd
from pathlib import Path

# Check wide format files
curated_dir = Path('data/curated')
prices_wide = curated_dir / 'prices_daily.parquet'
mcaps_wide = curated_dir / 'marketcap_daily.parquet'

if prices_wide.exists():
    df = pd.read_parquet(prices_wide)
    print(f'prices_daily.parquet: {len(df)} rows, {len(df.columns)} columns')
    
    # Check if ETH column exists
    eth_cols = [col for col in df.columns if 'ETH' in str(col).upper()]
    print(f'\nETH-like columns in prices_daily: {eth_cols[:10]}')
    
    if 'ETH' in df.columns:
        eth_data = df['ETH'].dropna()
        print(f'\nETH column: {len(eth_data)} non-null values')
        if len(eth_data) > 0:
            print(f'ETH date range: {eth_data.index.min()} to {eth_data.index.max()}')
            # Check 2024
            eth_2024 = eth_data[(eth_data.index >= '2024-01-01') & (eth_data.index <= '2024-12-31')]
            print(f'ETH in 2024: {len(eth_2024)} values')
    else:
        print('\nNo ETH column found in prices_daily.parquet')
        
    # Check BTC for comparison
    if 'BTC' in df.columns:
        btc_data = df['BTC'].dropna()
        print(f'\nBTC column: {len(btc_data)} non-null values')
        if len(btc_data) > 0:
            print(f'BTC date range: {btc_data.index.min()} to {btc_data.index.max()}')
else:
    print('prices_daily.parquet not found')
