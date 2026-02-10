import pandas as pd

# Check allowlist
df = pd.read_csv('data/perp_allowlist.csv')
print(f'Total coins in allowlist: {len(df)}')

# Check for ETH
eth = df[df['symbol'].str.upper() == 'ETH']
print(f'\nETH in allowlist: {len(eth)} rows')
if len(eth) > 0:
    print(eth[['symbol', 'coingecko_id']].to_string())

# Check for BTC for comparison
btc = df[df['symbol'].str.upper() == 'BTC']
print(f'\nBTC in allowlist: {len(btc)} rows')
if len(btc) > 0:
    print(btc[['symbol', 'coingecko_id']].to_string())

# Show sample
print(f'\nSample allowlist entries (first 10):')
print(df[['symbol', 'coingecko_id']].head(10).to_string())
