import pandas as pd

prices = pd.read_parquet('data/curated/prices_daily.parquet')
print(f'Prices date range: {prices.index.min()} to {prices.index.max()}')
print(f'Total price days: {len(prices)}')

snapshots = pd.read_parquet('data/curated/universe_snapshots.parquet')
print(f'\nSnapshots rebalance dates: {snapshots["rebalance_date"].min()} to {snapshots["rebalance_date"].max()}')
print(f'Total unique snapshots: {len(snapshots["rebalance_date"].unique())}')

results = pd.read_csv('outputs/backtest_results.csv')
print(f'\nBacktest results date range: {results["date"].min()} to {results["date"].max()}')
print(f'Total backtest days: {len(results)}')

