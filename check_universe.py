import pandas as pd

# Check allowlist
allowlist = pd.read_csv('data/perp_allowlist.csv')
print(f"=== ALLOWLIST (Full Universe) ===")
print(f"Total coins in allowlist: {len(allowlist)}")
print(f"\nCoins: {', '.join(allowlist['symbol'].tolist())}")

# Check downloaded price data
prices = pd.read_parquet('data/curated/prices_daily.parquet')
print(f"\n=== DOWNLOADED DATA ===")
print(f"Coins with price data: {len(prices.columns)}")
print(f"Coins: {', '.join(sorted(prices.columns.tolist()))}")

# Check snapshots
snapshots = pd.read_parquet('data/curated/universe_snapshots.parquet')
print(f"\n=== SNAPSHOTS (Selected Baskets) ===")
print(f"Total snapshots: {snapshots['rebalance_date'].nunique()}")
print(f"\nCoins per snapshot:")
for date in sorted(snapshots['rebalance_date'].unique()):
    snapshot_coins = snapshots[snapshots['rebalance_date'] == date]['symbol'].tolist()
    print(f"  {date}: {len(snapshot_coins)} coins")

print(f"\nUnique coins across all snapshots: {snapshots['symbol'].nunique()}")
print(f"All unique coins in baskets: {', '.join(sorted(snapshots['symbol'].unique()))}")

