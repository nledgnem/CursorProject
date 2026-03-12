import polars as pl
import numpy as np
import json

# Load backtest results
bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv')
print(f"Total days: {len(bt)}")
print(f"\nDate range: {bt['date'].min()} to {bt['date'].max()}")

# Check for extreme returns
print(f"\nReturn statistics:")
print(f"  Min daily return: {bt['r_ls_net'].min():.4f}")
print(f"  Max daily return: {bt['r_ls_net'].max():.4f}")
print(f"  Mean daily return: {bt['r_ls_net'].mean():.4f}")
print(f"  Std daily return: {bt['r_ls_net'].std():.4f}")

# Check for NaN or infinite values
print(f"\nData quality:")
print(f"  NaN values: {bt['r_ls_net'].null_count()}")
print(f"  Infinite values: {bt.filter(pl.col('r_ls_net').is_infinite()).height}")

# Compute equity curve manually
bt_sorted = bt.sort('date')
returns = bt_sorted['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)

# Find worst drawdown period
running_max = np.maximum.accumulate(equity)
drawdown = (equity - running_max) / running_max
max_dd_idx = int(np.argmin(drawdown))
dates_list = bt_sorted['date'].to_list()
max_dd_date = dates_list[max_dd_idx]

print(f"\nDrawdown analysis:")
print(f"  Max drawdown: {drawdown.min():.4f} ({drawdown.min()*100:.2f}%)")
print(f"  Max drawdown date: {max_dd_date}")
print(f"  Equity at max DD: {equity[max_dd_idx]:.4f}")
print(f"  Equity at peak before DD: {running_max[max_dd_idx]:.4f}")

# Find the worst single day
worst_day_idx = int(np.argmin(returns))
worst_day = dates_list[worst_day_idx]
worst_return = returns[worst_day_idx]

bt_list = bt_sorted.to_dicts()
worst_day_data = bt_list[worst_day_idx]

print(f"\nWorst single day:")
print(f"  Date: {worst_day}")
print(f"  Return: {worst_return:.4f} ({worst_return*100:.2f}%)")
print(f"  PnL: {worst_day_data['pnl']:.4f}")
print(f"  Cost: {worst_day_data['cost']:.4f}")
print(f"  Funding: {worst_day_data['funding']:.4f}")
print(f"  Regime: {worst_day_data['regime']}")

# Check for consecutive large losses
large_losses = bt.filter(pl.col('r_ls_net') < -0.1)
print(f"\nLarge losses (< -10%):")
print(f"  Count: {len(large_losses)}")
if len(large_losses) > 0:
    print(f"  Dates: {large_losses['date'].head(10).to_list()}")

# Check equity curve values
print(f"\nEquity curve statistics:")
print(f"  Min equity: {equity.min():.4f}")
print(f"  Max equity: {equity.max():.4f}")
print(f"  Final equity: {equity[-1]:.4f}")
print(f"  Starting equity: {equity[0]:.4f}")

# Check if equity goes negative or very close to zero
negative_equity = equity < 0
near_zero = equity < 0.1
print(f"\nEquity issues:")
print(f"  Negative equity days: {negative_equity.sum()}")
print(f"  Equity < 0.1 days: {near_zero.sum()}")

# Show worst periods
print(f"\nWorst 10 days:")
worst_days = bt_sorted.with_columns([
    pl.Series('equity', equity),
    pl.Series('drawdown', drawdown),
]).sort('r_ls_net').head(10)
for row in worst_days.iter_rows(named=True):
    print(f"  {row['date']}: return={row['r_ls_net']:.4f}, equity={row['equity']:.4f}, dd={row['drawdown']:.4f}")
