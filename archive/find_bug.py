"""
The issue: -96% drawdown is caused by extreme position sizing.

Looking at the backtest code:
1. ALT basket: weights sum to 1.0 (100% short exposure)
2. Major weights: calculated to offset beta, but could be large
3. PnL = sum(-weight_alt * ret_alt) + sum(weight_major * ret_major)

If ALT weights sum to 1.0 and major weights also sum to 1.0:
- Gross exposure = 200%
- A -10% move in alts = -10% return (correct)
- But if alts move +20% and majors move +10%:
  pnl = -1.0 * 0.20 + 1.0 * 0.10 = -0.10 = -10%

The problem is in _size_majors_for_neutrality():
- It calculates: btc_weight = -alt_btc_exp / 2.0
- If alt_btc_exp is large, this could create very large major positions
- The neutrality solver should constrain total gross exposure

Also, the equity curve starts at 1.0288 instead of 1.0, suggesting
the first day's return is being applied incorrectly.
"""

import polars as pl
import numpy as np

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')

# Check if the issue is position sizing
print("Position sizing analysis:")
print(f"Average PnL magnitude: {bt['pnl'].abs().mean():.4f}")
print(f"Average return magnitude: {bt['r_ls_net'].abs().mean():.4f}")

# The real issue: check if returns are being calculated on the right scale
# If weights are percentages (0-1), then returns should be in the same scale
# But if we have 100% short alts and 100% long majors, that's 200% gross

# Check the worst periods
print("\nWorst drawdown period (Nov-Dec 2024):")
nov_dec = bt.filter((pl.col('date') >= '2024-11-01') & (pl.col('date') <= '2024-12-31'))
print(f"Days: {len(nov_dec)}")
print(f"Total return: {nov_dec['r_ls_net'].sum():.4f}")
print(f"Worst day: {nov_dec['r_ls_net'].min():.4f}")
print(f"Consecutive negative days: {len(nov_dec.filter(pl.col('r_ls_net') < 0))}")

# Check equity curve
returns = bt['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)
print(f"\nEquity curve:")
print(f"Start: {equity[0]:.4f}")
print(f"Min: {equity.min():.4f} (on day {np.argmin(equity)})")
print(f"Max: {equity.max():.4f}")
print(f"End: {equity[-1]:.4f}")

# The bug: if equity starts at 1.0288, that means the first return was applied
# before we started tracking, OR the first day's return is wrong
if abs(equity[0] - 1.0) > 0.01:
    print(f"\nBUG FOUND: Equity doesn't start at 1.0!")
    print(f"First return: {returns[0]:.4f}")
    print(f"First equity: {equity[0]:.4f}")
    print("This suggests the equity curve calculation is wrong")

# Check if the issue is that we're trading every day when we shouldn't
print(f"\nTrading frequency:")
print(f"Total days: {len(bt)}")
print(f"Days with trades (turnover > 0): {bt.filter(pl.col('alt_turnover') > 0).height}")
print(f"Days with no trades: {bt.filter(pl.col('alt_turnover') == 0).height}")

# The real issue: -18.98% in a single day means either:
# 1. Position sizes are too large (200% gross exposure)
# 2. The return calculation is wrong
# 3. We're not properly normalizing by gross exposure

print("\nCONCLUSION:")
print("The -96% drawdown is caused by:")
print("1. Extreme daily returns (-18.98% in one day)")
print("2. These compound over time (equity drops to 0.0423)")
print("3. Position sizing likely creates >100% gross exposure")
print("4. Need to check if weights are being normalized correctly")
