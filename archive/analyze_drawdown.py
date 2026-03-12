import polars as pl
import numpy as np
from datetime import datetime

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns = bt['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)

# Calculate drawdown
running_max = np.maximum.accumulate(equity)
drawdown = (equity - running_max) / running_max

# Find max drawdown period
max_dd_idx = int(np.argmin(drawdown))
max_dd_value = drawdown[max_dd_idx]
max_dd_date = bt['date'][max_dd_idx]

# Find peak before drawdown
peak_idx = int(np.argmax(equity[:max_dd_idx+1]))
peak_date = bt['date'][peak_idx]
peak_equity = equity[peak_idx]

print("=" * 70)
print("MAX DRAWDOWN ANALYSIS")
print("=" * 70)

print(f"\nMax Drawdown: {max_dd_value*100:.2f}%")
print(f"Peak Date: {peak_date}")
print(f"Peak Equity: {peak_equity:.4f}")
print(f"Trough Date: {max_dd_date}")
print(f"Trough Equity: {equity[max_dd_idx]:.4f}")
print(f"Recovery: {equity[-1] / equity[max_dd_idx] - 1.0:.2%} from trough")

# Analyze the drawdown period
dd_period = bt.filter((pl.col('date') >= peak_date) & (pl.col('date') <= max_dd_date))
print(f"\nDrawdown Period: {peak_date} to {max_dd_date}")
print(f"Days in drawdown: {len(dd_period)}")
print(f"Total return during drawdown: {(equity[max_dd_idx] / equity[peak_idx] - 1.0)*100:.2f}%")

# Worst days during drawdown
print(f"\nWorst 10 Days During Drawdown:")
worst_days = dd_period.sort('r_ls_net').head(10)
for row in worst_days.iter_rows(named=True):
    print(f"  {row['date']}: {row['r_ls_net']*100:.2f}% (regime: {row['regime']})")

# Check for consecutive losses
print(f"\nConsecutive Loss Analysis:")
consecutive_losses = 0
max_consecutive = 0
for ret in returns:
    if ret < 0:
        consecutive_losses += 1
        max_consecutive = max(max_consecutive, consecutive_losses)
    else:
        consecutive_losses = 0
print(f"  Max consecutive losing days: {max_consecutive}")

# Check extreme returns
extreme_losses = bt.filter(pl.col('r_ls_net') < -0.05)  # > 5% loss
print(f"\nExtreme Loss Days (>5%): {len(extreme_losses)}")
if len(extreme_losses) > 0:
    print("  Dates:")
    for row in extreme_losses.head(10).iter_rows(named=True):
        print(f"    {row['date']}: {row['r_ls_net']*100:.2f}%")

# Check position sizing during worst period
print(f"\nPosition Sizing During Worst Period:")
worst_day = worst_days.head(1).to_dicts()[0]
print(f"  Worst day ({worst_day['date']}):")
print(f"    ALT gross: {worst_day['alt_gross']*100:.1f}%")
print(f"    Major gross: {worst_day['major_gross']*100:.1f}%")
print(f"    Total gross: {worst_day['total_gross']*100:.1f}%")
print(f"    Net exposure: {(worst_day['major_gross'] - worst_day['alt_gross'])*100:.1f}%")

# Check if drawdown is due to compounding of losses
print(f"\nCompounding Analysis:")
# Find periods with multiple large losses
large_loss_periods = bt.filter(pl.col('r_ls_net') < -0.03)  # > 3% loss
print(f"  Days with >3% loss: {len(large_loss_periods)}")
if len(large_loss_periods) > 0:
    print(f"  Clustered around:")
    for date in large_loss_periods['date'].head(5):
        print(f"    {date}")

# Check equity curve recovery
print(f"\nRecovery Analysis:")
if max_dd_idx < len(equity) - 1:
    recovery_period = equity[max_dd_idx:]
    recovery_max = np.max(recovery_period)
    recovery_ratio = recovery_max / equity[max_dd_idx]
    print(f"  Recovery from trough: {recovery_ratio*100:.1f}%")
    print(f"  Final equity / peak: {equity[-1] / peak_equity:.2%}")

print("\n" + "=" * 70)
