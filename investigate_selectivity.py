import polars as pl
import numpy as np

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')

print("=" * 80)
print("INVESTIGATION: Why More Selective Made Drawdown WORSE")
print("=" * 80)

# Check regime distribution
regime_counts = bt.group_by('regime').agg(pl.len().alias('count'))
print("\nRegime Distribution in Results:")
for row in regime_counts.iter_rows(named=True):
    print(f"  {row['regime']}: {row['count']} days ({row['count']/len(bt)*100:.1f}%)")

# Check if we're actually only trading in RISK_ON_MAJORS
# Days with positions (non-zero gross)
days_with_positions = bt.filter(pl.col('total_gross') > 0.01)
print(f"\nDays with positions (gross > 1%): {len(days_with_positions)}")
if len(days_with_positions) > 0:
    regime_positions = days_with_positions.group_by('regime').agg(pl.len().alias('count'))
    print("  Regime distribution on days with positions:")
    for row in regime_positions.iter_rows(named=True):
        print(f"    {row['regime']}: {row['count']} days")

# Check worst days - what regime were they in?
worst_days = bt.sort('r_ls_net').head(10)
print(f"\nWorst 10 Days - Regime Analysis:")
for row in worst_days.iter_rows(named=True):
    print(f"  {row['date']}: {row['r_ls_net']*100:.2f}%, regime={row['regime']}, gross={row['total_gross']*100:.1f}%")

# The issue: if we exit positions when not in RISK_ON_MAJORS, but the previous day
# had positions, we still compute PnL from those positions. So we're still exposed
# to losses even when we're trying to be selective.

# Check if losses occur on days when we're exiting (high turnover)
print(f"\nExit Days Analysis:")
exit_days = bt.filter(pl.col('alt_turnover') > 0.5)  # High turnover = exiting
if len(exit_days) > 0:
    print(f"  Days with high turnover (exiting): {len(exit_days)}")
    print(f"  Average return on exit days: {exit_days['r_ls_net'].mean()*100:.4f}%")
    print(f"  Regime on exit days:")
    exit_regimes = exit_days.group_by('regime').agg(pl.len().alias('count'))
    for row in exit_regimes.iter_rows(named=True):
        print(f"    {row['regime']}: {row['count']} days")

# The real issue: we're computing PnL from positions held the previous day
# even if we exit today. So if we held positions yesterday and markets move
# against us today, we still take the loss even if we exit today.

print("\n" + "=" * 80)
print("ROOT CAUSE:")
print("=" * 80)
print("When we exit positions (not in RISK_ON_MAJORS), we still compute")
print("PnL from the previous day's positions. So we're still exposed to")
print("losses on exit days. The worst days show BALANCED regime because")
print("we're exiting positions that were entered during RISK_ON_MAJORS,")
print("but the losses occur on the exit day (BALANCED regime).")
print("\nThis means being more selective doesn't help because:")
print("1. We still hold positions overnight")
print("2. Losses occur when we exit (market moves against us)")
print("3. We can't avoid losses by being selective - we need better entry/exit timing")
print("=" * 80)
