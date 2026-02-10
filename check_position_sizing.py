import polars as pl
import numpy as np

# Load backtest results
bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')

# Check position sizes and returns
print("Position sizing analysis:")
print(f"Average ALT turnover: {bt['alt_turnover'].mean():.4f}")
print(f"Average major turnover: {bt['major_turnover'].mean():.4f}")
print(f"Max ALT turnover: {bt['alt_turnover'].max():.4f}")
print(f"Max major turnover: {bt['major_turnover'].max():.4f}")

# Check if returns are proportional to position sizes
print(f"\nReturn vs Turnover correlation:")
print(f"  ALT turnover vs PnL: {bt.select([pl.corr('alt_turnover', 'pnl')]).item():.4f}")
print(f"  Major turnover vs PnL: {bt.select([pl.corr('major_turnover', 'pnl')]).item():.4f}")

# Check the worst days in detail
worst_days = bt.sort('r_ls_net').head(5)
print(f"\nWorst 5 days detail:")
for row in worst_days.iter_rows(named=True):
    print(f"\n  Date: {row['date']}")
    print(f"    Return: {row['r_ls_net']:.4f} ({row['r_ls_net']*100:.2f}%)")
    print(f"    PnL: {row['pnl']:.4f}")
    print(f"    Cost: {row['cost']:.4f}")
    print(f"    Funding: {row['funding']:.4f}")
    print(f"    ALT turnover: {row['alt_turnover']:.4f}")
    print(f"    Major turnover: {row['major_turnover']:.4f}")
    print(f"    Regime: {row['regime']}")

# Check if PnL makes sense relative to returns
print(f"\nPnL vs Return check:")
print(f"  If PnL = r_ls_net, they should match")
print(f"  Cases where |PnL - r_ls_net| > 0.01:")
mismatch = bt.filter((pl.col('pnl') - pl.col('r_ls_net')).abs() > 0.01)
print(f"    Count: {len(mismatch)}")
if len(mismatch) > 0:
    print(f"    Example: {mismatch.head(3).select(['date', 'pnl', 'r_ls_net'])}")

# Check equity curve calculation
returns = bt['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)
print(f"\nEquity curve check:")
print(f"  First return: {returns[0]:.4f}")
print(f"  First equity: {equity[0]:.4f} (should be 1.0 + first return)")
print(f"  If starting equity was 1.0, first equity should be: {1.0 + returns[0]:.4f}")

# Check if there's a compounding issue
print(f"\nCompounding check:")
print(f"  If we start at 1.0 and compound:")
equity_from_1 = np.cumprod(1.0 + returns)
print(f"    Final equity: {equity_from_1[-1]:.4f}")
print(f"    Max equity: {equity_from_1.max():.4f}")
print(f"    Min equity: {equity_from_1.min():.4f}")

# Check the period around max drawdown
bt_with_equity = bt.with_columns([
    pl.Series('equity', equity_from_1),
])
bt_with_equity = bt_with_equity.with_columns([
    (pl.col('equity') / pl.col('equity').cum_max() - 1.0).alias('dd')
])
max_dd_period = bt_with_equity.filter(pl.col('dd') < -0.5).sort('date')
print(f"\nPeriods with >50% drawdown:")
print(f"  Count: {len(max_dd_period)}")
if len(max_dd_period) > 0:
    print(f"  Date range: {max_dd_period['date'].min()} to {max_dd_period['date'].max()}")
    print(f"  Worst DD: {max_dd_period['dd'].min():.4f}")
