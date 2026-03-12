import polars as pl
import numpy as np

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')
returns = bt['r_ls_net'].to_numpy()
equity = np.cumprod(1.0 + returns)

total_return = equity[-1] / equity[0] - 1.0
n_days = len(returns)
cagr = (1.0 + total_return) ** (252.0 / n_days) - 1.0

print("=" * 60)
print("BETA-NEUTRAL MODE BACKTEST RESULTS")
print("=" * 60)
print(f"\nPROFITABILITY:")
print(f"   Total Return: {total_return*100:.2f}%")
print(f"   CAGR: {cagr*100:.2f}%")
print(f"   Final Equity: {equity[-1]:.4f}")
print(f"   Starting Equity: {equity[0]:.4f}")
print(f"   Profitable: {'YES' if equity[-1] > equity[0] else 'NO'}")

print(f"\nRISK-ADJUSTED METRICS:")
mean_ret = np.mean(returns)
std_ret = np.std(returns)
sharpe = (mean_ret / std_ret * np.sqrt(252)) if std_ret > 0 else 0.0
downside = returns[returns < 0]
downside_std = np.std(downside) if len(downside) > 0 else 0.0
sortino = (mean_ret / downside_std * np.sqrt(252)) if downside_std > 0 else 0.0
print(f"   Sharpe Ratio: {sharpe:.4f}")
print(f"   Sortino Ratio: {sortino:.4f}")

print(f"\nDRAWDOWN ANALYSIS:")
running_max = np.maximum.accumulate(equity)
drawdown = (equity - running_max) / running_max
max_dd = np.min(drawdown)
print(f"   Max Drawdown: {max_dd*100:.2f}%")
print(f"   Calmar Ratio: {cagr / abs(max_dd) if max_dd != 0 else 0.0:.4f}")

print(f"\nTRADING STATS:")
print(f"   Hit Rate: {np.mean(returns > 0)*100:.2f}%")
print(f"   Average Daily Return: {np.mean(returns)*100:.4f}%")
print(f"   Volatility (annualized): {np.std(returns) * (252**0.5)*100:.2f}%")
print(f"   Best Day: {np.max(returns)*100:.2f}%")
print(f"   Worst Day: {np.min(returns)*100:.2f}%")

print(f"\nPOSITION SIZING:")
print(f"   ALT Gross: {bt['alt_gross'].mean()*100:.1f}%")
print(f"   Major Gross: {bt['major_gross'].mean()*100:.1f}%")
print(f"   Total Gross: {bt['total_gross'].mean()*100:.1f}%")
print(f"   Net Exposure: {(bt['major_gross'].mean() - bt['alt_gross'].mean())*100:.1f}% (net long)")

print(f"\nPERIOD:")
print(f"   Trading Days: {len(bt)}")
print(f"   Date Range: {bt['date'].min()} to {bt['date'].max()}")

print("\n" + "=" * 60)
